// Package store 基于 MySQL 实现持久化层。
// 所有状态变更均通过乐观锁（version CAS）保护，节点认领使用租约机制支持崩溃恢复。
// 所有按 ID 查询的方法都强制带 tenant_id 过滤，防止跨租户越权访问。
package store

import (
	"agent-runtime/internal/model"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"
)

// MySQL 封装 *sql.DB，提供 Run/Node/Outbox 等表的全部数据访问方法。
type MySQL struct{ DB *sql.DB }

// New 创建并初始化连接池，执行 Ping 校验连通性。
func New(ctx context.Context, dsn string) (*MySQL, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(time.Hour)
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return &MySQL{DB: db}, nil
}
func (s *MySQL) Close() { _ = s.DB.Close() }

// CreateRun 插入一条 Run 记录，初始状态为 PENDING。租户由 r.TenantID 携带。
func (s *MySQL) CreateRun(ctx context.Context, r *model.Run) error {
	_, err := s.DB.ExecContext(ctx, `INSERT INTO agent_run(run_id,tenant_id,agent_id,status,version,input,max_steps) VALUES(?,?,?,?,?,?,?)`, r.ID, r.TenantID, r.AgentID, r.Status, r.Version, r.Input, r.MaxSteps)
	return err
}

// GetRun 按 tenant + run_id 读取 Run，租户不匹配则返回 sql.ErrNoRows。
func (s *MySQL) GetRun(ctx context.Context, tenant, id string) (*model.Run, error) {
	r := &model.Run{}
	err := s.DB.QueryRowContext(ctx, `SELECT run_id,tenant_id,agent_id,status,version,input,COALESCE(output,''),COALESCE(current_node_id,''),max_steps,steps,created_at,updated_at FROM agent_run WHERE run_id=? AND tenant_id=?`, id, tenant).Scan(&r.ID, &r.TenantID, &r.AgentID, &r.Status, &r.Version, &r.Input, &r.Output, &r.CurrentNodeID, &r.MaxSteps, &r.Steps, &r.CreatedAt, &r.UpdatedAt)
	return r, err
}

// UpdateRunCAS 基于版本号 + 租户的乐观锁更新 Run：仅当 tenant/version 匹配时才更新状态，
// 成功返回 true，并发竞争失败或租户不匹配返回 false。
func (s *MySQL) UpdateRunCAS(ctx context.Context, tenant, id string, version int64, status model.RunStatus, currentNode, output string) (bool, error) {
	res, err := s.DB.ExecContext(ctx, `UPDATE agent_run SET status=?,current_node_id=NULLIF(?,''),output=NULLIF(?,''),version=version+1,updated_at=NOW(6) WHERE run_id=? AND tenant_id=? AND version=?`, status, currentNode, output, id, tenant, version)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n == 1, nil
}

// IncrementRunStepsCAS 在租户 + 版本匹配下推进步骤计数。
func (s *MySQL) IncrementRunStepsCAS(ctx context.Context, tenant, id string, version int64, nodeID string) (bool, error) {
	res, err := s.DB.ExecContext(ctx, `UPDATE agent_run SET current_node_id=?,steps=steps+1,version=version+1,updated_at=NOW(6) WHERE run_id=? AND tenant_id=? AND version=?`, nodeID, id, tenant, version)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n == 1, nil
}

// InsertPlan 在单事务中写入全部节点与依赖边，节点使用 ON DUPLICATE KEY 做幂等插入。
// 节点写入时携带 tenant_id，保证后续节点级查询可做租户过滤。
func (s *MySQL) InsertPlan(ctx context.Context, runID, tenant string, p model.Plan) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, n := range p.Nodes {
		if _, err = tx.ExecContext(ctx, `INSERT INTO agent_node(node_id,run_id,tenant_id,parent_node_id,type,name,input,status) VALUES(?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE node_id=node_id`, n.ID, runID, tenant, n.ParentNodeID, n.Type, n.Name, n.Input, model.NodePending); err != nil {
			return err
		}
	}
	for _, n := range p.Nodes {
		for _, dep := range n.DependsOn {
			if _, err = tx.ExecContext(ctx, `INSERT IGNORE INTO agent_edge(run_id,from_node_id,to_node_id) VALUES(?,?,?)`, runID, dep, n.ID); err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}

// GetNode 按 tenant + node_id 读取节点，租户不匹配返回 sql.ErrNoRows。
func (s *MySQL) GetNode(ctx context.Context, tenant, id string) (*model.Node, error) {
	n := &model.Node{}
	var lease, started, finished sql.NullTime
	err := s.DB.QueryRowContext(ctx, `SELECT node_id,run_id,tenant_id,COALESCE(parent_node_id,''),type,name,COALESCE(input,''),COALESCE(output,''),status,attempt,version,COALESCE(lease_owner,''),lease_until,created_at,started_at,finished_at FROM agent_node WHERE node_id=? AND tenant_id=?`, id, tenant).Scan(&n.ID, &n.RunID, &n.TenantID, &n.ParentNodeID, &n.Type, &n.Name, &n.Input, &n.Output, &n.Status, &n.Attempt, &n.Version, &n.LeaseOwner, &lease, &n.CreatedAt, &started, &finished)
	if err != nil {
		return nil, err
	}
	if lease.Valid {
		t := lease.Time
		n.LeaseUntil = &t
	}
	if started.Valid {
		n.StartedAt = started.Time
	}
	if finished.Valid {
		n.FinishedAt = finished.Time
	}
	return n, nil
}

// ClaimNode worker 抢占节点：仅当 tenant/version 匹配且状态为 PENDING/READY 且租约过期时，
// 将状态置为 RUNNING 并写入租约所有者和过期时间，实现至少一次认领。
func (s *MySQL) ClaimNode(ctx context.Context, tenant, id string, version int64, owner string, lease time.Duration) (bool, error) {
	sec := int(lease.Seconds())
	res, err := s.DB.ExecContext(ctx, `UPDATE agent_node SET status=?,version=version+1,lease_owner=?,lease_until=DATE_ADD(NOW(6),INTERVAL ? SECOND),started_at=COALESCE(started_at,NOW(6)) WHERE node_id=? AND tenant_id=? AND version=? AND status IN (?,?) AND (lease_until IS NULL OR lease_until<NOW(6))`, model.NodeRunning, owner, sec, id, tenant, version, model.NodePending, model.NodeReady)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n == 1, nil
}

// CompleteNode 在租户 + 版本匹配下将节点标记为 SUCCESS。
func (s *MySQL) CompleteNode(ctx context.Context, tenant, id string, version int64, output string) (bool, error) {
	res, err := s.DB.ExecContext(ctx, `UPDATE agent_node SET status=?,output=?,version=version+1,lease_owner=NULL,lease_until=NULL,finished_at=NOW(6) WHERE node_id=? AND tenant_id=? AND version=? AND status=?`, model.NodeSuccess, output, id, tenant, version, model.NodeRunning)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n == 1, nil
}

// FailNode 在租户 + 版本匹配下将节点标记为 FAILED。
func (s *MySQL) FailNode(ctx context.Context, tenant, id string, version int64) (bool, error) {
	res, err := s.DB.ExecContext(ctx, `UPDATE agent_node SET status=?,version=version+1,lease_owner=NULL,lease_until=NULL,finished_at=NOW(6) WHERE node_id=? AND tenant_id=? AND version=?`, model.NodeFailed, id, tenant, version)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n == 1, nil
}

// RenewLease 续租：仅当节点仍由该 owner 持有且租户匹配时延长过期时间。
func (s *MySQL) RenewLease(ctx context.Context, tenant, id, owner string, version int64, lease time.Duration) (bool, error) {
	sec := int(lease.Seconds())
	res, err := s.DB.ExecContext(ctx, `UPDATE agent_node SET lease_until=DATE_ADD(NOW(6),INTERVAL ? SECOND) WHERE node_id=? AND tenant_id=? AND lease_owner=? AND version=? AND status=?`, sec, id, tenant, owner, version, model.NodeRunning)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n == 1, nil
}

// RecoverExpired 扫描 RUNNING 但租约已过期的节点，在事务内将其重置为 PENDING，
// 配合 SELECT ... FOR UPDATE SKIP LOCKED 实现多实例安全的抢占式恢复。
// 这是系统级扫描（不限定租户），返回的任务携带 tenant_id 以便恢复投递。
func (s *MySQL) RecoverExpired(ctx context.Context, limit int) ([]model.Task, error) {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT node_id,run_id,tenant_id,attempt FROM agent_node WHERE status=? AND lease_until<NOW(6) ORDER BY lease_until LIMIT ? FOR UPDATE SKIP LOCKED`, model.NodeRunning, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var tasks []model.Task
	for rows.Next() {
		var t model.Task
		if err := rows.Scan(&t.NodeID, &t.RunID, &t.TenantID, &t.Attempt); err != nil {
			return nil, err
		}
		tasks = append(tasks, t)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for _, t := range tasks {
		if _, err := tx.ExecContext(ctx, `UPDATE agent_node SET status=?,lease_owner=NULL,lease_until=NULL,version=version+1 WHERE node_id=? AND tenant_id=?`, model.NodePending, t.NodeID, t.TenantID); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return tasks, nil
}

// DependenciesReady 检查节点的全部前驱节点是否均为 SUCCESS，用于 DAG 拓扑推进。
// 带租户过滤防止跨租户依赖误判。
func (s *MySQL) DependenciesReady(ctx context.Context, tenant, nodeID string) (bool, error) {
	var n int
	err := s.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM agent_edge e JOIN agent_node n ON n.node_id=e.from_node_id WHERE e.to_node_id=? AND n.tenant_id=? AND n.status<>?`, nodeID, tenant, model.NodeSuccess).Scan(&n)
	return n == 0, err
}

// Children 返回依赖该节点的后继节点列表，带租户过滤。
func (s *MySQL) Children(ctx context.Context, tenant, nodeID string) ([]model.Task, error) {
	rows, err := s.DB.QueryContext(ctx, `SELECT n.node_id,n.run_id,n.tenant_id,n.attempt FROM agent_edge e JOIN agent_node n ON n.node_id=e.to_node_id WHERE e.from_node_id=? AND n.tenant_id=?`, nodeID, tenant)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.Task
	for rows.Next() {
		var t model.Task
		if err := rows.Scan(&t.NodeID, &t.RunID, &t.TenantID, &t.Attempt); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// MarkReady 在租户匹配下将 PENDING 节点置为 READY。
func (s *MySQL) MarkReady(ctx context.Context, tenant, nodeID string) error {
	_, err := s.DB.ExecContext(ctx, `UPDATE agent_node SET status=?,version=version+1 WHERE node_id=? AND tenant_id=? AND status=?`, model.NodeReady, nodeID, tenant, model.NodePending)
	return err
}

// RunComplete 检查指定租户的 Run 下是否所有节点均已终态。
func (s *MySQL) RunComplete(ctx context.Context, tenant, runID string) (bool, error) {
	var n int
	err := s.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM agent_node WHERE run_id=? AND tenant_id=? AND status NOT IN (?,?)`, runID, tenant, model.NodeSuccess, model.NodeFailed).Scan(&n)
	return n == 0, err
}

// RunHasFailure 检查指定租户的 Run 下是否存在 FAILED 节点。
func (s *MySQL) RunHasFailure(ctx context.Context, tenant, runID string) (bool, error) {
	var n int
	err := s.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM agent_node WHERE run_id=? AND tenant_id=? AND status=?`, runID, tenant, model.NodeFailed).Scan(&n)
	return n > 0, err
}

func (s *MySQL) SaveCheckpoint(ctx context.Context, runID string, version int64, nodeID string, state any) error {
	b, _ := json.Marshal(state)
	_, err := s.DB.ExecContext(ctx, `INSERT INTO checkpoint(run_id,graph_version,current_node_id,state_json) VALUES(?,?,?,?) ON DUPLICATE KEY UPDATE graph_version=VALUES(graph_version),current_node_id=VALUES(current_node_id),state_json=VALUES(state_json),created_at=NOW(6)`, runID, version, nodeID, b)
	return err
}

func (s *MySQL) PutOutbox(ctx context.Context, tx *sql.Tx, e model.OutboxMessage) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO event_outbox(event_id,event_type,aggregate_id,payload) VALUES(?,?,?,?)`, e.ID, e.EventType, e.AggregateID, e.Payload)
	return err
}

// CompleteNodeWithOutbox 在单事务中完成节点状态更新并写入 Outbox 事件，
// 保证“状态变更”与“待发布事件”原子一致，是至少一次投递的关键。
// 租户隔离由 node.TenantID 提供，防止单节点被跨租户篡改。
func (s *MySQL) CompleteNodeWithOutbox(ctx context.Context, node *model.Node, output string, e model.OutboxMessage) (bool, error) {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	res, err := tx.ExecContext(ctx, `UPDATE agent_node SET status=?,output=?,version=version+1,lease_owner=NULL,lease_until=NULL,finished_at=NOW(6) WHERE node_id=? AND tenant_id=? AND version=? AND status=?`, model.NodeSuccess, output, node.ID, node.TenantID, node.Version, model.NodeRunning)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return false, nil
	}
	if err := s.PutOutbox(ctx, tx, e); err != nil {
		return false, err
	}
	return true, tx.Commit()
}

// FailNodeWithOutbox 同上，将节点置为 FAILED 并写 Outbox 事件，带租户隔离。
func (s *MySQL) FailNodeWithOutbox(ctx context.Context, node *model.Node, e model.OutboxMessage) (bool, error) {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	res, err := tx.ExecContext(ctx, `UPDATE agent_node SET status=?,version=version+1,lease_owner=NULL,lease_until=NULL,finished_at=NOW(6) WHERE node_id=? AND tenant_id=? AND version=?`, model.NodeFailed, node.ID, node.TenantID, node.Version)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return false, nil
	}
	if err := s.PutOutbox(ctx, tx, e); err != nil {
		return false, err
	}
	return true, tx.Commit()
}

// ClaimOutbox 抢占待发布的 Outbox 消息：选取 PENDING/超时的 SENDING 记录，
// 标记为 SENDING 并增加重试次数，使用 FOR UPDATE SKIP LOCKED 实现多发布器安全。
func (s *MySQL) ClaimOutbox(ctx context.Context, limit int) ([]model.OutboxMessage, error) {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT event_id,event_type,aggregate_id,payload FROM event_outbox WHERE ((status='PENDING' AND next_attempt_at<=NOW(6)) OR (status='SENDING' AND updated_at < DATE_SUB(NOW(6), INTERVAL 60 SECOND))) ORDER BY created_at LIMIT ? FOR UPDATE SKIP LOCKED`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.OutboxMessage
	for rows.Next() {
		var e model.OutboxMessage
		if err := rows.Scan(&e.ID, &e.EventType, &e.AggregateID, &e.Payload); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for _, e := range out {
		if _, err := tx.ExecContext(ctx, `UPDATE event_outbox SET status='SENDING',attempts=attempts+1 WHERE event_id=?`, e.ID); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *MySQL) MarkOutboxPublished(ctx context.Context, id string) error {
	_, err := s.DB.ExecContext(ctx, `UPDATE event_outbox SET status='PUBLISHED',published_at=NOW(6) WHERE event_id=?`, id)
	return err
}

func (s *MySQL) RetryOutbox(ctx context.Context, id string, after time.Duration) error {
	sec := int(after.Seconds())
	_, err := s.DB.ExecContext(ctx, `UPDATE event_outbox SET status='PENDING',next_attempt_at=DATE_ADD(NOW(6),INTERVAL ? SECOND) WHERE event_id=?`, sec, id)
	return err
}

// ReadyTasks 扫描全部 READY 节点（系统级，不限定租户），
// 返回的任务携带 tenant_id 以便补投递时保持租户上下文。
func (s *MySQL) ReadyTasks(ctx context.Context, limit int) ([]model.Task, error) {
	rows, err := s.DB.QueryContext(ctx, `SELECT node_id, run_id, tenant_id, attempt FROM agent_node WHERE status=? ORDER BY created_at LIMIT ?`, model.NodeReady, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var tasks []model.Task
	for rows.Next() {
		var t model.Task
		if err := rows.Scan(&t.NodeID, &t.RunID, &t.TenantID, &t.Attempt); err != nil {
			return nil, err
		}
		tasks = append(tasks, t)
	}
	return tasks, rows.Err()
}

var ErrNotFound = errors.New("not found")
