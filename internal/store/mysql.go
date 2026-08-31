package store

import (
	"agent-runtime/internal/model"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"
)

type MySQL struct{ DB *sql.DB }

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

func (s *MySQL) CreateRun(ctx context.Context, r *model.Run) error {
	_, err := s.DB.ExecContext(ctx, `INSERT INTO agent_run(run_id,tenant_id,agent_id,status,version,input,max_steps) VALUES(?,?,?,?,?,?,?)`, r.ID, r.TenantID, r.AgentID, r.Status, r.Version, r.Input, r.MaxSteps)
	return err
}
func (s *MySQL) GetRun(ctx context.Context, id string) (*model.Run, error) {
	r := &model.Run{}
	err := s.DB.QueryRowContext(ctx, `SELECT run_id,tenant_id,agent_id,status,version,input,COALESCE(output,''),COALESCE(current_node_id,''),max_steps,steps,created_at,updated_at FROM agent_run WHERE run_id=?`, id).Scan(&r.ID, &r.TenantID, &r.AgentID, &r.Status, &r.Version, &r.Input, &r.Output, &r.CurrentNodeID, &r.MaxSteps, &r.Steps, &r.CreatedAt, &r.UpdatedAt)
	return r, err
}
func (s *MySQL) UpdateRunCAS(ctx context.Context, id string, version int64, status model.RunStatus, currentNode, output string) (bool, error) {
	res, err := s.DB.ExecContext(ctx, `UPDATE agent_run SET status=?,current_node_id=NULLIF(?,''),output=NULLIF(?,''),version=version+1,updated_at=NOW(6) WHERE run_id=? AND version=?`, status, currentNode, output, id, version)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n == 1, nil
}
func (s *MySQL) IncrementRunStepsCAS(ctx context.Context, id string, version int64, nodeID string) (bool, error) {
	res, err := s.DB.ExecContext(ctx, `UPDATE agent_run SET current_node_id=?,steps=steps+1,version=version+1,updated_at=NOW(6) WHERE run_id=? AND version=?`, nodeID, id, version)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n == 1, nil
}

func (s *MySQL) InsertPlan(ctx context.Context, runID string, p model.Plan) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, n := range p.Nodes {
		if _, err = tx.ExecContext(ctx, `INSERT INTO agent_node(node_id,run_id,parent_node_id,type,name,input,status) VALUES(?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE node_id=node_id`, n.ID, runID, n.ParentNodeID, n.Type, n.Name, n.Input, model.NodePending); err != nil {
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
func (s *MySQL) GetNode(ctx context.Context, id string) (*model.Node, error) {
	n := &model.Node{}
	var lease, started, finished sql.NullTime
	err := s.DB.QueryRowContext(ctx, `SELECT node_id,run_id,COALESCE(parent_node_id,''),type,name,COALESCE(input,''),COALESCE(output,''),status,attempt,version,COALESCE(lease_owner,''),lease_until,created_at,started_at,finished_at FROM agent_node WHERE node_id=?`, id).Scan(&n.ID, &n.RunID, &n.ParentNodeID, &n.Type, &n.Name, &n.Input, &n.Output, &n.Status, &n.Attempt, &n.Version, &n.LeaseOwner, &lease, &n.CreatedAt, &started, &finished)
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
func (s *MySQL) ClaimNode(ctx context.Context, id string, version int64, owner string, lease time.Duration) (bool, error) {
	sec := int(lease.Seconds())
	res, err := s.DB.ExecContext(ctx, `UPDATE agent_node SET status=?,version=version+1,lease_owner=?,lease_until=DATE_ADD(NOW(6),INTERVAL ? SECOND),started_at=COALESCE(started_at,NOW(6)) WHERE node_id=? AND version=? AND status IN (?,?) AND (lease_until IS NULL OR lease_until<NOW(6))`, model.NodeRunning, owner, sec, id, version, model.NodePending, model.NodeReady)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n == 1, nil
}
func (s *MySQL) CompleteNode(ctx context.Context, id string, version int64, output string) (bool, error) {
	res, err := s.DB.ExecContext(ctx, `UPDATE agent_node SET status=?,output=?,version=version+1,lease_owner=NULL,lease_until=NULL,finished_at=NOW(6) WHERE node_id=? AND version=? AND status=?`, model.NodeSuccess, output, id, version, model.NodeRunning)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n == 1, nil
}
func (s *MySQL) FailNode(ctx context.Context, id string, version int64) (bool, error) {
	res, err := s.DB.ExecContext(ctx, `UPDATE agent_node SET status=?,version=version+1,lease_owner=NULL,lease_until=NULL,finished_at=NOW(6) WHERE node_id=? AND version=?`, model.NodeFailed, id, version)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n == 1, nil
}
func (s *MySQL) RenewLease(ctx context.Context, id, owner string, version int64, lease time.Duration) (bool, error) {
	sec := int(lease.Seconds())
	res, err := s.DB.ExecContext(ctx, `UPDATE agent_node SET lease_until=DATE_ADD(NOW(6),INTERVAL ? SECOND) WHERE node_id=? AND lease_owner=? AND version=? AND status=?`, sec, id, owner, version, model.NodeRunning)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n == 1, nil
}
func (s *MySQL) RecoverExpired(ctx context.Context, limit int) ([]model.Task, error) {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT node_id,run_id,attempt FROM agent_node WHERE status=? AND lease_until<NOW(6) ORDER BY lease_until LIMIT ? FOR UPDATE SKIP LOCKED`, model.NodeRunning, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var tasks []model.Task
	for rows.Next() {
		var t model.Task
		if err := rows.Scan(&t.NodeID, &t.RunID, &t.Attempt); err != nil {
			return nil, err
		}
		tasks = append(tasks, t)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for _, t := range tasks {
		if _, err := tx.ExecContext(ctx, `UPDATE agent_node SET status=?,lease_owner=NULL,lease_until=NULL,version=version+1 WHERE node_id=?`, model.NodePending, t.NodeID); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return tasks, nil
}
func (s *MySQL) DependenciesReady(ctx context.Context, nodeID string) (bool, error) {
	var n int
	err := s.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM agent_edge e JOIN agent_node n ON n.node_id=e.from_node_id WHERE e.to_node_id=? AND n.status<>?`, nodeID, model.NodeSuccess).Scan(&n)
	return n == 0, err
}
func (s *MySQL) Children(ctx context.Context, nodeID string) ([]model.Task, error) {
	rows, err := s.DB.QueryContext(ctx, `SELECT n.node_id,n.run_id,n.attempt FROM agent_edge e JOIN agent_node n ON n.node_id=e.to_node_id WHERE e.from_node_id=?`, nodeID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.Task
	for rows.Next() {
		var t model.Task
		if err := rows.Scan(&t.NodeID, &t.RunID, &t.Attempt); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}
func (s *MySQL) MarkReady(ctx context.Context, nodeID string) error {
	_, err := s.DB.ExecContext(ctx, `UPDATE agent_node SET status=?,version=version+1 WHERE node_id=? AND status=?`, model.NodeReady, nodeID, model.NodePending)
	return err
}
func (s *MySQL) RunComplete(ctx context.Context, runID string) (bool, error) {
	var n int
	err := s.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM agent_node WHERE run_id=? AND status NOT IN (?,?)`, runID, model.NodeSuccess, model.NodeFailed).Scan(&n)
	return n == 0, err
}
func (s *MySQL) RunHasFailure(ctx context.Context, runID string) (bool, error) {
	var n int
	err := s.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM agent_node WHERE run_id=? AND status=?`, runID, model.NodeFailed).Scan(&n)
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
func (s *MySQL) CompleteNodeWithOutbox(ctx context.Context, node *model.Node, output string, e model.OutboxMessage) (bool, error) {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	res, err := tx.ExecContext(ctx, `UPDATE agent_node SET status=?,output=?,version=version+1,lease_owner=NULL,lease_until=NULL,finished_at=NOW(6) WHERE node_id=? AND version=? AND status=?`, model.NodeSuccess, output, node.ID, node.Version, model.NodeRunning)
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
func (s *MySQL) FailNodeWithOutbox(ctx context.Context, node *model.Node, e model.OutboxMessage) (bool, error) {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	res, err := tx.ExecContext(ctx, `UPDATE agent_node SET status=?,version=version+1,lease_owner=NULL,lease_until=NULL,finished_at=NOW(6) WHERE node_id=? AND version=?`, model.NodeFailed, node.ID, node.Version)
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

func (s *MySQL) ReadyTasks(ctx context.Context, limit int) ([]model.Task, error) {
	rows, err := s.DB.QueryContext(ctx, `SELECT node_id, run_id, attempt FROM agent_node WHERE status=? ORDER BY created_at LIMIT ?`, model.NodeReady, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var tasks []model.Task
	for rows.Next() {
		var t model.Task
		if err := rows.Scan(&t.NodeID, &t.RunID, &t.Attempt); err != nil {
			return nil, err
		}
		tasks = append(tasks, t)
	}
	return tasks, rows.Err()
}

var ErrNotFound = errors.New("not found")
