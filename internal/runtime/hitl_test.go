package runtime

import (
	"agent-runtime/internal/model"
	"context"
	"strings"
	"testing"
	"time"
)

type hitlFakeStore struct {
	run      *model.Run
	created  bool
	resolved bool
	rejected bool
	// suspendedNode 记录中断时挂起的节点，模拟 InterruptRun 把节点置为
	// WAITING_HUMAN 的效果，使 ResumeRun 能返回需要重新投递的任务。
	suspendedNode string
}

func (s *hitlFakeStore) CreateRun(context.Context, *model.Run) error { return nil }
func (s *hitlFakeStore) GetRun(context.Context, string, string) (*model.Run, error) {
	cp := *s.run
	return &cp, nil
}
func (s *hitlFakeStore) UpdateRunCAS(_ context.Context, _, _ string, version int64, status model.RunStatus, node, output string) (bool, error) {
	if s.run.Version != version {
		return false, nil
	}
	s.run.Status, s.run.CurrentNodeID, s.run.Output = status, node, output
	s.run.Version++
	return true, nil
}
func (s *hitlFakeStore) InsertPlan(context.Context, string, string, model.Plan) error { return nil }
func (s *hitlFakeStore) MarkReady(context.Context, string, string) error              { return nil }
func (s *hitlFakeStore) Children(context.Context, string, string) ([]model.Task, error) {
	return nil, nil
}
func (s *hitlFakeStore) DependenciesReady(context.Context, string, string) (bool, error) {
	return true, nil
}
func (s *hitlFakeStore) RunComplete(context.Context, string, string) (bool, error) { return true, nil }
func (s *hitlFakeStore) RunHasFailure(context.Context, string, string) (bool, error) {
	return false, nil
}
func (s *hitlFakeStore) CompletedNodes(context.Context, string, string) ([]model.Node, error) {
	return nil, nil
}
func (s *hitlFakeStore) CountNodes(context.Context, string, string) (int, error) {
	return 0, nil
}
func (s *hitlFakeStore) RunTokenUsage(context.Context, string, string) (int, error) {
	return 0, nil
}
func (s *hitlFakeStore) InboxSeen(context.Context, string, string) (bool, error) { return false, nil }
func (s *hitlFakeStore) MarkInbox(context.Context, string, string) error         { return nil }
func (s *hitlFakeStore) InterruptRun(_ context.Context, _, _, nodeID, _ string, version int64) (bool, error) {
	if s.run.Version != version {
		return false, nil
	}
	s.created = true
	s.suspendedNode = nodeID
	s.run.Status = model.RunWaitingHuman
	s.run.Version++
	return true, nil
}
func (s *hitlFakeStore) ResumeRun(_ context.Context, tenant, runID, decision string, version int64) (bool, []model.Task, error) {
	if s.run.Version != version {
		return false, nil, nil
	}
	s.resolved = true
	s.run.Status = model.RunRunning
	s.run.Output = decision
	s.run.Version++
	// 模拟真实实现：放行后把先前挂起的节点重新武装为待调度任务。
	// 返回中断时记录的 nodeID，使测试能验证"恢复必须伴随重新投递"。
	if s.suspendedNode == "" {
		return true, nil, nil
	}
	return true, []model.Task{{RunID: runID, NodeID: s.suspendedNode, TenantID: tenant}}, nil
}
func (s *hitlFakeStore) RejectRun(_ context.Context, _, _, decision string, version int64) (bool, error) {
	if s.run.Version != version {
		return false, nil
	}
	s.rejected = true
	// 与真实实现对齐（store.RejectRun）：Run 切回 RUNNING 而非直接 FAILED——
	// 收敛裁决是 Resumer 的职责，它按 AgentStepFailed 事件把 Run 收敛到终态。
	s.run.Status = model.RunRunning
	s.run.Output = decision
	s.run.Version++
	return true, nil
}

type hitlFakeQueue struct{ enqueued []model.Task }

func (q *hitlFakeQueue) Enqueue(_ context.Context, t model.Task) error {
	q.enqueued = append(q.enqueued, t)
	return nil
}

type noopPlanner struct{}

func (noopPlanner) Plan(context.Context, *model.Run) (model.Plan, error) { return model.Plan{}, nil }
func (noopPlanner) Replan(context.Context, *model.Run, []model.Node) (model.Plan, error) {
	return model.Plan{}, nil
}

func TestRuntime_HITLInterruptAndResume(t *testing.T) {
	s := &hitlFakeStore{run: &model.Run{ID: "r1", TenantID: "t1", Status: model.RunRunning, Version: 0, UpdatedAt: time.Now()}}
	q := &hitlFakeQueue{}
	r := &Runtime{Store: s, Queue: q, Planner: noopPlanner{}}
	if err := r.Interrupt(context.Background(), "t1", "r1", "n1", "approve deployment"); err != nil {
		t.Fatal(err)
	}
	if !s.created || s.run.Status != model.RunWaitingHuman {
		t.Fatalf("run=%+v created=%v", s.run, s.created)
	}
	if err := r.Resume(context.Background(), "t1", "r1", "approved"); err != nil {
		t.Fatal(err)
	}
	if !s.resolved || s.run.Status != model.RunRunning || s.run.Output != "approved" {
		t.Fatalf("run=%+v resolved=%v", s.run, s.resolved)
	}
	// 放行必须伴随重新投递：否则 Run 回到 RUNNING 却没有节点在跑，
	// 表现为"人工批了但流程没动"，且不会有任何错误暴露出来。
	if len(q.enqueued) != 1 {
		t.Fatalf("expected suspended node to be re-enqueued after resume, got %d task(s): %+v", len(q.enqueued), q.enqueued)
	}
	got := q.enqueued[0]
	if got.NodeID != "n1" || got.RunID != "r1" || got.TenantID != "t1" {
		t.Errorf("re-enqueued task lost identity: %+v", got)
	}
}

// TestRuntime_Reject 否决是 Resume 的对偶：状态机走 WAITING_HUMAN →
// RejectRun，Run 切回 RUNNING 等待 Resumer 收敛；与 Resume 的关键差异是
// 绝不重新投递——真实实现里被挂起节点已置 FAILED，若这里投递任务，
// 调度器会捞起一个已失败的节点再跑一遍。
func TestRuntime_Reject(t *testing.T) {
	s := &hitlFakeStore{run: &model.Run{ID: "r1", TenantID: "t1", Status: model.RunRunning, Version: 0, UpdatedAt: time.Now()}}
	q := &hitlFakeQueue{}
	r := &Runtime{Store: s, Queue: q, Planner: noopPlanner{}}
	if err := r.Interrupt(context.Background(), "t1", "r1", "n1", "l5 approval"); err != nil {
		t.Fatal(err)
	}
	if err := r.Reject(context.Background(), "t1", "r1", "denied by ops@example.com"); err != nil {
		t.Fatal(err)
	}
	if !s.rejected {
		t.Fatal("RejectRun must be invoked")
	}
	if s.run.Status != model.RunRunning {
		t.Fatalf("run must be back to RUNNING for the Resumer to converge, got %s", s.run.Status)
	}
	if s.run.Output != "denied by ops@example.com" {
		t.Fatalf("decision must be recorded for audit, got %q", s.run.Output)
	}
	if s.resolved {
		t.Error("reject must never resume the run")
	}
	if len(q.enqueued) != 0 {
		t.Fatalf("reject must not re-enqueue any task, got %+v", q.enqueued)
	}
}

// TestRuntime_RejectRequiresWaiting 只有等待人工的 Run 可被否决。
// 对 RUNNING 的 Run 调用 Reject 意味着并发状态竞争（审批人看到的
// 是过期快照），必须报错而非静默成功——否则否决记录会指向一个
// 已经继续执行的 Run，审计与实际状态脱节。
func TestRuntime_RejectRequiresWaiting(t *testing.T) {
	s := &hitlFakeStore{run: &model.Run{ID: "r1", TenantID: "t1", Status: model.RunRunning, Version: 0, UpdatedAt: time.Now()}}
	r := &Runtime{Store: s, Queue: &hitlFakeQueue{}, Planner: noopPlanner{}}
	err := r.Reject(context.Background(), "t1", "r1", "stale view")
	if err == nil || !strings.Contains(err.Error(), "not waiting for human") {
		t.Fatalf("expected state-machine error, got %v", err)
	}
	if s.rejected {
		t.Error("non-waiting run must not be touched")
	}
}

// TestRuntime_RejectRequiresRejectStore 能力探测：只实现 runtime.Store 的
// 存储实现（嵌入式接口替身）必须得到明确报错，而不是 panic——
// RejectStore 与 HITLStore/CancelStore 同属小接口模式，老替身/老实现
// 不该因为新增否决能力而全体升级。
func TestRuntime_RejectRequiresRejectStore(t *testing.T) {
	// rejectlessStore 嵌入 Store 接口：满足 Runtime 的全部既有依赖，
	// 但故意不实现 RejectStore。
	type rejectlessStore struct{ Store }
	s := &hitlFakeStore{run: &model.Run{ID: "r1", TenantID: "t1", Status: model.RunWaitingHuman, Version: 0, UpdatedAt: time.Now()}}
	r := &Runtime{Store: &rejectlessStore{Store: s}, Queue: &hitlFakeQueue{}, Planner: noopPlanner{}}
	err := r.Reject(context.Background(), "t1", "r1", "unsupported")
	if err == nil || !strings.Contains(err.Error(), "reject store is not configured") {
		t.Fatalf("expected capability error, got %v", err)
	}
}
