package runtime

import (
	"agent-runtime/internal/model"
	"context"
	"testing"
	"time"
)

type hitlFakeStore struct {
	run      *model.Run
	created  bool
	resolved bool
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
