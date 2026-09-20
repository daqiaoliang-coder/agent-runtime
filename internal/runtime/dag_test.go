package runtime

import (
	"agent-runtime/internal/model"
	"context"
	"errors"
	"testing"
)

// failPlanner 在 Plan 阶段总是报错，用于测试规划失败时的 Run 收敛。
type failPlanner struct{}

func (failPlanner) Plan(context.Context, *model.Run) (model.Plan, error) {
	return model.Plan{}, errors.New("boom planner")
}
func (failPlanner) Replan(context.Context, *model.Run, []model.Node) (model.Plan, error) {
	return model.Plan{}, nil
}

// TestCreateRun_PlannerFailure_MarksRunFailed 规划失败时应把已落库的 PENDING Run
// CAS 为 FAILED，且不插入计划、不投递任何任务。
func TestCreateRun_PlannerFailure_MarksRunFailed(t *testing.T) {
	fs := &fakeStore{}
	q := &fakeQueue{}
	rt := &Runtime{Store: fs, Queue: q, Planner: failPlanner{}}
	if _, err := rt.CreateRun(context.Background(), "tenant-A", "demo", "bad goal", ""); err == nil {
		t.Fatal("expected planning error")
	}
	if len(fs.updateCASCalls) != 1 {
		t.Fatalf("expected 1 UpdateRunCAS, got %d", len(fs.updateCASCalls))
	}
	call := fs.updateCASCalls[0]
	if call.status != model.RunFailed {
		t.Errorf("status=%s, want FAILED", call.status)
	}
	if call.tenant != "tenant-A" {
		t.Errorf("tenant=%q, want tenant-A", call.tenant)
	}
	if len(fs.insertPlanCalls) != 0 {
		t.Errorf("plan must not be inserted, got %d calls", len(fs.insertPlanCalls))
	}
	if len(q.enqueued) != 0 {
		t.Errorf("no tasks should be enqueued, got %d", len(q.enqueued))
	}
}

func TestDemoPlannerBuildsParallelDAG(t *testing.T) {
	p, err := DemoPlanner{}.Plan(context.Background(), &model.Run{ID: "r1", Input: "x"})
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Nodes) != 4 {
		t.Fatalf("nodes=%d", len(p.Nodes))
	}
	if len(p.Nodes[0].DependsOn) != 0 || len(p.Nodes[1].DependsOn) != 0 {
		t.Fatal("first two nodes should be parallel")
	}
	if len(p.Nodes[2].DependsOn) != 2 {
		t.Fatal("reason should have two dependencies")
	}
}
