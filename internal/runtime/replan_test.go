package runtime

import (
	"agent-runtime/internal/model"
	"context"
	"errors"
	"testing"
	"time"
)

// replanPlanner 是测试用 Planner，记录 Replan 调用并返回可配置的 Plan。
type replanPlanner struct {
	plan       model.Plan
	err        error
	called     bool
	lastRun    *model.Run
	lastNodes  []model.Node
}

func (p *replanPlanner) Plan(_ context.Context, _ *model.Run) (model.Plan, error) {
	return model.Plan{}, nil
}
func (p *replanPlanner) Replan(_ context.Context, run *model.Run, completed []model.Node) (model.Plan, error) {
	p.called = true
	p.lastRun = run
	p.lastNodes = completed
	return p.plan, p.err
}

func replanEvent() model.Event {
	return model.Event{
		ID:        "event-replan-1",
		Type:      "ReplanRequested",
		RunID:     "run-1",
		NodeID:    "reflect-1",
		TenantID:  "tenant-A",
		Attempt:   0,
		Output:    `{"action":"replan","reason":"need more info"}`,
		Timestamp: time.Now(),
	}
}

// TestResumer_ReplanRequested_StoresNewNodes 验证 ReplanRequested 事件触发
// Planner.Replan → InsertPlan → 激活新节点。
func TestResumer_ReplanRequested_StoresNewNodes(t *testing.T) {
	newNode := model.PlanNode{ID: "run-1:r2:finish", Type: model.NodeLLM, Name: "finish", Input: "final answer"}
	planner := &replanPlanner{plan: model.Plan{Nodes: []model.PlanNode{newNode}}}
	fs := &fakeStore{
		getRun:           &model.Run{ID: "run-1", TenantID: "tenant-A", Version: 5, Status: model.RunRunning},
		depsReady:        true,
		completedNodes:   []model.Node{{ID: "reflect-1", Name: "reflect", Output: `{"action":"replan"}`, Status: model.NodeSuccess, PlanningRound: 1}},
		insertPlanCalls:  nil,
	}
	q := &fakeQueue{}
	r := &Resumer{Store: fs, Queue: q, Planner: planner}
	if err := r.Handle(context.Background(), replanEvent()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !planner.called {
		t.Fatal("Planner.Replan was not called")
	}
	if len(fs.insertPlanCalls) != 1 {
		t.Fatalf("expected 1 InsertPlan call, got %d", len(fs.insertPlanCalls))
	}
	// 新节点应被链接到触发节点 reflect-1
	got := fs.insertPlanCalls[0]
	if len(got.plan.Nodes) != 1 {
		t.Fatalf("expected 1 node in plan, got %d", len(got.plan.Nodes))
	}
	node := got.plan.Nodes[0]
	if len(node.DependsOn) != 1 || node.DependsOn[0] != "reflect-1" {
		t.Errorf("expected DependsOn=[reflect-1], got %v", node.DependsOn)
	}
	if node.ParentNodeID != "reflect-1" {
		t.Errorf("expected ParentNodeID=reflect-1, got %q", node.ParentNodeID)
	}
	// 新节点应被 MarkReady + Enqueue
	if len(fs.markReadyCalls) != 1 || fs.markReadyCalls[0].nodeID != "run-1:r2:finish" {
		t.Errorf("expected MarkReady for run-1:r2:finish, got %+v", fs.markReadyCalls)
	}
	if len(q.enqueued) != 1 || q.enqueued[0].NodeID != "run-1:r2:finish" {
		t.Errorf("expected enqueue run-1:r2:finish, got %v", q.enqueued)
	}
}

// TestResumer_ReplanRequested_PlannerNotConfigured Resumer 无 Planner 时应返回错误。
func TestResumer_ReplanRequested_PlannerNotConfigured(t *testing.T) {
	fs := &fakeStore{
		getRun: &model.Run{ID: "run-1", Status: model.RunRunning, Version: 1},
	}
	r := &Resumer{Store: fs, Queue: &fakeQueue{}} // Planner is nil
	err := r.Handle(context.Background(), replanEvent())
	if err == nil {
		t.Fatal("expected error when planner not configured, got nil")
	}
}

// TestResumer_ReplanRequested_CancelledRun_NoReplan 已取消的 Run 不应续规。
func TestResumer_ReplanRequested_CancelledRun_NoReplan(t *testing.T) {
	planner := &replanPlanner{}
	fs := &fakeStore{
		getRun: &model.Run{ID: "run-1", Status: model.RunCancelRequested, Version: 3},
	}
	r := &Resumer{Store: fs, Queue: &fakeQueue{}, Planner: planner}
	if err := r.Handle(context.Background(), replanEvent()); err != nil {
		t.Fatalf("expected nil for cancelled run, got %v", err)
	}
	if planner.called {
		t.Error("Planner.Replan should not be called for cancelled run")
	}
}

// TestResumer_ReplanRequested_CompletedNodesError_Propagates
func TestResumer_ReplanRequested_CompletedNodesError_Propagates(t *testing.T) {
	want := errors.New("completed nodes query failed")
	planner := &replanPlanner{}
	fs := &fakeStore{
		getRun:            &model.Run{ID: "run-1", Status: model.RunRunning, Version: 1},
		completedNodesErr: want,
	}
	r := &Resumer{Store: fs, Queue: &fakeQueue{}, Planner: planner}
	err := r.Handle(context.Background(), replanEvent())
	if err == nil || !errors.Is(err, want) {
		t.Fatalf("expected wrapped error, got %v", err)
	}
	if planner.called {
		t.Error("Planner.Replan should not be called when CompletedNodes fails")
	}
}

// TestResumer_ReplanRequested_ReplanError_Propagates
func TestResumer_ReplanRequested_ReplanError_Propagates(t *testing.T) {
	want := errors.New("planner failed")
	planner := &replanPlanner{err: want}
	fs := &fakeStore{
		getRun:          &model.Run{ID: "run-1", Status: model.RunRunning, Version: 1},
		completedNodes:  []model.Node{{ID: "n1", Status: model.NodeSuccess}},
	}
	r := &Resumer{Store: fs, Queue: &fakeQueue{}, Planner: planner}
	err := r.Handle(context.Background(), replanEvent())
	if err == nil || !errors.Is(err, want) {
		t.Fatalf("expected wrapped error, got %v", err)
	}
}

// TestResumer_ReplanRequested_DepsNotReady_NotActivated 新节点依赖未就绪时不激活。
func TestResumer_ReplanRequested_DepsNotReady_NotActivated(t *testing.T) {
	// 新节点有自己的 DependsOn（指向一个尚未完成的节点），不应被激活
	newNode := model.PlanNode{ID: "run-1:r2:child", Type: model.NodeLLM, Name: "child", DependsOn: []string{"some-other-node"}}
	planner := &replanPlanner{plan: model.Plan{Nodes: []model.PlanNode{newNode}}}
	fs := &fakeStore{
		getRun:         &model.Run{ID: "run-1", Status: model.RunRunning, Version: 1},
		depsReady:      false, // 依赖未就绪
		completedNodes: []model.Node{{ID: "reflect-1", Status: model.NodeSuccess}},
	}
	q := &fakeQueue{}
	r := &Resumer{Store: fs, Queue: q, Planner: planner}
	if err := r.Handle(context.Background(), replanEvent()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// InsertPlan 仍应被调用（节点已入库，等待后续依赖完成后由正常 DAG 推进激活）
	if len(fs.insertPlanCalls) != 1 {
		t.Fatalf("expected 1 InsertPlan call, got %d", len(fs.insertPlanCalls))
	}
	// 但 MarkReady/Enqueue 不应被调用
	if len(fs.markReadyCalls) != 0 {
		t.Errorf("MarkReady should not be called when deps not ready, got %d calls", len(fs.markReadyCalls))
	}
	if len(q.enqueued) != 0 {
		t.Errorf("no tasks should be enqueued when deps not ready, got %d", len(q.enqueued))
	}
}
