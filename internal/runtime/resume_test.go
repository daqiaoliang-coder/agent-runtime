package runtime

import (
	"agent-runtime/internal/model"
	"context"
	"errors"
	"testing"
	"time"
)

// fakeStore 实现 runtime.Store 接口，记录每次调用的租户参数并返回可配置结果，
// 用于在无需真实 MySQL 的情况下单元测试 Resumer.Handle 的行为。
type fakeStore struct {
	// 可配置返回值
	getRun           *model.Run
	getRunErr        error
	casOK            bool
	casErr           error
	children         []model.Task
	childrenErr      error
	depsReady        bool
	depsReadyFn      func(nodeID string) (bool, error) // 优先于 depsReady，便于按节点控制
	depsErr          error
	markReadyErr     error
	runComplete      bool
	runCompleteErr   error
	runHasFailure    bool
	runHasFailureErr error

	// 调用记录（仅记录 tenant 参数，用于断言租户透传）
	getRunCalls        []getRunCall
	updateCASCalls     []casCall
	childrenCalls      []string
	depsReadyCalls     []string
	markReadyCalls     []string
	runCompleteCalls   []string
	runHasFailureCalls []string

	// Inbox 模拟：已处理事件集合。InboxSeen 查表、MarkInbox 写表。
	inbox map[string]bool
}

type getRunCall struct{ tenant, id string }
type casCall struct {
	tenant, id string
	version    int64
	status     model.RunStatus
}

func (f *fakeStore) CreateRun(context.Context, *model.Run) error { return nil }
func (f *fakeStore) GetRun(_ context.Context, tenant, id string) (*model.Run, error) {
	f.getRunCalls = append(f.getRunCalls, getRunCall{tenant, id})
	if f.getRunErr != nil {
		return nil, f.getRunErr
	}
	return f.getRun, nil
}
func (f *fakeStore) UpdateRunCAS(_ context.Context, tenant, id string, version int64, status model.RunStatus, _, _ string) (bool, error) {
	f.updateCASCalls = append(f.updateCASCalls, casCall{tenant, id, version, status})
	return f.casOK, f.casErr
}
func (f *fakeStore) InsertPlan(context.Context, string, string, model.Plan) error { return nil }
func (f *fakeStore) MarkReady(_ context.Context, tenant, _ string) error {
	f.markReadyCalls = append(f.markReadyCalls, tenant)
	return f.markReadyErr
}
func (f *fakeStore) Children(_ context.Context, tenant, _ string) ([]model.Task, error) {
	f.childrenCalls = append(f.childrenCalls, tenant)
	return f.children, f.childrenErr
}
func (f *fakeStore) DependenciesReady(_ context.Context, tenant, nodeID string) (bool, error) {
	f.depsReadyCalls = append(f.depsReadyCalls, tenant)
	if f.depsReadyFn != nil {
		return f.depsReadyFn(nodeID)
	}
	return f.depsReady, f.depsErr
}
func (f *fakeStore) RunComplete(_ context.Context, tenant, _ string) (bool, error) {
	f.runCompleteCalls = append(f.runCompleteCalls, tenant)
	return f.runComplete, f.runCompleteErr
}
func (f *fakeStore) RunHasFailure(_ context.Context, tenant, _ string) (bool, error) {
	f.runHasFailureCalls = append(f.runHasFailureCalls, tenant)
	return f.runHasFailure, f.runHasFailureErr
}
func (f *fakeStore) InboxSeen(_ context.Context, _, eventID string) (bool, error) {
	return f.inbox[eventID], nil
}
func (f *fakeStore) MarkInbox(_ context.Context, _, eventID string) error {
	if f.inbox == nil {
		f.inbox = map[string]bool{}
	}
	f.inbox[eventID] = true
	return nil
}

// fakeQueue 记录入队任务，用于断言子节点被正确投递且携带租户。
type fakeQueue struct {
	enqueued []model.Task
	err      error
}

func (q *fakeQueue) Enqueue(_ context.Context, t model.Task) error {
	q.enqueued = append(q.enqueued, t)
	return q.err
}

func completedEvent() model.Event {
	return model.Event{ID: "evt-1", Type: "AgentStepCompleted", RunID: "run-1", NodeID: "node-1", TenantID: "tenant-A", Attempt: 1, Timestamp: time.Now()}
}
func failedEvent() model.Event {
	return model.Event{ID: "evt-2", Type: "AgentStepFailed", RunID: "run-1", NodeID: "node-1", TenantID: "tenant-A", Error: "boom", Timestamp: time.Now()}
}

// TestResumer_IgnoresUnrelatedEvent 非完成/失败事件应被忽略且不触碰存储层。
func TestResumer_IgnoresUnrelatedEvent(t *testing.T) {
	fs := &fakeStore{}
	r := &Resumer{Store: fs, Queue: &fakeQueue{}}
	if err := r.Handle(context.Background(), model.Event{Type: "Other", TenantID: "tenant-A"}); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if len(fs.getRunCalls)+len(fs.updateCASCalls)+len(fs.childrenCalls) != 0 {
		t.Errorf("expected no store calls for unrelated event, got %+v", fs)
	}
}

// TestResumer_InboxDedup 同一事件第二次投递应被 Inbox 跳过，不重复推进 DAG。
func TestResumer_InboxDedup(t *testing.T) {
	fs := &fakeStore{
		getRun:      &model.Run{ID: "run-1", TenantID: "tenant-A", Version: 3, Status: model.RunRunning},
		casOK:       true,
		runComplete: true,
	}
	r := &Resumer{Store: fs, Queue: &fakeQueue{}}
	evt := completedEvent()
	if err := r.Handle(context.Background(), evt); err != nil {
		t.Fatalf("first handle: %v", err)
	}
	firstChildren := len(fs.childrenCalls)
	if firstChildren != 1 {
		t.Fatalf("expected 1 children call on first handle, got %d", firstChildren)
	}
	// 第二次投递：InboxSeen 命中，应直接跳过，不再查 Children。
	if err := r.Handle(context.Background(), evt); err != nil {
		t.Fatalf("second handle: %v", err)
	}
	if len(fs.childrenCalls) != firstChildren {
		t.Errorf("inbox should dedup: children calls grew %d -> %d", firstChildren, len(fs.childrenCalls))
	}
	if !fs.inbox[evt.ID] {
		t.Errorf("inbox should contain event %s after first handle", evt.ID)
	}
}

// TestResumer_StepFailed_FetchesVersionBeforeCAS 验证 Bug2 修复：
// AgentStepFailed 必须先 GetRun 拿到当前 version 再 CAS，而非传 version=0。
func TestResumer_StepFailed_FetchesVersionBeforeCAS(t *testing.T) {
	fs := &fakeStore{
		getRun: &model.Run{ID: "run-1", TenantID: "tenant-A", Version: 7, Status: model.RunRunning},
		casOK:  true,
	}
	r := &Resumer{Store: fs, Queue: &fakeQueue{}}
	if err := r.Handle(context.Background(), failedEvent()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if len(fs.getRunCalls) != 1 {
		t.Fatalf("expected 1 GetRun call before CAS, got %d", len(fs.getRunCalls))
	}
	if len(fs.updateCASCalls) != 1 {
		t.Fatalf("expected 1 UpdateRunCAS call, got %d", len(fs.updateCASCalls))
	}
	got := fs.updateCASCalls[0]
	if got.version != 7 {
		t.Errorf("Bug2: CAS version = %d, want 7 (must fetch current version, previously hardcoded 0)", got.version)
	}
	if got.status != model.RunFailed {
		t.Errorf("CAS status = %s, want FAILED", got.status)
	}
	if got.tenant != "tenant-A" {
		t.Errorf("CAS tenant = %s, want tenant-A", got.tenant)
	}
}

// TestResumer_StepFailed_GetRunError_Propagates 验证 Bug3 修复：
// 失败路径 GetRun 返回错误时不应被吞掉。
func TestResumer_StepFailed_GetRunError_Propagates(t *testing.T) {
	want := errors.New("db down")
	fs := &fakeStore{getRunErr: want}
	r := &Resumer{Store: fs, Queue: &fakeQueue{}}
	err := r.Handle(context.Background(), failedEvent())
	if err == nil || !errors.Is(err, want) {
		t.Fatalf("expected wrapped error, got %v", err)
	}
	if len(fs.updateCASCalls) != 0 {
		t.Errorf("CAS must not be called when GetRun fails, got %d calls", len(fs.updateCASCalls))
	}
}

// TestResumer_Completion_RunHasFailureError_Propagates 验证 Bug3 修复：
// 收敛路径 RunHasFailure 错误此前被 _ 吞掉。
func TestResumer_Completion_RunHasFailureError_Propagates(t *testing.T) {
	want := errors.New("has-failure query failed")
	fs := &fakeStore{runComplete: true, runHasFailureErr: want}
	r := &Resumer{Store: fs, Queue: &fakeQueue{}}
	err := r.Handle(context.Background(), completedEvent())
	if err == nil || !errors.Is(err, want) {
		t.Fatalf("expected wrapped error, got %v", err)
	}
}

// TestResumer_Completion_GetRunError_Propagates 验证 Bug3 修复：
// 收敛路径 GetRun 错误此前被 _ 吞掉，导致 run 为 nil 后续 panic。
func TestResumer_Completion_GetRunError_Propagates(t *testing.T) {
	want := errors.New("getrun failed")
	fs := &fakeStore{runComplete: true, runHasFailure: false, getRunErr: want}
	r := &Resumer{Store: fs, Queue: &fakeQueue{}}
	err := r.Handle(context.Background(), completedEvent())
	if err == nil || !errors.Is(err, want) {
		t.Fatalf("expected wrapped error, got %v", err)
	}
}

// TestResumer_PropagatesTenant 验证 Bug1 修复（调用方侧）：
// 事件携带的 TenantID 必须透传到全部 store 调用与入队任务。
func TestResumer_PropagatesTenant(t *testing.T) {
	child := model.Task{NodeID: "node-2", RunID: "run-1", TenantID: "tenant-A"}
	fs := &fakeStore{
		children:      []model.Task{child},
		depsReady:     true,
		runComplete:   true,
		runHasFailure: false,
		getRun:        &model.Run{ID: "run-1", TenantID: "tenant-A", Version: 3},
		casOK:         true,
	}
	q := &fakeQueue{}
	r := &Resumer{Store: fs, Queue: q}
	if err := r.Handle(context.Background(), completedEvent()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertTenant := func(name string, calls []string) {
		t.Helper()
		if len(calls) == 0 {
			t.Errorf("%s: expected at least one call", name)
			return
		}
		for i, c := range calls {
			if c != "tenant-A" {
				t.Errorf("%s call %d: tenant=%q, want tenant-A", name, i, c)
			}
		}
	}
	assertTenant("Children", fs.childrenCalls)
	assertTenant("DependenciesReady", fs.depsReadyCalls)
	assertTenant("MarkReady", fs.markReadyCalls)
	assertTenant("RunComplete", fs.runCompleteCalls)
	assertTenant("RunHasFailure", fs.runHasFailureCalls)
	if len(fs.getRunCalls) == 0 || fs.getRunCalls[0].tenant != "tenant-A" {
		t.Errorf("GetRun tenant not propagated: %+v", fs.getRunCalls)
	}
	if len(fs.updateCASCalls) == 0 || fs.updateCASCalls[0].tenant != "tenant-A" {
		t.Errorf("UpdateRunCAS tenant not propagated: %+v", fs.updateCASCalls)
	}
	if len(q.enqueued) != 1 || q.enqueued[0].TenantID != "tenant-A" {
		t.Errorf("enqueued task tenant not propagated: %+v", q.enqueued)
	}
}

// TestResumer_Completion_ConvergesToSuccess 收敛成功路径：所有节点终态且无失败 → CAS SUCCESS。
func TestResumer_Completion_ConvergesToSuccess(t *testing.T) {
	fs := &fakeStore{
		runComplete:   true,
		runHasFailure: false,
		getRun:        &model.Run{ID: "run-1", Version: 5},
		casOK:         true,
	}
	r := &Resumer{Store: fs, Queue: &fakeQueue{}}
	if err := r.Handle(context.Background(), completedEvent()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(fs.updateCASCalls) != 1 || fs.updateCASCalls[0].status != model.RunSuccess {
		t.Errorf("expected single CAS with SUCCESS, got %+v", fs.updateCASCalls)
	}
}

// TestResumer_Completion_CASConflict_ReturnsNil 收敛时 CAS 版本冲突（并发已推进）
// 应返回 nil，避免事件无意义重试。
func TestResumer_Completion_CASConflict_ReturnsNil(t *testing.T) {
	fs := &fakeStore{
		runComplete:   true,
		runHasFailure: false,
		getRun:        &model.Run{ID: "run-1", Version: 5},
		casOK:         false,
	}
	r := &Resumer{Store: fs, Queue: &fakeQueue{}}
	if err := r.Handle(context.Background(), completedEvent()); err != nil {
		t.Fatalf("expected nil on CAS conflict, got %v", err)
	}
}

// TestResumer_OnlyActivatesReadyChildren 验证 DAG 拓扑正确性：
// 依赖未就绪的子节点不应被 MarkReady/Enqueue，仅就绪者被激活。
func TestResumer_OnlyActivatesReadyChildren(t *testing.T) {
	ready := model.Task{NodeID: "ready-child", RunID: "run-1", TenantID: "tenant-A"}
	blocked := model.Task{NodeID: "blocked-child", RunID: "run-1", TenantID: "tenant-A"}
	// ready-child 依赖就绪，blocked-child 未就绪。
	fs := &fakeStore{
		children:    []model.Task{ready, blocked},
		depsReadyFn: func(nodeID string) (bool, error) { return nodeID == "ready-child", nil },
		runComplete: false, // 不进入收敛分支，聚焦子节点激活
	}
	q := &fakeQueue{}
	r := &Resumer{Store: fs, Queue: q}
	if err := r.Handle(context.Background(), completedEvent()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(fs.markReadyCalls) != 1 {
		t.Fatalf("expected exactly 1 MarkReady (ready child only), got %d", len(fs.markReadyCalls))
	}
	if len(q.enqueued) != 1 || q.enqueued[0].NodeID != "ready-child" {
		t.Errorf("expected only ready-child enqueued, got %+v", q.enqueued)
	}
}
