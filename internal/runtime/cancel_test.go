package runtime

import (
	"agent-runtime/internal/model"
	"context"
	"errors"
	"testing"
)

// TestCancel_Success 正常取消路径：Run 处于 RUNNING，CancelRun 返回 true，
// 应无错误返回，且 CancelRun 收到正确的 tenant/version。
func TestCancel_Success(t *testing.T) {
	fs := &fakeStore{
		getRun:      &model.Run{ID: "run-1", TenantID: "tenant-A", Version: 7, Status: model.RunRunning},
		cancelRunOK: true,
	}
	rt := &Runtime{Store: fs}
	if err := rt.Cancel(context.Background(), "tenant-A", "run-1", "user cancelled"); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if len(fs.cancelRunCalls) != 1 {
		t.Fatalf("expected 1 CancelRun call, got %d", len(fs.cancelRunCalls))
	}
	got := fs.cancelRunCalls[0]
	if got.tenant != "tenant-A" || got.runID != "run-1" {
		t.Errorf("CancelRun args = %+v, want tenant=tenant-A runID=run-1", got)
	}
	if got.version != 7 {
		t.Errorf("CancelRun version = %d, want 7", got.version)
	}
	if got.reason != "user cancelled" {
		t.Errorf("CancelRun reason = %q, want \"user cancelled\"", got.reason)
	}
}

// TestCancel_NotRunning 在非 RUNNING 状态下取消应返回错误，且不调用 CancelRun。
func TestCancel_NotRunning(t *testing.T) {
	fs := &fakeStore{
		getRun: &model.Run{ID: "run-1", Status: model.RunSuccess},
	}
	rt := &Runtime{Store: fs}
	err := rt.Cancel(context.Background(), "tenant-A", "run-1", "user cancelled")
	if err == nil {
		t.Fatal("expected error for non-RUNING run, got nil")
	}
	if len(fs.cancelRunCalls) != 0 {
		t.Errorf("CancelRun should not be called, got %d calls", len(fs.cancelRunCalls))
	}
}

// TestCancel_CASConflict CancelRun 返回 false（版本冲突）应返回错误。
func TestCancel_CASConflict(t *testing.T) {
	fs := &fakeStore{
		getRun:      &model.Run{ID: "run-1", Status: model.RunRunning, Version: 3},
		cancelRunOK: false, // 模拟版本冲突
	}
	rt := &Runtime{Store: fs}
	err := rt.Cancel(context.Background(), "tenant-A", "run-1", "user cancelled")
	if err == nil {
		t.Fatal("expected error on CAS conflict, got nil")
	}
}

// TestCancel_GetRunError GetRun 失败应透传错误。
func TestCancel_GetRunError(t *testing.T) {
	want := errors.New("db down")
	fs := &fakeStore{getRunErr: want}
	rt := &Runtime{Store: fs}
	err := rt.Cancel(context.Background(), "tenant-A", "run-1", "user cancelled")
	if err == nil || !errors.Is(err, want) {
		t.Fatalf("expected wrapped error, got %v", err)
	}
	if len(fs.cancelRunCalls) != 0 {
		t.Errorf("CancelRun should not be called when GetRun fails")
	}
}

// TestCancel_CancelRunError CancelRun 返回错误应透传。
func TestCancel_CancelRunError(t *testing.T) {
	want := errors.New("cancel tx failed")
	fs := &fakeStore{
		getRun:       &model.Run{ID: "run-1", Status: model.RunRunning, Version: 1},
		cancelRunErr: want,
	}
	rt := &Runtime{Store: fs}
	err := rt.Cancel(context.Background(), "tenant-A", "run-1", "user cancelled")
	if err == nil || !errors.Is(err, want) {
		t.Fatalf("expected wrapped error, got %v", err)
	}
}

// --- Resumer 取消收敛测试 ---

func cancelledRun() *model.Run {
	return &model.Run{ID: "run-1", TenantID: "tenant-A", Version: 4, Status: model.RunCancelRequested}
}

// TestResumer_CancelRequested_SkipsChildren Run 处于 CANCEL_REQUESTED 时，
// AgentStepCompleted 不应激活子节点（不调 Children/MarkReady/Enqueue）。
func TestResumer_CancelRequested_SkipsChildren(t *testing.T) {
	fs := &fakeStore{
		getRun:      cancelledRun(),
		children:    []model.Task{{NodeID: "child-1", RunID: "run-1", TenantID: "tenant-A"}},
		depsReady:   true,
		runComplete: false, // 仍有 RUNNING 节点，不收敛
	}
	q := &fakeQueue{}
	r := &Resumer{Store: fs, Queue: q}
	if err := r.Handle(context.Background(), completedEvent()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(fs.childrenCalls) != 0 {
		t.Errorf("Children should not be called for CANCEL_REQUESTED run, got %d calls", len(fs.childrenCalls))
	}
	if len(fs.markReadyCalls) != 0 {
		t.Errorf("MarkReady should not be called for CANCEL_REQUESTED run, got %d calls", len(fs.markReadyCalls))
	}
	if len(q.enqueued) != 0 {
		t.Errorf("no tasks should be enqueued for CANCEL_REQUESTED run, got %d", len(q.enqueued))
	}
}

// TestResumer_CancelRequested_ConvergesToCancelled 全部节点终态时应 CAS 到 CANCELLED。
func TestResumer_CancelRequested_ConvergesToCancelled(t *testing.T) {
	fs := &fakeStore{
		getRun:      cancelledRun(),
		runComplete: true,
		casOK:       true,
	}
	r := &Resumer{Store: fs, Queue: &fakeQueue{}}
	if err := r.Handle(context.Background(), completedEvent()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(fs.updateCASCalls) != 1 {
		t.Fatalf("expected 1 CAS call, got %d", len(fs.updateCASCalls))
	}
	got := fs.updateCASCalls[0]
	if got.status != model.RunCancelled {
		t.Errorf("CAS status = %s, want CANCELLED", got.status)
	}
	if got.version != 4 {
		t.Errorf("CAS version = %d, want 4", got.version)
	}
}

// TestResumer_CancelRequested_NotComplete_NoConverge 仍有 RUNNING 节点时不收敛。
func TestResumer_CancelRequested_NotComplete_NoConverge(t *testing.T) {
	fs := &fakeStore{
		getRun:      cancelledRun(),
		runComplete: false,
		casOK:       true,
	}
	r := &Resumer{Store: fs, Queue: &fakeQueue{}}
	if err := r.Handle(context.Background(), completedEvent()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(fs.updateCASCalls) != 0 {
		t.Errorf("CAS should not be called when run not complete, got %d calls", len(fs.updateCASCalls))
	}
}

// TestResumer_CancelRequested_CASConflict_ReturnsNil 收敛 CAS 冲突应返回 nil（并发已推进）。
func TestResumer_CancelRequested_CASConflict_ReturnsNil(t *testing.T) {
	fs := &fakeStore{
		getRun:      cancelledRun(),
		runComplete: true,
		casOK:       false, // 模拟版本冲突
	}
	r := &Resumer{Store: fs, Queue: &fakeQueue{}}
	if err := r.Handle(context.Background(), completedEvent()); err != nil {
		t.Fatalf("expected nil on CAS conflict, got %v", err)
	}
}

// TestResumer_CancelRequested_FailedEvent_TriggersConverge AgentStepFailed 在 CANCEL_REQUESTED
// 时不应标 FAILED，而应尝试收敛到 CANCELLED。
func TestResumer_CancelRequested_FailedEvent_TriggersConverge(t *testing.T) {
	fs := &fakeStore{
		getRun:      cancelledRun(),
		runComplete: true,
		casOK:       true,
	}
	r := &Resumer{Store: fs, Queue: &fakeQueue{}}
	if err := r.Handle(context.Background(), failedEvent()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(fs.updateCASCalls) != 1 {
		t.Fatalf("expected 1 CAS call, got %d", len(fs.updateCASCalls))
	}
	if fs.updateCASCalls[0].status != model.RunCancelled {
		t.Errorf("CAS status = %s, want CANCELLED (not FAILED)", fs.updateCASCalls[0].status)
	}
}
