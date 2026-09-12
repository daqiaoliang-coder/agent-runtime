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
func (s *hitlFakeStore) InterruptRun(_ context.Context, _, _, _, _ string, version int64) (bool, error) {
	if s.run.Version != version {
		return false, nil
	}
	s.created = true
	s.run.Status = model.RunWaitingHuman
	s.run.Version++
	return true, nil
}
func (s *hitlFakeStore) ResumeRun(_ context.Context, _, _, decision string, version int64) (bool, error) {
	if s.run.Version != version {
		return false, nil
	}
	s.resolved = true
	s.run.Status = model.RunRunning
	s.run.Output = decision
	s.run.Version++
	return true, nil
}

type hitlFakeQueue struct{}

func (hitlFakeQueue) Enqueue(context.Context, model.Task) error { return nil }

type noopPlanner struct{}

func (noopPlanner) Plan(context.Context, *model.Run) (model.Plan, error) { return model.Plan{}, nil }
func (noopPlanner) Replan(context.Context, *model.Run, []model.Node) (model.Plan, error) {
	return model.Plan{}, nil
}

func TestRuntime_HITLInterruptAndResume(t *testing.T) {
	s := &hitlFakeStore{run: &model.Run{ID: "r1", TenantID: "t1", Status: model.RunRunning, Version: 0, UpdatedAt: time.Now()}}
	r := &Runtime{Store: s, Queue: hitlFakeQueue{}, Planner: noopPlanner{}}
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
}
