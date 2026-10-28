package main

import (
	"agent-runtime/internal/model"
	"agent-runtime/internal/runtime"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestNewResumer_Replan(t *testing.T) {
	for _, tc := range []struct {
		name         string
		provider     string
		credential   bool
		defaultModel string
		plannerModel string
		wantModel    string
	}{
		{name: "demo"},
		{name: "legacy without credential", provider: "legacy"},
		{name: "legacy default model", provider: "legacy", credential: true, defaultModel: "default-model", wantModel: "default-model"},
		{name: "legacy planner override", provider: "legacy", credential: true, defaultModel: "default-model", plannerModel: "planning-model", wantModel: "planning-model"},
		{name: "named default model", provider: "named", credential: true, defaultModel: "test/default-model", wantModel: "default-model"},
		{name: "named planner override", provider: "named", credential: true, defaultModel: "test/default-model", plannerModel: "test/planning-model", wantModel: "planning-model"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clearPlannerEnv(t)
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.Method != http.MethodPost || r.URL.Path != "/chat/completions" {
					t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
				}
				if r.Header.Get("Authorization") != "Bearer test-key" {
					t.Error("model credential was not applied")
				}
				var request struct {
					Model    string `json:"model"`
					Messages []struct {
						Content string `json:"content"`
					} `json:"messages"`
				}
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Errorf("decode model request: %v", err)
					http.Error(w, "invalid request", http.StatusBadRequest)
					return
				}
				if request.Model != tc.wantModel {
					t.Errorf("model = %q, want %q", request.Model, tc.wantModel)
				}
				if len(request.Messages) != 2 || !strings.Contains(request.Messages[1].Content, "need more evidence") {
					t.Errorf("completed node output missing from replan context: %+v", request.Messages)
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, `{"choices":[{"message":{"content":"{\"nodes\":[{\"id\":\"finish\",\"type\":\"LLM\",\"name\":\"finish\",\"input\":\"summarize\",\"dependsOn\":[]}] }"}}]}`)
			}))
			defer server.Close()
			t.Setenv("LLM_DEFAULT_MODEL", tc.defaultModel)
			t.Setenv("LLM_PLANNER_MODEL", tc.plannerModel)
			switch tc.provider {
			case "legacy":
				t.Setenv("OPENAI_BASE_URL", server.URL)
				if tc.credential {
					t.Setenv("CHAT_API_KEY", "test-key")
				}
			case "named":
				t.Setenv("LLM_PROVIDERS", "test")
				t.Setenv("LLM_PROVIDER_TEST_BASE_URL", server.URL)
				t.Setenv("LLM_PROVIDER_TEST_API_KEY", "test-key")
			}

			s := &replanStore{}
			q := &replanQueue{}
			res, err := newResumer(s, q)
			if err != nil {
				t.Fatal(err)
			}
			if tc.wantModel == "" {
				if _, ok := res.Planner.(runtime.DemoPlanner); !ok {
					t.Fatalf("expected DemoPlanner, got %T", res.Planner)
				}
			}
			e := model.Event{ID: "event-1", Type: "ReplanRequested", TenantID: "tenant-1", RunID: "run-1", NodeID: "reflect-1"}
			if err := res.Handle(context.Background(), e); err != nil {
				t.Fatalf("handle replan: %v", err)
			}
			if len(s.plan.Nodes) != 1 || len(s.decision.Nodes) != 1 {
				t.Fatalf("replan was not persisted: plan=%+v decision=%+v", s.plan, s.decision)
			}
			node := s.plan.Nodes[0]
			if node.Type != model.NodeLLM || node.Name != "finish" || node.ParentNodeID != e.NodeID || len(node.DependsOn) != 1 || node.DependsOn[0] != e.NodeID {
				t.Fatalf("unexpected replanned node: %+v", node)
			}
			if s.ready != node.ID || !s.inbox {
				t.Fatalf("node not activated or event not acknowledged: ready=%q inbox=%v", s.ready, s.inbox)
			}
			if len(q.tasks) != 1 || q.tasks[0].NodeID != node.ID || q.tasks[0].RunID != e.RunID || q.tasks[0].TenantID != e.TenantID {
				t.Fatalf("unexpected queued tasks: %+v", q.tasks)
			}
			if err := res.Handle(context.Background(), e); err != nil {
				t.Fatalf("handle duplicate event: %v", err)
			}
			wantCalls := int32(0)
			if tc.wantModel != "" {
				wantCalls = 1
			}
			if calls.Load() != wantCalls || len(q.tasks) != 1 {
				t.Fatalf("model calls=%d, want %d; queued tasks=%d, want 1", calls.Load(), wantCalls, len(q.tasks))
			}
		})
	}
}

func TestNewResumer_InvalidModelConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name  string
		key   string
		value string
		want  string
	}{
		{name: "missing endpoint", key: "LLM_PROVIDER_TEST_BASE_URL", want: "BASE_URL"},
		{name: "missing credential", key: "LLM_PROVIDER_TEST_API_KEY", want: "credentials unavailable"},
		{name: "missing default model", key: "LLM_DEFAULT_MODEL", want: "LLM_DEFAULT_MODEL"},
		{name: "unknown planner provider", key: "LLM_PLANNER_MODEL", value: "missing/model", want: "LLM_PLANNER_MODEL"},
		{name: "invalid model limits", key: "LLM_MODEL_LIMITS", value: "not-json", want: "LLM_MODEL_LIMITS"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clearPlannerEnv(t)
			t.Setenv("LLM_PROVIDERS", "test")
			t.Setenv("LLM_PROVIDER_TEST_BASE_URL", "http://127.0.0.1")
			t.Setenv("LLM_PROVIDER_TEST_API_KEY", "test-key")
			t.Setenv("LLM_DEFAULT_MODEL", "test/default-model")
			t.Setenv(tc.key, tc.value)
			res, err := newResumer(nil, nil)
			if err == nil || !strings.Contains(err.Error(), tc.want) || res != nil {
				t.Fatalf("res=%v err=%v, want startup error containing %q", res, err, tc.want)
			}
		})
	}
}

func clearPlannerEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"OPENAI_BASE_URL", "OPENAI_API_KEY", "CHAT_API_KEY", "CREDENTIAL_FILE_DIR",
		"LLM_PROVIDERS", "LLM_DEFAULT_PROVIDER", "LLM_DEFAULT_MODEL", "LLM_PLANNER_MODEL",
		"LLM_FALLBACK_MODELS", "LLM_MODEL_LIMITS", "LLM_CONTEXT_SAFETY_TOKENS",
		"LLM_PROVIDER_TEST_BASE_URL", "LLM_PROVIDER_TEST_API_KEY", "TEST_API_KEY",
	} {
		t.Setenv(key, "")
	}
}

type replanStore struct {
	runtime.Store
	plan     model.Plan
	decision model.Plan
	ready    string
	inbox    bool
}

func (s *replanStore) GetRun(_ context.Context, tenant, id string) (*model.Run, error) {
	return &model.Run{ID: id, TenantID: tenant, Input: "answer the question", Status: model.RunRunning}, nil
}

func (s *replanStore) CompletedNodes(context.Context, string, string) ([]model.Node, error) {
	return []model.Node{{ID: "reflect-1", Name: "reflect", Type: model.NodeReflect, Status: model.NodeSuccess, Output: "need more evidence"}}, nil
}

func (s *replanStore) InsertPlan(_ context.Context, _, _ string, plan model.Plan) error {
	s.plan = plan
	return nil
}

func (s *replanStore) DependenciesReady(context.Context, string, string) (bool, error) {
	return true, nil
}

func (s *replanStore) MarkReady(_ context.Context, _, nodeID string) error {
	s.ready = nodeID
	return nil
}

func (s *replanStore) InboxSeen(context.Context, string, string) (bool, error) {
	return s.inbox, nil
}

func (s *replanStore) MarkInbox(context.Context, string, string) error {
	s.inbox = true
	return nil
}

func (s *replanStore) GetDecision(context.Context, string, string, string, int) (model.Plan, bool, error) {
	return s.decision, len(s.decision.Nodes) > 0, nil
}

func (s *replanStore) SaveDecision(_ context.Context, _, _, _ string, _ int, plan model.Plan) error {
	s.decision = plan
	return nil
}

type replanQueue struct{ tasks []model.Task }

func (q *replanQueue) Enqueue(_ context.Context, task model.Task) error {
	q.tasks = append(q.tasks, task)
	return nil
}
