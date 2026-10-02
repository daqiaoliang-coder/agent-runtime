package rerank

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestClient_TEIArrayResponse(t *testing.T) {
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/rerank" {
			t.Errorf("unexpected path %q", r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Errorf("decode: %v", err)
		}
		// TEI 形态：裸数组 + score 字段。
		_, _ = w.Write([]byte(`[{"index":2,"score":0.97},{"index":0,"score":0.31}]`))
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "", 500*time.Millisecond)
	got, err := c.Rerank(context.Background(), "q", []string{"a", "b", "c"}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Index != 2 || got[0].Score != 0.97 || got[1].Index != 0 {
		t.Fatalf("unexpected results: %+v", got)
	}
	// model 为空时不携带 model 字段（TEI 单模型部署）。
	if _, ok := gotBody["model"]; ok {
		t.Fatalf("expected no model field, got %v", gotBody["model"])
	}
	if gotBody["top_n"] != float64(2) {
		t.Fatalf("expected top_n=2, got %v", gotBody["top_n"])
	}
}

func TestClient_JinaWrappedResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body["model"] != "bge-reranker" {
			t.Errorf("expected model field, got %v", body["model"])
		}
		// Jina/Cohere 形态：results 包裹 + relevance_score 字段。
		_, _ = w.Write([]byte(`{"results":[{"index":1,"relevance_score":0.88}]}`))
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "bge-reranker", 500*time.Millisecond)
	got, err := c.Rerank(context.Background(), "q", []string{"a", "b"}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Index != 1 || got[0].Score != 0.88 {
		t.Fatalf("unexpected results: %+v", got)
	}
}

func TestClient_ErrorPropagates(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	c := NewClient(srv.URL, "", time.Second)
	if _, err := c.Rerank(context.Background(), "q", []string{"a"}, 1); err == nil {
		t.Fatal("expected error on 503")
	}
}

func TestClient_EmptyTexts(t *testing.T) {
	c := NewClient("http://127.0.0.1:1", "", time.Millisecond)
	got, err := c.Rerank(context.Background(), "q", nil, 5)
	if err != nil || got != nil {
		t.Fatalf("expected (nil,nil), got (%v,%v)", got, err)
	}
}

func TestClient_OutOfRangeIndexRejected(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`[{"index":5,"score":0.9}]`))
	}))
	defer srv.Close()
	c := NewClient(srv.URL, "", time.Second)
	if _, err := c.Rerank(context.Background(), "q", []string{"a"}, 1); err == nil {
		t.Fatal("expected error for out-of-range index")
	}
}
