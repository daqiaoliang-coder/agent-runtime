package llm

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestStubEmbedder_Deterministic 相同文本必须得到相同向量，不同文本得到不同向量。
// 确定性是"重放不产生语义漂移"的前提，索引器重试依赖这一点。
func TestStubEmbedder_Deterministic(t *testing.T) {
	e := StubEmbedder{Dim: 8}
	a1, err := e.Embed(context.Background(), []string{"hello"})
	if err != nil {
		t.Fatalf("embed: %v", err)
	}
	a2, _ := e.Embed(context.Background(), []string{"hello"})
	b, _ := e.Embed(context.Background(), []string{"world"})
	if len(a1) != 1 || len(a1[0]) != 8 {
		t.Fatalf("unexpected shape: %v", a1)
	}
	for i := range a1[0] {
		if a1[0][i] != a2[0][i] {
			t.Errorf("same text produced different vectors at %d", i)
		}
	}
	same := true
	for i := range a1[0] {
		if a1[0][i] != b[0][i] {
			same = false
			break
		}
	}
	if same {
		t.Error("different texts produced identical vectors")
	}
}

// TestStubEmbedder_Normalized 伪向量必须已归一化，否则余弦相似度比较失真。
func TestStubEmbedder_Normalized(t *testing.T) {
	v, _ := StubEmbedder{Dim: 16}.Embed(context.Background(), []string{"normalize me"})
	var sum float64
	for _, x := range v[0] {
		sum += float64(x) * float64(x)
	}
	if sum < 0.99 || sum > 1.01 {
		t.Errorf("expected unit norm, got %f", sum)
	}
}

// TestStubEmbedder_Empty 空入参应返回空结果而非报错，避免调用方额外判空。
func TestStubEmbedder_Empty(t *testing.T) {
	out, err := StubEmbedder{}.Embed(context.Background(), nil)
	if err != nil || len(out) != 0 {
		t.Fatalf("expected empty result, got %v err=%v", out, err)
	}
}

// TestOpenAIEmbedder_ReordersByIndex 网关乱序返回时必须按 index 归位，
// 否则 texts[i] 与 vectors[i] 错配会导致召回内容与查询无关（静默错误，极难排查）。
func TestOpenAIEmbedder_ReordersByIndex(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// 刻意反序返回：index 1 在前，index 0 在后。
		_ = json.NewEncoder(w).Encode(map[string]any{
			"data": []map[string]any{
				{"index": 1, "embedding": []float64{0.9, 0.1}},
				{"index": 0, "embedding": []float64{0.1, 0.9}},
			},
			"usage": map[string]any{"prompt_tokens": 4, "total_tokens": 4},
		})
	}))
	defer srv.Close()

	e := NewOpenAIEmbedder(srv.URL, "test-key", "text-embedding-3-small")
	out, err := e.Embed(context.Background(), []string{"first", "second"})
	if err != nil {
		t.Fatalf("embed: %v", err)
	}
	if out[0][0] != 0.1 || out[0][1] != 0.9 {
		t.Errorf("vector for index 0 not placed correctly: %v", out[0])
	}
	if out[1][0] != 0.9 || out[1][1] != 0.1 {
		t.Errorf("vector for index 1 not placed correctly: %v", out[1])
	}
}

// TestOpenAIEmbedder_SendsAuthAndModel 必须携带 Bearer 鉴权头与配置的模型名。
func TestOpenAIEmbedder_SendsAuthAndModel(t *testing.T) {
	var gotAuth, gotModel string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		var body embeddingRequest
		_ = json.NewDecoder(r.Body).Decode(&body)
		gotModel = body.Model
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"data": []map[string]any{{"index": 0, "embedding": []float64{1, 0}}},
		})
	}))
	defer srv.Close()

	e := NewOpenAIEmbedder(srv.URL, "sk-secret", "custom-model")
	if _, err := e.Embed(context.Background(), []string{"x"}); err != nil {
		t.Fatalf("embed: %v", err)
	}
	if gotAuth != "Bearer sk-secret" {
		t.Errorf("unexpected auth header: %q", gotAuth)
	}
	if gotModel != "custom-model" {
		t.Errorf("unexpected model: %q", gotModel)
	}
}

// TestOpenAIEmbedder_MissingKey API key 未配置应明确报错，而非发起匿名请求。
func TestOpenAIEmbedder_MissingKey(t *testing.T) {
	e := NewOpenAIEmbedder("http://127.0.0.1:1", "", "m")
	if _, err := e.Embed(context.Background(), []string{"x"}); err == nil {
		t.Fatal("expected error when api key is empty")
	}
}

// TestOpenAIEmbedder_CountMismatch 返回向量数与入参文本数不一致必须报错。
// 静默接受会导致后续 Index 越界或错配。
func TestOpenAIEmbedder_CountMismatch(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"data": []map[string]any{{"index": 0, "embedding": []float64{1, 0}}},
		})
	}))
	defer srv.Close()

	e := NewOpenAIEmbedder(srv.URL, "k", "m")
	if _, err := e.Embed(context.Background(), []string{"a", "b"}); err == nil {
		t.Fatal("expected error on vector count mismatch")
	}
}

// TestOpenAIEmbedder_HTTPError 4xx/5xx 必须转成 error 并带上响应体，便于定位网关问题。
func TestOpenAIEmbedder_HTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error":"rate limited"}`, http.StatusTooManyRequests)
	}))
	defer srv.Close()

	e := NewOpenAIEmbedder(srv.URL, "k", "m")
	_, err := e.Embed(context.Background(), []string{"a"})
	if err == nil {
		t.Fatal("expected error on 429")
	}
	if got := err.Error(); got == "" {
		t.Error("expected non-empty error message")
	}
}

// TestOpenAIEmbedder_Empty 空入参不应发起任何 HTTP 请求（省一次网关调用）。
func TestOpenAIEmbedder_Empty(t *testing.T) {
	called := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	}))
	defer srv.Close()

	e := NewOpenAIEmbedder(srv.URL, "k", "m")
	out, err := e.Embed(context.Background(), nil)
	if err != nil || len(out) != 0 {
		t.Fatalf("expected empty result, got %v err=%v", out, err)
	}
	if called {
		t.Error("expected no HTTP call for empty input")
	}
}
