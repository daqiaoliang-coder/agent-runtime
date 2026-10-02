package llm

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestComplete_RequestShape 断言请求体契约：model/messages 透传、
// temperature 固定 0（增强摘要的确定性前提，见 client.go 注释）。
func TestComplete_RequestShape(t *testing.T) {
	var gotPath string
	var gotAuth string
	var gotBody chatRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Errorf("decode request: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":"ok"}}]}`))
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "sk-test", "gpt-test", time.Second)
	out, err := c.Complete(context.Background(), "sys prompt", "user prompt", 128)
	if err != nil {
		t.Fatalf("complete: %v", err)
	}
	if out != "ok" {
		t.Fatalf("want %q, got %q", "ok", out)
	}
	if gotPath != "/chat/completions" {
		t.Fatalf("want /chat/completions, got %s", gotPath)
	}
	if gotAuth != "Bearer sk-test" {
		t.Fatalf("want Bearer auth, got %q", gotAuth)
	}
	if gotBody.Model != "gpt-test" {
		t.Fatalf("model not passed through: %q", gotBody.Model)
	}
	if len(gotBody.Messages) != 2 {
		t.Fatalf("want 2 messages, got %d", len(gotBody.Messages))
	}
	if gotBody.Messages[0].Role != "system" || gotBody.Messages[0].Content != "sys prompt" {
		t.Fatalf("system message wrong: %+v", gotBody.Messages[0])
	}
	if gotBody.Messages[1].Role != "user" || gotBody.Messages[1].Content != "user prompt" {
		t.Fatalf("user message wrong: %+v", gotBody.Messages[1])
	}
	if gotBody.MaxTokens != 128 {
		t.Fatalf("max_tokens not passed: %d", gotBody.MaxTokens)
	}
	if gotBody.Temperature != 0 {
		t.Fatalf("temperature must be fixed 0, got %v", gotBody.Temperature)
	}
}

// TestComplete_NoAPIKeyOmitsAuth：空 key 时不得携带 Authorization 头
// （本地网关匿名访问，与 embed.Client 行为一致）。
func TestComplete_NoAPIKeyOmitsAuth(t *testing.T) {
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"x"}}]}`))
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "", "m", time.Second)
	if _, err := c.Complete(context.Background(), "s", "u", 8); err != nil {
		t.Fatalf("complete: %v", err)
	}
	if gotAuth != "" {
		t.Fatalf("anonymous gateway must not carry auth, got %q", gotAuth)
	}
}

func TestComplete_Errors(t *testing.T) {
	// 非 200 → 错误。
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	if _, err := NewClient(srv.URL, "", "m", time.Second).Complete(context.Background(), "s", "u", 8); err == nil {
		t.Fatal("http 503 must be an error")
	}
	srv.Close()

	// 空 choices → 错误。
	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"choices":[]}`))
	}))
	defer srv2.Close()
	if _, err := NewClient(srv2.URL, "", "m", time.Second).Complete(context.Background(), "s", "u", 8); err == nil {
		t.Fatal("empty choices must be an error")
	}

	// 非法 JSON → 错误。
	srv3 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{not json`))
	}))
	defer srv3.Close()
	if _, err := NewClient(srv3.URL, "", "m", time.Second).Complete(context.Background(), "s", "u", 8); err == nil {
		t.Fatal("invalid json must be an error")
	}
}

// TestComplete_RespectsContextCancel：ctx 取消必须及时返回，不等待 HTTP 超时。
func TestComplete_RespectsContextCancel(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release
	}))
	defer srv.Close()
	defer close(release)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c := NewClient(srv.URL, "", "m", 10*time.Second)
	if _, err := c.Complete(ctx, "s", "u", 8); !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
}
