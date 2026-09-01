package llm

import (
	"agent-runtime/internal/trace"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

// OpenAIClient 是 OpenAI Chat Completions 兼容的 HTTP 实现。
// 任何兼容 /v1/chat/completions 的网关（OpenAI、Azure OpenAI、vLLM、本地 ollama 等）均可使用。
// 这是一个"真实"实现：生产中可替换 Stub，让节点执行真正发起 LLM 推理。
type OpenAIClient struct {
	BaseURL string // 如 https://api.openai.com/v1 ，不要带末尾斜杠
	APIKey  string
	HTTP    *http.Client
}

// NewOpenAIClient 创建默认超时 60s 的客户端。
func NewOpenAIClient(baseURL, apiKey string) *OpenAIClient {
	return &OpenAIClient{BaseURL: baseURL, APIKey: apiKey, HTTP: &http.Client{Timeout: 60 * time.Second}}
}

type chatRequest struct {
	Model    string        `json:"model"`
	Messages []chatMessage `json:"messages"`
}
type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}
type chatResponse struct {
	Choices []struct {
		Message chatMessage `json:"message"`
	} `json:"choices"`
	// Model 为实际使用的模型名（可能与请求不同，如自动降级）。
	Model string `json:"model"`
	// Usage 携带本次调用的 token 计数，用于 cost tracking。
	Usage struct {
		PromptTokens     int `json:"prompt_tokens"`
		CompletionTokens int `json:"completion_tokens"`
		TotalTokens      int `json:"total_tokens"`
	} `json:"usage"`
}

// Complete 向 {BaseURL}/chat/completions 发起请求，解析首个 choice 的文本。
// 创建 llm.complete span 标记对 LLM provider 的 HTTP 调用，串联到 executor.llm 子 span。
func (c *OpenAIClient) Complete(ctx context.Context, req Request) (_ Response, err error) {
	ctx, span := trace.StartSpan(ctx, "llm.complete")
	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}()
	span.SetAttributes(
		attribute.String("llm.base_url", c.BaseURL),
		attribute.String("llm.request_model", req.Model),
		attribute.Int("llm.message_count", len(req.Messages)),
	)
	if c.APIKey == "" {
		return Response{}, fmt.Errorf("llm: openai api key not configured")
	}
	body := chatRequest{Model: req.Model, Messages: make([]chatMessage, len(req.Messages))}
	for i, m := range req.Messages {
		body.Messages[i] = chatMessage{Role: string(m.Role), Content: m.Content}
	}
	b, err := json.Marshal(body)
	if err != nil {
		return Response{}, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+"/chat/completions", bytes.NewReader(b))
	if err != nil {
		return Response{}, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+c.APIKey)
	resp, err := c.HTTP.Do(httpReq)
	if err != nil {
		return Response{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		rb, _ := io.ReadAll(resp.Body)
		return Response{}, fmt.Errorf("llm: http %d: %s", resp.StatusCode, string(rb))
	}
	var cr chatResponse
	if err := json.NewDecoder(resp.Body).Decode(&cr); err != nil {
		return Response{}, err
	}
	if len(cr.Choices) == 0 {
		return Response{}, fmt.Errorf("llm: empty choices")
	}
	return Response{
		Content: cr.Choices[0].Message.Content,
		Model:   cr.Model,
		Usage: Usage{
			PromptTokens:     cr.Usage.PromptTokens,
			CompletionTokens: cr.Usage.CompletionTokens,
			TotalTokens:      cr.Usage.TotalTokens,
		},
	}, nil
}
