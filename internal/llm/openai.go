package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
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
}

// Complete 向 {BaseURL}/chat/completions 发起请求，解析首个 choice 的文本。
func (c *OpenAIClient) Complete(ctx context.Context, req Request) (Response, error) {
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
	return Response{Content: cr.Choices[0].Message.Content}, nil
}
