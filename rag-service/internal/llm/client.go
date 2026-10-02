// Package llm 提供最小的 OpenAI 兼容 chat 客户端（Phase 3）。
//
// 只服务于写路径的上下文增强（contextual retrieval）：一次调用、
// 一次回答，不需要流式/工具/多轮。构造范式与 embed.Client 一致，
// 任何 {BaseURL}/chat/completions 兼容网关均可使用。
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

// Completer 是单轮补全的最小接口。ingest.Enhancer 依赖它而非具体客户端，
// 单测可注入 fake。
type Completer interface {
	Complete(ctx context.Context, system, user string, maxTokens int) (string, error)
}

// Client 是 OpenAI Chat Completions 兼容的 HTTP 实现。
type Client struct {
	BaseURL string // 如 https://api.openai.com/v1 或本地网关，不带末尾斜杠
	APIKey  string // 空表示网关匿名可访问
	Model   string
	HTTP    *http.Client
}

// NewClient 创建 chat 客户端。timeout 是单次请求预算。
func NewClient(baseURL, apiKey, model string, timeout time.Duration) *Client {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return &Client{
		BaseURL: baseURL,
		APIKey:  apiKey,
		Model:   model,
		HTTP:    &http.Client{Timeout: timeout},
	}
}

type message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatRequest struct {
	Model       string    `json:"model"`
	Messages    []message `json:"messages"`
	MaxTokens   int       `json:"max_tokens,omitempty"`
	Temperature float64   `json:"temperature"`
}

type chatResponse struct {
	Choices []struct {
		Message message `json:"message"`
	} `json:"choices"`
}

// Complete 发起单轮补全。temperature 固定 0：增强摘要需要确定性输出，
// 同一文档重复嵌入不应因 LLM 的随机性产生不同的上下文前缀
// （那会让 content_hash 幂等失效——同内容重放会得到不同嵌入文本）。
func (c *Client) Complete(ctx context.Context, system, user string, maxTokens int) (string, error) {
	req := chatRequest{
		Model: c.Model,
		Messages: []message{
			{Role: "system", Content: system},
			{Role: "user", Content: user},
		},
		MaxTokens:   maxTokens,
		Temperature: 0,
	}
	b, err := json.Marshal(req)
	if err != nil {
		return "", err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+"/chat/completions", bytes.NewReader(b))
	if err != nil {
		return "", err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if c.APIKey != "" {
		httpReq.Header.Set("Authorization", "Bearer "+c.APIKey)
	}
	resp, err := c.HTTP.Do(httpReq)
	if err != nil {
		return "", fmt.Errorf("llm: http: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		rb, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return "", fmt.Errorf("llm: http %d: %s", resp.StatusCode, string(rb))
	}
	var cr chatResponse
	if err := json.NewDecoder(resp.Body).Decode(&cr); err != nil {
		return "", fmt.Errorf("llm: decode: %w", err)
	}
	if len(cr.Choices) == 0 {
		return "", fmt.Errorf("llm: empty choices")
	}
	return cr.Choices[0].Message.Content, nil
}
