// Package embed 抽象文本向量化，协议与 runtime 的 llm.OpenAIEmbedder 完全一致
// （OpenAI /embeddings 兼容网关）。查询侧与索引侧必须使用同一模型，
// 否则两个向量空间不可比，余弦相似度失去意义——这是等价性验收的隐含前提。
package embed

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Embedder 把一批文本转为向量。实现需尊重 ctx 的超时与取消。
type Embedder interface {
	Embed(ctx context.Context, texts []string) ([][]float32, error)
}

// Client 是 OpenAI Embeddings 兼容的 HTTP 实现。
// 任何兼容 {BaseURL}/embeddings 的网关（OpenAI、vLLM、TEI、本地 ollama）均可使用。
type Client struct {
	BaseURL string // 如 http://localhost:11434/v1 ，不要带末尾斜杠
	APIKey  string // 空表示网关匿名可访问（本地网关常见）
	Model   string // 如 bge-m3
	HTTP    *http.Client
}

// NewClient 创建向量化客户端。timeout 是单次 HTTP 请求预算，
// 应显著小于 search.Service 的整体 Budget，给向量检索留出余量。
func NewClient(baseURL, apiKey, model string, timeout time.Duration) *Client {
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	return &Client{
		BaseURL: baseURL,
		APIKey:  apiKey,
		Model:   model,
		HTTP:    &http.Client{Timeout: timeout},
	}
}

type embeddingRequest struct {
	Model string   `json:"model"`
	Input []string `json:"input"`
}

type embeddingResponse struct {
	Data []struct {
		// Index 用于把响应按请求顺序归位：部分网关不保证返回顺序与入参一致，
		// 只按 Index 取值才能确保 texts[i] 与 vectors[i] 对应。
		Index     int       `json:"index"`
		Embedding []float32 `json:"embedding"`
	} `json:"data"`
}

// Embed 发起批量请求，返回与入参等长、顺序一致的向量切片。
// 错误向上返回（是否降级由 search.Service 决定，与 runtime 分层一致）。
func (c *Client) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	if len(texts) == 0 {
		return nil, nil
	}
	b, err := json.Marshal(embeddingRequest{Model: c.Model, Input: texts})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+"/embeddings", bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.APIKey)
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, fmt.Errorf("embed: http: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		rb, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("embed: http %d: %s", resp.StatusCode, string(rb))
	}
	var er embeddingResponse
	if err := json.NewDecoder(resp.Body).Decode(&er); err != nil {
		return nil, fmt.Errorf("embed: decode: %w", err)
	}
	if len(er.Data) != len(texts) {
		return nil, fmt.Errorf("embed: returned %d vectors for %d texts", len(er.Data), len(texts))
	}
	// 按 Index 归位而非按数组顺序，避免网关乱序返回导致文本与向量错配。
	out := make([][]float32, len(texts))
	for _, d := range er.Data {
		if d.Index < 0 || d.Index >= len(out) {
			return nil, fmt.Errorf("embed: out-of-range index %d", d.Index)
		}
		if len(d.Embedding) == 0 {
			return nil, fmt.Errorf("embed: empty vector at index %d", d.Index)
		}
		out[d.Index] = d.Embedding
	}
	for i, v := range out {
		if v == nil {
			return nil, fmt.Errorf("embed: missing vector for index %d", i)
		}
	}
	return out, nil
}
