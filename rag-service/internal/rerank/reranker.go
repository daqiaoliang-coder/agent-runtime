// Package rerank 抽象 cross-encoder 重排（Phase 3）。
//
// 目标后端是 TEI（Text Embeddings Inference）的 /rerank 端点
// （bge-reranker-v2-m3 等 cross-encoder 模型）。响应解析同时兼容
// Jina/Cohere 风格（{"results":[...]} + relevance_score），部署侧
// 可任选。重排在读路径上，任何错误由 search.Service 降级为
// rerank_skipped——本包只报错，不吞错。
package rerank

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Result 是一条重排结果：Index 指向输入 texts 的下标，Score 是
// cross-encoder 相关性分（与余弦相似度、RRF 分均不可比）。
type Result struct {
	Index int
	Score float32
}

// Reranker 对候选文本按与 query 的相关性重排。
// topN <= 0 表示返回全部候选。
type Reranker interface {
	Rerank(ctx context.Context, query string, texts []string, topN int) ([]Result, error)
}

// Client 是 TEI /rerank 兼容的 HTTP 实现。
// Model 为空时不携带 model 字段（TEI 单模型部署；Jina 类网关则必填）。
type Client struct {
	BaseURL string // 如 http://tei-reranker，不带末尾斜杠（TEI 的 /rerank 不在 /v1 下）
	Model   string
	HTTP    *http.Client
}

// NewClient 创建重排客户端。timeout 应显著小于 docs profile 总预算。
func NewClient(baseURL, model string, timeout time.Duration) *Client {
	if timeout <= 0 {
		timeout = 500 * time.Millisecond
	}
	return &Client{BaseURL: baseURL, Model: model, HTTP: &http.Client{Timeout: timeout}}
}

type rerankRequest struct {
	Model  string   `json:"model,omitempty"`
	Query  string   `json:"query"`
	Texts  []string `json:"texts"`
	TopN   int      `json:"top_n,omitempty"`
}

// rawResult 兼容两种字段名：TEI 用 score，Jina/Cohere 用 relevance_score。
type rawResult struct {
	Index          int     `json:"index"`
	Score          float32 `json:"score"`
	RelevanceScore float32 `json:"relevance_score"`
}

func (r rawResult) score() float32 {
	if r.Score != 0 {
		return r.Score
	}
	return r.RelevanceScore
}

// rerankResponse 兼容两种响应形态：TEI 直接返回数组，Jina/Cohere 包在 results 里。
type rerankResponse struct {
	Results []rawResult `json:"results"`
}

func (c *Client) Rerank(ctx context.Context, query string, texts []string, topN int) ([]Result, error) {
	if len(texts) == 0 {
		return nil, nil
	}
	b, err := json.Marshal(rerankRequest{Model: c.Model, Query: query, Texts: texts, TopN: topN})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+"/rerank", bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, fmt.Errorf("rerank: http: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		rb, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("rerank: http %d: %s", resp.StatusCode, string(rb))
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<22))
	if err != nil {
		return nil, fmt.Errorf("rerank: read body: %w", err)
	}
	// 先按 TEI 的裸数组解析，失败再按 results 包裹解析。
	var raw []rawResult
	if err := json.Unmarshal(body, &raw); err != nil {
		var wrapped rerankResponse
		if err2 := json.Unmarshal(body, &wrapped); err2 != nil {
			return nil, fmt.Errorf("rerank: decode: %w", err)
		}
		raw = wrapped.Results
	}
	out := make([]Result, 0, len(raw))
	for _, r := range raw {
		if r.Index < 0 || r.Index >= len(texts) {
			return nil, fmt.Errorf("rerank: out-of-range index %d", r.Index)
		}
		out = append(out, Result{Index: r.Index, Score: r.score()})
	}
	return out, nil
}
