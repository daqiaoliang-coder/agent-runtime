package llm

import (
	"agent-runtime/internal/contracts"
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
//
// 密钥有两条互斥来源，Credentials 优先：
//   - Credentials 非 nil：每次请求前经 CredentialProvider 取凭证，密钥生命周期由托管层负责，
//     轮转对本客户端透明，明文不落在结构体字段上；
//   - Credentials 为 nil：回退读取 APIKey 字段（兼容既有部署与测试）。
//
// 保留 APIKey 字段而不是直接删掉，是为了避免一次 flag day 迁移：
// 既有调用方与测试全部按 NewOpenAIClient(base, key) 构造，强行改签名会波及全仓库。
// 新代码应走 NewOpenAIClientWithCredentials。
type OpenAIClient struct {
	BaseURL string // 如 https://api.openai.com/v1 ，不要带末尾斜杠
	APIKey  string // 兼容路径；Credentials 非 nil 时被忽略
	HTTP    *http.Client
	// Credentials 为密钥托管 port；非 nil 时取代 APIKey 成为鉴权来源。
	Credentials contracts.CredentialProvider
	// Purpose 指定凭证用途，空值按 CredentialPurposeChat 处理。
	// 分用途是为了让托管层能按最小权限签发（推理 Key 不该兼作向量化 Key）。
	Purpose contracts.CredentialPurpose
}

// NewOpenAIClient 创建默认超时 60s 的客户端，密钥取自 apiKey 明文。
func NewOpenAIClient(baseURL, apiKey string) *OpenAIClient {
	return &OpenAIClient{BaseURL: baseURL, APIKey: apiKey, HTTP: &http.Client{Timeout: 60 * time.Second}}
}

// NewOpenAIClientWithCredentials 创建走密钥托管的客户端，密钥不进入结构体字段。
// creds 为 nil 时等价于 NewOpenAIClient(baseURL, "")，即未配置密钥。
func NewOpenAIClientWithCredentials(baseURL string, creds contracts.CredentialProvider, purpose contracts.CredentialPurpose) *OpenAIClient {
	if purpose == "" {
		purpose = contracts.CredentialPurposeChat
	}
	return &OpenAIClient{BaseURL: baseURL, HTTP: &http.Client{Timeout: 60 * time.Second}, Credentials: creds, Purpose: purpose}
}

// resolveAuth 统一两条密钥来源，返回可直接写入 Authorization 头的值。
//
// 未配置密钥时返回明确错误而非发起匿名请求：匿名请求会得到网关的 401，
// 排查方向被误导到"Key 无效"而不是"Key 没配"，这是两类完全不同的故障。
func (c *OpenAIClient) resolveAuth(ctx context.Context) (string, error) {
	return resolveAuthorization(ctx, c.Credentials, c.Purpose, contracts.CredentialPurposeChat, c.APIKey)
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
	// 密钥未配置（既无托管层也无明文）时立即失败，不发起匿名请求。
	auth, err := c.resolveAuth(ctx)
	if err != nil {
		return Response{}, err
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
	httpReq.Header.Set("Authorization", auth)
	resp, err := c.HTTP.Do(httpReq)
	if err != nil {
		return Response{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		rb, _ := io.ReadAll(resp.Body)
		// 401/403 极可能是凭证已被轮转或吊销。主动失效缓存，让下一次调用回源取新凭证，
		// 否则缓存会一直复用到自然过期，故障恢复时间被拉长到分钟级。
		if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
			invalidateCredential(c.Credentials, c.Purpose)
		}
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
