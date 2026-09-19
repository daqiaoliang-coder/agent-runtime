// Embedder 抽象文本向量化能力，与 Client（对话补全）刻意分离。
//
// 为什么不扩展 Client 接口：Client 已有 Stub / Echo 两个实现和大量测试 mock，
// 加入 Embed 会强制所有实现都具备向量能力，破坏面过大。记忆检索属于可选增强，
// 因此以独立接口表达——未注入 Embedder 时记忆能力整体关闭，主链路零感知。
package llm

import (
	"agent-runtime/internal/contracts"
	"agent-runtime/internal/trace"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

// Embedder 把一批文本转为向量。实现需尊重 ctx 的超时与取消。
// 批量入参是为了摊薄网关调用：索引器一次 embed 多条记忆，而非逐条请求。
type Embedder interface {
	Embed(ctx context.Context, texts []string) ([][]float32, error)
}

// OpenAIEmbedder 是 OpenAI Embeddings 兼容的 HTTP 实现。
// 任何兼容 {BaseURL}/embeddings 的网关（OpenAI、Azure OpenAI、vLLM、本地 ollama 等）均可使用。
// 与 OpenAIClient 共用 BaseURL 配置，因为记忆向量化与对话推理通常走同一网关。
//
// 密钥来源与 OpenAIClient 同构：Credentials 非 nil 时优先走托管层，
// 否则回退 APIKey 明文。向量化刻意使用独立的 CredentialPurposeEmbedding，
// 使托管层可以只签发 embedding 额度的低权限凭证。
type OpenAIEmbedder struct {
	BaseURL string // 如 https://api.openai.com/v1 ，不要带末尾斜杠
	APIKey  string // 兼容路径；Credentials 非 nil 时被忽略
	Model   string // 如 text-embedding-3-small
	HTTP    *http.Client
	// Credentials 为密钥托管 port；非 nil 时取代 APIKey 成为鉴权来源。
	Credentials contracts.CredentialProvider
	// Purpose 指定凭证用途，空值按 CredentialPurposeEmbedding 处理。
	Purpose contracts.CredentialPurpose
}

// NewOpenAIEmbedder 创建默认超时 30s 的向量化客户端，密钥取自 apiKey 明文。
// 超时预算刻意短于 OpenAIClient 的 60s：向量化是记忆的旁路增强，
// 不应像推理那样长时间占用调用方（读路径总预算仅 800ms，见 worker.ContextLoader）。
func NewOpenAIEmbedder(baseURL, apiKey, model string) *OpenAIEmbedder {
	return &OpenAIEmbedder{
		BaseURL: baseURL,
		APIKey:  apiKey,
		Model:   model,
		HTTP:    &http.Client{Timeout: 30 * time.Second},
	}
}

// NewOpenAIEmbedderWithCredentials 创建走密钥托管的向量化客户端，密钥不进入结构体字段。
func NewOpenAIEmbedderWithCredentials(baseURL, model string, creds contracts.CredentialProvider, purpose contracts.CredentialPurpose) *OpenAIEmbedder {
	if purpose == "" {
		purpose = contracts.CredentialPurposeEmbedding
	}
	return &OpenAIEmbedder{
		BaseURL:     baseURL,
		Model:       model,
		HTTP:        &http.Client{Timeout: 30 * time.Second},
		Credentials: creds,
		Purpose:     purpose,
	}
}

// resolveAuth 统一两条密钥来源，返回可直接写入 Authorization 头的值。
func (e *OpenAIEmbedder) resolveAuth(ctx context.Context) (string, error) {
	return resolveAuthorization(ctx, e.Credentials, e.Purpose, contracts.CredentialPurposeEmbedding, e.APIKey)
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
	Usage struct {
		PromptTokens int `json:"prompt_tokens"`
		TotalTokens  int `json:"total_tokens"`
	} `json:"usage"`
}

// Embed 向 {BaseURL}/embeddings 发起批量请求，返回与入参等长、顺序一致的向量切片。
// 创建 llm.embed span 标记对 embedding 网关的 HTTP 调用。
func (e *OpenAIEmbedder) Embed(ctx context.Context, texts []string) (_ [][]float32, err error) {
	if len(texts) == 0 {
		return nil, nil
	}
	ctx, span := trace.StartSpan(ctx, "llm.embed")
	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}()
	span.SetAttributes(
		attribute.String("llm.base_url", e.BaseURL),
		attribute.String("llm.embedding_model", e.Model),
		attribute.Int("llm.text_count", len(texts)),
	)
	// 密钥未配置时立即失败，不发起匿名请求（匿名请求只会换来网关 401，误导排查方向）。
	auth, err := e.resolveAuth(ctx)
	if err != nil {
		return nil, err
	}
	b, err := json.Marshal(embeddingRequest{Model: e.Model, Input: texts})
	if err != nil {
		return nil, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, e.BaseURL+"/embeddings", bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", auth)
	resp, err := e.HTTP.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		rb, _ := io.ReadAll(resp.Body)
		// 凭证被拒时主动失效托管层缓存，让下次调用回源取新凭证。
		if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
			invalidateCredential(e.Credentials, e.Purpose)
		}
		return nil, fmt.Errorf("llm: embed http %d: %s", resp.StatusCode, string(rb))
	}
	var er embeddingResponse
	if err := json.NewDecoder(resp.Body).Decode(&er); err != nil {
		return nil, err
	}
	if len(er.Data) != len(texts) {
		return nil, fmt.Errorf("llm: embed returned %d vectors for %d texts", len(er.Data), len(texts))
	}
	// 按 Index 归位而非按数组顺序，避免网关乱序返回导致文本与向量错配。
	out := make([][]float32, len(texts))
	for _, d := range er.Data {
		if d.Index < 0 || d.Index >= len(out) {
			return nil, fmt.Errorf("llm: embed response has out-of-range index %d", d.Index)
		}
		if len(d.Embedding) == 0 {
			return nil, fmt.Errorf("llm: embed returned empty vector at index %d", d.Index)
		}
		out[d.Index] = d.Embedding
	}
	for i, v := range out {
		if v == nil {
			return nil, fmt.Errorf("llm: embed response missing vector for index %d", i)
		}
	}
	span.SetAttributes(
		attribute.Int("llm.embedding_dim", len(out[0])),
		attribute.Int("llm.prompt_tokens", er.Usage.PromptTokens),
	)
	return out, nil
}

// StubEmbedder 是确定性伪向量化实现，用于单元测试与本地演示：
// 相同文本永远得到相同向量，不同文本得到不同向量，且已做归一化（可直接算余弦相似度）。
//
// 它不是语义向量——"猫"和"猫咪"不会靠近——因此只能验证写入/召回/隔离/降级等
// 工程链路的正确性，不能用于评估检索质量。检索质量需接真实 embedding 模型验证。
type StubEmbedder struct {
	Dim int // 向量维度，<=0 时取 8
}

// Embed 由文本的 SHA-256 摘要派生伪向量并归一化。
func (s StubEmbedder) Embed(_ context.Context, texts []string) ([][]float32, error) {
	dim := s.Dim
	if dim <= 0 {
		dim = 8
	}
	out := make([][]float32, 0, len(texts))
	for _, t := range texts {
		out = append(out, hashVector(t, dim))
	}
	return out, nil
}

// hashVector 用 SHA-256 的前 dim*4 字节生成确定性伪向量并做 L2 归一化。
// 归一化后余弦相似度退化为点积，便于 fake VectorStore 直接比较。
func hashVector(text string, dim int) []float32 {
	h := sha256.Sum256([]byte(text))
	v := make([]float32, dim)
	var sum float64
	for i := 0; i < dim; i++ {
		off := (i * 4) % len(h)
		// 取 4 字节转 uint32 再映射到 [-1,1)，保证同一文本结果稳定。
		n := binary.BigEndian.Uint32(h[off : off+4])
		f := float64(n)/math.MaxUint32*2 - 1
		v[i] = float32(f)
		sum += float64(f) * float64(f)
	}
	if sum == 0 {
		return v
	}
	norm := float32(math.Sqrt(sum))
	for i := range v {
		v[i] /= norm
	}
	return v
}
