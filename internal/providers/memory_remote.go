// 本文件实现基于远程 rag-api 的记忆提供方（Phase 1：memory profile）。
//
// 与 VectorMemory（进程内直连 Qdrant）的分工：
//   - RemoteMemory 把 embedding + 向量检索整体外移到 rag-service，
//     worker 不再需要 embedding 凭证与 Qdrant 连接，部署面更小；
//   - 两者实现相同的 MemorySearcher 契约（相同默认参数、相同降级语义），
//     因此可在装配层互换或组合（shadow 对比 / 失败回退，见 memory_shadow.go）。
package providers

import (
	"agent-runtime/internal/contracts"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
)

// remoteProfileMemory 是 rag-api 的 memory profile 标识，Phase 1 唯一取值。
const remoteProfileMemory = "memory"

// RemoteMemory 是 MemoryProvider + MemorySearcher 的远程 rag-api 实现。
//
// 它只依赖 HTTP：BaseURL 指向 rag-api，Token 为服务间 Bearer 令牌（空表示
// 对端未开启鉴权，仅限内网/本地）。检索参数（TopK/MinScore）显式随请求
// 发送而非依赖服务端缺省值——这是影子对比的前提：两路行为必须由同一份
// 配置钉死，服务端改缺省值才不会悄悄造成"本地与远程结果不一致"。
type RemoteMemory struct {
	BaseURL string
	Token   string
	// Client 由装配方提供；超时与 ContextLoader 的召回预算保持一致，
	// 作为 ctx 之外的兜底（防止绕过 ContextLoader 的调用方无限等待）。
	Client   *http.Client
	TopK     int
	MinScore float32
}

// 编译期断言：RemoteMemory 与 VectorMemory 满足同一组接口，可互换装配。
var (
	_ MemoryProvider = (*RemoteMemory)(nil)
	_ MemorySearcher = (*RemoteMemory)(nil)
)

// remoteScope 是 /v1/search 请求中的隔离作用域，与 rag-api 契约对齐。
type remoteScope struct {
	TenantID string `json:"tenant_id"`
	ThreadID string `json:"thread_id"`
}

// remoteSearchRequest 是 /v1/search 的请求体。字段名与 rag-api httpapi 契约一致。
type remoteSearchRequest struct {
	Query        string      `json:"query"`
	Profile      string      `json:"profile"`
	Scope        remoteScope `json:"scope"`
	ExcludeRunID string      `json:"exclude_run_id"`
	TopK         int         `json:"top_k"`
	MinScore     *float32    `json:"min_score"` // 显式指针：nil 会让服务端用自己的缺省阈值
}

// remoteResult 是一条召回结果，含溯源信息（node_id/run_id/score/created_at）。
type remoteResult struct {
	Role      string    `json:"role"`
	Content   string    `json:"content"`
	NodeID    string    `json:"node_id"`
	RunID     string    `json:"run_id"`
	Score     float32   `json:"score"`
	CreatedAt time.Time `json:"created_at"`
}

// remoteSearchResponse 是 /v1/search 的响应体。Degraded 非空表示对端内部
// 降级（embed/vector 故障），结果不可信；Timings 供延迟预算对账。
type remoteSearchResponse struct {
	Results []remoteResult `json:"results"`
	Timings struct {
		EmbedMS  float64 `json:"embed_ms"`
		SearchMS float64 `json:"search_ms"`
		TotalMS  float64 `json:"total_ms"`
	} `json:"timings"`
	Degraded []string `json:"degraded"`
}

// messages 把 rag-api 响应映射为对话消息：跳过空内容、角色归一化。
// 排序（created_at, node_id 正序）与阈值过滤已在服务端完成且与本地实现等价，
// 客户端保持响应顺序即可。
func (resp remoteSearchResponse) messages() []contracts.Message {
	if len(resp.Results) == 0 {
		return nil
	}
	msgs := make([]contracts.Message, 0, len(resp.Results))
	for _, r := range resp.Results {
		if r.Content == "" {
			continue
		}
		msgs = append(msgs, contracts.Message{Role: roleOrUser(r.Role), Content: r.Content})
	}
	return msgs
}

// Search 经 rag-api 召回历史记忆。
//
// **降级契约**与 VectorMemory 一致：任何错误（网络故障、非 200、解码失败）
// 或对端降级（Degraded 非空）都转成 (nil, nil) + warn 日志，绝不向上抛。
// rag-api 自身也承诺"内部故障恒 200 + degraded 标记"，因此这里的错误
// 分支主要覆盖传输层与请求侧（400/401）故障。
func (m *RemoteMemory) Search(ctx context.Context, ec contracts.ExecutionContext, query string, topK int) ([]contracts.Message, error) {
	resp, err := m.searchDetailed(ctx, ec, query, topK)
	if err != nil {
		m.degrade("rag-api request", err, ec)
		return nil, nil
	}
	if len(resp.Degraded) > 0 {
		m.degrade("rag-api degraded", fmt.Errorf("reasons=%s", strings.Join(resp.Degraded, ",")), ec)
		return nil, nil
	}
	return resp.messages(), nil
}

// searchDetailed 发起检索并返回原始响应，供同包组合实现（ShadowMemory /
// FallbackMemory）区分"合法空结果"与"远程降级"——MemorySearcher 契约把
// 二者都折叠成 (nil, nil)，而对比与回退决策需要更细的信号。
func (m *RemoteMemory) searchDetailed(ctx context.Context, ec contracts.ExecutionContext, query string, topK int) (remoteSearchResponse, error) {
	if m == nil || m.BaseURL == "" || m.Client == nil {
		return remoteSearchResponse{}, fmt.Errorf("remote memory not configured (base_url or client missing)")
	}
	// 空查询无法向量化、空租户无权召回：等价 VectorMemory 的入参检查，
	// 且不必发起 HTTP 请求（返回"合法空"而非降级）。
	if query == "" || ec.TenantID == "" {
		return remoteSearchResponse{}, nil
	}
	if topK <= 0 {
		topK = m.TopK
	}
	if topK <= 0 {
		topK = DefaultMemoryTopK
	}
	// 显式钉死 min_score（含 0=不过滤）：服务端对 nil 用自己的缺省值（0.7），
	// 不显式传递会让"本地不过滤、远程按 0.7 过滤"这类配置漂移躲过影子对比。
	minScore := m.MinScore
	if minScore < 0 {
		minScore = 0
	}
	body, err := json.Marshal(remoteSearchRequest{
		Query:        query,
		Profile:      remoteProfileMemory,
		Scope:        remoteScope{TenantID: ec.TenantID, ThreadID: ec.ThreadID},
		ExcludeRunID: ec.RunID, // 服务端据此源头排除当前 Run，防上下文重复注入
		TopK:         topK,
		MinScore:     &minScore,
	})
	if err != nil {
		return remoteSearchResponse{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		strings.TrimSuffix(m.BaseURL, "/")+"/v1/search", bytes.NewReader(body))
	if err != nil {
		return remoteSearchResponse{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	if m.Token != "" {
		req.Header.Set("Authorization", "Bearer "+m.Token)
	}
	httpResp, err := m.Client.Do(req)
	if err != nil {
		return remoteSearchResponse{}, err
	}
	defer httpResp.Body.Close()
	if httpResp.StatusCode != http.StatusOK {
		snippet, _ := io.ReadAll(io.LimitReader(httpResp.Body, 4<<10))
		return remoteSearchResponse{}, fmt.Errorf("rag-api status %d: %s",
			httpResp.StatusCode, strings.TrimSpace(string(snippet)))
	}
	var out remoteSearchResponse
	if err := json.NewDecoder(io.LimitReader(httpResp.Body, 4<<20)).Decode(&out); err != nil {
		return remoteSearchResponse{}, err
	}
	return out, nil
}

// Load 返回空记忆：语义召回必须带查询文本，Load 签名无法表达，
// 与 VectorMemory.Load 的取舍一致（详见 docs/architecture-v3.md 对 port 的要求）。
func (m *RemoteMemory) Load(context.Context, contracts.ExecutionContext) ([]contracts.Message, error) {
	return nil, nil
}

// Save 返回明确错误：RemoteMemory 是只读视图，记忆写入由
// cmd/memory-indexer 投影（幂等、可全量重建），静默 no-op 会掩盖接线错误。
func (m *RemoteMemory) Save(context.Context, contracts.ExecutionContext, []contracts.Message) error {
	return fmt.Errorf("providers: RemoteMemory is read-only; memory writes are projected by cmd/memory-indexer from committed nodes")
}

// degrade 统一记录降级：只打 warn，不中断执行链路（与 VectorMemory.degrade 对齐）。
func (m *RemoteMemory) degrade(stage string, err error, ec contracts.ExecutionContext) {
	log.Printf("memory(remote): degraded (%s) tenant=%s thread=%s run=%s: %v",
		stage, ec.TenantID, ec.ThreadID, ec.RunID, err)
}
