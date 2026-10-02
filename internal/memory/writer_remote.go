// RemoteWriter 是 Writer 的 rag-api 实现：把文档经 HTTP 提交给 rag-service
// 的文档写路径（POST /v1/collections/{name}/documents），embedding 与 Qdrant
// 连接整体外移。INDEXER_BACKEND=remote 时索引器进程不再需要 embedding
// 凭证与 Qdrant 连接，部署面收敛为"MySQL + rag-api URL"。
//
// **错误语义与 RemoteMemory（读路径）刻意相反**：读路径任何故障降级为
// (nil, nil) 不拖垮主链路；这里任何失败（网络错误、非 200、解码失败）
// 都显式返回 error——索引器据此退避重试，若静默吞错，进度标记会领先于
// 实际数据，记忆静默丢失且无法察觉。rag-api 文档端点同样承诺写错误
// 返回 4xx/5xx 而非 200+degraded，两端契约对齐。
package memory

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// RemoteWriter 通过 rag-api 写入记忆文档。
//
// Client 超时由装配方提供（建议 ≥30s：批量 embed 在请求内完成，同步 ingest
// 的耗时上界是整批 embedding 网关往返）。Token 为服务间 Bearer 令牌，
// 空表示对端未开启鉴权（仅限内网/本地）。
type RemoteWriter struct {
	BaseURL string
	Token   string
	Client  *http.Client
}

var _ Writer = (*RemoteWriter)(nil)

// remoteDocMetadata 与 rag-api ingest.DocumentMetadata 契约一一对应。
// 任何一侧擅自改键名都会导致写入侧 fail-closed 校验失败或 payload 缺字段。
type remoteDocMetadata struct {
	TenantID  string    `json:"tenant_id"`
	ThreadID  string    `json:"thread_id"`
	RunID     string    `json:"run_id"`
	NodeID    string    `json:"node_id"`
	Role      string    `json:"role"`
	CreatedAt time.Time `json:"created_at"`
}

// remoteDoc 是单条提交文档。ID 用十进制字符串承载 uint64：
// rag-api 按 Qdrant 数值型 point ID 解析（ParseUint），也是 memory 集合
// 与 runtime 直连写入互通的 ID 契约。
type remoteDoc struct {
	ID       string            `json:"id"`
	Content  string            `json:"content"`
	Metadata remoteDocMetadata `json:"metadata"`
}

// remoteUpsertRequest / remoteUpsertResponse 与 rag-api 文档端点契约对齐。
type remoteUpsertRequest struct {
	Documents []remoteDoc `json:"documents"`
}

type remoteUpsertResponse struct {
	Indexed int `json:"indexed"`
	Skipped int `json:"skipped"` // content_hash 相同的幂等跳过（崩溃恢复重扫的常态）
}

// EnsureCollection 确认 rag-api 就绪。remote 模式下集合管理整体交给
// rag-api：它启动时 fail-fast 建集合并校验维度（EnsureMemoryCollection），
// 这里只需 /readyz 探活——服务不可用时索引器无法履职，早失败好过空转。
// dim 参数不参与远端建集合，仅为满足接口签名。
func (w *RemoteWriter) EnsureCollection(ctx context.Context, _ string, _ int) error {
	if w == nil || w.BaseURL == "" || w.Client == nil {
		return fmt.Errorf("memory: remote writer not configured (base_url/client required)")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		strings.TrimSuffix(w.BaseURL, "/")+"/readyz", nil)
	if err != nil {
		return err
	}
	resp, err := w.Client.Do(req)
	if err != nil {
		return fmt.Errorf("rag-api readyz: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("rag-api not ready (status %d)", resp.StatusCode)
	}
	return nil
}

// Write 经 rag-api 批量写入。幂等是双重的：确定性 point ID（本侧派生）+
// rag-api 的 content_hash 跳过（对端识别重复提交，不重复消耗 embedding 配额）。
// 因此崩溃恢复后的重扫既安全（不产生重复点）又省钱（不重复 embed）。
func (w *RemoteWriter) Write(ctx context.Context, collection string, docs []Document) error {
	if w == nil || w.BaseURL == "" || w.Client == nil {
		return fmt.Errorf("memory: remote writer not configured (base_url/client required)")
	}
	if len(docs) == 0 {
		return nil
	}
	body, err := json.Marshal(remoteUpsertRequest{Documents: toRemoteDocs(docs)})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		strings.TrimSuffix(w.BaseURL, "/")+"/v1/collections/"+collection+"/documents",
		bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if w.Token != "" {
		req.Header.Set("Authorization", "Bearer "+w.Token)
	}
	resp, err := w.Client.Do(req)
	if err != nil {
		return fmt.Errorf("rag-api upsert %d docs: %w", len(docs), err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		return fmt.Errorf("rag-api upsert %d docs: status %d: %s",
			len(docs), resp.StatusCode, strings.TrimSpace(string(snippet)))
	}
	var out remoteUpsertResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&out); err != nil {
		return fmt.Errorf("rag-api upsert: decode response: %w", err)
	}
	return nil
}

// toRemoteDocs 把内部文档模型映射为 rag-api 请求体。
func toRemoteDocs(docs []Document) []remoteDoc {
	out := make([]remoteDoc, 0, len(docs))
	for _, d := range docs {
		out = append(out, remoteDoc{
			ID:      strconv.FormatUint(d.ID, 10),
			Content: d.Text,
			Metadata: remoteDocMetadata{
				TenantID:  d.TenantID,
				ThreadID:  d.ThreadID,
				RunID:     d.RunID,
				NodeID:    d.NodeID,
				Role:      d.Role,
				CreatedAt: d.CreatedAt,
			},
		})
	}
	return out
}
