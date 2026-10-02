// Package httpapi 暴露 rag-api 的 HTTP 接口。
//
// Phase 2 的端点：
//   - GET    /healthz                              存活探针（不依赖后端）
//   - GET    /readyz                               就绪探针（检查 Qdrant 可达）
//   - POST   /v1/search                            memory / docs profile 检索
//   - POST   /v1/collections/{name}/documents      批量 upsert（同步 ingest）
//   - GET    /v1/collections/{name}/documents/{id} 文档状态查询（docs 集合需 ?tenant_id=）
//   - DELETE /v1/collections/{name}/documents/{id} 文档删除（幂等；docs 集合需 ?tenant_id=）
//
// 读路径降级语义：/v1/search 恒返回 200（除 400/401 的请求侧错误）——
// 检索失败表现为空 results + degraded 标记，与 runtime (nil, nil)
// 降级契约对齐，调用方永不因记忆故障而收到 5xx。
//
// 写路径错误语义刻意相反（见 ingest 包说明）：文档端点返回 4xx/5xx。
// 写入是调用方（memory-indexer）的主职责，把失败伪装成 200 会让
// 调用方把"未写入"误当"已写入"，那是记忆静默丢失的唯一通道。
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/daqiaoliang-coder/rag-service/internal/ingest"
	"github.com/daqiaoliang-coder/rag-service/internal/search"
	"github.com/daqiaoliang-coder/rag-service/internal/vector"
)

// HealthChecker 供就绪探针检查后端依赖（Qdrant）。
type HealthChecker interface {
	Health(ctx context.Context) error
}

// New 组装路由与中间件。token 为空表示关闭鉴权（仅限内网/本地开发）；
// 非空时除探针端点外均要求 Authorization: Bearer <token>。
// ingest 允许为 nil（纯读部署），此时文档端点返回 503。
func New(svc *search.Service, ing *ingest.Service, token string, health HealthChecker) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if health == nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "no health checker"})
			return
		}
		if err := health.Health(r.Context()); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unavailable"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	})
	mux.HandleFunc("POST /v1/search", handleSearch(svc))
	mux.HandleFunc("POST /v1/collections/{name}/documents", handleUpsertDocuments(ing))
	mux.HandleFunc("GET /v1/collections/{name}/documents/{id}", handleGetDocument(ing))
	mux.HandleFunc("DELETE /v1/collections/{name}/documents/{id}", handleDeleteDocument(ing))

	var h http.Handler = mux
	h = withAuth(token, h)
	h = withRecover(h)
	h = withLog(h)
	return h
}

// scope 是检索请求中的隔离作用域。
type scope struct {
	TenantID string `json:"tenant_id"`
	ThreadID string `json:"thread_id"`
}

// searchRequest 是 /v1/search 的请求体。
type searchRequest struct {
	Query        string   `json:"query"`
	Profile      string   `json:"profile"`        // 缺省 memory；支持 memory|docs
	Scope        scope    `json:"scope"`          // tenant_id 必填（fail-closed）
	ExcludeRunID string   `json:"exclude_run_id"` // 排除当前 Run，防上下文重复注入
	TopK         int      `json:"top_k"`          // 缺省用服务端配置
	MinScore     *float32 `json:"min_score"`      // 缺省用服务端配置；显式 0 表示不过滤
}

func handleSearch(svc *search.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var body searchRequest
		if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&body); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json body: " + err.Error()})
			return
		}
		profile := search.Profile(body.Profile)
		if profile == "" {
			profile = search.ProfileMemory
		}
		if profile != search.ProfileMemory && profile != search.ProfileDocs {
			writeJSON(w, http.StatusBadRequest, map[string]string{
				"error": "unknown profile " + body.Profile + ": supported profiles are \"memory\" and \"docs\"",
			})
			return
		}
		resp := svc.Search(r.Context(), search.Request{
			Profile:      profile,
			Query:        body.Query,
			TenantID:     body.Scope.TenantID,
			ThreadID:     body.Scope.ThreadID,
			ExcludeRunID: body.ExcludeRunID,
			TopK:         body.TopK,
			MinScore:     body.MinScore,
		})
		// 降级语义：检索失败也是 200 + 空 results + degraded 标记。
		writeJSON(w, http.StatusOK, resp)
	}
}

// ==== 文档写路径端点（Phase 2） ====

// upsertRequest 是批量 upsert 的请求体。
type upsertRequest struct {
	Documents []ingest.Document `json:"documents"`
}

// mapIngestError 把 ingest 错误映射为 HTTP 状态码。
// 请求侧错误（校验/未知集合/缺租户/超块数上限）→ 4xx；其余（embed 网关/Qdrant 故障）→ 503。
func mapIngestError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ingest.ErrUnknownCollection):
		writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
	case errors.Is(err, ingest.ErrInvalidDocument), errors.Is(err, ingest.ErrTooManyDocs),
		errors.Is(err, ingest.ErrTooManyChunks), errors.Is(err, ingest.ErrTenantRequired):
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
	default:
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error()})
	}
}

func handleUpsertDocuments(ing *ingest.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if ing == nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "ingest not configured"})
			return
		}
		var body upsertRequest
		// 上限 16MB：单批文档数已有服务端上限，这里防的是单条超长文档
		// 把请求体撑爆内存（MaxTextLen 的截断责任在调用方）。
		if err := json.NewDecoder(io.LimitReader(r.Body, 16<<20)).Decode(&body); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json body: " + err.Error()})
			return
		}
		resp, err := ing.Upsert(r.Context(), r.PathValue("name"), body.Documents)
		if err != nil {
			mapIngestError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, resp)
	}
}

func handleGetDocument(ing *ingest.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if ing == nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "ingest not configured"})
			return
		}
		name := r.PathValue("name")
		// docs 集合：ID 是逻辑标识（任意非空字符串），子块点 ID 由
		// (tenant, docID) 派生，必须从查询参数取 tenant_id（缺省 400）。
		if ing.IsDocsCollection(name) {
			doc, found, err := ing.GetDoc(r.Context(), name, r.URL.Query().Get("tenant_id"), r.PathValue("id"))
			if err != nil {
				mapIngestError(w, err)
				return
			}
			writeDocStatus(w, doc, found)
			return
		}
		id, err := strconv.ParseUint(r.PathValue("id"), 10, 64)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "document id must be a decimal uint64"})
			return
		}
		doc, found, err := ing.Get(r.Context(), name, id)
		if err != nil {
			mapIngestError(w, err)
			return
		}
		writeDocStatus(w, doc, found)
	}
}

// writeDocStatus 统一输出文档状态查询结果。found:false 也返回 200：
// 这是状态查询而非资源获取，轮询方需要区分"已删除/尚未写入"
// （found=false）与"服务故障"（5xx）。
func writeDocStatus(w http.ResponseWriter, doc vector.RetrievedDoc, found bool) {
	var body any
	if found {
		body = documentStatus{Found: true, Document: doc}
	} else {
		body = documentStatus{Found: false}
	}
	writeJSON(w, http.StatusOK, body)
}

func handleDeleteDocument(ing *ingest.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if ing == nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "ingest not configured"})
			return
		}
		name := r.PathValue("name")
		// docs 集合：删除按 (tenant, docID) 整文档清理全部子块，
		// tenant_id 查询参数必填（缺省 400）。
		if ing.IsDocsCollection(name) {
			deleted, err := ing.DeleteDoc(r.Context(), name, r.URL.Query().Get("tenant_id"), r.PathValue("id"))
			if err != nil {
				mapIngestError(w, err)
				return
			}
			writeJSON(w, http.StatusOK, map[string]bool{"deleted": deleted})
			return
		}
		id, err := strconv.ParseUint(r.PathValue("id"), 10, 64)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "document id must be a decimal uint64"})
			return
		}
		deleted, err := ing.Delete(r.Context(), name, id)
		if err != nil {
			mapIngestError(w, err)
			return
		}
		// 幂等：不存在的 ID 返回 200 + deleted=false，而非 404。
		writeJSON(w, http.StatusOK, map[string]bool{"deleted": deleted})
	}
}

// documentStatus 是 GET 文档状态的响应体。Document 使用 any：
// 未找到时省略该字段（零值 RetrievedDoc 会序列化出一堆空串，徒增噪音）。
type documentStatus struct {
	Found    bool `json:"found"`
	Document any  `json:"document,omitempty"`
}

// withAuth 校验 Bearer 令牌。探针端点豁免：k8s/LB 探针不会携带业务凭证，
// 若强制鉴权会把"未配置令牌"误判为"服务未就绪"。
func withAuth(token string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if token == "" || r.URL.Path == "/healthz" || r.URL.Path == "/readyz" {
			next.ServeHTTP(w, r)
			return
		}
		if r.Header.Get("Authorization") != "Bearer "+token {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
			return
		}
		next.ServeHTTP(w, r)
	})
}

// withRecover 把 panic 转成 500，避免单次请求的 bug 拖垮整个进程。
func withRecover(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				log.Printf("httpapi: panic: %v", rec)
				writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// statusRecorder 捕获最终状态码供访问日志使用。
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

// withLog 输出一行访问日志（方法/路径/状态/耗时），位于最外层，
// 保证 401 与 panic 恢复出的 500 也被记录。
func withLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		log.Printf("httpapi: %s %s -> %d (%s)", r.Method, r.URL.Path, rec.status, time.Since(start).Round(time.Millisecond))
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("httpapi: write json: %v", err)
	}
}
