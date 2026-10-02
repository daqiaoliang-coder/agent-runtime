// Package config 从环境变量装配 rag-api 配置。
//
// 原则：可选项给安全默认值；必选项缺失直接报错 fail-fast（如 EmbedBaseURL），
// 避免服务带着残缺依赖启动、每次检索都静默降级——那会把配置错误伪装成
// 后端故障，误导排查方向。
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config 是 rag-api 的全部运行时配置。字段默认值与 runtime 的
// providers.DefaultMemory* 及 deploy/docker-compose.yml 中的环境变量一一对应。
type Config struct {
	Addr     string // 监听地址
	APIToken string // Bearer 鉴权令牌，空表示关闭鉴权（仅限内网/本地开发）

	QdrantHost   string
	QdrantPort   int // gRPC 端口，默认 6334（6333 是 REST/Web UI）
	QdrantAPIKey string

	Collection string // 记忆集合名，默认 agent_memory（与 runtime 共用同一集合）

	// DocsCollection 是 docs profile 的目标集合（命名 dense+sparse 布局），
	// 与 memory 集合分离：后者必须保持未命名向量布局以兼容 runtime 直连写入。
	DocsCollection string

	// EmbedDim 是向量维度。必须与 memory-indexer 的 EMBEDDING_DIM 一致，
	// 启动时用它校验/创建两个集合——维度漂移（换 embedding 模型）必须显式失败。
	EmbedDim int

	// MaxDocsPerRequest 限制单次 upsert 的文档数（同步 ingest 的请求内预算）。
	MaxDocsPerRequest int

	EmbedBaseURL string // OpenAI Embeddings 兼容网关，必填
	EmbedModel   string // 必须与索引侧（memory-indexer）使用的模型一致
	EmbedAPIKey  string // 空表示网关匿名可访问（本地 ollama/TEI）
	EmbedTimeout time.Duration

	DefaultTopK     int           // 请求未指定 top_k 时的默认值
	DefaultMinScore float32       // 请求未指定 min_score 时的默认值（memory profile）
	Budget          time.Duration // memory profile 单次检索总预算（含 embed + 向量检索）
	DocsBudget      time.Duration // docs profile 总预算（dense+sparse+RRF）

	// Phase 3：docs profile 的分块/增强/重排配置。
	ChunkParentSize   int // parent 块上限（rune），缺省 1200；<=0 由 chunk 包兜底
	ChunkChildSize    int // child 块上限（rune），缺省 400
	MaxChunksPerReq   int // 单请求 docs 子块总数上限（同步管道时长护栏），缺省 256
	DocsOverfetch     int // 子块过采样倍数（去重前），缺省 4；<=0 由 search 包兜底

	ContextualEnabled     bool          // LLM 上下文增强开关，缺省关闭（写路径零 LLM 依赖）
	ContextualBaseURL     string        // OpenAI 兼容 chat 网关；开启增强时必填
	ContextualAPIKey      string        // 空 = 网关匿名可访问
	ContextualModel       string        // 增强用的模型名；开启增强时必填（参与 content_hash 派生）
	ContextualConcurrency int           // 子块增强并发信号量，缺省 4
	RerankBaseURL         string        // TEI /rerank 兼容端点；空 = 关闭重排（保持 RRF 序）
	RerankModel           string        // 空 = TEI 单模型部署（请求不携带 model 字段）
	RerankTimeout         time.Duration // 单次重排预算，缺省 500ms；须显著小于 DocsBudget
}

// FromEnv 读取并校验配置。
func FromEnv() (Config, error) {
	cfg := Config{
		Addr:              getenv("RAG_ADDR", ":8080"),
		APIToken:          os.Getenv("RAG_API_TOKEN"),
		QdrantHost:        getenv("RAG_QDRANT_HOST", "localhost"),
		QdrantPort:        getenvInt("RAG_QDRANT_PORT", 6334),
		QdrantAPIKey:      os.Getenv("RAG_QDRANT_API_KEY"),
		Collection:        getenv("RAG_COLLECTION", "agent_memory"),
		DocsCollection:    getenv("RAG_DOCS_COLLECTION", "rag_documents"),
		EmbedDim:          getenvInt("RAG_EMBED_DIM", 1536),
		MaxDocsPerRequest: getenvInt("RAG_MAX_DOCS_PER_REQUEST", 64),
		EmbedBaseURL:      os.Getenv("RAG_EMBED_BASE_URL"),
		EmbedModel:        getenv("RAG_EMBED_MODEL", "bge-m3"),
		EmbedAPIKey:       os.Getenv("RAG_EMBED_API_KEY"),
		EmbedTimeout:      time.Duration(getenvInt("RAG_EMBED_TIMEOUT_MS", 3000)) * time.Millisecond,
		DefaultTopK:       getenvInt("RAG_TOPK", 10),
		DefaultMinScore:   float32(getenvFloat("RAG_MIN_SCORE", 0.7)),
		Budget:            time.Duration(getenvInt("RAG_BUDGET_MS", 400)) * time.Millisecond,
		DocsBudget:        time.Duration(getenvInt("RAG_DOCS_BUDGET_MS", 1500)) * time.Millisecond,

		ChunkParentSize:       getenvInt("RAG_CHUNK_PARENT_SIZE", 1200),
		ChunkChildSize:        getenvInt("RAG_CHUNK_CHILD_SIZE", 400),
		MaxChunksPerReq:       getenvInt("RAG_MAX_CHUNKS_PER_REQUEST", 256),
		DocsOverfetch:         getenvInt("RAG_DOCS_OVERFETCH", 4),
		ContextualEnabled:     getenvBool("RAG_CONTEXTUAL_ENABLED"),
		ContextualBaseURL:     os.Getenv("RAG_CONTEXTUAL_BASE_URL"),
		ContextualAPIKey:      os.Getenv("RAG_CONTEXTUAL_API_KEY"),
		ContextualModel:       os.Getenv("RAG_CONTEXTUAL_MODEL"),
		ContextualConcurrency: getenvInt("RAG_CONTEXTUAL_CONCURRENCY", 4),
		RerankBaseURL:         os.Getenv("RAG_RERANK_BASE_URL"),
		RerankModel:           os.Getenv("RAG_RERANK_MODEL"),
		RerankTimeout:         time.Duration(getenvInt("RAG_RERANK_TIMEOUT_MS", 500)) * time.Millisecond,
	}
	if cfg.EmbedBaseURL == "" {
		return cfg, fmt.Errorf("RAG_EMBED_BASE_URL is required: rag-api cannot answer any query without an embedding gateway")
	}
	if cfg.EmbedDim <= 0 {
		return cfg, fmt.Errorf("RAG_EMBED_DIM must be positive, got %d", cfg.EmbedDim)
	}
	// 增强开启但网关/模型缺失 → fail-fast：否则带着残缺依赖启动，
	// 第一次 docs 写入才失败，把配置错误伪装成后端故障。
	if cfg.ContextualEnabled {
		if cfg.ContextualBaseURL == "" {
			return cfg, fmt.Errorf("RAG_CONTEXTUAL_BASE_URL is required when RAG_CONTEXTUAL_ENABLED=true")
		}
		if cfg.ContextualModel == "" {
			return cfg, fmt.Errorf("RAG_CONTEXTUAL_MODEL is required when RAG_CONTEXTUAL_ENABLED=true")
		}
	}
	return cfg, nil
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getenvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func getenvFloat(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

func getenvBool(key string) bool {
	switch os.Getenv(key) {
	case "1", "true", "TRUE", "True", "yes":
		return true
	default:
		return false
	}
}
