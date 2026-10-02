// cmd/memory-indexer 是记忆的**写入路径**：把已完成节点的内容投影为向量写入向量库。
//
// 独立成进程（而非塞进 worker）的理由：
//  1. 故障域隔离——向量库或 embedding 网关故障绝不能影响节点执行；
//  2. 限速——它与主链路推理共用同一个 embedding 网关与 API key，
//     必须能独立控制调用频率，宁可索引滞后也不拖累在线推理；
//  3. 可重建——向量库只是派生索引，清空 memory_indexed 表即可触发全量重建。
//
// 写入后端由 INDEXER_BACKEND 选择（Phase 2，与 worker 读路径的 MEMORY_BACKEND 对应）：
//   - local（缺省）：进程内 embed + 直连 Qdrant；
//   - remote：经 rag-api 文档 API 写入，本进程不再需要 embedding 凭证与 Qdrant 连接；
//   - shadow：双写对比，直连为准、远程失败仅告警——切换 remote 前的等价性验收手段。
//
// 结构与 cmd/outbox 同构：轮询扫描 → 处理 → 标记完成。
package main

import (
	"agent-runtime/internal/adapters/credential"
	"agent-runtime/internal/adapters/vector"
	"agent-runtime/internal/contracts"
	"agent-runtime/internal/llm"
	"agent-runtime/internal/memory"
	"agent-runtime/internal/store"
	"agent-runtime/internal/trace"
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
	// 优雅退出：收到 SIGINT/SIGTERM 时取消 ctx，让当前批次收尾后退出，
	// 避免在"向量已写入、进度未标记"之间被硬杀（虽然该情况幂等可恢复，
	// 但干净退出能省下重扫与重复 embedding 的开销）。
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 初始化 OpenTelemetry 轨迹追踪；失败时降级为 no-op，不阻断启动。
	if shutdown, err := trace.Init("memory-indexer"); err != nil {
		log.Printf("trace init skipped: %v", err)
	} else {
		defer shutdown(ctx)
	}

	s, err := store.New(ctx, env("DATABASE_DSN", "agent:agent@tcp(localhost:3306)/agent_runtime?parseTime=true"))
	if err != nil {
		log.Fatal(err)
	}
	defer s.Close()

	// 写入后端选择：值非法或缺 RAG_API_URL 时 warn 后回退 local
	// （配置笔误不应让进程起不来，与 envDuration 的回退哲学一致；
	// 回退后若缺本地凭证，会在下方装配处 fail-fast，错误信息依然可定位）。
	backend := env("INDEXER_BACKEND", "local")
	switch backend {
	case "local", "remote", "shadow":
	default:
		log.Printf("memory-indexer: invalid INDEXER_BACKEND %q, using local", backend)
		backend = "local"
	}
	apiURL := env("RAG_API_URL", "")
	if (backend == "remote" || backend == "shadow") && apiURL == "" {
		log.Printf("memory-indexer: INDEXER_BACKEND=%s requires RAG_API_URL; falling back to local", backend)
		backend = "local"
	}

	// local 与 shadow 需要本地 embedding + 直连 Qdrant；remote 模式整体外移，
	// 跳过这两项凭证检查（部署面收敛为 MySQL + rag-api URL）。
	//
	// embedding 复用与对话推理相同的网关配置，但密钥一律经托管层获取。
	// 未配置凭证时明确退出：local/shadow 模式下索引器没有 embedding 就无法工作，
	// 静默降级成 no-op 只会让"记忆功能失效"变得难以察觉。
	// 与 worker 的降级策略相反是刻意的——worker 缺记忆只是少个增强，
	// 索引器缺 embedding 则完全无法履职，早失败好过空转。
	var localWriter *memory.LocalWriter
	if backend == "local" || backend == "shadow" {
		creds := credential.FromEnv()
		base := env("OPENAI_BASE_URL", "")
		if base == "" {
			log.Fatal("memory-indexer: OPENAI_BASE_URL must be set (embedding gateway is required)")
		}
		if _, cerr := creds.Credential(ctx, contracts.CredentialPurposeEmbedding); cerr != nil {
			log.Fatalf("memory-indexer: no embedding credential available: %v", cerr)
		}

		// 向量库 SDK 只接受 string 型 apiKey，无法接 port，走托管层的降级出口。
		// 明文短暂存在于局部变量，但不再由 os.Getenv 散落各处，
		// 换托管设施（文件 / Vault）时此处一行都不用改。
		vs, verr := vector.NewQdrant(env("QDRANT_HOST", "localhost"), envInt("QDRANT_PORT", 6334),
			credential.Value(ctx, creds, contracts.CredentialPurposeVectorDB))
		if verr != nil {
			log.Fatal(verr)
		}
		defer vs.Close()

		localWriter = &memory.LocalWriter{
			Embedder: llm.NewOpenAIEmbedderWithCredentials(base, env("EMBEDDING_MODEL", "text-embedding-3-small"), creds, contracts.CredentialPurposeEmbedding),
			Vectors:  vs,
		}
	}

	// 写入超时 60s：rag-api 同步 ingest 在请求内完成整批 embedding
	// （单批最多 BatchSize×2 条文本），写路径无读路径的 400ms 预算约束。
	var writer memory.Writer
	switch backend {
	case "remote":
		writer = newRemoteWriter(apiURL)
	case "shadow":
		writer = &memory.ShadowWriter{Primary: localWriter, Secondary: newRemoteWriter(apiURL)}
	default:
		writer = localWriter
	}

	dim := envInt("EMBEDDING_DIM", 1536)
	ix := memory.New(
		writer,
		s,
		memory.Config{
			Collection:   env("QDRANT_COLLECTION", memory.DefaultCollection),
			Dim:          dim,
			ScanLimit:    envInt("MEMORY_SCAN_LIMIT", memory.DefaultScanLimit),
			BatchSize:    envInt("MEMORY_BATCH_SIZE", memory.DefaultBatchSize),
			MaxTextLen:   envInt("MEMORY_MAX_TEXT_LEN", memory.DefaultMaxTextLen),
			BatchDelay:   envDuration("MEMORY_BATCH_DELAY", memory.DefaultBatchDelay),
			PollInterval: envDuration("MEMORY_POLL_INTERVAL", memory.DefaultPollInterval),
		},
	)

	// 启动前确保写入后端就绪（local：建集合；remote：rag-api 探活；shadow：两者），
	// 幂等。维度不匹配时直接失败：这通常意味着换了 embedding 模型，
	// 必须显式处理（新建集合或重建索引），静默继续会产生难以定位的写入错误。
	if err := ix.EnsureCollection(ctx); err != nil {
		log.Fatalf("memory-indexer: ensure collection: %v", err)
	}

	log.Printf("memory-indexer started backend=%s collection=%s dim=%d scan_limit=%d batch_size=%d poll=%s",
		backend, ix.Config.Collection, dim, ix.Config.ScanLimit, ix.Config.BatchSize, ix.Config.PollInterval)
	// Run 阻塞轮询直到 ctx 取消；返回非 nil 表示启动即失败（如集合维度不符）。
	log.Fatal(ix.Run(ctx))
}

// newRemoteWriter 构造 rag-api 写入端。Token 为空表示对端未开启鉴权（仅限内网/本地）。
func newRemoteWriter(apiURL string) *memory.RemoteWriter {
	return &memory.RemoteWriter{
		BaseURL: apiURL,
		Token:   env("RAG_API_TOKEN", ""),
		Client:  &http.Client{Timeout: 60 * time.Second},
	}
}

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func envInt(k string, d int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
		log.Printf("memory-indexer: invalid int for %s=%q, using default %d", k, v, d)
	}
	return d
}

// envDuration 解析如 "500ms" / "5s" 的时长；解析失败时回退默认值并记日志。
// 回退而非退出：配置笔误不应让整个索引器起不来，用默认值更安全。
func envDuration(k string, d time.Duration) time.Duration {
	if v := os.Getenv(k); v != "" {
		if t, err := time.ParseDuration(v); err == nil {
			return t
		}
		log.Printf("memory-indexer: invalid duration for %s=%q, using default %s", k, v, d)
	}
	return d
}
