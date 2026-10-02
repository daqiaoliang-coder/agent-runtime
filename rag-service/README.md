# RAG Service

> 从 [agent-runtime](../) 抽取的检索服务：把 embedding 与向量检索从 runtime 进程中外移，
> 同时承接记忆写路径（文档 API）与混合召回（dense + sparse + RRF）。

一个用 Go 编写的独立 HTTP 服务，与 runtime 同仓不同模块（`rag-service/`），
拥有独立的 `go.mod`，可单独构建、部署与伸缩。

***

## 1. 服务定位

runtime 的长期记忆原本由 worker（读）与 memory-indexer（写）直连 embedding 网关 +
Qdrant 实现。RAG service 把这两条路径收敛为一个独立服务：

```text
runtime worker（读）      memory-indexer（写）
        │                        │
        └─────── rag-api ────────┘
                 │
       ┌─────────┼─────────┐
       ▼         ▼         ▼
   embedding   Qdrant   TF sparse
     网关      (gRPC)     编码器
```

迁移完成后 worker 与索引器的部署面收敛为：MySQL + rag-api URL（不再需要
embedding 凭证与 Qdrant 连接），且检索能力（混合召回、重排、多路融合）可以
独立演进而不动 runtime。

### 双集合双布局

| 集合                | 向量布局                | 用途                                |
| :------------------ | :--------------------- | :---------------------------------- |
| `agent_memory`      | 未命名默认 dense 向量    | 记忆（与 runtime 直连写入兼容，影子对比的前提） |
| `rag_documents`     | 命名 dense + sparse     | 文档（混合召回的物理前提）              |

memory 集合**必须**保持未命名向量布局：runtime 直连模式（`local`）仍用旧布局写入，
若 rag-service 擅自改成命名向量，影子对比期间两路写入会落进不同向量槽，检索结果
必然 MISMATCH。docs 集合则用命名向量以支持 dense + sparse 混合检索。

### 双契约：读降级，写上抛

读写两条路径的错误语义**刻意相反**：

- **读（`/v1/search`）**：任何后端故障（embedding 网关、Qdrant、sparse 编码、预算超时）
  恒返回 `200` + 空 `results` + `degraded` 数组 + warn 日志，**绝不 5xx**。
  调用方（worker 的 ContextLoader）据此降级为"仅用当前 Run 历史"，
  记忆检索失败绝不能阻断主执行链路。
- **写（文档 API）**：错误显式上抛（请求侧错误 4xx，后端故障 503）。
  调用方（memory-indexer）据此退避重试。写路径若静默吞错，
  "未写入"会被误当"已写入"——这是记忆静默丢失的唯一通道。

***

## 2. 端点

| 方法     | 路径                                        | 说明                          |
| :------- | :------------------------------------------ | :---------------------------- |
| `GET`    | `/healthz`                                  | 存活探针（豁免鉴权）            |
| `GET`    | `/readyz`                                   | 就绪探针（Qdrant 连通性，豁免鉴权） |
| `POST`   | `/v1/search`                                | 检索（`memory` / `docs` profile） |
| `POST`   | `/v1/collections/{name}/documents`          | 批量 upsert（同步 ingest，幂等） |
| `GET`    | `/v1/collections/{name}/documents/{id}`     | 文档状态查询（幂等；docs 集合需 `?tenant_id=`） |
| `DELETE` | `/v1/collections/{name}/documents/{id}`     | 文档删除（墓碑，幂等；docs 集合需 `?tenant_id=`） |

除探针外全部端点要求 Bearer 鉴权（`Authorization: Bearer <token>`）。

### 检索

```bash
curl -X POST http://localhost:8080/v1/search \
  -H "Authorization: Bearer $RAG_API_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "如何配置退避重试",
    "profile": "memory",
    "scope": {"tenant_id": "t1", "thread_id": "th1"},
    "exclude_run_id": "run-123",
    "top_k": 10,
    "min_score": 0.7
  }'
```

- `profile`：缺省 `memory`。`memory` 为 dense-only、预算 400ms、按
  `(created_at, node_id)` 正序重排（对话时序）；`docs` 为 parent-child 分块 +
  dense + sparse + RRF 混合召回 + 可选 cross-encoder 重排 + 文档级去重，
  预算 1.5s，保持相关性顺序（不按时间重排）。
- `scope.tenant_id`：**必填**，缺失即 fail-closed 返回空结果（租户隔离的越权防线）。
- `scope.thread_id`：空表示不按会话过滤（存量数据兼容），非空严格匹配。
- `exclude_run_id`：在源头排除当前 Run，防止上下文重复注入。
- `top_k` / `min_score`：缺省用服务端配置；`min_score` 显式传 `0` 表示不过滤。
  注意 `docs` profile 的服务端缺省阈值是 `0`——RRF 融合分（约 `1/(60+rank)`）
  与余弦相似度不可比，沿用 0.7 会过滤掉所有结果；重排生效时分数切换为
  cross-encoder 分，服务端缺省阈值不参与过滤，只有请求显式携带的
  `min_score` 才生效。

响应（`docs` profile 示意）：

```json
{
  "results": [
    {
      "content": "……(父块全文)",
      "role": "user",
      "doc_id": "faq-deploy",
      "chunk_seq": 2,
      "score": 0.93
    }
  ],
  "timings": {"embed_ms": 42, "search_ms": 65, "rerank_ms": 120, "total_ms": 230},
  "degraded": []
}
```

`degraded` 非空（如 `rerank_skipped`、`embed_failed`）表示本轮为降级结果。
`node_id` / `run_id` / `score` / `doc_id` / `chunk_seq` 透出供调用方溯源与去重；
`timings` 分阶段耗时供延迟预算对账。

#### docs profile 管道（Phase 3）

```text
写入（docs 集合，同步）
  Document ──分块──> children（child ≤400 rune / parent ≤1200 rune）
        │（可选，RAG_CONTEXTUAL_ENABLED）
        ├──LLM 摘要前缀（80 字内定位摘要；失败 = 整批写入失败）
        ▼
  embed(前缀+child) + sparse(前缀+child)，子批 64
        ▼
  child 点（ID=SHA256(tenant|docID|seq)[:8]，payload 含 doc_id/chunk_seq/parent_text）

读取（docs profile）
  query → embed + sparse → HybridSearch(overfetch×topK 子块)
        → rerank(children)（TEI，失败降级 rerank_skipped 保持 RRF 序）
        → 按 doc_id 去重（每文档保留最高分子块）→ 截断 topK
        → Result.content = parent_text（子块命中，父块作上下文返回）
```

关键机制：
- **配置演进即重嵌**：文档级 `content_hash` 输入含分块参数与增强模型
  （`p{parent}-c{child}[+enh:{model}]`），改配置后同内容重放自动重嵌。
- **孤儿清理**：内容变更后新子块数可能少于旧版，写入前按
  `(tenant, docID)` 整文档删除旧子块，删除放在增强与嵌入之后、写入之前。
- **docs ID 语义**：任意非空逻辑字符串（如 `faq-deploy`），与 memory 集合
  的十进制 uint64 约束刻意不同。

### 文档写入

```bash
curl -X POST http://localhost:8080/v1/collections/agent_memory/documents \
  -H "Authorization: Bearer $RAG_API_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "documents": [
      {
        "id": "12345678901234567890",
        "content": "节点文本",
        "metadata": {
          "tenant_id": "t1", "thread_id": "th1", "run_id": "run-123",
          "node_id": "node-456", "role": "assistant"
        }
      }
    ]
  }'
```

幂等三重保障：调用方派生确定性 ID（runtime 侧 `SHA256(tenant|thread|node|role)`
前 8 字节）+ 调用方进度表（`memory_indexed` INSERT IGNORE）+ 服务端
`content_hash` 识别重复提交并跳过重嵌入。响应 `{"indexed": 1, "skipped": 0}`
区分新写入与幂等跳过。

向 **docs 集合**（`rag_documents`）写入时走 Phase 3 管道（分块/增强/子块点，
见上文）：`id` 是任意非空逻辑字符串，响应多一个 `chunks` 字段（本次写入的
子块点数，memory 集合恒为 `0`）：

```bash
curl -X POST http://localhost:8080/v1/collections/rag_documents/documents \
  -H "Authorization: Bearer $RAG_API_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "documents": [
      {"id": "faq-deploy", "content": "……(文档全文)", "metadata": {"tenant_id": "t1"}}
    ]
  }'
# -> {"indexed": 1, "skipped": 0, "chunks": 7}

# 状态查询/删除（docs 集合必须带 tenant_id：子块点 ID 由 (tenant, docID) 派生）
curl "http://localhost:8080/v1/collections/rag_documents/documents/faq-deploy?tenant_id=t1" \
  -H "Authorization: Bearer $RAG_API_TOKEN"
curl -X DELETE "http://localhost:8080/v1/collections/rag_documents/documents/faq-deploy?tenant_id=t1" \
  -H "Authorization: Bearer $RAG_API_TOKEN"
```

单请求子块总数上限 `RAG_MAX_CHUNKS_PER_REQUEST`（缺省 256）——同步管道的
时长护栏，超限返回 400（请求侧错误：拆批重提即可）。

***

## 3. 与 runtime 对接

runtime 侧通过两个环境变量切换（详见 runtime README 的长期记忆章节）：

| runtime 变量          | rag-service 侧对应                 |
| :-------------------- | :--------------------------------- |
| `MEMORY_BACKEND`（读） | `remote` → `POST /v1/search`（memory profile） |
| `INDEXER_BACKEND`（写）| `remote` → 文档 API（upsert）       |

推荐的迁移验收顺序：

1. `MEMORY_BACKEND=shadow`：读路径双跑对比，日志输出 `memory(shadow): match`，
   持续无 `MISMATCH` 后切换 `remote`。
2. `INDEXER_BACKEND=shadow`：写路径双写对比，日志输出
   `memory(shadow-write): match`，远程失败仅告警（直连为准），
   持续一致后切换 `remote`。
3. 双双 `remote` 后，worker 与索引器不再需要 embedding 凭证与 Qdrant 连接。

***

## 4. 配置

| 变量                      | 默认值              | 说明                                          |
| :------------------------ | :------------------ | :-------------------------------------------- |
| `RAG_ADDR`                | `:8080`             | 监听地址                                       |
| `RAG_API_TOKEN`           | 空（不鉴权）         | Bearer 令牌；生产必须设置                        |
| `RAG_QDRANT_HOST`         | `localhost`         | Qdrant 地址                                    |
| `RAG_QDRANT_PORT`         | `6334`              | gRPC 端口（**不是** REST 的 6333）              |
| `RAG_QDRANT_API_KEY`      | 空（不鉴权）         | Qdrant API key                                 |
| `RAG_COLLECTION`          | `agent_memory`      | 记忆集合名（与 runtime 共用）                    |
| `RAG_DOCS_COLLECTION`     | `rag_documents`     | 文档集合名（命名 dense+sparse 布局）             |
| `RAG_EMBED_BASE_URL`      | **必填**            | OpenAI Embeddings 兼容网关；缺失直接启动失败      |
| `RAG_EMBED_MODEL`         | `bge-m3`            | 必须与索引侧使用的模型一致                       |
| `RAG_EMBED_API_KEY`       | 空                  | 网关匿名可访问时留空（本地 ollama/TEI）           |
| `RAG_EMBED_TIMEOUT_MS`    | `3000`              | embedding 调用超时                              |
| `RAG_EMBED_DIM`           | `1536`              | 向量维度；须与 runtime 的 `EMBEDDING_DIM` 一致   |
| `RAG_TOPK`                | `10`                | 请求未指定 `top_k` 时的缺省                      |
| `RAG_MIN_SCORE`           | `0.7`               | memory profile 缺省阈值                         |
| `RAG_BUDGET_MS`           | `400`               | memory profile 单次检索总预算                    |
| `RAG_DOCS_BUDGET_MS`      | `1500`              | docs profile 总预算（dense+sparse+RRF）          |
| `RAG_MAX_DOCS_PER_REQUEST`| `64`                | 单次 upsert 文档数上限（memory 集合）            |
| `RAG_CHUNK_PARENT_SIZE`   | `1200`              | docs 父块上限（rune），命中子块返回父块全文        |
| `RAG_CHUNK_CHILD_SIZE`    | `400`               | docs 子块上限（rune），参与嵌入与检索             |
| `RAG_MAX_CHUNKS_PER_REQUEST` | `256`            | 单请求 docs 子块总数上限（同步管道时长护栏）       |
| `RAG_DOCS_OVERFETCH`      | `4`                 | 子块过采样倍数：去重前取 topK×N，凑满 topK 篇文档 |
| `RAG_RERANK_BASE_URL`     | 空（关闭重排）       | TEI `/rerank` 兼容端点；空 = 保持 RRF 序          |
| `RAG_RERANK_MODEL`        | 空                  | 空 = TEI 单模型部署（请求不带 model 字段）        |
| `RAG_RERANK_TIMEOUT_MS`   | `500`               | 单次重排预算；须显著小于 docs 总预算              |
| `RAG_CONTEXTUAL_ENABLED`  | `false`             | LLM 上下文增强开关；关闭时写路径零 LLM 依赖       |
| `RAG_CONTEXTUAL_BASE_URL` | 开启时必填          | OpenAI 兼容 chat 网关；开启而缺失直接启动失败      |
| `RAG_CONTEXTUAL_API_KEY`  | 空                  | 网关密钥（空 = 匿名）                             |
| `RAG_CONTEXTUAL_MODEL`    | 开启时必填          | 增强模型名；参与 content_hash，换模型触发重嵌      |
| `RAG_CONTEXTUAL_CONCURRENCY` | `4`              | 子块增强并发信号量                                |

启动时 fail-fast：`RAG_EMBED_BASE_URL` 缺失、维度非法、任一集合创建/校验失败
（含维度漂移——换 embedding 模型后 `RAG_EMBED_DIM` 与存量集合不一致）、
上下文增强开启但网关/模型缺失，都会直接退出，
避免服务带着残缺依赖启动、每次检索都静默降级。

***

## 5. 目录结构

```text
rag-service/
├── cmd/rag-api/            # 入口：装配 + 优雅关停（15s 排空在途请求）
├── cmd/rag-eval/           # 评测 CLI：灌语料 → 检索 → 指标 → 回归门禁
├── internal/
│   ├── config/             # 环境变量装配（fail-fast 校验）
│   ├── httpapi/            # 路由 / Bearer 鉴权 / recover / 访问日志
│   ├── search/             # 检索管道：memory 等价 + docs（重排/去重/父块返回）
│   ├── ingest/             # 写路径：content_hash 幂等 + 分块 + 上下文增强
│   ├── chunk/              # parent-child 分块（纯函数）
│   ├── llm/                # OpenAI 兼容 chat 客户端（上下文增强用）
│   ├── rerank/             # TEI /rerank 兼容客户端（TEI/Jina/Cohere 格式）
│   ├── eval/               # 评测指标纯函数（recall@k / MRR@k / nDCG@k / 分位）
│   ├── sparse/             # TF sparse 编码器（CJK 二元组 + ASCII 词）
│   ├── embed/              # OpenAI 兼容 embeddings 客户端
│   └── vector/             # Qdrant gRPC 适配（Writer / Searcher / HybridSearcher）
├── eval/golden/            # golden set：corpus.jsonl（54 篇）+ queries.jsonl（34 条）
├── deploy/docker-compose.yml  # 本地开发：Qdrant + rag-api（+ 可选 TEI 重排器）
└── Dockerfile
```

构建与测试：

```bash
cd rag-service
go build ./...
go vet ./...
go test ./...
```

本地起一套（embedding 网关需外部提供，下例指向宿主机 ollama）：

```bash
cd deploy && docker compose up -d
```

带重排器起一套（首次拉取 bge-reranker-v2-m3 约 2GB；取消 compose 文件里
`RAG_RERANK_BASE_URL` 的注释）：

```bash
cd deploy && docker compose --profile rerank up -d
```

***

## 6. 评测（rag-eval，Phase 3）

`cmd/rag-eval` 是 docs profile 的评测闭环 CLI：灌入 golden 语料（限速分批
16 篇 + 500ms 间隔，重复运行幂等跳过）→ 逐查询检索（串行，保证 p95 不被
并发挤压失真）→ 计算指标 → 输出人读表格与 JSON 报告。

```bash
# 建立基线（首次运行的读数即后续所有变更的回归锚点）
go run ./cmd/rag-eval -api http://localhost:8080 -out eval-report.json

# 带阈值的 CI 门禁：不达标 exit 1
go run ./cmd/rag-eval -api http://localhost:8080 \
  -min-recall 0.8 -min-mrr 0.7

# 对比上一份报告：recall/MRR/nDCG 任一回退 >2pp exit 1
go run ./cmd/rag-eval -api http://localhost:8080 \
  -baseline eval-report.json -out eval-report-new.json
```

指标（宏平均，二值相关，纯函数有单测）：

| 指标        | 含义                                        |
| :---------- | :------------------------------------------ |
| `recall@k`  | 前 k 个结果命中的期望文档占比                  |
| `mrr@k`     | 首个命中期望文档的排名倒数                      |
| `ndcg@k`    | 折损累计增益（位置越靠前的命中贡献越大）           |
| `p50 / p95` | 检索总耗时（`timings.total_ms`）分位            |

golden set（`eval/golden/`）：54 篇中文语料取材于真实项目文档改写，
34 条查询覆盖直接匹配、同义改写、分块边界（查询词只出现在长文档中部）、
跨文档干扰（近似主题区分）与多目标召回。语料与查询都是 JSONL 纯文本，
任何变更都有 diff 可审。

