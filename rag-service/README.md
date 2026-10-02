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
| `GET`    | `/v1/collections/{name}/documents/{id}`     | 文档状态查询（幂等）            |
| `DELETE` | `/v1/collections/{name}/documents/{id}`     | 文档删除（墓碑，幂等）          |

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
  `(created_at, node_id)` 正序重排（对话时序）；`docs` 为 dense + sparse + RRF
  混合召回、预算 1.5s、保持相关性顺序（融合分降序，不按时间重排）。
- `scope.tenant_id`：**必填**，缺失即 fail-closed 返回空结果（租户隔离的越权防线）。
- `scope.thread_id`：空表示不按会话过滤（存量数据兼容），非空严格匹配。
- `exclude_run_id`：在源头排除当前 Run，防止上下文重复注入。
- `top_k` / `min_score`：缺省用服务端配置；`min_score` 显式传 `0` 表示不过滤。
  注意 `docs` profile 的服务端缺省阈值是 `0`——RRF 融合分（约 `1/(60+rank)`）
  与余弦相似度不可比，沿用 0.7 会过滤掉所有结果。

响应（`docs` profile 示意）：

```json
{
  "results": [
    {
      "content": "……",
      "role": "user",
      "node_id": "node-456",
      "run_id": "run-123",
      "score": 0.0322
    }
  ],
  "degraded": []
}
```

`degraded` 非空（如 `sparse_timeout`、`embed_error`）表示本轮为降级结果。
`node_id` / `run_id` / `score` 透出供调用方溯源与去重。

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
| `RAG_MAX_DOCS_PER_REQUEST`| `64`                | 单次 upsert 文档数上限                          |

启动时 fail-fast：`RAG_EMBED_BASE_URL` 缺失、维度非法、任一集合创建/校验失败
（含维度漂移——换 embedding 模型后 `RAG_EMBED_DIM` 与存量集合不一致）都会直接退出，
避免服务带着残缺依赖启动、每次检索都静默降级。

***

## 5. 目录结构

```text
rag-service/
├── cmd/rag-api/            # 入口：装配 + 优雅关停（15s 排空在途请求）
├── internal/
│   ├── config/             # 环境变量装配（fail-fast 校验）
│   ├── httpapi/            # 路由 / Bearer 鉴权 / recover / 访问日志
│   ├── search/             # 检索管道：embed → 过滤 → 重排 → 角色归类
│   ├── ingest/             # 写路径：content_hash 幂等 + 双集合布局分发
│   ├── sparse/             # TF sparse 编码器（CJK 二元组 + ASCII 词）
│   ├── embed/              # OpenAI 兼容 embeddings 客户端
│   └── vector/             # Qdrant gRPC 适配（Writer / Searcher / HybridSearcher）
├── deploy/docker-compose.yml  # 本地开发：Qdrant + rag-api
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
