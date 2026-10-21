# 上下文压缩（Context Compaction）设计

长 Run 的对话历史由 `ContextLoader` 从已提交节点派生，且随节点数单调增长。
工具输出通常比对话大一到两个数量级，是上下文膨胀的首要来源。现有的"塑形层"
（[internal/worker/context.go](../internal/worker/context.go)：祖先作用域 + 工具结果遮蔽）
只做了**形状裁剪**，没有**预算治理**：它不知道目标模型的窗口有多大，
不知道当前组装出来的上下文已经占了多少 token，也没有"超限之后怎么办"的完整答案。

本文档补齐这一层：参考 Claude Code（2.1.88 泄露版逆向分析）的五层级联压缩管线，
把它适配到本项目的分布式持久执行架构上。

***

## 1. 设计参考：Claude Code 的五层压缩管线

Claude Code 是单进程 CLI，上下文是内存中一条 append-only 的消息数组，
压缩 = 在恰当的时机**就地改写**这条数组。其管线要点：

```text
L1  工具结果执行时截断        确定性   工具返回即截断（字符/行数上限）
L2  窗口裁剪 / 老结果遮蔽      确定性   接近阈值时，老工具结果替换为占位符
L3  微压缩（micro-compaction）LLM     对较早轮次做选择性摘要，近期轮次保持完整
L4  自动压缩（auto-compact）  LLM     全量对话 → 结构化摘要，摘要成为新的上下文基座
L5  硬截断兜底                确定性   摘要失败也绝不因超窗而崩溃
```

三条核心思想，与具体实现无关，全部继承：

1. **级联**：无 LLM 的确定性操作永远优先于 LLM 摘要——截断和遮蔽零成本，
   摘要本身要花 token，只在前者不够时才发生。
2. **阈值动态计算**：触发点不是写死的消息条数，而是
   `窗口大小 × 比例 − 输出预留`，随模型窗口自适应。
3. **摘要结构化**：L4 的全量摘要不是自由文本，而是固定八段
   （任务目标 / 关键概念 / 文件与代码 / 错误与修复 / 问题求解 / 用户全部请求 /
   待办 / 当前工作与下一步），保证压缩后模型仍能继续执行任务。

（注：不同逆向分析对"五层"的编号口径略有差异，本文以机制对齐为准，不纠结编号。）

### 1.1 本项目与 Claude Code 的根本差异

差异决定设计，必须先讲清楚：

| 维度 | Claude Code（CLI） | Agent Runtime（分布式持久） |
|---|---|---|
| 上下文形态 | 进程内存中的消息数组 | **派生物**：每次执行时从 SUCCESS 节点重建 |
| 压缩动作 | 就地改写数组 | **不能改写任何节点**——`agent_node.output` 是权威事实 |
| 进程模型 | 单进程常驻 | Worker 无状态，崩溃后换进程恢复 |
| 压缩产物 | 随进程消亡 | 必须持久化，否则每次组装都重复压缩 |

结论：在本项目里，**压缩 = 持久化的"派生视图" + 水位线（waterline）**。
压缩产生一条新记录（摘要 + 被覆盖的节点上界），原文不动；
`ContextLoader` 组装时从水位线之后开始取节点。这同时满足仓库既定原则：
MySQL 持有权威事实，其余（含向量索引）皆为可重建的派生物。

***

## 2. 总体设计

触发点选在 `ContextLoader` 内部——它是 executor 与 ReAct 两条路径唯一的上下文
组装点（`executeLLM` / `executeReflect` / `react.Engine.Run` 都经过它），
在这里做预算检查，不需要新增任何调用方改动。

```text
ContextLoader(tenant, run, node)
   │
   ① 读该 Run 最新压缩记录（最大水位线 W）
   ② 加载 W 之后的 SUCCESS 祖先节点
   ③ 塑形：REFLECT 排除 / 工具遮蔽 / 超长截断（现有逻辑）
      + 微压缩产物内联回填
   ④ 组装：[全量摘要(system)] → 历史 → （记忆召回前置，现状不变）
   ⑤ 估算本次上下文 token（实测校准 + 增量估算）
   │
   ├─ 估算 ≤ T_micro ────────────────→ 直接返回
   ├─ 超 T_micro → L2 收缩：缩小 keepFull 窗口，重估算
   ├─ 仍超      → L3 微压缩：摘要水位线后前缀节点，持久化，回 ②
   ├─ 超 T_auto → L4 全量压缩：八段摘要，水位线推进，持久化，回 ②
   └─ 压缩失败 / 压缩后仍超 → L5 硬截断（保尾部 + 指针），返回
```

五层与现状的映射：

| 层 | 机制 | 现状 | 本设计动作 |
|---|---|---|---|
| L1 | 执行时截断 | **已有**：`DefaultToolOutputMaxRunes=4000`，ReAct 路径 `capRunesForReact` | 保持，参数化按工具分级（P2） |
| L2 | 组装时遮蔽 | **已有**：`ToolMaskWindow=6` 固定窗口 | 从"固定条数"升级为"预算驱动收缩" |
| L3 | 微压缩 | 无 | 新增：前缀节点批量摘要，产物持久化 |
| L4 | 全量压缩 | 无 | 新增：八段结构化摘要 + 水位线推进 |
| L5 | 硬截断兜底 | 无 | 新增：确定性保底，压缩失败也绝不挂 Run |

`fetch_tool_result` 工具（按 node_id 取回遮蔽原文）与本管线天然协同：
所有占位符/摘要都保留 node_id 指针，模型需要细节时按需展开，而非全量预载。

***

## 3. 触发与预算模型

### 3.1 两个预算，互不替代

先澄清一个容易混淆的点：

- **窗口预算**（本设计新增）：单次 LLM 请求的上下文上限，防的是
  `prompt 超过模型窗口导致请求失败`。压缩服务于它。
- **成本预算**（已有）：`run.MaxTokens` 按 `llm_usage` 累计，防的是 Run 失控烧钱，
  超限入 DLQ。**压缩不"省出"成本预算**——摘要本身还要花 token，
  该检查（worker.Handle 内）语义不变。

### 3.2 阈值公式

```text
T_auto  = floor(window(model) × R_auto)  − Rsv_out     # 全量压缩触发线，默认 R_auto=0.92
T_micro = floor(window(model) × R_micro) − Rsv_out     # 微压缩/收缩触发线，默认 R_micro=0.80
```

- `window(model)`：按模型名查配置表（env 提供默认值），未知模型取保守默认 32K；
- `Rsv_out`：为模型输出预留的空间（输出也要占窗口），默认 8192；
- 早警线 `T_micro` 的意义：在还来得及做低成本处理时就开始收缩，
  不要等到撞上 `T_auto` 才动手。

### 3.3 token 估算：实测优先，估算兜底

仓库不引入 tokenizer 依赖（Go 侧缺少与各网关一致的实现，引入即制造口径分歧）。
估算策略：

```text
base    = 该 Run 最近一次 llm_usage.prompt_tokens（实测值）
delta   = 上次实测之后新增内容的字符数 ÷ K（默认 K=4，可配）
估算值  = base + delta          （无实测值时，全部内容按 ÷K 估算）
```

实测校准让误差随 Run 推进自动收敛（每次 LLM 调用都会落 `llm_usage`），
估算只承担"自上次实测以来的增量"，误差上界可控。宁可高估：
高估只会提前压缩（质量损失），低估才会超窗请求失败（正确性损失）。

***

## 4. 各层机制

### 4.1 L2 收缩：预算驱动的遮蔽

现状 `keepFull` 窗口是固定 6 条。改造为三档收缩：

```text
估算超 T_micro → keepFull 窗口 6 → 4 → 2 → 0，每收缩一档重估算
```

遮蔽产物不变（带 node_id 指针的占位符），只是"保留完整结果的窗口"
从静态配置变成预算的函数。收敛到 0 仍超，才进入 L3。

### 4.2 L3 微压缩：前缀节点批量摘要

对水位线之后的节点序列取**前缀**（保留最近 keepFull 个 TOOL 完整 +
最近若干轮 LLM 对话完整），将其余节点一次 LLM 调用批量摘要：

```json
{ "node-xxx": "搜索返回 3 条结果，确认了 Qdrant 集合命名规则……",
  "node-yyy": "读取 migrations/006，llm_usage 表结构……" }
```

产物持久化后，组装时**内联回填**：被摘要的节点在历史中不再展开原文，
而是替换为 `[Summary of node-xxx: …]`。逐节点保留结构的理由：
微压缩不合并语义（那是 L4 的事），只是把"大原文"换成"小转述"，
对话轮次与顺序完全不变，模型行为扰动最小。

### 4.3 L4 全量压缩：八段摘要 + 水位线推进

触发 `T_auto` 时，把水位线后的**全部历史**（含 LLM 对话与工具结果）
摘要为固定八段结构，水位线推进到当前最新节点：

```text
1 任务目标与约束      —— Run 要解决什么，用户给定的边界条件
2 关键决策与理由      —— 已做出的技术/业务选择
3 完成节点与产出指针  —— node_id 清单 + 一句话产出（供 fetch_tool_result 回溯）
4 工具调用概要        —— 用过哪些工具、目的、结果要点
5 错误与修复          —— 走过的弯路，避免重复踩坑
6 用户显式要求        —— 全部约束性指令的原意复述
7 待办与未决          —— 尚未收敛的事项
8 当前工作与下一步    —— 正在做什么、建议的下一个动作
```

组装布局随之改变：摘要以 system 消息占据历史位置——

```text
[记忆召回] → [八段摘要(system)] → [水位线后新增节点] → [当前节点输入]
```

八段模板是固定提示词 + 插槽（原文消息列表注入围栏），摘要调用本身：
走 `ModelProvider`（与主推理同一网关与凭证路径）、过 `ModelChain`
（摘要输入包含全部历史，泄露面与主调用同量级，没有理由豁免护栏）、
token 用量记入 `llm_usage`（node_id 归属触发压缩的节点）。

### 4.4 L5 硬截断：确定性保底

L3/L4 是 LLM 调用，可能超时、失败、产出劣质摘要（压缩后估算仍超窗）。
L5 是最后一道确定性防线：

- 从**头部**丢弃最老的消息，直到估算 ≤ `T_auto`；
- 八段摘要 / system 消息**永不丢弃**（它们就是为压缩而生的紧凑表示）；
- 丢弃处插入一条指针说明：`[Dropped N old messages; originals remain in agent_node rows run_id=…]`。

**压缩失败永远不让 Run 失败。** 原文都在 MySQL，截断损失的只是本次请求的信息量，
是质量问题而非一致性问题；反之，让 Run 因"上下文装不下"而入 DLQ，
才是把可自愈的缺陷升级成了需要人工介入的故障。

***

## 5. 持久化设计

### 5.1 表结构（落地为 `migrations/014_run_compaction.sql`）

```sql
CREATE TABLE run_compaction (
  id                VARCHAR(64)  NOT NULL,
  tenant_id         VARCHAR(64)  NOT NULL,
  run_id            VARCHAR(64)  NOT NULL,
  kind              VARCHAR(16)  NOT NULL,   -- 'micro' | 'full'
  waterline_node_id VARCHAR(64)  NOT NULL,   -- 覆盖到的最后一个节点（按 (finished_at,node_id) 序）
  summary           MEDIUMTEXT,              -- full：八段结构化摘要
  sections          JSON,                    -- micro：{node_id: 摘要} 映射
  estimated_tokens  INT          NOT NULL,   -- 压缩前估算，用于可观测与回归分析
  model             VARCHAR(128) NOT NULL,   -- 生成摘要的模型
  created_at        BIGINT       NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_waterline (tenant_id, run_id, kind, waterline_node_id),
  KEY idx_run (tenant_id, run_id, created_at)
);
```

微压缩与全量压缩统一为同一机制（前缀水位线）的两种产物形态：
`micro` 保留逐节点结构内联回填，`full` 收敛为单条摘要消息。
统一水位线语义使组装逻辑只有一条规则：**取 waterline 最大的记录，从其后取节点**。

### 5.2 并发与幂等

并行 DAG 分支的多个节点同时组装上下文、同时触发压缩，是常态而非竞态角界。
取舍：**先算后插、冲突丢弃**——

```text
1. 摘要生成（本地，无副作用）
2. INSERT run_compaction（唯一键 (tenant,run,kind,waterline)）
3. 冲突 → 丢弃本地结果，读取已存在记录使用
```

- 摘要无副作用、输入相同产出等价，浪费上界是一次并发竞态的重复 LLM 调用；
- 备选方案"先插 PENDING 占位 + 租约"能省这次重复调用，但要引入一套
  占位清理与崩溃恢复逻辑——为省一次调用引入一套状态机，复杂度不对等。
- Worker 崩溃在摘要后、插入前：无任何持久化痕迹，下次组装重新触发，幂等成立；
- 崩溃在插入后：记录已在，下次组装直接使用，`微压缩产物内联回填`天然恢复。

水位线只推进不回退：组装时按 `kind` 取该 Run `waterline` 语义序上最新的
`full` 记录为基座，`micro` 记录按 `waterline ≤ full 水位线` 的最大者叠加。

***

## 6. 与 PromptCache 的交互

`LLM_PROMPT_CACHE` 开启时，同一 Run 的 append-only 前缀命中 KV 缓存。
压缩**改写前缀**，缓存必然失效，这是压缩的固有代价。处理：

- cache key 从 `agent-run:{tenant}:{run}` 扩展为 `agent-run:{tenant}:{run}:g{N}`，
  `N` 为该 Run 的 full 压缩代数（无压缩时 `g0`，每次 L4 递增）；
- 代价语义清晰：压缩一次性付出"缓存冷启动"，换取之后每一跳的 prompt 显著变短；
- 微压缩（L3）内联回填同样破坏缓存前缀——因此 L3 的 sections 一经生成即持久化
  且不再变更（对同一 waterline 幂等），避免每次组装都产生新前缀。

***

## 7. 失败语义

| 层 | 失败模式 | 处置 | Run 影响 |
|---|---|---|---|
| L2 | 无外部依赖 | 不可失败 | — |
| L3 | LLM 超时/失败/输出不合法 JSON | 跳过微压缩，停留在 L2 收缩态，warn 日志 | 无 |
| L4 | 同上 | 降级 L5 硬截断，warn + `EventContextCompacted(fallback=l5)` | 无 |
| L4 后仍超窗 | 摘要劣质 / 窗口配置过小 | L5 硬截断 | 无 |
| L5 | 无外部依赖 | 不可失败 | 无 |
| 估算偏低 | 无实测校准点 | 超窗请求被网关拒 → 现有重试机制重跑该节点，此时已有实测值可校准 | 重试一次 |
| run_compaction 写冲突 | 并发压缩 | 丢弃本地结果用既有记录 | 无 |

原则：**任何压缩层故障都只降级、不阻断**；可观测信号（warn 日志 + 事件 +
span 属性 `compaction.layer` / `before_tokens` / `after_tokens` / `waterline_node_id`）
必须齐全，让"长期靠 L5 兜底"这种质量劣化可被监控发现，而不是静默存在。

新增 RuntimeEvent：`EventContextCompacted`，data 携带
`{kind, before_tokens, after_tokens, waterline_node_id, fallback}`——
与既有事件一样不携带任何原文（摘要内容进事件流只会扩大敏感面）。

***

## 8. 配置项

沿用 worker 的 env 装配风格（`newContextOptionsFromEnv` 扩展字段）：

```text
CONTEXT_COMPACTION_ENABLED   默认 false（新表依赖，P1 验证后转 true）
CONTEXT_COMPACTION_RATIO     默认 0.92（R_auto）
CONTEXT_MICRO_RATIO          默认 0.80（R_micro）
CONTEXT_OUTPUT_RESERVE       默认 8192
CONTEXT_MODEL_WINDOW         未知模型的默认窗口，默认 32768
CONTEXT_MODEL_WINDOW_<NAME>  按模型名覆盖，如 CONTEXT_MODEL_WINDOW_GPT4O=128000
CONTEXT_TOKEN_CHARS_PER      估算除数 K，默认 4
CONTEXT_SUMMARY_MODEL        摘要专用模型（可选，缺省复用主模型）
```

关闭开关的行为：整条管线旁路，`ContextLoader` 保持现状——
与祖先作用域、工具遮蔽的开关一样，**降级路径本身就是受支持的运行模式**。

***

## 9. ReAct 路径的边界说明

`react.Engine`（Phase 3 形态）的循环内消息是进程内的，不走 `ContextLoader`
重建，长循环同样会膨胀，但水位线机制对它**暂不生效**：

- Phase 3：ReAct 已接 L1/L2（`ToolMasking` + `capRunesForReact` + `fetch_tool_result`），够用；
- Phase 4（Roadmap：ReAct 决策持久化为 Durable Node）后，ReAct 的每一步
  都成为节点、每次 Think 都经 `ContextLoader` 组装，届时**同一套压缩管线
  无需任何修改自然覆盖**——这是把预算治理放在 `ContextLoader` 而非
  executor 某处的直接收益。

***

## 10. 分阶段落地

```text
P0  估算器 + 阈值模型 + L2 预算驱动收缩
    纯函数改造（nodesToMessages 收缩逻辑）+ 估算校准，无新表，单测可全量覆盖
P1  run_compaction 表 + L3/L4/L5 + 事件与 span
    摘要模板 v1（八段），失败降级链，默认开关仍为 false
P2  按工具分级的 L1 上限 / 摘要质量评估（对比压缩前后任务表现）
    / 窗口配置从 env 迁移到模型元数据 Provider
```

验收口径：构造长 Run（含大量超长工具输出），压缩开启后——
1. 任何节点不因上下文超窗失败；
2. 崩溃恢复（杀 Worker）后压缩产物不丢、不重复推进水位线；
3. `llm_usage` 中 prompt_tokens 在压缩节点处可见回落；
4. `fetch_tool_result` 仍可取回全部原文。
