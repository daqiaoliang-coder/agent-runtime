// 本文件实现上下文压缩管线（docs/context-compaction.md）：
//
//	L1 执行时截断        已有（context.go 的 capRunesWithPointer / react 的 capRunesForReact）
//	L2 预算驱动收缩      keepFull 窗口 6→4→2→0，每收缩一档重估算
//	L3 微压缩            水位线后前缀节点批量摘要，产物持久化后逐节点内联回填
//	L4 全量压缩          八段结构化摘要成为 system 基座，水位线推进
//	L5 硬截断兜底        确定性保底，压缩失败也绝不挂 Run
//
// 三条与 Claude Code 不同的根因（单进程 CLI 的"就地改写"在分布式持久 Runtime
// 里不成立）：压缩产物必须**持久化**（run_compaction 表），原文必须**不可变**
// （agent_node.output 是权威事实），Worker 必须无状态（崩溃后从水位线恢复）。
//
// 触发点在 ContextLoader 内部（newContextLoader）：executor 与 ReAct 两条路径
// 唯一的上下文组装点，因此不需要新增任何调用方改动。
//
// 失败语义总原则：任何压缩层故障都只降级、不阻断（L3 失败停留在 L2 收缩态、
// L4 失败降级 L5、L5 不可失败）。原文都在 MySQL，截断损失的只是本次请求的
// 信息量；反之，让 Run 因"上下文装不下"入 DLQ，是把可自愈缺陷升级成人工故障。
package worker

import (
	"agent-runtime/internal/contracts"
	"agent-runtime/internal/executor"
	"agent-runtime/internal/llm"
	"agent-runtime/internal/middleware"
	"agent-runtime/internal/model"
	"agent-runtime/internal/obs"
	"agent-runtime/internal/providers"
	"agent-runtime/internal/trace"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"go.opentelemetry.io/otel/attribute"
	oteltrace "go.opentelemetry.io/otel/trace"
)

// 压缩记录的 kind 值。micro 与 full 是同一水位线机制的两种产物形态。
const (
	CompactionKindMicro = "micro"
	CompactionKindFull  = "full"
)

// 管线默认参数（均可经环境变量覆盖，见 newCompactionOptionsFromEnv）。
const (
	// DefaultCompactionAutoRatio 全量压缩触发比例（R_auto）：估算占窗口比例超过它触发 L4。
	DefaultCompactionAutoRatio = 0.92
	// DefaultCompactionMicroRatio 早警线比例（R_micro）：超过它就开始低成本收缩，
	// 不要等到撞上危险线才动手。
	DefaultCompactionMicroRatio = 0.80
	// DefaultCompactionOutputReserve 为模型输出预留的窗口空间（输出也占窗口）。
	DefaultCompactionOutputReserve = 8192
	// DefaultCompactionModelWindow 未知模型的保守默认窗口。
	DefaultCompactionModelWindow = 32768
	// DefaultCompactionCharsPerToken 估算除数 K。宁高估：高估只是提前压缩（质量损失），
	// 低估才会超窗请求失败（正确性损失）。实测锚定（llm_usage）会让误差随 Run 推进收敛。
	DefaultCompactionCharsPerToken = 4
	// minCompactionThreshold 阈值下限：窗口配置过小时防止算出负/零预算让管线空转。
	minCompactionThreshold = 1024
	// summaryTimeout 限制单次摘要 LLM 调用。摘要失败只降级不阻断，不值得久等。
	summaryTimeout = 60 * time.Second
	// compactedSummaryPrefix 全量摘要作为 system 消息的前缀说明：
	// 让模型知道这是压缩基座而非用户指令。
	compactedSummaryPrefix = "The earlier part of this run has been compacted into the summary below. Treat it as authoritative context for the ongoing task.\n\n"
)

// CompactionOptions 是压缩管线的纯配置（可整体从环境变量装配）。
type CompactionOptions struct {
	// AutoRatio / MicroRatio 触发比例，取值 (0,1]，越界回退默认。
	AutoRatio  float64
	MicroRatio float64
	// OutputReserve 输出预留 token。
	OutputReserve int
	// ModelWindow 未知模型的默认窗口；ModelWindows 按模型名覆盖（键为大写化处理后的名字）。
	ModelWindow  int
	ModelWindows map[string]int
	// CharsPerToken 估算除数 K。
	CharsPerToken int
	// SummaryModel 摘要专用模型；空则复用主模型。
	SummaryModel string
}

func (o *CompactionOptions) autoRatio() float64 {
	if o.AutoRatio <= 0 || o.AutoRatio > 1 {
		return DefaultCompactionAutoRatio
	}
	return o.AutoRatio
}

func (o *CompactionOptions) microRatio() float64 {
	if o.MicroRatio <= 0 || o.MicroRatio > 1 {
		return DefaultCompactionMicroRatio
	}
	return o.MicroRatio
}

func (o *CompactionOptions) outputReserve() int {
	if o.OutputReserve <= 0 {
		return DefaultCompactionOutputReserve
	}
	return o.OutputReserve
}

func (o *CompactionOptions) modelWindow() int {
	if o.ModelWindow <= 0 {
		return DefaultCompactionModelWindow
	}
	return o.ModelWindow
}

func (o *CompactionOptions) charsPerToken() int {
	if o.CharsPerToken <= 0 {
		return DefaultCompactionCharsPerToken
	}
	return o.CharsPerToken
}

// modelWindowKey 把模型名归一为环境变量键后缀：分隔符直接删除（gpt-4o → GPT4O）。
// 与 modelWindowsFromEnv 的解析规则互逆，一处改动必须同步另一处。
func modelWindowKey(model string) string {
	r := strings.NewReplacer("-", "", ".", "", " ", "_")
	return strings.ToUpper(r.Replace(model))
}

// windowFor 返回模型窗口：按名覆盖优先，未知模型用保守默认。
func (o *CompactionOptions) windowFor(model string) int {
	if model != "" && len(o.ModelWindows) > 0 {
		if w, ok := o.ModelWindows[modelWindowKey(model)]; ok && w > 0 {
			return w
		}
	}
	return o.modelWindow()
}

// thresholds 按模型窗口计算两级触发线：T = floor(window × ratio) − reserve。
// micro 不得高于 auto（配置写反时钳到危险线，宁可早点进入全量压缩）。
func (c *Compactor) thresholds(model string) (tMicro, tAuto int) {
	w := c.Opt.windowFor(model)
	tAuto = int(float64(w)*c.Opt.autoRatio()) - c.Opt.outputReserve()
	tMicro = int(float64(w)*c.Opt.microRatio()) - c.Opt.outputReserve()
	if tAuto < minCompactionThreshold {
		tAuto = minCompactionThreshold
	}
	if tMicro > tAuto {
		tMicro = tAuto
	}
	if tMicro < minCompactionThreshold {
		tMicro = minCompactionThreshold
	}
	return tMicro, tAuto
}

// compactionStore 是压缩管线所需的持久化能力（*store.MySQL 天然实现）。
// 独立于 contextStore 声明，让纯拼接单测的 fake 不必实现压缩方法。
type compactionStore interface {
	LastLLMPromptUsage(ctx context.Context, tenant, runID string) (model.LLMUsage, bool, error)
	LatestCompaction(ctx context.Context, tenant, runID, kind string) (*model.RunCompaction, error)
	InsertCompaction(ctx context.Context, rec *model.RunCompaction) (bool, error)
	CountCompactions(ctx context.Context, tenant, runID, kind string) (int, error)
}

// Compactor 在 ContextLoader 内执行预算管线。
//
// Model/ModelChain/Usage/Pricer/Events 均可缺省：Model 为 nil 时 L3/L4 直接
// 降级（L2/L5 仍可用，纯确定性），Events 为 nil 时日志是唯一的可观测信号
// （当前 cmd/worker 尚无 RuntimeEvent 出口，日志必须能独立支撑排查）。
type Compactor struct {
	Store      compactionStore
	Model      providers.ModelProvider
	ModelChain *middleware.ModelChain
	Usage      executor.UsageRecorder
	Pricer     executor.Pricer
	Events     eventSink
	Opt        CompactionOptions
}

// eventSink 与 event.Sink 同形；单独声明避免 worker 编译依赖 RocketMQ producer。
// emit 的实现保证 nil 安全。
type eventSink interface {
	Emit(context.Context, contracts.RuntimeEvent) error
}

// compactionID 由 (run,kind,waterline) 哈希派生：与唯一键一一对应，
// 同一水位线的重复压缩天然幂等（并发场景"先算后插、冲突丢弃"的落点）。
func compactionID(runID, kind, waterline string) string {
	h := sha256.Sum256([]byte(runID + "|" + kind + "|" + waterline))
	return hex.EncodeToString(h[:])
}

// Apply 是管线的唯一入口：输入全部已完成祖先节点与跨 Run 记忆召回，
// 输出预算内的最终消息列表。任何内部错误都只降级、绝不返回 error。
//
// 分工：水位线查询/过滤、组装（buildCur）、估算（estimate）都在本函数内
// 闭包化，因为它们共享 active/keep/sections/full 这组随管线推进而变化的状态。
func (c *Compactor) Apply(ctx context.Context, ec contracts.ExecutionContext, nodes []model.Node, recalled []llm.Message, opt ContextOptions) []llm.Message {
	ctx, span := trace.StartSpan(ctx, "context.compaction")
	defer span.End()

	// 压缩记录读取失败按"无基座"降级：组装回退为全量原文——fail-open 到
	// 信息更多的一侧，超窗交给 L5 兜底，绝不如"读不到摘要就丢历史"。
	full := c.latest(ctx, ec, CompactionKindFull)
	micro := c.latest(ctx, ec, CompactionKindMicro)

	// 回填映射：有 micro 记录就整体携带，命中才生效。浅于 full 水位线的
	// 旧条目对应的节点已被 afterWaterline 过滤，不会命中，无需显式剔除。
	sections := map[string]string{}
	if micro != nil {
		sections = micro.Sections
	}

	active := afterWaterline(nodes, waterlineOf(full))

	// 实测锚点：窗口按模型自适应也依赖它（最近一次主推理的模型即本次模型，
	// 同 Run 内通常一致；锚点不可用时按默认窗口 + 全量估算，偏保守）。
	anchor, hasAnchor := c.usageAnchor(ctx, ec)
	anchorModel := ""
	if hasAnchor {
		anchorModel = anchor.Model
	}
	tMicro, tAuto := c.thresholds(anchorModel)

	keep := opt.toolMaskWindow()
	runesRecalled := messagesRunes(recalled)

	// buildCur 组装 DAG 视图：[全量摘要(system)] → [水位线后节点展开]。
	// finalize 叠加记忆召回（与现状 newContextLoader 的记忆路径一致）。
	buildCur := func(keep int, secs map[string]string) []llm.Message {
		cur := nodesToMessages(active, opt, keep, secs)
		if full != nil {
			cur = append([]llm.Message{{Role: llm.RoleSystem, Content: compactedSummaryPrefix + full.Summary}}, cur...)
		}
		return cur
	}
	finalize := func(cur []llm.Message) []llm.Message {
		if opt.Memory.Memory == nil {
			return cur
		}
		return mergeMessages(recalled, cur, opt.Memory)
	}

	// estimate 实测锚定 + 增量估算（docs §3.3）：
	//   base = 最近一次主推理实测 prompt_tokens
	//   delta = 自锚点节点之后新增内容的字符数 ÷ K
	// 锚点节点不在 active（被水位线覆盖/未识别）时退化为全量估算——
	// 锚点失效意味着上次实测与当前视图之间隔了一次压缩，比例校准不再可信，
	// 全量 ÷K 偏高估，方向安全。prev 里带上当前 system 与 recalled：
	// 锚点在 active 中 ⇒ 上次请求已含同一条 system（水位线未推进）与近似
	// 等量的记忆召回，二者相消，delta 只剩真正的新增节点。
	estimate := func(cur []llm.Message, act []model.Node, keep int, secs map[string]string) int {
		curRunes := messagesRunes(cur) + runesRecalled
		if !hasAnchor {
			return curRunes / c.Opt.charsPerToken()
		}
		idx := indexOfNode(act, anchor.NodeID)
		if idx < 0 {
			return curRunes / c.Opt.charsPerToken()
		}
		var prev []llm.Message
		if full != nil {
			prev = append(prev, llm.Message{Role: llm.RoleSystem, Content: compactedSummaryPrefix + full.Summary})
		}
		prev = append(prev, nodesToMessages(act[:idx+1], opt, keep, secs)...)
		prevRunes := messagesRunes(prev) + runesRecalled
		delta := (curRunes - prevRunes) / c.Opt.charsPerToken()
		if delta < 0 {
			delta = 0
		}
		return anchor.PromptTokens + delta
	}

	cur := buildCur(keep, sections)
	msgs := finalize(cur)
	est := estimate(cur, active, keep, sections)
	before := est

	// finish 统一出口：span 属性 + 观测信号，layer 标记最后生效的一层。
	finish := func(layer string, after int, msgs []llm.Message) []llm.Message {
		span.SetAttributes(
			attribute.String("compaction.layer", layer),
			attribute.Int("compaction.before_tokens", before),
			attribute.Int("compaction.after_tokens", after),
		)
		if after > tMicro {
			// 长期靠兜底截断是质量劣化，必须可被监控发现而不是静默存在。
			obs.From(ctx).WarnContext(ctx, "context over early-warning threshold after compaction",
				"run_id", ec.RunID, "node_id", ec.NodeID, "layer", layer,
				"before_tokens", before, "after_tokens", after, "t_micro", tMicro, "t_auto", tAuto)
		}
		return msgs
	}

	if est <= tMicro {
		return finish("none", est, msgs)
	}

	// ---- L2 预算驱动收缩：keepFull 窗口逐档收窄，纯确定性、零成本 ----
	for keep > 0 && est > tMicro {
		keep = shrinkKeep(keep)
		cur = buildCur(keep, sections)
		msgs = finalize(cur)
		est = estimate(cur, active, keep, sections)
	}
	if est <= tMicro {
		return finish("l2_shrink", est, msgs)
	}

	// ---- L3 微压缩：前缀节点批量摘要，持久化后内联回填 ----
	if newSecs, ok := c.tryMicro(ctx, span, ec, active, sections, keep, est); ok {
		sections = newSecs
		cur = buildCur(keep, sections)
		msgs = finalize(cur)
		est = estimate(cur, active, keep, sections)
		if est <= tAuto {
			return finish("l3_micro", est, msgs)
		}
	}

	// ---- L4 全量压缩：八段摘要成为基座，水位线推进 ----
	if est > tAuto {
		if newFull, ok := c.tryFull(ctx, span, ec, cur, active, est); ok {
			full = newFull
			active = afterWaterline(nodes, newFull.WaterlineNodeID)
			sections = map[string]string{} // 旧回填已被新基座吸收
			keep = opt.toolMaskWindow()
			cur = buildCur(keep, sections)
			msgs = finalize(cur)
			est = estimate(cur, active, keep, sections)
			if est <= tAuto {
				return finish("l4_full", est, msgs)
			}
		}
	}

	// ---- L5 硬截断：确定性保底，压缩失败也绝不挂 Run ----
	msgs = hardTruncate(msgs, tAuto, c.Opt.charsPerToken(), ec.RunID)
	// 截断后改用全量估算而非 estimate：msgs 已含 recalled（finalize 已合并），
	// 再走 estimate 会叠加 runesRecalled 双计；且截断改写了前缀，锚点增量口径
	// （append-only 假设）不再成立，全量 ÷K 才是对最终请求的真实刻画。
	after := messagesRunes(msgs) / c.Opt.charsPerToken()
	return finish("l5_truncate", after, msgs)
}

// latest 读取指定 kind 的最新压缩记录；读取失败按无记录降级（warn 日志）。
func (c *Compactor) latest(ctx context.Context, ec contracts.ExecutionContext, kind string) *model.RunCompaction {
	rec, err := c.Store.LatestCompaction(ctx, ec.TenantID, ec.RunID, kind)
	if err != nil {
		obs.From(ctx).WarnContext(ctx, "load compaction record failed; treating as none",
			"run_id", ec.RunID, "kind", kind, "error", err)
		return nil
	}
	return rec
}

func waterlineOf(rec *model.RunCompaction) string {
	if rec == nil {
		return ""
	}
	return rec.WaterlineNodeID
}

// usageAnchor 读取最近一次主推理用量作为估算锚点；读取失败按无锚点降级。
func (c *Compactor) usageAnchor(ctx context.Context, ec contracts.ExecutionContext) (model.LLMUsage, bool) {
	u, ok, err := c.Store.LastLLMPromptUsage(ctx, ec.TenantID, ec.RunID)
	if err != nil {
		obs.From(ctx).WarnContext(ctx, "load usage anchor failed; falling back to full estimation",
			"run_id", ec.RunID, "error", err)
		return model.LLMUsage{}, false
	}
	return u, ok
}

// ---- L3 微压缩 ----

// tryMicro 对水位线后、保留区之前的前缀节点做一次批量摘要，持久化后返回
// 继承合并的回填映射。任何失败都返回 (_, false)：管线停留在 L2 收缩态。
func (c *Compactor) tryMicro(ctx context.Context, span oteltrace.Span, ec contracts.ExecutionContext, active []model.Node, inherited map[string]string, keep int, estBefore int) (map[string]string, bool) {
	prefix := microPrefix(active, inherited, keep)
	if len(prefix) == 0 {
		return nil, false // 保留区已经覆盖到头部：没有可摘要的前缀
	}
	waterline := prefix[len(prefix)-1].ID

	var b strings.Builder
	b.WriteString("You are a context compactor. Summarize each completed agent step below in one sentence (max ~120 characters). Keep: what was done, the key result or conclusion, and any pointers later steps need (file names, IDs, numbers). Respond with ONLY a JSON object mapping node_id to its summary. Cover every node, do not omit any, no extra text.\n<nodes>\n")
	for _, n := range prefix {
		fmt.Fprintf(&b, "[%s] %s %s\ninput: %s\noutput: %s\n\n", n.ID, n.Type, n.Name, n.Input, capForSummary(n.Output))
	}
	b.WriteString("</nodes>")

	content, err := c.generate(ctx, ec, b.String())
	if err != nil {
		obs.From(ctx).WarnContext(ctx, "micro-compaction skipped (summary generation failed)",
			"run_id", ec.RunID, "node_id", ec.NodeID, "error", err)
		return nil, false
	}
	parsed, err := parseSummaryMap(content)
	if err != nil {
		obs.From(ctx).WarnContext(ctx, "micro-compaction skipped (unparseable summary)",
			"run_id", ec.RunID, "node_id", ec.NodeID, "error", err)
		return nil, false
	}
	// 宽容缺失：模型漏掉的节点补短占位，继续比整体失败好（缺一个节点的
	// 转述远好于整段前缀继续以原文膨胀）。
	for _, n := range prefix {
		if _, ok := parsed[n.ID]; !ok {
			parsed[n.ID] = "(summary unavailable)"
		}
	}
	// 继承旧条目：旧记录的水位线语义被新记录整体接管，读取端永远只看
	// 最新一条（LatestCompaction），不叠加多条——回填规则只有一条。
	merged := make(map[string]string, len(inherited)+len(parsed))
	for k, v := range inherited {
		merged[k] = v
	}
	for k, v := range parsed {
		merged[k] = v
	}

	rec := &model.RunCompaction{
		ID:              compactionID(ec.RunID, CompactionKindMicro, waterline),
		TenantID:        ec.TenantID,
		RunID:           ec.RunID,
		Kind:            CompactionKindMicro,
		WaterlineNodeID: waterline,
		Sections:        merged,
		EstimatedTokens: estBefore,
		Model:           c.Opt.SummaryModel,
	}
	inserted, err := c.Store.InsertCompaction(ctx, rec)
	if err != nil {
		obs.From(ctx).WarnContext(ctx, "micro-compaction skipped (persist failed)",
			"run_id", ec.RunID, "waterline", waterline, "error", err)
		return nil, false
	}
	if !inserted {
		// 并发方先插入成功（先算后插、冲突丢弃）：复用已存在记录。
		// 摘要无副作用、输入相同、产出等价，丢弃本地结果没有信息损失。
		existing, lerr := c.Store.LatestCompaction(ctx, ec.TenantID, ec.RunID, CompactionKindMicro)
		if lerr != nil || existing == nil {
			return nil, false
		}
		merged = existing.Sections
		waterline = existing.WaterlineNodeID
	}
	span.SetAttributes(attribute.String("compaction.micro_waterline", waterline))
	c.emit(ctx, ec, map[string]any{
		"kind": CompactionKindMicro, "before_tokens": estBefore,
		"waterline_node_id": waterline, "fallback": false,
	})
	return merged, true
}

// microPrefix 选择微压缩的前缀：保留区 = 最后 keep 个 TOOL 节点中最早者
// 之后的全部节点（含其间与之后的 LLM 轮次）；前缀 = 保留区之前、尚未被
// 回填映射覆盖的节点（重复摘要已覆盖节点没有增益，只浪费 LLM 预算）。
// keep==0（L2 收缩终点档）时保留区为空，全部节点都可进入摘要——此时若仍
// 超阈值，能压缩的只剩"把整段 active 换成转述"这一条路。
func microPrefix(active []model.Node, sections map[string]string, keep int) []model.Node {
	var toolIdx []int
	for i, n := range active {
		if n.Type == model.NodeTool {
			toolIdx = append(toolIdx, i)
		}
	}
	// keep==0 起点设为 len(active)（前缀=全部）；keep>0 时工具数不足窗口则无前缀。
	suffixStart := len(active)
	if keep > 0 {
		suffixStart = 0
		if len(toolIdx) >= keep {
			suffixStart = toolIdx[len(toolIdx)-keep]
		}
	}
	var prefix []model.Node
	for i := 0; i < suffixStart; i++ {
		if _, done := sections[active[i].ID]; done {
			continue
		}
		prefix = append(prefix, active[i])
	}
	return prefix
}

// capForSummary 控制单个节点进入摘要 prompt 的输出规模。
// 摘要输入本身就是为省 token 而生，原文细节的边际价值远低于其体积。
const summaryOutputMaxRunes = 2000

func capForSummary(s string) string {
	if utf8.RuneCountInString(s) <= summaryOutputMaxRunes {
		return s
	}
	runes := []rune(s)
	return string(runes[:summaryOutputMaxRunes]) + "…[truncated]"
}

// parseSummaryMap 解析 L3 输出的 {node_id: 摘要} JSON。
// 复用 executor.extractJSON 的围栏剥离思路，但那是 executor 包的私有实现，
// 摘要解析的容错口径也不同（这里要求是纯对象），故独立实现。
func parseSummaryMap(content string) (map[string]string, error) {
	raw := strings.TrimSpace(content)
	if start := strings.Index(raw, "{"); start >= 0 {
		if end := strings.LastIndex(raw, "}"); end > start {
			raw = raw[start : end+1]
		}
	}
	var m map[string]string
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		return nil, fmt.Errorf("parse summary map: %w", err)
	}
	if len(m) == 0 {
		return nil, errors.New("empty summary map")
	}
	return m, nil
}

// ---- L4 全量压缩 ----

// tryFull 把当前 DAG 视图摘要为固定八段结构，水位线推进到最新已完成节点。
// 摘要输入是 cur（system 基座 + 节点展开 + 回填）而不含跨 Run 记忆召回：
// recalled 每次组装都会重新前置，若被摘要吸收会在下一代上下文中重复出现。
func (c *Compactor) tryFull(ctx context.Context, span oteltrace.Span, ec contracts.ExecutionContext, cur []llm.Message, active []model.Node, estBefore int) (*model.RunCompaction, bool) {
	if len(active) == 0 {
		// 无节点可覆盖：膨胀来自记忆召回或旧摘要自身，L4 无从下手，交给 L5。
		return nil, false
	}
	waterline := active[len(active)-1].ID

	var b strings.Builder
	b.WriteString(`You are a context compactor. Compress the agent conversation history below into a structured summary that will serve as the ONLY context base for the remaining execution. Preserve all information needed to continue the task.

Respond with exactly these eight numbered sections, keeping the headings, content dense but complete:
1 任务目标与约束
2 关键决策与理由
3 完成节点与产出指针（node_id + 一句话产出）
4 工具调用概要
5 错误与修复
6 用户显式要求
7 待办与未决
8 当前工作与下一步

<history>
`)
	for _, m := range cur {
		fmt.Fprintf(&b, "%s: %s\n", m.Role, m.Content)
	}
	b.WriteString("</history>")

	summary, err := c.generate(ctx, ec, b.String())
	if err != nil {
		obs.From(ctx).WarnContext(ctx, "full compaction skipped (summary generation failed)",
			"run_id", ec.RunID, "node_id", ec.NodeID, "error", err)
		return nil, false
	}
	summary = strings.TrimSpace(summary)
	if summary == "" {
		obs.From(ctx).WarnContext(ctx, "full compaction skipped (empty summary)",
			"run_id", ec.RunID, "node_id", ec.NodeID)
		return nil, false
	}

	rec := &model.RunCompaction{
		ID:              compactionID(ec.RunID, CompactionKindFull, waterline),
		TenantID:        ec.TenantID,
		RunID:           ec.RunID,
		Kind:            CompactionKindFull,
		WaterlineNodeID: waterline,
		Summary:         summary,
		EstimatedTokens: estBefore,
		Model:           c.Opt.SummaryModel,
	}
	inserted, err := c.Store.InsertCompaction(ctx, rec)
	if err != nil {
		obs.From(ctx).WarnContext(ctx, "full compaction skipped (persist failed)",
			"run_id", ec.RunID, "waterline", waterline, "error", err)
		return nil, false
	}
	if !inserted {
		existing, lerr := c.Store.LatestCompaction(ctx, ec.TenantID, ec.RunID, CompactionKindFull)
		if lerr != nil || existing == nil {
			return nil, false
		}
		rec = existing
	}
	span.SetAttributes(attribute.String("compaction.full_waterline", rec.WaterlineNodeID))
	c.emit(ctx, ec, map[string]any{
		"kind": CompactionKindFull, "before_tokens": estBefore,
		"waterline_node_id": rec.WaterlineNodeID, "fallback": false,
	})
	return rec, true
}

// ---- 摘要生成 ----

// generate 发起一次摘要调用：与主推理同一网关与凭证路径（ModelProvider），
// 过 ModelChain 护栏（摘要输入包含全部历史，泄露面与主调用同量级，
// 没有理由豁免），token 用量记入 llm_usage（usage-compact- 前缀供实测
// 锚点排除，见 store.LastLLMPromptUsage）。
func (c *Compactor) generate(ctx context.Context, ec contracts.ExecutionContext, prompt string) (string, error) {
	if c.Model == nil {
		return "", errors.New("compactor: model provider not configured")
	}
	sctx, cancel := context.WithTimeout(ctx, summaryTimeout)
	defer cancel()
	req := contracts.GenerateRequest{
		Model:   c.Opt.SummaryModel,
		Messages: []contracts.Message{{Role: contracts.RoleUser, Content: prompt}},
	}
	if c.ModelChain != nil {
		var err error
		req, err = c.ModelChain.Before(sctx, ec, req)
		if err != nil {
			// 护栏拦截摘要请求（含 ErrAwaitingApproval/ErrBlocked）按压缩失败降级：
			// 摘要是增强路径，把审批语义引入它会让人工闸门多一类无关负载。
			return "", fmt.Errorf("model chain before: %w", err)
		}
	}
	resp, err := c.Model.Generate(sctx, req)
	if err != nil {
		return "", fmt.Errorf("generate summary: %w", err)
	}
	if c.ModelChain != nil {
		resp, err = c.ModelChain.After(sctx, ec, req, resp)
		if err != nil {
			return "", fmt.Errorf("model chain after: %w", err)
		}
	}
	if c.Usage != nil && resp.Usage.TotalTokens > 0 {
		var cost float64
		if c.Pricer != nil {
			cost = c.Pricer(resp.Model, resp.Usage.PromptTokens, resp.Usage.CompletionTokens)
		}
		if uerr := c.Usage.RecordLLMUsage(sctx, model.LLMUsage{
			ID:              fmt.Sprintf("usage-compact-%d", time.Now().UnixNano()),
			RunID:           ec.RunID,
			NodeID:          ec.NodeID,
			TenantID:        ec.TenantID,
			Model:           resp.Model,
			PromptTokens:    resp.Usage.PromptTokens,
			CompletionTokens: resp.Usage.CompletionTokens,
			TotalTokens:     resp.Usage.TotalTokens,
			Cost:            cost,
		}); uerr != nil {
			// 记账失败不影响摘要产出，但必须可见：锚点长期缺失会让估算退化为全量。
			obs.From(sctx).WarnContext(sctx, "record summary usage failed",
				"run_id", ec.RunID, "node_id", ec.NodeID, "error", uerr)
		}
	}
	return resp.Message.Content, nil
}

// ---- L5 硬截断 ----

// hardTruncate 从头部丢弃最老的消息直到估算 ≤ budget token。
// system 消息（全量摘要基座）永不丢弃——它就是为压缩而生的紧凑表示，
// 丢它等于让 L4 白做；指针说明插在保留段头部，模型仍知道中间缺失及其规模。
// 与 trimToTokenBudget 同构（尾部向前吸收、放不下的单条跳过），但多了
// system 保留与丢弃指针两个语义。
func hardTruncate(msgs []llm.Message, budgetTokens, charsPerToken int, runID string) []llm.Message {
	if budgetTokens <= 0 {
		budgetTokens = minCompactionThreshold
	}
	if charsPerToken <= 0 {
		charsPerToken = DefaultCompactionCharsPerToken
	}
	ptr := fmt.Sprintf("[Dropped %d old message(s); originals remain in agent_node rows run_id=%q.]", 0, runID)
	ptrCost := (utf8.RuneCountInString(ptr) + charsPerToken - 1) / charsPerToken
	budget := budgetTokens - ptrCost
	if budget < 0 {
		budget = 0
	}

	kept := make([]llm.Message, 0, len(msgs))
	used := 0
	dropped := 0
	for i := len(msgs) - 1; i >= 0; i-- {
		cost := (utf8.RuneCountInString(msgs[i].Content) + charsPerToken - 1) / charsPerToken
		if msgs[i].Role == llm.RoleSystem {
			kept = append(kept, msgs[i])
			used += cost // 保留但计费：预算被 system 挤占时其余消息更快触底
			continue
		}
		if used+cost > budget {
			dropped++
			continue
		}
		kept = append(kept, msgs[i])
		used += cost
	}
	// 反转回正序（上面是从尾部向前收集）。
	for i, j := 0, len(kept)-1; i < j; i, j = i+1, j-1 {
		kept[i], kept[j] = kept[j], kept[i]
	}
	if dropped > 0 {
		ptr = fmt.Sprintf("[Dropped %d old message(s); originals remain in agent_node rows run_id=%q.]", dropped, runID)
		kept = append([]llm.Message{{Role: llm.RoleUser, Content: ptr}}, kept...)
	}
	return kept
}

// ---- 纯函数辅助 ----

// afterWaterline 返回节点序列中位于水位线之后（不含）的部分。
// 节点序为 (finished_at, node_id) 确定序，水位线是历史快照上的最后一个节点。
// 找不到水位线节点（异常数据/清理策略回收）时回退为全量原文：
// fail-open 到信息更多的一侧，超窗交给 L5 兜底。
func afterWaterline(nodes []model.Node, waterline string) []model.Node {
	if waterline == "" {
		return nodes
	}
	for i, n := range nodes {
		if n.ID == waterline {
			return nodes[i+1:]
		}
	}
	return nodes
}

func indexOfNode(nodes []model.Node, id string) int {
	for i, n := range nodes {
		if n.ID == id {
			return i
		}
	}
	return -1
}

// shrinkKeep 收缩一档保留窗口：>4 → 4，>2 → 2，其余 → 0。
// 档位刻意稀疏：收缩的收益来自"遮蔽整段工具结果"，半档调整没有意义。
func shrinkKeep(keep int) int {
	switch {
	case keep > 4:
		return 4
	case keep > 2:
		return 2
	default:
		return 0
	}
}

// messagesRunes 返回消息列表的 rune 总数（估算口径的统一入口）。
func messagesRunes(msgs []llm.Message) int {
	total := 0
	for _, m := range msgs {
		total += utf8.RuneCountInString(m.Content)
	}
	return total
}

// ---- 观测 ----

// emit 发射 EventContextCompacted。Data 只带量级与指针、绝不带摘要原文：
// 事件会进 SSE 与日志系统，摘要里是全量历史的转述，抄进去等于二次扩散。
func (c *Compactor) emit(ctx context.Context, ec contracts.ExecutionContext, data map[string]any) {
	obs.From(ctx).InfoContext(ctx, "context compacted",
		"run_id", ec.RunID, "node_id", ec.NodeID,
		"kind", fmt.Sprint(data["kind"]), "waterline_node_id", fmt.Sprint(data["waterline_node_id"]))
	if c.Events == nil {
		return
	}
	_ = c.Events.Emit(ctx, contracts.RuntimeEvent{
		ID:        fmt.Sprintf("runtime-event-compacted-%d", time.Now().UnixNano()),
		RunID:     ec.RunID,
		NodeID:    ec.NodeID,
		TenantID:  ec.TenantID,
		Type:      contracts.EventContextCompacted,
		Timestamp: time.Now(),
		Data:      data,
	})
}

// CacheGen 返回 prompt cache key 的代数后缀（docs §6）：全量压缩改写前缀，
// 缓存必然失效，key 按代数换代。查询失败退 g0：代价只是缓存不命中。
func (c *Compactor) CacheGen(ctx context.Context, tenant, runID string) string {
	n, err := c.Store.CountCompactions(ctx, tenant, runID, CompactionKindFull)
	if err != nil || n < 0 {
		n = 0
	}
	return ":g" + strconv.Itoa(n)
}

// ---- 环境变量装配 ----

// newCompactionOptionsFromEnv 装配压缩配置；未启用（默认）返回 nil，
// ContextLoader 整条管线旁路、行为与接入前完全一致——降级路径本身
// 就是受支持的运行模式。
func newCompactionOptionsFromEnv() *CompactionOptions {
	if !envBool("CONTEXT_COMPACTION_ENABLED", false) {
		return nil
	}
	return &CompactionOptions{
		AutoRatio:     envFloat64("CONTEXT_COMPACTION_RATIO", DefaultCompactionAutoRatio),
		MicroRatio:    envFloat64("CONTEXT_MICRO_RATIO", DefaultCompactionMicroRatio),
		OutputReserve: envInt("CONTEXT_OUTPUT_RESERVE", DefaultCompactionOutputReserve),
		ModelWindow:   envInt("CONTEXT_MODEL_WINDOW", DefaultCompactionModelWindow),
		ModelWindows:  modelWindowsFromEnv(),
		CharsPerToken: envInt("CONTEXT_TOKEN_CHARS_PER", DefaultCompactionCharsPerToken),
		SummaryModel:  envString("CONTEXT_SUMMARY_MODEL", ""),
	}
}

// modelWindowsFromEnv 解析按模型名覆盖的窗口配置：CONTEXT_MODEL_WINDOW_<NAME>。
// 键名规则与 modelWindowKey 互逆（gpt-4o ↔ GPT4O）。
func modelWindowsFromEnv() map[string]int {
	var out map[string]int
	const prefix = "CONTEXT_MODEL_WINDOW_"
	for _, kv := range os.Environ() {
		eq := strings.IndexByte(kv, '=')
		if eq <= len(prefix) {
			continue
		}
		key := kv[:eq]
		if !strings.HasPrefix(key, prefix) || key == "CONTEXT_MODEL_WINDOW" {
			continue
		}
		w, err := strconv.Atoi(kv[eq+1:])
		if err != nil || w <= 0 {
			continue
		}
		if out == nil {
			out = make(map[string]int)
		}
		out[strings.TrimPrefix(key, prefix)] = w
	}
	return out
}

func envFloat64(key string, def float64) float64 {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		log.Printf("worker: invalid float for %s=%q, using default %v", key, v, def)
		return def
	}
	return f
}
