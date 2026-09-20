// 本文件实现记忆的**读取路径**：把跨 Run 的语义记忆注入 LLM 的对话上下文。
//
// 接入点选在 executor.Dispatcher.ContextLoader（而非改 executor 本身），
// 因为该闭包在 executeLLM / executeReflect / StreamLLM 三处被调用，
// 改一处即可让三者同时受益，且 Dispatcher.ContextLoader 的签名保持不变。
//
// 最重要的约束：**记忆是增强项，不是依赖项**。
// 任何记忆相关故障（向量库宕机、embedding 网关限流、超时）都只降级为
// "本次没有召回到长期记忆"，绝不让节点执行失败。
package worker

import (
	"agent-runtime/internal/contracts"
	"agent-runtime/internal/llm"
	"agent-runtime/internal/providers"
	"context"
	"log"
	"os"
	"strconv"
	"time"
)

// 读取路径的默认参数。
const (
	// DefaultMemorySearchTimeout 限定单次语义召回的总耗时（含 embedding + 向量检索）。
	//
	// 刻意只包裹记忆召回、**不包裹** CompletedNodes：后者是主链路必需的数据，
	// 给它加短超时会让"DB 稍慢"直接变成节点执行失败。
	// 800ms 是在"能等到网关返回"与"不显著拖慢节点启动"之间的折中。
	DefaultMemorySearchTimeout = 800 * time.Millisecond
	// DefaultMaxMemoryMessages 拼接后交给 LLM 的消息条数上限。
	// 这是上下文膨胀与 token 预算的闸门：记忆召回越多，每次推理越贵。
	// P1 后 MaxContextTokens 作为主要预算闸门，MaxMessages 退为条数硬上限（向后兼容）。
	DefaultMaxMemoryMessages = 20
	// DefaultMaxContextTokens 上下文消息列表的 token 预算上限（启发式估算）。
	// 先保证当前 Run 历史，再用剩余预算从最新向前吸收跨 Run 记忆。
	DefaultMaxContextTokens = 32000
)

// MemoryOptions 描述读取路径的记忆装配。
//
// Memory 为 nil 表示未启用长期记忆，此时 ContextLoader 的行为与接入向量检索之前
// **完全一致**——这保证了未配置向量库的部署与现有测试零感知。
type MemoryOptions struct {
	Memory        providers.MemoryProvider
	SearchTimeout time.Duration
	MaxMessages   int
	// MaxContextTokens 为上下文消息列表的 token 预算上限（0=用默认）。
	// mergeMessages 先保证当前 Run 历史的完整性，再用剩余预算从最新向前吸收记忆。
	MaxContextTokens int
}

// searchTimeout 返回生效的召回超时，未配置或非法值回退到默认值。
func (o MemoryOptions) searchTimeout() time.Duration {
	if o.SearchTimeout <= 0 {
		return DefaultMemorySearchTimeout
	}
	return o.SearchTimeout
}

// maxMessages 返回生效的消息条数上限。
func (o MemoryOptions) maxMessages() int {
	if o.MaxMessages <= 0 {
		return DefaultMaxMemoryMessages
	}
	return o.MaxMessages
}

// maxContextTokens 返回生效的 token 预算上限。
func (o MemoryOptions) maxContextTokens() int {
	if o.MaxContextTokens <= 0 {
		return DefaultMaxContextTokens
	}
	return o.MaxContextTokens
}

// estimateTokens 启发式估算字符串的 token 数。
// ASCII ~3 字符/token，CJK ~2 字符/token，+1 消息开销。
// 精度约 ±30%，作为软预算闸门足够；精确计数需 tokenizer，会耦合到具体模型族。
func estimateTokens(s string) int {
	if len(s) == 0 {
		return 0
	}
	ascii, cjk := 0, 0
	for _, r := range s {
		switch {
		case r >= 0x4E00 && r <= 0x9FFF, r >= 0x3400 && r <= 0x4DBF:
			cjk++
		default:
			ascii++
		}
	}
	return (ascii+2)/3 + cjk/2 + 1
}

func estimateMessagesTokens(msgs []llm.Message) int {
	total := 0
	for _, m := range msgs {
		total += estimateTokens(m.Content)
	}
	return total
}

// ContextLoader 的组装与上下文塑形（祖先作用域、工具结果遮蔽）见 context.go；
// 本文件只保留跨 Run 语义记忆的召回与拼接逻辑。

// recallMemory 召回跨 Run 的语义记忆。任何失败都返回 nil（降级），绝不返回 error。
//
// 召回前先 GetRun 取 thread_id 与查询文本：thread_id 决定会话隔离范围，
// Run.Input 是最能代表"本轮要解决什么"的查询——用节点自己的 Input 做查询会
// 退化成"找相似子任务"，而用 Run.Input 才是"找与此会话相关的历史"。
func recallMemory(ctx context.Context, s contextStore, opt MemoryOptions, tenant, runID string) []llm.Message {
	if opt.Memory == nil {
		return nil
	}
	// 能力探测：只有实现了 MemorySearcher 的记忆才能做语义召回。
	// 用类型断言而非扩展 MemoryProvider 接口，是为了保持 provider port 稳定
	// （docs/architecture-v3.md 的硬要求），范式同 store 层的 s.(CancelStore)。
	searcher, ok := opt.Memory.(providers.MemorySearcher)
	if !ok {
		return nil
	}

	run, err := s.GetRun(ctx, tenant, runID)
	if err != nil || run == nil {
		// 关键日志：取不到 Run 就无法确定会话维度，放弃召回但不影响节点执行。
		log.Printf("worker: memory recall skipped (load run) tenant=%s run=%s: %v", tenant, runID, err)
		return nil
	}
	// 没有会话身份就没有可召回的范围：跳过而非全量检索。
	// 全量检索会把该租户所有会话的记忆混进上下文（跨会话串味）。
	if run.Input == "" {
		return nil
	}

	// 超时只包裹记忆召回，不包裹上面的 CompletedNodes（主链路必需）。
	sctx, cancel := context.WithTimeout(ctx, opt.searchTimeout())
	defer cancel()

	msgs, err := searcher.Search(sctx, contracts.ExecutionContext{
		TenantID: tenant,
		ThreadID: run.ThreadID,
		RunID:    runID, // provider 据此在源头排除当前 Run，避免与其历史重复注入
	}, run.Input, opt.maxMessages())
	if err != nil {
		// VectorMemory 的契约是"失败静默降级返回 (nil,nil)"，
		// 这里仍做防御性处理：实现可能不遵守契约，主链路不能因此失败。
		log.Printf("worker: memory recall failed tenant=%s thread=%s run=%s: %v", tenant, run.ThreadID, runID, err)
		return nil
	}
	if len(msgs) == 0 {
		return nil
	}
	out := make([]llm.Message, 0, len(msgs))
	for _, m := range msgs {
		if m.Content == "" {
			continue // 空消息只会浪费 token 并可能干扰模型
		}
		out = append(out, llm.Message{Role: llm.Role(m.Role), Content: m.Content})
	}
	if len(out) > 0 {
		// 关键日志：召回命中，是排查"Agent 是否真的用上了长期记忆"的主要信号。
		log.Printf("worker: recalled %d memory message(s) tenant=%s thread=%s run=%s", len(out), tenant, run.ThreadID, runID)
	}
	return out
}

// mergeMessages 拼接记忆与当前 Run 历史，施加 token 预算与条数上限。
//
// 预算策略（双层闸门）：
//  1. Token 预算（MaxContextTokens）：先算 current 的 token 量；若超预算，
//     从最新向前保留到预算耗尽。否则用剩余预算从最新向前吸收 memory。
//  2. 条数上限（MaxMessages）：>0 时仍作硬上限，向后兼容旧配置。
//
// 优先保证当前 Run 历史的完整性：超限时从最老的跨 Run 记忆开始丢弃。
// 总是返回新分配的切片，避免共享底层数组。
func mergeMessages(memory, current []llm.Message, opt MemoryOptions) []llm.Message {
	maxTokens := opt.maxContextTokens()
	maxMsgs := opt.maxMessages()

	// Step 1: 条数上限先于 token 计算（向后兼容）。
	if maxMsgs > 0 && len(current) >= maxMsgs {
		current = current[len(current)-maxMsgs:]
	}
	if maxMsgs > 0 && len(memory)+len(current) > maxMsgs {
		budget := maxMsgs - len(current)
		if budget <= 0 {
			memory = nil
		} else if len(memory) > budget {
			memory = memory[len(memory)-budget:]
		}
	}

	// Step 2: token 预算——current 优先，超限从最新向前保留。
	currentTokens := estimateMessagesTokens(current)
	if currentTokens > maxTokens {
		return trimToTokenBudget(current, maxTokens)
	}

	// Step 3: 用剩余 token 预算从最新向前吸收 memory。
	remaining := maxTokens - currentTokens
	kept := make([]llm.Message, 0, len(memory))
	used := 0
	for i := len(memory) - 1; i >= 0; i-- {
		cost := estimateTokens(memory[i].Content)
		if used+cost > remaining {
			break
		}
		kept = append([]llm.Message{memory[i]}, kept...)
		used += cost
	}

	out := make([]llm.Message, 0, len(kept)+len(current))
	out = append(out, kept...)
	out = append(out, current...)
	return out
}

// trimToTokenBudget 从最新消息向前保留，跳过单条超预算的消息，直到预算耗尽。
func trimToTokenBudget(msgs []llm.Message, maxTokens int) []llm.Message {
	out := make([]llm.Message, 0, len(msgs))
	used := 0
	for i := len(msgs) - 1; i >= 0; i-- {
		cost := estimateTokens(msgs[i].Content)
		if used+cost > maxTokens {
			continue // 单条超预算时跳过，保留更老的较小消息
		}
		out = append([]llm.Message{msgs[i]}, out...)
		used += cost
	}
	return out
}

// ==== 环境变量装配辅助 ====
//
// NewFromEnv 是 internal 包内的装配入口（cmd/worker/main.go 调用），
// 因此这里需要自带 env 解析辅助；cmd/* 下的同名函数属于各自的 main 包，互不冲突。

func envString(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envBool(key string, def bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		log.Printf("worker: invalid bool for %s=%q, using default %v", key, v, def)
		return def
	}
	return b
}

func envInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		log.Printf("worker: invalid int for %s=%q, using default %d", key, v, def)
		return def
	}
	return n
}

func envFloat32(key string, def float32) float32 {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 32)
	if err != nil {
		log.Printf("worker: invalid float for %s=%q, using default %v", key, v, def)
		return def
	}
	return float32(f)
}

func envDuration(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		log.Printf("worker: invalid duration for %s=%q, using default %s", key, v, def)
		return def
	}
	return d
}
