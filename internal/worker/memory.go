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
	"agent-runtime/internal/model"
	"agent-runtime/internal/providers"
	"context"
	"database/sql"
	"errors"
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
	DefaultMaxMemoryMessages = 20
)

// MemoryOptions 描述读取路径的记忆装配。
//
// Memory 为 nil 表示未启用长期记忆，此时 ContextLoader 的行为与接入向量检索之前
// **完全一致**——这保证了未配置向量库的部署与现有测试零感知。
type MemoryOptions struct {
	Memory        providers.MemoryProvider
	SearchTimeout time.Duration
	MaxMessages   int
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

// contextStore 是 ContextLoader 所需的持久化能力（*store.MySQL 天然实现）。
//
// 抽象为接口而非直接用 *store.MySQL，是为了让拼接/截断/降级逻辑可以被单测覆盖，
// 不必为了测一个纯函数而起一个真实 MySQL。
type contextStore interface {
	CompletedNodes(ctx context.Context, tenant, runID string) ([]model.Node, error)
	GetRun(ctx context.Context, tenant, id string) (*model.Run, error)
}

// newContextLoader 构造 executor.Dispatcher.ContextLoader。
//
// 返回的闭包把两段上下文拼成对话历史：
//  1. **跨 Run 语义记忆**（前置）——从向量库召回同会话的历史，让 Agent 跨 Run 连续；
//  2. **当前 Run 历史**（后置）——从已提交的 SUCCESS 节点派生，这是接入前就有的行为。
//
// 记忆在前、当前历史在后，是为了让最近、最相关的上下文紧邻本轮 user prompt，
// 符合对话的时间顺序直觉，也便于模型优先关注。
//
// 错误语义是刻意分层的：
//   - CompletedNodes 失败 → **向上返回 error**。它是主链路必需数据，
//     静默吞掉会让 Agent 在缺失历史的情况下推理（executor_test 已锁定该行为）。
//   - 记忆召回失败 → 只记 warn，返回仅含当前 Run 历史的结果。
func newContextLoader(s contextStore, opt MemoryOptions) func(context.Context, string, string) ([]llm.Message, error) {
	return func(ctx context.Context, tenant, runID string) ([]llm.Message, error) {
		nodes, err := s.CompletedNodes(ctx, tenant, runID)
		if err != nil {
			if !errors.Is(err, sql.ErrNoRows) {
				return nil, err
			}
			nodes = nil
		}
		current := make([]llm.Message, 0, len(nodes)*2)
		for _, n := range nodes {
			current = append(current,
				llm.Message{Role: llm.RoleUser, Content: n.Input},
				llm.Message{Role: llm.RoleAssistant, Content: n.Output},
			)
		}

		// 记忆未启用时必须**原样返回**，不走截断：
		// 接入向量检索前 ContextLoader 就是返回全部当前 Run 历史，
		// 若这里套用 MaxMessages 截断，会悄悄改变未启用记忆部署的既有行为
		// （长 Run 的上下文被砍掉），违背"现有部署零感知"的前提。
		if opt.Memory == nil {
			return current, nil
		}

		recalled := recallMemory(ctx, s, opt, tenant, runID)
		return mergeMessages(recalled, current, opt.maxMessages()), nil
	}
}

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

// mergeMessages 拼接记忆与当前 Run 历史，并施加条数上限。
//
// 截断策略：**优先保证当前 Run 历史的完整性**，超限时从最老的跨 Run 记忆开始丢弃。
// 理由是当前 Run 的历史直接决定本轮推理的连贯性，而跨 Run 记忆是锦上添花；
// 丢弃最老的记忆也符合"越近的上下文越相关"。
//
// 总是返回新分配的切片：调用方（executor）会对返回值做 append，
// 若返回内部子切片，append 可能写入共享底层数组，造成难以定位的数据串改。
func mergeMessages(memory, current []llm.Message, max int) []llm.Message {
	if max <= 0 {
		max = DefaultMaxMemoryMessages
	}
	if len(current) >= max {
		out := make([]llm.Message, max)
		copy(out, current[len(current)-max:])
		return out
	}
	budget := max - len(current)
	if len(memory) > budget {
		memory = memory[len(memory)-budget:]
	}
	out := make([]llm.Message, 0, len(memory)+len(current))
	out = append(out, memory...)
	out = append(out, current...)
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
