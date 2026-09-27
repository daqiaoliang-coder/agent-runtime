// 本文件实现上下文加载的"塑形"层：在记忆读取路径（memory.go）之前，
// 决定哪些已完成节点以什么形态进入 LLM 的对话历史。
//
// 两项 P0 优化都在这里落地：
//  1. **DAG 祖先作用域**——节点只加载自己在 agent_edge 上传递闭包可达的 SUCCESS
//     祖先，而不是全 Run 所有节点（并行分支的无关产物不再灌入上下文）；
//  2. **工具结果遮蔽（observation masking）**——REFLECT 控制节点不进对话，
//     窗口外的工具结果替换为带 node_id 指针的占位符，窗口内的超长结果截断，
//     原文始终保留在 agent_node.output（可重建/可审计）。
//
// 两者各自可经环境变量关闭，退化为接入前的全量无差别行为。
package worker

import (
	"agent-runtime/internal/contracts"
	"agent-runtime/internal/llm"
	"agent-runtime/internal/model"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"unicode/utf8"
)

// 上下文塑形的默认参数。
const (
	// DefaultToolMaskWindow 保留完整工具结果的"最近工具节点"数量。
	// 超出该窗口的更老 TOOL 节点结果被占位符替换（调用本身仍保留，模型知道发生过什么）。
	DefaultToolMaskWindow = 6
	// DefaultToolOutputMaxRunes 限制单条工具结果进入上下文的最大字符数（按 rune）。
	// 即使在保留窗口内，超长结果也只保留头部并附原文指针——工具输出通常比对话大
	// 一到两个数量级，是上下文膨胀的首要来源。
	DefaultToolOutputMaxRunes = 4000
)

// ContextOptions 描述 ContextLoader 的完整装配：上下文塑形开关 + 跨 Run 记忆 + 压缩管线。
type ContextOptions struct {
	// Memory 为跨 Run 语义记忆配置；Memory.Memory 为 nil 表示未启用。
	Memory MemoryOptions
	// AncestorScope=true 时只加载当前节点经 agent_edge 传递闭包可达的 SUCCESS 祖先；
	// false 时加载全 Run SUCCESS 节点（旧行为）。
	AncestorScope bool
	// ToolMasking=true 时启用塑形：排除 REFLECT、工具调用加角色前缀、
	// 老/长工具结果替换为占位符。false 时保持"全部节点无差别 input/output 配对"。
	ToolMasking bool
	// ToolMaskWindow 保留完整结果的最近 TOOL 节点数；<=0 用默认值。
	// 注意这只是**初始**窗口：压缩管线（Compaction）启用时，L2 收缩会
	// 在预算压力下把实际窗口逐档收窄到 4/2/0，这里配置的是起点而非下限。
	ToolMaskWindow int
	// ToolOutputMaxRunes 单条工具结果的字符上限；<=0 用默认值。
	ToolOutputMaxRunes int
	// Compaction 为上下文压缩管线（docs/context-compaction.md）；nil 时整条
	// 管线旁路，行为与接入前完全一致。压缩开关与塑形开关正交：微压缩的回填
	// 在塑形关闭时同样生效（水位线语义不依赖塑形），L2 收缩则只在塑形开启时
	// 有收缩空间。
	Compaction *Compactor
}

func (o ContextOptions) toolMaskWindow() int {
	if o.ToolMaskWindow <= 0 {
		return DefaultToolMaskWindow
	}
	return o.ToolMaskWindow
}

func (o ContextOptions) toolOutputMaxRunes() int {
	if o.ToolOutputMaxRunes <= 0 {
		return DefaultToolOutputMaxRunes
	}
	return o.ToolOutputMaxRunes
}

// contextStore 是 ContextLoader 所需的持久化能力（*store.MySQL 天然实现）。
//
// 抽象为接口而非直接用 *store.MySQL，是为了让拼接/截断/降级逻辑可以被单测覆盖，
// 不必为了测一个纯函数而起一个真实 MySQL。
type contextStore interface {
	CompletedNodes(ctx context.Context, tenant, runID string) ([]model.Node, error)
	CompletedAncestorNodes(ctx context.Context, tenant, runID, nodeID string) ([]model.Node, error)
	GetRun(ctx context.Context, tenant, id string) (*model.Run, error)
}

// newContextLoader 构造 executor.Dispatcher.ContextLoader。
//
// 闭包签名带 nodeID：祖先作用域需要以"当前要执行的节点"为起点沿边追溯。
// 组装顺序为：[塑形后的当前 Run 历史] →（启用记忆时）前置跨 Run 召回。
// 压缩管线启用时，整条组装交给 Compactor.Apply（它在同一位置叠加
// 水位线基座/回填/预算收缩，详见 compaction.go）。
//
// 错误语义：节点历史查询失败（含祖先查询）按主链路必需数据上抛；
// 记忆召回失败只 warn 降级，见 memory.go。
func newContextLoader(s contextStore, opt ContextOptions) func(context.Context, string, string, string) ([]llm.Message, error) {
	return func(ctx context.Context, tenant, runID, nodeID string) ([]llm.Message, error) {
		var nodes []model.Node
		var err error
		if opt.AncestorScope {
			nodes, err = s.CompletedAncestorNodes(ctx, tenant, runID, nodeID)
		} else {
			nodes, err = s.CompletedNodes(ctx, tenant, runID)
		}
		if err != nil {
			if !errors.Is(err, sql.ErrNoRows) {
				return nil, err
			}
			nodes = nil
		}

		// 压缩管线接管组装：水位线过滤/基座/回填/L2-L5 都在 Apply 内。
		// 它只降级不报错，因此这里的错误语义与无压缩路径完全一致。
		if opt.Compaction != nil {
			recalled := recallMemory(ctx, s, opt.Memory, tenant, runID)
			return opt.Compaction.Apply(ctx, contracts.ExecutionContext{
				TenantID: tenant,
				RunID:    runID,
				NodeID:   nodeID,
			}, nodes, recalled, opt), nil
		}

		current := nodesToMessages(nodes, opt, opt.toolMaskWindow(), nil)

		// 记忆未启用时原样返回当前 Run 历史（截断是记忆路径的预算策略，不偷渡到这里）。
		if opt.Memory.Memory == nil {
			return current, nil
		}

		recalled := recallMemory(ctx, s, opt.Memory, tenant, runID)
		return mergeMessages(recalled, current, opt.Memory), nil
	}
}

// nodesToMessages 把已完成节点派生为对话消息，是 worker 与单测共用的纯函数。
//
// keepFullWindow 为保留完整工具结果的最近 TOOL 节点数（压缩管线的 L2 收缩
// 会传入收缩后的档位；0 表示全部遮蔽）。sections 为微压缩回填映射，
// node_id 命中时该节点整对替换为一条摘要消息——微压缩不合并语义，
// 对话轮次与顺序完全不变，只是"大原文"换成"小转述"。
//
// 塑形关闭时严格保持旧行为：所有节点（含 TOOL/REFLECT）按时间序无差别展开为
// user(input)/assistant(output) 对；但 sections 回填仍然生效——摘要产物替换
// 原文是水位线语义（持久化事实），不随塑形开关退化，否则微压缩白做。
// 塑形开启时：
//   - REFLECT：整节点排除（其输出是 {"action":...} 控制信号，不是业务对话）；
//   - TOOL：user 消息加 [tool_call:<name>] 前缀让模型知道文本来源；assistant 消息
//     按窗口保留/遮蔽/截断，占位符携带 node_id 作为原文指针；
//   - LLM：维持 input→user、output→assistant。
func nodesToMessages(nodes []model.Node, opt ContextOptions, keepFullWindow int, sections map[string]string) []llm.Message {
	if !opt.ToolMasking {
		out := make([]llm.Message, 0, len(nodes)*2)
		for _, n := range nodes {
			if s, ok := sections[n.ID]; ok {
				out = append(out, llm.Message{Role: llm.RoleUser, Content: summarizedNodePlaceholder(n, s)})
				continue
			}
			out = append(out,
				llm.Message{Role: llm.RoleUser, Content: n.Input},
				llm.Message{Role: llm.RoleAssistant, Content: n.Output},
			)
		}
		return out
	}

	// 找出全部 TOOL 节点的位置，只保留最近 keepFullWindow 个的完整结果；其余遮蔽。
	// keepFullWindow==0 是 L2 收缩的终点档：全部遮蔽（调用本身仍保留，模型知道发生过什么）。
	toolPositions := make([]int, 0, len(nodes))
	for i, n := range nodes {
		if n.Type == model.NodeTool {
			toolPositions = append(toolPositions, i)
		}
	}
	keepFull := make(map[int]bool, len(toolPositions))
	w := keepFullWindow
	if w < 0 || len(toolPositions) <= w {
		for _, i := range toolPositions {
			keepFull[i] = true
		}
	} else {
		for _, i := range toolPositions[len(toolPositions)-w:] {
			keepFull[i] = true
		}
	}

	maxRunes := opt.toolOutputMaxRunes()
	out := make([]llm.Message, 0, len(nodes)*2)
	for i, n := range nodes {
		switch {
		case n.Type == model.NodeReflect:
			continue
		case n.Type == model.NodeTool:
			if s, ok := sections[n.ID]; ok {
				out = append(out, llm.Message{Role: llm.RoleUser, Content: summarizedNodePlaceholder(n, s)})
				continue
			}
			out = append(out, llm.Message{
				Role:    llm.RoleUser,
				Content: fmt.Sprintf("[tool_call:%s] %s", n.Name, n.Input),
			})
			if keepFull[i] {
				out = append(out, llm.Message{
					Role:    llm.RoleAssistant,
					Content: capRunesWithPointer(n.Output, n, maxRunes),
				})
			} else {
				out = append(out, llm.Message{
					Role:    llm.RoleAssistant,
					Content: maskedToolPlaceholder(n),
				})
			}
		default:
			if s, ok := sections[n.ID]; ok {
				out = append(out, llm.Message{Role: llm.RoleUser, Content: summarizedNodePlaceholder(n, s)})
				continue
			}
			out = append(out,
				llm.Message{Role: llm.RoleUser, Content: n.Input},
				llm.Message{Role: llm.RoleAssistant, Content: n.Output},
			)
		}
	}
	return out
}

// summarizedNodePlaceholder 生成微压缩回填的摘要消息。
// 保留 node_id 指针：摘要丢失细节时，模型可经 fetch_tool_result 按需取回原文。
func summarizedNodePlaceholder(n model.Node, summary string) string {
	return fmt.Sprintf("[Summary of node %s (%s %s): %s]", n.ID, n.Type, n.Name, summary)
}

// maskedToolPlaceholder 生成窗口外工具结果的占位符。
// 保留工具名、规模与 node_id 指针：模型仍知道这一步做过什么、产出多大，
// 需要细节时可凭 node_id 定位（待模型侧工具调用循环接入后可按需取回原文）。
func maskedToolPlaceholder(n model.Node) string {
	return fmt.Sprintf("[Compacted tool result from %q: %d characters. Full content retained at node_id=%q.]",
		n.Name, utf8.RuneCountInString(n.Output), n.ID)
}

// capRunesWithPointer 对窗口内但超长的工具结果做头部截断，并附原文指针。
// 按 rune 截断，避免在多字节字符中间切断产生非法 UTF-8。
func capRunesWithPointer(s string, n model.Node, max int) string {
	if max <= 0 || utf8.RuneCountInString(s) <= max {
		return s
	}
	runes := []rune(s)
	return string(runes[:max]) + fmt.Sprintf(
		"\n\n[Truncated: result was %d characters; full content retained at node_id=%q.]",
		len(runes), n.ID)
}

// newContextOptionsFromEnv 装配上下文策略：塑形开关 + 跨 Run 记忆。
// 两项塑形默认开启（P0），均可经环境变量退回旧行为。
func newContextOptionsFromEnv(creds contracts.CredentialProvider) ContextOptions {
	return ContextOptions{
		Memory:             newMemoryOptionsFromEnv(creds),
		AncestorScope:      envBool("CONTEXT_ANCESTOR_SCOPE", true),
		ToolMasking:        envBool("CONTEXT_TOOL_MASKING", true),
		ToolMaskWindow:     envInt("CONTEXT_TOOL_MASK_WINDOW", DefaultToolMaskWindow),
		ToolOutputMaxRunes: envInt("CONTEXT_TOOL_MAX_RUNES", DefaultToolOutputMaxRunes),
	}
}
