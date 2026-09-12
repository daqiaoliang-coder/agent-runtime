package runtime

import (
	"agent-runtime/internal/llm"
	"agent-runtime/internal/model"
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

// Planner 负责将用户输入转化为可执行的 DAG 计划。
// 实现可替换为基于 LLM 的动态规划器（LLMPlanner），DemoPlanner 为演示用的静态规划器。
// Plan 接收 ctx 以便 LLM 规划器在超时/取消时及时退出。
// Replan 在多轮 Plan 场景下由 Resumer 调用：基于已完成节点的 outputs 续规划新一轮节点。
// 新节点的 DependsOn 留空时，Resumer 会自动将其链接到触发续规的 REFLECT 节点。
type Planner interface {
	Plan(ctx context.Context, run *model.Run) (model.Plan, error)
	Replan(ctx context.Context, run *model.Run, completed []model.Node) (model.Plan, error)
}

// DemoPlanner 是一个演示用规划器，生成固定的 DAG：
//
//	Search A ──┐
//	           ├──> Reason ──> Report
//	Search B ──┘
//
// 其中 Search A/B 为可并行的工具节点，Reason 依赖两者，Report 依赖 Reason。
type DemoPlanner struct{}

func (DemoPlanner) Plan(_ context.Context, run *model.Run) (model.Plan, error) {
	base := fmt.Sprintf("%s:%s", run.ID, run.Input)
	return model.Plan{Nodes: []model.PlanNode{
		{ID: base + ":search-a", Type: model.NodeTool, Name: "search", Input: "search A", DependsOn: nil},
		{ID: base + ":search-b", Type: model.NodeTool, Name: "search", Input: "search B", DependsOn: nil},
		{ID: base + ":reason", Type: model.NodeLLM, Name: "reasoning", Input: "reason over search results", DependsOn: []string{base + ":search-a", base + ":search-b"}},
		{ID: base + ":reflect", Type: model.NodeReflect, Name: "reflect", Input: "evaluate progress and decide replan or finish", DependsOn: []string{base + ":reason"}},
	}}, nil
}

// Replan 在 DemoPlanner 下总是产生一个 finish 节点（第 2 轮），演示"反思→续规→收尾"的单次续规。
// 新节点无 DependsOn，由 Resumer 自动链接到触发续规的 REFLECT 节点。
func (DemoPlanner) Replan(_ context.Context, run *model.Run, completed []model.Node) (model.Plan, error) {
	round := nextRound(completed)
	prefix := fmt.Sprintf("%s:r%d", run.ID, round)
	return model.Plan{Nodes: []model.PlanNode{
		{ID: prefix + ":finish", Type: model.NodeLLM, Name: "finish", Input: "generate final answer based on all prior results", PlanningRound: round},
	}}, nil
}

// LLMPlanner 通过 LLM 动态生成 DAG：把用户输入作为提示，要求模型返回 JSON 计划。
// 相比 DemoPlanner 的固定拓扑，它能根据目标自适应拆解步骤（ReAct / plan-and-execute 范式）。
type LLMPlanner struct{ LLM llm.Client }

const planSystemPrompt = `You are a task planner for an agent runtime.
Decompose the user's goal into a JSON DAG of nodes.
Node types: "LLM" (reasoning/generation), "TOOL" (tool call; "name" must be a registered tool like "search" or "calculator").
Respond with ONLY a JSON object, no prose, in this exact shape:
{"nodes":[{"id":"n1","type":"TOOL","name":"search","input":"query","dependsOn":[]}]}`

func (p *LLMPlanner) Plan(ctx context.Context, run *model.Run) (model.Plan, error) {
	if p.LLM == nil {
		return model.Plan{}, fmt.Errorf("llm planner: client not configured")
	}
	resp, err := p.LLM.Complete(ctx, llm.Request{Messages: []llm.Message{
		{Role: llm.RoleSystem, Content: planSystemPrompt},
		{Role: llm.RoleUser, Content: fmt.Sprintf("Goal: %s\nRunID: %s", run.Input, run.ID)},
	}})
	if err != nil {
		return model.Plan{}, fmt.Errorf("llm planner: %w", err)
	}
	raw := extractJSON(resp.Content)
	var pj planJSON
	if err := json.Unmarshal([]byte(raw), &pj); err != nil {
		return model.Plan{}, fmt.Errorf("llm planner: parse plan json: %w", err)
	}
	plan := model.Plan{Nodes: make([]model.PlanNode, 0, len(pj.Nodes))}
	for _, n := range pj.Nodes {
		// 节点 ID 缺省时按序号兜底，命名空间化由 namespacePlan 统一处理。
		if n.ID == "" {
			n.ID = fmt.Sprintf("n%d", len(plan.Nodes))
		}
		plan.Nodes = append(plan.Nodes, model.PlanNode{
			ID: n.ID, ParentNodeID: n.ParentNodeID, Type: model.NodeType(n.Type),
			Name: n.Name, Input: n.Input, DependsOn: n.DependsOn,
		})
	}
	// 服务端命名空间化：所有 node_id 加 run 前缀，防止跨 Run 撞全局主键。
	namespacePlan(&plan, run.ID)
	if err := validatePlan(plan); err != nil {
		return model.Plan{}, fmt.Errorf("llm planner: invalid plan: %w", err)
	}
	return plan, nil
}

// planJSON 是 LLM 返回的 JSON 计划的解析结构。
type planJSON struct {
	Nodes []struct {
		ID, ParentNodeID  string
		Type, Name, Input string
		DependsOn         []string
	} `json:"nodes"`
}

// nextRound 根据已完成节点的最大 planning_round 推算下一轮轮次。
func nextRound(completed []model.Node) int {
	max := 1
	for _, n := range completed {
		if n.PlanningRound > max {
			max = n.PlanningRound
		}
	}
	return max + 1
}

const replanSystemPrompt = `You are a task planner for an agent runtime.
The user's goal was not fully achieved yet. Based on the completed steps and their outputs,
produce a JSON DAG of NEW nodes to execute next.
Node types: "LLM" (reasoning/generation), "TOOL" (tool call), "REFLECT" (decide whether another round is needed).
Include a REFLECT node if the task might need further iteration.
Respond with ONLY a JSON object, no prose, in this exact shape:
{"nodes":[{"id":"n1","type":"TOOL","name":"search","input":"query","dependsOn":[]}]}`

// Replan 通过 LLM 基于已完成节点的 outputs 动态续规划。
// 将各已完成节点的 name/output 拼入 prompt，要求 LLM 产出新一轮节点。
func (p *LLMPlanner) Replan(ctx context.Context, run *model.Run, completed []model.Node) (model.Plan, error) {
	if p.LLM == nil {
		return model.Plan{}, fmt.Errorf("llm planner: client not configured")
	}
	var sb strings.Builder
	for _, n := range completed {
		fmt.Fprintf(&sb, "- %s (type=%s): %s\n", n.Name, n.Type, n.Output)
	}
	resp, err := p.LLM.Complete(ctx, llm.Request{Messages: []llm.Message{
		{Role: llm.RoleSystem, Content: replanSystemPrompt},
		{Role: llm.RoleUser, Content: fmt.Sprintf("Goal: %s\nRunID: %s\nCompleted steps:\n%s", run.Input, run.ID, sb.String())},
	}})
	if err != nil {
		return model.Plan{}, fmt.Errorf("llm replan: %w", err)
	}
	raw := extractJSON(resp.Content)
	var pj planJSON
	if err := json.Unmarshal([]byte(raw), &pj); err != nil {
		return model.Plan{}, fmt.Errorf("llm replan: parse plan json: %w", err)
	}
	round := nextRound(completed)
	plan := model.Plan{Nodes: make([]model.PlanNode, 0, len(pj.Nodes))}
	for _, n := range pj.Nodes {
		if n.ID == "" {
			n.ID = fmt.Sprintf("n%d", len(plan.Nodes))
		}
		plan.Nodes = append(plan.Nodes, model.PlanNode{
			ID: n.ID, ParentNodeID: n.ParentNodeID, Type: model.NodeType(n.Type),
			Name: n.Name, Input: n.Input, DependsOn: n.DependsOn, PlanningRound: round,
		})
	}
	// 服务端命名空间化：续规节点 ID 加 run + 轮次前缀，防止跨 Run/跨轮次撞全局主键。
	namespacePlan(&plan, fmt.Sprintf("%s:r%d", run.ID, round))
	if err := validatePlan(plan); err != nil {
		return model.Plan{}, fmt.Errorf("llm replan: invalid plan: %w", err)
	}
	return plan, nil
}

// extractJSON 从可能含 Markdown 代码块或前后说明文本的响应中提取首个 JSON 对象。
// 让 LLM 规划器对不严格遵循"仅输出 JSON"指令的模型更具鲁棒性。
func extractJSON(s string) string {
	s = strings.TrimSpace(s)
	// 去除 ```json ... ``` 代码块围栏。
	if strings.HasPrefix(s, "```") {
		s = strings.TrimPrefix(s, "```json")
		s = strings.TrimPrefix(s, "```")
		s = strings.TrimSuffix(s, "```")
		s = strings.TrimSpace(s)
	}
	start := strings.Index(s, "{")
	if start < 0 {
		return s
	}
	end := strings.LastIndex(s, "}")
	if end < start {
		return s
	}
	return s[start : end+1]
}

// namespacePlan 为 LLM 返回的节点 ID 添加 run 级命名空间前缀，防止跨 Run 撞全局主键。
// 同时更新 DependsOn 和 ParentNodeID 中对同批次节点的引用。
// 引用既有节点（来自前序轮次，已是命名空间化 ID）的边不会匹配映射，保持不变。
func namespacePlan(plan *model.Plan, prefix string) {
	idMap := make(map[string]string, len(plan.Nodes))
	for i, n := range plan.Nodes {
		ns := prefix + ":" + n.ID
		idMap[n.ID] = ns
		plan.Nodes[i].ID = ns
	}
	for i, n := range plan.Nodes {
		for j, dep := range n.DependsOn {
			if ns, ok := idMap[dep]; ok {
				plan.Nodes[i].DependsOn[j] = ns
			}
		}
		if n.ParentNodeID != "" {
			if ns, ok := idMap[n.ParentNodeID]; ok {
				plan.Nodes[i].ParentNodeID = ns
			}
		}
	}
}

// validatePlan 校验 DAG 完整性：
//   - 非空计划；
//   - 无空 ID、无重复 ID；
//   - 节点类型合法；
//   - 所有 DependsOn 指向已知节点（无悬空引用）；
//   - 无环（Kahn 拓扑排序）。
func validatePlan(plan model.Plan) error {
	if len(plan.Nodes) == 0 {
		return fmt.Errorf("plan is empty")
	}
	ids := make(map[string]bool, len(plan.Nodes))
	for _, n := range plan.Nodes {
		if n.ID == "" {
			return fmt.Errorf("plan contains empty node ID")
		}
		if ids[n.ID] {
			return fmt.Errorf("duplicate node ID: %s", n.ID)
		}
		ids[n.ID] = true
		switch n.Type {
		case model.NodeLLM, model.NodeTool, model.NodeSubAgent, model.NodeReflect:
		default:
			return fmt.Errorf("node %s has invalid type %q", n.ID, n.Type)
		}
	}
	// 悬空依赖检查。
	for _, n := range plan.Nodes {
		for _, dep := range n.DependsOn {
			if !ids[dep] {
				return fmt.Errorf("node %s depends on unknown node %s", n.ID, dep)
			}
		}
	}
	// 环检测：Kahn 拓扑排序，若拓扑序长度 < 节点数则有环。
	inDeg := make(map[string]int, len(plan.Nodes))
	adj := make(map[string][]string, len(plan.Nodes))
	for _, n := range plan.Nodes {
		inDeg[n.ID] += len(n.DependsOn)
		for _, dep := range n.DependsOn {
			adj[dep] = append(adj[dep], n.ID)
		}
	}
	var queue []string
	for id, d := range inDeg {
		if d == 0 {
			queue = append(queue, id)
		}
	}
	visited := 0
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		visited++
		for _, next := range adj[cur] {
			inDeg[next]--
			if inDeg[next] == 0 {
				queue = append(queue, next)
			}
		}
	}
	if visited != len(plan.Nodes) {
		return fmt.Errorf("plan contains a cycle")
	}
	return nil
}
