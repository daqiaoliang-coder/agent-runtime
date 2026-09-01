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
type Planner interface {
	Plan(ctx context.Context, run *model.Run) (model.Plan, error)
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
		{ID: base + ":report", Type: model.NodeLLM, Name: "report", Input: "generate final report", DependsOn: []string{base + ":reason"}},
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
		// 节点 ID 缺省时按 run + 序号兜底，避免空 ID 导致依赖解析失败。
		if n.ID == "" {
			n.ID = fmt.Sprintf("%s:n%d", run.ID, len(plan.Nodes))
		}
		plan.Nodes = append(plan.Nodes, model.PlanNode{
			ID: n.ID, ParentNodeID: n.ParentNodeID, Type: model.NodeType(n.Type),
			Name: n.Name, Input: n.Input, DependsOn: n.DependsOn,
		})
	}
	if len(plan.Nodes) == 0 {
		return model.Plan{}, fmt.Errorf("llm planner: produced empty plan")
	}
	return plan, nil
}

// planJSON 是 LLM 返回的 JSON 计划的解析结构。
type planJSON struct {
	Nodes []struct {
		ID, ParentNodeID       string
		Type, Name, Input      string
		DependsOn              []string
	} `json:"nodes"`
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
