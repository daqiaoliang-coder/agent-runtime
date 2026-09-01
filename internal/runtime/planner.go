package runtime

import (
	"agent-runtime/internal/model"
	"fmt"
)

// Planner 负责将用户输入转化为可执行的 DAG 计划。
// 实现可替换为基于 LLM 的动态规划器，DemoPlanner 为演示用的静态规划器。
type Planner interface {
	Plan(run *model.Run) (model.Plan, error)
}

// DemoPlanner 是一个演示用规划器，生成固定的 DAG：
//
//	Search A ──┐
//	           ├──> Reason ──> Report
//	Search B ──┘
//
// 其中 Search A/B 为可并行的工具节点，Reason 依赖两者，Report 依赖 Reason。
type DemoPlanner struct{}

func (DemoPlanner) Plan(run *model.Run) (model.Plan, error) {
	base := fmt.Sprintf("%s:%s", run.ID, run.Input)
	return model.Plan{Nodes: []model.PlanNode{
		{ID: base + ":search-a", Type: model.NodeTool, Name: "search", Input: "search A", DependsOn: nil},
		{ID: base + ":search-b", Type: model.NodeTool, Name: "search", Input: "search B", DependsOn: nil},
		{ID: base + ":reason", Type: model.NodeLLM, Name: "reasoning", Input: "reason over search results", DependsOn: []string{base + ":search-a", base + ":search-b"}},
		{ID: base + ":report", Type: model.NodeLLM, Name: "report", Input: "generate final report", DependsOn: []string{base + ":reason"}},
	}}, nil
}
