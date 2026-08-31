package runtime

import (
	"agent-runtime/internal/model"
	"fmt"
)

type Planner interface {
	Plan(run *model.Run) (model.Plan, error)
}
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
