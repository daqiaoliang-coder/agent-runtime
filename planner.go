package main

import "context"

// Planner 负责根据当前 Run 的进度决定下一步做什么。
// 真实实现通常会调 LLM 来产出动作，这里用 Mock 固定走四步流程。
type Planner interface {
	Plan(context.Context, *AgentRun) (PlanAction, error)
}

type MockPlanner struct{}

// Plan 按 Steps 计数走固定流程：规划 → 搜索 → 推理 → 结束。
// 这只是演示用的桩，真实规划逻辑应由 LLM 驱动。
func (p *MockPlanner) Plan(ctx context.Context, run *AgentRun) (PlanAction, error) {
	switch run.Steps {
	case 0:
		return PlanAction{
			Type:  StepLLM,
			Name:  "planning",
			Input: run.Input,
		}, nil
	case 1:
		return PlanAction{
			Type:  StepTool,
			Name:  "search",
			Input: "search information about " + run.Input,
		}, nil
	case 2:
		return PlanAction{
			Type:  StepLLM,
			Name:  "reasoning",
			Input: "Analyze the search result for: " + run.Input,
		}, nil
	default:
		return PlanAction{Type: StepFinish, Name: "finish"}, nil
	}
}
