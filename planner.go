package main

import "context"

type Planner interface {
	Plan(context.Context, *AgentRun) (PlanAction, error)
}

type MockPlanner struct{}

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
