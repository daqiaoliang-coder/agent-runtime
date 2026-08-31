package main

import (
	"context"
	"fmt"
	"math/rand"
	"time"
)

type Executor struct {
	store *MemoryStore
	llm   LLM
	tools *ToolRegistry
}

func NewExecutor(store *MemoryStore, llm LLM, tools *ToolRegistry) *Executor {
	return &Executor{store: store, llm: llm, tools: tools}
}

func (e *Executor) Execute(ctx context.Context, stepID string) error {
	step, err := e.store.GetStep(stepID)
	if err != nil {
		return err
	}
	if step.Status != StepPending {
		return nil
	}

	if err := e.store.ClaimStep(step.ID, step.Version); err != nil {
		return nil
	}

	e.emit(Event{
		ID: newID("event"), RunID: step.RunID, StepID: step.ID,
		Type: "STEP_STARTED", Timestamp: time.Now(), Message: step.Name,
	})

	var output string

	switch step.Type {
	case StepLLM:
		output, err = e.executeLLM(ctx, step)
	case StepTool:
		output, err = e.executeTool(ctx, step)
	case StepFinish:
		output = "finished"
	default:
		err = fmt.Errorf("unknown step type: %s", step.Type)
	}

	if err != nil {
		return e.handleFailure(step, err)
	}

	current, err := e.store.GetStep(step.ID)
	if err != nil {
		return err
	}

	if err := e.store.UpdateStepCAS(step.ID, current.Version, func(s *AgentStep) {
		s.Status = StepSuccess
		s.Output = output
		s.FinishedAt = time.Now()
	}); err != nil {
		return err
	}

	_ = e.checkpoint(step.RunID, step.ID)

	e.emit(Event{
		ID: newID("event"), RunID: step.RunID, StepID: step.ID,
		Type: "STEP_SUCCESS", Timestamp: time.Now(), Message: "step completed",
	})

	return nil
}

func (e *Executor) executeLLM(ctx context.Context, step *AgentStep) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return e.llm.Generate(ctx, step.Input)
}

func (e *Executor) executeTool(ctx context.Context, step *AgentStep) (string, error) {
	key := fmt.Sprintf("%s:%s", step.RunID, step.ID)

	if result, ok := e.store.GetToolResult(key); ok {
		return result, nil
	}

	tool, err := e.tools.Get(step.Name)
	if err != nil {
		return "", err
	}

	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	output, err := tool.Execute(ctx, step.Input)
	if err != nil {
		return "", err
	}

	if err := e.store.SaveToolResult(key, output); err != nil {
		return "", err
	}

	return output, nil
}

func (e *Executor) handleFailure(step *AgentStep, cause error) error {
	const maxAttempts = 3

	current, err := e.store.GetStep(step.ID)
	if err != nil {
		return err
	}

	if current.Attempt >= maxAttempts {
		return e.store.UpdateStepCAS(step.ID, current.Version, func(s *AgentStep) {
			s.Status = StepFailed
			s.FinishedAt = time.Now()
		})
	}

	delay := time.Duration(1<<current.Attempt) * 100 * time.Millisecond
	delay += time.Duration(rand.Intn(100)) * time.Millisecond

	e.emit(Event{
		ID: newID("event"), RunID: step.RunID, StepID: step.ID,
		Type: "STEP_RETRY", Timestamp: time.Now(),
		Message: fmt.Sprintf("retry after error=%v delay=%s", cause, delay),
	})

	time.Sleep(delay)

	current, err = e.store.GetStep(step.ID)
	if err != nil {
		return err
	}

	return e.store.UpdateStepCAS(step.ID, current.Version, func(s *AgentStep) {
		s.Status = StepPending
		s.Attempt++
	})
}

func (e *Executor) checkpoint(runID, stepID string) error {
	run, err := e.store.GetRun(runID)
	if err != nil {
		return err
	}

	return e.store.SaveCheckpoint(&Checkpoint{
		RunID:       runID,
		StepID:      stepID,
		Version:     run.Version,
		Completed:   []string{stepID},
		CurrentStep: stepID,
		CreatedAt:   time.Now(),
	})
}

func (e *Executor) emit(event Event) {
	e.store.AddEvent(event)
}
