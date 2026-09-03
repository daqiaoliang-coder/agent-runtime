// Package react implements a small provider-agnostic ReAct loop.
// The engine owns agent decisions; execution/persistence stays behind StepRunner so
// the same loop can run in-process or be backed by the durable runtime kernel.
package react

import (
	"agent-runtime/internal/contracts"
	"agent-runtime/internal/event"
	"agent-runtime/internal/middleware"
	"agent-runtime/internal/providers"
	"context"
	"fmt"
	"time"
)

type StepRunner interface {
	RunLLM(context.Context, contracts.GenerateRequest) (contracts.GenerateResponse, error)
	RunTool(context.Context, contracts.ToolCallRequest) (contracts.ToolResult, error)
}

type ProviderRunner struct {
	Model providers.ModelProvider
	Tool  providers.ToolProvider
}

func (r ProviderRunner) RunLLM(ctx context.Context, req contracts.GenerateRequest) (contracts.GenerateResponse, error) {
	return r.Model.Generate(ctx, req)
}
func (r ProviderRunner) RunTool(ctx context.Context, req contracts.ToolCallRequest) (contracts.ToolResult, error) {
	return r.Tool.CallTool(ctx, req)
}

type Engine struct {
	Runner        StepRunner
	ToolChain     *middleware.ToolChain
	Events        event.Sink
	MaxIterations int
}

type Input struct {
	ExecutionContext contracts.ExecutionContext
	Messages         []contracts.Message
	Tools            []contracts.ToolDefinition
}

type Result struct {
	Message    contracts.Message
	Iterations int
}

func (e *Engine) Run(ctx context.Context, in Input) (Result, error) {
	if e.Runner == nil {
		return Result{}, fmt.Errorf("react: step runner not configured")
	}
	max := e.MaxIterations
	if max <= 0 {
		max = 8
	}
	messages := append([]contracts.Message(nil), in.Messages...)
	for i := 0; i < max; i++ {
		resp, err := e.Runner.RunLLM(ctx, contracts.GenerateRequest{Messages: messages, Tools: in.Tools})
		if err != nil {
			return Result{}, err
		}
		if len(resp.ToolCalls) == 0 {
			msg := resp.Message
			if msg.Role == "" {
				msg.Role = contracts.RoleAssistant
			}
			messages = append(messages, msg)
			e.emit(ctx, contracts.RuntimeEvent{ID: fmt.Sprintf("react-text-%d", time.Now().UnixNano()), RunID: in.ExecutionContext.RunID, TenantID: in.ExecutionContext.TenantID, Type: contracts.EventTextDelta, Timestamp: time.Now(), Data: map[string]any{"content": msg.Content}})
			return Result{Message: msg, Iterations: i + 1}, nil
		}
		messages = append(messages, resp.Message)
		for _, call := range resp.ToolCalls {
			req := contracts.ToolCallRequest{CallID: call.ID, Name: call.Name, Arguments: call.Arguments}
			if e.ToolChain != nil {
				req, err = e.ToolChain.Before(ctx, in.ExecutionContext, req)
				if err != nil {
					return Result{}, err
				}
			}
			e.emit(ctx, contracts.RuntimeEvent{ID: fmt.Sprintf("react-tool-%d", time.Now().UnixNano()), RunID: in.ExecutionContext.RunID, TenantID: in.ExecutionContext.TenantID, Type: contracts.EventToolCall, Timestamp: time.Now(), Data: req})
			result, err := e.Runner.RunTool(ctx, req)
			if err != nil {
				return Result{}, err
			}
			if e.ToolChain != nil {
				result, err = e.ToolChain.After(ctx, in.ExecutionContext, req, result)
				if err != nil {
					return Result{}, err
				}
			}
			messages = append(messages, contracts.Message{Role: contracts.RoleTool, Content: result.Output})
			e.emit(ctx, contracts.RuntimeEvent{ID: fmt.Sprintf("react-tool-result-%d", time.Now().UnixNano()), RunID: in.ExecutionContext.RunID, TenantID: in.ExecutionContext.TenantID, Type: contracts.EventToolResult, Timestamp: time.Now(), Data: result})
		}
	}
	return Result{}, fmt.Errorf("react: max iterations %d exceeded", max)
}

func (e *Engine) emit(ctx context.Context, ev contracts.RuntimeEvent) {
	if e.Events != nil {
		_ = e.Events.Emit(ctx, ev)
	}
}
