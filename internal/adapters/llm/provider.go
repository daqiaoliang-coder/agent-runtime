package llmadapter

import (
	"agent-runtime/internal/contracts"
	"agent-runtime/internal/llm"
	"context"
)

type Provider struct {
	Client llm.Client
}

func New(c llm.Client) *Provider { return &Provider{Client: c} }

func (p *Provider) Generate(ctx context.Context, req contracts.GenerateRequest) (contracts.GenerateResponse, error) {
	msgs := make([]llm.Message, 0, len(req.Messages))
	for _, m := range req.Messages {
		msgs = append(msgs, llm.Message{Role: llm.Role(m.Role), Content: m.Content})
	}
	resp, err := p.Client.Complete(ctx, llm.Request{Model: req.Model, Messages: msgs})
	if err != nil {
		return contracts.GenerateResponse{}, err
	}
	return contracts.GenerateResponse{
		Message: contracts.Message{Role: contracts.RoleAssistant, Content: resp.Content},
		Model:   resp.Model,
		Usage: contracts.Usage{
			PromptTokens:     resp.Usage.PromptTokens,
			CompletionTokens: resp.Usage.CompletionTokens,
			TotalTokens:      resp.Usage.TotalTokens,
		},
	}, nil
}

// Stream is intentionally implemented as a compatibility stream: legacy llm.Client
// implementations are request/response only. Native streaming adapters can replace this
// implementation without changing the Runtime contract.
func (p *Provider) Stream(ctx context.Context, req contracts.GenerateRequest) (<-chan contracts.ModelEvent, error) {
	resp, err := p.Generate(ctx, req)
	if err != nil {
		return nil, err
	}
	ch := make(chan contracts.ModelEvent, 2)
	ch <- contracts.ModelEvent{Type: contracts.ModelEventTextDelta, Delta: resp.Message.Content}
	ch <- contracts.ModelEvent{Type: contracts.ModelEventCompleted, Usage: resp.Usage}
	close(ch)
	return ch, nil
}
