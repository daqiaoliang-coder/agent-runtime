package providers

import (
	"agent-runtime/internal/contracts"
	"context"
)

type ModelProvider interface {
	Generate(context.Context, contracts.GenerateRequest) (contracts.GenerateResponse, error)
	Stream(context.Context, contracts.GenerateRequest) (<-chan contracts.ModelEvent, error)
}
