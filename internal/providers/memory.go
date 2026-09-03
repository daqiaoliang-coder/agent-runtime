package providers

import (
	"agent-runtime/internal/contracts"
	"context"
)

type MemoryProvider interface {
	Load(context.Context, contracts.ExecutionContext) ([]contracts.Message, error)
	Save(context.Context, contracts.ExecutionContext, []contracts.Message) error
}
