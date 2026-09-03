package providers

import (
	"agent-runtime/internal/contracts"
	"context"
)

type PromptProvider interface {
	Resolve(context.Context, contracts.ExecutionContext, string) (string, error)
}
