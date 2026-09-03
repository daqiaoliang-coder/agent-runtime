package providers

import "context"

type SandboxProvider interface {
	CreateSession(context.Context) (string, error)
	Execute(context.Context, string, string) (string, error)
}
