// Package hitl provides the public human-in-the-loop control surface.
// Runtime owns the durable state transition; this package keeps API callers decoupled
// from MySQL and the underlying transaction implementation.
package hitl

import (
	"context"
	"fmt"
)

type Runtime interface {
	Interrupt(context.Context, string, string, string, string) error
	Resume(context.Context, string, string, string) error
}

type Manager struct{ Runtime Runtime }

func (m *Manager) Request(ctx context.Context, tenantID, runID, nodeID, reason string) error {
	if m.Runtime == nil {
		return fmt.Errorf("hitl: runtime not configured")
	}
	return m.Runtime.Interrupt(ctx, tenantID, runID, nodeID, reason)
}

func (m *Manager) Resume(ctx context.Context, tenantID, runID, decision string) error {
	if m.Runtime == nil {
		return fmt.Errorf("hitl: runtime not configured")
	}
	return m.Runtime.Resume(ctx, tenantID, runID, decision)
}
