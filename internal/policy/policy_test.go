package policy

import (
	"context"
	"testing"
)

func TestCommandPolicy(t *testing.T) {
	p := DefaultCommandPolicy()

	tests := []struct {
		name     string
		req      Request
		expected Decision
		risk     RiskLevel
	}{
		{
			name:     "safe tool",
			req:      Request{ToolName: "search", Input: "golang"},
			expected: Allow,
			risk:     RiskLow,
		},
		{
			name:     "high risk tool",
			req:      Request{ToolName: "shell", Input: "git status"},
			expected: RequireApproval,
			risk:     RiskHigh,
		},
		{
			name:     "critical command",
			req:      Request{ToolName: "shell", Input: "rm -rf /"},
			expected: Deny,
			risk:     RiskCritical,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := p.Evaluate(context.Background(), tt.req)
			if err != nil {
				t.Fatal(err)
			}
			if got.Decision != tt.expected || got.Risk != tt.risk {
				t.Fatalf("got decision=%s risk=%s, want decision=%s risk=%s",
					got.Decision, got.Risk, tt.expected, tt.risk)
			}
		})
	}
}

func TestChainDenyWins(t *testing.T) {
	c := NewChain(
		Func(func(context.Context, Request) (DecisionResult, error) {
			return DecisionResult{Decision: Allow, Risk: RiskLow, PolicyID: "allow"}, nil
		}),
		Func(func(context.Context, Request) (DecisionResult, error) {
			return DecisionResult{Decision: Deny, Risk: RiskCritical, PolicyID: "security"}, nil
		}),
	)

	got, err := c.Evaluate(context.Background(), Request{})
	if err != nil {
		t.Fatal(err)
	}
	if got.Decision != Deny {
		t.Fatalf("got %s, want deny", got.Decision)
	}
}
