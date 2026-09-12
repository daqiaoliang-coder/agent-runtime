// Package policy provides an extensible policy decision point between
// Agent planning and tool execution.
//
// The package deliberately owns no persistence and no executor implementation.
// It can therefore be reused by shell, filesystem, MCP, HTTP and other tools.
package policy

import (
	"context"
	"strings"
)

type Decision string

const (
	Allow           Decision = "allow"
	RequireApproval Decision = "require_approval"
	Deny            Decision = "deny"
)

type RiskLevel string

const (
	RiskLow      RiskLevel = "low"
	RiskMedium   RiskLevel = "medium"
	RiskHigh     RiskLevel = "high"
	RiskCritical RiskLevel = "critical"
)

type Request struct {
	TenantID string
	UserID   string
	RunID    string
	NodeID   string
	ToolName string
	Input    string
}

type DecisionResult struct {
	Decision Decision
	Risk     RiskLevel
	PolicyID string
	Reason   string
}

type Policy interface {
	Evaluate(context.Context, Request) (DecisionResult, error)
}

type Func func(context.Context, Request) (DecisionResult, error)

func (f Func) Evaluate(ctx context.Context, req Request) (DecisionResult, error) {
	return f(ctx, req)
}

// Chain combines policies with the safety ordering:
// DENY > REQUIRE_APPROVAL > ALLOW.
// A more restrictive policy can therefore never be bypassed by a permissive one.
type Chain struct {
	policies []Policy
}

func NewChain(policies ...Policy) *Chain {
	return &Chain{policies: policies}
}

func (c *Chain) Evaluate(ctx context.Context, req Request) (DecisionResult, error) {
	final := DecisionResult{
		Decision: Allow,
		Risk:     RiskLow,
		PolicyID: "default",
		Reason:   "no policy matched",
	}

	for _, p := range c.policies {
		if p == nil {
			continue
		}
		result, err := p.Evaluate(ctx, req)
		if err != nil {
			return DecisionResult{}, err
		}
		if decisionRank(result.Decision) > decisionRank(final.Decision) ||
			(decisionRank(result.Decision) == decisionRank(final.Decision) &&
				riskRank(result.Risk) > riskRank(final.Risk)) {
			final = result
		}
	}
	return final, nil
}

func decisionRank(d Decision) int {
	switch d {
	case Deny:
		return 3
	case RequireApproval:
		return 2
	default:
		return 1
	}
}

func riskRank(r RiskLevel) int {
	switch r {
	case RiskCritical:
		return 4
	case RiskHigh:
		return 3
	case RiskMedium:
		return 2
	default:
		return 1
	}
}

// CommandPolicy is a conservative default policy for coding-agent tools.
// It is intentionally not a sandbox: production deployments should add
// filesystem, network, resource and capability policies.
type CommandPolicy struct {
	ApprovalTools []string
	DenyTokens    []string
}

func DefaultCommandPolicy() CommandPolicy {
	return CommandPolicy{
		ApprovalTools: []string{
			"shell", "exec", "subprocess", "terminal",
			"kubectl", "git",
		},
		DenyTokens: []string{
			"rm -rf /",
			"mkfs",
			"dd if=",
			":(){:|:&};:",
			"shutdown",
			"reboot",
			"kubectl delete namespace",
			"kubectl delete ns",
		},
	}
}

func (p CommandPolicy) Evaluate(_ context.Context, req Request) (DecisionResult, error) {
	name := strings.ToLower(strings.TrimSpace(req.ToolName))
	input := strings.ToLower(strings.TrimSpace(req.Input))

	for _, token := range p.DenyTokens {
		if strings.Contains(input, strings.ToLower(token)) {
			return DecisionResult{
				Decision: Deny,
				Risk:     RiskCritical,
				PolicyID: "command-deny",
				Reason:   "blocked high-risk command token",
			}, nil
		}
	}

	for _, tool := range p.ApprovalTools {
		if name == strings.ToLower(tool) {
			return DecisionResult{
				Decision: RequireApproval,
				Risk:     RiskHigh,
				PolicyID: "tool-approval",
				Reason:   "tool requires explicit human approval",
			}, nil
		}
	}

	return DecisionResult{
		Decision: Allow,
		Risk:     RiskLow,
		PolicyID: "command-default",
		Reason:   "tool is not covered by high-risk rules",
	}, nil
}
