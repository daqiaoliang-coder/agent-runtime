package middleware

import (
	"agent-runtime/internal/contracts"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	ErrRateLimited     = errors.New("middleware: tool rate limit exceeded")
	ErrCircuitOpen     = errors.New("middleware: tool circuit breaker open")
	ErrCircuitHalfOpen = errors.New("middleware: tool circuit breaker probe in progress")
)

// ToolAuthorizer enforces per-tool scopes before a tool call is claimed or executed.
// An absent rule means the tool is allowed after identity validation; RequireIdentity
// can make anonymous calls fail closed for deployments that require authentication.
type ToolAuthorizer struct {
	RequiredScopes  map[string][]string
	RequireIdentity bool
}

var _ Tool = (*ToolAuthorizer)(nil)

func (a *ToolAuthorizer) Before(_ context.Context, ec contracts.ExecutionContext, req contracts.ToolCallRequest) (contracts.ToolCallRequest, error) {
	if a.RequireIdentity && ec.UserID == "" {
		return req, fmt.Errorf("tool %q: %w", req.Name, contracts.ErrUnauthenticated)
	}
	for _, scope := range a.RequiredScopes[req.Name] {
		if ec.UserID == "" || !(contracts.Identity{UserID: ec.UserID, Scopes: ec.Scopes}).HasScope(scope) {
			return req, fmt.Errorf("tool %q: %w", req.Name, contracts.ErrInsufficientScope)
		}
	}
	return req, nil
}

func (*ToolAuthorizer) After(_ context.Context, _ contracts.ExecutionContext, _ contracts.ToolCallRequest, result contracts.ToolResult) (contracts.ToolResult, error) {
	return result, nil
}

// ToolAuditRecord contains only identity and outcome metadata. It deliberately omits
// arguments and output because both can contain secrets or attacker-controlled data.
type ToolAuditRecord struct {
	TenantID, UserID, RunID, NodeID string
	ToolName, CallID                string
	Allowed, IsError                bool
	Reason                          string
	At                              time.Time
}

// ToolAudit emits an attempt record before execution and a completion record afterward.
// Place it before short-circuiting authorizers if denied attempts should be visible.
// The callback should be fast and must not block tool execution.
type ToolAudit struct {
	OnAudit func(context.Context, ToolAuditRecord)
	Now     func() time.Time
}

var _ Tool = (*ToolAudit)(nil)

func (a *ToolAudit) Before(ctx context.Context, ec contracts.ExecutionContext, req contracts.ToolCallRequest) (contracts.ToolCallRequest, error) {
	a.emit(ctx, ec, req, true, false, "")
	return req, nil
}

func (a *ToolAudit) After(ctx context.Context, ec contracts.ExecutionContext, req contracts.ToolCallRequest, result contracts.ToolResult) (contracts.ToolResult, error) {
	reason := ""
	if result.IsError {
		reason = "tool_error"
	}
	a.emit(ctx, ec, req, true, result.IsError, reason)
	return result, nil
}

func (a *ToolAudit) emit(ctx context.Context, ec contracts.ExecutionContext, req contracts.ToolCallRequest, allowed, failed bool, reason string) {
	if a.OnAudit == nil {
		return
	}
	now := time.Now
	if a.Now != nil {
		now = a.Now
	}
	a.OnAudit(ctx, ToolAuditRecord{TenantID: ec.TenantID, UserID: ec.UserID, RunID: ec.RunID, NodeID: ec.NodeID, ToolName: req.Name, CallID: req.CallID, Allowed: allowed, IsError: failed, Reason: reason, At: now()})
}

// RateLimiter is a process-local token bucket, keyed by tenant and tool. A zero or
// negative rate disables it; Burst defaults to one token when omitted.
type RateLimiter struct {
	Rate    float64 // tokens per second
	Burst   int
	Now     func() time.Time
	mu      sync.Mutex
	buckets map[string]tokenBucket
}

type tokenBucket struct {
	tokens float64
	at     time.Time
}

var _ Tool = (*RateLimiter)(nil)

func (r *RateLimiter) Before(_ context.Context, ec contracts.ExecutionContext, req contracts.ToolCallRequest) (contracts.ToolCallRequest, error) {
	if r.Rate <= 0 {
		return req, nil
	}
	now := time.Now
	if r.Now != nil {
		now = r.Now
	}
	burst := r.Burst
	if burst <= 0 {
		burst = 1
	}
	key := ec.TenantID + "\x00" + req.Name
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.buckets == nil {
		r.buckets = make(map[string]tokenBucket)
	}
	b, ok := r.buckets[key]
	if !ok {
		b = tokenBucket{tokens: float64(burst), at: now()}
	}
	t := now()
	if t.After(b.at) {
		b.tokens += t.Sub(b.at).Seconds() * r.Rate
		if b.tokens > float64(burst) {
			b.tokens = float64(burst)
		}
	}
	b.at = t
	if b.tokens < 1 {
		r.buckets[key] = b
		return req, ErrRateLimited
	}
	b.tokens--
	r.buckets[key] = b
	return req, nil
}

func (*RateLimiter) After(_ context.Context, _ contracts.ExecutionContext, _ contracts.ToolCallRequest, result contracts.ToolResult) (contracts.ToolResult, error) {
	return result, nil
}

// CircuitBreaker opens after consecutive tool errors and permits one probe after
// ResetTimeout. It is process-local; configure shared protection at the tool service
// boundary when multiple worker processes must share breaker state.
type CircuitBreaker struct {
	FailureThreshold int
	ResetTimeout     time.Duration
	Now              func() time.Time
	mu               sync.Mutex
	states           map[string]circuitState
}

type circuitState struct {
	failures  int
	openUntil time.Time
	probing   bool
}

var _ Tool = (*CircuitBreaker)(nil)

func (b *CircuitBreaker) Before(_ context.Context, _ contracts.ExecutionContext, req contracts.ToolCallRequest) (contracts.ToolCallRequest, error) {
	threshold := b.FailureThreshold
	if threshold <= 0 {
		threshold = 5
	}
	now := time.Now
	if b.Now != nil {
		now = b.Now
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.states == nil {
		b.states = make(map[string]circuitState)
	}
	s := b.states[req.Name]
	if s.openUntil.IsZero() {
		return req, nil
	}
	t := now()
	if t.Before(s.openUntil) {
		return req, ErrCircuitOpen
	}
	if s.probing {
		return req, ErrCircuitHalfOpen
	}
	s.probing = true
	b.states[req.Name] = s
	return req, nil
}

func (b *CircuitBreaker) After(_ context.Context, _ contracts.ExecutionContext, req contracts.ToolCallRequest, result contracts.ToolResult) (contracts.ToolResult, error) {
	threshold := b.FailureThreshold
	if threshold <= 0 {
		threshold = 5
	}
	timeout := b.ResetTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	now := time.Now
	if b.Now != nil {
		now = b.Now
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.states == nil {
		b.states = make(map[string]circuitState)
	}
	s := b.states[req.Name]
	s.probing = false
	if result.IsError {
		s.failures++
		if s.failures >= threshold {
			s.openUntil = now().Add(timeout)
		}
	} else {
		s.failures = 0
		s.openUntil = time.Time{}
	}
	b.states[req.Name] = s
	return result, nil
}
