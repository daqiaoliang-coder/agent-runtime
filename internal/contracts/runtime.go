// Package contracts contains the stable semantic contracts exposed by the Agent Runtime.
// It deliberately does not depend on a concrete model/agent SDK so adapters can change
// without changing the durable runtime kernel.
package contracts

import "time"

type Role string

const (
	RoleSystem    Role = "system"
	RoleUser      Role = "user"
	RoleAssistant Role = "assistant"
	RoleTool      Role = "tool"
)

type Message struct {
	Role    Role
	Content string
}

type Usage struct {
	PromptTokens     int
	CompletionTokens int
	TotalTokens      int
}

type ToolDefinition struct {
	Name        string
	Description string
	InputSchema []byte
}

type ToolCall struct {
	ID        string
	Name      string
	Arguments string
}

type GenerateRequest struct {
	Model    string
	Messages []Message
	Tools    []ToolDefinition
}

type GenerateResponse struct {
	Message   Message
	Model     string
	Usage     Usage
	ToolCalls []ToolCall
}

type ModelEventType string

const (
	ModelEventTextDelta ModelEventType = "TEXT_DELTA"
	ModelEventToolCall  ModelEventType = "TOOL_CALL"
	ModelEventUsage     ModelEventType = "USAGE"
	ModelEventCompleted ModelEventType = "COMPLETED"
)

type ModelEvent struct {
	Type     ModelEventType
	Delta    string
	ToolCall *ToolCall
	Usage    Usage
}

type ToolCallRequest struct {
	CallID    string
	Name      string
	Arguments string
}

type ToolResult struct {
	CallID  string
	Output  string
	IsError bool
}

type ExecutionContext struct {
	TenantID string
	UserID   string
	ThreadID string
	RunID    string
	TraceID  string
}

type RuntimeEventType string

const (
	EventRunStarted    RuntimeEventType = "RUN_STARTED"
	EventRunFinished   RuntimeEventType = "RUN_FINISHED"
	EventRunFailed     RuntimeEventType = "RUN_FAILED"
	EventRunCancelled  RuntimeEventType = "RUN_CANCELLED"
	EventNodeStarted   RuntimeEventType = "NODE_STARTED"
	EventNodeFinished  RuntimeEventType = "NODE_FINISHED"
	EventNodeFailed    RuntimeEventType = "NODE_FAILED"
	EventTextStart     RuntimeEventType = "TEXT_START"
	EventTextDelta     RuntimeEventType = "TEXT_DELTA"
	EventTextEnd       RuntimeEventType = "TEXT_END"
	EventToolCall      RuntimeEventType = "TOOL_CALL"
	EventToolResult    RuntimeEventType = "TOOL_RESULT"
	EventReasoning     RuntimeEventType = "REASONING"
	EventHITLRequested RuntimeEventType = "HITL_REQUESTED"
	EventHITLResumed   RuntimeEventType = "HITL_RESUMED"
)

type RuntimeEvent struct {
	ID        string
	RunID     string
	NodeID    string
	TenantID  string
	Type      RuntimeEventType
	Timestamp time.Time
	Data      any
}

type AgentRuntimeConfig struct {
	AgentType string
	Model     string
	Tools     []string
	MCP       []string
	Skills    []string
	Memory    string
	Prompt    string
}
