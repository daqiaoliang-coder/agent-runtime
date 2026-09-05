// Package contracts contains the stable semantic contracts exposed by the Agent Runtime.
// It deliberately does not depend on a concrete model/agent SDK so adapters can change
// without changing the durable runtime kernel.
package contracts

import "time"

// Role 标识对话消息的来源角色，对齐 OpenAI 风格的 chat completion 语义。
type Role string

const (
	RoleSystem    Role = "system"    // 系统指令，设定 Agent 行为约束
	RoleUser      Role = "user"      // 用户输入
	RoleAssistant Role = "assistant" // LLM 输出
	RoleTool      Role = "tool"       // 工具调用结果回填给 LLM
)

// Message 是 chat completion 风格的单条消息，Role 与 Content 成对出现。
type Message struct {
	Role    Role
	Content string
}

// Usage 记录单次模型调用的 token 用量，用于成本归集与配额统计。
type Usage struct {
	PromptTokens     int // 输入 token（含历史消息）
	CompletionTokens int // 输出 token
	TotalTokens      int // 合计
}

// ToolDefinition 描述 Agent 可调用的工具元信息，供 LLM 决定何时调用。
// InputSchema 为 JSON Schema 字节，约束工具入参结构。
type ToolDefinition struct {
	Name        string
	Description string
	InputSchema []byte
}

// ToolCall 是 LLM 在推理中请求执行的工具调用，ID 用于关联对应 ToolResult。
type ToolCall struct {
	ID        string
	Name      string
	Arguments string
}

// GenerateRequest 是一次模型生成请求：模型名、对话历史、可用工具集合。
type GenerateRequest struct {
	Model    string
	Messages []Message
	Tools    []ToolDefinition
}

// GenerateResponse 是一次模型生成的响应：回复消息、token 用量、可能附带的工具调用。
type GenerateResponse struct {
	Message   Message
	Model     string
	Usage     Usage
	ToolCalls []ToolCall
}

// ModelEventType 标识流式生成中的事件类型，对应 ModelEvent.Type。
type ModelEventType string

const (
	ModelEventTextDelta ModelEventType = "TEXT_DELTA" // 文本增量
	ModelEventToolCall  ModelEventType = "TOOL_CALL"  // 工具调用请求
	ModelEventUsage     ModelEventType = "USAGE"      // token 用量
	ModelEventCompleted ModelEventType = "COMPLETED"  // 流结束
)

// ModelEvent 是流式模型生成的事件单元，按 Type 决定读取哪个字段。
type ModelEvent struct {
	Type     ModelEventType
	Delta    string
	ToolCall *ToolCall
	Usage    Usage
}

// ToolCallRequest 是执行器调用工具时的请求，CallID 用于幂等关联。
type ToolCallRequest struct {
	CallID    string
	Name      string
	Arguments string
}

// ToolResult 是工具调用的返回，IsError 标记工具侧自身报错（区别于传输/执行异常）。
type ToolResult struct {
	CallID  string
	Output  string
	IsError bool
}

// ExecutionContext 贯穿一次 Run 的执行上下文，携带多租户与追踪维度，
// 供 middleware / provider / 事件发射器统一读取身份信息。
type ExecutionContext struct {
	TenantID string
	UserID   string
	ThreadID string
	RunID    string
	TraceID  string
}

// RuntimeEventType 标识运行时事件类型，驱动前端流式 UI 与 DAG 推进。
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

// RuntimeEvent 是运行时对外发布的事件，跨进程边界传递 Run/Node 进度与流式数据。
// Data 字段携带与 Type 相关的结构化数据（如 delta 文本、工具调用详情）。
type RuntimeEvent struct {
	ID        string
	RunID     string
	NodeID    string
	TenantID  string
	Type      RuntimeEventType
	Timestamp time.Time
	Data      any
}

// AgentRuntimeConfig 描述一次 Agent 实例的静态配置，供工厂层据此装配 LLM/工具/MCP/Skill。
type AgentRuntimeConfig struct {
	AgentType string
	Model     string
	Tools     []string
	MCP       []string
	Skills    []string
	Memory    string
	Prompt    string
}
