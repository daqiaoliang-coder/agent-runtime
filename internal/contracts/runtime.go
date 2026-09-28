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
	RoleTool      Role = "tool"      // 工具调用结果回填给 LLM
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
	// CacheKey 是透传给网关的 prompt 前缀缓存键（如 OpenAI 兼容网关的
	// prompt_cache_key）。为空表示不透传。同一 Run 内保持稳定，使 append-only
	// 的历史前缀复用服务端 KV 缓存；是否生成该键由执行器侧开关决定。
	CacheKey string
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
//
// NodeID 为当前执行的节点标识，仅在节点级执行路径上有值（Run 级操作为空）。
// 加这个字段的必要性来自人工闸门：护栏检测到中危要转人工时，
// 审批记录必须落到具体节点，否则人工放行后无法知道该重跑哪一步——
// Run 级的 current_node_id 会被并行节点相互覆盖，不能作为审批锚点。
//
// UserID 的来源是认证层：入口进程用 Authenticator 校验凭证得到 Identity（见 auth.go），
// 身份随 agent_run.user_id 落库，worker 执行节点时读库回填本字段。
// 未配置认证时为空 —— 空值意味着"身份未经证明"，依赖身份的授权判定应当拒绝而非放行。
//
// 刻意不在此携带原始凭证：EC 会进审计记录、随 Run 落库、跨进程传递，
// 凭证一旦进入这些通道就等于泄露。原始 token 只在进程内 ctx 中传递，
// 见 auth.go 的 WithAuthToken / AuthTokenFrom 及其取舍说明。
//
// AuthMethod / Scopes 是身份的**属性**而非凭证本身，可以安全地随 EC 流转并落审计：
// AuthMethod 让下游与审计消费方知道"这个身份是被怎么认证的"（强机制还是弱机制），
// Scopes 让 Tool 侧能做权限判定。两者都不含可被复用来冒充身份的秘密材料。
type ExecutionContext struct {
	TenantID string
	UserID   string
	// AuthMethod 记录 UserID 是被哪种机制认证的（jwt-hs256 / jwt-rs256 / static-token）。
	// 空串表示身份未经凭证校验；依赖身份的判定应把空 UserID 当作拒绝。
	AuthMethod string
	// Scopes 是身份的权限范围，为空表示凭证未声明范围。
	// 由入口进程认证时从 Identity 填充；worker 侧从 agent_run 还原身份时
	// 只能恢复 UserID/TenantID/AuthMethod（Scopes 不落库），因此 worker 侧此字段常为空。
	Scopes   []string
	ThreadID string
	RunID    string
	NodeID   string
	TraceID  string
}

// RuntimeEventType 标识运行时事件类型，驱动前端流式 UI 与 DAG 推进。
type RuntimeEventType string

const (
	EventRunStarted      RuntimeEventType = "RUN_STARTED"
	EventRunFinished     RuntimeEventType = "RUN_FINISHED"
	EventRunFailed       RuntimeEventType = "RUN_FAILED"
	EventRunCancelled    RuntimeEventType = "RUN_CANCELLED"
	EventReplanRequested RuntimeEventType = "REPLAN_REQUESTED"
	EventNodeStarted     RuntimeEventType = "NODE_STARTED"
	EventNodeFinished    RuntimeEventType = "NODE_FINISHED"
	EventNodeFailed      RuntimeEventType = "NODE_FAILED"
	EventTextStart       RuntimeEventType = "TEXT_START"
	EventTextDelta       RuntimeEventType = "TEXT_DELTA"
	EventTextEnd         RuntimeEventType = "TEXT_END"
	EventToolCall        RuntimeEventType = "TOOL_CALL"
	EventToolResult      RuntimeEventType = "TOOL_RESULT"
	EventReasoning       RuntimeEventType = "REASONING"
	EventHITLRequested   RuntimeEventType = "HITL_REQUESTED"
	EventHITLResumed     RuntimeEventType = "HITL_RESUMED"
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
