// Package model 定义 Agent Runtime 的核心领域模型。
// 这些模型以 MySQL 为持久化载体，同时作为各组件之间传递的统一数据结构。
package model

import "time"

// RunStatus 表示一次 Agent 运行（Run）的生命周期状态。
type RunStatus string

const (
	RunPending   RunStatus = "PENDING"   // 已创建，尚未开始调度
	RunRunning   RunStatus = "RUNNING"   // 运行中
	RunSuccess   RunStatus = "SUCCESS"   // 全部节点成功完成
	RunFailed    RunStatus = "FAILED"    // 存在失败节点或运行出错
	RunCancelled RunStatus = "CANCELLED" // 被取消
)

// NodeStatus 表示 DAG 中单个节点（步骤）的状态。
// PENDING -> READY -> RUNNING -> SUCCESS/FAILED 的状态机流转由 store 层 CAS 保护。
type NodeStatus string

const (
	NodePending NodeStatus = "PENDING" // 已入库，依赖尚未满足
	NodeReady   NodeStatus = "READY"   // 依赖就绪，可被投递到队列
	NodeRunning NodeStatus = "RUNNING" // 已被 worker 认领并执行中
	NodeSuccess NodeStatus = "SUCCESS" // 执行成功
	NodeFailed  NodeStatus = "FAILED"  // 执行失败
)

// NodeType 表示节点的执行类型，决定 worker 如何处理该节点。
type NodeType string

const (
	NodeLLM      NodeType = "LLM"       // LLM 推理节点
	NodeTool     NodeType = "TOOL"      // 工具调用节点
	NodeSubAgent NodeType = "SUB_AGENT" // 子 Agent 节点
)

// Run 是一次完整的 Agent 运行记录，对应 agent_run 表。
// Version 用于乐观锁（CAS），避免并发更新覆盖。
type Run struct {
	ID, TenantID, AgentID        string
	Status                       RunStatus
	Version                      int64
	Input, Output, CurrentNodeID string
	MaxSteps, Steps              int
	CreatedAt, UpdatedAt         time.Time
}

// Node 是 DAG 中的一个执行节点，对应 agent_node 表。
// LeaseOwner/LeaseUntil 实现租约机制，用于崩溃恢复。
// TenantID 用于节点级多租户隔离，所有节点查询均需带 tenant_id 过滤。
type Node struct {
	ID, RunID, ParentNodeID, TenantID string
	Type                              NodeType
	Name, Input, Output               string
	Status                            NodeStatus
	Attempt                           int
	Version                           int64
	LeaseOwner                        string
	LeaseUntil                        *time.Time
	CreatedAt, StartedAt, FinishedAt  time.Time
}

// PlanNode 是规划阶段产出的节点描述，尚未落库。
// DependsOn 列出依赖节点 ID，构成 DAG 边。
type PlanNode struct {
	ID, ParentNodeID string
	Type             NodeType
	Name, Input      string
	DependsOn        []string
}

// Plan 是一次规划的结果，包含全部待执行节点。
type Plan struct{ Nodes []PlanNode }

// Task 是投递到 Redis 队列的最小工作单元，仅引用 Run/Node。
// TenantID 跨越异步边界携带租户身份，确保 worker 侧仍可做租户隔离校验。
type Task struct {
	RunID, NodeID, TenantID string
	Attempt                 int
}

// Event 是领域事件，通过 RocketMQ 在组件间传递。
// 典型事件：AgentStepCompleted / AgentStepFailed，用于驱动 DAG 前进。
// TenantID 随事件跨进程传递，Resume Controller 据此做租户隔离。
type Event struct {
	ID, Type, RunID, NodeID, TenantID string
	Attempt                           int
	Output, Error                     string
	Timestamp                         time.Time
}

// OutboxMessage 是 MySQL Outbox 表中的待发布消息。
// 节点完成时与状态变更写入同一事务，保证至少一次投递到 RocketMQ。
type OutboxMessage struct{ ID, EventType, AggregateID, Payload string }

// ToolCall 记录一次工具调用的幂等状态，对应 tool_call 表。
// IdempotencyKey 全局唯一，由 (run_id, node_id, tool_name, input) 派生、跨重试稳定。
// 状态机：RUNNING -> SUCCESS/FAILED。SUCCESS 的记录可被复用以跳过重复副作用。
type ToolCall struct {
	CallID, TenantID, RunID, NodeID, ToolName, IdempotencyKey string
	Status                                                     string
	Output                                                     string
	Attempt                                                    int
}
