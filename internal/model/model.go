package model

import "time"

type RunStatus string

const (
	RunPending   RunStatus = "PENDING"
	RunRunning   RunStatus = "RUNNING"
	RunSuccess   RunStatus = "SUCCESS"
	RunFailed    RunStatus = "FAILED"
	RunCancelled RunStatus = "CANCELLED"
)

type NodeStatus string

const (
	NodePending NodeStatus = "PENDING"
	NodeReady   NodeStatus = "READY"
	NodeRunning NodeStatus = "RUNNING"
	NodeSuccess NodeStatus = "SUCCESS"
	NodeFailed  NodeStatus = "FAILED"
)

type NodeType string

const (
	NodeLLM      NodeType = "LLM"
	NodeTool     NodeType = "TOOL"
	NodeSubAgent NodeType = "SUB_AGENT"
)

type Run struct {
	ID, TenantID, AgentID        string
	Status                       RunStatus
	Version                      int64
	Input, Output, CurrentNodeID string
	MaxSteps, Steps              int
	CreatedAt, UpdatedAt         time.Time
}

type Node struct {
	ID, RunID, ParentNodeID          string
	Type                             NodeType
	Name, Input, Output              string
	Status                           NodeStatus
	Attempt                          int
	Version                          int64
	LeaseOwner                       string
	LeaseUntil                       *time.Time
	CreatedAt, StartedAt, FinishedAt time.Time
}

type PlanNode struct {
	ID, ParentNodeID string
	Type             NodeType
	Name, Input      string
	DependsOn        []string
}
type Plan struct{ Nodes []PlanNode }

type Task struct {
	RunID, NodeID string
	Attempt       int
}

type Event struct {
	ID, Type, RunID, NodeID string
	Attempt                 int
	Output, Error           string
	Timestamp               time.Time
}

type OutboxMessage struct{ ID, EventType, AggregateID, Payload string }
