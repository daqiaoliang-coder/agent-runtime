package main

import "time"

// RunStatus 表示一次 Agent 运行的生命周期状态。
// 注意 RunCancelRequested 是一个中间态：调用方请求取消后，
// 运行主循环需要在下一次检查时才会真正落到 RunCancelled。
type RunStatus string

const (
	RunPending         RunStatus = "PENDING"
	RunRunning         RunStatus = "RUNNING"
	RunSuccess         RunStatus = "SUCCESS"
	RunFailed          RunStatus = "FAILED"
	RunCancelRequested RunStatus = "CANCEL_REQUESTED" // 已收到取消请求，等待主循环处理
	RunCancelled       RunStatus = "CANCELLED"
)

// StepStatus 表示单个步骤的执行状态。
type StepStatus string

const (
	StepPending   StepStatus = "PENDING"
	StepRunning   StepStatus = "RUNNING"
	StepSuccess   StepStatus = "SUCCESS"
	StepFailed    StepStatus = "FAILED"
	StepCancelled StepStatus = "CANCELLED"
)

// StepType 描述步骤的种类：调 LLM、调外部工具、还是直接结束。
type StepType string

const (
	StepLLM    StepType = "LLM"
	StepTool   StepType = "TOOL"
	StepFinish StepType = "FINISH"
)

type AgentRun struct {
	ID          string
	AgentID     string
	Status      RunStatus
	Version     int64 // 乐观锁版本号，每次 CAS 更新自增
	CurrentStep string
	Input       string
	Output      string
	MaxSteps    int
	Steps       int
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

type AgentStep struct {
	ID         string
	RunID      string
	Parent     string
	Type       StepType
	Name       string
	Input      string
	Output     string
	Status     StepStatus
	Attempt    int // 已重试次数，达到上限后直接判失败
	Version    int64
	CreatedAt  time.Time
	StartedAt  time.Time
	FinishedAt time.Time
}

// Checkpoint 是一次运行在某个步骤上的快照，用于断点恢复。
// SaveCheckpoint 会按 Version 单调递增地写入，旧版本会被忽略。
type Checkpoint struct {
	RunID       string
	StepID      string
	Version     int64
	Completed   []string
	CurrentStep string
	CreatedAt   time.Time
}

type Event struct {
	ID        string
	RunID     string
	StepID    string
	Type      string
	Timestamp time.Time
	Message   string
}

type PlanAction struct {
	Type  StepType
	Name  string
	Input string
}
