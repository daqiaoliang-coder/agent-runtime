package main

import "time"

type RunStatus string

const (
	RunPending         RunStatus = "PENDING"
	RunRunning         RunStatus = "RUNNING"
	RunSuccess         RunStatus = "SUCCESS"
	RunFailed          RunStatus = "FAILED"
	RunCancelRequested RunStatus = "CANCEL_REQUESTED"
	RunCancelled       RunStatus = "CANCELLED"
)

type StepStatus string

const (
	StepPending   StepStatus = "PENDING"
	StepRunning   StepStatus = "RUNNING"
	StepSuccess   StepStatus = "SUCCESS"
	StepFailed    StepStatus = "FAILED"
	StepCancelled StepStatus = "CANCELLED"
)

type StepType string

const (
	StepLLM    StepType = "LLM"
	StepTool   StepType = "TOOL"
	StepFinish StepType = "FINISH"
)

type AgentRun struct {
	ID           string
	AgentID      string
	Status       RunStatus
	Version      int64
	CurrentStep  string
	Input        string
	Output       string
	MaxSteps     int
	Steps        int
	CreatedAt    time.Time
	UpdatedAt    time.Time
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
	Attempt    int
	Version    int64
	CreatedAt  time.Time
	StartedAt  time.Time
	FinishedAt time.Time
}

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
