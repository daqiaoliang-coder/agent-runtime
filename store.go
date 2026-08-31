package main

import (
	"errors"
	"fmt"
	"sync"
)

// StateStore 是运行时状态的持久化抽象。
// 所有更新都走 CAS（带版本号），保证并发更新不会互相覆盖。
type StateStore interface {
	CreateRun(*AgentRun) error
	GetRun(string) (*AgentRun, error)
	UpdateRunCAS(string, int64, func(*AgentRun)) error

	CreateStep(*AgentStep) error
	GetStep(string) (*AgentStep, error)
	ClaimStep(string, int64) error
	UpdateStepCAS(string, int64, func(*AgentStep)) error

	SaveCheckpoint(*Checkpoint) error
	GetCheckpoint(string) (*Checkpoint, error)

	SaveToolResult(string, string) error
	GetToolResult(string) (string, bool)

	AddEvent(Event)
	GetEvents(string) []Event
}

type MemoryStore struct {
	mu          sync.RWMutex
	runs        map[string]*AgentRun
	steps       map[string]*AgentStep
	checkpoints map[string]*Checkpoint
	toolResults map[string]string
	events      map[string][]Event
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		runs:        make(map[string]*AgentRun),
		steps:       make(map[string]*AgentStep),
		checkpoints: make(map[string]*Checkpoint),
		toolResults: make(map[string]string),
		events:      make(map[string][]Event),
	}
}

func (s *MemoryStore) CreateRun(run *AgentRun) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.runs[run.ID]; ok {
		return errors.New("run already exists")
	}
	cp := *run
	s.runs[run.ID] = &cp
	return nil
}

func (s *MemoryStore) GetRun(id string) (*AgentRun, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	run, ok := s.runs[id]
	if !ok {
		return nil, errors.New("run not found")
	}
	cp := *run
	return &cp, nil
}

// UpdateRunCAS 用乐观锁更新 Run：版本号不匹配说明期间被别人改过，直接报错让调用方重试。
func (s *MemoryStore) UpdateRunCAS(id string, expectedVersion int64, update func(*AgentRun)) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	run, ok := s.runs[id]
	if !ok {
		return errors.New("run not found")
	}
	if run.Version != expectedVersion {
		return fmt.Errorf("version conflict: expected=%d actual=%d", expectedVersion, run.Version)
	}
	update(run)
	run.Version++
	return nil
}

func (s *MemoryStore) CreateStep(step *AgentStep) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.steps[step.ID]; ok {
		return errors.New("step already exists")
	}
	cp := *step
	s.steps[step.ID] = &cp
	return nil
}

func (s *MemoryStore) GetStep(id string) (*AgentStep, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	step, ok := s.steps[id]
	if !ok {
		return nil, errors.New("step not found")
	}
	cp := *step
	return &cp, nil
}

// ClaimStep 把步骤从 Pending 抢到 Running，是 worker 之间互斥执行同一步骤的关键。
// 抢不到（版本冲突或状态已变）就返回 error，调用方应静默退出。
func (s *MemoryStore) ClaimStep(id string, expectedVersion int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	step, ok := s.steps[id]
	if !ok {
		return errors.New("step not found")
	}
	if step.Version != expectedVersion {
		return errors.New("version conflict")
	}
	if step.Status != StepPending {
		return fmt.Errorf("step cannot be claimed from %s", step.Status)
	}

	step.Status = StepRunning
	step.Version++
	return nil
}

func (s *MemoryStore) UpdateStepCAS(id string, expectedVersion int64, update func(*AgentStep)) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	step, ok := s.steps[id]
	if !ok {
		return errors.New("step not found")
	}
	if step.Version != expectedVersion {
		return errors.New("version conflict")
	}

	update(step)
	step.Version++
	return nil
}

// SaveCheckpoint 只接受比当前版本更新的快照，旧版本直接丢弃，避免回退。
func (s *MemoryStore) SaveCheckpoint(cp *Checkpoint) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	existing := s.checkpoints[cp.RunID]
	if existing != nil && existing.Version >= cp.Version {
		return nil
	}

	copyCP := *cp
	copyCP.Completed = append([]string(nil), cp.Completed...)
	s.checkpoints[cp.RunID] = &copyCP
	return nil
}

func (s *MemoryStore) GetCheckpoint(runID string) (*Checkpoint, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	cp, ok := s.checkpoints[runID]
	if !ok {
		return nil, errors.New("checkpoint not found")
	}
	copyCP := *cp
	copyCP.Completed = append([]string(nil), cp.Completed...)
	return &copyCP, nil
}

func (s *MemoryStore) SaveToolResult(key, output string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.toolResults[key]; exists {
		return nil
	}
	s.toolResults[key] = output
	return nil
}

func (s *MemoryStore) GetToolResult(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result, ok := s.toolResults[key]
	return result, ok
}

func (s *MemoryStore) AddEvent(event Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events[event.RunID] = append(s.events[event.RunID], event)
}

func (s *MemoryStore) GetEvents(runID string) []Event {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return append([]Event(nil), s.events[runID]...)
}
