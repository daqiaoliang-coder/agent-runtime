package main

import (
	"context"
	"fmt"
)

type Task struct {
	StepID string
}

// Scheduler 是一个固定大小的 worker pool，任务通过带缓冲的 channel 派发。
type Scheduler struct {
	queue    chan Task
	workers  int
	executor *Executor
}

// NewScheduler workers 决定并发度，队列容量 1000 起到削峰作用。
func NewScheduler(workers int, executor *Executor) *Scheduler {
	return &Scheduler{
		queue:    make(chan Task, 1000),
		workers:  workers,
		executor: executor,
	}
}

func (s *Scheduler) Start(ctx context.Context) {
	for i := 0; i < s.workers; i++ {
		go s.worker(ctx, i)
	}
}

func (s *Scheduler) Submit(task Task) {
	s.queue <- task
}

func (s *Scheduler) worker(ctx context.Context, workerID int) {
	for {
		select {
		case <-ctx.Done():
			return
		case task := <-s.queue:
			if err := s.executor.Execute(ctx, task.StepID); err != nil {
				fmt.Printf("worker=%d step=%s error=%v\n", workerID, task.StepID, err)
			}
		}
	}
}
