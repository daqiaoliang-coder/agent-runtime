package worker

import (
	"agent-runtime/internal/event"
	"agent-runtime/internal/model"
	"agent-runtime/internal/queue"
	"agent-runtime/internal/store"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"
)

type Worker struct {
	Store  *store.MySQL
	Queue  *queue.RedisQueue
	Events *event.RocketMQ
	ID     string
}

func (w *Worker) Handle(ctx context.Context, t model.Task) error {
	n, err := w.Store.GetNode(ctx, t.NodeID)
	if err != nil {
		return err
	}
	ok, err := w.Store.ClaimNode(ctx, n.ID, n.Version, w.ID, 30*time.Second)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	n, _ = w.Store.GetNode(ctx, n.ID)
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	// Keep the lease alive while a long-running LLM/tool call is executing.
	heartbeat := time.NewTicker(10 * time.Second)
	defer heartbeat.Stop()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-heartbeat.C:
				_, _ = w.Store.RenewLease(ctx, n.ID, w.ID, n.Version, 30*time.Second)
			}
		}
	}()

	output := fmt.Sprintf("executed %s/%s input=%s", n.Type, n.Name, n.Input)
	e := model.Event{ID: fmt.Sprintf("event-%s-%d", n.ID, time.Now().UnixNano()), Type: "AgentStepCompleted", RunID: n.RunID, NodeID: n.ID, Attempt: n.Attempt, Output: output, Timestamp: time.Now()}
	payload, _ := json.Marshal(e)
	_, err = w.Store.CompleteNodeWithOutbox(ctx, n, output, model.OutboxMessage{ID: e.ID, EventType: e.Type, AggregateID: n.RunID, Payload: string(payload)})
	if err != nil {
		return err
	}
	return nil
}
func NewFromEnv(s *store.MySQL, q *queue.RedisQueue, r *event.RocketMQ) *Worker {
	id := os.Getenv("WORKER_ID")
	if id == "" {
		id = fmt.Sprintf("worker-%d", time.Now().UnixNano())
	}
	return &Worker{Store: s, Queue: q, Events: r, ID: id}
}
