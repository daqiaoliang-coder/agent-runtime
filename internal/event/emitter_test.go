package event

import (
	"agent-runtime/internal/contracts"
	"context"
	"testing"
	"time"
)

func TestChannelEmitter_EmitsAndAppliesBackpressure(t *testing.T) {
	e := NewChannelEmitter(1)
	ev := contracts.RuntimeEvent{ID: "1", Type: contracts.EventNodeStarted, Timestamp: time.Now()}
	if err := e.Emit(context.Background(), ev); err != nil {
		t.Fatal(err)
	}
	if err := e.Emit(context.Background(), ev); err != ErrBackpressure {
		t.Fatalf("expected backpressure, got %v", err)
	}
	got := <-e.Events()
	if got.ID != "1" {
		t.Fatalf("unexpected event: %+v", got)
	}
}
