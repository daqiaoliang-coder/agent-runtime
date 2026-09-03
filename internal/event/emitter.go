package event

import (
	"agent-runtime/internal/contracts"
	"context"
	"sync"
)

// Sink is the runtime-facing event output. Implementations may forward events to
// SSE/WebSocket, MQ, logs, or an observability pipeline.
type Sink interface {
	Emit(context.Context, contracts.RuntimeEvent) error
}

// ChannelEmitter is a bounded in-process event stream useful for API/SSE bridges,
// tests, and local demos. A slow consumer never blocks the Runtime indefinitely:
// Emit returns ErrBackpressure when the buffer is full.
type ChannelEmitter struct {
	ch   chan contracts.RuntimeEvent
	once sync.Once
}

func NewChannelEmitter(buffer int) *ChannelEmitter {
	if buffer < 1 {
		buffer = 1
	}
	return &ChannelEmitter{ch: make(chan contracts.RuntimeEvent, buffer)}
}

func (e *ChannelEmitter) Emit(ctx context.Context, ev contracts.RuntimeEvent) error {
	select {
	case e.ch <- ev:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	default:
		return ErrBackpressure
	}
}

func (e *ChannelEmitter) Events() <-chan contracts.RuntimeEvent { return e.ch }

func (e *ChannelEmitter) Close() { e.once.Do(func() { close(e.ch) }) }
