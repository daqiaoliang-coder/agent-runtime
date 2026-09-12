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

// Events 返回只读通道，消费者从中读取事件以桥接到 SSE/WebSocket 等前端流。
// 通道在 Close 后关闭，消费者应配合 for-range 处理。
func (e *ChannelEmitter) Events() <-chan contracts.RuntimeEvent { return e.ch }

// Close 关闭事件通道，幂等（多次调用安全）。关闭后消费者读取将读到零值并退出循环。
func (e *ChannelEmitter) Close() { e.once.Do(func() { close(e.ch) }) }
