//go:build integration

// Package queue 集成测试：验证 Redis Streams 队列在真实 Redis 下的关键可靠性不变量。
// 运行方式：
//
//	go test -tags=integration ./internal/queue/ -run TestIntegration -v
//
// 依赖 deploy/docker-compose.yml 启动的 Redis（默认 6380 端口）。
package queue

import (
	"agent-runtime/internal/model"
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"
)

func testAddr() string {
	if v := os.Getenv("REDIS_ADDR"); v != "" {
		return v
	}
	return "localhost:6380"
}

func newIntegrationQueue(t *testing.T) *RedisQueue {
	t.Helper()
	q := New(testAddr(), "agent-test-stream", "agent-test-group")
	ctx := context.Background()
	if err := q.Ping(ctx); err != nil {
		t.Fatalf("ping redis: %v (是否已 docker compose up?)", err)
	}
	// 清理旧 stream 与 group，保证测试隔离
	_ = q.Client.Del(ctx, "agent-test-stream").Err()
	if err := q.Init(ctx); err != nil {
		t.Fatalf("Init: %v", err)
	}
	return q
}

// TestIntegration_EnqueueAndConsume 验证至少一次投递：
// Enqueue 写入后，Consume 能读到并处理，成功后 XAck。
func TestIntegration_EnqueueAndConsume(t *testing.T) {
	q := newIntegrationQueue(t)
	defer q.Client.Del(context.Background(), "agent-test-stream")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	task := model.Task{RunID: "it-run-1", NodeID: "it-node-1", TenantID: "tenant-it", Attempt: 0}
	if err := q.Enqueue(ctx, task); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// 启动消费者
	got := make(chan model.Task, 1)
	go func() {
		_ = q.Consume(ctx, "test-consumer", func(_ context.Context, t model.Task) error {
			got <- t
			return nil
		})
	}()

	select {
	case received := <-got:
		if received.NodeID != "it-node-1" || received.TenantID != "tenant-it" {
			t.Errorf("received task mismatch: %+v", received)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for message consumption")
	}
}

// TestIntegration_ConsumeFailure_NoAck 验证至少一次语义：
// handler 返回错误时消息不 Ack，留在 PEL 中可被 ReclaimPending 回收。
func TestIntegration_ConsumeFailure_NoAck(t *testing.T) {
	q := newIntegrationQueue(t)
	defer q.Client.Del(context.Background(), "agent-test-stream")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	task := model.Task{RunID: "it-run-2", NodeID: "it-node-2", TenantID: "tenant-it"}
	if err := q.Enqueue(ctx, task); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// 消费者：总是失败，不 Ack，消息留在 PEL
	go func() {
		_ = q.Consume(ctx, "failing-consumer", func(_ context.Context, _ model.Task) error {
			return errFailed
		})
	}()

	// 等待消息进入 PEL
	time.Sleep(1500 * time.Millisecond)
	cancel() // 停止消费者

	// 验证 PEL 中有未确认消息（用独立 ctx，cancel 后的 ctx 会报错）
	pendingInfo, err := q.Client.XPending(context.Background(), "agent-test-stream", "agent-test-group").Result()
	if err != nil {
		t.Fatalf("XPending: %v", err)
	}
	if pendingInfo.Count == 0 {
		t.Fatal("PEL should contain unacked message after handler failure")
	}
	t.Logf("PEL count before reclaim: %d", pendingInfo.Count)

	// ReclaimPending 用 XAutoClaim 回收 idle 超过 500ms 的消息
	// 给足时间让消息 idle
	time.Sleep(600 * time.Millisecond)
	reclaimed, err := q.ReclaimPending(context.Background(), "reclaim-consumer", 500*time.Millisecond, 10)
	if err != nil {
		t.Fatalf("ReclaimPending: %v", err)
	}
	if reclaimed == 0 {
		t.Fatal("ReclaimPending should reclaim at least 1 unacked message")
	}
	t.Logf("reclaimed %d messages", reclaimed)

	// ReclaimPending 内部 Enqueue 新消息 + XAck 旧消息
	// 验证重新入队：stream 长度应增加（原消息仍保留 + 新消息）
	streamLen, err := q.Client.XLen(context.Background(), "agent-test-stream").Result()
	if err != nil {
		t.Fatalf("XLen: %v", err)
	}
	if streamLen < 2 {
		t.Fatalf("stream should have >=2 messages after reclaim (original + re-enqueued), got %d", streamLen)
	}
	t.Logf("stream length after reclaim: %d", streamLen)

	// 验证重新入队的消息可被新消费者读到（用 XRANGE 直接读，避免 XREADGROUP > 的时序问题）
	xrange, err := q.Client.XRange(context.Background(), "agent-test-stream", "-", "+").Result()
	if err != nil {
		t.Fatalf("XRange: %v", err)
	}
	var foundReenqueued bool
	for _, msg := range xrange {
		if raw, ok := msg.Values["task"].(string); ok {
			var tt model.Task
			if json.Unmarshal([]byte(raw), &tt) == nil && tt.NodeID == "it-node-2" {
				foundReenqueued = true
			}
		}
	}
	if !foundReenqueued {
		t.Error("re-enqueued message should be readable from stream")
	}
}

// errFailed 是测试用错误，模拟 handler 失败。
var errFailed = testErr{}

type testErr struct{}

func (testErr) Error() string { return "handler failed" }

// TestInit_NonDefaultAddr 明确 Init 在新 stream 上可创建 group。
func TestIntegration_Init_CreatesGroup(t *testing.T) {
	q := newIntegrationQueue(t)
	defer q.Client.Del(context.Background(), "agent-test-stream")
	// 重复 Init 应幂等（BUSYGROUP 忽略）
	if err := q.Init(context.Background()); err != nil {
		t.Errorf("idempotent Init should not error: %v", err)
	}
}
