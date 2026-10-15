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
	"fmt"
	"os"
	"sync/atomic"
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
	q := newShardedQueue(t, 1) // 存量用例固定单分片（Shards=1 = 旧单 Stream 行为）
	return q
}

// newShardedQueue 构造指定分片数的测试队列并做隔离清理。
func newShardedQueue(t *testing.T, shards int) *RedisQueue {
	t.Helper()
	q := New(testAddr(), "agent-test-stream", "agent-test-group")
	q.Shards = shards
	ctx := context.Background()
	if err := q.Ping(ctx); err != nil {
		t.Fatalf("ping redis: %v (是否已 docker compose up?)", err)
	}
	// 清理旧 stream 与 group（含全部分片），保证测试隔离
	keys := make([]string, 0, shards)
	for i := 0; i < shards; i++ {
		keys = append(keys, q.streamName(i))
	}
	_ = q.Client.Del(ctx, keys...).Err()
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
		_ = q.Consume(ctx, "test-consumer", 1, func(_ context.Context, t model.Task) error {
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
	// XDel 清理断言：成功 Ack 后消息应从 Stream 删除（防无界增长的核心路径）。
	// Ack/Del 在 handler 返回后异步执行，轮询等待归零。
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if n, _ := q.Client.XLen(context.Background(), "agent-test-stream").Result(); n == 0 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("stream should be empty after successful consume+ack (XDel cleanup)")
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
		_ = q.Consume(ctx, "failing-consumer", 1, func(_ context.Context, _ model.Task) error {
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

	// ReclaimPending 内部 Enqueue 新消息 + XAck 旧消息 + XDel 旧消息（P1 清理）
	// 验证重新入队：stream 应恰好剩 1 条（重入队的新消息；原消息已被 XDel 清理，
	// 不再像旧行为那样保留尸体）
	streamLen, err := q.Client.XLen(context.Background(), "agent-test-stream").Result()
	if err != nil {
		t.Fatalf("XLen: %v", err)
	}
	if streamLen != 1 {
		t.Fatalf("stream should have exactly 1 message after reclaim (re-enqueued only; original XDel'd), got %d", streamLen)
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

// TestIntegration_ConsumeGracefulShutdown_DrainsInFlight 验证优雅停机语义：
// 执行中触发停机信号（ctx 取消）只停止拉取新消息，不打断 in-flight 执行——
// handler 运行在剥离取消链的派生 ctx 上，完成后正常 Ack，Consume 返回 nil。
// 若取消穿透到 handler ctx，滚动更新会把正在执行的节点半途打成失败，
// 换来本可避免的重跑与双份 token 消耗。
func TestIntegration_ConsumeGracefulShutdown_DrainsInFlight(t *testing.T) {
	q := newIntegrationQueue(t)
	defer q.Client.Del(context.Background(), "agent-test-stream")
	ctx, cancel := context.WithCancel(context.Background())

	task := model.Task{RunID: "it-run-3", NodeID: "it-node-3", TenantID: "tenant-it", Attempt: 0}
	if err := q.Enqueue(ctx, task); err != nil {
		cancel()
		t.Fatalf("Enqueue: %v", err)
	}

	handlerDone := make(chan struct{})
	consumeErr := make(chan error, 1)
	go func() {
		consumeErr <- q.Consume(ctx, "drain-consumer", 1, func(hctx context.Context, _ model.Task) error {
			defer close(handlerDone)
			cancel()                             // 模拟 SIGTERM：执行中触发停机信号
			<-time.After(300 * time.Millisecond) // 模拟仍在收尾的 in-flight 工作
			if hctx.Err() != nil {               // 关键断言：停机信号不得穿透到 handler
				t.Errorf("handler ctx must stay alive during drain, got %v", hctx.Err())
			}
			return nil
		})
	}()

	select {
	case <-handlerDone:
	case <-time.After(3 * time.Second):
		t.Fatal("timeout: in-flight handler should complete despite shutdown signal")
	}
	// Consume 应在 handler 完成后返回 nil（取消不是错误）。
	select {
	case err := <-consumeErr:
		if err != nil {
			t.Fatalf("Consume should return nil on graceful shutdown, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for Consume to return after drain")
	}
	// 成功完成的 in-flight 必须已 Ack：停机瞬间 Ack 失败会让消息滞留 PEL 被
	// recovery 重投，白跑一遍已成功的工作。（查询用独立 ctx，停机 ctx 已取消。）
	pending, err := q.Client.XPending(context.Background(), "agent-test-stream", "agent-test-group").Result()
	if err != nil {
		t.Fatalf("XPending: %v", err)
	}
	if pending.Count != 0 {
		t.Fatalf("drained message must be acked after graceful shutdown, PEL count = %d", pending.Count)
	}
}

// TestIntegration_ConsumeParallel_FillsSlots 验证进程内并发：
// concurrency=N 时，N 条消息应同时在 N 个槽位上执行（handler 阻塞等满槽才放行，
// 串行退化会超时失败），全部成功后逐条 Ack，PEL 清零。
func TestIntegration_ConsumeParallel_FillsSlots(t *testing.T) {
	q := newIntegrationQueue(t)
	defer q.Client.Del(context.Background(), "agent-test-stream")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const n = 3
	for i := 0; i < n; i++ {
		task := model.Task{RunID: "it-run-par", NodeID: fmt.Sprintf("it-node-par%d", i), TenantID: "tenant-it", Attempt: 0}
		if err := q.Enqueue(ctx, task); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}

	var inflight atomic.Int32
	release := make(chan struct{})
	allDone := make(chan struct{})
	consumeErr := make(chan error, 1)
	go func() {
		consumeErr <- q.Consume(ctx, "parallel-consumer", n, func(_ context.Context, _ model.Task) error {
			if inflight.Add(1) == n {
				close(release) // N 个槽位并发到齐，放行全部
			}
			select {
			case <-release:
			case <-time.After(5 * time.Second): // 串行退化兜底，防止挂死整个测试
			}
			if inflight.Add(-1) == 0 {
				close(allDone)
			}
			return nil
		})
	}()

	select {
	case <-release:
		t.Logf("%d handlers running concurrently", n)
	case <-time.After(6 * time.Second):
		t.Fatalf("concurrency=%d should run %d handlers simultaneously", n, n)
	}

	cancel() // 停机：Consume 应排空全部 in-flight 后返回 nil
	select {
	case err := <-consumeErr:
		if err != nil {
			t.Fatalf("Consume should return nil on graceful shutdown, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for Consume to drain in-flight handlers")
	}
	select {
	case <-allDone:
	default:
		t.Fatal("all handlers should have completed before Consume returned")
	}
	// 全部成功必须已 Ack（查询用独立 ctx，停机 ctx 已取消）。
	pending, err := q.Client.XPending(context.Background(), "agent-test-stream", "agent-test-group").Result()
	if err != nil {
		t.Fatalf("XPending: %v", err)
	}
	if pending.Count != 0 {
		t.Fatalf("all successfully handled messages must be acked, PEL count = %d", pending.Count)
	}
}

// TestIntegration_ShardRouting 验证租户分片：两个租户的任务落在各自 hash 分片的
// 物理 Stream 上（同租户稳定同分片），Consume 一次多流拉取能取到全部分片的消息，
// 成功后按消息所属分片 Ack + XDel。
func TestIntegration_ShardRouting(t *testing.T) {
	const shards = 4
	q := newShardedQueue(t, shards)
	defer func() {
		keys := make([]string, 0, shards)
		for i := 0; i < shards; i++ {
			keys = append(keys, q.streamName(i))
		}
		_ = q.Client.Del(context.Background(), keys...).Err()
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 找两个必然落在不同分片的租户（FNV 可本地预判），保证"分散"断言确定性。
	tenants := []string{"tenant-s-a", "tenant-s-b"}
	for _, b := range []string{"tenant-s-c", "tenant-s-d", "tenant-s-e"} {
		if q.shardFor(tenants[0]) != q.shardFor(b) {
			tenants[1] = b
			break
		}
	}
	if q.shardFor(tenants[0]) == q.shardFor(tenants[1]) {
		t.Fatal("test setup: could not find tenants on distinct shards")
	}

	for i, tenant := range tenants {
		if err := q.Enqueue(ctx, model.Task{RunID: "it-shard", NodeID: fmt.Sprintf("it-shard-%d", i), TenantID: tenant}); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}
	// 路由断言：消息只出现在各自分片的 Stream。
	for _, tenant := range tenants {
		want := q.streamName(q.shardFor(tenant))
		if n, _ := q.Client.XLen(ctx, want).Result(); n != 1 {
			t.Errorf("tenant %s: stream %s length = %d, want 1", tenant, want, n)
		}
	}

	got := make(chan model.Task, 2)
	go func() {
		_ = q.Consume(ctx, "shard-consumer", 2, func(_ context.Context, tk model.Task) error {
			got <- tk
			return nil
		})
	}()
	for want := 2; want > 0; want-- {
		select {
		case <-got:
		case <-time.After(3 * time.Second):
			t.Fatal("timeout: multi-stream consume should reach messages on all shards")
		}
	}
	cancel()
	// 全部 Ack + XDel 后，两条分片 Stream 应归零。
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		total := int64(0)
		for _, tenant := range tenants {
			total += q.Client.XLen(context.Background(), q.streamName(q.shardFor(tenant))).Val()
		}
		if total == 0 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("shard streams should be empty after consume (per-shard Ack+XDel)")
}

// TestIntegration_CancelBus 验证取消信号总线：PublishCancel 广播 → 同进程
// CancelBus 订阅收到（Cancelled=true）→ Clear 移除。这是 worker 心跳
// pub/sub 快路径与 runtime 取消发布点的端到端契约。
func TestIntegration_CancelBus(t *testing.T) {
	q := newIntegrationQueue(t)
	bus := q.NewCancelBus()
	subCtx, stop := context.WithCancel(context.Background())
	defer stop()
	if err := bus.Start(subCtx); err != nil {
		t.Fatalf("bus.Start: %v", err)
	}

	if err := q.PublishCancel(context.Background(), "run-bus-1"); err != nil {
		t.Fatalf("PublishCancel: %v", err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && !bus.Cancelled("run-bus-1") {
		time.Sleep(20 * time.Millisecond)
	}
	if !bus.Cancelled("run-bus-1") {
		t.Fatal("cancel signal not received via pub/sub")
	}
	bus.Clear("run-bus-1")
	if bus.Cancelled("run-bus-1") {
		t.Fatal("signal should be cleared")
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
