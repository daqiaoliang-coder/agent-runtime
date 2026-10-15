// notify.go 基于 Redis pub/sub 的 Run 取消信号总线。
// 背景：worker 心跳原为每 3s 一次 GetRun 轮询取消状态（1000 并发节点 ≈ 333 读 QPS）。
// 改造后：runtime 在 CancelRun 落库成功后 PublishCancel 广播；worker 进程级
// CancelBus 订阅并维护本地 run 集合，心跳 tick 查内存集合（零 DB），
// GetRun 降频为兜底轮询（缺省 30s 一次）。
//
// 信号是 best-effort 加速器而非事实源：丢失/迟到不影响正确性——
// 节点认领时的前置 GetRun 与降频兜底轮询都会拦截取消（MySQL 是唯一事实源）。
package queue

import (
	"context"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// cancelChannel 是取消广播的 pub/sub 频道名。
const cancelChannel = "agent-run-cancel"

// cancelSignalTTL 限制本地信号的滞留时间：信号是瞬时加速器，超龄即失效，
// 后续取消感知交给兜底轮询。同时防止 map 无界增长（无人消费的取消信号）。
const cancelSignalTTL = time.Minute

// CancelBus 进程级单例：订阅取消频道，维护本地已取消 run 集合。
// worker 心跳通过 Cancelled/Clear 消费（读内存，不打 DB）。
type CancelBus struct {
	client *redis.Client

	mu   sync.RWMutex
	runs map[string]time.Time // runID -> 收到信号的时间
}

// NewCancelBus 基于现有 RedisQueue 的连接创建总线（不新建连接）。
func (q *RedisQueue) NewCancelBus() *CancelBus {
	return &CancelBus{client: q.Client, runs: make(map[string]time.Time)}
}

// Start 建立订阅并阻塞到订阅确认到达（保证返回后立即能收到消息），
// 然后启动消费 goroutine 直到 ctx 结束。
// 订阅失败返回 error（Redis 不可达等）——调用方记日志即可：取消感知
// 退化为兜底轮询，正确性不受影响。go-redis PubSub 断线自动重连。
func (b *CancelBus) Start(ctx context.Context) error {
	sub := b.client.Subscribe(ctx, cancelChannel)
	// Receive 阻塞到第一条订阅确认：不等确认就返回，紧随其后的 Publish
	// 会落进"订阅命令尚未落盘"的窗口，信号静默丢失（测试实测复现）。
	if _, err := sub.Receive(ctx); err != nil {
		_ = sub.Close()
		return err
	}
	go func() {
		defer sub.Close()
		ch := sub.Channel()
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-ch:
				if !ok {
					return // 频道关闭（ctx 取消触发重连失败等极端场景）
				}
				b.record(msg.Payload)
			}
		}
	}()
	return nil
}

// PublishCancel 广播 run 取消信号。由 runtime 在 CancelRun 落库成功后调用；
// 失败仅意味着 worker 靠兜底轮询晚一点感知（≤30s），调用方记日志即可，不必重试。
func (q *RedisQueue) PublishCancel(ctx context.Context, runID string) error {
	return q.Client.Publish(ctx, cancelChannel, runID).Err()
}

func (b *CancelBus) record(runID string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.runs[runID] = time.Now()
}

// Cancelled 报告 run 是否收到过取消信号（且未过期）。纯内存读。
// 查询时顺带惰性清扫过期项，避免无人消费的信号长期驻留。
func (b *CancelBus) Cancelled(runID string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	for id, at := range b.runs {
		if now.Sub(at) > cancelSignalTTL {
			delete(b.runs, id)
		}
	}
	_, ok := b.runs[runID]
	return ok
}

// Clear 移除 run 的本地信号：worker 处理完（确认或忽略）后调用，
// 防止已终结的取消信号在 TTL 内反复触发 DB 确认。
func (b *CancelBus) Clear(runID string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.runs, runID)
}
