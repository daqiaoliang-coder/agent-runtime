// Package queue 基于 Redis Streams 实现任务队列。
// 使用消费者组（XGroup）实现多 worker 消费，XAck 保证至少一次处理。
package queue

import (
	"agent-runtime/internal/model"
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// RedisQueue 封装 Redis 客户端、Stream 名称与消费者组名。
type RedisQueue struct {
	Client        *redis.Client
	Stream, Group string
	// Shards 是租户分片数（缺省 8，New 内设置）：任务按 hash(TenantID) 落到
	// {Stream}-s{i}，单租户洪峰最多占满 1/Shards 的消费能力，其余分片的
	// 租户不受干扰。1 = 退回单 Stream 旧行为（消息仍走 Stream 本名）。
	//
	// ⚠ 必须全进程一致（runtime/worker/resume/recovery 各 cmd 的装配）：
	// 写侧与消费侧分片数不同 = 消息被写进无人消费的 Stream，静默滞留。
	// 因此缺省值收敛在 New() 里而非环境变量——调整时改代码并同步全部部署。
	// 升级提示：从单 Stream 切到分片后，旧 Stream 的 PEL 残留不再被回收，
	// 但 DB 租约过期扫描（RecoverExpired → 重置 READY → 补投递）会兜底推进。
	Shards int
	// MaxLen 是每条分片 Stream 的近似长度上界（缺省 10000，New 内设置）。
	// 正常路径由 Ack 后 XDel 清理，Stream 长度趋近 in-flight 数；本上界只防御
	// 异常堆积（大量失败消息滞留 PEL）。被裁剪掉的 PEL 条目会由 XAutoClaim
	// 自动清出 PEL（Redis 7+），消息层面丢失由 DB 租约兜底——与架构约束一致：
	// MySQL 是唯一事实源，Redis 只做触发。0 = 不裁剪。
	MaxLen int64
}

// New 创建 Redis 客户端实例，尚未初始化消费者组。
func New(addr, stream, group string) *RedisQueue {
	return &RedisQueue{
		Client: redis.NewClient(&redis.Options{Addr: addr}),
		Stream: stream, Group: group,
		Shards: defaultShards, MaxLen: defaultMaxLen,
	}
}

// 分片与裁剪的代码级缺省：理由见 RedisQueue.Shards / MaxLen 字段注释。
const (
	defaultShards = 8
	defaultMaxLen = 10000
)

// shardCount 返回有效分片数（<=1 规范化为 1）。
func (q *RedisQueue) shardCount() int {
	if q.Shards > 1 {
		return q.Shards
	}
	return 1
}

// streamName 返回第 i 条分片的物理 Stream 名；单分片时就是 Stream 本名（兼容旧部署）。
func (q *RedisQueue) streamName(i int) string {
	if q.shardCount() == 1 {
		return q.Stream
	}
	return fmt.Sprintf("%s-s%d", q.Stream, i)
}

// shardFor 按 FNV-1a(TenantID) 稳定映射租户到分片：同一租户的任务永远同一条
// Stream（保证分片内 FIFO），不同租户大概率分散（hash 均匀性）。
func (q *RedisQueue) shardFor(tenant string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(tenant))
	return int(h.Sum32() % uint32(q.shardCount()))
}

// Init 创建消费者组，若已存在（BUSYGROUP）则忽略。对全部分片执行。
func (q *RedisQueue) Init(ctx context.Context) error {
	for i := 0; i < q.shardCount(); i++ {
		_, err := q.Client.XGroupCreateMkStream(ctx, q.streamName(i), q.Group, "0").Result()
		if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
			return err
		}
	}
	return nil
}

// Enqueue 将 Task 序列化后通过 XAdd 写入 Stream，等待消费者组读取。
// 入队前把当前 ctx 中的 trace 上下文注入 Task.TraceContext，
// 使消费端能重建同一条 Trace，避免 runtime→worker 跨进程断链。
func (q *RedisQueue) Enqueue(ctx context.Context, t model.Task) error {
	if t.TraceContext == nil {
		t.TraceContext = map[string]string{}
	}
	otel.GetTextMapPropagator().Inject(ctx, propagation.MapCarrier(t.TraceContext))
	b, _ := json.Marshal(t)
	// 租户分片路由：同租户同分片（分片内 FIFO），跨租户分散（互不挤占）。
	args := &redis.XAddArgs{Stream: q.streamName(q.shardFor(t.TenantID)), Values: map[string]any{"task": string(b)}}
	if q.MaxLen > 0 {
		args.MaxLen = q.MaxLen // 近似裁剪（~），防御异常堆积；正常清理靠 Ack 后 XDel
		args.Approx = true
	}
	_, err := q.Client.XAdd(ctx, args).Result()
	return err
}

// Consume 消费循环：批量拉取 + 进程内并发执行 handler，成功后逐条 XAck。
// concurrency 是并发上限（≤0 按 1 处理，即串行语义）。拉取 Count 与并发对齐；
// 满载时派发阻塞形成背压，本消费者 PEL 中未完成消息上界约为 2×concurrency。
//
// 失败语义：handler 失败不 Ack，消息留在 PEL 由 recovery 重投（至少一次）；
// XReadGroup/XAck 等基础设施错误记录首个错误并停止拉取，排空 in-flight 后返回。
//
// 优雅停机：ctx 取消只停止拉取新消息，返回 nil，不中断 in-flight 执行——
// handler 与 XAck 都运行在剥离了取消信号的派生 ctx 上（保留 trace/日志等 values）。
// 停机瞬间在途的节点要么已烧了 token、要么已产生部分副作用，半途打断只会
// 换来重跑一遍的浪费；让它跑完再退出，异常场景仍由租约过期 + recovery 兜底。
func (q *RedisQueue) Consume(ctx context.Context, consumer string, concurrency int, handler func(context.Context, model.Task) error) error {
	// 防御性兜底：≤0 回退串行。零容量信号量会让派发处永久阻塞（BatchSize=0 死循环教训）。
	if concurrency <= 0 {
		concurrency = 1
	}
	// fetchCtx 只服务于拉取：基础设施出错时经 stopFetch 停止后续 XReadGroup；
	// 与用户停机取消（同样表现为 fetchCtx.Done）的区分靠 firstErr 是否非空。
	fetchCtx, stopFetch := context.WithCancel(ctx)
	defer stopFetch()

	var (
		sem      = make(chan struct{}, concurrency) // 并发槽位
		wg       sync.WaitGroup                     // 跟踪全部 in-flight 消息
		mu       sync.Mutex
		firstErr error
	)
	// fail 记录首个基础设施错误并停止拉取；in-flight 继续跑完（可能成功并 Ack）。
	fail := func(err error) {
		mu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		mu.Unlock()
		stopFetch()
	}

	// 多分片一次拉取：XReadGroup 支持多 Stream。Redis 协议要求 STREAMS 后
	// 先全部流名、再每流一个 ID（这里都是 ">"）——只放一个 ">" 会被当作
	// 流名解析，协议错误。COUNT 是 per-stream 上限，取 concurrency/分片数
	// 使一次拉取总量 ≈ concurrency（in-flight 上界不膨胀）。
	shards := q.shardCount()
	streams := make([]string, 0, shards*2)
	for i := 0; i < shards; i++ {
		streams = append(streams, q.streamName(i))
	}
	for i := 0; i < shards; i++ {
		streams = append(streams, ">")
	}
	count := max(1, concurrency/shards)

	for {
		// Block 必须短：go-redis 不会因 ctx 取消中断阻塞中的 XReadGroup（会等满
		// Block 才返回），Block 即停机排空的响应上界。500ms 意味着空转时每秒
		// 2 次空拉（对 Redis 可忽略），滚动更新不再白等秒级。
		res, err := q.Client.XReadGroup(fetchCtx, &redis.XReadGroupArgs{
			Group: q.Group, Consumer: consumer,
			Streams: streams,
			Count:   int64(count), Block: 500 * time.Millisecond,
		}).Result()
		if err != nil {
			if fetchCtx.Err() != nil {
				break // 停机或已因基础设施错误停拉：进入排空
			}
			if err == redis.Nil {
				continue // Block 超时无新消息
			}
			fail(err)
			break
		}
		for _, stream := range res {
			for _, msg := range stream.Messages {
				// 满载时阻塞在此形成背压：已读取未派发的消息至多多一批（≤2×concurrency），
				// 全部由 wg 跟踪，停机/出错路径都会等它们处理完才返回。
				sem <- struct{}{}
				wg.Add(1)
				go func(msg redis.XMessage, src string) {
					defer wg.Done()
					defer func() { <-sem }()
					raw, ok := msg.Values["task"].(string)
					if !ok {
						return // 坏消息不 Ack，滞留 PEL 交由 recovery 处置（与串行版一致）
					}
					var t model.Task
					if err := json.Unmarshal([]byte(raw), &t); err != nil {
						return
					}
					// 停机只停拉取，不斩 in-flight：handler 的 ctx 剥离取消链（保留 values），
					// 否则停机信号会沿 ctx 穿进 LLM/工具调用，把本可正常完成的节点打成失败。
					base := context.WithoutCancel(ctx)
					// 从 Task 提取 trace 上下文，为每条消息重建独立 ctx（不复用循环变量，
					// 避免上一条消息的 trace 泄漏到下一条）。无 TraceContext 时
					// （如 recovery 补投递），沿用上层 ctx 作为新 trace 根。
					msgCtx := base
					if len(t.TraceContext) > 0 {
						msgCtx = otel.GetTextMapPropagator().Extract(base, propagation.MapCarrier(t.TraceContext))
					}
					if err := handler(msgCtx, t); err != nil {
						return // handler 失败不 Ack，留在 PEL 待重投
					}
					// 仅在 handler 成功后确认，否则消息保留待重试。
					// XAck 同样用剥离取消的 ctx：停机瞬间完成的节点若因 ctx 已取消而
					// Ack 失败，消息会滞留 PEL 被 recovery 重投——白跑一遍已成功的工作。
					// Ack/Del 用消息物理所属的分片 Stream（src），不是 q.Stream。
					if _, err := q.Client.XAck(base, src, q.Group, msg.ID).Result(); err != nil {
						fail(err)
						return
					}
					// 生命周期终结即清理（XDel）：否则 Stream 只进不出，靠 MAXLEN
					// 裁剪会误伤滞留 PEL 的失败消息。失败仅忽略——消息已 Ack，
					// 残留尸体由 MAXLEN 兜底清掉，不影响正确性。
					_, _ = q.Client.XDel(base, src, msg.ID).Result()
				}(msg, stream.Stream)
			}
		}
	}
	// 排空全部 in-flight（含停机瞬间已读取未派发的消息），再返回首个错误。
	wg.Wait()
	return firstErr
}

// Ping 检查 Redis 连通性。
func (q *RedisQueue) Ping(ctx context.Context) error { return q.Client.Ping(ctx).Err() }

// ReclaimPending 回收消费者组 PEL 中停滞超过 minIdle 的未确认消息：
// 将其归属改为指定 consumer 后重新入队为新消息并确认旧消息，
// 使正常 Consume 循环可重新消费。用于 worker 崩溃后未 Ack 消息的回收。
// 重复投递由 worker 侧 ClaimNode CAS 拦截，不会产生重复副作用。
// 对全部分片执行；回收的旧消息 Ack 后 XDel，避免 Stream 尸体累积。
func (q *RedisQueue) ReclaimPending(ctx context.Context, consumer string, minIdle time.Duration, count int64) (int, error) {
	total := 0
	for i := 0; i < q.shardCount(); i++ {
		n, err := q.reclaimShard(ctx, consumer, minIdle, count, q.streamName(i))
		if err != nil {
			return total, err
		}
		total += n
	}
	return total, nil
}

func (q *RedisQueue) reclaimShard(ctx context.Context, consumer string, minIdle time.Duration, count int64, stream string) (int, error) {
	msgs, _, err := q.Client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Group: q.Group, Consumer: consumer, MinIdle: minIdle,
		Stream: stream, Start: "0-0", Count: count,
	}).Result()
	if err != nil && err != redis.Nil {
		return 0, err
	}
	for _, msg := range msgs {
		if raw, ok := msg.Values["task"].(string); ok {
			var t model.Task
			if json.Unmarshal([]byte(raw), &t) == nil {
				_ = q.Enqueue(ctx, t)
			}
		}
		if _, err := q.Client.XAck(ctx, stream, q.Group, msg.ID).Result(); err == nil {
			_, _ = q.Client.XDel(ctx, stream, msg.ID).Result() // 同 Consume：终结即清理
		}
	}
	return len(msgs), nil
}

// String 返回队列的可读标识。
func (q *RedisQueue) String() string {
	return fmt.Sprintf("redis://%s/%s", q.Client.Options().Addr, q.Stream)
}
