// Package queue 基于 Redis Streams 实现任务队列。
// 使用消费者组（XGroup）实现多 worker 消费，XAck 保证至少一次处理。
package queue

import (
	"agent-runtime/internal/model"
	"context"
	"encoding/json"
	"fmt"
	"github.com/redis/go-redis/v9"
	"time"
)

// RedisQueue 封装 Redis 客户端、Stream 名称与消费者组名。
type RedisQueue struct {
	Client        *redis.Client
	Stream, Group string
}

// New 创建 Redis 客户端实例，尚未初始化消费者组。
func New(addr, stream, group string) *RedisQueue {
	return &RedisQueue{Client: redis.NewClient(&redis.Options{Addr: addr}), Stream: stream, Group: group}
}

// Init 创建消费者组，若已存在（BUSYGROUP）则忽略。
func (q *RedisQueue) Init(ctx context.Context) error {
	_, err := q.Client.XGroupCreateMkStream(ctx, q.Stream, q.Group, "0").Result()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return err
	}
	return nil
}

// Enqueue 将 Task 序列化后通过 XAdd 写入 Stream，等待消费者组读取。
func (q *RedisQueue) Enqueue(ctx context.Context, t model.Task) error {
	b, _ := json.Marshal(t)
	_, err := q.Client.XAdd(ctx, &redis.XAddArgs{Stream: q.Stream, Values: map[string]any{"task": string(b)}}).Result()
	return err
}

// Consume 阻塞式消费循环：通过 XReadGroup 读取消息，成功执行 handler 后 XAck。
// 处理失败时不 Ack，消息会留在 PEL 中以便后续重投，保证至少一次语义。
func (q *RedisQueue) Consume(ctx context.Context, consumer string, handler func(context.Context, model.Task) error) error {
	for {
		res, err := q.Client.XReadGroup(ctx, &redis.XReadGroupArgs{Group: q.Group, Consumer: consumer, Streams: []string{q.Stream, ">"}, Count: 1, Block: 5 * time.Second}).Result()
		if err != nil {
			if err == redis.Nil {
				continue
			}
			return err
		}
		for _, stream := range res {
			for _, msg := range stream.Messages {
				raw, ok := msg.Values["task"].(string)
				if !ok {
					continue
				}
				var t model.Task
				if err := json.Unmarshal([]byte(raw), &t); err != nil {
					continue
				}
				if err := handler(ctx, t); err != nil {
					continue
				}
				// 仅在 handler 成功后确认，否则消息保留待重试。
				if _, err := q.Client.XAck(ctx, q.Stream, q.Group, msg.ID).Result(); err != nil {
					return err
				}
			}
		}
	}
}

// Ping 检查 Redis 连通性。
func (q *RedisQueue) Ping(ctx context.Context) error { return q.Client.Ping(ctx).Err() }

// String 返回队列的可读标识。
func (q *RedisQueue) String() string {
	return fmt.Sprintf("redis://%s/%s", q.Client.Options().Addr, q.Stream)
}
