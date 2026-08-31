package queue

import (
	"agent-runtime/internal/model"
	"context"
	"encoding/json"
	"fmt"
	"github.com/redis/go-redis/v9"
	"time"
)

type RedisQueue struct {
	Client        *redis.Client
	Stream, Group string
}

func New(addr, stream, group string) *RedisQueue {
	return &RedisQueue{Client: redis.NewClient(&redis.Options{Addr: addr}), Stream: stream, Group: group}
}
func (q *RedisQueue) Init(ctx context.Context) error {
	_, err := q.Client.XGroupCreateMkStream(ctx, q.Stream, q.Group, "0").Result()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return err
	}
	return nil
}
func (q *RedisQueue) Enqueue(ctx context.Context, t model.Task) error {
	b, _ := json.Marshal(t)
	_, err := q.Client.XAdd(ctx, &redis.XAddArgs{Stream: q.Stream, Values: map[string]any{"task": string(b)}}).Result()
	return err
}
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
				if _, err := q.Client.XAck(ctx, q.Stream, q.Group, msg.ID).Result(); err != nil {
					return err
				}
			}
		}
	}
}
func (q *RedisQueue) Ping(ctx context.Context) error { return q.Client.Ping(ctx).Err() }
func (q *RedisQueue) String() string {
	return fmt.Sprintf("redis://%s/%s", q.Client.Options().Addr, q.Stream)
}
