// Package event 基于 RocketMQ 实现领域事件的发布与消费。
// RocketMQ 作为 MySQL 与 Resume Controller 之间的可靠事件通道。
package event

import (
	"agent-runtime/internal/model"
	"context"
	"encoding/json"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
)

// RocketMQ 封装生产者，负责将领域事件发送到指定 Topic。
type RocketMQ struct {
	Producer          rocketmq.Producer
	NameServer, Topic string
}

// NewProducer 创建并启动生产者。
func NewProducer(nameserver, topic string) (*RocketMQ, error) {
	p, err := rocketmq.NewProducer(producer.WithNameServer([]string{nameserver}), producer.WithRetry(2))
	if err != nil {
		return nil, err
	}
	if err := p.Start(); err != nil {
		return nil, err
	}
	return &RocketMQ{Producer: p, NameServer: nameserver, Topic: topic}, nil
}

// Publish 将 Event 序列化后同步发送到默认 Topic。
func (r *RocketMQ) Publish(ctx context.Context, e model.Event) error {
	b, _ := json.Marshal(e)
	_, err := r.Producer.SendSync(ctx, primitive.NewMessage(r.Topic, b))
	return err
}

// PublishRaw 向指定 Topic 发送原始字节，供 Outbox 发布器使用。
func (r *RocketMQ) PublishRaw(ctx context.Context, topic string, payload []byte) error {
	_, err := r.Producer.SendSync(ctx, primitive.NewMessage(topic, payload))
	return err
}

// Close 关闭生产者。
func (r *RocketMQ) Close() { _ = r.Producer.Shutdown() }

// Consumer 封装 RocketMQ 推送消费者，将消息体反序列化为 Event 后交给 handler。
type Consumer struct{ c rocketmq.PushConsumer }

// NewConsumer 创建消费者并注册订阅：收到消息后反序列化为 Event 并回调 handler。
// handler 返回错误时以 ConsumeRetryLater 稍后重试，成功则 ConsumeSuccess。
func NewConsumer(nameserver, topic, group string, handler func(context.Context, model.Event) error) (*Consumer, error) {
	c, err := rocketmq.NewPushConsumer(consumer.WithGroupName(group), consumer.WithNameServer([]string{nameserver}))
	if err != nil {
		return nil, err
	}
	err = c.Subscribe(topic, consumer.MessageSelector{}, func(ctx context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		for _, m := range msgs {
			var e model.Event
			if err := json.Unmarshal(m.Body, &e); err != nil {
				return consumer.ConsumeRetryLater, err
			}
			if err := handler(ctx, e); err != nil {
				return consumer.ConsumeRetryLater, err
			}
		}
		return consumer.ConsumeSuccess, nil
	})
	if err != nil {
		return nil, err
	}
	return &Consumer{c: c}, nil
}

// Start 启动消费者。
func (c *Consumer) Start() error { return c.c.Start() }

// Close 关闭消费者。
func (c *Consumer) Close() error { return c.c.Shutdown() }
