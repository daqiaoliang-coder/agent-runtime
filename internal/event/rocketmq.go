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

type RocketMQ struct {
	Producer          rocketmq.Producer
	NameServer, Topic string
}

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
func (r *RocketMQ) Publish(ctx context.Context, e model.Event) error {
	b, _ := json.Marshal(e)
	_, err := r.Producer.SendSync(ctx, primitive.NewMessage(r.Topic, b))
	return err
}

func (r *RocketMQ) PublishRaw(ctx context.Context, topic string, payload []byte) error {
	_, err := r.Producer.SendSync(ctx, primitive.NewMessage(topic, payload))
	return err
}

func (r *RocketMQ) Close() { _ = r.Producer.Shutdown() }

type Consumer struct{ c rocketmq.PushConsumer }

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
func (c *Consumer) Start() error { return c.c.Start() }
func (c *Consumer) Close() error { return c.c.Shutdown() }
