package main

import (
	"context"
	"log"
	"os"
	"time"

	"agent-runtime/internal/event"
	"agent-runtime/internal/store"
	_ "github.com/go-sql-driver/mysql"
)

func main() {
	ctx := context.Background()
	s, err := store.New(ctx, env("DATABASE_DSN", "agent:agent@tcp(localhost:3306)/agent_runtime?parseTime=true"))
	if err != nil {
		log.Fatal(err)
	}
	defer s.Close()

	mq, err := event.NewProducer(env("ROCKETMQ_NAMESRV", "localhost:9876"), env("ROCKETMQ_TOPIC", "agent.events"))
	if err != nil {
		log.Fatal(err)
	}
	defer mq.Close()

	log.Println("outbox publisher started")
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for range ticker.C {
		batch, err := s.ClaimOutbox(ctx, 100)
		if err != nil {
			log.Println("claim outbox:", err)
			continue
		}
		for _, item := range batch {
			if err := mq.PublishRaw(ctx, env("ROCKETMQ_TOPIC", "agent.events"), []byte(item.Payload)); err != nil {
				_ = s.RetryOutbox(ctx, item.ID, 2*time.Second)
				log.Println("publish outbox:", err)
				continue
			}
			if err := s.MarkOutboxPublished(ctx, item.ID); err != nil {
				log.Println("mark published:", err)
			}
		}
	}
}

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
