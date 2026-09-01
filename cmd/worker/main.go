// cmd/worker 是任务执行入口：连接 MySQL/Redis/RocketMQ，阻塞消费队列并调用 Worker.Handle 执行节点。
package main

import (
	"agent-runtime/internal/event"
	"agent-runtime/internal/queue"
	"agent-runtime/internal/store"
	"agent-runtime/internal/worker"
	"context"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"os"
)

func main() {
	ctx := context.Background()
	dsn := env("DATABASE_DSN", "agent:agent@tcp(localhost:3306)/agent_runtime?parseTime=true")
	s, err := store.New(ctx, dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer s.Close()
	q := queue.New(env("REDIS_ADDR", "localhost:6379"), env("REDIS_STREAM", "agent.tasks"), env("REDIS_GROUP", "agent-workers"))
	if err := q.Init(ctx); err != nil {
		log.Fatal(err)
	}
	rmq, err := event.NewProducer(env("ROCKETMQ_NAMESRV", "localhost:9876"), env("ROCKETMQ_TOPIC", "agent.events"))
	if err != nil {
		log.Fatal(err)
	}
	defer rmq.Close()
	w := worker.NewFromEnv(s, q, rmq)
	log.Println("worker started")
	log.Fatal(q.Consume(ctx, env("WORKER_ID", "worker"), w.Handle))
}
func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
