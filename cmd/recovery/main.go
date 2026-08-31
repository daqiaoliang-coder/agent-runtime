package main

import (
	"agent-runtime/internal/queue"
	"agent-runtime/internal/store"
	"context"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"os"
	"time"
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
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			tasks, err := s.RecoverExpired(ctx, 100)
			if err != nil {
				log.Println("recovery:", err)
			}
			ready, err := s.ReadyTasks(ctx, 100)
			if err != nil {
				log.Println("ready scan:", err)
			}
			tasks = append(tasks, ready...)
			for _, t := range tasks {
				if err := q.Enqueue(ctx, t); err != nil {
					log.Println("enqueue recovered task:", err)
				}
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
