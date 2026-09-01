// cmd/runtime 是 Run 创建入口：连接 MySQL/Redis，调用 Runtime.CreateRun 生成 DAG 并投递初始任务。
package main

import (
	"agent-runtime/internal/llm"
	"agent-runtime/internal/queue"
	"agent-runtime/internal/runtime"
	"agent-runtime/internal/store"
	"context"
	"fmt"
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
	// 默认使用静态 DemoPlanner（确定性 DAG，无需 LLM）；
	// 配置 OPENAI_API_KEY 时切换为 LLMPlanner，由模型动态生成 DAG。
	planner := runtime.Planner(runtime.DemoPlanner{})
	if base := os.Getenv("OPENAI_BASE_URL"); base != "" {
		if key := os.Getenv("OPENAI_API_KEY"); key != "" {
			planner = &runtime.LLMPlanner{LLM: llm.NewOpenAIClient(base, key)}
		}
	}
	rt := &runtime.Runtime{Store: s, Queue: q, Planner: planner}
	run, err := rt.CreateRun(ctx, "default", "demo", "why is project delayed?")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("created run:", run.ID)
}
func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
