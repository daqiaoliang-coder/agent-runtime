// cmd/resume 是 DAG 推进入口：消费 RocketMQ 完成事件，调用 Resumer.Handle 激活后继节点并收敛 Run。
package main

import (
	"agent-runtime/internal/event"
	"agent-runtime/internal/middleware"
	"agent-runtime/internal/queue"
	"agent-runtime/internal/runtime"
	"agent-runtime/internal/store"
	"agent-runtime/internal/trace"
	"context"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"os"
)

func main() {
	ctx := context.Background()
	if shutdown, err := trace.Init("agent-resumer"); err != nil {
		log.Printf("trace init skipped: %v", err)
	} else {
		defer shutdown(ctx)
	}
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
	res := &runtime.Resumer{Store: s, Queue: q}
	// 生命周期钩子：Resumer 是 Run 收敛到终态的地方，OnRunFinish 在此执行，
	// 记录"谁发起的 Run、最终成功还是失败"。身份从 agent_run 还原（见 runExecutionContext），
	// 因此无需在此进程重新认证 —— 这正是身份落库的价值：收敛发生在另一个进程、
	// 另一个时刻，靠持久化才能还原当初的发起者。
	res.Lifecycle = newAuthLifecycle()
	c, err := event.NewConsumer(env("ROCKETMQ_NAMESRV", "localhost:9876"), env("ROCKETMQ_TOPIC", "agent.events"), env("ROCKETMQ_CONSUMER_GROUP", "agent-resumer"), res.Handle)
	if err != nil {
		log.Fatal(err)
	}
	if err := c.Start(); err != nil {
		log.Fatal(err)
	}
	defer c.Close()
	log.Println("resume controller started")
	select {}
}

// newAuthLifecycle 构造收敛侧的生命周期钩子。
//
// 只做审计、不做策略校验（RequireIdentity=false）：Run 能走到收敛，
// 说明它早已通过创建侧的认证，此处再强制身份没有意义，
// 反而会让存量 Run（认证接入前创建、user_id 为空）的收敛审计变成噪音告警。
func newAuthLifecycle() runtime.RunLifecycle {
	lc := &middleware.AuthLifecycle{}
	lc.OnAudit = func(_ context.Context, rec middleware.AuditRecord) {
		log.Print(rec.AuditString())
	}
	return middleware.NewLifecycleChain(lc)
}
func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
