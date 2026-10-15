// cmd/cancel 是运行取消入口：连接 MySQL，调用 Runtime.Cancel 把指定 Run
// 从 RUNNING 切到 CANCEL_REQUESTED，并取消其下所有 PENDING/READY 节点。
// 落库成功后通过 Redis 广播，worker 确认 DB 状态后取消在途执行；轮询负责兜底。
//
// 用法：
//
//	go run ./cmd/cancel -run <run_id> -tenant <tenant_id> [-reason "user cancelled"]
package main

import (
	"agent-runtime/internal/queue"
	"agent-runtime/internal/runtime"
	"agent-runtime/internal/store"
	"context"
	"flag"
	"log"
	"os"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
	runID := flag.String("run", "", "Run ID to cancel")
	tenant := flag.String("tenant", "default", "Tenant ID")
	reason := flag.String("reason", "user cancelled", "Cancellation reason")
	dsn := flag.String("dsn", "agent:agent@tcp(localhost:3306)/agent_runtime?parseTime=true", "MySQL DSN")
	flag.Parse()
	if *runID == "" {
		log.Fatal("missing -run flag")
	}

	ctx := context.Background()
	s, err := store.New(ctx, *dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer s.Close()

	// 仅使用 pub/sub，无需 Init 创建任务 stream 或消费者组。
	q := queue.New(env("REDIS_ADDR", "localhost:6379"), env("REDIS_STREAM", "agent.tasks"), env("REDIS_GROUP", "agent-workers"))
	rt := newCancelRuntime(s, q)
	err = rt.Cancel(ctx, *tenant, *runID, *reason)
	// log.Fatal 不执行 defer；在错误退出之前也关闭 Redis client。
	_ = q.Client.Close()
	if err != nil {
		log.Fatalf("cancel run %s: %v", *runID, err)
	}
	log.Printf("run %s cancel requested", *runID)
}

func newCancelRuntime(s *store.MySQL, pub runtime.CancelPublisher) *runtime.Runtime {
	return &runtime.Runtime{Store: s, CancelPub: pub}
}

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
