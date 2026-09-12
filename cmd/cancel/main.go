// cmd/cancel 是运行取消入口：连接 MySQL，调用 Runtime.Cancel 把指定 Run
// 从 RUNNING 切到 CANCEL_REQUESTED，并取消其下所有 PENDING/READY 节点。
// 正在执行的 RUNNING 节点不被中断，由 Resumer 在完成后收敛 Run 到 CANCELLED。
//
// 用法：
//
//	go run ./cmd/cancel -run <run_id> -tenant <tenant_id> [-reason "user cancelled"]
package main

import (
	"agent-runtime/internal/runtime"
	"agent-runtime/internal/store"
	"context"
	"flag"
	_ "github.com/go-sql-driver/mysql"
	"log"
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

	rt := &runtime.Runtime{Store: s}
	if err := rt.Cancel(ctx, *tenant, *runID, *reason); err != nil {
		log.Fatalf("cancel run %s: %v", *runID, err)
	}
	log.Printf("run %s cancel requested", *runID)
}
