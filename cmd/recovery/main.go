// cmd/recovery 是崩溃恢复入口：定时扫描租约过期节点重置为 PENDING，
// 同时补投递 READY 节点，关闭“提交 READY 后崩溃未入队”的投递缺口。
// 此外执行取消扫描：取消 CANCEL_REQUESTED Run 下遗留的 PENDING/READY 节点，
// 并在全部节点终态时收敛 Run 到 CANCELLED，作为 Resumer 崩溃的安全网。
package main

import (
	"agent-runtime/internal/model"
	"agent-runtime/internal/queue"
	"agent-runtime/internal/store"
	"agent-runtime/internal/trace"
	"context"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"os"
	"time"
)

func main() {
	ctx := context.Background()
	if shutdown, err := trace.Init("agent-recovery"); err != nil {
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
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// 取消扫描：处理 CANCEL_REQUESTED 的 Run。
			// 1. 取消遗留的 PENDING/READY 节点（覆盖 RecoverExpired 重置回 PENDING 的竞态）；
			// 2. 若全部节点终态，CAS 收敛到 CANCELLED（Resumer 崩溃时的安全网）。
			runs, err := s.CancelRequestedRuns(ctx, 100)
			if err != nil {
				log.Println("cancel scan:", err)
			}
			for _, run := range runs {
				if _, err := s.CancelRunNodes(ctx, run.TenantID, run.ID); err != nil {
					log.Println("cancel nodes:", err)
					continue
				}
				complete, err := s.RunComplete(ctx, run.TenantID, run.ID)
				if err != nil {
					log.Println("cancel complete check:", err)
					continue
				}
				if !complete {
					continue
				}
				ok, err := s.UpdateRunCAS(ctx, run.TenantID, run.ID, run.Version, model.RunCancelled, "", "cancelled by user")
				if err != nil {
					log.Println("cancel converge:", err)
					continue
				}
				if ok {
					log.Printf("run cancelled run=%s tenant=%s", run.ID, run.TenantID)
				}
			}

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
