// cmd/recovery 是崩溃恢复入口：定时扫描租约过期节点重置为 READY，
// 同时补投递 READY 节点，关闭"提交 READY 后崩溃未入队"的投递缺口。
// 此外执行取消扫描：取消 CANCEL_REQUESTED Run 下遗留的 PENDING/READY 节点，
// 并在全部节点终态时收敛 Run 到 CANCELLED，作为 Resumer 崩溃的安全网。
// 同时回收 Redis Streams PEL 中停滞的未确认消息，覆盖 worker 崩溃后消息无人处理的缺口。
package main

import (
	"agent-runtime/internal/model"
	"agent-runtime/internal/obs"
	"agent-runtime/internal/queue"
	"agent-runtime/internal/store"
	"agent-runtime/internal/trace"
	"context"
	_ "github.com/go-sql-driver/mysql"
	"os"
	"time"
)

func main() {
	ctx := context.Background()
	if shutdown, err := trace.Init("agent-recovery"); err != nil {
		obs.From(ctx).ErrorContext(ctx, "trace init skipped", "error", err)
	} else {
		defer shutdown(ctx)
	}
	dsn := env("DATABASE_DSN", "agent:agent@tcp(localhost:3306)/agent_runtime?parseTime=true")
	s, err := store.New(ctx, dsn)
	if err != nil {
		obs.From(ctx).ErrorContext(ctx, "failed to connect store", "error", err)
		os.Exit(1)
	}
	defer s.Close()
	q := queue.New(env("REDIS_ADDR", "localhost:6379"), env("REDIS_STREAM", "agent.tasks"), env("REDIS_GROUP", "agent-workers"))
	if err := q.Init(ctx); err != nil {
		obs.From(ctx).ErrorContext(ctx, "failed to init queue", "error", err)
		os.Exit(1)
	}
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// 取消扫描：处理 CANCEL_REQUESTED 的 Run。
			// 1. 取消遗留的 PENDING/READY 节点（覆盖 RecoverExpired 重置回 READY 的竞态）；
			// 2. 若全部节点终态，CAS 收敛到 CANCELLED（Resumer 崩溃时的安全网）。
			runs, err := s.CancelRequestedRuns(ctx, 100)
			if err != nil {
				obs.From(ctx).ErrorContext(ctx, "cancel scan failed", "error", err)
			}
			for _, run := range runs {
				if _, err := s.CancelRunNodes(ctx, run.TenantID, run.ID); err != nil {
					obs.From(ctx).ErrorContext(ctx, "cancel nodes failed", "run_id", run.ID, "error", err)
					continue
				}
				complete, err := s.RunComplete(ctx, run.TenantID, run.ID)
				if err != nil {
					obs.From(ctx).ErrorContext(ctx, "cancel complete check failed", "run_id", run.ID, "error", err)
					continue
				}
				if !complete {
					continue
				}
				ok, err := s.UpdateRunCAS(ctx, run.TenantID, run.ID, run.Version, model.RunCancelled, "", "cancelled by user")
				if err != nil {
					obs.From(ctx).ErrorContext(ctx, "cancel converge failed", "run_id", run.ID, "error", err)
					continue
				}
				if ok {
					obs.From(ctx).InfoContext(ctx, "run cancelled by recovery", "run_id", run.ID, "tenant_id", run.TenantID)
				}
			}

			tasks, err := s.RecoverExpired(ctx, 100)
			if err != nil {
				obs.From(ctx).ErrorContext(ctx, "recovery scan failed", "error", err)
			}
			ready, err := s.ReadyTasks(ctx, 100)
			if err != nil {
				obs.From(ctx).ErrorContext(ctx, "ready scan failed", "error", err)
			}
			tasks = append(tasks, ready...)
			for _, t := range tasks {
				if err := q.Enqueue(ctx, t); err != nil {
					obs.From(ctx).ErrorContext(ctx, "enqueue recovered task failed", "run_id", t.RunID, "node_id", t.NodeID, "error", err)
				}
			}
			// 回收 Redis Streams PEL 中停滞的未确认消息（worker 崩溃后未 Ack），
			// 重新入队使其可被正常消费。重复投递由 ClaimNode CAS 拦截。
			if n, err := q.ReclaimPending(ctx, "recovery", 30*time.Second, 100); err != nil {
				obs.From(ctx).ErrorContext(ctx, "reclaim pending failed", "error", err)
			} else if n > 0 {
				obs.From(ctx).InfoContext(ctx, "reclaimed stale PEL messages", "count", n)
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
