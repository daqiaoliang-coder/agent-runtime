// cmd/worker 是任务执行入口：连接 MySQL/Redis/RocketMQ，阻塞消费队列并调用 Worker.Handle 执行节点。
// 优雅停机：SIGTERM/SIGINT 停止拉取新任务，等待 in-flight 节点完成（不超过宽限期）后退出。
package main

import (
	"agent-runtime/internal/event"
	"agent-runtime/internal/queue"
	"agent-runtime/internal/store"
	"agent-runtime/internal/trace"
	"agent-runtime/internal/worker"
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
	// 停机信号接入 ctx：SIGTERM/SIGINT 取消 ctx，Consume 据此停止拉取并等 in-flight 完成。
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	// 初始化 OpenTelemetry 轨迹追踪；失败时降级为 no-op，不阻断启动。
	// 收尾用独立 Background ctx：停机时 ctx 已被信号取消，拿它 flush 会丢尾部轨迹。
	if shutdown, err := trace.Init("agent-worker"); err != nil {
		log.Printf("trace init skipped: %v", err)
	} else {
		defer shutdown(context.Background())
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
	rmq, err := event.NewProducer(env("ROCKETMQ_NAMESRV", "localhost:9876"), env("ROCKETMQ_TOPIC", "agent.events"))
	if err != nil {
		log.Fatal(err)
	}
	defer rmq.Close()
	w := worker.NewFromEnv(s, q, rmq)
	// 取消信号订阅（进程级）：NewFromEnv 只装配 bus 对象，这里启动订阅——
	// Start 建立真实 Redis pub/sub 连接并等到订阅确认，属进程生命周期行为，
	// 随停机 ctx 一起退出。订阅失败仅降级为兜底轮询，不阻断启动。
	if bus, ok := w.CancelBus.(*queue.CancelBus); ok {
		if err := bus.Start(ctx); err != nil {
			log.Printf("cancel bus subscribe failed; runs cancel will rely on polling: %v", err)
		}
	}
	// 消费者名必须进程唯一：多副本共用一个名字会把 PEL 归属混在一起，
	// XAutoClaim 的 idle 回收与按消费者的积压指标全部失真。
	// WORKER_ID 未设置时 NewFromEnv 已生成唯一 ID；K8s 部署建议经
	// Downward API 注入 pod 名（fieldRef metadata.name），稳定且可读。
	// 进程内并发：单副本吞吐 = WORKER_CONCURRENCY（handler 主要是 LLM/工具
	// 等网络 I/O 等待，天然适合并发），横向扩副本前先在单副本拿满这层收益。
	// 心跳账本：每个 RUNNING 节点约 3s 一次 GetRun，调大并发时按此估算 DB QPS。
	concurrency := envInt("WORKER_CONCURRENCY", 4)
	log.Println("worker started", "worker_id", w.ID, "concurrency", concurrency)
	errCh := make(chan error, 1)
	go func() { errCh <- q.Consume(ctx, w.ID, concurrency, w.Handle) }()
	select {
	case err := <-errCh:
		// 无停机信号时 Consume 仅在队列故障下返回，沿用 Fatal 语义。
		if err != nil {
			log.Fatal(err)
		}
		log.Println("worker stopped")
	case <-ctx.Done():
		// 信号已到：Consume 已停止拉取，等待 in-flight 节点完成，但不超过宽限期。
		log.Println("worker shutting down; draining in-flight node")
		select {
		case err := <-errCh:
			if err != nil {
				log.Fatal(err)
			}
			log.Println("worker stopped gracefully")
		case <-time.After(envDuration("WORKER_SHUTDOWN_GRACE", 30*time.Second)):
			// 宽限超时强制退出：正确性由租约兜底——节点租约（30s）过期后
			// recovery 重置 READY 并补投递，ClaimNode CAS 拦截重复副作用。
			// 调大 WORKER_NODE_TIMEOUT 时应同步调大本值与 K8s terminationGracePeriodSeconds。
			log.Println("worker shutdown grace exceeded; exiting, in-flight nodes will be re-dispatched after lease expiry")
		}
	}
}
func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func envInt(k string, d int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
		log.Printf("invalid int for %s=%q, using default %d", k, v, d)
	}
	return d
}

func envDuration(k string, d time.Duration) time.Duration {
	if v := os.Getenv(k); v != "" {
		if dur, err := time.ParseDuration(v); err == nil {
			return dur
		}
		log.Printf("invalid duration for %s=%q, using default %s", k, v, d)
	}
	return d
}
