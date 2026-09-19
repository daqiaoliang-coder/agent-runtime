// cmd/runtime 是 Run 创建入口：连接 MySQL/Redis，调用 Runtime.CreateRun 生成 DAG 并投递初始任务。
package main

import (
	"agent-runtime/internal/adapters/credential"
	"agent-runtime/internal/contracts"
	"agent-runtime/internal/llm"
	"agent-runtime/internal/queue"
	"agent-runtime/internal/runtime"
	"agent-runtime/internal/store"
	"agent-runtime/internal/trace"
	"context"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"os"
)

func main() {
	ctx := context.Background()
	// 初始化 OpenTelemetry 轨迹追踪；失败时降级为 no-op，不阻断启动。
	if shutdown, err := trace.Init("agent-runtime"); err != nil {
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
	// 默认使用静态 DemoPlanner（确定性 DAG，无需 LLM）；
	// 托管层能取到对话凭证时切换为 LLMPlanner，由模型动态生成 DAG。
	// 密钥经 CredentialProvider 获取而非 os.Getenv 直读：换托管设施（文件 / Vault）
	// 时本文件一行都不用改，轮转由托管层驱动、进程无需重启。
	planner := runtime.Planner(runtime.DemoPlanner{})
	creds := credential.FromEnv()
	if base := os.Getenv("OPENAI_BASE_URL"); base != "" {
		// 取不到凭证时降级为 DemoPlanner：此时发起真实请求只会换来网关 401，
		// 把排查方向从"密钥没配"误导到"密钥无效"。降级则行为与未配置网关时一致。
		if _, cerr := creds.Credential(ctx, contracts.CredentialPurposeChat); cerr == nil {
			planner = &runtime.LLMPlanner{LLM: llm.NewOpenAIClientWithCredentials(base, creds, contracts.CredentialPurposeChat)}
		} else {
			log.Printf("runtime: no chat credential available, falling back to demo planner: %v", cerr)
		}
	}
	rt := &runtime.Runtime{Store: s, Queue: q, Planner: planner}
	// THREAD_ID 标识会话维度：同一 Thread 下的多个 Run 共享长期记忆。
	// 留空表示无会话隔离，记忆检索将跳过 thread_id 过滤。
	run, err := rt.CreateRun(ctx, "default", "demo", "why is project delayed?", env("THREAD_ID", ""))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("created run:", run.ID, "thread:", run.ThreadID)
}
func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
