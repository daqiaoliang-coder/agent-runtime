// cmd/runtime 是 Run 创建入口：连接 MySQL/Redis，调用 Runtime.CreateRun 生成 DAG 并投递初始任务。
package main

import (
	"agent-runtime/internal/adapters/auth"
	"agent-runtime/internal/adapters/credential"
	"agent-runtime/internal/contracts"
	"agent-runtime/internal/llm"
	"agent-runtime/internal/middleware"
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

	// 认证装配：AUTH_ENABLED=true 时校验入口凭证，并把身份写入 agent_run。
	//
	// 装配失败一律 log.Fatal 而不降级：认证开启却拿不到验签密钥是配置错误，
	// 静默降级成"不认证"会让进程看起来正常运行、实际对所有请求放行 ——
	// 那比启动失败危险得多，因为没人会注意到。
	authCfg, err := auth.FromEnv(creds)
	if err != nil {
		log.Fatalf("runtime: authentication misconfigured: %v", err)
	}
	rt.Authenticator = authCfg.Authenticator
	rt.RequireIdentity = authCfg.RequireIdentity
	// 生命周期钩子承担租户交叉校验与持久化审计。
	// 装配它的进程是 Run 的创建方，因此 OnRunStart 在这里执行；
	// OnRunFinish 由 cmd/resume 侧执行（Run 收敛发生在另一个进程）。
	rt.Lifecycle = newAuthLifecycle(authCfg)
	if authCfg.Enabled {
		log.Printf("runtime: authentication enabled method=%s require_identity=%v",
			authCfg.Method, authCfg.RequireIdentity)
	} else {
		// 未开启认证必须留下痕迹：这是"这个集群的所有 Run 都没有已认证身份"
		// 的唯一可观测证据。安全复盘时若发现 agent_run.auth_method 全为空，
		// 这条日志能直接说明原因，而不必去猜是配置丢了还是代码没接。
		log.Println("runtime: authentication disabled (AUTH_ENABLED is not set); runs will carry no authenticated identity")
	}

	// THREAD_ID 标识会话维度：同一 Thread 下的多个 Run 共享长期记忆。
	// 留空表示无会话隔离，记忆检索将跳过 thread_id 过滤。
	//
	// AUTH_TOKEN 是本进程的凭证来源：CLI 场景下由调用方通过环境变量传入，
	// 注入 ctx 后由 Runtime.CreateRun 取出校验。刻意不进 ExecutionContext ——
	// EC 会落库并进入审计日志，凭证一旦进去就等于泄露（见 contracts/auth.go）。
	ctx = contracts.WithAuthToken(ctx, env("AUTH_TOKEN", ""))
	run, err := rt.CreateRun(ctx, env("AUTH_TENANT", "default"), "demo", "why is project delayed?", env("THREAD_ID", ""))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("created run:", run.ID, "thread:", run.ThreadID, "user:", run.UserID)
}

// newAuthLifecycle 构造 Run 生命周期的认证审计环节。
//
// 无论认证是否开启都返回非 nil 实例：开启时它执行策略校验，
// 未开启时它仍会记录"这次 Run 没有已认证身份"（AuthMethod="none"），
// 让未接认证的部署在审计里可见，而不是静默地什么都不留。
func newAuthLifecycle(cfg auth.Config) runtime.RunLifecycle {
	lc := &middleware.AuthLifecycle{RequireIdentity: cfg.RequireIdentity}
	lc.OnAudit = func(_ context.Context, rec middleware.AuditRecord) {
		// 审计输出不含凭证与被拦内容，只有身份维度与原因标识（见 AuditRecord 注释）。
		if rec.Denied {
			log.Printf("warn: %s", rec.AuditString())
			return
		}
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
