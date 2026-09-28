package runtime

import (
	"agent-runtime/internal/contracts"
	"agent-runtime/internal/model"
	"agent-runtime/internal/obs"
	"agent-runtime/internal/trace"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

// CancelStore 把 Run 翻到 CANCEL_REQUESTED 并取消其 PENDING/READY 节点的原子操作。
// 与 HITLStore 同理：以独立接口暴露，*store.MySQL 天然满足，fakeStore 可按需实现。
type CancelStore interface {
	CancelRun(ctx context.Context, tenant, runID, reason string, version int64) (bool, error)
}

// RunLifecycle 是 Run 生命周期的横切钩子，*middleware.LifecycleChain 天然满足。
//
// 在 runtime 包内声明这个窄接口而不是直接依赖 middleware，是沿用本包既有惯例
// （CancelStore / HITLStore / DecisionStore 同理）：runtime 是内核层，
// 不该知道横切层的具体类型；由装配层把中间件链注入进来即可。
// 这同时避免了 runtime ↔ middleware 的依赖纠缠 —— middleware 侧的
// AuthLifecycle 只需要 contracts，不反向依赖 runtime。
type RunLifecycle interface {
	OnRunStart(ctx context.Context, ec contracts.ExecutionContext) error
	OnRunFinish(ctx context.Context, ec contracts.ExecutionContext, runErr error) error
}

// Runtime 负责创建 Run 并完成初始调度。
// 它组合了持久化（Store）、任务队列（Queue）和规划器（Planner）。
// Store/Queue 为接口类型，便于单元测试注入 fake。
//
// Authenticator / RequireIdentity / Lifecycle 三个字段共同构成认证接入点，
// 全部为可选：零值时 CreateRun 的行为与接入认证前完全一致
// （身份为空、不执行钩子），存量部署与既有测试无需任何改动。
type Runtime struct {
	Store   Store
	Queue   Queue
	Planner Planner
	// Authenticator 非 nil 时，CreateRun 从 ctx 取出原始凭证（见 contracts.AuthTokenFrom）
	// 校验身份，并把结果写入 agent_run.user_id / auth_method。
	//
	// 为 nil 时不校验：身份留空，下游依赖身份的判定应当拒绝而非放行。
	Authenticator contracts.Authenticator
	// RequireIdentity 为 true 时，无法确定身份的 CreateRun 一律拒绝。
	//
	// 默认 false 是为了不破坏未接入 IdP 的存量部署 —— 认证是横切整条链路的能力，
	// 若默认强制，升级会让这些部署立刻全站不可用，结果往往是干脆不升级，
	// 安全状况反而更差。生产环境应当显式置 true。
	RequireIdentity bool
	// Lifecycle 是 Run 生命周期的横切链；为 nil 时不执行任何钩子。
	// OnRunStart 在 Run 落库**之前**执行，OnRunFinish 由 Resumer 在收敛到终态时执行。
	Lifecycle RunLifecycle
}

// authenticate 把 ctx 中的原始凭证换成可信身份。
//
// 三个分支的处置各不相同，必须区分：
//   - 未配置 Authenticator：返回零值身份（未认证），由 RequireIdentity 决定是否放行；
//   - 配置了但 ctx 里没有凭证：这是部署缺陷或越权尝试，返回 ErrMissingToken；
//   - 配置了且凭证校验失败：原样上抛适配器的错误，保留 errors.Is 可分流的哨兵语义。
//
// 认证发生在**任何数据库写入之前**：被拒绝的 Run 不该在 agent_run 留下痕迹，
// 否则未授权的尝试会污染业务表，而它们本该只出现在审计日志里。
func (r *Runtime) authenticate(ctx context.Context, tenant, runID string) (contracts.Identity, error) {
	if r.Authenticator == nil {
		if r.RequireIdentity {
			return contracts.Identity{}, fmt.Errorf("%w: run %s in tenant %q: no authenticator configured",
				contracts.ErrUnauthenticated, runID, tenant)
		}
		return contracts.Identity{}, nil
	}
	token, ok := contracts.AuthTokenFrom(ctx)
	if !ok {
		return contracts.Identity{}, fmt.Errorf("%w: run %s", contracts.ErrMissingToken, runID)
	}
	id, err := r.Authenticator.Authenticate(ctx, contracts.StripBearerScheme(token))
	if err != nil {
		// 不回显 token 与错误原文之外的任何内容；适配器已保证错误里不含凭证。
		return contracts.Identity{}, fmt.Errorf("authenticate run %s: %w", runID, err)
	}
	if !id.Authenticated() {
		return contracts.Identity{}, fmt.Errorf("%w: run %s: authenticator returned empty identity",
			contracts.ErrUnauthenticated, runID)
	}
	// 租户交叉校验：凭证声称的租户必须与本次 Run 的租户一致。
	//
	// 这里校验一次、middleware.AuthLifecycle.OnRunStart 又校验一次，看似重复，
	// 实则覆盖两条不同的路径：本处挡住"A 租户凭证创建 B 租户 Run"，
	// 那是**创建时**的越权；AuthLifecycle 另外覆盖 worker/resume 侧
	// 从库里还原身份后重新执行 OnRunStart 的场景。两处判据一致但触发点不同，
	// 缺一不可 —— 只在入口校验会让"库里被改过的身份"逃过检查。
	if id.TenantID != "" && tenant != "" && id.TenantID != tenant {
		return contracts.Identity{}, fmt.Errorf("%w: identity tenant %q cannot create run in tenant %q",
			contracts.ErrTenantMismatch, id.TenantID, tenant)
	}
	return id, nil
}

// runExecutionContext 从已落库的 Run 还原执行上下文。
//
// 只还原能落库的三个身份维度（UserID/TenantID/AuthMethod）：
// Scopes 与 ExpiresAt 不入库，因此 worker 侧的 EC 里它们恒为空。
// 需要 scope 判定的环节应当在入口进程完成（见 middleware.AuthLifecycle 的 identityOf 注释）。
func runExecutionContext(run *model.Run) contracts.ExecutionContext {
	return contracts.ExecutionContext{
		TenantID:   run.TenantID,
		UserID:     run.UserID,
		AuthMethod: run.AuthMethod,
		ThreadID:   run.ThreadID,
		RunID:      run.ID,
	}
}

// CreateRun 创建一次 Agent 运行，流程如下：
//  1. 生成 Run 记录并写入 MySQL；
//  2. 调用 Planner 生成 DAG 计划并落库；
//  3. 将无依赖的根节点标记为 READY 并投递到 Redis 队列；
//  4. 通过 CAS 将 Run 状态从 PENDING 切换为 RUNNING，保证并发安全。
//
// threadID 标识会话维度：同一 Thread 下的多个 Run 共享长期记忆（见 providers.MemorySearcher）。
// 传空串表示无会话隔离，记忆检索将跳过 thread_id 过滤。
//
// 所有落库与入队操作均携带租户身份（run.TenantID），保证后续跨进程链路可做租户隔离。
//
// 身份认证在**任何数据库写入之前**完成：未配置 Authenticator 时行为与从前完全一致；
// 配置后则从 ctx 取原始凭证校验，身份写入 agent_run.user_id / auth_method。
// 顺序是刻意的 —— 被拒绝的 Run 不该在业务表留下痕迹，
// 未授权尝试只应出现在审计日志里，否则 agent_run 会被撞库流量污染。
func (r *Runtime) CreateRun(ctx context.Context, tenant, agent, input, threadID string) (*model.Run, error) {
	// Run 创建的根 span：整条 Trace 的起点，串联 planner、入队与下游 worker 执行。
	ctx, span := trace.StartSpan(ctx, "run.create")
	defer span.End()
	id := fmt.Sprintf("run-%d", time.Now().UnixNano())
	span.SetAttributes(
		attribute.String("run.id", id),
		attribute.String("tenant.id", tenant),
		attribute.String("agent.id", agent),
		attribute.String("thread.id", threadID),
	)

	// 1) 认证：凭证 → 身份。失败即返回，不产生任何持久化痕迹。
	identity, err := r.authenticate(ctx, tenant, id)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		// 认证失败是安全事件，必须留下日志：它是"有人在用无效凭证尝试创建 Run"
		// 的唯一可观测证据。刻意只记身份维度与错误类别，不记凭证原文。
		obs.From(ctx).WarnContext(ctx, "run creation denied by authentication",
			"run_id", id, "tenant_id", tenant, "error", err.Error())
		return nil, err
	}
	// 身份维度进 span：让追踪系统能按"谁发起的"检索整条 Trace。
	if identity.UserID != "" {
		span.SetAttributes(
			attribute.String("user.id", identity.UserID),
			attribute.String("auth.method", identity.Method),
		)
	}

	run := &model.Run{
		ID: id, TenantID: tenant, ThreadID: threadID, AgentID: agent,
		UserID: identity.UserID, AuthMethod: identity.Method,
		Status: model.RunPending, Input: input,
		MaxSteps: 50, MaxRounds: 10, CreatedAt: time.Now(), UpdatedAt: time.Now(),
	}

	// 2) 生命周期钩子（认证策略复核 + 审计）在落库前执行，同样遵循"被拒不落库"。
	//    ec 里的 Scopes 来自凭证，此刻是本进程内唯一能拿到完整 scope 的时机。
	if r.Lifecycle != nil {
		ec := runExecutionContext(run)
		ec.Scopes = identity.Scopes
		if err := r.Lifecycle.OnRunStart(ctx, ec); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return nil, fmt.Errorf("run lifecycle start: %w", err)
		}
	}

	if err := r.Store.CreateRun(ctx, run); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	plan, err := r.Planner.Plan(ctx, run)
	if err != nil {
		// 计划失败：Run 已落库为 PENDING 但不会有任何节点，事件驱动的 Resumer 无法感知它。
		// 主动 CAS 收敛为 FAILED，避免留下永不终止的孤儿 Run（最佳努力：标记失败不掩盖原始错误）。
		reason := fmt.Sprintf("planning failed: %v", err)
		if ok, cerr := r.Store.UpdateRunCAS(ctx, run.TenantID, run.ID, run.Version, model.RunFailed, "", reason); cerr != nil {
			obs.From(ctx).ErrorContext(ctx, "run mark-failed on planning error failed",
				"run_id", run.ID, "tenant_id", run.TenantID, "error", cerr, "planning_error", err)
		} else if !ok {
			obs.From(ctx).WarnContext(ctx, "run mark-failed on planning error skipped (version conflict)",
				"run_id", run.ID, "tenant_id", run.TenantID)
		}
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	if err := r.Store.InsertPlan(ctx, run.ID, run.TenantID, plan); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	// 先将 Run 切换到 RUNNING，再入队根节点，避免 worker 抢先消费时看到 PENDING 而误取消。
	// 根节点已 MarkReady，即使入队失败，ReadyTasks 扫描也会在恢复周期中补投递。
	ok, _ := r.Store.UpdateRunCAS(ctx, run.TenantID, run.ID, run.Version, model.RunRunning, "", "")
	if !ok {
		err := fmt.Errorf("run version conflict")
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	// 仅入队无依赖的根节点，其余节点等待依赖完成后由 Resumer 推进。
	for _, n := range plan.Nodes {
		if len(n.DependsOn) == 0 {
			if err := r.Store.MarkReady(ctx, run.TenantID, n.ID); err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
				return nil, err
			}
			if err := r.Queue.Enqueue(ctx, model.Task{RunID: run.ID, NodeID: n.ID, TenantID: run.TenantID}); err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
				return nil, err
			}
		}
	}
	// 关键日志：Run 创建并切到 RUNNING，标志一次 Agent 运行的真正起点，串联调度入口与下游 worker。
	obs.From(ctx).InfoContext(ctx, "run created",
		"run_id", run.ID, "tenant_id", run.TenantID, "agent_id", run.AgentID, "root_nodes", countRoots(plan.Nodes))
	span.SetAttributes(attribute.Int("plan.node_count", len(plan.Nodes)))
	return r.Store.GetRun(ctx, run.TenantID, run.ID)
}

// countRoots 统计 DAG 中无依赖的根节点数，用于在创建日志中反映初始并行度。
func countRoots(nodes []model.PlanNode) (n int) {
	for _, x := range nodes {
		if len(x.DependsOn) == 0 {
			n++
		}
	}
	return n
}

// EventJSON 将领域事件序列化为 JSON 字符串，便于投递到消息中间件。
func EventJSON(e model.Event) string { b, _ := json.Marshal(e); return string(b) }

// Cancel 请求取消一个运行中的 Run：通过 CAS 把 Run 从 RUNNING 切到 CANCEL_REQUESTED，
// 并在同一事务内把该 Run 下所有 PENDING/READY 节点置为 CANCELLED，阻止 worker 认领新节点。
// 正在执行的 RUNNING 节点不被中断：它们会自然完成，Resumer 据此收敛 Run 到 CANCELLED。
// 仅 RunRunning 状态可被取消；并发变更（version 不匹配）返回错误而非静默失败。
func (r *Runtime) Cancel(ctx context.Context, tenant, runID, reason string) error {
	cs, ok := r.Store.(CancelStore)
	if !ok {
		return fmt.Errorf("cancel store is not configured")
	}
	run, err := r.Store.GetRun(ctx, tenant, runID)
	if err != nil {
		return err
	}
	if run.Status != model.RunRunning {
		return fmt.Errorf("run %s cannot be cancelled from %s", runID, run.Status)
	}
	ok, err = cs.CancelRun(ctx, tenant, runID, reason, run.Version)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("run %s changed while cancelling", runID)
	}
	// 关键日志：用户取消请求已落盘，Run 进入 CANCEL_REQUESTED，等待节点收敛到 CANCELLED。
	obs.From(ctx).InfoContext(ctx, "run cancel requested",
		"run_id", runID, "tenant_id", tenant, "reason", reason)
	return nil
}
