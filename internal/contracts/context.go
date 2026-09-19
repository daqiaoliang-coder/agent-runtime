package contracts

import "context"

// 本文件解决一个装配层的现实约束：Executor 接口是
//
//	Execute(ctx context.Context, n *model.Node) (string, error)
//
// 它只认节点，不认执行上下文。而中间件链（护栏 / 脱敏）的每一次拦截都需要
// ExecutionContext 才能做租户级审计与人工闸门定位。给 Executor 加一个参数会
// 波及全部实现与测试，且 ExecutionContext 本质上是"随调用链下行的环境信息"，
// 与 ctx 的语义一致——所以走 ctx 传递，而不是改接口。
//
// 取舍必须说清：ctx 传值的代价是**编译期不可见**。调用方忘了注入时，
// 下游拿到的是零值 ExecutionContext（TenantID 为空），审计记录会静默缺失租户维度。
// 因此约定：注入点只有 worker.Handle 一处（节点执行的唯一入口），
// 且注入前已断言拿到非空 TenantID；读取方在 EC 缺失时按"无身份"降级而非报错，
// 保证未接线的旧路径（测试、ReAct 内存执行）行为不变。

// ecKey 是 ExecutionContext 在 ctx 中的键。
// 用不可导出的自定义类型而非字符串，避免与其他包的 ctx 键碰撞。
type ecKey struct{}

// WithExecutionContext 把执行上下文注入 ctx，返回派生 ctx。
func WithExecutionContext(ctx context.Context, ec ExecutionContext) context.Context {
	return context.WithValue(ctx, ecKey{}, ec)
}

// ExecutionContextFrom 读取 ctx 中的执行上下文。
// 未注入时返回零值与 false，调用方据此决定是否降级。
func ExecutionContextFrom(ctx context.Context) (ExecutionContext, bool) {
	ec, ok := ctx.Value(ecKey{}).(ExecutionContext)
	return ec, ok
}
