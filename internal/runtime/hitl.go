package runtime

import (
	"agent-runtime/internal/model"
	"context"
	"fmt"
	"log"
)

// HITLStore keeps the interrupt record and Run state transition in one transaction.
// This prevents a crash from leaving WAITING_HUMAN without a durable interrupt record.
type HITLStore interface {
	InterruptRun(context.Context, string, string, string, string, int64) (bool, error)
	ResumeRun(context.Context, string, string, string, int64) (bool, error)
}

// Interrupt 在指定节点处中断运行中的 Run：通过 CAS 把 Run 状态从 RUNNING 切到
// WAITING_HUMAN，并在同一事务内持久化 interrupt 记录，避免崩溃后丢失人工介入上下文。
// 仅 RunRunning 状态可被中断；并发变更（version 不匹配）返回错误而非静默失败。
func (r *Runtime) Interrupt(ctx context.Context, tenant, runID, nodeID, reason string) error {
	h, ok := r.Store.(HITLStore)
	if !ok {
		return fmt.Errorf("hitl store is not configured")
	}
	run, err := r.Store.GetRun(ctx, tenant, runID)
	if err != nil {
		return err
	}
	if run.Status != model.RunRunning {
		return fmt.Errorf("run %s cannot be interrupted from %s", runID, run.Status)
	}
	ok, err = h.InterruptRun(ctx, tenant, runID, nodeID, reason, run.Version)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("run %s changed while interrupting", runID)
	}
	// 关键日志：Run 进入 WAITING_HUMAN，标志人工介入节点出现，需上层及时呈现给审核人。
	log.Printf("run interrupted run=%s tenant=%s node=%s reason=%q", runID, tenant, nodeID, reason)
	return nil
}

// Resume 在人工决策回流后把 Run 从 WAITING_HUMAN 切回执行流程，decision 携带
// 人工裁决内容。同样基于 CAS 保证只有等待中的 Run 能被恢复，避免重复恢复。
func (r *Runtime) Resume(ctx context.Context, tenant, runID, decision string) error {
	h, ok := r.Store.(HITLStore)
	if !ok {
		return fmt.Errorf("hitl store is not configured")
	}
	run, err := r.Store.GetRun(ctx, tenant, runID)
	if err != nil {
		return err
	}
	if run.Status != model.RunWaitingHuman {
		return fmt.Errorf("run %s is not waiting for human: %s", runID, run.Status)
	}
	ok, err = h.ResumeRun(ctx, tenant, runID, decision, run.Version)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("run %s changed while resuming", runID)
	}
	// 关键日志：Run 从 WAITING_HUMAN 恢复执行，标志人工决策回流到自动流程。
	log.Printf("run resumed run=%s tenant=%s decision=%q", runID, tenant, decision)
	return nil
}
