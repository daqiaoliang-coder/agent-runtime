package runtime

import (
	"agent-runtime/internal/model"
	"context"
	"fmt"
	"log"
)

// HITLStore keeps the interrupt record and Run state transition in one transaction.
// This prevents a crash from leaving WAITING_HUMAN without a durable interrupt record.
//
// ResumeRun 返回被重新武装为 READY 的节点，调用方必须投递到队列。
// 少这一步的后果是 Run 回到 RUNNING 但没有任何节点在跑：
// ReadyTasks 会捞到它们，但要等下一次 recovery 扫描周期，
// 期间整个 Run 表现为"恢复了却没动"，是最难排查的一类卡死。
type HITLStore interface {
	InterruptRun(context.Context, string, string, string, string, int64) (bool, error)
	ResumeRun(context.Context, string, string, string, int64) (bool, []model.Task, error)
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
	ok, tasks, err := h.ResumeRun(ctx, tenant, runID, decision, run.Version)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("run %s changed while resuming", runID)
	}
	// 把重新就绪的节点投递出去，否则 Run 回到 RUNNING 却没有节点在跑。
	// 入队失败不视为致命：节点状态已是 READY，recovery 的 ReadyTasks 扫描
	// 会在下一周期补投递，链路仍能收敛 —— 而把整个 Resume 判失败会让
	// 人工决策看起来"没生效"，运维会反复点确认，反而制造重复恢复。
	for _, t := range tasks {
		if r.Queue != nil {
			if qerr := r.Queue.Enqueue(ctx, t); qerr != nil {
				log.Printf("warn: re-enqueue after resume failed run=%s node=%s: %v (recovery scan will retry)", runID, t.NodeID, qerr)
			}
		}
	}
	// 关键日志：Run 从 WAITING_HUMAN 恢复执行，标志人工决策回流到自动流程。
	// 重新投递的 tasks 覆盖两类中断：护栏触发（节点已 Claim，挂起前为 RUNNING）
	// 与 policy gateway 触发（ClaimNode 之前中断，节点为 READY）——
	// InterruptRun 在同一事务内把两种状态的节点都置为 WAITING_HUMAN，
	// ResumeRun 再统一重新武装，无需再按 run.CurrentNodeID 补一次 MarkReady。
	log.Printf("run resumed run=%s tenant=%s decision=%q requeued=%d", runID, tenant, decision, len(tasks))
	return nil
}

// RejectStore 是人工否决的存储端口。与 CancelStore 同理以独立小接口暴露：
// runtime 的测试替身按需实现，不必为一个新能力全体升级。
type RejectStore interface {
	RejectRun(context.Context, string, string, string, int64) (bool, error)
}

// Reject 是 Resume 的对偶：人工否决被挂起的 Run。被挂起节点在同一事务内
// 置为 FAILED 并写 AgentStepFailed Outbox 事件（store.RejectRun），由
// Resumer 按既有失败语义收敛 Run；不重新投递任何任务。decision 记录
// 否决人与理由，进入 run_interrupt.decision——HasResolvedApproval 会排除
// deny 开头的 decision，杜绝"已否决"被误读成"已放行"。
func (r *Runtime) Reject(ctx context.Context, tenant, runID, decision string) error {
	h, ok := r.Store.(RejectStore)
	if !ok {
		return fmt.Errorf("reject store is not configured")
	}
	run, err := r.Store.GetRun(ctx, tenant, runID)
	if err != nil {
		return err
	}
	if run.Status != model.RunWaitingHuman {
		return fmt.Errorf("run %s is not waiting for human: %s", runID, run.Status)
	}
	ok, err = h.RejectRun(ctx, tenant, runID, decision, run.Version)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("run %s changed while rejecting", runID)
	}
	// 关键日志：人工否决回流，Run 回到 RUNNING 等待 Resumer 按失败事件收敛。
	log.Printf("run rejected run=%s tenant=%s decision=%q", runID, tenant, decision)
	return nil
}
