// 本文件实现记忆后端的两种组合模式，服务于读路径外移到 rag-api 的迁移：
//   - ShadowMemory：双跑对比（MEMORY_BACKEND=shadow），本地直连与远程并行执行，
//     结果恒以直连为准，差异只打日志——切换后端前的等价性验收手段；
//   - FallbackMemory：失败回退（MEMORY_BACKEND=remote + MEMORY_REMOTE_FALLBACK），
//     远程故障或降级时自动落回本地直连，记忆能力不中断。
package providers

import (
	"agent-runtime/internal/contracts"
	"context"
	"fmt"
	"log"
	"strings"
	"unicode/utf8"
)

// ShadowMemory 双跑对比 Primary（直连 Qdrant）与 Secondary（rag-api）的召回结果。
//
// 关键约束：**结果恒以 Primary 为准**。即使对比发现差异也只打 warn 日志，
// 绝不改变注入 LLM 的上下文——影子期内线上行为与 MEMORY_BACKEND=local
// 完全一致，切换后端的风险被限制在"日志里多了对比结果"。
//
// 两路并行执行且共享同一 ctx 预算，总耗时是 max(本地, 远程) 而非两者之和，
// 影子期不放大召回延迟（800ms 预算不变）。
type ShadowMemory struct {
	Primary   MemoryProvider // 结果来源：直连 Qdrant 的 VectorMemory
	Secondary MemoryProvider // 对比对象：rag-api 的 RemoteMemory；nil 时退化为纯本地
}

var (
	_ MemoryProvider = (*ShadowMemory)(nil)
	_ MemorySearcher = (*ShadowMemory)(nil)
)

// shadowOutcome 是一路双跑的观测结果。degraded 仅远程路径填充：
// MemorySearcher 契约把降级折叠成 (nil,nil)，区分二者才能避免把
// 远程故障误报成"结果不一致"。
type shadowOutcome struct {
	msgs     []contracts.Message
	err      error
	degraded []string
}

// Search 并行执行两路召回，返回 Primary 的结果并记录对比结论。
func (s *ShadowMemory) Search(ctx context.Context, ec contracts.ExecutionContext, query string, topK int) ([]contracts.Message, error) {
	if s == nil || s.Primary == nil {
		return nil, nil
	}
	prim, ok := s.Primary.(MemorySearcher)
	if !ok {
		return nil, nil
	}
	// 无对比对象时退化为纯本地：shadow 是观测手段，
	// 不该因 Secondary 缺席而丢记忆。
	if s.Secondary == nil {
		return prim.Search(ctx, ec, query, topK)
	}

	primCh := make(chan shadowOutcome, 1)
	secCh := make(chan shadowOutcome, 1)
	go func() {
		msgs, err := prim.Search(ctx, ec, query, topK)
		primCh <- shadowOutcome{msgs: msgs, err: err}
	}()
	go func() {
		secCh <- s.runSecondary(ctx, ec, query, topK)
	}()

	// 主路必须等到结果（预算耗尽时 VectorMemory 按 ctx 契约返回空）；
	// ctx.Done 后仍尝试非阻塞接收一次，兜住"结果与超时同时到达"的竞态。
	var p, r shadowOutcome
	select {
	case p = <-primCh:
	case <-ctx.Done():
		select {
		case p = <-primCh:
		default:
		}
	}
	// 副路在预算耗尽后放弃等待：不守约的 Secondary 不能拖住召回主链路。
	// 缓冲通道保证迟到的 goroutine 仍能完成发送后退出，不泄漏。
	select {
	case r = <-secCh:
	case <-ctx.Done():
		r = shadowOutcome{err: ctx.Err()}
	}

	if p.err != nil {
		// Primary 契约上不该返回 error，防御性降级（同 recallMemory 的兜底）。
		log.Printf("memory(shadow): primary error tenant=%s thread=%s run=%s: %v",
			ec.TenantID, ec.ThreadID, ec.RunID, p.err)
		return nil, nil
	}
	s.compare(ec, query, p.msgs, r)
	return p.msgs, nil
}

// runSecondary 执行远程一路并保留可区分的降级信号。
// 非 RemoteMemory 的 Secondary 退回普通 Search（无法区分降级与空结果）。
func (s *ShadowMemory) runSecondary(ctx context.Context, ec contracts.ExecutionContext, query string, topK int) shadowOutcome {
	if rm, ok := s.Secondary.(*RemoteMemory); ok {
		resp, err := rm.searchDetailed(ctx, ec, query, topK)
		if err != nil {
			return shadowOutcome{err: err}
		}
		return shadowOutcome{msgs: resp.messages(), degraded: resp.Degraded}
	}
	if sec, ok := s.Secondary.(MemorySearcher); ok {
		msgs, err := sec.Search(ctx, ec, query, topK)
		return shadowOutcome{msgs: msgs, err: err}
	}
	return shadowOutcome{}
}

// compare 记录对比结论。恒只打日志，不影响任何返回值。
func (s *ShadowMemory) compare(ec contracts.ExecutionContext, query string, local []contracts.Message, remote shadowOutcome) {
	switch {
	case remote.err != nil:
		log.Printf("memory(shadow): remote error, comparison skipped tenant=%s thread=%s run=%s query=%q: %v",
			ec.TenantID, ec.ThreadID, ec.RunID, truncateForLog(query, 80), remote.err)
	case len(remote.degraded) > 0:
		log.Printf("memory(shadow): remote degraded(%s), comparison skipped tenant=%s thread=%s run=%s query=%q",
			strings.Join(remote.degraded, ","), ec.TenantID, ec.ThreadID, ec.RunID, truncateForLog(query, 80))
	case shadowEqual(local, remote.msgs):
		// 一致是影子验收的正态：逐条记录，供灰度统计"一致率"。
		log.Printf("memory(shadow): match local=%d remote=%d tenant=%s thread=%s run=%s",
			len(local), len(remote.msgs), ec.TenantID, ec.ThreadID, ec.RunID)
	default:
		idx := firstShadowDiff(local, remote.msgs)
		log.Printf("memory(shadow): MISMATCH local=%d remote=%d first_diff=%d tenant=%s thread=%s run=%s query=%q local[%d]=%s remote[%d]=%s",
			len(local), len(remote.msgs), idx, ec.TenantID, ec.ThreadID, ec.RunID, truncateForLog(query, 80),
			idx, describeForLog(local, idx), idx, describeForLog(remote.msgs, idx))
	}
}

// shadowEqual 逐条比较角色与内容。顺序敏感：两路都按 (created_at, node_id)
// 正序返回，顺序差异本身就是行为差异，必须被对比捕获。
func shadowEqual(a, b []contracts.Message) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Role != b[i].Role || a[i].Content != b[i].Content {
			return false
		}
	}
	return true
}

// firstShadowDiff 返回首个不一致的下标；一侧多出的尾部记为较短侧的长度。
func firstShadowDiff(a, b []contracts.Message) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if a[i].Role != b[i].Role || a[i].Content != b[i].Content {
			return i
		}
	}
	return n
}

func describeForLog(msgs []contracts.Message, idx int) string {
	if idx < 0 || idx >= len(msgs) {
		return "<missing>"
	}
	return fmt.Sprintf("{%s %q}", msgs[idx].Role, truncateForLog(msgs[idx].Content, 80))
}

// truncateForLog 按 rune 截断，避免多字节字符被切断与日志爆炸。
func truncateForLog(s string, n int) string {
	if n <= 0 || utf8.RuneCountInString(s) <= n {
		return s
	}
	return string([]rune(s)[:n]) + "..."
}

// Load 委托 Primary（影子期行为与本地一致）。
func (s *ShadowMemory) Load(ctx context.Context, ec contracts.ExecutionContext) ([]contracts.Message, error) {
	if s == nil || s.Primary == nil {
		return nil, nil
	}
	return s.Primary.Load(ctx, ec)
}

// Save 返回明确错误：只读视图，写入由 cmd/memory-indexer 投影。
func (s *ShadowMemory) Save(context.Context, contracts.ExecutionContext, []contracts.Message) error {
	return fmt.Errorf("providers: ShadowMemory is read-only; memory writes are projected by cmd/memory-indexer from committed nodes")
}

// FallbackMemory 在主记忆（rag-api）失败或降级时回退到备记忆（直连 Qdrant）。
//
// 回退触发条件是刻意的：只在远程**明确失败（error）或自报降级（degraded
// 非空）**时回退；远程返回空结果属于合法答案（可能真没有相关历史），
// 不触发回退——否则"无记忆"会被错误地放大成一次额外的本地全量检索，
// 双倍消耗 embedding 与 Qdrant 配额。
type FallbackMemory struct {
	Primary  MemoryProvider // 主：RemoteMemory（rag-api）
	Fallback MemoryProvider // 备：VectorMemory（直连 Qdrant）；nil 时远程失败即降级为空
}

var (
	_ MemoryProvider = (*FallbackMemory)(nil)
	_ MemorySearcher = (*FallbackMemory)(nil)
)

// Search 先走远程，失败/降级时回退本地。
func (f *FallbackMemory) Search(ctx context.Context, ec contracts.ExecutionContext, query string, topK int) ([]contracts.Message, error) {
	if f == nil || f.Primary == nil {
		return nil, nil
	}
	if rm, ok := f.Primary.(*RemoteMemory); ok {
		resp, err := rm.searchDetailed(ctx, ec, query, topK)
		switch {
		case err != nil:
			log.Printf("memory(fallback): remote failed, falling back to local tenant=%s thread=%s run=%s: %v",
				ec.TenantID, ec.ThreadID, ec.RunID, err)
			return f.searchFallback(ctx, ec, query, topK)
		case len(resp.Degraded) > 0:
			log.Printf("memory(fallback): remote degraded(%s), falling back to local tenant=%s thread=%s run=%s",
				strings.Join(resp.Degraded, ","), ec.TenantID, ec.ThreadID, ec.RunID)
			return f.searchFallback(ctx, ec, query, topK)
		}
		return resp.messages(), nil
	}
	// 非 RemoteMemory 主路：契约只承诺 (nil,nil) 降级，无法区分空结果，
	// 仅显式 error 时回退（防御分支，当前装配不会走到）。
	if prim, ok := f.Primary.(MemorySearcher); ok {
		msgs, err := prim.Search(ctx, ec, query, topK)
		if err == nil {
			return msgs, nil
		}
		log.Printf("memory(fallback): primary error, falling back to local tenant=%s thread=%s run=%s: %v",
			ec.TenantID, ec.ThreadID, ec.RunID, err)
	}
	return f.searchFallback(ctx, ec, query, topK)
}

// searchFallback 执行本地直连召回；VectorMemory 自身契约保证失败降级为
// (nil,nil)，因此这里的返回值可以直接透传。
func (f *FallbackMemory) searchFallback(ctx context.Context, ec contracts.ExecutionContext, query string, topK int) ([]contracts.Message, error) {
	if f.Fallback == nil {
		return nil, nil
	}
	if fs, ok := f.Fallback.(MemorySearcher); ok {
		return fs.Search(ctx, ec, query, topK)
	}
	return nil, nil
}

// Load 委托 Primary。
func (f *FallbackMemory) Load(ctx context.Context, ec contracts.ExecutionContext) ([]contracts.Message, error) {
	if f == nil || f.Primary == nil {
		return nil, nil
	}
	return f.Primary.Load(ctx, ec)
}

// Save 返回明确错误：只读视图，写入由 cmd/memory-indexer 投影。
func (f *FallbackMemory) Save(context.Context, contracts.ExecutionContext, []contracts.Message) error {
	return fmt.Errorf("providers: FallbackMemory is read-only; memory writes are projected by cmd/memory-indexer from committed nodes")
}
