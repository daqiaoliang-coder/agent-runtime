// ShadowWriter 是 Writer 的双写对比实现（INDEXER_BACKEND=shadow），
// 与读路径的 ShadowMemory 对应：直连（Primary）与 rag-api（Secondary）
// 并行写入，结果恒以直连为准，远程失败只打 warn 日志——影子期内
// 索引行为与 local 后端完全一致，切换 remote 的风险被限制在日志噪音。
//
// 与 ShadowMemory 的差异：写路径无 800ms 预算约束（索引器是后台进程），
// 因此 Secondary 同步等待完成而非超时放弃——对比结论必须覆盖"远程是否
// 真的写成功"，半途而废的对比没有验收价值。
package memory

import (
	"context"
	"fmt"
	"log"
)

// ShadowWriter 双写 Primary（直连 Qdrant）与 Secondary（rag-api）。
// Secondary 为 nil 时退化为纯本地写入。
type ShadowWriter struct {
	Primary   Writer // 结果来源：LocalWriter
	Secondary Writer // 对比对象：RemoteWriter
}

var _ Writer = (*ShadowWriter)(nil)

// EnsureCollection 两边都必须就绪：影子对比的前提是两路都活着，
// 任一缺失（本地集合建不了 / rag-api 不可用）都应启动即失败，
// 带病运行的影子只会产出"远程错误"日志，毫无对比价值。
func (w *ShadowWriter) EnsureCollection(ctx context.Context, collection string, dim int) error {
	if w == nil || w.Primary == nil {
		return fmt.Errorf("memory: shadow writer not configured (primary required)")
	}
	if err := w.Primary.EnsureCollection(ctx, collection, dim); err != nil {
		return fmt.Errorf("shadow primary: %w", err)
	}
	if w.Secondary == nil {
		return nil
	}
	if err := w.Secondary.EnsureCollection(ctx, collection, dim); err != nil {
		return fmt.Errorf("shadow secondary: %w", err)
	}
	return nil
}

// Write 并行双写，返回 Primary 的结果。
//
// Primary 失败 → 错误上抛（索引器退避重试，远程一路的结果无关紧要）；
// Primary 成功而 Secondary 失败 → 仅 warn，不影响进度标记——
// 影子期远程本就可能滞后（部署节奏不同），这正是要在日志里暴露的问题。
// 两路都成功 → 记 match 日志，供灰度统计"写路径一致率"。
func (w *ShadowWriter) Write(ctx context.Context, collection string, docs []Document) error {
	if w == nil || w.Primary == nil {
		return fmt.Errorf("memory: shadow writer not configured (primary required)")
	}
	if len(docs) == 0 {
		return nil
	}
	type outcome struct{ err error }
	secCh := make(chan outcome, 1) // 缓冲：迟到的 goroutine 仍能发送后退出，不泄漏
	if w.Secondary != nil {
		go func() { secCh <- outcome{w.Secondary.Write(ctx, collection, docs)} }()
	}
	primErr := w.Primary.Write(ctx, collection, docs)
	if w.Secondary == nil {
		return primErr
	}
	sec := <-secCh
	if primErr != nil {
		// 主路失败已决定整批退避，远程一路的结果仅作诊断信息。
		if sec.err != nil {
			log.Printf("memory(shadow-write): both backends failed collection=%s docs=%d: primary=%v secondary=%v",
				collection, len(docs), primErr, sec.err)
		}
		return primErr
	}
	if sec.err != nil {
		log.Printf("memory(shadow-write): remote write failed (local succeeded, comparison inconclusive) collection=%s docs=%d: %v",
			collection, len(docs), sec.err)
		return nil
	}
	log.Printf("memory(shadow-write): match collection=%s docs=%d (local+remote both ok)", collection, len(docs))
	return nil
}
