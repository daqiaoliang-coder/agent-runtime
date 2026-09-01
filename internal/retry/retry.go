// Package retry 实现节点执行的重试策略：指数退避 + 抖动 + 最大次数，耗尽后进入死信队列（DLQ）。
//
// 设计要点：
//   - ShouldRetry(attempt) 判断下一次执行是否仍允许，避免无限重试；
//   - Backoff(attempt) 给出指数增长 +抖动的等待时长，封顶 Max，避免惊群与雪崩；
//   - 与 store.RetryNode 配合：失败可重试时把节点置回 READY 并安排 ready_at（退避到期），
//     recovery 的 ReadyTasks 扫描在 ready_at 到期后补投递。
package retry

import (
	"math/rand"
	"time"
)

// Policy 描述重试策略。
// MaxAttempts 为节点最大执行总次数（含首次）；Initial 为首次退避；Factor 为指数倍数；
// Max 为退避上限；Jitter 为抖动比例（0~1）。
type Policy struct {
	MaxAttempts int
	Initial     time.Duration
	Factor      float64
	Max         time.Duration
	Jitter      float64
}

// Default 返回常用策略：最多 3 次、1s 起、2 倍指数、上限 30s、20% 抖动。
func Default() Policy {
	return Policy{MaxAttempts: 3, Initial: 1 * time.Second, Factor: 2, Max: 30 * time.Second, Jitter: 0.2}
}

// ShouldRetry 判断下一次 attempt（1 基）是否仍允许重试。
func (p Policy) ShouldRetry(nextAttempt int) bool {
	return nextAttempt <= p.MaxAttempts
}

// Backoff 计算第 attempt 次（1 基）重试前的等待时长：指数增长 + 抖动，封顶 Max。
// 抖动在 ±(Jitter/2)*base 范围内偏移，避免大量任务同时被唤醒。
func (p Policy) Backoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := p.Initial
	for i := 1; i < attempt; i++ {
		d = time.Duration(float64(d) * p.Factor)
		if d > p.Max {
			d = p.Max
			break
		}
	}
	if p.Jitter > 0 {
		half := time.Duration(float64(d) * p.Jitter / 2)
		if half > 0 {
			span := int64(half) * 2
			offset := time.Duration(rand.Int63n(span)) - half
			d = d + offset
		}
	}
	if d < 0 {
		d = 0
	}
	if d > p.Max {
		d = p.Max
	}
	return d
}
