// Package eval 提供检索评测的指标纯函数（Phase 3 golden set 闭环）。
//
// 全部函数无副作用、无 IO：单测给定输入即可断言输出，评测 CLI 只是
// 它们的一层编排。相关性采用二值判定（文档要么在 expected 集合里，
// 要么不在），与 golden set 的标注粒度一致。
package eval

import (
	"math"
	"sort"
)

// RecallAtK 计算 recall@k：前 k 个结果命中的期望文档占比。
// 重复命中的文档只计一次（结果列表理论上已去重，防御性兜底）。
// 期望集为空时返回 0（评测语料不应出现，防御性兜底）。
func RecallAtK(got, expected []string, k int) float64 {
	if len(expected) == 0 || k <= 0 {
		return 0
	}
	want := make(map[string]struct{}, len(expected))
	for _, id := range expected {
		want[id] = struct{}{}
	}
	if k > len(got) {
		k = len(got)
	}
	hitSet := make(map[string]struct{}, len(expected))
	for _, id := range got[:k] {
		if _, ok := want[id]; ok {
			hitSet[id] = struct{}{}
		}
	}
	return float64(len(hitSet)) / float64(len(expected))
}

// MRRAtK 计算 MRR@k：第一个命中期望文档的排名倒数，无命中为 0。
func MRRAtK(got, expected []string, k int) float64 {
	if len(expected) == 0 || k <= 0 {
		return 0
	}
	want := make(map[string]struct{}, len(expected))
	for _, id := range expected {
		want[id] = struct{}{}
	}
	if k > len(got) {
		k = len(got)
	}
	for i, id := range got[:k] {
		if _, ok := want[id]; ok {
			return 1 / float64(i+1)
		}
	}
	return 0
}

// NDCGAtK 计算 nDCG@k（二值相关）：DCG 的折损位置从第 2 位起
// （log2(rank+1)，rank 从 1 计）。IDCG 取 min(k, |expected|) 个
// 相关文档全部排在头部的理想值。
func NDCGAtK(got, expected []string, k int) float64 {
	if len(expected) == 0 || k <= 0 {
		return 0
	}
	want := make(map[string]struct{}, len(expected))
	for _, id := range expected {
		want[id] = struct{}{}
	}
	if k > len(got) {
		k = len(got)
	}
	dcg := 0.0
	for i, id := range got[:k] {
		if _, ok := want[id]; ok {
			dcg += 1 / math.Log2(float64(i+2))
		}
	}
	ideal := k
	if ideal > len(expected) {
		ideal = len(expected)
	}
	idcg := 0.0
	for i := 0; i < ideal; i++ {
		idcg += 1 / math.Log2(float64(i+2))
	}
	if idcg == 0 {
		return 0
	}
	return dcg / idcg
}

// Percentile 计算最近邻分位值（p ∈ (0,1]，如 0.5/0.95）。
// 输入会被拷贝后排序，不修改原切片；空输入返回 0。
func Percentile(values []float64, p float64) float64 {
	if len(values) == 0 || p <= 0 || p > 1 {
		return 0
	}
	sorted := make([]float64, len(values))
	copy(sorted, values)
	sort.Float64s(sorted)
	idx := int(math.Ceil(p*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// Mean 求算术平均，空输入返回 0。
func Mean(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	sum := 0.0
	for _, x := range xs {
		sum += x
	}
	return sum / float64(len(xs))
}
