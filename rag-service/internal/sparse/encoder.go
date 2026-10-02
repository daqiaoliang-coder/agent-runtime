// Package sparse 实现查询侧与索引侧共用的词法（稀疏）编码器。
//
// Phase 2 采用零依赖的 TF（词频）编码：CJK 二元组 + ASCII 词，
// 权重经 L2 归一化。它不是最优的稀疏表示（生产可换 SPLADE / BGE-M3
// sparse），但满足两个硬要求：
//  1. 确定性——同一文本永远编码出同一稀疏向量，这是写入/检索两侧
//     可比的前提（与 dense embedding 的模型一致性要求同理）；
//  2. 查询与文档同一编码器——两侧分词规则必须完全一致，
//     否则词法召回退化为随机匹配。
package sparse

import (
	"math"
	"sort"
	"unicode"

	"github.com/daqiaoliang-coder/rag-service/internal/vector"
)

// Encoder 把文本编码为稀疏向量。
type Encoder interface {
	Encode(text string) vector.SparseVector
}

// TF 是内置 TF 编码器。无状态、并发安全。
type TF struct{}

// NewTF 返回内置 TF 编码器。
func NewTF() TF { return TF{} }

// Encode 实现：小写化 → 分词（ASCII 连续字母数字段、CJK 连续段切二元组）
// → 词频统计 → L2 归一化 → 按 Indices 升序输出（Qdrant 要求）。
//
// 单字 CJK 段（长度 1）直接取该字，避免整段丢失。
func (TF) Encode(text string) vector.SparseVector {
	counts := make(map[string]float64)
	for _, tok := range tokenize(text) {
		counts[tok]++
	}
	if len(counts) == 0 {
		return vector.SparseVector{}
	}
	// L2 归一化：使稀疏点积落在 [0,1]，与余弦相似度量纲对齐，
	// 便于未来做分数融合或阈值调参。
	var norm float64
	for _, c := range counts {
		norm += c * c
	}
	norm = math.Sqrt(norm)

	out := vector.SparseVector{
		Indices: make([]uint32, 0, len(counts)),
		Values:  make([]float32, 0, len(counts)),
	}
	for tok, c := range counts {
		out.Indices = append(out.Indices, termIndex(tok))
		out.Values = append(out.Values, float32(c/norm))
	}
	// 升序排序并同步交换 Values（Indices 与 Values 按下标配对）。
	// byIndex 持有的是切片头拷贝，底层数组共享，排序结果直接反映到 out。
	sort.Sort(byIndex{v: out})
	return out
}

// byIndex 同时交换 Indices/Values 的排序器。
type byIndex struct{ v vector.SparseVector }

func (b byIndex) Len() int           { return len(b.v.Indices) }
func (b byIndex) Less(i, j int) bool { return b.v.Indices[i] < b.v.Indices[j] }
func (b byIndex) Swap(i, j int) {
	b.v.Indices[i], b.v.Indices[j] = b.v.Indices[j], b.v.Indices[i]
	b.v.Values[i], b.v.Values[j] = b.v.Values[j], b.v.Values[i]
}

// termIndex 把词项映射为稳定的 uint32 槽位（FNV-1a）。
// 不做词表映射的理由：词表需要预先构建并两侧同步，而 hash 槽位
// 天然无状态且确定性，碰撞率在词项量级下可接受。
func termIndex(term string) uint32 {
	var h uint32 = 2166136261
	for i := 0; i < len(term); i++ {
		h ^= uint32(term[i])
		h *= 16777619
	}
	return h
}

// tokenize 分词：ASCII 字母数字段为词；CJK 段切二元组。
// 其他字符（标点/空白/emoji）为分隔符。
func tokenize(text string) []string {
	var tokens []string
	runes := []rune(text)
	start := -1 // 当前 ASCII 词的起始下标
	cjkStart := -1

	flushWord := func(end int) {
		if start >= 0 {
			tokens = append(tokens, string(runes[start:end]))
			start = -1
		}
	}
	flushCJK := func(end int) {
		if cjkStart < 0 {
			return
		}
		seg := runes[cjkStart:end]
		switch len(seg) {
		case 1:
			tokens = append(tokens, string(seg))
		default:
			for i := 0; i+1 < len(seg); i++ {
				tokens = append(tokens, string(seg[i:i+2]))
			}
		}
		cjkStart = -1
	}

	for i, r := range runes {
		switch {
		case unicode.IsLetter(r) && r < 128 || unicode.IsDigit(r) && r < 128:
			if cjkStart >= 0 {
				flushCJK(i)
			}
			if start < 0 {
				start = i
			}
		case isCJK(r):
			flushWord(i)
			if cjkStart < 0 {
				cjkStart = i
			}
		default:
			flushWord(i)
			flushCJK(i)
		}
	}
	flushWord(len(runes))
	flushCJK(len(runes))

	// 小写化在词级别做（二元组拼接后整体小写，与逐 rune 小写等价，
	// 但避免改变分词边界）。
	for i, t := range tokens {
		tokens[i] = toLowerASCII(t)
	}
	return tokens
}

// isCJK 判定 CJK 表意文字与假名。Hangul 不含：韩文搜素习惯依赖形态素
// 分析，二元组切分收益低，Phase 2 先按分隔符处理。
func isCJK(r rune) bool {
	return (r >= 0x4E00 && r <= 0x9FFF) || // CJK 统一表意文字
		(r >= 0x3040 && r <= 0x30FF) || // 平假名 + 片假名
		(r >= 0xF900 && r <= 0xFAFF) // CJK 兼容表意文字
}

// toLowerASCII 只处理 ASCII 字母：Unicode 小写化可能改变字符宽度
// （如 Ａ→ａ），引入两侧不一致的风险。
func toLowerASCII(s string) string {
	b := []byte(s)
	changed := false
	for i, c := range b {
		if c >= 'A' && c <= 'Z' {
			b[i] = c + ('a' - 'A')
			changed = true
		}
	}
	if !changed {
		return s
	}
	return string(b)
}
