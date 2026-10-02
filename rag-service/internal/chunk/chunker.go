// Package chunk 把长文档切成 parent-child 两级块（Phase 3）。
//
// 为什么两级：child（小块）向量命中精度高——块越大，主题越杂，embedding 越"糊"；
// parent（大块）作为检索返回内容喂给 LLM——上下文完整，省去二次拼装。
// 这是 small-to-big 检索的标准形态。
//
// 纯函数、确定性：同一输入永远产出同一批块。这是幂等写入的前提——
// child 点 ID 由 (tenant, docID, seq) 派生，分块结果不稳定会导致
// 重放时同一文档产生不同 ID 的孤儿点。
//
// 切分层级：句子（保留结尾定界符）→ 贪心装块 → 超长单元硬切。
// v1 无 overlap：重叠能小幅提升边界召回，但让块数与 ID 序复杂化，
// 收益留给评测数据驱动后再决定。
package chunk

import (
	"strings"
	"unicode/utf8"
)

// 缺省块大小（rune 计）。child 400：中文技术文本约 1-3 句，
// 主题足够聚焦；parent 1200：约 3 个 child 的体量，够 LLM 还原语境。
const (
	DefaultParentSize = 1200
	DefaultChildSize  = 400
)

// Config 是分块参数。零值字段回退默认值。
type Config struct {
	ParentSize int // 父块上限（rune）
	ChildSize  int // 子块上限（rune）
}

// Chunk 是一个子块及其所属父块。ParentText 随子块冗余存储在向量 payload
// （parent_text），检索命中 child 后直接返回 parent，免去二次取回。
type Chunk struct {
	ParentIndex int    // 所属父块编号（0 起，文档内连续）
	Text        string // 子块文本
	ParentText  string // 所属父块全文
}

// ChunkDocument 把文档切成子块序列。content 全空白时返回 nil
// （由调用方决定如何拒绝这种文档）。
func ChunkDocument(content string, cfg Config) []Chunk {
	cfg = cfg.WithDefaults()
	parents := pack(splitUnits(content), cfg.ParentSize)
	var out []Chunk
	for i, p := range parents {
		for _, c := range pack(splitUnits(p), cfg.ChildSize) {
			out = append(out, Chunk{ParentIndex: i, Text: c, ParentText: p})
		}
	}
	return out
}

// WithDefaults 返回零值回填与钳制后的配置。它同时是 ingest 侧
// docsCtxVersion 的派生依据：版本串参数必须与分块实际生效的参数一致，
// 否则改配置不会强制重嵌，存量子块与新版子块将混用不同切分。
func (c Config) WithDefaults() Config {
	if c.ParentSize <= 0 {
		c.ParentSize = DefaultParentSize
	}
	if c.ChildSize <= 0 {
		c.ChildSize = DefaultChildSize
	}
	if c.ChildSize > c.ParentSize {
		c.ChildSize = c.ParentSize
	}
	return c
}

// splitUnits 按句子边界切单元，结尾定界符保留在单元尾部
// （中文句读 + 英文标点 + 换行）。纯空白单元丢弃。
func splitUnits(s string) []string {
	runes := []rune(s)
	var units []string
	start := 0
	for i, r := range runes {
		switch r {
		case '。', '！', '？', '；', '!', '?', ';', '.', '\n':
			if u := string(runes[start : i+1]); strings.TrimSpace(u) != "" {
				units = append(units, u)
			}
			start = i + 1
		}
	}
	if start < len(runes) {
		if u := string(runes[start:]); strings.TrimSpace(u) != "" {
			units = append(units, u)
		}
	}
	return units
}

// pack 贪心把单元装成不超过 max 的块。单元间直接拼接
// （定界符已保留在尾部，原文本基本无损）；超 max 的单元先硬切。
func pack(units []string, max int) []string {
	var blocks []string
	var cur []rune
	flush := func() {
		if len(cur) > 0 {
			blocks = append(blocks, string(cur))
			cur = nil
		}
	}
	for _, u := range units {
		ur := []rune(u)
		if len(ur) > max {
			// 超长单元独立成块序列：先冲销在途块，保证块边界对齐。
			flush()
			blocks = append(blocks, hardWrap(u, max)...)
			continue
		}
		if len(cur)+len(ur) > max {
			flush()
		}
		cur = append(cur, ur...)
	}
	flush()
	return blocks
}

// hardWrap 把单个超长单元按 max 等宽硬切（无标点的极端输入兜底）。
func hardWrap(s string, max int) []string {
	runes := []rune(s)
	var out []string
	for start := 0; start < len(runes); start += max {
		end := start + max
		if end > len(runes) {
			end = len(runes)
		}
		out = append(out, string(runes[start:end]))
	}
	return out
}

// runeLen 供测试断言使用（暴露 rune 计数语义）。
func runeLen(s string) int { return utf8.RuneCountInString(s) }
