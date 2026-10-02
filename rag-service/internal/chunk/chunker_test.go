package chunk

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func TestChunkDocument_Deterministic(t *testing.T) {
	doc := strings.Repeat("这是第一句。这是第二句！这是第三句？", 30)
	a := ChunkDocument(doc, Config{})
	b := ChunkDocument(doc, Config{})
	if len(a) == 0 {
		t.Fatal("expected non-empty chunks")
	}
	if len(a) != len(b) {
		t.Fatalf("non-deterministic: %d vs %d chunks", len(a), len(b))
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("chunk %d differs", i)
		}
	}
}

func TestChunkDocument_RespectsSizes(t *testing.T) {
	cfg := Config{ParentSize: 100, ChildSize: 30}
	// 无标点长文本 + 有标点文本混合，覆盖硬切与贪心两条路径。
	doc := strings.Repeat("无标点超长文本必须走硬切路径且每块都不超过上限", 20) +
		"接下来是正常句子。第二句来了！第三句？最后一句。"
	chunks := ChunkDocument(doc, cfg)
	if len(chunks) == 0 {
		t.Fatal("expected chunks")
	}
	parents := map[int]string{}
	for _, c := range chunks {
		if got := utf8.RuneCountInString(c.Text); got > cfg.ChildSize {
			t.Fatalf("child %d exceeds ChildSize: %d > %d", c.ParentIndex, got, cfg.ChildSize)
		}
		if prev, ok := parents[c.ParentIndex]; ok && prev != c.ParentText {
			t.Fatalf("parent %d has inconsistent text", c.ParentIndex)
		}
		parents[c.ParentIndex] = c.ParentText
	}
	for i, p := range parents {
		if got := utf8.RuneCountInString(p); got > cfg.ParentSize {
			t.Fatalf("parent %d exceeds ParentSize: %d > %d", i, got, cfg.ParentSize)
		}
	}
	// 父块编号必须从 0 起连续。
	for i := range parents {
		if _, ok := parents[i-1]; i > 0 && !ok {
			t.Fatalf("parent index %d not contiguous", i)
		}
	}
}

func TestChunkDocument_ShortDocSingleChunk(t *testing.T) {
	doc := "只有一句话的短文档。"
	chunks := ChunkDocument(doc, Config{})
	if len(chunks) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(chunks))
	}
	if chunks[0].Text != doc || chunks[0].ParentText != doc || chunks[0].ParentIndex != 0 {
		t.Fatalf("unexpected chunk: %+v", chunks[0])
	}
}

func TestChunkDocument_MultipleParentsAndChildren(t *testing.T) {
	// 3 个段落 × 每段 10 句，每句 10 rune → 必然多 parent 多 child。
	var doc strings.Builder
	for p := 0; p < 3; p++ {
		if p > 0 {
			doc.WriteString("\n\n")
		}
		for s := 0; s < 10; s++ {
			doc.WriteString("段落主题句子内容。")
		}
	}
	cfg := Config{ParentSize: 120, ChildSize: 30}
	chunks := ChunkDocument(doc.String(), cfg)
	maxParent := 0
	for _, c := range chunks {
		if c.ParentIndex > maxParent {
			maxParent = c.ParentIndex
		}
	}
	if maxParent < 2 {
		t.Fatalf("expected >=3 parents, got %d", maxParent+1)
	}
	if len(chunks) <= 3 {
		t.Fatalf("expected multiple children, got %d", len(chunks))
	}
}

func TestChunkDocument_BlankContent(t *testing.T) {
	for _, in := range []string{"", "   ", "\n\n\n"} {
		if got := ChunkDocument(in, Config{}); len(got) > 0 {
			t.Fatalf("expected no chunks for %q, got %d", in, len(got))
		}
	}
	// 纯标点不是空白：会产出块（无害，嵌入后匹配不到任何查询）。
	if got := ChunkDocument("。！？", Config{}); len(got) != 1 {
		t.Fatalf("expected 1 chunk for punctuation-only, got %d", len(got))
	}
}

func TestChunkDocument_CJKAndASCII(t *testing.T) {
	doc := "English sentence one. 中英混合 sentence 两个。Another line\n纯中文第二行！"
	chunks := ChunkDocument(doc, Config{ParentSize: 80, ChildSize: 20})
	if len(chunks) == 0 {
		t.Fatal("expected chunks for mixed-language doc")
	}
	for _, c := range chunks {
		if runeLen(c.Text) > 20 {
			t.Fatalf("child exceeds limit: %d", runeLen(c.Text))
		}
	}
}

func TestConfig_Defaults(t *testing.T) {
	c := Config{}.WithDefaults()
	if c.ParentSize != DefaultParentSize || c.ChildSize != DefaultChildSize {
		t.Fatalf("unexpected defaults: %+v", c)
	}
	// ChildSize 不允许超过 ParentSize。
	c2 := Config{ParentSize: 100, ChildSize: 500}.WithDefaults()
	if c2.ChildSize != 100 {
		t.Fatalf("expected ChildSize clamped to 100, got %d", c2.ChildSize)
	}
}
