package sparse

import (
	"math"
	"testing"
)

func TestEncode_Deterministic(t *testing.T) {
	enc := NewTF()
	a := enc.Encode("部署数据库迁移")
	b := enc.Encode("部署数据库迁移")
	if len(a.Indices) != len(b.Indices) {
		t.Fatalf("same text must encode to same length: %d vs %d", len(a.Indices), len(b.Indices))
	}
	for i := range a.Indices {
		if a.Indices[i] != b.Indices[i] || a.Values[i] != b.Values[i] {
			t.Fatalf("encoding not deterministic at %d", i)
		}
	}
}

func TestEncode_CJKBigrams(t *testing.T) {
	toks := tokenize("数据库")
	want := []string{"数据", "据库"}
	if len(toks) != len(want) {
		t.Fatalf("want bigrams %v, got %v", want, toks)
	}
	for i, w := range want {
		if toks[i] != w {
			t.Fatalf("token[%d]: want %q, got %q", i, w, toks[i])
		}
	}
}

func TestEncode_SingleCJKChar(t *testing.T) {
	toks := tokenize("库")
	if len(toks) != 1 || toks[0] != "库" {
		t.Fatalf("single CJK char should be its own token, got %v", toks)
	}
}

func TestEncode_ASCIITokensAndLowercase(t *testing.T) {
	toks := tokenize("Deploy PostgreSQL 16!")
	want := []string{"deploy", "postgresql", "16"}
	if len(toks) != len(want) {
		t.Fatalf("want %v, got %v", want, toks)
	}
	for i, w := range want {
		if toks[i] != w {
			t.Fatalf("token[%d]: want %q, got %q", i, w, toks[i])
		}
	}
}

func TestEncode_MixedText(t *testing.T) {
	// 中英混排：各段独立分词，边界处互不粘连。
	toks := tokenize("RAG服务rag")
	want := []string{"rag", "服务", "rag"}
	if len(toks) != len(want) {
		t.Fatalf("want %v, got %v", want, toks)
	}
	for i, w := range want {
		if toks[i] != w {
			t.Fatalf("token[%d]: want %q, got %q", i, w, toks[i])
		}
	}
}

func TestEncode_L2NormalizedAndSorted(t *testing.T) {
	sv := NewTF().Encode("a b a c a")
	var sumSq float64
	for _, v := range sv.Values {
		sumSq += float64(v) * float64(v)
	}
	if math.Abs(sumSq-1) > 1e-5 {
		t.Fatalf("values must be L2-normalized, got norm²=%f", sumSq)
	}
	if len(sv.Indices) != 3 {
		t.Fatalf("want 3 distinct terms, got %d", len(sv.Indices))
	}
	for i := 1; i < len(sv.Indices); i++ {
		if sv.Indices[i] <= sv.Indices[i-1] {
			t.Fatalf("indices must be strictly ascending: %v", sv.Indices)
		}
	}
	// "a" 出现 3 次，权重应最大。
	if sv.Values[0] <= sv.Values[1] || sv.Values[0] <= sv.Values[2] {
		t.Fatalf("most frequent term must have the largest weight: %v", sv.Values)
	}
}

func TestEncode_EmptyText(t *testing.T) {
	sv := NewTF().Encode("  !? \n")
	if len(sv.Indices) != 0 || len(sv.Values) != 0 {
		t.Fatalf("empty/punct-only text must encode to empty vector, got %+v", sv)
	}
}

func TestEncode_SameTermSameIndex(t *testing.T) {
	// 写入侧与查询侧一致性：同词项必须命中同槽位。
	if termIndex("部署") != termIndex("部署") {
		t.Fatal("termIndex must be stable")
	}
	if termIndex("部署") == termIndex("数据") {
		t.Fatal("distinct terms must not collide on this tiny sample")
	}
}

// 编译期断言接口实现。
var _ Encoder = TF{}
