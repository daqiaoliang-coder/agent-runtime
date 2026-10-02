package eval

import (
	"math"
	"testing"
)

func almostEqual(a, b float64) bool { return math.Abs(a-b) < 1e-9 }

func TestRecallAtK(t *testing.T) {
	got := []string{"a", "b", "c", "d"}
	cases := []struct {
		name     string
		got      []string
		expected []string
		k        int
		want     float64
	}{
		{"all hit", []string{"a", "b"}, []string{"a", "b"}, 2, 1},
		{"partial hit", got, []string{"b", "d", "x"}, 4, 2.0 / 3.0},
		{"hit beyond k", got, []string{"d"}, 2, 0},
		{"hit within k", got, []string{"d"}, 4, 1},
		{"dup result counts once", []string{"a", "a", "b"}, []string{"a"}, 3, 1},
		{"empty expected", got, nil, 4, 0},
		{"zero k", got, []string{"a"}, 0, 0},
		{"k beyond len", []string{"a"}, []string{"a"}, 10, 1},
	}
	for _, c := range cases {
		if got := RecallAtK(c.got, c.expected, c.k); !almostEqual(got, c.want) {
			t.Errorf("%s: RecallAtK(%v,%v,%d)=%v, want %v", c.name, c.got, c.expected, c.k, got, c.want)
		}
	}
}

func TestMRRAtK(t *testing.T) {
	cases := []struct {
		name     string
		got      []string
		expected []string
		k        int
		want     float64
	}{
		{"first hit", []string{"a", "b"}, []string{"a"}, 2, 1},
		{"second hit", []string{"x", "a"}, []string{"a"}, 2, 0.5},
		{"third hit", []string{"x", "y", "a"}, []string{"a"}, 3, 1.0 / 3.0},
		{"no hit", []string{"x", "y"}, []string{"a"}, 2, 0},
		{"hit beyond k", []string{"x", "a"}, []string{"a"}, 1, 0},
		{"multi expected takes best rank", []string{"x", "b", "a"}, []string{"a", "b"}, 3, 0.5},
		{"empty expected", []string{"a"}, nil, 2, 0},
	}
	for _, c := range cases {
		if got := MRRAtK(c.got, c.expected, c.k); !almostEqual(got, c.want) {
			t.Errorf("%s: MRRAtK(%v,%v,%d)=%v, want %v", c.name, c.got, c.expected, c.k, got, c.want)
		}
	}
}

func TestNDCGAtK(t *testing.T) {
	// 手算基准：got=[x,a,b], expected=[a,b], k=3。
	// DCG = 1/log2(3) + 1/log2(4)；IDCG = 1/log2(2) + 1/log2(3)。
	want := (1/math.Log2(3) + 1/math.Log2(4)) / (1/math.Log2(2) + 1/math.Log2(3))
	if got := NDCGAtK([]string{"x", "a", "b"}, []string{"a", "b"}, 3); !almostEqual(got, want) {
		t.Errorf("NDCGAtK basic=%v, want %v", got, want)
	}
	cases := []struct {
		name     string
		got      []string
		expected []string
		k        int
		want     float64
	}{
		{"perfect order", []string{"a", "b", "c"}, []string{"a", "b", "c"}, 3, 1},
		{"no hit", []string{"x", "y"}, []string{"a"}, 2, 0},
		{"hit beyond k", []string{"x", "a"}, []string{"a"}, 1, 0},
		{"empty expected", []string{"a"}, nil, 2, 0},
		{"expected larger than k", []string{"a", "b"}, []string{"a", "b", "c", "d"}, 2, 1},
	}
	for _, c := range cases {
		if got := NDCGAtK(c.got, c.expected, c.k); !almostEqual(got, c.want) {
			t.Errorf("%s: NDCGAtK(%v,%v,%d)=%v, want %v", c.name, c.got, c.expected, c.k, got, c.want)
		}
	}
}

func TestPercentile(t *testing.T) {
	// 1..10：p50=5、p95=10（最近邻）。
	vals := []float64{10, 9, 8, 7, 6, 5, 4, 3, 2, 1}
	if got := Percentile(vals, 0.5); got != 5 {
		t.Errorf("p50 of 1..10 = %v, want 5", got)
	}
	if got := Percentile(vals, 0.95); got != 10 {
		t.Errorf("p95 of 1..10 = %v, want 10", got)
	}
	if got := Percentile(vals, 1.0); got != 10 {
		t.Errorf("p100 of 1..10 = %v, want 10", got)
	}
	// 单元素。
	if got := Percentile([]float64{42}, 0.5); got != 42 {
		t.Errorf("p50 of single = %v, want 42", got)
	}
	// 空输入与非法 p。
	if got := Percentile(nil, 0.5); got != 0 {
		t.Errorf("p50 of empty = %v, want 0", got)
	}
	if got := Percentile(vals, 0); got != 0 {
		t.Errorf("p0 = %v, want 0", got)
	}
	// 原切片不得被修改（输入序保留）。
	if vals[0] != 10 {
		t.Errorf("input slice must not be mutated, got %v", vals)
	}
}

func TestMean(t *testing.T) {
	if got := Mean([]float64{1, 2, 3, 4}); got != 2.5 {
		t.Errorf("Mean(1..4)=%v, want 2.5", got)
	}
	if got := Mean(nil); got != 0 {
		t.Errorf("Mean(nil)=%v, want 0", got)
	}
}
