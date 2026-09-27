package policy

import (
	"strings"
	"testing"
)

func TestMatchTokens(t *testing.T) {
	cases := []struct {
		name    string
		pattern string
		tokens  string
		full    bool
		want    bool
	}{
		// allow 语义（full=true）：全消费。
		{"allow exact", "git status", "git status", true, true},
		{"allow extra args rejected", "git status", "git status --short", true, false},
		{"allow trailing star absorbs", "git status *", "git status --short", true, true},
		{"allow trailing star zero args", "npm *", "npm", true, true},
		{"allow star mid pattern one token", "a * b", "a x b", true, true},
		{"allow mismatch", "npm *", "pip install", true, false},
		{"allow prefix token", "dd if=*", "dd if=/dev/zero", true, true},
		{"allow case insensitive", "GIT STATUS", "git status", true, true},
		// deny/ask 语义（full=false）：前缀命中即可。
		{"deny prefix extra args ok", "mkfs", "mkfs /dev/sda", false, true},
		{"deny exact", "rm -rf /", "rm -rf /", false, true},
		{"deny positional mismatch", "rm -rf /", "echo rm -rf /", false, false},
		{"deny different flag order", "rm -rf /", "rm -fr /", false, false},
		{"deny needs start alignment", "rm -rf /", "cd /tmp rm -rf /", false, false},
		{"deny prefix token mid", "dd if=*", "dd if=/dev/zero of=/dev/sda", false, true},
		{"empty input", "ls", "", false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := matchTokens(strings.Fields(tc.pattern), strings.Fields(tc.tokens), tc.full)
			if got != tc.want {
				t.Errorf("matchTokens(%q, %q, full=%v) = %v, want %v",
					tc.pattern, tc.tokens, tc.full, got, tc.want)
			}
		})
	}
}

func TestRuleSetAggregate(t *testing.T) {
	rs := NewRuleSet(append(BuiltinRules(),
		Rule{Tool: "shell", Pattern: "git *", Effect: RequireApproval, Source: "test"},
		Rule{Tool: "shell", Pattern: "git status", Effect: Allow, Source: "test"}, // 细化：具体 allow 压过泛化 ask
		Rule{Tool: "shell", Pattern: "npm *", Effect: Allow, Source: "test"},
		Rule{Tool: "shell", Pattern: "npm publish", Effect: RequireApproval, Source: "test"},
	))
	toks := func(s string) []string { return strings.Fields(s) }

	t.Run("deny anywhere wins", func(t *testing.T) {
		d, _, ok := rs.Aggregate("shell", [][]string{toks("ls")}, [][]string{toks("rm -rf /")}, true)
		if !ok || d != Deny {
			t.Fatalf("inner deny must dominate, got %v ok=%v", d, ok)
		}
	})

	t.Run("specific allow refines generic ask", func(t *testing.T) {
		d, r, ok := rs.Aggregate("shell", [][]string{toks("git status")}, nil, true)
		if !ok || d != Allow {
			t.Fatalf("git status must be allowed by refinement, got %v ok=%v", d, ok)
		}
		if r.Pattern != "git status" {
			t.Errorf("representative rule must be the specific allow, got %q", r.Pattern)
		}
	})

	t.Run("generic ask catches uncovered", func(t *testing.T) {
		d, _, ok := rs.Aggregate("shell", [][]string{toks("git push")}, nil, true)
		if !ok || d != RequireApproval {
			t.Fatalf("git push must ask, got %v ok=%v", d, ok)
		}
	})

	t.Run("specific ask beats generic allow", func(t *testing.T) {
		d, _, ok := rs.Aggregate("shell", [][]string{toks("npm publish")}, nil, true)
		if !ok || d != RequireApproval {
			t.Fatalf("npm publish must ask, got %v ok=%v", d, ok)
		}
	})

	t.Run("one unrecognized segment blocks allow", func(t *testing.T) {
		d, _, ok := rs.Aggregate("shell", [][]string{toks("ls"), toks("curl example.com")}, nil, true)
		if ok {
			t.Fatalf("mixed segments must not allow, got %v", d)
		}
	})

	t.Run("all segments allow", func(t *testing.T) {
		d, _, ok := rs.Aggregate("shell", [][]string{toks("ls"), toks("npm install")}, nil, true)
		if !ok || d != Allow {
			t.Fatalf("all-allow segments must allow, got %v ok=%v", d, ok)
		}
	})

	t.Run("allowEligible=false suppresses allow", func(t *testing.T) {
		d, _, ok := rs.Aggregate("shell", [][]string{toks("ls")}, nil, false)
		if ok {
			t.Fatalf("allow must be suppressed, got %v", d)
		}
	})

	t.Run("same pattern most severe wins", func(t *testing.T) {
		rs2 := NewRuleSet([]Rule{
			{Tool: "*", Pattern: "ls", Effect: Allow, Source: "builtin"},
			{Tool: "*", Pattern: "ls", Effect: Deny, Source: "tenant"},
		})
		d, r, ok := rs2.Aggregate("shell", [][]string{toks("ls")}, nil, true)
		if !ok || d != Deny || r.Source != "tenant" {
			t.Fatalf("same-pattern tie must pick most severe, got %v %+v ok=%v", d, r, ok)
		}
	})
}

func TestParseRulesEnv(t *testing.T) {
	rules, err := ParseRulesEnv("shell:npm *:allow, http_fetch:https://api.internal/*:allow, *:rm -rf /:deny, shell:npm publish:ask")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(rules) != 4 {
		t.Fatalf("want 4 rules, got %d", len(rules))
	}
	if rules[3].Effect != RequireApproval {
		t.Errorf("ask alias must map to require_approval, got %v", rules[3].Effect)
	}
	for _, bad := range []string{
		"shell:npm",         // 缺 effect
		"shell::allow",      // 空 pattern
		"shell:npm x:maybe", // 未知 effect
	} {
		if _, err := ParseRulesEnv(bad); err == nil {
			t.Errorf("invalid rule %q must fail fast", bad)
		}
	}
	if rules, _ := ParseRulesEnv(""); len(rules) != 0 {
		t.Errorf("empty env must yield no rules")
	}
}
