package worker

import (
	"testing"

	"agent-runtime/internal/providers"
)

// MEMORY_BACKEND 装配矩阵：验证 local/remote/shadow 三种后端与降级组合。
// NewQdrant 是惰性连接，本地侧装配无需真实向量库。

// TestNewMemoryOptionsFromEnv_BackendLocalDefault 缺省后端必须是直连 VectorMemory，
// 与引入 MEMORY_BACKEND 之前的行为完全一致（现有部署零感知）。
func TestNewMemoryOptionsFromEnv_BackendLocalDefault(t *testing.T) {
	t.Setenv("MEMORY_ENABLED", "true")
	t.Setenv("OPENAI_API_KEY", "sk-test")

	opt := newMemoryOptionsFromEnv(newCredentialsFromEnv())
	if _, ok := opt.Memory.(*providers.VectorMemory); !ok {
		t.Fatalf("default backend must assemble VectorMemory, got %T", opt.Memory)
	}
}

// TestNewMemoryOptionsFromEnv_BackendInvalid 未知后端值回退 local（fail-safe）。
func TestNewMemoryOptionsFromEnv_BackendInvalid(t *testing.T) {
	t.Setenv("MEMORY_ENABLED", "true")
	t.Setenv("OPENAI_API_KEY", "sk-test")
	t.Setenv("MEMORY_BACKEND", "bogus")

	opt := newMemoryOptionsFromEnv(newCredentialsFromEnv())
	if _, ok := opt.Memory.(*providers.VectorMemory); !ok {
		t.Fatalf("invalid backend must fall back to VectorMemory, got %T", opt.Memory)
	}
}

// TestNewMemoryOptionsFromEnv_BackendRemoteNoFallback remote 后端关闭回退时：
// 装配纯 RemoteMemory，且**不需要任何 embedding 凭证**（已外移到 rag-api）。
func TestNewMemoryOptionsFromEnv_BackendRemoteNoFallback(t *testing.T) {
	t.Setenv("MEMORY_ENABLED", "true")
	t.Setenv("MEMORY_BACKEND", "remote")
	t.Setenv("MEMORY_REMOTE_FALLBACK", "false")
	t.Setenv("RAG_API_URL", "http://127.0.0.1:9")
	// 刻意不设 OPENAI_API_KEY：证明 remote-only 部署摆脱了 embedding 依赖。

	opt := newMemoryOptionsFromEnv(newCredentialsFromEnv())
	rm, ok := opt.Memory.(*providers.RemoteMemory)
	if !ok {
		t.Fatalf("remote backend without fallback must assemble RemoteMemory, got %T", opt.Memory)
	}
	if rm.BaseURL != "http://127.0.0.1:9" {
		t.Errorf("BaseURL mismatch: %q", rm.BaseURL)
	}
}

// TestNewMemoryOptionsFromEnv_BackendRemoteWithFallback remote 后端开启回退
// （缺省）且本地依赖可用时：装配 FallbackMemory（主远程、备直连）。
func TestNewMemoryOptionsFromEnv_BackendRemoteWithFallback(t *testing.T) {
	t.Setenv("MEMORY_ENABLED", "true")
	t.Setenv("MEMORY_BACKEND", "remote")
	t.Setenv("RAG_API_URL", "http://127.0.0.1:9")
	t.Setenv("OPENAI_API_KEY", "sk-test")

	opt := newMemoryOptionsFromEnv(newCredentialsFromEnv())
	fm, ok := opt.Memory.(*providers.FallbackMemory)
	if !ok {
		t.Fatalf("remote backend with fallback must assemble FallbackMemory, got %T", opt.Memory)
	}
	if _, ok := fm.Primary.(*providers.RemoteMemory); !ok {
		t.Errorf("FallbackMemory.Primary must be RemoteMemory, got %T", fm.Primary)
	}
	if _, ok := fm.Fallback.(*providers.VectorMemory); !ok {
		t.Errorf("FallbackMemory.Fallback must be VectorMemory, got %T", fm.Fallback)
	}
}

// TestNewMemoryOptionsFromEnv_BackendRemoteNoURL remote 后端但缺 RAG_API_URL：
// 降级为本地行为（缺啥降啥，不让 worker 起不来）。
func TestNewMemoryOptionsFromEnv_BackendRemoteNoURL(t *testing.T) {
	t.Setenv("MEMORY_ENABLED", "true")
	t.Setenv("MEMORY_BACKEND", "remote")
	t.Setenv("OPENAI_API_KEY", "sk-test")
	// 刻意不设 RAG_API_URL。

	opt := newMemoryOptionsFromEnv(newCredentialsFromEnv())
	if _, ok := opt.Memory.(*providers.VectorMemory); !ok {
		t.Fatalf("remote backend without RAG_API_URL must degrade to local, got %T", opt.Memory)
	}
}

// TestNewMemoryOptionsFromEnv_BackendRemoteNoLocalCred remote 后端 + 回退缺省开启，
// 但本地依赖不可用（无 embedding 凭证）：装配纯 RemoteMemory，不因此关闭记忆。
func TestNewMemoryOptionsFromEnv_BackendRemoteNoLocalCred(t *testing.T) {
	t.Setenv("MEMORY_ENABLED", "true")
	t.Setenv("MEMORY_BACKEND", "remote")
	t.Setenv("RAG_API_URL", "http://127.0.0.1:9")
	// 刻意不设 OPENAI_API_KEY：本地侧降级，远程独立可用。

	opt := newMemoryOptionsFromEnv(newCredentialsFromEnv())
	if _, ok := opt.Memory.(*providers.RemoteMemory); !ok {
		t.Fatalf("remote must survive unavailable local side, got %T", opt.Memory)
	}
}

// TestNewMemoryOptionsFromEnv_BackendShadow shadow 后端：装配 ShadowMemory，
// Primary 为直连 VectorMemory、Secondary 为 RemoteMemory（Phase 1 影子对比）。
func TestNewMemoryOptionsFromEnv_BackendShadow(t *testing.T) {
	t.Setenv("MEMORY_ENABLED", "true")
	t.Setenv("MEMORY_BACKEND", "shadow")
	t.Setenv("RAG_API_URL", "http://127.0.0.1:9")
	t.Setenv("OPENAI_API_KEY", "sk-test")

	opt := newMemoryOptionsFromEnv(newCredentialsFromEnv())
	sm, ok := opt.Memory.(*providers.ShadowMemory)
	if !ok {
		t.Fatalf("shadow backend must assemble ShadowMemory, got %T", opt.Memory)
	}
	if _, ok := sm.Primary.(*providers.VectorMemory); !ok {
		t.Errorf("ShadowMemory.Primary must be VectorMemory, got %T", sm.Primary)
	}
	if _, ok := sm.Secondary.(*providers.RemoteMemory); !ok {
		t.Errorf("ShadowMemory.Secondary must be RemoteMemory, got %T", sm.Secondary)
	}
}

// TestNewMemoryOptionsFromEnv_BackendShadowNoRemoteURL shadow 后端缺 RAG_API_URL：
// 仍装配 ShadowMemory（Secondary 为 nil，Search 内部退化为纯本地），行为不劣化。
func TestNewMemoryOptionsFromEnv_BackendShadowNoRemoteURL(t *testing.T) {
	t.Setenv("MEMORY_ENABLED", "true")
	t.Setenv("MEMORY_BACKEND", "shadow")
	t.Setenv("OPENAI_API_KEY", "sk-test")

	opt := newMemoryOptionsFromEnv(newCredentialsFromEnv())
	sm, ok := opt.Memory.(*providers.ShadowMemory)
	if !ok {
		t.Fatalf("shadow backend without RAG_API_URL must keep ShadowMemory, got %T", opt.Memory)
	}
	if sm.Secondary != nil {
		t.Errorf("Secondary must be nil without RAG_API_URL, got %T", sm.Secondary)
	}
	if _, ok := sm.Primary.(*providers.VectorMemory); !ok {
		t.Errorf("ShadowMemory.Primary must be VectorMemory, got %T", sm.Primary)
	}
}
