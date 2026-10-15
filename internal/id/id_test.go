package id

import (
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
)

func TestNew_PrefixedUUIDv7(t *testing.T) {
	got := New("run")
	const prefix = "run-"
	if !strings.HasPrefix(got, prefix) {
		t.Fatalf("New(run) = %q, want prefix %q", got, prefix)
	}
	u, err := uuid.Parse(strings.TrimPrefix(got, prefix))
	if err != nil {
		t.Fatalf("parse uuid: %v", err)
	}
	if v := u.Version(); v != 7 {
		t.Errorf("uuid version = %d, want 7", v)
	}
	if len(got) > 64 {
		t.Errorf("id length %d exceeds VARCHAR(64)", len(got))
	}
}

func TestNew_EmptyPrefix(t *testing.T) {
	got := New("")
	if _, err := uuid.Parse(got); err != nil {
		t.Fatalf("New(\"\") = %q, not a uuid: %v", got, err)
	}
}

func TestNew_UniqueUnderConcurrency(t *testing.T) {
	const n = 10_000
	seen := make(map[string]struct{}, n)
	var mu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			id := New("event")
			mu.Lock()
			defer mu.Unlock()
			if _, dup := seen[id]; dup {
				t.Errorf("duplicate id %q", id)
				return
			}
			seen[id] = struct{}{}
		}()
	}
	wg.Wait()
	if len(seen) != n {
		t.Errorf("unique ids = %d, want %d", len(seen), n)
	}
}
