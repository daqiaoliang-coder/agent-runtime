package llmadapter

import (
	"agent-runtime/internal/contracts"
	"agent-runtime/internal/llm"
	"context"
	"testing"
)

func TestProvider_StreamCompatibility(t *testing.T) {
	p := New(&llm.Stub{Responder: func(llm.Request) string { return "hello" }})
	ch, err := p.Stream(context.Background(), contracts.GenerateRequest{Messages: []contracts.Message{{Role: contracts.RoleUser, Content: "hi"}}})
	if err != nil {
		t.Fatal(err)
	}
	var got string
	count := 0
	for ev := range ch {
		count++
		if ev.Type == contracts.ModelEventTextDelta {
			got += ev.Delta
		}
	}
	if got != "hello" || count != 2 {
		t.Fatalf("got=%q events=%d", got, count)
	}
}
