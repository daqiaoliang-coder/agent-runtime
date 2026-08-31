package main

import (
	"context"
	"time"
)

// LLM 抽象出大模型调用，Generate 应尊重传入 ctx 的超时与取消。
type LLM interface {
	Generate(context.Context, string) (string, error)
}

type MockLLM struct{}

func (m *MockLLM) Generate(ctx context.Context, prompt string) (string, error) {
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case <-time.After(300 * time.Millisecond):
		return "LLM response: " + prompt, nil
	}
}
