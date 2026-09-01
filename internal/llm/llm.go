// Package llm 抽象大语言模型调用，屏蔽不同 provider（OpenAI / 自建网关 / 本地模型）差异。
// 核心是 Client 接口：节点执行器与动态规划器均依赖它，便于注入 Stub 做测试与本地演示。
package llm

import "context"

// Role 表示对话消息角色。
type Role string

const (
	RoleSystem    Role = "system"
	RoleUser      Role = "user"
	RoleAssistant Role = "assistant"
)

// Message 是一条对话消息。
type Message struct {
	Role    Role
	Content string
}

// Request 是一次补全请求。Model 为空时由实现决定默认模型。
type Request struct {
	Model    string
	Messages []Message
}

// Usage 描述单次 LLM 调用的 token 消耗，用于成本追踪与配额核算。
type Usage struct {
	PromptTokens     int // 输入 token 数
	CompletionTokens int // 输出 token 数
	TotalTokens      int // 合计（部分 provider 会直接返回）
}

// Response 是补全结果。除文本内容外，还携带实际使用的模型名与 token 用量，
// 供执行器落库做 token/cost tracking。
type Response struct {
	Content string
	Model   string // 实际生成使用的模型（可能与请求不同，如 provider 降级）
	Usage   Usage
}

// Client 抽象 LLM 调用。实现需尊重 ctx 的超时与取消。
type Client interface {
	Complete(ctx context.Context, req Request) (Response, error)
}

// Stub 是确定性 LLM，用于本地演示与单元测试。
// 通过 Responder 可定制返回内容（例如动态规划器让 Stub 返回计划 JSON）。
type Stub struct {
	Responder func(req Request) string
}

// Complete 立即返回 Responder 的结果，尊重 ctx 取消。
func (s *Stub) Complete(ctx context.Context, req Request) (Response, error) {
	select {
	case <-ctx.Done():
		return Response{}, ctx.Err()
	default:
	}
	if s.Responder != nil {
		return Response{Content: s.Responder(req)}, nil
	}
	return Response{Content: "stub response"}, nil
}

// Echo 返回最后一条 user 消息的 Stub，便于本地调试。
func Echo() *Stub {
	return &Stub{Responder: func(req Request) string {
		for i := len(req.Messages) - 1; i >= 0; i-- {
			if req.Messages[i].Role == RoleUser {
				return req.Messages[i].Content
			}
		}
		return ""
	}}
}
