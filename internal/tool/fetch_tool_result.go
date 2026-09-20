// fetch_tool_result 工具让 LLM 按需取回被遮蔽的工具原文。
//
// P0 的工具结果遮蔽把窗口外的 TOOL 节点输出替换为带 node_id 指针的占位符。
// 本工具让模型在需要细节时凭 node_id 主动拉取原文，实现"按需展开"而非全量预载。
//
// 安全：租户从 ExecutionContext（ctx）取，不从 LLM 输入取，
// 防止 prompt injection 跨租户读取其他租户的节点数据。
package tool

import (
	"agent-runtime/internal/contracts"
	"agent-runtime/internal/model"
	"context"
	"fmt"
	"strings"
)

// NodeOutputStore 是 fetch_tool_result 所需的只读存储能力。
// *store.MySQL 天然实现（GetNode 按 tenant + node_id 查询）。
type NodeOutputStore interface {
	GetNode(ctx context.Context, tenant, nodeID string) (*model.Node, error)
}

// FetchToolResult 让 LLM 按需取回被遮蔽的工具结果原文。
type FetchToolResult struct {
	Store NodeOutputStore
}

func (FetchToolResult) Name() string { return "fetch_tool_result" }

// Execute 接收 node_id 字符串，从存储中加载对应 TOOL 节点的完整输出。
// 租户从 ctx 中的 ExecutionContext 提取（worker.Handle 已注入），不从 input 取。
func (f FetchToolResult) Execute(ctx context.Context, input string) (string, error) {
	ec, ok := contracts.ExecutionContextFrom(ctx)
	if !ok || ec.TenantID == "" {
		return "", fmt.Errorf("fetch_tool_result: tenant not in context")
	}
	nodeID := strings.TrimSpace(input)
	if nodeID == "" {
		return "", fmt.Errorf("fetch_tool_result: empty node_id")
	}
	n, err := f.Store.GetNode(ctx, ec.TenantID, nodeID)
	if err != nil {
		return "", fmt.Errorf("fetch_tool_result: load node %s: %w", nodeID, err)
	}
	if n.Type != model.NodeTool {
		return "", fmt.Errorf("fetch_tool_result: node %s is not a TOOL node (type=%s)", nodeID, n.Type)
	}
	if n.Status != model.NodeSuccess {
		return "", fmt.Errorf("fetch_tool_result: node %s not SUCCESS (status=%s)", nodeID, n.Status)
	}
	return n.Output, nil
}
