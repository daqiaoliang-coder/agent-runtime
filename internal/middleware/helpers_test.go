package middleware

import (
	"agent-runtime/internal/contracts"
	"time"
)

// 本文件集中定义测试用的构造器，避免各测试文件重复拼装契约结构体。
//
// 刻意用函数而不是字面量：契约结构体字段较多（ToolResult 有 CallID/Output/IsError，
// RuntimeEvent 有七个字段），逐个测试写全字段会让断言淹没在样板里，
// 且新增字段时要改几十处。构造器把"哪些字段与本测试相关"显式表达出来。

// newEC 构造带完整身份维度的执行上下文。
func newEC(tenant, user, run string) contracts.ExecutionContext {
	return contracts.ExecutionContext{
		TenantID: tenant,
		UserID:   user,
		ThreadID: "thread-1",
		RunID:    run,
		TraceID:  "trace-1",
	}
}

// toolRequest 构造工具调用请求。
func toolRequest(callID, name, args string) contracts.ToolCallRequest {
	return contracts.ToolCallRequest{CallID: callID, Name: name, Arguments: args}
}

// toolResult 构造工具调用结果。
func toolResult(callID, output string, isError bool) contracts.ToolResult {
	return contracts.ToolResult{CallID: callID, Output: output, IsError: isError}
}

// runtimeEvent 构造带负载的运行时事件。
// 时间与 ID 由构造器填充，测试只关心 RunID/NodeID/Data 三个字段。
func runtimeEvent(runID, nodeID string, data any) contracts.RuntimeEvent {
	return contracts.RuntimeEvent{
		ID:        "ev-" + nodeID,
		RunID:     runID,
		NodeID:    nodeID,
		TenantID:  "tenant-1",
		Type:      contracts.EventNodeFinished,
		Timestamp: time.Now(),
		Data:      data,
	}
}
