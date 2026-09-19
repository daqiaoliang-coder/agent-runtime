-- 会话级记忆隔离：为 Run 增加 thread_id，作为跨 Run 长期记忆的召回维度。
-- ThreadID 是 Run 级属性，Node 通过 Run 继承，因此不动 agent_node（最热的表）。
-- DEFAULT '' 保证存量 Run 与现有测试不受影响；
-- 空串语义 = 无会话隔离，检索时跳过 thread_id 过滤（见 providers/memory_vector.go）。
USE agent_runtime;

ALTER TABLE agent_run ADD COLUMN thread_id VARCHAR(64) NOT NULL DEFAULT '' AFTER tenant_id;

-- 记忆索引器按 (tenant_id, thread_id) 定位会话历史，需要联合索引支撑。
CREATE INDEX idx_run_thread ON agent_run (tenant_id, thread_id);
