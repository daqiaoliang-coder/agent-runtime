-- 003: 给 tool_call 增加租户列，与 agent_run/agent_node 的多租户隔离保持一致。
-- tool_call 的 idempotency_key 虽全局唯一，但由 server 端从租户作用域的 node_id 派生，
-- 加上 tenant_id 过滤可形成纵深防御（与 002 同样的处理方式）。
USE agent_runtime;

ALTER TABLE tool_call ADD COLUMN tenant_id VARCHAR(128) NOT NULL DEFAULT '';

-- 从 agent_run 回填租户（agent_run.tenant_id 已在 002 就位）。
UPDATE tool_call tc JOIN agent_run r ON r.run_id = tc.run_id SET tc.tenant_id = r.tenant_id;

ALTER TABLE tool_call MODIFY COLUMN tenant_id VARCHAR(128) NOT NULL;
ALTER TABLE tool_call ADD INDEX idx_toolcall_tenant (tenant_id);
