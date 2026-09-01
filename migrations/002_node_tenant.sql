-- 为 agent_node 增加 tenant_id 列，实现节点级多租户隔离。
-- 此前 tenant_id 仅存在于 agent_run，节点查询以 node_id 为唯一过滤条件，存在跨租户越权风险。
USE agent_runtime;

ALTER TABLE agent_node ADD COLUMN tenant_id VARCHAR(128) NOT NULL DEFAULT '';

-- 用所属 Run 的 tenant_id 回填历史节点。
UPDATE agent_node n JOIN agent_run r ON r.run_id = n.run_id SET n.tenant_id = r.tenant_id;

ALTER TABLE agent_node MODIFY COLUMN tenant_id VARCHAR(128) NOT NULL;
ALTER TABLE agent_node ADD INDEX idx_node_tenant (tenant_id);
