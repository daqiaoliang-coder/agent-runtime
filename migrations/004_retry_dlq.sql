-- 004: 节点退避重试 + 死信队列（DLQ）。
-- agent_node 增加 ready_at：可重试失败时把节点置回 READY 并安排 ready_at（退避到期时间），
-- ReadyTasks 扫描仅投递 ready_at 已到期者，实现指数退避重试。
USE agent_runtime;

ALTER TABLE agent_node ADD COLUMN ready_at DATETIME(6) NULL;
ALTER TABLE agent_node ADD INDEX idx_node_ready (status, ready_at);

-- 死信队列：重试耗尽的节点进入此表，供人工排查或补偿流程处理。
CREATE TABLE IF NOT EXISTS agent_dlq (
  dlq_id VARCHAR(64) PRIMARY KEY,
  run_id VARCHAR(64) NOT NULL,
  node_id VARCHAR(64) NOT NULL,
  tenant_id VARCHAR(128) NOT NULL,
  reason LONGTEXT NOT NULL,
  attempt INT NOT NULL,
  payload LONGTEXT,
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  INDEX idx_dlq_run (run_id),
  INDEX idx_dlq_tenant (tenant_id)
) ENGINE=InnoDB;
