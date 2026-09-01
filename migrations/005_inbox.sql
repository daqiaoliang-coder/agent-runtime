-- 005: 消费端幂等 Inbox。Resume 消费 RocketMQ 事件时按 event_id 去重，
-- 防止消息中间件"至少一次"投递导致的重复处理（如重复激活子节点、重复收敛 Run）。
USE agent_runtime;

CREATE TABLE IF NOT EXISTS agent_inbox (
  event_id VARCHAR(64) NOT NULL,
  tenant_id VARCHAR(128) NOT NULL,
  processed_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (event_id, tenant_id),
  INDEX idx_inbox_tenant (tenant_id)
) ENGINE=InnoDB;
