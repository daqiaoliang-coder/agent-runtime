-- 授权学习规则表（docs/permission-classifier.md §7，P2）。
-- 审批闭环的最后一环：审批结果的可复用。"always allow (本 Run/本租户)"
-- 与 deny 写回都落在这一张表，同模式的后续调用在 L1 直接命中，
-- 不再打扰人工。
--
-- 软删而非硬删：learned_by/learned_at 是审计字段，硬删会让"谁在何时
-- 授权过什么"从账面上消失——撤销本身也要可审计。
-- run_id 为 NULL 表示租户级规则（source=tenant/learned），
-- 非空表示 Run 级（source=run，Run 结束后由清理任务回收，当前先随 Run 留存）。
USE agent_runtime;

CREATE TABLE IF NOT EXISTS permission_rule (
  rule_id VARCHAR(64) PRIMARY KEY,
  tenant_id VARCHAR(128) NOT NULL,
  run_id VARCHAR(64) DEFAULT NULL,
  tool VARCHAR(64) NOT NULL,
  pattern VARCHAR(255) NOT NULL,
  effect VARCHAR(32) NOT NULL,
  source VARCHAR(32) NOT NULL,
  learned_by VARCHAR(128) NOT NULL,
  learned_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  revoked_at DATETIME(6) DEFAULT NULL,
  INDEX idx_perm_rule_scope (tenant_id, run_id, revoked_at),
  INDEX idx_perm_rule_audit (tenant_id, source, revoked_at)
) ENGINE=InnoDB;
