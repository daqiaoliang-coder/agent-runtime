-- 持久化 Planner 决策：ReplanRequested 事件重投时复用原决策，
-- 避免重复调用非确定性 LLM 生成不同计划导致语义偏移。
-- 以 (run_id, trigger_node_id, planning_round) 为唯一键，
-- 崩溃在"生成决策后、InsertPlan 前"时，事件重投会命中已保存的决策。
CREATE TABLE IF NOT EXISTS planner_decision (
  decision_id VARCHAR(128) PRIMARY KEY,
  run_id VARCHAR(64) NOT NULL,
  tenant_id VARCHAR(128) NOT NULL,
  trigger_node_id VARCHAR(64) NOT NULL,
  planning_round INT NOT NULL,
  plan_json LONGTEXT NOT NULL,
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  UNIQUE KEY uk_decision (run_id, trigger_node_id, planning_round),
  CONSTRAINT fk_decision_run FOREIGN KEY (run_id) REFERENCES agent_run(run_id) ON DELETE CASCADE
) ENGINE=InnoDB;
