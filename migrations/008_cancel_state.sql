-- 008: 用户取消链路。
-- 新增 Run 中间态 CANCEL_REQUESTED 与 Node 终态 CANCELLED。
-- status 列为 VARCHAR，无需 ALTER；仅添加 agent_run.status 索引以支撑
-- recovery 的取消扫描（SELECT ... WHERE status='CANCEL_REQUESTED'）。
USE agent_runtime;

ALTER TABLE agent_run ADD INDEX idx_run_status (status);
