-- 009: 多轮 Plan 支持。
-- 新增 planning_round 列标识节点属于第几轮规划（默认 1 = 初始规划）。
-- 新增 NodeReflect 节点类型 (REFLECT) 和 ReplanRequested 事件类型。
-- REFLECT 节点由 Executor 执行 LLM 决策，输出 JSON {"action":"replan"|"finish"}。
-- "replan" 触发 Resumer 调用 Planner.Replan 追加新节点到 DAG。
USE agent_runtime;

ALTER TABLE agent_node ADD COLUMN planning_round INT NOT NULL DEFAULT 1;
ALTER TABLE agent_node ADD INDEX idx_node_round (run_id, planning_round);
