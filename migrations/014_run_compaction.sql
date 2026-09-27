-- 014: 上下文压缩水位线表（run_compaction）。
-- 按 docs/context-compaction.md 落地：压缩不是改写 agent_node（权威事实），
-- 而是持久化一份"派生视图"+水位线——ContextLoader 组装时从水位线之后取节点，
-- 水位线之前的历史由摘要/逐节点回填替代。
--
-- kind 语义：
--   'micro'：L3 微压缩。sections 为 {node_id: 一句话摘要} 的 JSON 映射，
--            组装时逐节点内联回填（对话轮次与顺序不变，仅原文换成转述）。
--   'full' ：L4 全量压缩。summary 为固定八段结构化摘要，组装时作为 system
--            消息成为上下文基座，水位线推进到最新已完成节点。
--
-- 唯一键 (tenant,run,kind,waterline)：并发分支同时触发压缩时"先算后插、
-- 冲突丢弃"——摘要输入相同、产出等价，冲突方直接复用已插入的记录，
-- 不需要占位租约状态机。compaction_id 由 (run,kind,waterline) 哈希派生，
-- 与唯一键一一对应，天然幂等。
--
-- 读取端取"最新产生的记录"（created_at DESC）即水位线最深的记录：
-- 水位线只推进不回退，产生序与水位线序一致。
-- 本表与 agent_node 一样是租户隔离的运行时数据，无外键——压缩记录的
-- waterline 节点可能随后续扩展被任何清理策略回收，读取端对"找不到
-- waterline 节点"按全量原文降级（fail-open 到信息更多的一侧）。
USE agent_runtime;

CREATE TABLE IF NOT EXISTS run_compaction (
  compaction_id VARCHAR(64) NOT NULL,
  tenant_id VARCHAR(128) NOT NULL,
  run_id VARCHAR(64) NOT NULL,
  kind VARCHAR(16) NOT NULL,
  waterline_node_id VARCHAR(64) NOT NULL,
  summary MEDIUMTEXT,
  sections JSON,
  estimated_tokens INT NOT NULL DEFAULT 0,
  model VARCHAR(128) NOT NULL DEFAULT '',
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (compaction_id),
  UNIQUE KEY uk_compaction_waterline (tenant_id, run_id, kind, waterline_node_id),
  INDEX idx_compaction_run (tenant_id, run_id, created_at)
) ENGINE=InnoDB;
