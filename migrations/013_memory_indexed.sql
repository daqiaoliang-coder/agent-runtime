-- 013: 记忆索引进度表（写入路径）。
-- cmd/memory-indexer 把 agent_node 中已成功完成的 LLM/REFLECT 节点投影为向量写入 Qdrant，
-- 本表记录"哪些节点已投影"，使扫描只需处理增量、且崩溃重启后不重复劳动。
--
-- 为什么用独立表而不给 agent_node 加列：agent_node 是全系统最热的表
-- （认领、CAS、租约续期、恢复扫描都在其上），加列会放大写入与锁竞争。
-- 独立进度表与 agent_inbox(005) 是同一套"消费幂等"惯例。
--
-- 幂等保证是双重的：本表的 INSERT IGNORE + 向量库的确定性 point ID
-- （见 internal/adapters/vector.PointID）。因此重复投影既不产生重复进度行，
-- 也不产生重复向量。
--
-- 注意：向量库只是**派生索引**，权威内容在 agent_node.output。
-- 清空本表即可触发全量重建，因此这里不需要外键级联之外的额外保障。
USE agent_runtime;

CREATE TABLE IF NOT EXISTS memory_indexed (
  node_id VARCHAR(64) NOT NULL,
  tenant_id VARCHAR(128) NOT NULL,
  thread_id VARCHAR(64) NOT NULL DEFAULT '',
  run_id VARCHAR(64) NOT NULL,
  indexed_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (node_id),
  INDEX idx_memory_indexed_tenant (tenant_id),
  CONSTRAINT fk_memory_indexed_node FOREIGN KEY (node_id) REFERENCES agent_node(node_id) ON DELETE CASCADE
) ENGINE=InnoDB;
