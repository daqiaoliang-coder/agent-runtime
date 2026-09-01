-- 006_llm_usage.sql
-- LLM token/cost tracking：记录每次 LLM 调用的 token 消耗与估算成本。
-- 按 run / tenant / model 维度聚合，支撑成本分析与配额管控。
CREATE TABLE IF NOT EXISTS llm_usage (
    usage_id           VARCHAR(64)  PRIMARY KEY,
    run_id             VARCHAR(64)  NOT NULL,
    node_id            VARCHAR(64)  NOT NULL,
    tenant_id          VARCHAR(128) NOT NULL,
    model              VARCHAR(64)  NOT NULL,
    prompt_tokens      INT          NOT NULL,
    completion_tokens  INT          NOT NULL,
    total_tokens       INT          NOT NULL,
    cost               DECIMAL(10,6),
    created_at         DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    INDEX idx_llm_usage_run    (run_id),
    INDEX idx_llm_usage_tenant (tenant_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
