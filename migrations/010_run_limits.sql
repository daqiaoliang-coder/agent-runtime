-- 多轮 Plan 死循环防护：限制续规轮次与 token 消耗上限。
-- max_rounds: 多轮 Plan 的续规轮次上限，0 表示不限制。
-- max_tokens: Run 累计 LLM token 消耗上限，0 表示不限制。
ALTER TABLE agent_run ADD COLUMN max_rounds INT NOT NULL DEFAULT 10;
ALTER TABLE agent_run ADD COLUMN max_tokens INT NOT NULL DEFAULT 0;
