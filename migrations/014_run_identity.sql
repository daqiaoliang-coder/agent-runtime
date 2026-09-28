-- 014: 为 agent_run 增加发起者身份列（user_id / auth_method）。
--
-- 为什么身份必须落库，而不能只在进程内随 ExecutionContext 流转：
-- Run 由 cmd/runtime 创建、由 cmd/worker 执行，两者是**独立进程**，
-- 中间隔着 MySQL 与 Redis 队列。身份若不写进 agent_run，
-- worker 侧就无从得知这次 Run 是谁发起的 —— 这正是此前
-- worker.go 注入 ExecutionContext 时 UserID 只能恒为空的原因。
--
-- 后果不只是审计缺一列：middleware/guard.go 三层防御的第 1 层是"权限收窄"，
-- 要求 Tool 调用携带 Run 发起者身份而非全局服务账号，
-- 于是"骗 LLM 去调删库工具"在权限层面走不通。身份传不到工具调用层，
-- 这层防御就只剩人工闸门与输入隔离在支撑。
--
-- auth_method 记录身份是被**怎么**认证的（jwt-hs256 / jwt-rs256 / static-token / ...）。
-- 单独存一列的理由：一个由静态 token 明文比对认证的身份，与一个由 RSA 非对称签名
-- 认证的身份，可信程度完全不同。若只存 user_id，事后追溯时就无法区分
-- "这个高危操作是弱机制放行的"还是"强机制放行的"，而这恰是安全复盘最需要的信息。
-- 未配置认证层时该列为空串，空值即"身份未经证明"。
--
-- 为什么不加 NOT NULL：存量 Run 没有发起者身份信息，回填不出来。
-- 用空串作默认值并保持可空语义，让"未知"与"匿名的某个具体用户"可区分。
-- 依赖身份的授权判定必须把空值当作拒绝而非放行。
--
-- 为什么不给 agent_node 也加列：节点的身份恒等于其所属 Run 的身份，
-- worker 已经为每个节点加载 Run（见 worker.go 的 GetRun 调用），
-- 从 Run 取即可。在 agent_node 上冗余存储会让"最热的表"多两个列，
-- 而该表的写入与锁竞争是全系统最敏感的（认领、CAS、租约续期都在其上）——
-- 这与 013 选择独立进度表而非给 agent_node 加列是同一个权衡。
USE agent_runtime;

ALTER TABLE agent_run ADD COLUMN user_id VARCHAR(128) NOT NULL DEFAULT '';
ALTER TABLE agent_run ADD COLUMN auth_method VARCHAR(32) NOT NULL DEFAULT '';

-- 按发起者检索 Run 的常见运维查询（某用户的全部运行、某用户的异常运行），
-- 没有索引会退化成全表扫描。tenant_id 在前，与既有 idx_run_tenant 的隔离约定一致。
ALTER TABLE agent_run ADD INDEX idx_run_user (tenant_id, user_id);
