#!/usr/bin/env bash
#
# migrations/bootstrap.sh —— 把数据库引导到最新 schema。
#
# 为什么需要它，而不是文档里那串 mysql < 命令：
# README 曾长期漏掉 014_run_identity.sql，而 store.CreateRun 的 INSERT 依赖它加的
# user_id / auth_method 两列 —— 结果是任何照文档从零部署的人，第一次 CreateRun
# 就撞上 Unknown column 'user_id'。手工命令列表每加一个迁移就要同步一处文档，
# 漏一处就断一次部署，而这个同步没有任何机制来校验。
#
# 本脚本按文件名顺序扫描本目录，用 schema_migrations 记录已应用版本：
#   - 全新库：依次应用全部迁移；
#   - 已存在的库：只应用增量（或提示已最新）；
#   - 重复运行：幂等，不做任何变更。
#
# 用法：
#   ./migrations/bootstrap.sh                       # 用默认连接参数
#   DATABASE_DSN=agent:agent@tcp(127.0.0.1:3306)/agent_runtime ./migrations/bootstrap.sh
#   MYSQL_HOST=127.0.0.1 MYSQL_PORT=3308 ./migrations/bootstrap.sh
#
# 依赖：mysql 客户端（在 PATH 中）。

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# 支持从 DATABASE_DSN 推导连接参数（与运行时配置同源，避免两处维护）。
# 形如 user:pass@tcp(host:port)/dbname?parseTime=true
if [[ -n "${DATABASE_DSN:-}" && "${DATABASE_DSN}" == *"@tcp("* ]]; then
  dsn_userpass="${DATABASE_DSN%%@tcp(*}"
  dsn_tail="${DATABASE_DSN#*@tcp(}"
  # 括号用字符类 [)] 而不是转义 \) ：后者在参数展开的 pattern 里不会按预期匹配，
  # 一旦匹配失败,主机会被解析成"到库名末尾的整串"，端口与库名全错。
  dsn_hostport="${dsn_tail%%[)]*}"
  dsn_db_and_opts="${dsn_tail#*[)]/}"
  dsn_db="${dsn_db_and_opts%%\?*}"
  # 判据是"userpass 里有没有冒号"，不能拿"密码是否等于用户名"来判断无密码：
  # 本项目默认凭据恰恰是 agent:agent，两者同名，按后者判断会把密码擦成空串，
  # 于是在**唯一一个默认场景下**连不上库，而且错误信息指向主机而非密码。
  if [[ "${dsn_userpass}" == *":"* ]]; then
    MYSQL_USER="${dsn_userpass%%:*}"
    MYSQL_PASSWORD="${dsn_userpass#*:}"
  else
    MYSQL_USER="${dsn_userpass}"
    MYSQL_PASSWORD=""
  fi
  MYSQL_HOST="${dsn_hostport%%:*}"
  MYSQL_PORT="${dsn_hostport##*:}"
  MYSQL_DATABASE="${dsn_db}"
  if [[ -z "${MYSQL_HOST}" || -z "${MYSQL_DATABASE}" ]]; then
    echo "bootstrap: 无法从 DATABASE_DSN 解析主机或库名，请改用 MYSQL_* 变量" >&2
    exit 1
  fi
fi

MYSQL_HOST="${MYSQL_HOST:-127.0.0.1}"
MYSQL_PORT="${MYSQL_PORT:-3306}"
MYSQL_USER="${MYSQL_USER:-agent}"
MYSQL_PASSWORD="${MYSQL_PASSWORD:-agent}"
MYSQL_DATABASE="${MYSQL_DATABASE:-agent_runtime}"

if ! command -v mysql >/dev/null 2>&1; then
  echo "bootstrap: mysql 客户端不在 PATH 中，请先安装 mysql client" >&2
  exit 1
fi

mysql_run() {
  local db="$1"
  shift
  mysql --host="${MYSQL_HOST}" --port="${MYSQL_PORT}" \
        --user="${MYSQL_USER}" --password="${MYSQL_PASSWORD}" \
        --database="${db}" --batch --raw "$@"
}

# 连通性自检：凭据或端口不对时，后面的失败会变成一堆难懂的 SQL 报错，
# 不如在这里把"连不上"和"SQL 有问题"这两类故障分开。
if ! mysql --host="${MYSQL_HOST}" --port="${MYSQL_PORT}" \
           --user="${MYSQL_USER}" --password="${MYSQL_PASSWORD}" \
           --execute="SELECT 1" >/dev/null 2>&1; then
  echo "bootstrap: 无法连接 MySQL ${MYSQL_USER}@${MYSQL_HOST}:${MYSQL_PORT}" >&2
  echo "          请检查 MYSQL_HOST/MYSQL_PORT/MYSQL_USER/MYSQL_PASSWORD 或 DATABASE_DSN" >&2
  exit 1
fi

# agent_runtime 库由 001_init.sql 自建；先建出来才能在其上建版本表。
mysql --host="${MYSQL_HOST}" --port="${MYSQL_PORT}" \
      --user="${MYSQL_USER}" --password="${MYSQL_PASSWORD}" \
      --execute="CREATE DATABASE IF NOT EXISTS ${MYSQL_DATABASE} CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci"

mysql_run "${MYSQL_DATABASE}" --execute="
CREATE TABLE IF NOT EXISTS schema_migrations (
  version    VARCHAR(16) PRIMARY KEY,
  applied_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
) ENGINE=InnoDB"

applied=0
skipped=0

for file in "${SCRIPT_DIR}"/*.sql; do
  [[ -e "${file}" ]] || continue
  version="$(basename "${file}" .sql)"
  version="${version%%_*}"
  if [[ -z "${version}" ]]; then
    echo "bootstrap: 跳过无法解析版本号的文件 $(basename "${file}")" >&2
    continue
  fi

  exists="$(mysql_run "${MYSQL_DATABASE}" --skip-column-names --execute="
    SELECT COUNT(*) FROM schema_migrations WHERE version='${version}'")"

  if [[ "${exists}" == "1" ]]; then
    skipped=$((skipped + 1))
    continue
  fi

  echo "bootstrap: applying ${version} ($(basename "${file}"))"
  # 迁移内容与版本记录在同一个事务里提交：要么两者都生效，要么都不生效。
  # 否则"迁移成功但没登记"会让下一次运行重复执行不可重入的 ALTER，
  # 而"登记了但没执行"会让环境静默停在旧 schema 上。
  mysql_run "${MYSQL_DATABASE}" <<SQL
START TRANSACTION;
$(cat "${file}")
INSERT INTO schema_migrations(version) VALUES('${version}');
COMMIT;
SQL
  applied=$((applied + 1))
done

if [[ "${applied}" -eq 0 ]]; then
  echo "bootstrap: schema 已是最新（已应用 ${skipped} 个迁移，本次无变更）"
else
  echo "bootstrap: 完成，新增应用 ${applied} 个迁移，跳过已应用 ${skipped} 个"
fi
