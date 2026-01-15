#!/usr/bin/env bash
set -euo pipefail

# obloader 4.3.4 CLI 模板
# 说明：字段与参数来自 obloader 4.3.4 --help，请按实际环境调整。

OB_HOST="your_oceanbase_host"
OB_PORT="2881"
OB_USER="root@tenant"
OB_TENANT="tenant"
OB_CLUSTER="obcluster"
OB_DB="your_database"
OB_PASSWORD="your_password"

DATA_DIR="/path/to/csv_files"
FILE_SUFFIX=".csv"

obloader \
  -h "${OB_HOST}" \
  -P "${OB_PORT}" \
  -u "${OB_USER}" \
  -t "${OB_TENANT}" \
  -c "${OB_CLUSTER}" \
  -p "${OB_PASSWORD}" \
  -D "${OB_DB}" \
  --csv \
  --file-encoding="UTF-8" \
  --character-set="utf8mb4" \
  --column-separator=',' \
  --line-separator=$'\n' \
  --null-string="\\N" \
  --file-suffix="${FILE_SUFFIX}" \
  --thread=8 \
  -f "${DATA_DIR}"

# 覆盖/替换策略：
# 1) replace（按主键覆盖）
#   加上：--replace-data
# 2) 全量重载（清空表再导入）
#   加上：--truncate-table -y
# 3) 删除后导入（删除表数据再导入）
#   加上：--delete-from-table -y
