#!/usr/bin/env bash
set -euo pipefail

# obloader 4.3.4 CLI 模板
# 说明：字段与参数来自 obloader 4.3.4 --help，请按实际环境调整。

export OB_LOADER_PATH="/data/ob-loader-dumper"


OB_HOST="183.6.70.7"
OB_PORT="2881"
OB_USER="root@test"
OB_TENANT="test"
OB_CLUSTER="obcluster"
OB_DB="test"
OB_PASSWORD="kwaidoo123"



DATA_DIR="./export"
FILE_SUFFIX=".csv"

$OB_LOADER_PATH/bin/obloader \
  -h "${OB_HOST}" \
  -P "${OB_PORT}" \
  -u "${OB_USER}" \
  -t "${OB_TENANT}" \
  -p "${OB_PASSWORD}" \
  -D "${OB_DB}" \
  --csv \
  --file-encoding="UTF-8" \
  --character-set="utf8" \
  --column-separator=',' \
  --line-separator=$'\n' \
  --null-string="\\N" \
  --column-quote="\"" \
  --escape-character="\\" \
  --file-suffix="${FILE_SUFFIX}" \
  --thread=8 \
  -f "${DATA_DIR}"
  --replace-data
  --all


# 覆盖/替换策略：
# 1) replace（按主键覆盖）
#   加上：--replace-data
# 2) 全量重载（清空表再导入）
#   加上：--truncate-table -y
# 3) 删除后导入（删除表数据再导入）
#   加上：--delete-from-table -y
