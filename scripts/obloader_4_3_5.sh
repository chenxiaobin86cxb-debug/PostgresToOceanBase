#!/usr/bin/env bash
set -euo pipefail

# obloader 4.3.4 CLI 模板
# 说明：字段与参数来自 obloader 4.3.4 --help，请按实际环境调整。

# 1. 环境路径配置
export OB_LOADER_PATH="/data/ob-loader-dumper"
DATA_DIR="./export"
FILE_SUFFIX=".csv"

OB_HOST="183.6.70.7"
OB_PORT="2881"
OB_USER="root@test"
OB_TENANT="test"
OB_CLUSTER="obcluster"
OB_DB="test"
OB_PASSWORD="kwaidoo123"


# 3. JVM 内存优化 (解决 committed > max 报错)
# 将堆内存固定为 8GB，防止 JVM 在大对象读写时因动态扩容崩溃
export JAVA_OPTS="-Xms8g -Xmx8g -XX:+HeapDumpOnOutOfMemoryError -XX:MetaspaceSize=256m"

# 4. 执行导入
echo "Starting OBLOADER ..."


$OB_LOADER_PATH/bin/obloader-debug \
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
  --batch=100 \
  -f "${DATA_DIR}" \
  --replace-data \
  --all

#  --table $1
#  --all
#  --ctl-path "/data/controls" \
#  --ignore-unhex \


# 覆盖/替换策略：
# 1) replace（按主键覆盖）
#   加上：--replace-data
# 2) 全量重载（清空表再导入）
#   加上：--truncate-table -y
# 3) 删除后导入（删除表数据再导入）
#   加上：--delete-from-table -y

# 说明：
# --thread=2: 降低并发，减少大字段在内存中的堆积
# --batch=1000: 减小每批写入行数，防止生成的 SQL 超过 max_allowed_packet
# --memory-threshold=90: 内存使用达到 90% 时自动限速，防止 OOM
# --external-jdbc-arguments: 在驱动层开启大数据包支持
