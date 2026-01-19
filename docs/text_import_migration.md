# 文本导出/导入迁移方案（OceanBase 导入工具）

本方案用于在数据量较大时，使用文本导出 + OceanBase 导入工具来提升迁移速度，并支持覆盖/替换已有数据。

## 目标

- 提升大表迁移速度，减少逐行插入开销
- 导出时对数据进行必要的预处理（类型、NULL、布尔等）
- 支持追加、覆盖替换、全量重载三种模式

## 总体流程

1. 从 PostgreSQL 导出为文本文件（CSV/TSV）
2. 导出阶段按需预处理（字段过滤、类型转换、格式统一）
3. 使用 OceanBase 导入工具加载数据
4. 根据需求选择追加、覆盖替换或全量重载策略
5. 校验（行数、抽样校验）

## 导出规则（PostgreSQL -> Text）

建议使用 `\copy` 或 `COPY TO` 导出 CSV：

- 格式：CSV（逗号分隔）
- NULL：`\N`
- 字符集：UTF-8
- 行结束：`\n`
- 不输出表头

示例（按字段导出，自动忽略 JSON/JSONB/数组字段）：

```sql
COPY (
  SELECT
    id,
    name,
    CASE WHEN is_active THEN 1 ELSE 0 END AS is_active,
    created_at
  FROM public.test_users
) TO STDOUT WITH (
  FORMAT csv,
  DELIMITER ',',
  NULL '\N',
  QUOTE '"',
  ESCAPE '\\',
  HEADER false
);
```

### 导出脚本（内置）

项目内置脚本可直接导出为 obloader 使用的 CSV：

```bash
python scripts/export_to_tsv.py --config config/config.yaml --output-dir export
```

如需批量创建表结构，可先导出 SQL，再在 OceanBase 侧一次性执行：

```bash
python scripts/export_schema_sql.py --config config/config.yaml \
  --output-file export/schema.sql --include-indexes
```

执行导出的 SQL：

```bash
python scripts/apply_schema_sql.py --config config/config.yaml \
  --sql-file export/schema.sql
```

若重复执行导致索引已存在，可加参数跳过重复索引错误：

```bash
python scripts/apply_schema_sql.py --config config/config.yaml \
  --sql-file export/schema.sql --skip-duplicate-index
```

如需校验 bytea 十六进制数据格式（无 `\\x` 前缀、偶数长度、仅 0-9a-f）：

```bash
python scripts/validate_hex_export.py --config config/config.yaml \
  --table your_table --file /path/to/your_table.csv
```

### 建议的预处理规则

- 布尔：`true/false` -> `1/0`
- timestamp/timestamptz：统一为 `YYYY-MM-DD HH:MM:SS`（不带时区）
- JSON/JSONB/数组字段：不导出（已忽略）
- 超长字符：确保目标列类型为 `TEXT/LONGTEXT`
- NULL：统一为 `\N`
- bytea：导出为十六进制字符串（不包含 `\\x` 前缀），目标列建议使用 `LONGBLOB`

## 导入方式（OceanBase）

### 使用 obloader（推荐）

适合超大规模文件导入，支持多线程、断点续传。建议用 CSV 并显式配置 NULL、分隔符、目标表等。

**步骤概览**

1. 为每个表准备 CSV 文件（可按主键区间分片）
2. 编写 obloader 配置（目标库连接、文件路径、分隔符、NULL 表示、并发）
3. 执行 obloader 导入
4. 根据覆盖策略选择 REPLACE/UPSERT 或先导入临时表再合并

**配置要点（按 obloader 版本调整）**

- 目标库连接：host/port/user/password/database
- 文件路径与表映射
- 文件格式：CSV（字段分隔符 `,`，NULL 为 `\N`，转义为 `\\`）
- 并发参数：并发 worker、批大小
- 覆盖策略：replace / upsert / truncate + load

**命令示例（obloader 4.3.4）**

```bash
obloader -h <host> -P <port> -u <user> -t <tenant> -c <cluster> -p <password> -D <database> \
  --csv --file-encoding=UTF-8 --character-set=utf8mb4 \
  --column-separator=, --line-separator=$'\n' --null-string='\\N' \
  --column-quote='"' --escape-character='\\' \
  --file-suffix=.csv --thread=8 -f /path/to/csv_dir
```

> 模板脚本：`docs/obloader_4_3_4_example.sh`

### 备用方式：LOAD DATA INFILE（MySQL 兼容）

```sql
LOAD DATA INFILE '/path/to/test_users.csv'
INTO TABLE test_users
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
ESCAPED BY '\\'
LINES TERMINATED BY '\n'
(@id, @name, @is_active, @created_at)
SET
  id = NULLIF(@id, '\\N'),
  name = NULLIF(@name, '\\N'),
  is_active = NULLIF(@is_active, '\\N'),
  created_at = NULLIF(@created_at, '\\N');
```

## 覆盖/替换策略

### 1. 追加（INSERT/APPEND）

- 默认导入模式，若主键冲突会报错
- 适用于全量为空或不需要覆盖的场景

### 2. 覆盖替换（REPLACE/UPSERT）

**方式一：obloader 直接覆盖**

obloader 4.3.4 支持 `--replace-data`，可用于主键冲突时覆盖写入。

**方式二：导入到临时表 + 合并**

1. 导入到临时表（同结构）
2. `INSERT ... ON DUPLICATE KEY UPDATE` 或分批 `DELETE + INSERT`
3. 清理临时表

### 3. 全量重载（TRUNCATE + 导入）

- 先清空目标表
- 适用于目标库可停机或允许完全覆盖的场景

## 并行导出/导入建议

- 按主键区间切分导出（例如 `id` 分段）
- 多文件并行导入（注意控制并发，避免压垮目标库）

示例导出分段：

```sql
COPY (
  SELECT * FROM public.test_users
  WHERE id BETWEEN 1 AND 100000
) TO STDOUT WITH (FORMAT csv, DELIMITER E'\t', NULL '\N');
```

## 校验建议

- 行数校验（源库/目标库 count）
- 抽样校验（按主键抽样比对）
- 若覆盖替换，确保主键或唯一键完整

## 建议落地方式（与现有工具结合）

1. 通过脚本生成每表的导出 SQL（自动忽略 JSON/数组字段）
2. 输出为 TSV 文件
3. 使用导入工具批量加载
4. 若需覆盖，使用 `REPLACE` 或临时表策略
