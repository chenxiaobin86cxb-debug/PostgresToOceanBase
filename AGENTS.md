# AGENTS.md

## 项目概述

PostgreSQL 到 OceanBase 迁移工具，专注于**全量数据迁移**和**停机迁移**策略。该工具自动忽略 `JSON/JSONB` 和 `数组` 类型字段，确保核心业务数据平滑迁移至 OceanBase MySQL 租户。

## 核心能力
- **全量迁移**：支持表结构 + 索引 + 数据的完整迁移，自动忽略 JSON/JSONB 和数组类型字段。
- **增量迁移**：支持仅调整表结构和索引，通过模型比对生成差异报告，实现最小化变更。
- **高性能数据迁移**：支持批量插入、并发处理和流式读取。
- **智能字段过滤**：根据配置自动跳过不支持或不需要的复杂数据类型。
- **完整性校验**：提供记录数、抽样 MD5 和汇总值多维度验证。

---

## 构建与测试

### 快速开始
```bash
# 1. 创建并激活虚拟环境
python3 -m venv venv
source venv/bin/activate

# 2. 安装依赖
pip install -r requirements.txt

# 3. 配置环境变量
cp .env.example .env  # 编辑 .env 设置密码

# 4. 运行迁移（表结构 + 数据 + 验证）
python src/main.py --config config/config.yaml --validate
```

### 测试命令
```bash
# 运行所有测试
pytest tests/

# 运行单个测试文件
pytest tests/test_converter.py -v

# 运行特定测试函数
pytest tests/test_converter.py::test_convert_boolean -v

# 运行测试并生成覆盖率报告
pytest --cov=src --cov-report=html tests/

# 查看覆盖率报告
open htmlcov/index.html  # Mac
xdg-open htmlcov/index.html  # Linux
```

### 代码质量命令
```bash
# 代码风格检查
ruff check src/

# 自动修复 lint 错误
ruff check --fix src/

# 格式化代码
black src/ tests/

# 类型检查
mypy src/ --ignore-missing-imports
```

### 运行命令
| 命令 | 说明 |
|------|------|
| `python src/main.py --config config/config.yaml --validate` | 完整迁移（表结构 + 数据 + 验证） |
| `python src/main.py --schema-only` | 仅迁移表结构（含字段过滤） |
| `python src/main.py --data-only` | 仅迁移存量数据 |
| `python src/main.py --validate` | 迁移完成后执行一致性校验 |

---

## 代码风格规范

### 导入顺序
```python
# 1. 标准库
import os
import sys
from typing import Dict, List, Optional

# 2. 第三方库
import yaml
from loguru import logger
import psycopg2
import pymysql

# 3. 本地模块
from src.database.connection import ConnectionManager
from src.migration.converter import TypeConverter
```

### 命名约定
- **类名**: PascalCase（如 `TypeConverter`, `PostgreSQLClient`, `DataMigrator`）
- **函数/变量**: snake_case（如 `convert_value`, `get_table_count`, `batch_size`）
- **私有方法**: 前缀 `_`（如 `_quote_identifier`, `_normalize_default`）
- **常量**: UPPER_SNAKE_CASE（如 `MAX_RETRIES`, `DEFAULT_BATCH_SIZE`）

### 类型注解
所有函数必须包含类型注解：
```python
def convert_column_type(self, postgres_type: str, column_info: Dict) -> str:
    """转换列类型"""
    pass

def get_table_data(self, table_name: str, schema: str = 'public',
                  offset: int = 0, limit: int = 1000) -> List[Dict]:
    """获取表数据"""
    pass
```

### 文档字符串
使用中文文档字符串，遵循 Google 风格：
```python
def should_ignore_column(self, column_info: Dict, ignore_types: List[str]) -> bool:
    """判断是否应该忽略该列

    Args:
        column_info: 列信息字典，包含 data_type 和 udt_name
        ignore_types: 需要忽略的类型列表

    Returns:
        bool: 如果应该忽略返回 True，否则返回 False
    """
    pass
```

### 错误处理
```python
# 数据库操作必须使用 try/except/finally
def create_table(self, schema_sql: str) -> bool:
    """创建表"""
    with self.conn_mgr.get_target_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(schema_sql)
            conn.commit()
            logger.info(f"表创建成功: {schema_sql[:50]}...")
            return True
        except Exception as e:
            conn.rollback()
            logger.error(f"表创建失败: {e}")
            return False
        finally:
            cursor.close()

# 使用重试装饰器处理瞬态故障
from src.utils.retry import retry

@retry(max_retries=3, delay=5, backoff=1.0)
def safe_insert(self, table, batch):
    """带重试的安全插入"""
    pass
```

### 数据库操作规范
```python
# 1. 必须使用上下文管理器（with 语句）
with self.conn_mgr.get_source_connection() as conn:
    cursor = conn.cursor()
    # 操作
    cursor.close()

# 2. 关闭游标（finally 块）
try:
    cursor.execute(sql)
    conn.commit()
finally:
    cursor.close()

# 3. 批量插入使用 executemany
cursor.executemany(sql, values)
conn.commit()

# 4. OceanBase/MySQL 标识符必须加反引号
@staticmethod
def _quote_identifier(name: str) -> str:
    escaped = name.replace('`', '``')
    return f"`{escaped}`"

# 5. 批量大小推荐 1000-5000
batch_size = 1000  # 或根据数据量调整
```

### 日志规范
```python
from loguru import logger

# 信息日志
logger.info(f"配置文件加载成功: {config_file}")

# 警告日志
logger.warning(f"表 {table_name} 将忽略字段: {ignored_cols}")

# 错误日志
logger.error(f"表创建失败: {e}")
```

### 测试规范
```python
import pytest
from unittest.mock import Mock

# 使用 Fixture
@pytest.fixture
def mock_pg_client():
    client = Mock()
    client.get_table_schema.return_value = {...}
    return client

# 测试命名: test_<function_name>
def test_convert_boolean():
    converter = TypeConverter('config/type_mapping.yaml')
    assert converter.convert_value(True, 'boolean') == 1
    assert converter.convert_value(False, 'boolean') == 0

# 测试成功和失败路径
def test_migrate_schema_success(mock_pg_client, mock_ob_client, converter):
    migrator = SchemaMigrator(mock_pg_client, mock_ob_client, converter)
    results = migrator.migrate_schema(tables, 'public', ['json'])
    assert len(results['success']) == 1
```

---

## 核心模块规范

### 1. 字段忽略逻辑 (`src/migration/converter.py`)
在转换过程中，必须显式检查字段类型。如果属于 `ignore_types`，则应从 DDL 生成和数据读取中剔除。
```python
def should_ignore_column(self, column_info: Dict, ignore_types: List[str]) -> bool:
    data_type = column_info.get('data_type', '').lower()
    udt_name = column_info.get('udt_name', '').lower()
    return any(t in data_type or t in udt_name for t in ignore_types)
```

### 2. 批量插入优化 (`src/database/oceanbase.py`)
使用 `executemany` 配合连接池，设置合理的 `batch_size`（推荐 1000-5000）。
```python
cursor.executemany(sql, values)
conn.commit()
```

### 3. 错误处理与重试
对网络波动和数据库临时锁竞争应具备重试机制。
```python
@retry(max_retries=3, delay=5)
def safe_insert(self, table, batch):
    # 实现插入逻辑
    pass
```

---

## 迁移检查清单

### 迁移前准备
- [ ] **备份**：完成 PostgreSQL 源数据库的全量物理/逻辑备份。
- [ ] **权限**：确保迁移账号具备 `SELECT` (源端) 和 `CREATE/INSERT/UPDATE` (目标端) 权限。
- [ ] **空间**：OceanBase 磁盘空间需预留源端的 1.5-2 倍。
- [ ] **服务停机**：确认应用已停止写入 PostgreSQL。

### 迁移中监控
- [ ] **日志**：观察 `logs/migration.log` 是否有 `ERROR` 级别报错。
- [ ] **进度**：通过 `tqdm` 进度条监控大表迁移时效。
- [ ] **负载**：监控 OceanBase 的 CPU 和 IOPS，避免写入压垮租户。

### 迁移后验证
- [ ] **记录数**：比对 PG 和 OB 的 `COUNT(*)` 结果。
- [ ] **抽样比对**：对前 1000 条数据进行校验。
- [ ] **应用测试**：启动测试环境应用验证核心功能。

---

## 重要约束与风险

1. **类型丢失**：`JSON/JSONB` 和 `数组` 字段将被完全忽略，不进行迁移。
2. **时区转换**：`TIMESTAMP WITH TIME ZONE` 将转换为普通 `TIMESTAMP`，时区信息会丢失。
3. **布尔处理**：`BOOLEAN` 转换为 `TINYINT(1)`，应用层需处理 0/1 到 True/False 的映射。
4. **不可回退操作**：在 OceanBase 上执行 `TRUNCATE` 或 `DROP` 操作是不可逆的。
5. **回退策略**：若迁移失败且数据损坏，应立即清空 OB 表并切回 PG，重新评估方案。

---

## 项目结构
```text
PostgresToOceanBase/
├── config/              # 配置文件 (config.yaml, type_mapping.yaml)
├── src/
│   ├── database/        # 数据库客户端 (postgres.py, oceanbase.py)
│   ├── migration/       # 迁移逻辑 (schema.py, data.py, converter.py)
│   └── utils/           # 工具类 (logger.py, retry.py)
├── tests/               # 单元测试与集成测试
└── backup/              # 存放迁移过程中的检查点文件
```