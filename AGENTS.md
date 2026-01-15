# AGENTS.md

## 项目概述

PostgreSQL 到 OceanBase 迁移工具，用于全量数据迁移，自动忽略 JSON/JSONB 和数组类型字段。

## 构建命令

```bash
# 创建虚拟环境
python3 -m venv venv
source venv/bin/activate  # Linux/Mac
# venv\Scripts\activate  # Windows

# 安装依赖
pip install -r requirements.txt

# 运行完整迁移（表结构 + 数据 + 验证）
python src/main.py --config config/config.yaml --validate

# 仅迁移表结构
python src/main.py --config config/config.yaml --schema-only

# 仅迁移数据
python src/main.py --config config/config.yaml --data-only

# Docker 构建
docker build -t postgres2ob:latest .
docker-compose up -d
```

## 测试命令

```bash
# 运行所有测试
pytest tests/

# 运行单个测试文件
pytest tests/test_converter.py

# 运行特定测试函数
pytest tests/test_converter.py::test_convert_boolean

# 显示详细输出
pytest -v tests/

# 生成覆盖率报告
pytest --cov=src --cov-report=html tests/
```

## Lint 和格式化

```bash
# 代码风格检查（推荐使用 ruff）
ruff check src/

# 自动修复问题
ruff check --fix src/

# 格式化代码（使用 black）
black src/ tests/

# 类型检查（使用 mypy）
mypy src/ --ignore-missing-imports
```

## 代码风格指南

### 导入顺序

1. 标准库导入
2. 第三方库导入
3. 本项目导入
4. 各部分之间空一行

```python
import logging
from typing import List, Dict, Optional

import yaml
from loguru import logger

from src.database.connection import ConnectionManager
from src.migration.converter import TypeConverter
```

### 类型注解

- 所有函数必须使用类型注解
- 复杂类型使用 typing 模块
- 类属性使用类型提示

```python
from typing import List, Dict, Any, Optional

def get_tables(self, schema: str = 'public') -> List[str]:
    pass

def convert_column_type(self, postgres_type: str, column_info: Dict) -> str:
    pass

class ConnectionManager:
    def __init__(self, config: dict):
        self.source_config: dict = config.get('source', {})
        self.source_pool: Optional[pg_pool.ThreadedConnectionPool] = None
```

### 命名约定

- 类名：PascalCase（如 ConnectionManager）
- 函数/变量名：snake_case（如 get_table_count）
- 常量：UPPER_SNAKE_CASE（如 MAX_RETRIES）
- 私有方法/属性：_leading_underscore

### 错误处理

- 使用 try-except 捕获异常
- 记录错误日志（使用 loguru.logger）
- 资源清理放在 finally 中
- 连接使用 contextmanager

```python
@contextmanager
def get_source_connection(self):
    conn = None
    try:
        conn = self.source_pool.getconn()
        yield conn
    except Exception as e:
        logger.error(f"PostgreSQL 连接错误: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            self.source_pool.putconn(conn)
```

### 日志规范

- 使用 loguru.logger（不使用标准 logging）
- 关键操作：logger.info()
- 成功完成：logger.info(f"操作成功: {table_name}")
- 失败错误：logger.error(f"操作失败: {table_name}, 错误: {e}")
- 警告信息：logger.warning(f"警告信息: {detail}")

### 配置管理

- 使用 YAML 配置文件
- 敏感信息通过环境变量
- 使用 pydantic 数据验证
- 配置文件位置：config/

```python
def load_config(config_file: str = 'config/config.yaml') -> dict:
    with open(config_file, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    logger.info(f"配置文件加载成功: {config_file}")
    return config
```

### 数据库操作

- 使用连接池管理连接
- 批量操作使用 executemany
- 分批读取大表数据
- 事务手动提交或回滚

```python
def insert_batch(self, table_name: str, data: List[Dict], batch_size: int = 1000):
    for i in range(0, len(data), batch_size):
        batch = data[i:i + batch_size]
        cursor.executemany(sql, values)
        conn.commit()
```

### 重试机制

- 网络操作和数据库操作支持重试
- 使用装饰器实现重试逻辑
- 记录重试次数

```python
@retry(max_retries=3, delay=5)
def insert_data(self, table_name: str, data: List[Dict]):
    pass
```

### 进度显示

- 使用 tqdm 显示进度条
- 批量操作显示进度
- 实时更新进度信息

```python
from tqdm import tqdm

pbar = tqdm(total=total_count, desc=f"Migrating {table_name}")
for batch in batches:
    process(batch)
    pbar.update(len(batch))
pbar.close()
```

### 数据类型转换

- PostgreSQL 类型转换为 OceanBase MySQL 类型
- 类型映射配置在 config/type_mapping.yaml
- 特殊类型处理：BOOLEAN → TINYINT(1), TIMESTAMP WITH TIME ZONE → TIMESTAMP

### 忽略字段

- JSON/JSONB 类型自动忽略
- 数组类型自动忽略
- 在 config.yaml 中配置 ignore_types

### 文档注释

- 类和主要方法添加文档字符串
- 复杂逻辑添加中文注释
- 配置项说明使用注释

```python
class PostgreSQLClient:
    """PostgreSQL 客户端"""

    def get_table_schema(self, table_name: str, schema: str = 'public') -> Dict[str, Any]:
        """获取表结构"""
        pass
```

## 项目结构

```
PostgresToOceanBase/
├── config/              # 配置文件目录
│   ├── config.yaml      # 主配置文件
│   ├── type_mapping.yaml # 数据类型映射
│   └── logger.yaml      # 日志配置
├── src/                 # 源代码目录
│   ├── database/        # 数据库模块
│   │   ├── postgres.py  # PostgreSQL 客户端
│   │   ├── oceanbase.py # OceanBase 客户端
│   │   └── connection.py # 连接池管理
│   ├── migration/        # 迁移模块
│   │   ├── schema.py    # 表结构迁移
│   │   ├── data.py      # 数据迁移
│   │   ├── validator.py # 数据验证
│   │   └── converter.py # 类型转换
│   ├── utils/           # 工具模块
│   │   ├── logger.py    # 日志工具
│   │   ├── progress.py  # 进度显示
│   │   └── retry.py     # 重试机制
│   └── main.py          # 主程序入口
├── tests/               # 测试文件
│   ├── test_converter.py
│   ├── test_schema.py
│   └── test_data.py
├── logs/                # 日志目录
├── backup/              # 备份目录
├── .env                 # 环境变量
├── .gitignore
├── requirements.txt
├── setup.py
└── README.md
```

## 重要约束

1. 忽略 JSON/JSONB 类型字段
2. 忽略数组类型字段
3. OceanBase 使用 MySQL 租户模式
4. 统一使用 UTF-8 字符集
5. 连接使用连接池
6. 迁移前必须备份源数据库
