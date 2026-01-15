# PostgreSQL 到 OceanBase 迁移工具测试方案

## 1. 概述

本文档描述 PostgreSQL 到 OceanBase 迁移工具的完整测试方案，包括单元测试、集成测试和端到端测试策略。

## 2. 测试目标

- 验证数据类型转换的准确性
- 验证表结构迁移的正确性
- 验证数据迁移的完整性和一致性
- 验证连接池管理的稳定性
- 验证重试机制和错误处理
- 验证进度显示功能

## 3. 测试环境

### 3.1 依赖服务

```yaml
services:
  postgres:
    image: postgres:17
    environment:
      POSTGRES_DB: test_db
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
    ports:
      - "5432:5432"

  oceanbase:
    image: oceanbase/oceanbase-ce
    ports:
      - "2881:2881"
```

### 3.2 测试数据库配置

```python
# tests/conftest.py
import pytest

@pytest.fixture(scope="session")
def pg_config():
    return {
        'host': 'localhost',
        'port': 5432,
        'database': 'test_db',
        'user': 'postgres',
        'password': 'postgres',
        'schema': 'public'
    }

@pytest.fixture(scope="session")
def ob_config():
    return {
        'host': '183.6.70.7',
        'port': 2881,
        'database': 'test',
        'user': 'root@test',
        'password': 'kwaidoo123'
    }
```

## 4. 测试用例

### 4.1 类型转换器测试 (TypeConverter)

#### 4.1.1 基本类型转换

| 测试用例 | 输入 | 预期输出 | 优先级 |
|---------|------|---------|--------|
| 整数类型转换 | integer | INT | P0 |
| 大整数类型转换 | bigint | BIGINT | P0 |
| 布尔类型转换 | boolean | TINYINT(1) | P0 |
| 文本类型转换 | text | TEXT | P0 |
| 可变长字符串转换 | varchar(100) | VARCHAR(100) | P0 |
| 固定长度字符串转换 | char(50) | CHAR(50) | P1 |
| 日期类型转换 | date | DATE | P1 |
| 时间戳转换 | timestamp | TIMESTAMP | P0 |
| 带时区时间戳转换 | timestamptz | TIMESTAMP | P0 |
| UUID转换 | uuid | VARCHAR(36) | P1 |
| DECIMAL精度转换 | decimal(10,2) | DECIMAL(10,2) | P0 |
| 浮点类型转换 | real | FLOAT | P1 |
| 双精度转换 | double precision | DOUBLE | P1 |
| 自增类型转换 | serial | INT AUTO_INCREMENT | P0 |
| 大自增类型转换 | bigserial | BIGINT AUTO_INCREMENT | P1 |
| BYTEA转换 | bytea | VARBINARY(65535) | P2 |
| ENUM转换 | enum | VARCHAR(255) | P2 |

#### 4.1.2 数据值转换

| 测试用例 | 输入 | 预期输出 | 优先级 |
|---------|------|---------|--------|
| 布尔值True转换 | True, 'boolean' | 1 | P0 |
| 布尔值False转换 | False, 'boolean' | 0 | P0 |
| NULL值处理 | None, 'integer' | None | P0 |
| UUID值转换 | 'a1b2c3d4', 'uuid' | 'a1b2c3d4' | P1 |
| 时间戳转换 | datetime对象, 'timestamp' | 保持不变 | P1 |

#### 4.1.3 字段忽略测试

| 测试用例 | 输入 | 预期结果 | 优先级 |
|---------|------|---------|--------|
| JSON字段忽略 | {'data_type': 'json', ...} | True | P0 |
| JSONB字段忽略 | {'data_type': 'jsonb', ...} | True | P0 |
| 数组字段忽略 | {'udt_name': '_text', ...} | True | P0 |
| text数组忽略 | {'data_type': 'text[]', ...} | True | P0 |
| 普通字段不忽略 | {'data_type': 'varchar', ...} | False | P0 |
| 自定义忽略类型 | {'data_type': 'xml', ...} | 根据配置 | P1 |

### 4.2 表结构迁移测试 (SchemaMigrator)

#### 4.2.1 SQL生成测试

| 测试用例 | 输入 | 预期结果 | 优先级 |
|---------|------|---------|--------|
| 基础表结构SQL生成 | 简单表定义 | 包含所有列和主键 | P0 |
| 忽略JSON字段 | 含JSON列的表 | SQL中不包含JSON列 | P0 |
| 忽略JSONB字段 | 含JSONB列的表 | SQL中不包含JSONB列 | P0 |
| 忽略数组字段 | 含数组列的表 | SQL中不包含数组列 | P0 |
| 主键处理 | 单列主键 | 正确生成PRIMARY KEY | P0 |
| 复合主键处理 | 多列主键 | 正确生成复合主键 | P1 |
| 自增主键处理 | serial类型主键 | 添加AUTO_INCREMENT | P1 |
| 可空列处理 | 允许NULL的列 | 不添加NOT NULL | P1 |
| 默认值处理 | 有默认值的列 | 包含DEFAULT子句 | P1 |
| 被忽略的主键列 | 主键列被忽略 | 主键约束正确生成 | P1 |

#### 4.2.2 迁移流程测试

| 测试用例 | 场景 | 预期结果 | 优先级 |
|---------|------|---------|--------|
| 单表迁移成功 | 表结构正确 | success列表包含表名 | P0 |
| 多表迁移成功 | 多个表 | 所有表都成功 | P0 |
| 迁移失败处理 | 创建表失败 | failed列表包含表名 | P1 |
| 异常处理 | 数据库异常 | 捕获异常并记录 | P1 |

### 4.3 数据迁移测试 (DataMigrator)

#### 4.3.1 单表数据迁移

| 测试用例 | 场景 | 预期结果 | 优先级 |
|---------|------|---------|--------|
| 基础数据迁移 | 100条记录 | 成功迁移100条 | P0 |
| 空表迁移 | 0条记录 | 跳过迁移 | P0 |
| 大数据量迁移 | 100000条记录 | 分批正确迁移 | P0 |
| 包含NULL值 | 字段含NULL | 正确处理NULL | P1 |
| 包含特殊字符 | 中文/特殊符号 | 正确插入 | P1 |
| 包含二进制数据 | bytea类型 | 正确转换 | P2 |
| 插入失败重试 | 模拟插入失败 | 执行重试逻辑 | P1 |
| 超过最大重试次数 | 持续失败 | 放弃并记录错误 | P1 |

#### 4.3.2 多表数据迁移

| 测试用例 | 场景 | 预期结果 | 优先级 |
|---------|------|---------|--------|
| 多表顺序迁移 | 5个表 | 全部成功 | P0 |
| 部分表迁移失败 | 1个表失败 | 正确记录partial | P1 |
| 忽略字段处理 | 包含忽略字段 | 字段被排除 | P1 |
| 并发迁移控制 | 配置并发数 | 不超过配置数 | P2 |

#### 4.3.3 批量插入测试

| 测试用例 | 场景 | 预期结果 | 优先级 |
|---------|------|---------|--------|
| 小批量插入 | 100条/批 | 正确分批 | P0 |
| 大批量插入 | 10000条/批 | 正确分批 | P1 |
| 批量大小边界 | batch_size=1 | 正常工作 | P1 |
| 空数据插入 | 空列表 | 返回0 | P1 |

### 4.4 数据验证测试 (DataValidator)

#### 4.4.1 记录数验证

| 测试用例 | 场景 | 预期结果 | 优先级 |
|---------|------|---------|--------|
| 记录数匹配 | PG和OB记录数相同 | matched=True | P0 |
| 记录数不匹配 | PG和OB记录数不同 | matched=False | P0 |
| 空表验证 | 0条记录 | 验证通过 | P0 |
| 大表验证 | 百万级记录 | 快速完成 | P1 |

#### 4.4.2 校验和验证

| 测试用例 | 场景 | 预期结果 | 优先级 |
|---------|------|---------|--------|
| 校验和匹配 | 数据完全一致 | matched=True | P0 |
| 校验和不匹配 | 数据有差异 | matched=False | P1 |
| 忽略字段验证 | 配置忽略字段 | 排除后计算 | P1 |
| 采样验证 | sample_size=1000 | 采样正确 | P1 |

### 4.5 数据库客户端测试

#### 4.5.1 PostgreSQL客户端

| 测试用例 | 场景 | 预期结果 | 优先级 |
|---------|------|---------|--------|
| 获取所有表 | 正常连接 | 返回表名列表 | P0 |
| 获取表结构 | 正常查询 | 返回完整结构 | P0 |
| 获取记录数 | COUNT查询 | 返回正确数量 | P0 |
| 分页获取数据 | OFFSET/LIMIT | 返回分页数据 | P0 |
| 排除字段查询 | exclude_columns | 不包含指定字段 | P1 |
| 异常处理 | 连接失败 | 抛出异常 | P1 |

#### 4.5.2 OceanBase客户端

| 测试用例 | 场景 | 预期结果 | 优先级 |
|---------|------|---------|--------|
| 创建表 | 正常建表 | 返回True | P0 |
| 创建表失败 | SQL错误 | 返回False | P1 |
| 批量插入 | 正常插入 | 返回插入数量 | P0 |
| 批量插入失败 | 异常情况 | 回滚并返回0 | P1 |
| 获取记录数 | COUNT查询 | 返回正确数量 | P0 |
| 清空表 | TRUNCATE | 返回True | P1 |

### 4.6 连接池测试 (ConnectionManager)

| 测试用例 | 场景 | 预期结果 | 优先级 |
|---------|------|---------|--------|
| PostgreSQL连接池初始化 | 正常配置 | 成功创建池 | P0 |
| OceanBase连接池初始化 | 正常配置 | 成功创建池 | P0 |
| 连接池耗尽处理 | 并发连接 | 等待可用连接 | P1 |
| 获取连接 | 正常获取 | 返回有效连接 | P0 |
| 释放连接 | 正常释放 | 连接归池 | P0 |
| 连接异常处理 | 连接失败 | 回滚并重试 | P1 |
| 关闭所有连接 | 程序结束 | 正确清理 | P1 |

### 4.7 重试机制测试 (RetryManager)

| 测试用例 | 场景 | 预期结果 | 优先级 |
|---------|------|---------|--------|
| 成功时无重试 | 一次成功 | 执行1次 | P0 |
| 单次失败重试 | 失败1次后成功 | 执行2次 | P0 |
| 多次失败重试 | 失败3次后成功 | 执行4次 | P0 |
| 达到最大重试次数 | 始终失败 | 抛出异常 | P0 |
| 重试延迟递增 | 失败重试 | 延迟逐渐增加 | P1 |
| 装饰器功能 | 使用@retry | 正确装饰函数 | P1 |

### 4.8 进度跟踪测试 (ProgressTracker)

| 测试用例 | 场景 | 预期结果 | 优先级 |
|---------|------|---------|--------|
| 初始化进度条 | 总数1000 | 显示进度条 | P0 |
| 更新进度 | 更新100 | 进度增加100 | P0 |
| 设置描述 | 更新描述 | 正确显示 | P1 |
| 关闭进度条 | 处理完成 | 正确清理 | P0 |
| 多进度条并发 | 多个Tracker | 独立显示 | P2 |

## 5. 测试数据准备

### 5.1 PostgreSQL测试数据

```sql
-- 创建测试表
CREATE TABLE test_users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(100) NOT NULL,
    email VARCHAR(255),
    age INTEGER,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT NOW(),
    profile_data JSONB,
    tags TEXT[]
);

CREATE TABLE test_orders (
    order_id BIGSERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES test_users(id),
    total_amount DECIMAL(10,2),
    order_date DATE,
    status VARCHAR(50) DEFAULT 'pending'
);

-- 插入测试数据
INSERT INTO test_users (username, email, age, is_active) VALUES
('user1', 'user1@example.com', 25, true),
('user2', 'user2@example.com', 30, false),
('user3', 'user3@example.com', 35, true);

INSERT INTO test_orders (user_id, total_amount, order_date) VALUES
(1, 99.99, '2024-01-15'),
(1, 149.99, '2024-01-16'),
(2, 199.99, '2024-01-17');
```

### 5.2 预期OceanBase表结构

```sql
CREATE TABLE IF NOT EXISTS test_users (
    id INT NOT NULL AUTO_INCREMENT,
    username VARCHAR(100) NOT NULL,
    email VARCHAR(255),
    age INT,
    is_active TINYINT(1) DEFAULT 1,
    created_at TIMESTAMP,
    PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS test_orders (
    order_id BIGINT NOT NULL AUTO_INCREMENT,
    user_id INT,
    total_amount DECIMAL(10,2),
    order_date DATE,
    status VARCHAR(50) DEFAULT 'pending',
    PRIMARY KEY (order_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

## 6. 性能测试

### 6.1 大数据量测试

| 测试项 | 数据量 | 预期指标 |
|-------|-------|---------|
| 100万记录迁移 | 1,000,000 | 完成时间 < 10分钟 |
| 100万记录验证 | 1,000,000 | 完成时间 < 5分钟 |
| 内存占用 | 100万记录 | < 500MB |
| 连接池稳定性 | 持续运行24小时 | 无连接泄漏 |

### 6.2 并发测试

| 测试项 | 并发数 | 预期指标 |
|-------|-------|---------|
| 多表并发迁移 | 4 | 正确协调 |
| 连接池并发获取 | 20 | 无死锁 |
| 批量插入并发 | 10 | 正确分批 |

## 7. 错误处理测试

### 7.1 网络异常

| 测试场景 | 预期行为 |
|---------|---------|
| PostgreSQL连接中断 | 重试后记录错误 |
| OceanBase连接中断 | 重试后记录错误 |
| 网络闪断 | 自动恢复 |

### 7.2 数据异常

| 测试场景 | 预期行为 |
|---------|---------|
| 字符编码错误 | 正确处理UTF-8 |
| 超长字符串 | 截断或报错 |
| 违反约束 | 回滚并记录 |
| 事务冲突 | 重试处理 |

## 8. 测试执行

### 8.1 运行所有测试

```bash
# 运行所有测试
pytest tests/ -v --tb=short

# 生成覆盖率报告
pytest tests/ --cov=src --cov-report=html

# 运行特定测试文件
pytest tests/test_converter.py -v

# 运行特定测试函数
pytest tests/test_converter.py::test_convert_boolean -v

# 运行带有 fixture 的测试
pytest tests/ --setup-show
```

### 8.2 测试配置

```python
# pytest.ini
[pytest]
testpaths = tests
python_files = test_*.py
python_classes = Test*
python_functions = test_*
addopts = -v --tb=short
filterwarnings =
    ignore::DeprecationWarning
```

## 9. 测试报告

### 9.1 报告内容

- 测试用例执行统计
- 失败用例详情
- 代码覆盖率分析
- 性能测试结果
- 错误日志汇总

### 9.2 生成报告

```bash
# 生成HTML报告
pytest tests/ --html=test_report.html

# 生成JUnit XML报告
pytest tests/ --junitxml=junit_report.xml

# 生成Allure报告
pytest tests/ --alluredir=allure_results
```

## 10. 验收标准

### 10.1 功能验收

- [ ] 所有P0测试用例通过
- [ ] 所有P1测试用例通过率 > 95%
- [ ] 数据类型转换100%准确
- [ ] 字段忽略功能100%准确
- [ ] 记录数验证100%通过

### 10.2 性能验收

- [ ] 单表迁移性能满足需求
- [ ] 连接池稳定无泄漏
- [ ] 内存使用在合理范围内

### 10.3 稳定性验收

- [ ] 24小时持续运行测试通过
- [ ] 异常恢复机制正常工作
- [ ] 错误日志清晰可追溯
