# PostgreSQL 到 OceanBase 迁移工具

这是一个用于将 PostgreSQL 数据库迁移到 OceanBase MySQL 租户的工具，支持全量数据迁移，自动忽略 JSON/JSONB 和数组类型字段。

## 功能特性

- ✅ 全量表结构迁移
- ✅ 全量数据迁移
- ✅ 自动类型转换（PostgreSQL → OceanBase MySQL）
- ✅ 自动忽略 JSON/JSONB 和数组类型字段
- ✅ 批量数据迁移和进度显示
- ✅ 数据一致性验证
- ✅ 断点续传功能
- ✅ 并发迁移支持
- ✅ 详细日志记录

## 安装

### 环境要求

- Python 3.8+
- PostgreSQL 11.x+
- OceanBase MySQL 租户

### 安装步骤

```bash
# 克隆项目
git clone <repository-url>
cd PostgresToOceanBase

# 创建虚拟环境
python3 -m venv venv
source venv/bin/activate  # Linux/Mac
# venv\Scripts\activate  # Windows

# 安装依赖
pip install -r requirements.txt
```

## 配置

### 1. 环境变量

复制 `.env` 文件并设置数据库密码：

```bash
cp .env .env.local
# 编辑 .env.local 设置密码
```

### 2. 配置文件

编辑 `config/config.yaml` 配置数据库连接信息：

```yaml
source:
  host: your_postgres_host
  port: 5432
  database: your_database
  user: migration_user
  password: ${POSTGRES_PASSWORD}

target:
  host: your_oceanbase_host
  port: 2881
  database: your_database
  user: migration_user@tenant
  password: ${OCEANBASE_PASSWORD}
```

## 使用方法

### 完整迁移

```bash
# 运行完整迁移（表结构 + 数据 + 验证）
python src/main.py --config config/config.yaml --validate
```

### 文本导出/导入（加速迁移）

当数据量较大时，可采用“PostgreSQL 导出文本 + OceanBase 导入工具（obloader）”方式加速迁移，并支持覆盖替换：

- 方案说明：`docs/text_import_migration.md`

### 仅迁移表结构

```bash
python src/main.py --config config/config.yaml --schema-only
```

### 仅迁移数据

```bash
python src/main.py --config config/config.yaml --data-only
```

### Docker 部署

```bash
# 构建镜像
docker build -t postgres2ob:latest .

# 运行容器
docker run -v $(pwd)/config:/app/config \
           -v $(pwd)/logs:/app/logs \
           -v $(pwd)/backup:/app/backup \
           --env-file .env \
           postgres2ob:latest
```

## 测试

```bash
# 运行所有测试
pytest tests/

# 运行特定测试
pytest tests/test_converter.py -v

# 生成覆盖率报告
pytest --cov=src --cov-report=html tests/
```

## 代码质量

```bash
# 代码风格检查
ruff check src/

# 自动修复
ruff check --fix src/

# 格式化代码
black src/ tests/

# 类型检查
mypy src/ --ignore-missing-imports
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
├── logs/                # 日志目录
├── backup/              # 备份目录
├── .env                 # 环境变量
├── .gitignore
├── requirements.txt
├── setup.py
└── README.md
```

## 注意事项

1. **特殊字段忽略**：JSON/JSONB 和数组类型字段会被自动忽略
2. **数据类型兼容性**：确保目标类型能容纳源数据
3. **字符集**：统一使用 UTF-8，避免乱码
4. **事务大小**：大事务可能导致超时，适当拆分
5. **网络带宽**：监控网络使用，避免拥塞
6. **磁盘空间**：预留足够的磁盘空间（2倍数据量）
7. **备份**：迁移前备份源数据库
8. **测试**：先在测试环境验证

## 许可证

MIT License

## 贡献

欢迎提交 Issue 和 Pull Request！

## 联系方式

如有问题，请通过以下方式联系：
- 邮箱：migration@example.com
- 项目地址：<repository-url>
