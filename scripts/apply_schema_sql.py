import argparse
import os
from pathlib import Path
import sys
from typing import Dict, List

import pymysql
import yaml
from loguru import logger

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
os.environ.setdefault('PYTHONPATH', str(PROJECT_ROOT))


def load_config(config_file: str) -> Dict:
    """加载配置文件"""
    with open(config_file, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    logger.info(f"配置文件加载成功: {config_file}")
    return config


def read_statements(sql_file: Path) -> List[str]:
    """按分号分割 SQL 语句"""
    content = sql_file.read_text(encoding='utf-8')
    statements: List[str] = []
    buffer: List[str] = []
    in_string = False
    escape = False

    for ch in content:
        if escape:
            buffer.append(ch)
            escape = False
            continue

        if ch == '\\\\':
            buffer.append(ch)
            escape = True
            continue

        if ch == "'":
            buffer.append(ch)
            in_string = not in_string
            continue

        if ch == ';' and not in_string:
            stmt = ''.join(buffer).strip()
            if stmt:
                statements.append(stmt)
            buffer = []
            continue

        buffer.append(ch)

    tail = ''.join(buffer).strip()
    if tail:
        statements.append(tail)
    return statements


def main() -> None:
    parser = argparse.ArgumentParser(description='批量执行 OceanBase 表结构 SQL')
    parser.add_argument('--config', default='config/config.yaml', help='配置文件路径')
    parser.add_argument('--sql-file', default='export/schema.sql', help='SQL 文件路径')
    parser.add_argument('--stop-on-error', action='store_true', help='遇错停止')
    parser.add_argument('--skip-duplicate-index', action='store_true', help='忽略重复索引错误')
    args = parser.parse_args()

    config = load_config(args.config)
    target_config = config.get('target', {})
    sql_file = Path(args.sql_file)

    if not sql_file.exists():
        raise FileNotFoundError(f"SQL 文件不存在: {sql_file}")

    conn = pymysql.connect(
        host=target_config.get('host'),
        port=int(target_config.get('port', 2881)),
        user=target_config.get('user'),
        password=target_config.get('password'),
        database=target_config.get('database'),
        charset='utf8mb4',
        autocommit=False
    )

    try:
        statements = read_statements(sql_file)
        logger.info(f"准备执行 SQL 数量: {len(statements)}")

        with conn.cursor() as cursor:
            for idx, statement in enumerate(statements, start=1):
                try:
                    cursor.execute(statement)
                    conn.commit()
                except Exception as e:
                    error_code = None
                    if hasattr(e, 'args') and e.args:
                        error_code = e.args[0]
                    conn.rollback()
                    if (args.skip_duplicate_index and error_code == 1061 and
                            statement.strip().upper().startswith('CREATE') and
                            ' INDEX ' in statement.upper()):
                        logger.warning(f"索引已存在，跳过: {statement[:200]}...")
                        continue

                    logger.error(f"执行失败 #{idx}: {e}")
                    logger.error(f"SQL: {statement[:200]}...")
                    if args.stop_on_error:
                        raise
        logger.info("SQL 执行完成")
    finally:
        conn.close()


if __name__ == '__main__':
    main()
