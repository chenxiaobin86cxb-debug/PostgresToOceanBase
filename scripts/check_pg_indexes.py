import argparse
import os
from pathlib import Path
import sys
from typing import Dict, List, Tuple

import psycopg2
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


def _rows_to_dicts(cursor, rows) -> List[Dict]:
    columns = [desc[0] for desc in cursor.description]
    return [dict(zip(columns, row)) for row in rows]


def main() -> None:
    parser = argparse.ArgumentParser(description='检查 PostgreSQL 索引类型')
    parser.add_argument('--config', default='config/config.yaml', help='配置文件路径')
    parser.add_argument('--schema', default=None, help='源 schema')
    parser.add_argument('--table', default=None, help='指定表')
    args = parser.parse_args()

    config = load_config(args.config)
    source_config = config.get('source', {})
    schema = args.schema or source_config.get('schema', 'public')

    conn = psycopg2.connect(
        host=source_config.get('host'),
        port=source_config.get('port'),
        database=source_config.get('database'),
        user=source_config.get('user'),
        password=source_config.get('password')
    )

    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                  ns.nspname AS schema,
                  t.relname AS table_name,
                  i.relname AS index_name,
                  ix.indisunique AS is_unique,
                  ix.indisprimary AS is_primary,
                  ix.indpred IS NOT NULL AS is_partial,
                  0 = ANY(ix.indkey) AS has_expression,
                  pg_get_expr(ix.indpred, ix.indrelid) AS predicate,
                  pg_get_indexdef(i.oid) AS index_def
                FROM pg_index ix
                JOIN pg_class t ON t.oid = ix.indrelid
                JOIN pg_class i ON i.oid = ix.indexrelid
                JOIN pg_namespace ns ON ns.oid = t.relnamespace
                WHERE ns.nspname = %s
                  AND t.relkind = 'r'
                  AND (%s IS NULL OR t.relname = %s)
                ORDER BY schema, table_name, index_name
                """,
                (schema, args.table, args.table)
            )
            rows = _rows_to_dicts(cursor, cursor.fetchall())

        primary = sum(1 for r in rows if r['is_primary'])
        unique = sum(1 for r in rows if r['is_unique'] and not r['is_primary'])
        normal = sum(1 for r in rows if not r['is_unique'] and not r['is_primary'])
        expression_indexes = [r for r in rows if r['has_expression']]
        partial_indexes = [r for r in rows if r['is_partial']]

        logger.info(
            f"Schema {schema} 索引统计: primary={primary}, "
            f"unique={unique}, normal={normal}"
        )
        logger.info(f"Expression indexes: {len(expression_indexes)}")
        logger.info(f"Partial indexes: {len(partial_indexes)}")

        if expression_indexes:
            logger.warning("Expression index samples:")
            for row in expression_indexes[:10]:
                logger.warning(
                    f"  {row['table_name']}.{row['index_name']} -> {row['index_def']}"
                )

        if partial_indexes:
            logger.warning("Partial index samples:")
            for row in partial_indexes[:10]:
                logger.warning(
                    f"  {row['table_name']}.{row['index_name']} predicate={row['predicate']}"
                )
    finally:
        conn.close()


if __name__ == '__main__':
    main()
