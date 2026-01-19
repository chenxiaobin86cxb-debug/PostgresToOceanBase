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

from src.migration.converter import TypeConverter
from src.migration.schema import SchemaMigrator


def load_config(config_file: str) -> Dict:
    """加载配置文件"""
    with open(config_file, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    logger.info(f"配置文件加载成功: {config_file}")
    return config


def _rows_to_dicts(cursor, rows) -> List[Dict]:
    columns = [desc[0] for desc in cursor.description]
    return [dict(zip(columns, row)) for row in rows]


def get_tables(cursor, schema: str) -> List[str]:
    cursor.execute(
        """
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = %s
        ORDER BY tablename
        """,
        (schema,)
    )
    return [row[0] for row in cursor.fetchall()]


def get_table_schema(cursor, table_name: str, schema: str) -> Dict:
    cursor.execute(
        """
        SELECT
            column_name,
            data_type,
            udt_name,
            character_maximum_length,
            numeric_precision,
            numeric_scale,
            is_nullable,
            column_default
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
        """,
        (schema, table_name)
    )
    columns = _rows_to_dicts(cursor, cursor.fetchall())

    cursor.execute(
        """
        SELECT a.attname
        FROM pg_index i
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = %s::regclass AND i.indisprimary
        """,
        (f"{schema}.{table_name}",)
    )
    primary_keys = [row[0] for row in cursor.fetchall()]

    return {
        'table_name': table_name,
        'columns': columns,
        'primary_keys': primary_keys
    }


def get_table_indexes(cursor, table_name: str, schema: str) -> List[Dict]:
    cursor.execute(
        """
        SELECT
            i.relname AS index_name,
            ix.indisunique AS is_unique,
            ord.ordinality AS position,
            a.attname AS column_name
        FROM pg_index ix
        JOIN pg_class t ON t.oid = ix.indrelid
        JOIN pg_class i ON i.oid = ix.indexrelid
        JOIN pg_namespace ns ON ns.oid = t.relnamespace
        JOIN unnest(ix.indkey) WITH ORDINALITY AS ord(attnum, ordinality)
          ON ord.attnum > 0
        JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ord.attnum
        WHERE ns.nspname = %s
          AND t.relname = %s
          AND NOT ix.indisprimary
        ORDER BY index_name, position
        """,
        (schema, table_name)
    )
    rows = cursor.fetchall()

    indexes_map = {}
    for index_name, is_unique, _, column_name in rows:
        if index_name not in indexes_map:
            indexes_map[index_name] = {
                'index_name': index_name,
                'is_unique': is_unique,
                'columns': []
            }
        indexes_map[index_name]['columns'].append(column_name)

    return list(indexes_map.values())


def write_sql(file_path: Path, statements: List[str], append: bool = False) -> None:
    file_path.parent.mkdir(parents=True, exist_ok=True)
    mode = 'a' if append else 'w'
    with file_path.open(mode, encoding='utf-8') as f:
        for statement in statements:
            stmt = statement.rstrip()
            if not stmt.endswith(';'):
                stmt += ';'
            f.write(stmt + '\n')


def main() -> None:
    parser = argparse.ArgumentParser(description='导出 OceanBase 表结构 SQL（批量执行）')
    parser.add_argument('--config', default='config/config.yaml', help='配置文件路径')
    parser.add_argument('--output-file', default='export/schema.sql', help='输出 SQL 文件')
    parser.add_argument('--index-file', default=None, help='索引 SQL 输出文件（可选）')
    parser.add_argument('--schema', default=None, help='源 schema')
    parser.add_argument('--tables', default=None, help='指定表（逗号分隔）')
    parser.add_argument('--exclude-tables', default=None, help='排除表（逗号分隔）')
    parser.add_argument('--include-indexes', action='store_true', help='包含索引语句')
    args = parser.parse_args()

    config = load_config(args.config)
    source_config = config.get('source', {})
    migration_config = config.get('migration', {})
    schema_config = migration_config.get('schema', {})

    schema = args.schema or source_config.get('schema', 'public')
    ignore_types = schema_config.get('ignore_types', ['json', 'jsonb', 'array'])

    include_tables = schema_config.get('include_tables', [])
    exclude_tables = schema_config.get('exclude_tables', [])

    if args.tables:
        include_tables = [t.strip() for t in args.tables.split(',') if t.strip()]
    if args.exclude_tables:
        exclude_tables = [t.strip() for t in args.exclude_tables.split(',') if t.strip()]

    conn = psycopg2.connect(
        host=source_config.get('host'),
        port=source_config.get('port'),
        database=source_config.get('database'),
        user=source_config.get('user'),
        password=source_config.get('password')
    )

    create_statements: List[str] = []
    index_statements: List[str] = []

    try:
        with conn.cursor() as cursor:
            all_tables = get_tables(cursor, schema)
            if include_tables:
                tables = [t for t in all_tables if t in include_tables]
            else:
                tables = [t for t in all_tables if t not in exclude_tables]

            logger.info(f"需要导出的表数量: {len(tables)}")
            converter = TypeConverter('config/type_mapping.yaml')
            migrator = SchemaMigrator(None, None, converter)

            for table_name in tables:
                table_schema = get_table_schema(cursor, table_name, schema)
                create_sql, ignored_columns = migrator.generate_create_table_sql(
                    table_schema, ignore_types
                )
                create_statements.append(create_sql)

                if args.include_indexes:
                    indexes = get_table_indexes(cursor, table_name, schema)
                    for index_info in indexes:
                        index_columns = index_info['columns']
                        if any(col in ignored_columns for col in index_columns):
                            logger.warning(
                                f"索引包含忽略字段，跳过: {table_name}."
                                f"{index_info['index_name']}"
                            )
                            continue
                        index_statements.append(
                            migrator._generate_create_index_sql(
                                table_name,
                                index_info['index_name'],
                                index_columns,
                                index_info['is_unique']
                            )
                        )
    finally:
        conn.close()

    output_file = Path(args.output_file)
    write_sql(output_file, create_statements)
    logger.info(f"表结构 SQL 已输出: {output_file}")

    if args.include_indexes:
        if args.index_file:
            index_file = Path(args.index_file)
            write_sql(index_file, index_statements)
            logger.info(f"索引 SQL 已输出: {index_file}")
        else:
            write_sql(output_file, index_statements, append=True)
            logger.info("索引 SQL 已追加到表结构文件")


if __name__ == '__main__':
    main()
