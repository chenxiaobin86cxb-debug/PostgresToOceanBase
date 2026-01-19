import argparse
import os
from pathlib import Path
import sys
from typing import Dict, List, Sequence, Tuple

import psycopg2
import yaml
from loguru import logger

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
os.environ.setdefault('PYTHONPATH', str(PROJECT_ROOT))

from src.migration.converter import TypeConverter


def load_config(config_file: str) -> Dict:
    """加载配置文件"""
    with open(config_file, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    logger.info(f"配置文件加载成功: {config_file}")
    return config


def _quote_pg_identifier(name: str) -> str:
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def _pg_escape_literal(value: str) -> str:
    escaped = value.replace('\\', '\\\\').replace("'", "''")
    return f"E'{escaped}'"


def _build_select_columns(columns: Sequence[Dict], ignore_types: List[str]) -> Tuple[List[str], List[str]]:
    converter = TypeConverter('config/type_mapping.yaml')
    select_exprs = []
    column_names = []

    for col in columns:
        if converter.should_ignore_column(col, ignore_types):
            continue

        col_name = col['column_name']
        data_type = col['data_type'].lower()
        quoted_col = _quote_pg_identifier(col_name)

        if data_type == 'boolean':
            expr = f"CASE WHEN {quoted_col} THEN 1 ELSE 0 END AS {quoted_col}"
        elif data_type in {'timestamp with time zone', 'timestamp without time zone'}:
            expr = (
                f"to_char({quoted_col}, 'YYYY-MM-DD HH24:MI:SS') AS {quoted_col}"
            )
        elif data_type == 'date':
            expr = f"to_char({quoted_col}, 'YYYY-MM-DD') AS {quoted_col}"
        elif data_type == 'bytea':
            expr = f"encode({quoted_col}, 'hex') AS {quoted_col}"
        else:
            expr = quoted_col

        select_exprs.append(expr)
        column_names.append(col_name)

    return select_exprs, column_names


def _rows_to_dicts(cursor, rows) -> List[Dict]:
    columns = [desc[0] for desc in cursor.description]
    return [dict(zip(columns, row)) for row in rows]


def _fetch_columns(cursor, schema: str, table: str) -> List[Dict]:
    cursor.execute(
        """
        SELECT
            column_name,
            data_type,
            udt_name,
            character_maximum_length
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
        """,
        (schema, table)
    )
    return _rows_to_dicts(cursor, cursor.fetchall())


def _get_tables(cursor, schema: str) -> List[str]:
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


def export_table(
    cursor,
    schema: str,
    table: str,
    output_path: Path,
    ignore_types: List[str],
    delimiter: str,
    null_string: str,
    quote_char: str,
    escape_char: str
) -> bool:
    columns = _fetch_columns(cursor, schema, table)
    select_exprs, column_names = _build_select_columns(columns, ignore_types)

    if not select_exprs:
        logger.warning(f"表 {table} 无可导出的字段，跳过")
        return False

    select_sql = (
        f"SELECT {', '.join(select_exprs)} FROM "
        f"{_quote_pg_identifier(schema)}.{_quote_pg_identifier(table)}"
    )
    delimiter_literal = _pg_escape_literal(delimiter)
    null_literal = _pg_escape_literal(null_string)
    quote_literal = _pg_escape_literal(quote_char)
    escape_literal = _pg_escape_literal(escape_char)
    copy_sql = (
        "COPY ("
        f"{select_sql}"
        ") TO STDOUT WITH (FORMAT csv, "
        f"DELIMITER {delimiter_literal}, NULL {null_literal}, "
        f"QUOTE {quote_literal}, ESCAPE {escape_literal}, HEADER false)"
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open('w', encoding='utf-8', newline='') as f:
        cursor.copy_expert(copy_sql, f)

    logger.info(f"导出完成: {table} -> {output_path} (columns={len(column_names)})")
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description='导出 PostgreSQL 数据为 CSV（obloader 格式）')
    parser.add_argument('--config', default='config/config.yaml', help='配置文件路径')
    parser.add_argument('--output-dir', default='export', help='导出目录')
    parser.add_argument('--schema', default=None, help='源 schema')
    parser.add_argument('--tables', default=None, help='指定表（逗号分隔）')
    parser.add_argument('--exclude-tables', default=None, help='排除表（逗号分隔）')
    parser.add_argument('--suffix', default='.csv', help='导出文件后缀')
    parser.add_argument('--delimiter', default=',', help='字段分隔符（默认逗号）')
    parser.add_argument('--null-string', default='\\N', help='NULL 替代字符')
    parser.add_argument('--quote', default='"', help='引号字符')
    parser.add_argument('--escape', default='\\', help='转义字符')
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

    try:
        with conn.cursor() as cursor:
            all_tables = _get_tables(cursor, schema)

            if include_tables:
                tables = [t for t in all_tables if t in include_tables]
            else:
                tables = [t for t in all_tables if t not in exclude_tables]

            logger.info(f"需要导出的表数量: {len(tables)}")
            output_dir = Path(args.output_dir)

            exported = 0
            skipped = 0
            for table in tables:
                output_path = output_dir / f"{table}{args.suffix}"
                success = export_table(
                    cursor,
                    schema,
                    table,
                    output_path,
                    ignore_types,
                    args.delimiter,
                    args.null_string,
                    args.quote,
                    args.escape
                )
                if success:
                    exported += 1
                else:
                    skipped += 1

            logger.info(f"导出完成: 成功 {exported}, 跳过 {skipped}")
    finally:
        conn.close()


if __name__ == '__main__':
    main()
