import argparse
import os
from pathlib import Path
import re
import sys
from typing import Dict, List, Tuple

import csv
import psycopg2
import yaml
from loguru import logger

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
os.environ.setdefault('PYTHONPATH', str(PROJECT_ROOT))

from src.migration.converter import TypeConverter


HEX_RE = re.compile(r'^[0-9a-fA-F]*$')


def load_config(config_file: str) -> Dict:
    """加载配置文件"""
    with open(config_file, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    logger.info(f"配置文件加载成功: {config_file}")
    return config


def _fetch_columns(cursor, schema: str, table: str) -> List[Dict]:
    cursor.execute(
        """
        SELECT
            column_name,
            data_type,
            udt_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
        """,
        (schema, table)
    )
    columns = [desc[0] for desc in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def _export_column_meta(columns: List[Dict], ignore_types: List[str]) -> List[Tuple[str, str]]:
    converter = TypeConverter('config/type_mapping.yaml')
    export_columns = []
    for col in columns:
        if converter.should_ignore_column(col, ignore_types):
            continue
        export_columns.append((col['column_name'], col['data_type'].lower()))
    return export_columns


def validate_hex_file(
    csv_path: Path,
    export_columns: List[Tuple[str, str]],
    delimiter: str,
    quote_char: str,
    escape_char: str,
    null_string: str,
    max_errors: int
) -> int:
    total_rows = 0
    invalid_rows = 0

    bytea_indexes = [
        idx for idx, (_, dtype) in enumerate(export_columns) if dtype == 'bytea'
    ]
    if not bytea_indexes:
        logger.info("当前表无 bytea 字段，无需校验十六进制")
        return 0

    with csv_path.open('r', encoding='utf-8', newline='') as f:
        reader = csv.reader(
            f,
            delimiter=delimiter,
            quotechar=quote_char,
            escapechar=escape_char
        )
        for row_idx, row in enumerate(reader, start=1):
            total_rows += 1
            if len(row) != len(export_columns):
                logger.error(
                    f"列数不匹配: line {row_idx}, got {len(row)}, "
                    f"expected {len(export_columns)}"
                )
                invalid_rows += 1
                if invalid_rows >= max_errors:
                    break
                continue

            def is_null_value(value: str) -> bool:
                if value == '' or value == null_string:
                    return True
                if escape_char and null_string.startswith(escape_char):
                    if value == null_string[len(escape_char):]:
                        return True
                return False

            for col_idx in bytea_indexes:
                value = row[col_idx]
                if is_null_value(value):
                    continue
                if value.startswith('\\x'):
                    logger.error(
                        f"bytea 前缀不允许: line {row_idx}, column "
                        f"{export_columns[col_idx][0]}"
                    )
                    invalid_rows += 1
                    break
                if len(value) % 2 != 0 or not HEX_RE.match(value):
                    logger.error(
                        f"无效十六进制: line {row_idx}, column "
                        f"{export_columns[col_idx][0]}"
                    )
                    invalid_rows += 1
                    break

            if invalid_rows >= max_errors:
                break

    logger.info(f"校验完成: 总行数 {total_rows}, 异常行数 {invalid_rows}")
    return invalid_rows


def main() -> None:
    parser = argparse.ArgumentParser(description='校验导出 CSV 中 bytea 十六进制字段')
    parser.add_argument('--config', default='config/config.yaml', help='配置文件路径')
    parser.add_argument('--schema', default=None, help='源 schema')
    parser.add_argument('--table', required=True, help='表名')
    parser.add_argument('--file', required=True, help='CSV 文件路径')
    parser.add_argument('--delimiter', default=',', help='字段分隔符')
    parser.add_argument('--quote', default='"', help='引号字符')
    parser.add_argument('--escape', default='\\', help='转义字符')
    parser.add_argument('--null-string', default='\\N', help='NULL 替代字符')
    parser.add_argument('--max-errors', type=int, default=20, help='最大错误数')
    args = parser.parse_args()

    config = load_config(args.config)
    source_config = config.get('source', {})
    migration_config = config.get('migration', {})
    schema_config = migration_config.get('schema', {})
    ignore_types = schema_config.get('ignore_types', ['json', 'jsonb', 'array'])

    schema = args.schema or source_config.get('schema', 'public')
    csv_path = Path(args.file)

    conn = psycopg2.connect(
        host=source_config.get('host'),
        port=source_config.get('port'),
        database=source_config.get('database'),
        user=source_config.get('user'),
        password=source_config.get('password')
    )

    try:
        with conn.cursor() as cursor:
            columns = _fetch_columns(cursor, schema, args.table)
            export_columns = _export_column_meta(columns, ignore_types)
    finally:
        conn.close()

    validate_hex_file(
        csv_path,
        export_columns,
        args.delimiter,
        args.quote,
        args.escape,
        args.null_string,
        args.max_errors
    )


if __name__ == '__main__':
    main()
