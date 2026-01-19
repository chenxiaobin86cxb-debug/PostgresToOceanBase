import re
from typing import List, Dict, Tuple
from loguru import logger
from src.migration.converter import TypeConverter


class SchemaMigrator:
    """表结构迁移器"""

    def __init__(self, pg_client, ob_client, converter: TypeConverter):
        self.pg_client = pg_client
        self.ob_client = ob_client
        self.converter = converter

    @staticmethod
    def _quote_identifier(name: str) -> str:
        """为 OceanBase/MySQL 标识符加反引号"""
        escaped = name.replace('`', '``')
        return f"`{escaped}`"

    @staticmethod
    def _normalize_default(base_type: str, column_default: str) -> str:
        """规范化默认值表达式"""
        if not column_default:
            return ''

        expr = column_default.strip()
        if expr.startswith('(') and expr.endswith(')'):
            expr = expr[1:-1].strip()

        expr = re.sub(r"::[A-Za-z0-9_ ]+$", "", expr).strip()
        normalized = expr.lower()

        if base_type == 'boolean':
            if normalized in {'true', 't', '1'}:
                return '1'
            if normalized in {'false', 'f', '0'}:
                return '0'

        if normalized == 'now()':
            return 'CURRENT_TIMESTAMP'

        return expr

    def generate_create_table_sql(self, schema: Dict,
                                  ignore_types: List[str]) -> Tuple[str, List[str]]:
        """生成 CREATE TABLE SQL"""
        table_name = schema['table_name']
        columns = schema['columns']
        primary_keys = schema['primary_keys']

        # 过滤需要忽略的列
        filtered_columns = []
        ignored_columns = []

        for col in columns:
            if self.converter.should_ignore_column(col, ignore_types):
                ignored_columns.append(col['column_name'])
                logger.warning(f"忽略字段: {table_name}.{col['column_name']} (类型: {col['data_type']})")
            else:
                filtered_columns.append(col)

        # 生成列定义
        column_defs = []
        for col in filtered_columns:
            col_name = col['column_name']
            quoted_col_name = self._quote_identifier(col_name)
            postgres_type = col['data_type']
            base_type = postgres_type.lower()
            nullable = 'NOT NULL' if col['is_nullable'] == 'NO' else ''

            # 转换类型
            target_type = self.converter.convert_column_type(
                postgres_type, col
            )

            # 处理主键和自增
            is_primary = col_name in primary_keys
            column_default = col.get('column_default') or ''
            has_sequence_default = 'nextval(' in column_default.lower()
            is_serial = base_type in ['serial', 'bigserial'] or has_sequence_default

            # 对于 serial/bigserial 或 nextval 默认值，不添加 DEFAULT 子句
            if is_serial:
                default = ''
            else:
                normalized_default = self._normalize_default(base_type, column_default)
                default = f"DEFAULT {normalized_default}" if normalized_default else ''

            if is_primary and is_serial:
                target_type += ' AUTO_INCREMENT'

            column_def = f"    {quoted_col_name} {target_type} {nullable} {default}"
            column_defs.append(column_def.strip())

        # 生成主键约束（过滤掉被忽略的主键列）
        valid_primary_keys = [pk for pk in primary_keys if pk not in ignored_columns]
        primary_key_def = ''
        if valid_primary_keys:
            pk_columns = ', '.join(
                self._quote_identifier(pk) for pk in valid_primary_keys
            )
            primary_key_def = f",\n    PRIMARY KEY ({pk_columns})"

        # 组装 SQL
        sql = f"CREATE TABLE IF NOT EXISTS {self._quote_identifier(table_name)} (\n"
        sql += ',\n'.join(column_defs)
        sql += primary_key_def
        sql += "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"

        return sql, ignored_columns

    def _generate_create_index_sql(self, table_name: str, index_name: str,
                                   columns: List[str], is_unique: bool) -> str:
        quoted_table = self._quote_identifier(table_name)
        quoted_index = self._quote_identifier(index_name)
        columns_str = ', '.join(self._quote_identifier(col) for col in columns)
        unique = 'UNIQUE ' if is_unique else ''
        return f"CREATE {unique}INDEX {quoted_index} ON {quoted_table} ({columns_str});"

    def migrate_schema(self, tables: List[str], schema: str = 'public',
                      ignore_types: List[str] = None) -> Dict:
        """迁移表结构"""
        if ignore_types is None:
            ignore_types = ['json', 'jsonb', 'array']

        results = {
            'success': [],
            'failed': [],
            'ignored_columns': {},
            'indexes_failed': []
        }

        for table_name in tables:
            try:
                # 获取表结构
                pg_schema = self.pg_client.get_table_schema(table_name, schema)

                # 生成 CREATE TABLE SQL
                create_sql, ignored_columns = self.generate_create_table_sql(
                    pg_schema, ignore_types
                )
                results['ignored_columns'][table_name] = ignored_columns

                # 创建表
                success = self.ob_client.create_table(create_sql)

                if success:
                    results['success'].append(table_name)
                    logger.info(f"表结构迁移成功: {table_name}")

                    # 创建索引（不含主键）
                    indexes = self.pg_client.get_table_indexes(table_name, schema)
                    for index_info in indexes:
                        index_columns = index_info['columns']
                        if any(col in ignored_columns for col in index_columns):
                            logger.warning(
                                f"索引包含忽略字段，跳过: {table_name}."
                                f"{index_info['index_name']}"
                            )
                            continue

                        index_sql = self._generate_create_index_sql(
                            table_name,
                            index_info['index_name'],
                            index_columns,
                            index_info['is_unique']
                        )
                        index_success = self.ob_client.create_index(index_sql)
                        if not index_success:
                            results['indexes_failed'].append({
                                'table_name': table_name,
                                'index_name': index_info['index_name']
                            })
                else:
                    results['failed'].append(table_name)
                    logger.error(f"表结构迁移失败: {table_name}")
            except Exception as e:
                results['failed'].append(table_name)
                logger.error(f"表结构迁移异常: {table_name}, 错误: {e}")

        return results
