from typing import List, Dict, Any, Optional
import psycopg2.extras
from loguru import logger


class PostgreSQLClient:
    """PostgreSQL 客户端"""

    def __init__(self, connection_manager):
        self.conn_mgr = connection_manager

    def get_tables(self, schema: str = 'public') -> List[str]:
        """获取所有表名"""
        with self.conn_mgr.get_source_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT tablename
                FROM pg_tables
                WHERE schemaname = %s
                ORDER BY tablename
            """, (schema,))
            tables = [row[0] for row in cursor.fetchall()]
            cursor.close()
            return tables

    def get_table_schema(self, table_name: str, schema: str = 'public') -> Dict[str, Any]:
        """获取表结构"""
        with self.conn_mgr.get_source_connection() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

            # 获取列信息
            cursor.execute("""
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
            """, (schema, table_name))
            columns = [dict(row) for row in cursor.fetchall()]

            # 获取主键信息
            cursor.execute("""
                SELECT a.attname
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                WHERE i.indrelid = %s::regclass AND i.indisprimary
            """, (f"{schema}.{table_name}",))
            primary_keys = [row[0] for row in cursor.fetchall()]

            cursor.close()
            return {
                'table_name': table_name,
                'columns': columns,
                'primary_keys': primary_keys
            }

    def get_table_count(self, table_name: str, schema: str = 'public') -> int:
        """获取表记录数"""
        with self.conn_mgr.get_source_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {schema}.{table_name}")
            count = cursor.fetchone()[0]
            cursor.close()
            return count

    def get_table_data(self, table_name: str, schema: str = 'public',
                      offset: int = 0, limit: int = 1000,
                      exclude_columns: List[str] = None) -> List[Dict]:
        """获取表数据"""
        if exclude_columns is None:
            exclude_columns = []

        with self.conn_mgr.get_source_connection() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

            # 构建 SELECT 语句，排除特定字段
            if exclude_columns:
                # 获取所有列名
                cursor.execute("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                    ORDER BY ordinal_position
                """, (schema, table_name))
                all_columns = [row[0] for row in cursor.fetchall()]
                # 过滤排除的列
                select_columns = [col for col in all_columns if col not in exclude_columns]
                columns_str = ', '.join(select_columns)
            else:
                columns_str = '*'

            cursor.execute(f"""
                SELECT {columns_str} FROM {schema}.{table_name}
                ORDER BY (SELECT NULL)
                OFFSET %s LIMIT %s
            """, (offset, limit))
            rows = [dict(row) for row in cursor.fetchall()]
            cursor.close()
            return rows