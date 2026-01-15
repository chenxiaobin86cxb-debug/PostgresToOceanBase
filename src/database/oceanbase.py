from typing import List, Dict

import pymysql
from loguru import logger


class OceanBaseClient:
    """OceanBase MySQL 客户端"""

    def __init__(self, connection_manager):
        self.conn_mgr = connection_manager

    @staticmethod
    def _quote_identifier(name: str) -> str:
        """为 OceanBase/MySQL 标识符加反引号"""
        escaped = name.replace('`', '``')
        return f"`{escaped}`"

    def create_table(self, schema_sql: str) -> bool:
        """创建表"""
        with self.conn_mgr.get_target_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(schema_sql)
                conn.commit()
                logger.info(f"表创建成功: {schema_sql[:50]}...")
                return True
            except Exception as e:
                conn.rollback()
                logger.error(f"表创建失败: {e}")
                return False
            finally:
                cursor.close()

    def insert_batch(self, table_name: str, data: List[Dict],
                    batch_size: int = 1000) -> int:
        """批量插入数据"""
        if not data:
            return 0

        with self.conn_mgr.get_target_connection() as conn:
            cursor = conn.cursor()
            inserted = 0
            try:
                # 分批插入
                for i in range(0, len(data), batch_size):
                    batch = data[i:i + batch_size]
                    columns = list(batch[0].keys())
                    placeholders = ', '.join(['%s'] * len(columns))
                    columns_str = ', '.join(
                        self._quote_identifier(col) for col in columns
                    )
                    quoted_table = self._quote_identifier(table_name)

                    values = [[row[col] for col in columns] for row in batch]

                    cursor.executemany(
                        f"INSERT INTO {quoted_table} ({columns_str}) VALUES ({placeholders})",
                        values
                    )
                    conn.commit()
                    inserted += len(batch)

                logger.info(f"批量插入完成: {table_name}, 共 {inserted} 条")
                return inserted
            except Exception as e:
                conn.rollback()
                logger.error(f"批量插入失败: {table_name}, 错误: {e}")
                return 0
            finally:
                cursor.close()

    def get_table_count(self, table_name: str) -> int:
        """获取表记录数"""
        with self.conn_mgr.get_target_connection() as conn:
            cursor = conn.cursor()
            quoted_table = self._quote_identifier(table_name)
            cursor.execute(f"SELECT COUNT(*) FROM {quoted_table}")
            count = cursor.fetchone()[0]
            cursor.close()
            return count

    def get_table_data(self, table_name: str, offset: int = 0, limit: int = 1000,
                       exclude_columns: List[str] = None) -> List[Dict]:
        """获取表数据"""
        if exclude_columns is None:
            exclude_columns = []

        with self.conn_mgr.get_target_connection() as conn:
            cursor = conn.cursor(pymysql.cursors.DictCursor)
            try:
                # 构建 SELECT 语句，排除特定字段
                if exclude_columns:
                    cursor.execute("""
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_schema = DATABASE() AND table_name = %s
                        ORDER BY ordinal_position
                    """, (table_name,))
                    all_columns = [row['column_name'] for row in cursor.fetchall()]
                    select_columns = [
                        col for col in all_columns if col not in exclude_columns
                    ]
                    columns_str = ', '.join(
                        self._quote_identifier(col) for col in select_columns
                    )
                else:
                    columns_str = '*'

                quoted_table = self._quote_identifier(table_name)
                cursor.execute(
                    f"SELECT {columns_str} FROM {quoted_table} LIMIT %s OFFSET %s",
                    (limit, offset)
                )
                rows = [dict(row) for row in cursor.fetchall()]
                return rows
            finally:
                cursor.close()

    def truncate_table(self, table_name: str) -> bool:
        """清空表"""
        with self.conn_mgr.get_target_connection() as conn:
            cursor = conn.cursor()
            try:
                quoted_table = self._quote_identifier(table_name)
                cursor.execute(f"TRUNCATE TABLE {quoted_table}")
                conn.commit()
                logger.info(f"表清空成功: {table_name}")
                return True
            except Exception as e:
                conn.rollback()
                logger.error(f"表清空失败: {table_name}, 错误: {e}")
                return False
            finally:
                cursor.close()
