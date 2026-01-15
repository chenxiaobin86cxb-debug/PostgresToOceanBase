from typing import List, Dict
from tqdm import tqdm
from loguru import logger
from src.migration.converter import TypeConverter


class DataMigrator:
    """数据迁移器"""

    def __init__(self, pg_client, ob_client, converter: TypeConverter,
                 config: Dict):
        self.pg_client = pg_client
        self.ob_client = ob_client
        self.converter = converter
        self.config = config
        migration_config = config.get('migration', {})
        self.data_config = migration_config.get('data', config.get('data', {}))
        self.error_config = migration_config.get('error', config.get('error', {}))

    @staticmethod
    def _normalize_postgres_type(data_type: str) -> str:
        """规范化 PostgreSQL 类型名称"""
        normalized = (data_type or '').lower()
        if normalized == 'timestamp with time zone':
            return 'timestamptz'
        if normalized == 'timestamp without time zone':
            return 'timestamp'
        return normalized

    def _get_column_type_map(self, table_name: str, schema: str,
                             ignore_columns: List[str]) -> Dict[str, str]:
        """获取列类型映射"""
        schema_info = self.pg_client.get_table_schema(table_name, schema)
        column_types = {}

        for column in schema_info.get('columns', []):
            col_name = column.get('column_name')
            if not col_name or col_name in ignore_columns:
                continue
            data_type = self._normalize_postgres_type(column.get('data_type', ''))
            column_types[col_name] = data_type or 'text'

        return column_types

    def migrate_table_data(self, table_name: str, schema: str = 'public',
                          ignore_columns: List[str] = None) -> Dict:
        """迁移单个表的数据"""
        if ignore_columns is None:
            ignore_columns = []

        batch_size = self.data_config.get('batch_size', 1000)
        chunk_size = self.data_config.get('chunk_size', 10000)
        max_retries = self.error_config.get('max_retries', 3)
        retry_delay = self.error_config.get('retry_delay', 5)

        # 获取总记录数
        total_count = self.pg_client.get_table_count(table_name, schema)
        logger.info(f"表 {table_name} 总记录数: {total_count}")

        if total_count == 0:
            logger.info(f"表 {table_name} 无数据，跳过")
            return {'table_name': table_name, 'status': 'skipped'}

        column_types = self._get_column_type_map(
            table_name, schema, ignore_columns
        )

        # 进度条
        pbar = tqdm(total=total_count, desc=f"Migrating {table_name}")

        migrated = 0
        failed = 0
        retry_count = 0

        while migrated < total_count:
            try:
                # 读取数据（排除忽略的字段）
                data = self.pg_client.get_table_data(
                    table_name, schema, migrated, chunk_size, ignore_columns
                )

                if not data:
                    break

                # 转换数据
                converted_data = []
                for row in data:
                    converted_row = {}
                    for col_name, value in row.items():
                        postgres_type = column_types.get(col_name) or 'text'
                        converted_value = self.converter.convert_value(
                            value, postgres_type
                        )
                        converted_row[col_name] = converted_value
                    converted_data.append(converted_row)

                # 批量插入
                inserted = self.ob_client.insert_batch(
                    table_name, converted_data, batch_size
                )

                if inserted == 0:
                    failed += len(converted_data)
                    logger.error(f"批量插入失败: {table_name}")

                    # 重试
                    if retry_count < max_retries:
                        retry_count += 1
                        logger.warning(f"重试 {retry_count}/{max_retries}...")
                        import time
                        time.sleep(retry_delay)
                        continue
                    else:
                        logger.error(f"达到最大重试次数，放弃: {table_name}")
                        break
                else:
                    migrated += inserted
                    retry_count = 0
                    pbar.update(inserted)

            except Exception as e:
                logger.error(f"数据迁移异常: {table_name}, 错误: {e}")
                failed += chunk_size
                if not self.error_config.get('continue_on_error', False):
                    break

        pbar.close()

        return {
            'table_name': table_name,
            'status': 'success' if failed == 0 else 'partial',
            'migrated': migrated,
            'failed': failed,
            'total': total_count
        }

    def migrate_all_data(self, tables: List[str], schema: str = 'public',
                        ignore_columns_map: Dict[str, List[str]] = None) -> Dict:
        """迁移所有表的数据"""
        if ignore_columns_map is None:
            ignore_columns_map = {}

        results = {
            'success': [],
            'failed': [],
            'partial': [],
            'skipped': []
        }

        for table_name in tables:
            ignore_columns = ignore_columns_map.get(table_name, [])
            result = self.migrate_table_data(table_name, schema, ignore_columns)

            if result['status'] == 'success':
                results['success'].append(result)
            elif result['status'] == 'partial':
                results['partial'].append(result)
            elif result['status'] == 'skipped':
                results['skipped'].append(result)
            else:
                results['failed'].append(result)

        return results
