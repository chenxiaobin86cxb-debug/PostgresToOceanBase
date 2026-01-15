import hashlib
from typing import List, Dict
from loguru import logger


class DataValidator:
    """数据验证器"""

    def __init__(self, pg_client, ob_client, config: Dict):
        self.pg_client = pg_client
        self.ob_client = ob_client
        self.config = config
        migration_config = config.get('migration', {})
        self.validation_config = migration_config.get(
            'validation', config.get('validation', {})
        )

    def validate_count(self, table_name: str, schema: str = 'public') -> Dict:
        """验证记录数"""
        pg_count = self.pg_client.get_table_count(table_name, schema)
        ob_count = self.ob_client.get_table_count(table_name)

        matched = pg_count == ob_count

        result = {
            'table_name': table_name,
            'pg_count': pg_count,
            'ob_count': ob_count,
            'matched': matched
        }

        if matched:
            logger.info(f"记录数验证通过: {table_name} ({pg_count})")
        else:
            logger.error(
                f"记录数验证失败: {table_name} "
                f"(PG: {pg_count}, OB: {ob_count})"
            )

        return result

    def validate_checksum(self, table_name: str, schema: str = 'public',
                         sample_size: int = 1000,
                         ignore_columns: List[str] = None) -> Dict:
        """验证数据校验和"""
        if ignore_columns is None:
            ignore_columns = []

        # PostgreSQL 采样
        pg_data = self.pg_client.get_table_data(
            table_name, schema, 0, sample_size, ignore_columns
        )
        pg_checksum = self._calculate_checksum(pg_data)

        # OceanBase 采样
        ob_data = self.ob_client.get_table_data(
            table_name, offset=0, limit=sample_size, exclude_columns=ignore_columns
        )
        ob_checksum = self._calculate_checksum(ob_data)

        matched = pg_checksum == ob_checksum

        result = {
            'table_name': table_name,
            'pg_checksum': pg_checksum,
            'ob_checksum': ob_checksum,
            'matched': matched
        }

        if matched:
            logger.info(f"校验和验证通过: {table_name}")
        else:
            logger.error(f"校验和验证失败: {table_name}")

        return result

    def _calculate_checksum(self, data: List[Dict]) -> str:
        """计算数据校验和"""
        # 排序数据
        sorted_data = sorted(data, key=lambda x: str(x))

        # 转换为字符串
        data_str = str(sorted_data)

        # 计算 MD5
        return hashlib.md5(data_str.encode()).hexdigest()

    def validate_all(self, tables: List[str], schema: str = 'public',
                    ignore_columns_map: Dict[str, List[str]] = None) -> Dict:
        """验证所有表"""
        results = {
            'count_validation': [],
            'checksum_validation': []
        }

        if ignore_columns_map is None:
            ignore_columns_map = {}

        check_count = self.validation_config.get('check_count', True)
        check_checksum = self.validation_config.get('check_checksum', True)
        sample_size = self.validation_config.get('sample_size', 1000)

        for table_name in tables:
            ignore_columns = ignore_columns_map.get(table_name, [])

            if check_count:
                result = self.validate_count(table_name, schema)
                results['count_validation'].append(result)

            if check_checksum:
                result = self.validate_checksum(
                    table_name, schema, sample_size, ignore_columns
                )
                results['checksum_validation'].append(result)

        return results
