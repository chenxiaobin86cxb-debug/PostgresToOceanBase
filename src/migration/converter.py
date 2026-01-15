import yaml
from typing import Dict, Any, List
from loguru import logger


class TypeConverter:
    """数据类型转换器"""

    def __init__(self, mapping_file: str = 'config/type_mapping.yaml'):
        with open(mapping_file, 'r', encoding='utf-8') as f:
            self.mapping = yaml.safe_load(f)
        logger.info(f"类型映射配置加载成功: {mapping_file}")

    def convert_column_type(self, postgres_type: str, column_info: Dict) -> str:
        """转换列类型"""
        type_mapping = self.mapping.get('postgres_to_mysql', {})

        base_type = postgres_type.lower()
        type_aliases = {
            'character varying': 'varchar',
            'character': 'char',
            'timestamp with time zone': 'timestamptz',
            'timestamp without time zone': 'timestamp'
        }
        base_type = type_aliases.get(base_type, base_type)

        # 处理带有精度的类型
        if base_type in ['decimal', 'numeric']:
            precision = column_info.get('numeric_precision')
            scale = column_info.get('numeric_scale')
            target_type = type_mapping.get('decimal', 'DECIMAL({precision},{scale})')
            return target_type.format(precision=precision or 10, scale=scale or 0)

        # 处理字符串类型
        elif base_type in ['char', 'varchar']:
            length = column_info.get('character_maximum_length')
            if base_type == 'varchar' and length is None:
                return type_mapping.get('text', 'TEXT')
            if base_type == 'char' and length is None:
                length = 1
            target_type = type_mapping.get(base_type, f'{base_type.upper()}')
            return target_type.format(length=length or 255)

        # 直接映射
        else:
            return type_mapping.get(base_type, postgres_type.upper())

    def convert_value(self, value: Any, postgres_type: str) -> Any:
        """转换数据值"""
        if value is None:
            return None

        # PostgreSQL 类型转换
        base_type = postgres_type.lower()

        # 布尔值转换
        if base_type == 'boolean':
            return 1 if value else 0

        # UUID 转换
        elif base_type == 'uuid':
            return str(value)

        # 时间戳转换（timestamptz 转为 timestamp）
        elif base_type in ['timestamp', 'timestamptz']:
            return value  # psycopg2 会自动转换

        return value

    def should_ignore_column(self, column_info: Dict, ignore_types: List[str]) -> bool:
        """判断是否应该忽略该列"""
        data_type = column_info.get('data_type', '').lower()
        udt_name = column_info.get('udt_name', '').lower()

        # 检查是否在忽略列表中
        for ignore_type in ignore_types:
            if ignore_type in data_type:
                return True
            if ignore_type in udt_name:
                return True
            if udt_name.startswith('_'):
                return True

        return False
