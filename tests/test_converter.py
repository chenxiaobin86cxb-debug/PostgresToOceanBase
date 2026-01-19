import pytest
from src.migration.converter import TypeConverter


def test_convert_boolean():
    converter = TypeConverter('config/type_mapping.yaml')
    assert converter.convert_value(True, 'boolean') == 1
    assert converter.convert_value(False, 'boolean') == 0


def test_convert_timestamp():
    converter = TypeConverter('config/type_mapping.yaml')
    import datetime
    dt = datetime.datetime(2024, 1, 1, 12, 0, 0)
    result = converter.convert_value(dt, 'timestamp')
    assert result is not None


def test_should_ignore_column():
    converter = TypeConverter('config/type_mapping.yaml')

    # JSON 字段应该被忽略
    json_column = {'data_type': 'json', 'udt_name': 'json'}
    assert converter.should_ignore_column(json_column, ['json']) == True

    # JSONB 字段应该被忽略
    jsonb_column = {'data_type': 'jsonb', 'udt_name': 'jsonb'}
    assert converter.should_ignore_column(jsonb_column, ['jsonb']) == True

    # 数组字段应该被忽略（udt_name以_array结尾）
    array_column = {'data_type': 'text[]', 'udt_name': '_text'}
    assert converter.should_ignore_column(array_column, ['array', '_array']) == True

    # 普通字段不应该被忽略
    normal_column = {'data_type': 'varchar', 'udt_name': 'varchar'}
    assert converter.should_ignore_column(normal_column, ['json']) == False


def test_convert_column_type():
    converter = TypeConverter('config/type_mapping.yaml')

    # 基本类型转换
    assert converter.convert_column_type('integer', {}) == 'INT'
    assert converter.convert_column_type('boolean', {}) == 'TINYINT(1)'
    assert converter.convert_column_type('text', {}) == 'LONGTEXT'
    assert converter.convert_column_type('bytea', {}) == 'LONGBLOB'

    # 带精度的类型转换
    decimal_info = {'numeric_precision': 10, 'numeric_scale': 2}
    assert 'DECIMAL(10,2)' in converter.convert_column_type('decimal', decimal_info)

    # 字符串类型转换
    varchar_info = {'character_maximum_length': 255}
    assert 'VARCHAR(255)' in converter.convert_column_type('varchar', varchar_info)
    assert 'VARCHAR(255)' in converter.convert_column_type('character varying', varchar_info)
    assert converter.convert_column_type('varchar', {}) == 'LONGTEXT'
