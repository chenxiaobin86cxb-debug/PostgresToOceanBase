import pytest
from unittest.mock import Mock, MagicMock
from src.migration.data import DataMigrator
from src.migration.converter import TypeConverter


@pytest.fixture
def mock_pg_client():
    """模拟 PostgreSQL 客户端"""
    client = Mock()
    client.get_table_count.return_value = 2  # 返回2条记录
    client.get_table_data.return_value = [
        {'id': 1, 'name': 'test1', 'is_active': True},
        {'id': 2, 'name': 'test2', 'is_active': False}
    ]
    client.get_table_schema.return_value = {
        'table_name': 'test_table',
        'columns': [
            {'column_name': 'id', 'data_type': 'integer'},
            {'column_name': 'name', 'data_type': 'varchar'},
            {'column_name': 'is_active', 'data_type': 'boolean'}
        ],
        'primary_keys': ['id']
    }
    return client


@pytest.fixture
def mock_ob_client():
    """模拟 OceanBase 客户端"""
    client = Mock()
    client.insert_batch.return_value = 2
    return client


@pytest.fixture
def converter():
    """类型转换器"""
    return TypeConverter('config/type_mapping.yaml')


@pytest.fixture
def config():
    """配置"""
    return {
        'migration': {
            'data': {
                'batch_size': 1000,
                'chunk_size': 100,
                'max_retries': 3,
                'retry_delay': 5
            },
            'error': {
                'max_retries': 3,
                'retry_delay': 5,
                'continue_on_error': False
            }
        }
    }


def test_migrate_table_data_success(mock_pg_client, mock_ob_client, converter, config):
    """测试表数据迁移成功"""
    migrator = DataMigrator(mock_pg_client, mock_ob_client, converter, config)

    result = migrator.migrate_table_data('test_table')

    assert result['table_name'] == 'test_table'
    assert result['status'] == 'success'
    assert result['migrated'] == 2
    assert result['failed'] == 0
    assert result['total'] == 2

    mock_ob_client.insert_batch.assert_called()


def test_migrate_table_data_converts_boolean(mock_pg_client, mock_ob_client, converter, config):
    """测试布尔类型转换"""
    migrator = DataMigrator(mock_pg_client, mock_ob_client, converter, config)

    migrator.migrate_table_data('test_table')

    args, _ = mock_ob_client.insert_batch.call_args
    inserted_rows = args[1]
    assert inserted_rows[0]['is_active'] == 1
    assert inserted_rows[1]['is_active'] == 0


def test_migrate_table_data_empty_table(mock_pg_client, mock_ob_client, converter, config):
    """测试空表迁移"""
    mock_pg_client.get_table_count.return_value = 0
    migrator = DataMigrator(mock_pg_client, mock_ob_client, converter, config)

    result = migrator.migrate_table_data('empty_table')

    assert result['table_name'] == 'empty_table'
    assert result['status'] == 'skipped'
    assert 'migrated' not in result  # 空表返回的字典不包含migrated键

    # 空表不应该调用插入方法
    mock_ob_client.insert_batch.assert_not_called()


def test_migrate_table_data_insert_failure(mock_pg_client, mock_ob_client, converter, config):
    """测试插入失败的情况"""
    mock_ob_client.insert_batch.return_value = 0  # 插入失败
    migrator = DataMigrator(mock_pg_client, mock_ob_client, converter, config)

    result = migrator.migrate_table_data('test_table')

    assert result['table_name'] == 'test_table'
    assert result['status'] == 'partial'
    assert result['migrated'] == 0
    assert result['failed'] > 0


def test_migrate_all_data(mock_pg_client, mock_ob_client, converter, config):
    """测试迁移所有表的数据"""
    migrator = DataMigrator(mock_pg_client, mock_ob_client, converter, config)

    tables = ['table1', 'table2']
    results = migrator.migrate_all_data(tables)

    assert len(results['success']) == 2
    assert len(results['failed']) == 0
    assert len(results['partial']) == 0
    assert len(results['skipped']) == 0


def test_migrate_all_data_with_skipped_tables(mock_pg_client, mock_ob_client, converter, config):
    """测试空表统计为跳过"""
    mock_pg_client.get_table_count.return_value = 0
    migrator = DataMigrator(mock_pg_client, mock_ob_client, converter, config)

    tables = ['table1', 'table2']
    results = migrator.migrate_all_data(tables)

    assert len(results['success']) == 0
    assert len(results['partial']) == 0
    assert len(results['failed']) == 0
    assert len(results['skipped']) == 2


def test_migrate_all_data_with_ignored_columns(mock_pg_client, mock_ob_client, converter, config):
    """测试迁移时忽略特定字段"""
    migrator = DataMigrator(mock_pg_client, mock_ob_client, converter, config)

    tables = ['table1']
    ignore_columns_map = {'table1': ['ignored_column']}

    results = migrator.migrate_all_data(tables, ignore_columns_map=ignore_columns_map)

    assert len(results['success']) == 1
    # 验证是否传递了忽略字段参数（检查第一次调用）
    calls = mock_pg_client.get_table_data.call_args_list
    assert len(calls) > 0
    first_call = calls[0]
    assert first_call[0][0] == 'table1'
    assert first_call[0][1] == 'public'
    assert first_call[0][4] == ['ignored_column']
