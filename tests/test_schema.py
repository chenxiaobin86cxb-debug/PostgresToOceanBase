import pytest
from unittest.mock import Mock, MagicMock
from src.migration.schema import SchemaMigrator
from src.migration.converter import TypeConverter


@pytest.fixture
def mock_pg_client():
    """模拟 PostgreSQL 客户端"""
    client = Mock()
    client.get_table_schema.return_value = {
        'table_name': 'test_table',
        'columns': [
            {
                'column_name': 'id',
                'data_type': 'integer',
                'is_nullable': 'NO',
                'column_default': None
            },
            {
                'column_name': 'name',
                'data_type': 'varchar',
                'character_maximum_length': 100,
                'is_nullable': 'YES',
                'column_default': None
            },
            {
                'column_name': 'data',
                'data_type': 'jsonb',
                'is_nullable': 'YES',
                'column_default': None
            }
        ],
        'primary_keys': ['id']
    }
    client.get_table_indexes.return_value = []
    return client


@pytest.fixture
def mock_ob_client():
    """模拟 OceanBase 客户端"""
    client = Mock()
    client.create_table.return_value = True
    return client


@pytest.fixture
def converter():
    """类型转换器"""
    return TypeConverter('config/type_mapping.yaml')


def test_generate_create_table_sql(mock_pg_client, mock_ob_client, converter):
    """测试生成 CREATE TABLE SQL"""
    migrator = SchemaMigrator(mock_pg_client, mock_ob_client, converter)

    schema = {
        'table_name': 'test_table',
        'columns': [
            {
                'column_name': 'id',
                'data_type': 'integer',
                'is_nullable': 'NO',
                'column_default': None
            },
            {
                'column_name': 'name',
                'data_type': 'varchar',
                'character_maximum_length': 100,
                'is_nullable': 'YES',
                'column_default': None
            }
        ],
        'primary_keys': ['id']
    }

    sql, _ = migrator.generate_create_table_sql(schema, ['json', 'jsonb', 'array'])

    assert 'CREATE TABLE IF NOT EXISTS `test_table`' in sql
    assert '`id` INT NOT NULL' in sql
    assert '`name` VARCHAR(100)' in sql
    assert 'PRIMARY KEY (`id`)' in sql
    assert 'ENGINE=InnoDB' in sql


def test_generate_create_table_sql_with_ignored_columns(mock_pg_client, mock_ob_client, converter):
    """测试生成 CREATE TABLE SQL 时忽略特定字段"""
    migrator = SchemaMigrator(mock_pg_client, mock_ob_client, converter)

    schema = {
        'table_name': 'test_table',
        'columns': [
            {
                'column_name': 'id',
                'data_type': 'integer',
                'is_nullable': 'NO',
                'column_default': None
            },
            {
                'column_name': 'data',
                'data_type': 'jsonb',
                'is_nullable': 'YES',
                'column_default': None
            }
        ],
        'primary_keys': ['id']
    }

    sql, _ = migrator.generate_create_table_sql(schema, ['json', 'jsonb', 'array'])

    # JSONB 字段应该被忽略
    assert 'data' not in sql
    assert '`id` INT NOT NULL' in sql


def test_generate_create_table_sql_with_sequence_default(mock_pg_client, mock_ob_client, converter):
    """测试 nextval 默认值转换为自增"""
    migrator = SchemaMigrator(mock_pg_client, mock_ob_client, converter)

    schema = {
        'table_name': 'test_table',
        'columns': [
            {
                'column_name': 'id',
                'data_type': 'integer',
                'is_nullable': 'NO',
                'column_default': "nextval('test_table_id_seq'::regclass)"
            }
        ],
        'primary_keys': ['id']
    }

    sql, _ = migrator.generate_create_table_sql(schema, ['json', 'jsonb', 'array'])

    assert 'AUTO_INCREMENT' in sql
    assert 'nextval' not in sql


def test_generate_create_table_sql_with_cast_default(mock_pg_client, mock_ob_client, converter):
    """测试移除默认值类型转换"""
    migrator = SchemaMigrator(mock_pg_client, mock_ob_client, converter)

    schema = {
        'table_name': 'test_table',
        'columns': [
            {
                'column_name': 'status',
                'data_type': 'varchar',
                'character_maximum_length': 32,
                'is_nullable': 'NO',
                'column_default': "'pending'::character varying"
            }
        ],
        'primary_keys': []
    }

    sql, _ = migrator.generate_create_table_sql(schema, ['json', 'jsonb', 'array'])

    assert "DEFAULT 'pending'" in sql
    assert '::character varying' not in sql


def test_generate_create_table_sql_with_boolean_default(mock_pg_client, mock_ob_client, converter):
    """测试布尔默认值转换"""
    migrator = SchemaMigrator(mock_pg_client, mock_ob_client, converter)

    schema = {
        'table_name': 'test_table',
        'columns': [
            {
                'column_name': 'is_active',
                'data_type': 'boolean',
                'is_nullable': 'NO',
                'column_default': 'true'
            }
        ],
        'primary_keys': []
    }

    sql, _ = migrator.generate_create_table_sql(schema, ['json', 'jsonb', 'array'])

    assert 'DEFAULT 1' in sql


def test_migrate_schema_success(mock_pg_client, mock_ob_client, converter):
    """测试表结构迁移成功"""
    migrator = SchemaMigrator(mock_pg_client, mock_ob_client, converter)

    tables = ['test_table']
    results = migrator.migrate_schema(tables, 'public', ['json', 'jsonb', 'array'])

    assert len(results['success']) == 1
    assert results['success'][0] == 'test_table'
    assert len(results['failed']) == 0
    mock_ob_client.create_table.assert_called_once()
    mock_pg_client.get_table_indexes.assert_called_once()


def test_migrate_schema_failure(mock_pg_client, mock_ob_client, converter):
    """测试表结构迁移失败"""
    mock_ob_client.create_table.return_value = False
    migrator = SchemaMigrator(mock_pg_client, mock_ob_client, converter)

    tables = ['test_table']
    results = migrator.migrate_schema(tables, 'public', ['json', 'jsonb', 'array'])

    assert len(results['success']) == 0
    assert len(results['failed']) == 1
    assert results['failed'][0] == 'test_table'
