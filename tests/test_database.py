import pytest
from unittest.mock import Mock, MagicMock, patch


class TestPostgreSQLClient:
    """PostgreSQL客户端测试"""

    @pytest.fixture
    def mock_connection_manager(self):
        """模拟连接管理器"""
        conn_mgr = Mock()
        return conn_mgr

    def test_get_tables(self, mock_connection_manager):
        """测试获取表名列表"""
        from src.database.postgres import PostgreSQLClient

        cursor = Mock()
        cursor.fetchall.return_value = [('table1',), ('table2',), ('table3',)]

        mock_conn = Mock()
        mock_conn.cursor.return_value = cursor

        mock_connection_manager.get_source_connection.return_value.__enter__ = Mock(
            return_value=mock_conn
        )
        mock_connection_manager.get_source_connection.return_value.__exit__ = Mock(
            return_value=False
        )

        client = PostgreSQLClient(mock_connection_manager)
        tables = client.get_tables('public')

        assert len(tables) == 3
        assert 'table1' in tables
        assert 'table2' in tables
        assert 'table3' in tables

    def test_get_table_count(self, mock_connection_manager):
        """测试获取表记录数"""
        from src.database.postgres import PostgreSQLClient

        cursor = Mock()
        cursor.fetchone.return_value = (100,)

        mock_conn = Mock()
        mock_conn.cursor.return_value = cursor

        mock_connection_manager.get_source_connection.return_value.__enter__ = Mock(
            return_value=mock_conn
        )
        mock_connection_manager.get_source_connection.return_value.__exit__ = Mock(
            return_value=False
        )

        client = PostgreSQLClient(mock_connection_manager)
        count = client.get_table_count('test_table', 'public')

        assert count == 100

    def test_get_table_schema(self, mock_connection_manager):
        """测试获取表结构"""
        from src.database.postgres import PostgreSQLClient

        cursor = Mock()
        cursor.fetchall.side_effect = [
            [
                {'column_name': 'id', 'data_type': 'integer', 'is_nullable': 'NO'},
                {'column_name': 'name', 'data_type': 'varchar', 'is_nullable': 'YES'}
            ],
            [('id',)]
        ]

        mock_conn = Mock()
        mock_conn.cursor.return_value = cursor

        mock_connection_manager.get_source_connection.return_value.__enter__ = Mock(
            return_value=mock_conn
        )
        mock_connection_manager.get_source_connection.return_value.__exit__ = Mock(
            return_value=False
        )

        client = PostgreSQLClient(mock_connection_manager)
        schema = client.get_table_schema('test_table', 'public')

        assert schema['table_name'] == 'test_table'
        assert len(schema['columns']) == 2
        assert schema['primary_keys'] == ['id']

    def test_get_table_data(self, mock_connection_manager):
        """测试获取表数据"""
        from src.database.postgres import PostgreSQLClient

        cursor = Mock()
        cursor.fetchall.return_value = [
            {'id': 1, 'name': 'test1'},
            {'id': 2, 'name': 'test2'}
        ]

        mock_conn = Mock()
        mock_conn.cursor.return_value = cursor

        mock_connection_manager.get_source_connection.return_value.__enter__ = Mock(
            return_value=mock_conn
        )
        mock_connection_manager.get_source_connection.return_value.__exit__ = Mock(
            return_value=False
        )

        client = PostgreSQLClient(mock_connection_manager)
        data = client.get_table_data('test_table', 'public', 0, 100)

        assert len(data) == 2
        assert data[0]['id'] == 1


class TestOceanBaseClient:
    """OceanBase客户端测试"""

    @pytest.fixture
    def mock_connection_manager(self):
        """模拟连接管理器"""
        conn_mgr = Mock()
        return conn_mgr

    def test_create_table_success(self, mock_connection_manager):
        """测试创建表成功"""
        from src.database.oceanbase import OceanBaseClient

        cursor = Mock()

        mock_conn = Mock()
        mock_conn.cursor.return_value = cursor

        mock_connection_manager.get_target_connection.return_value.__enter__ = Mock(
            return_value=mock_conn
        )
        mock_connection_manager.get_target_connection.return_value.__exit__ = Mock(
            return_value=False
        )

        client = OceanBaseClient(mock_connection_manager)
        result = client.create_table("CREATE TABLE test (id INT)")

        assert result is True
        cursor.execute.assert_called_once()
        mock_conn.commit.assert_called_once()

    def test_create_table_failure(self, mock_connection_manager):
        """测试创建表失败"""
        from src.database.oceanbase import OceanBaseClient

        cursor = Mock()
        cursor.execute.side_effect = Exception("SQL syntax error")

        mock_conn = Mock()
        mock_conn.cursor.return_value = cursor
        mock_conn.rollback = Mock()

        mock_connection_manager.get_target_connection.return_value.__enter__ = Mock(
            return_value=mock_conn
        )
        mock_connection_manager.get_target_connection.return_value.__exit__ = Mock(
            return_value=False
        )

        client = OceanBaseClient(mock_connection_manager)
        result = client.create_table("INVALID SQL")

        assert result is False
        mock_conn.rollback.assert_called_once()

    def test_insert_batch_success(self, mock_connection_manager):
        """测试批量插入成功"""
        from src.database.oceanbase import OceanBaseClient

        cursor = Mock()

        mock_conn = Mock()
        mock_conn.cursor.return_value = cursor

        mock_connection_manager.get_target_connection.return_value.__enter__ = Mock(
            return_value=mock_conn
        )
        mock_connection_manager.get_target_connection.return_value.__exit__ = Mock(
            return_value=False
        )

        client = OceanBaseClient(mock_connection_manager)
        data = [
            {'id': 1, 'name': 'test1'},
            {'id': 2, 'name': 'test2'}
        ]
        result = client.insert_batch('test_table', data, batch_size=1000)

        assert result == 2
        cursor.executemany.assert_called_once()
        mock_conn.commit.assert_called_once()

    def test_insert_batch_empty_data(self, mock_connection_manager):
        """测试空数据批量插入"""
        from src.database.oceanbase import OceanBaseClient

        client = OceanBaseClient(mock_connection_manager)
        result = client.insert_batch('test_table', [])

        assert result == 0

    def test_get_table_count(self, mock_connection_manager):
        """测试获取表记录数"""
        from src.database.oceanbase import OceanBaseClient

        cursor = Mock()
        cursor.fetchone.return_value = (50,)

        mock_conn = Mock()
        mock_conn.cursor.return_value = cursor

        mock_connection_manager.get_target_connection.return_value.__enter__ = Mock(
            return_value=mock_conn
        )
        mock_connection_manager.get_target_connection.return_value.__exit__ = Mock(
            return_value=False
        )

        client = OceanBaseClient(mock_connection_manager)
        count = client.get_table_count('test_table')

        assert count == 50

    def test_truncate_table_success(self, mock_connection_manager):
        """测试清空表成功"""
        from src.database.oceanbase import OceanBaseClient

        cursor = Mock()

        mock_conn = Mock()
        mock_conn.cursor.return_value = cursor

        mock_connection_manager.get_target_connection.return_value.__enter__ = Mock(
            return_value=mock_conn
        )
        mock_connection_manager.get_target_connection.return_value.__exit__ = Mock(
            return_value=False
        )

        client = OceanBaseClient(mock_connection_manager)
        result = client.truncate_table('test_table')

        assert result is True
        cursor.execute.assert_called_with("TRUNCATE TABLE test_table")
        mock_conn.commit.assert_called_once()

    def test_truncate_table_failure(self, mock_connection_manager):
        """测试清空表失败"""
        from src.database.oceanbase import OceanBaseClient

        cursor = Mock()
        cursor.execute.side_effect = Exception("Table not found")

        mock_conn = Mock()
        mock_conn.cursor.return_value = cursor
        mock_conn.rollback = Mock()

        mock_connection_manager.get_target_connection.return_value.__enter__ = Mock(
            return_value=mock_conn
        )
        mock_connection_manager.get_target_connection.return_value.__exit__ = Mock(
            return_value=False
        )

        client = OceanBaseClient(mock_connection_manager)
        result = client.truncate_table('nonexistent_table')

        assert result is False
        mock_conn.rollback.assert_called_once()
