import pytest
from unittest.mock import Mock
from src.migration.validator import DataValidator


class TestDataValidator:
    """数据验证器测试"""

    @pytest.fixture
    def mock_pg_client(self):
        """模拟PostgreSQL客户端"""
        client = Mock()
        client.get_table_count.return_value = 100
        client.get_table_data.return_value = [
            {'id': 1, 'name': 'test1'},
            {'id': 2, 'name': 'test2'}
        ]
        return client

    @pytest.fixture
    def mock_ob_client(self):
        """模拟OceanBase客户端"""
        client = Mock()
        client.get_table_count.return_value = 100
        return client

    @pytest.fixture
    def config(self):
        """测试配置"""
        return {
            'validation': {
                'check_count': True,
                'check_checksum': True,
                'sample_size': 1000
            }
        }

    def test_validate_count_matched(self, mock_pg_client, mock_ob_client, config):
        """测试记录数验证匹配"""
        validator = DataValidator(mock_pg_client, mock_ob_client, config)
        result = validator.validate_count('test_table', 'public')

        assert result['table_name'] == 'test_table'
        assert result['pg_count'] == 100
        assert result['ob_count'] == 100
        assert result['matched'] is True

    def test_validate_count_not_matched(self, mock_pg_client, mock_ob_client, config):
        """测试记录数验证不匹配"""
        mock_ob_client.get_table_count.return_value = 90

        validator = DataValidator(mock_pg_client, mock_ob_client, config)
        result = validator.validate_count('test_table', 'public')

        assert result['pg_count'] == 100
        assert result['ob_count'] == 90
        assert result['matched'] is False

    def test_validate_checksum_matched(self, mock_pg_client, mock_ob_client, config):
        """测试校验和验证匹配"""
        validator = DataValidator(mock_pg_client, mock_ob_client, config)
        result = validator.validate_checksum('test_table', 'public', 100)

        assert result['table_name'] == 'test_table'
        assert 'pg_checksum' in result
        assert 'ob_checksum' in result
        assert result['matched'] is True

    def test_validate_checksum_not_matched(self, mock_pg_client, mock_ob_client, config):
        """测试校验和验证不匹配"""
        # 模拟不同数据 - 修改pg_client的返回值
        mock_pg_client.get_table_data.side_effect = [
            [{'id': 1, 'name': 'test1'}],
            [{'id': 1, 'name': 'different'}]  # 第二次调用返回不同数据
        ]

        validator = DataValidator(mock_pg_client, mock_ob_client, config)
        result = validator.validate_checksum('test_table', 'public', 100)

        assert result['matched'] is False

    def test_validate_checksum_with_ignore_columns(self, mock_pg_client, mock_ob_client, config):
        """测试带忽略字段的校验和验证"""
        validator = DataValidator(mock_pg_client, mock_ob_client, config)
        result = validator.validate_checksum(
            'test_table', 'public', 100, ignore_columns=['data']
        )

        assert result['table_name'] == 'test_table'

    def test_validate_all(self, mock_pg_client, mock_ob_client, config):
        """测试验证所有表"""
        validator = DataValidator(mock_pg_client, mock_ob_client, config)
        results = validator.validate_all(['table1', 'table2'], 'public')

        assert 'count_validation' in results
        assert 'checksum_validation' in results
        assert len(results['count_validation']) == 2
        assert len(results['checksum_validation']) == 2

    def test_validate_all_count_only(self, mock_pg_client, mock_ob_client, config):
        """测试仅验证记录数"""
        config['validation']['check_checksum'] = False

        validator = DataValidator(mock_pg_client, mock_ob_client, config)
        results = validator.validate_all(['table1'], 'public')

        assert len(results['count_validation']) == 1
        assert len(results['checksum_validation']) == 0

    def test_validate_all_checksum_only(self, mock_pg_client, mock_ob_client, config):
        """测试仅验证校验和"""
        config['validation']['check_count'] = False

        validator = DataValidator(mock_pg_client, mock_ob_client, config)
        results = validator.validate_all(['table1'], 'public')

        assert len(results['count_validation']) == 0
        assert len(results['checksum_validation']) == 1

    def test_calculate_checksum(self, mock_pg_client, mock_ob_client, config):
        """测试校验和计算"""
        validator = DataValidator(mock_pg_client, mock_ob_client, config)

        data = [
            {'id': 1, 'name': 'test1'},
            {'id': 2, 'name': 'test2'}
        ]
        checksum = validator._calculate_checksum(data)

        assert len(checksum) == 32  # MD5 hash length
        assert checksum.isalnum()

    def test_calculate_checksum_empty(self, mock_pg_client, mock_ob_client, config):
        """测试空数据校验和计算"""
        validator = DataValidator(mock_pg_client, mock_ob_client, config)

        checksum = validator._calculate_checksum([])
        assert len(checksum) == 32

    def test_calculate_checksum_stable(self, mock_pg_client, mock_ob_client, config):
        """测试校验和稳定性"""
        validator = DataValidator(mock_pg_client, mock_ob_client, config)

        data = [{'id': 1, 'name': 'test'}]
        checksum1 = validator._calculate_checksum(data)
        checksum2 = validator._calculate_checksum(data)

        assert checksum1 == checksum2
