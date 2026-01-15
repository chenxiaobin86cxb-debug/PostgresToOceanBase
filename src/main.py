import yaml
import argparse
from typing import List, Dict
from loguru import logger
from src.database.connection import ConnectionManager
from src.database.postgres import PostgreSQLClient
from src.database.oceanbase import OceanBaseClient
from src.migration.converter import TypeConverter
from src.migration.schema import SchemaMigrator
from src.migration.data import DataMigrator
from src.migration.validator import DataValidator


def load_config(config_file: str = 'config/config.yaml') -> dict:
    """加载配置文件"""
    with open(config_file, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    logger.info(f"配置文件加载成功: {config_file}")
    return config


def get_ignored_columns(pg_client, tables: List[str], schema: str,
                       ignore_types: List[str]) -> Dict[str, List[str]]:
    """获取每个表需要忽略的字段"""
    ignored_columns_map = {}

    for table_name in tables:
        schema_info = pg_client.get_table_schema(table_name, schema)
        ignored_cols = []

        for col in schema_info['columns']:
            if TypeConverter().should_ignore_column(col, ignore_types):
                ignored_cols.append(col['column_name'])

        if ignored_cols:
            ignored_columns_map[table_name] = ignored_cols
            logger.warning(f"表 {table_name} 将忽略字段: {ignored_cols}")

    return ignored_columns_map


def main():
    """主程序"""
    parser = argparse.ArgumentParser(description='PostgreSQL 到 OceanBase 迁移工具')
    parser.add_argument('--config', default='config/config.yaml',
                       help='配置文件路径')
    parser.add_argument('--schema-only', action='store_true',
                       help='仅迁移表结构')
    parser.add_argument('--data-only', action='store_true',
                       help='仅迁移数据')
    parser.add_argument('--validate', action='store_true',
                       help='迁移后进行数据验证')

    args = parser.parse_args()

    # 加载配置
    config = load_config(args.config)
    source_config = config.get('source', {})
    target_config = config.get('target', {})
    migration_config = config.get('migration', {})

    # 初始化日志
    logger.add(
        config.get('logging', {}).get('file', 'logs/migration.log'),
        rotation=config.get('logging', {}).get('rotation', '100 MB'),
        retention=config.get('logging', {}).get('retention', '30 days')
    )

    # 初始化连接管理器
    conn_mgr = ConnectionManager(config)
    conn_mgr.init_pools()

    try:
        # 初始化客户端
        pg_client = PostgreSQLClient(conn_mgr)
        ob_client = OceanBaseClient(conn_mgr)

        # 初始化转换器
        converter = TypeConverter('config/type_mapping.yaml')

        # 获取需要迁移的表
        schema_config = migration_config.get('schema', {})
        schema_name = source_config.get('schema', 'public')
        all_tables = pg_client.get_tables(schema_name)

        include_tables = schema_config.get('include_tables', [])
        exclude_tables = schema_config.get('exclude_tables', [])
        ignore_types = schema_config.get('ignore_types', ['json', 'jsonb', 'array'])

        # 过滤表
        if include_tables:
            tables = [t for t in all_tables if t in include_tables]
        else:
            tables = [t for t in all_tables if t not in exclude_tables]

        logger.info(f"需要迁移的表数量: {len(tables)}")
        for table in tables:
            logger.info(f"  - {table}")

        # 获取每个表需要忽略的字段
        ignored_columns_map = get_ignored_columns(
            pg_client, tables, schema_name, ignore_types
        )

        # 表结构迁移
        if not args.data_only and schema_config.get('enabled', True):
            logger.info("=" * 50)
            logger.info("开始表结构迁移")
            logger.info("=" * 50)

            schema_migrator = SchemaMigrator(pg_client, ob_client, converter)
            schema_results = schema_migrator.migrate_schema(
                tables, schema_name, ignore_types
            )

            logger.info(f"表结构迁移完成:")
            logger.info(f"  成功: {len(schema_results['success'])}")
            logger.info(f"  失败: {len(schema_results['failed'])}")

        # 数据迁移
        if not args.schema_only and migration_config.get('data', {}).get('enabled', True):
            logger.info("=" * 50)
            logger.info("开始数据迁移")
            logger.info("=" * 50)

            data_migrator = DataMigrator(pg_client, ob_client, converter, config)
            data_results = data_migrator.migrate_all_data(
                tables, schema_name, ignored_columns_map
            )

            logger.info(f"数据迁移完成:")
            logger.info(f"  完全成功: {len(data_results['success'])}")
            logger.info(f"  部分成功: {len(data_results['partial'])}")
            logger.info(f"  跳过: {len(data_results['skipped'])}")
            logger.info(f"  失败: {len(data_results['failed'])}")

        # 数据验证
        if args.validate and migration_config.get('validation', {}).get('enabled', True):
            logger.info("=" * 50)
            logger.info("开始数据验证")
            logger.info("=" * 50)

            validator = DataValidator(pg_client, ob_client, config)
            validation_results = validator.validate_all(
                tables, schema_name, ignored_columns_map
            )

            # 统计结果
            count_passed = sum(
                1 for r in validation_results['count_validation'] if r['matched']
            )
            checksum_passed = sum(
                1 for r in validation_results['checksum_validation'] if r['matched']
            )

            logger.info(f"数据验证完成:")
            logger.info(f"  记录数验证: {count_passed}/{len(tables)}")
            logger.info(f"  校验和验证: {checksum_passed}/{len(tables)}")

        logger.info("=" * 50)
        logger.info("迁移完成！")
        logger.info("=" * 50)

    finally:
        conn_mgr.close_all()


if __name__ == '__main__':
    main()
