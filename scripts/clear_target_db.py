import argparse
from typing import List, Tuple

import pymysql
import yaml
from loguru import logger


def load_config(config_file: str) -> dict:
    """加载配置文件"""
    with open(config_file, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


def fetch_objects(cursor, schema: str) -> Tuple[List[str], List[str]]:
    """获取目标库的视图和表"""
    cursor.execute(
        """
        SELECT table_name
        FROM information_schema.views
        WHERE table_schema = %s
        ORDER BY table_name
        """,
        (schema,)
    )
    views = [row[0] for row in cursor.fetchall()]

    cursor.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = %s AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """,
        (schema,)
    )
    tables = [row[0] for row in cursor.fetchall()]

    return views, tables


def drop_objects(cursor, objects: List[str], object_type: str) -> int:
    """删除指定对象"""
    dropped = 0
    for name in objects:
        cursor.execute(f"DROP {object_type} IF EXISTS `{name}`")
        dropped += 1
    return dropped


def clear_target_database(config_file: str) -> None:
    """清空目标数据库中的表和视图"""
    config = load_config(config_file)
    target_config = config.get('target', {})

    conn = pymysql.connect(
        host=target_config.get('host'),
        port=int(target_config.get('port', 2881)),
        user=target_config.get('user'),
        password=target_config.get('password'),
        database=target_config.get('database'),
        charset='utf8mb4',
        autocommit=False
    )

    try:
        with conn.cursor() as cursor:
            schema = target_config.get('database')
            views, tables = fetch_objects(cursor, schema)

            if not views and not tables:
                logger.info(f"目标库 {schema} 无可删除对象")
                return

            cursor.execute("SET FOREIGN_KEY_CHECKS = 0")

            dropped_views = drop_objects(cursor, views, "VIEW")
            dropped_tables = drop_objects(cursor, tables, "TABLE")

            cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
            conn.commit()

            logger.info(
                f"目标库 {schema} 清理完成: "
                f"删除视图 {dropped_views} 个, 删除表 {dropped_tables} 个"
            )
    except Exception as e:
        conn.rollback()
        logger.error(f"清理目标库失败: {e}")
        raise
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description='清空 OceanBase 目标库')
    parser.add_argument('--config', default='config/config.yaml', help='配置文件路径')
    args = parser.parse_args()

    clear_target_database(args.config)


if __name__ == '__main__':
    main()
