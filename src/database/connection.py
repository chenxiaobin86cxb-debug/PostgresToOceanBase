import logging
from typing import Optional
from contextlib import contextmanager
import psycopg2
import pymysql
from psycopg2 import pool as pg_pool
from dbutils.pooled_db import PooledDB
from loguru import logger


class ConnectionManager:
    """数据库连接管理器"""

    def __init__(self, config: dict):
        self.source_config = config.get('source', {})
        self.target_config = config.get('target', {})
        self.source_pool: Optional[pg_pool.ThreadedConnectionPool] = None
        self.target_pool: Optional[PooledDB] = None

    def init_pools(self):
        """初始化连接池"""
        # PostgreSQL 连接池
        try:
            self.source_pool = pg_pool.ThreadedConnectionPool(
                minconn=1,
                maxconn=self.source_config.get('pool_size', 10) +
                       self.source_config.get('max_overflow', 20),
                host=self.source_config.get('host'),
                port=self.source_config.get('port'),
                database=self.source_config.get('database'),
                user=self.source_config.get('user'),
                password=self.source_config.get('password')
            )
            logger.info("PostgreSQL 连接池初始化成功")
        except Exception as e:
            logger.error(f"PostgreSQL 连接池初始化失败: {e}")
            raise

        # OceanBase 连接池（MySQL 模式）
        try:
            self.target_pool = PooledDB(
                creator=pymysql,
                maxconnections=self.target_config.get('pool_size', 10) +
                             self.target_config.get('max_overflow', 20),
                host=self.target_config.get('host'),
                port=self.target_config.get('port'),
                database=self.target_config.get('database'),
                user=self.target_config.get('user'),
                password=self.target_config.get('password'),
                charset='utf8mb4',
                autocommit=False
            )
            logger.info("OceanBase 连接池初始化成功")
        except Exception as e:
            logger.error(f"OceanBase 连接池初始化失败: {e}")
            raise

    @contextmanager
    def get_source_connection(self):
        """获取 PostgreSQL 连接"""
        conn = None
        try:
            conn = self.source_pool.getconn()
            yield conn
        except Exception as e:
            logger.error(f"PostgreSQL 连接错误: {e}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                self.source_pool.putconn(conn)

    @contextmanager
    def get_target_connection(self):
        """获取 OceanBase 连接"""
        conn = None
        try:
            conn = self.target_pool.connection()
            yield conn
        except Exception as e:
            logger.error(f"OceanBase 连接错误: {e}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                conn.close()

    def close_all(self):
        """关闭所有连接池"""
        if self.source_pool:
            self.source_pool.closeall()
            logger.info("PostgreSQL 连接池已关闭")
        if self.target_pool:
            self.target_pool.close()
            logger.info("OceanBase 连接池已关闭")