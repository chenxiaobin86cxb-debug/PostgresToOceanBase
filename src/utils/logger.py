import yaml
from loguru import logger


def setup_logger(config_file: str = 'config/logger.yaml'):
    """设置日志"""
    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)

        # 清除默认处理器
        logger.remove()

        # 添加处理器
        handlers = config.get('handlers', [])
        for handler in handlers:
            logger.add(**handler)

        logger.info("日志配置加载成功")
    except Exception as e:
        # 如果配置文件不存在，使用默认配置
        logger.warning(f"无法加载日志配置: {e}，使用默认配置")
        logger.add("logs/migration.log", rotation="100 MB", retention="30 days")