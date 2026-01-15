import time
from functools import wraps
from typing import Any, Callable
from loguru import logger


def retry(max_retries: int = 3, delay: int = 5, backoff: float = 1.0):
    """重试装饰器"""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            retries = 0
            last_exception = None
            current_delay = delay

            while retries < max_retries:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    retries += 1
                    if retries < max_retries:
                        logger.warning(
                            f"{func.__name__} 失败, 重试 {retries}/{max_retries} "
                            f"after {current_delay}s..."
                        )
                        time.sleep(current_delay)
                        current_delay *= backoff
                    else:
                        logger.error(f"{func.__name__} 达到最大重试次数")

            raise last_exception
        return wrapper
    return decorator


class RetryManager:
    """重试管理器"""

    def __init__(self, max_retries: int = 3, delay: int = 5, backoff: float = 1.0):
        self.max_retries = max_retries
        self.delay = delay
        self.backoff = backoff

    def execute(self, func: Callable, *args, **kwargs) -> Any:
        """执行带重试的函数"""
        retries = 0
        last_exception = None
        current_delay = self.delay

        while retries < self.max_retries:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                last_exception = e
                retries += 1
                if retries < self.max_retries:
                    logger.warning(
                        f"执行失败, 重试 {retries}/{self.max_retries} "
                        f"after {current_delay}s..."
                    )
                    time.sleep(current_delay)
                    current_delay *= self.backoff
                else:
                    logger.error("达到最大重试次数")

        raise last_exception