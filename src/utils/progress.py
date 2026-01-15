from typing import Optional
from tqdm import tqdm
from loguru import logger


class ProgressTracker:
    """进度跟踪器"""

    def __init__(self, total: int, desc: str = "Processing"):
        self.total = total
        self.desc = desc
        self.pbar: Optional[tqdm] = None

    def start(self):
        """开始进度跟踪"""
        self.pbar = tqdm(total=self.total, desc=self.desc)
        logger.info(f"开始处理: {self.desc}, 总数: {self.total}")

    def update(self, n: int = 1):
        """更新进度"""
        if self.pbar:
            self.pbar.update(n)

    def set_description(self, desc: str):
        """设置进度条描述"""
        if self.pbar:
            self.pbar.set_description(desc)

    def close(self):
        """关闭进度条"""
        if self.pbar:
            self.pbar.close()
            self.pbar = None
            logger.info(f"完成处理: {self.desc}")


def create_progress_tracker(total: int, desc: str = "Processing") -> ProgressTracker:
    """创建进度跟踪器"""
    tracker = ProgressTracker(total, desc)
    tracker.start()
    return tracker