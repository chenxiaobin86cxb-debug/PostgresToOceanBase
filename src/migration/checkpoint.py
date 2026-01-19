from typing import Dict, Optional
import json
import os
from datetime import datetime
from loguru import logger


class CheckpointManager:
    """检查点管理器"""

    def __init__(self, checkpoint_dir: str = 'backup/checkpoints'):
        """初始化检查点管理器

        Args:
            checkpoint_dir: 检查点文件存储目录
        """
        self.checkpoint_dir = checkpoint_dir
        os.makedirs(checkpoint_dir, exist_ok=True)

    def get_checkpoint(self, table_name: str) -> Optional[Dict]:
        """获取表的检查点

        Args:
            table_name: 表名

        Returns:
            检查点字典，如果不存在返回 None
        """
        checkpoint_file = os.path.join(self.checkpoint_dir, f"{table_name}.json")

        if not os.path.exists(checkpoint_file):
            return None

        try:
            with open(checkpoint_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"读取检查点失败: {table_name}, 错误: {e}")
            return None

    def save_checkpoint(self, table_name: str,
                       last_sync_time: str,
                       last_sync_count: int,
                       status: str = 'success') -> bool:
        """保存检查点

        Args:
            table_name: 表名
            last_sync_time: 最后同步时间
            last_sync_count: 最后同步记录数
            status: 同步状态

        Returns:
            是否保存成功
        """
        checkpoint_file = os.path.join(self.checkpoint_dir, f"{table_name}.json")

        checkpoint = {
            'table_name': table_name,
            'last_sync_time': last_sync_time,
            'last_sync_count': last_sync_count,
            'status': status,
            'updated_at': datetime.now().isoformat()
        }

        try:
            with open(checkpoint_file, 'w', encoding='utf-8') as f:
                json.dump(checkpoint, f, indent=2, ensure_ascii=False)
            logger.info(f"检查点保存成功: {table_name}, 时间: {last_sync_time}")
            return True
        except Exception as e:
            logger.error(f"检查点保存失败: {table_name}, 错误: {e}")
            return False

    def reset_checkpoint(self, table_name: str) -> bool:
        """重置检查点

        Args:
            table_name: 表名

        Returns:
            是否重置成功
        """
        checkpoint_file = os.path.join(self.checkpoint_dir, f"{table_name}.json")

        try:
            if os.path.exists(checkpoint_file):
                os.remove(checkpoint_file)
                logger.info(f"检查点重置成功: {table_name}")
            return True
        except Exception as e:
            logger.error(f"检查点重置失败: {table_name}, 错误: {e}")
            return False

    def list_checkpoints(self) -> Dict[str, Dict]:
        """列出所有检查点

        Returns:
            检查点字典 {table_name: checkpoint}
        """
        checkpoints = {}

        try:
            for filename in os.listdir(self.checkpoint_dir):
                if filename.endswith('.json'):
                    table_name = filename[:-5]  # 移除 .json 后缀
                    checkpoint = self.get_checkpoint(table_name)
                    if checkpoint:
                        checkpoints[table_name] = checkpoint
        except Exception as e:
            logger.error(f"列出检查点失败: {e}")

        return checkpoints