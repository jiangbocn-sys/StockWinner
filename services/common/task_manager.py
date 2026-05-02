"""
后台任务状态管理器

防止长时间任务被重复调用：
- 数据下载
- 日频因子计算
- 月频因子更新

支持：
- 检查任务是否正在运行
- 获取任务进度
- 任务完成后自动清理
"""

from typing import Dict, Optional, Any
from datetime import datetime
from enum import Enum
import asyncio


class TaskType(Enum):
    """任务类型"""
    DATA_DOWNLOAD = "data_download"
    DAILY_FACTOR_CALC = "daily_factor_calc"
    MONTHLY_FACTOR_UPDATE = "monthly_factor_update"


class TaskStatus(Enum):
    """任务状态"""
    IDLE = "idle"           # 无任务运行
    RUNNING = "running"     # 正在运行
    COMPLETED = "completed" # 已完成
    FAILED = "failed"       # 失败


class TaskInfo:
    """任务信息"""
    def __init__(self, task_type: TaskType):
        self.task_type = task_type
        self.status = TaskStatus.IDLE
        self.progress: Dict[str, Any] = {}
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        self.result: Optional[Dict] = None
        self.error: Optional[str] = None

    def start(self):
        """启动任务"""
        self.status = TaskStatus.RUNNING
        self.start_time = datetime.now()
        self.progress = {"percent": 0, "message": "正在启动..."}

    def update_progress(self, percent: float, message: str = None, **extra):
        """更新进度"""
        self.progress = {"percent": percent, "message": message or ""}
        self.progress.update(extra)

    def complete(self, result: Dict = None):
        """完成任务"""
        self.status = TaskStatus.COMPLETED
        self.end_time = datetime.now()
        self.result = result
        self.progress = {"percent": 100, "message": "已完成"}

    def fail(self, error: str):
        """任务失败"""
        self.status = TaskStatus.FAILED
        self.end_time = datetime.now()
        self.error = error
        self.progress = {"percent": 0, "message": f"失败: {error}"}

    def reset(self):
        """重置任务状态"""
        self.status = TaskStatus.IDLE
        self.progress = {}
        self.start_time = None
        self.end_time = None
        self.result = None
        self.error = None

    def to_dict(self) -> Dict:
        """转换为字典"""
        elapsed_seconds = 0
        if self.start_time:
            end_time = self.end_time or datetime.now()
            elapsed_seconds = (end_time - self.start_time).total_seconds()

        return {
            "task_type": self.task_type.value,
            "status": self.status.value,
            "progress": self.progress,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "elapsed_seconds": elapsed_seconds,
            "result": self.result,
            "error": self.error
        }


class TaskManager:
    """任务管理器（单例）"""

    _instance: Optional['TaskManager'] = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._tasks: Dict[TaskType, TaskInfo] = {}
            for task_type in TaskType:
                cls._instance._tasks[task_type] = TaskInfo(task_type)
        return cls._instance

    def is_running(self, task_type: TaskType) -> bool:
        """检查任务是否正在运行"""
        return self._tasks[task_type].status == TaskStatus.RUNNING

    def get_status(self, task_type: TaskType) -> TaskInfo:
        """获取任务状态"""
        return self._tasks[task_type]

    def start_task(self, task_type: TaskType) -> bool:
        """启动任务（如果未运行）"""
        if self.is_running(task_type):
            return False
        self._tasks[task_type].start()
        return True

    def update_progress(self, task_type: TaskType, percent: float, message: str = None, **extra):
        """更新任务进度"""
        self._tasks[task_type].update_progress(percent, message, **extra)

    def complete_task(self, task_type: TaskType, result: Dict = None):
        """完成任务"""
        self._tasks[task_type].complete(result)

    def fail_task(self, task_type: TaskType, error: str):
        """标记任务失败"""
        self._tasks[task_type].fail(error)

    def reset_task(self, task_type: TaskType):
        """重置任务状态"""
        self._tasks[task_type].reset()

    def get_all_status(self) -> Dict:
        """获取所有任务状态"""
        return {
            task_type.value: task_info.to_dict()
            for task_type, task_info in self._tasks.items()
        }


# 全局单例
_task_manager: Optional[TaskManager] = None


def get_task_manager() -> TaskManager:
    """获取任务管理器单例"""
    global _task_manager
    if _task_manager is None:
        _task_manager = TaskManager()
    return _task_manager