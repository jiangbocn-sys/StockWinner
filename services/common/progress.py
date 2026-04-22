"""
进度追踪器模块

为长时间运行的任务（下载、因子计算等）提供统一的进度追踪接口
"""

import json
import time
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any
from dataclasses import dataclass, asdict


@dataclass
class ProgressState:
    """进度状态数据类"""
    task_id: str
    task_type: str  # download, factor_calc, screening, etc.
    total: int
    current: int
    status: str  # pending, running, completed, failed, cancelled
    message: str
    started_at: Optional[str]
    completed_at: Optional[str]
    error: Optional[str]
    metadata: Dict[str, Any]


class ProgressTracker:
    """进度追踪器"""

    def __init__(
        self,
        task_id: str,
        task_type: str,
        total: int,
        persist_dir: Optional[Path] = None
    ):
        """
        初始化进度追踪器

        Args:
            task_id: 任务唯一标识
            task_type: 任务类型（download, factor_calc, screening 等）
            total: 总工作量
            persist_dir: 持久化目录，None 则不持久化
        """
        self.state = ProgressState(
            task_id=task_id,
            task_type=task_type,
            total=total,
            current=0,
            status="pending",
            message="等待开始",
            started_at=None,
            completed_at=None,
            error=None,
            metadata={}
        )
        self.persist_dir = persist_dir or Path(__file__).parent.parent.parent / "data" / "progress"
        self._callbacks = []

    def start(self, message: str = "任务开始"):
        """标记任务开始"""
        self.state.status = "running"
        self.state.message = message
        self.state.started_at = datetime.now().isoformat()
        self._persist()
        self._notify_callbacks()

    def update(self, current: int, message: str = ""):
        """
        更新进度

        Args:
            current: 当前完成量
            message: 进度消息
        """
        self.state.current = current
        if message:
            self.state.message = message
        self._persist()
        self._notify_callbacks()

    def increment(self, amount: int = 1, message: str = ""):
        """
        增加进度

        Args:
            amount: 增加量
            message: 进度消息
        """
        self.state.current += amount
        if message:
            self.state.message = message
        self._persist()
        self._notify_callbacks()

    def complete(self, message: str = "任务完成"):
        """标记任务完成"""
        self.state.status = "completed"
        self.state.message = message
        self.state.completed_at = datetime.now().isoformat()
        self.state.current = self.state.total
        self._persist()
        self._notify_callbacks()

    def fail(self, error: str, message: str = "任务失败"):
        """标记任务失败"""
        self.state.status = "failed"
        self.state.message = message
        self.state.error = error
        self.state.completed_at = datetime.now().isoformat()
        self._persist()
        self._notify_callbacks()

    def cancel(self, message: str = "任务取消"):
        """标记任务取消"""
        self.state.status = "cancelled"
        self.state.message = message
        self.state.completed_at = datetime.now().isoformat()
        self._persist()
        self._notify_callbacks()

    def set_metadata(self, key: str, value: Any):
        """设置元数据"""
        self.state.metadata[key] = value
        self._persist()

    def get_progress(self) -> Dict[str, Any]:
        """获取当前进度"""
        return asdict(self.state)

    def get_progress_percent(self) -> float:
        """获取完成百分比"""
        if self.state.total == 0:
            return 0.0
        return min(100.0, (self.state.current / self.state.total) * 100)

    def get_eta(self) -> Optional[int]:
        """
        获取预估剩余时间（秒）

        Returns:
            预估剩余秒数，如果无法计算返回 None
        """
        if not self.state.started_at:
            return None

        elapsed = (datetime.now() - datetime.fromisoformat(self.state.started_at)).total_seconds()
        if self.state.current == 0:
            return None

        rate = self.state.current / elapsed
        remaining = self.state.total - self.state.current

        if rate > 0:
            return int(remaining / rate)
        return None

    def add_callback(self, callback):
        """添加进度更新回调函数"""
        self._callbacks.append(callback)

    def _persist(self):
        """持久化进度"""
        if self.persist_dir:
            try:
                self.persist_dir.mkdir(parents=True, exist_ok=True)
                persist_file = self.persist_dir / f"{self.state.task_id}.json"
                with open(persist_file, 'w', encoding='utf-8') as f:
                    json.dump(asdict(self.state), f, ensure_ascii=False, indent=2)
            except Exception:
                pass  # 持久化失败不影响主逻辑

    def _notify_callbacks(self):
        """通知回调函数"""
        for callback in self._callbacks:
            try:
                callback(self.state)
            except Exception:
                pass  # 回调失败不影响主逻辑


class ProgressManager:
    """进度管理器 - 管理所有任务的进度"""

    def __init__(self, persist_dir: Optional[Path] = None):
        self.persist_dir = persist_dir or Path(__file__).parent.parent.parent / "data" / "progress"
        self._trackers: Dict[str, ProgressTracker] = {}

    def create_tracker(
        self,
        task_id: str,
        task_type: str,
        total: int
    ) -> ProgressTracker:
        """创建新的进度追踪器"""
        tracker = ProgressTracker(task_id, task_type, total, self.persist_dir)
        self._trackers[task_id] = tracker
        return tracker

    def get_tracker(self, task_id: str) -> Optional[ProgressTracker]:
        """获取已有的进度追踪器"""
        return self._trackers.get(task_id)

    def remove_tracker(self, task_id: str):
        """删除进度追踪器"""
        if task_id in self._trackers:
            del self._trackers[task_id]

    def get_all_progress(self) -> Dict[str, Dict[str, Any]]:
        """获取所有任务进度"""
        return {
            task_id: tracker.get_progress()
            for task_id, tracker in self._trackers.items()
        }

    def load_progress(self, task_id: str) -> Optional[Dict[str, Any]]:
        """从文件加载进度"""
        persist_file = self.persist_dir / f"{task_id}.json"
        if persist_file.exists():
            with open(persist_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        return None

    def cleanup_old_progress(self, max_age_days: int = 7):
        """清理过期的进度文件"""
        if not self.persist_dir.exists():
            return

        now = time.time()
        max_age_seconds = max_age_days * 24 * 60 * 60

        for f in self.persist_dir.glob("*.json"):
            if now - f.stat().st_mtime > max_age_seconds:
                f.unlink()


# 全局进度管理器
_progress_manager: Optional[ProgressManager] = None


def get_progress_manager() -> ProgressManager:
    """获取全局进度管理器"""
    global _progress_manager
    if _progress_manager is None:
        _progress_manager = ProgressManager()
    return _progress_manager


def reset_progress_manager():
    """重置进度管理器（用于测试）"""
    global _progress_manager
    _progress_manager = None
