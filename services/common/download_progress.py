"""
下载进度跟踪模块
提供全局的下载进度状态管理和查询
"""

import asyncio
import threading
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, Any
from enum import Enum

# 中国时区
CHINA_TZ = timezone(timedelta(hours=8))

def get_china_time():
    """获取中国时区时间"""
    return datetime.now(CHINA_TZ).replace(tzinfo=None)


class DownloadStatus(Enum):
    IDLE = "idle"
    PREPARING = "preparing"
    DOWNLOADING = "downloading"
    CALCULATING_FACTORS = "calculating_factors"
    COMPLETED = "completed"
    ERROR = "error"


class DownloadProgressTracker:
    """下载进度跟踪器（单例）"""

    def __init__(self):
        self._status = DownloadStatus.IDLE
        self._total_stocks = 0
        self._processed_stocks = 0
        self._total_tasks = 0
        self._processed_tasks = 0
        self._total_records = 0
        self._downloaded_records = 0
        self._current_stock = ""
        self._current_batch = 0
        self._total_batches = 0
        self._message = ""
        self._error = ""
        self._start_time: Optional[datetime] = None
        self._end_time: Optional[datetime] = None
        # asyncio.Lock 延迟初始化，避免绑定到错误的事件循环
        self._lock: Optional[asyncio.Lock] = None
        self._sync_lock = threading.Lock()  # 同步锁

    def _get_lock(self) -> asyncio.Lock:
        """获取当前事件循环的Lock（延迟初始化）"""
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return None

        if self._lock is None:
            self._lock = asyncio.Lock()
        elif hasattr(self._lock, '_loop') and self._lock._loop != loop:
            self._lock = asyncio.Lock()

        return self._lock

    async def reset(self):
        """重置进度"""
        lock = self._get_lock()
        if lock:
            async with lock:
                self._status = DownloadStatus.IDLE
                self._total_stocks = 0
                self._processed_stocks = 0
                self._total_tasks = 0
                self._processed_tasks = 0
                self._total_records = 0
                self._downloaded_records = 0
                self._current_stock = ""
                self._current_batch = 0
                self._total_batches = 0
                self._message = ""
                self._error = ""
                self._start_time = None
                self._end_time = None

    async def start(self, total_stocks: int, total_tasks: int = 0, total_records: int = 0):
        """开始下载"""
        lock = self._get_lock()
        if lock:
            async with lock:
                self._status = DownloadStatus.DOWNLOADING
                self._total_stocks = total_stocks
                self._total_tasks = total_tasks if total_tasks > 0 else total_stocks
                self._total_records = total_records
                self._processed_stocks = 0
                self._processed_tasks = 0
                self._downloaded_records = 0
                self._start_time = get_china_time()
                self._end_time = None
                self._error = ""

    async def update(self, processed: int = None, current_stock: str = "",
                     downloaded_records: int = 0, message: str = ""):
        """更新进度（异步版本）"""
        lock = self._get_lock()
        if lock:
            async with lock:
                if processed is not None:
                    self._processed_tasks = processed
                if current_stock:
                    self._current_stock = current_stock
                if downloaded_records > 0:
                    self._downloaded_records += downloaded_records
                if message:
                    self._message = message

    def update_sync(self, processed: int = None, total_tasks: int = None,
                    current_stock: str = "", downloaded_records: int = 0,
                    message: str = ""):
        """更新进度（同步版本，用于在同步函数中调用）"""
        with self._sync_lock:
            if processed is not None:
                self._processed_tasks = processed
            if total_tasks is not None:
                self._total_tasks = total_tasks
            if current_stock:
                self._current_stock = current_stock
            if downloaded_records > 0:
                self._downloaded_records += downloaded_records
            if message:
                self._message = message

    async def set_status(self, status: DownloadStatus, message: str = ""):
        """设置状态（异步版本）"""
        lock = self._get_lock()
        if lock:
            async with lock:
                self._status = status
                if message:
                    self._message = message

    def set_status_sync(self, status: DownloadStatus, message: str = ""):
        """设置状态（同步版本，用于在同步函数中调用）"""
        with self._sync_lock:
            self._status = status
            if message:
                self._message = message

    async def complete(self, error: str = ""):
        """完成下载（异步版本）"""
        lock = self._get_lock()
        if lock:
            async with lock:
                self._status = DownloadStatus.ERROR if error else DownloadStatus.COMPLETED
                self._error = error
                self._end_time = get_china_time()
                self._processed_tasks = self._total_tasks

    def complete_sync(self, error: str = ""):
        """完成下载（同步版本，用于在同步函数中调用）"""
        with self._sync_lock:
            self._status = DownloadStatus.ERROR if error else DownloadStatus.COMPLETED
            self._error = error
            self._end_time = get_china_time()
            self._processed_tasks = self._total_tasks

    def get_progress(self) -> Dict[str, Any]:
        """获取当前进度"""
        # 使用同步锁保证读取一致性
        with self._sync_lock:
            percent = 0
            if self._total_tasks > 0:
                percent = round((self._processed_tasks / self._total_tasks) * 100)

            elapsed_seconds = 0
            estimated_remaining = 0

            if self._start_time:
                end = self._end_time or get_china_time()
                elapsed_seconds = (end - self._start_time).total_seconds()

                if self._processed_tasks > 0 and self._status == DownloadStatus.DOWNLOADING:
                    avg_time_per_task = elapsed_seconds / self._processed_tasks
                    remaining_tasks = self._total_tasks - self._processed_tasks
                    estimated_remaining = avg_time_per_task * remaining_tasks

            return {
                "status": self._status.value,
                "total_stocks": self._total_stocks,
                "total_tasks": self._total_tasks,
                "processed_tasks": self._processed_tasks,
                "downloaded_records": self._downloaded_records,
                "percent": percent,
                "current_stock": self._current_stock,
                "current_batch": self._current_batch,
                "total_batches": self._total_batches,
                "message": self._message,
                "error": self._error,
                "elapsed_seconds": round(elapsed_seconds),
                "estimated_remaining": round(estimated_remaining),
                "start_time": self._start_time.isoformat() if self._start_time else None,
                "end_time": self._end_time.isoformat() if self._end_time else None
            }


# 全局单例
_tracker: Optional[DownloadProgressTracker] = None


def get_progress_tracker() -> DownloadProgressTracker:
    """获取进度跟踪器单例"""
    global _tracker
    if _tracker is None:
        _tracker = DownloadProgressTracker()
    return _tracker


def reset_progress_tracker():
    """重置进度跟踪器（用于测试）"""
    global _tracker
    _tracker = None
