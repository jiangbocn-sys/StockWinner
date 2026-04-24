"""
因子计算进度跟踪模块
提供全局的因子计算进度状态管理和查询
"""

import threading
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, Any
from enum import Enum

# 中国时区
CHINA_TZ = timezone(timedelta(hours=8))

def get_china_time():
    """获取中国时区时间"""
    return datetime.now(CHINA_TZ).replace(tzinfo=None)


class FactorCalcStatus(Enum):
    IDLE = "idle"
    CALCULATING = "calculating"
    COMPLETED = "completed"
    ERROR = "error"


class FactorCalcProgressTracker:
    """因子计算进度跟踪器（单例）"""

    def __init__(self):
        self._status = FactorCalcStatus.IDLE
        self._total_stocks = 0
        self._processed_stocks = 0
        self._inserted_count = 0
        self._updated_count = 0
        self._current_stock = ""
        self._current_batch = 0
        self._total_batches = 0
        self._message = ""
        self._error = ""
        self._start_time: Optional[datetime] = None
        self._end_time: Optional[datetime] = None
        self._sync_lock = threading.Lock()

    def start(self, total_stocks: int, total_batches: int = 0):
        """开始计算"""
        with self._sync_lock:
            self._status = FactorCalcStatus.CALCULATING
            self._total_stocks = total_stocks
            self._total_batches = total_batches if total_batches > 0 else total_stocks
            self._processed_stocks = 0
            self._inserted_count = 0
            self._updated_count = 0
            self._current_stock = ""
            self._current_batch = 0
            self._start_time = get_china_time()
            self._end_time = None
            self._error = ""
            self._message = "开始因子计算..."

    def update(self, processed: int = None, current_stock: str = "",
               current_batch: int = None, inserted: int = 0, updated: int = 0,
               message: str = ""):
        """更新进度"""
        with self._sync_lock:
            if processed is not None:
                self._processed_stocks = processed
            if current_stock:
                self._current_stock = current_stock
            if current_batch is not None:
                self._current_batch = current_batch
            if inserted > 0:
                self._inserted_count += inserted
            if updated > 0:
                self._updated_count += updated
            if message:
                self._message = message

    def update_sync(self, processed: int = None, total_tasks: int = None,
                    current_stock: str = "", current_batch: int = None,
                    inserted: int = 0, updated: int = 0, message: str = ""):
        """更新进度（同步版本，用于在同步函数中调用）"""
        with self._sync_lock:
            if processed is not None:
                self._processed_stocks = processed
            if total_tasks is not None:
                self._total_stocks = total_tasks
            if current_stock:
                self._current_stock = current_stock
            if current_batch is not None:
                self._current_batch = current_batch
            if inserted > 0:
                self._inserted_count += inserted
            if updated > 0:
                self._updated_count += updated
            if message:
                self._message = message

    def set_status(self, status: FactorCalcStatus, message: str = ""):
        """设置状态"""
        with self._sync_lock:
            self._status = status
            if message:
                self._message = message

    def complete(self, inserted: int = None, updated: int = None, error: str = ""):
        """完成计算"""
        with self._sync_lock:
            self._status = FactorCalcStatus.ERROR if error else FactorCalcStatus.COMPLETED
            self._error = error
            self._end_time = get_china_time()
            self._processed_stocks = self._total_stocks
            if inserted is not None:
                self._inserted_count = inserted
            if updated is not None:
                self._updated_count = updated
            if error:
                self._message = f"错误: {error}"
            else:
                self._message = f"完成！插入 {self._inserted_count} 条，更新 {self._updated_count} 条"

    def complete_sync(self, inserted: int = None, updated: int = None, error: str = ""):
        """完成计算（同步版本）"""
        self.complete(inserted, updated, error)

    def get_progress(self) -> Dict[str, Any]:
        """获取当前进度"""
        with self._sync_lock:
            percent = 0
            if self._total_stocks > 0:
                percent = round((self._processed_stocks / self._total_stocks) * 100)

            elapsed_seconds = 0
            estimated_remaining = 0

            if self._start_time:
                end = self._end_time or get_china_time()
                elapsed_seconds = (end - self._start_time).total_seconds()

                if self._processed_stocks > 0 and self._status == FactorCalcStatus.CALCULATING:
                    avg_time_per_stock = elapsed_seconds / self._processed_stocks
                    remaining_stocks = self._total_stocks - self._processed_stocks
                    estimated_remaining = avg_time_per_stock * remaining_stocks

            return {
                "status": self._status.value,
                "total_stocks": self._total_stocks,
                "processed_stocks": self._processed_stocks,
                "inserted_count": self._inserted_count,
                "updated_count": self._updated_count,
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
_tracker: Optional[FactorCalcProgressTracker] = None


def get_factor_calc_tracker() -> FactorCalcProgressTracker:
    """获取因子计算进度跟踪器单例"""
    global _tracker
    if _tracker is None:
        _tracker = FactorCalcProgressTracker()
    return _tracker


def reset_factor_calc_tracker():
    """重置进度跟踪器（用于测试）"""
    global _tracker
    _tracker = None