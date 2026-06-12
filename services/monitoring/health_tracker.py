"""
健康追踪器 — SDK 健康状态、数据新鲜度、通知防抖。
"""
from typing import Optional, List

from services.common.timezone import get_china_time
from services.common.structured_logger import get_logger


class HealthTracker:
    """SDK 健康状态追踪 + 数据新鲜度 + 通知防抖"""

    def __init__(self):
        self._sdk_healthy = True
        self._sdk_error_time: Optional[str] = None
        self._sdk_error_msg: str = ""
        self._consecutive_errors = 0
        self._last_data_time: Optional[str] = None
        self._data_stale = False
        self._last_notify_time: float = 0.0
        self._notify_cooldown: float = 300  # 5 分钟
        self._last_notify_type: str = ""  # 上次通知的类型，同一类型不重复
        self._account_ids: List[str] = []

    def set_account_ids(self, account_ids: List[str]):
        self._account_ids = account_ids

    def record_sdk_success(self):
        """记录 SDK 连接恢复"""
        if not self._sdk_healthy:
            get_logger("monitor").log_event("sdk_recovered", "SDK/TGW 连接已恢复")
        self._sdk_healthy = True
        self._sdk_error_time = None
        self._sdk_error_msg = ""
        self._consecutive_errors = 0
        self._last_notify_type = ""  # 重置通知状态，允许下次异常时再次通知

    def record_sdk_error(self, error: Exception):
        """记录 SDK 连接失败"""
        self._sdk_healthy = False
        self._sdk_error_time = get_china_time().strftime("%Y-%m-%d %H:%M:%S")
        self._sdk_error_msg = str(error)
        self._consecutive_errors += 1
        self._data_stale = True
        self._notify_sdk_event("SDK 连接失败", f"监控循环获取行情失败: {error}")

    def record_data_valid(self, valid_count: int, total_count: int):
        """记录数据有效性检查结果"""
        if valid_count > 0:
            self._last_data_time = get_china_time().strftime("%Y-%m-%d %H:%M:%S")
            if self._data_stale:
                get_logger("monitor").log_event("data_recovered", f"行情数据已恢复，有效股票数={valid_count}")
                self._data_stale = False
        elif total_count > 0:
            self._data_stale = True
            self._notify_sdk_event("行情数据异常", f"SDK 返回 {total_count} 只股票但全部现价无效")

    def is_healthy(self) -> bool:
        return self._sdk_healthy

    def reset_if_sdk_connected(self) -> bool:
        """检查 SDK 连接状态，如果正常则重置健康状态

        用于非交易时段恢复状态（监控休眠时调用）。
        返回 True 表示状态已恢复，False 表示 SDK 仍有问题。
        """
        try:
            from services.common.sdk_manager import get_sdk_manager
            sdk_mgr = get_sdk_manager()
            if sdk_mgr.is_connected():
                self._sdk_healthy = True
                self._sdk_error_time = None
                self._sdk_error_msg = ""
                self._consecutive_errors = 0
                # 数据状态不自动恢复，需要实际数据刷新验证
                get_logger("monitor").log_event("health_auto_reset", "SDK 连接正常，健康状态已重置")
                return True
        except Exception as e:
            get_logger("monitor").log_event("health_reset_failed", f"SDK 连接检查失败: {e}")
        return False

    def get_status(self) -> dict:
        return {
            "sdk_healthy": self._sdk_healthy,
            "sdk_error_time": self._sdk_error_time,
            "sdk_error_msg": self._sdk_error_msg,
            "consecutive_errors": self._consecutive_errors,
            "data_stale": self._data_stale,
            "last_data_time": self._last_data_time,
        }

    def _notify_sdk_event(self, issue_type: str, detail: str):
        """发送 SDK 异常飞书通知（仅状态变化时发送一次，持续异常不重复）

        使用 run_coroutine_threadsafe 提交到主事件循环，避免临时循环导致 aiosqlite 连接失效。
        """
        import time
        import asyncio

        # 同一 issue 已通知过，不再重复
        if issue_type == self._last_notify_type:
            return

        try:
            from services.notifications import get_notification_manager
            notification = get_notification_manager()

            # 获取主事件循环（FastAPI/uvicorn 的循环）
            try:
                main_loop = asyncio.get_event_loop()
                if main_loop.is_closed():
                    get_logger("monitor").log_event("notify_loop_closed", "主事件循环已关闭，跳过通知")
                    return
            except RuntimeError:
                # 没有运行中的循环（可能在线程中调用）
                get_logger("monitor").log_event("notify_no_loop", "无法获取事件循环，跳过通知")
                return

            for acct_id in self._account_ids:
                # 提交到主循环执行，不创建临时循环
                future = asyncio.run_coroutine_threadsafe(
                    notification.trigger(
                        event_type="sdk_connection_error",
                        account_id=acct_id,
                        payload={
                            "detected_at": get_china_time().strftime("%Y-%m-%d %H:%M:%S"),
                            "issue": issue_type,
                            "detail": detail,
                        },
                    ),
                    main_loop
                )
                # 等待完成（最多 5 秒）
                try:
                    future.result(timeout=5.0)
                except Exception as e:
                    get_logger("monitor").log_event("notify_failed", f"通知发送失败: {e}")

            self._last_notify_time = time.time()
            self._last_notify_type = issue_type
            get_logger("monitor").log_event("sdk_notify_sent", f"已发送飞书通知: {issue_type}")
        except Exception as e:
            get_logger("monitor").log_event("notify_error", f"通知发送异常: {e}")
