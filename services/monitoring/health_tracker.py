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
        """发送 SDK 异常飞书通知（带防抖，5 分钟内不重复）"""
        import time
        import asyncio

        now = time.time()
        if now - self._last_notify_time < self._notify_cooldown:
            return

        try:
            from services.notifications import get_notification_service
            notification = get_notification_service()
            for acct_id in self._account_ids:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    loop.run_until_complete(notification.emit(
                        event_type="sdk_connection_error",
                        account_id=acct_id,
                        payload={
                            "detected_at": get_china_time().strftime("%Y-%m-%d %H:%M:%S"),
                            "issue": issue_type,
                            "detail": detail,
                        },
                    ))
                finally:
                    loop.close()
            self._last_notify_time = now
            get_logger("monitor").log_event("sdk_notify_sent", f"已发送飞书通知: {issue_type}")
        except Exception:
            pass
