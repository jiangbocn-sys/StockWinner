"""
SDK 管理器（v7.5.0 — 子进程隔离架构）

所有 SDK 数据调用通过 IPC 代理 → SDK 子进程
- SDK segfault 只杀死子进程，不影响主进程和其他数据源
- SDK 子进程负责 login + 所有 SDK 调用
- SDKConnectionManager 管理子进程生命周期
- 接口与旧版本一致，调用方无需修改
"""

import time
from collections import deque
from threading import Lock
from typing import Optional, Dict
import pandas as pd

from services.common.structured_logger import get_logger


class SDKManager:
    """SDK 管理器（单例模式）

    所有 SDK 数据调用通过 IPC 代理 → SDK 子进程
    连接生命周期委托给 SDKConnectionManager（管理子进程）
    """

    _instance: Optional['SDKManager'] = None
    _sdk_metrics_lock: Optional[Lock] = None
    _sdk_total_calls = 0
    _sdk_success_calls = 0
    _sdk_total_rows = 0
    _sdk_call_log: Optional[deque] = None
    _proxy = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if SDKManager._sdk_metrics_lock is None:
            SDKManager._sdk_metrics_lock = Lock()
            SDKManager._sdk_total_calls = 0
            SDKManager._sdk_success_calls = 0
            SDKManager._sdk_total_rows = 0
            SDKManager._sdk_call_log = deque(maxlen=200)

    @classmethod
    def get_instance(cls) -> 'SDKManager':
        if cls._instance is None:
            cls._instance = SDKManager()
        return cls._instance

    def _get_proxy(self):
        """获取 IPC 代理客户端（延迟初始化）"""
        if SDKManager._proxy is None:
            from services.common.sdk_proxy_client import get_sdk_proxy
            SDKManager._proxy = get_sdk_proxy()
        return SDKManager._proxy

    def _get_conn_mgr(self):
        from services.common.sdk_connection_manager import get_connection_manager
        return get_connection_manager()

    def _record_sdk_call(self, method: str, row_count: int = 0, success: bool = True):
        with SDKManager._sdk_metrics_lock:
            SDKManager._sdk_total_calls += 1
            if success:
                SDKManager._sdk_success_calls += 1
            SDKManager._sdk_total_rows += row_count
            if SDKManager._sdk_call_log is not None:
                SDKManager._sdk_call_log.append((time.time(), method, row_count, success))

    @staticmethod
    def _count_result_rows(result) -> int:
        if isinstance(result, pd.DataFrame):
            return len(result)
        if isinstance(result, dict):
            return sum(len(v) for v in result.values() if isinstance(v, pd.DataFrame))
        if isinstance(result, list):
            return len(result)
        return 0

    def get_sdk_metrics(self) -> Dict:
        now = time.time()
        with SDKManager._sdk_metrics_lock:
            recent_calls = [e for e in (SDKManager._sdk_call_log or []) if now - e[0] <= 60]
            recent_count = len(recent_calls)
            recent_rows = sum(e[2] for e in recent_calls)
            recent_success = sum(1 for e in recent_calls if e[3])
            recent_rate = round(recent_success / recent_count * 100, 1) if recent_count > 0 else 0
            method_counts: Dict[str, int] = {}
            for _, method, _, _ in recent_calls:
                method_counts[method] = method_counts.get(method, 0) + 1
            return {
                "recent_60s": {
                    "calls": recent_count, "rows": recent_rows,
                    "success_rate": recent_rate,
                    "active_methods": sorted(method_counts.keys()),
                },
                "session": {
                    "total_calls": SDKManager._sdk_total_calls,
                    "success_calls": SDKManager._sdk_success_calls,
                    "total_rows": SDKManager._sdk_total_rows,
                },
            }

    # ================================================================
    # 连接管理
    # ================================================================

    def connect(self) -> bool:
        """确保 SDK 连接可用（通过 IPC 代理检查子进程是否就绪）"""
        try:
            return self._get_proxy().is_connected()
        except Exception:
            return False

    def disconnect(self):
        """主动断开连接（通过 IPC 代理 + 停止子进程）"""
        try:
            self._get_proxy().disconnect()
        except Exception:
            pass
        try:
            from services.common.sdk_proxy_client import get_subprocess_manager
            get_subprocess_manager().stop_subprocess()
        except Exception:
            pass

    def is_connected(self) -> bool:
        """检查 SDK 连接状态（通过 IPC 代理）"""
        try:
            return self._get_proxy().is_connected()
        except Exception:
            return False

    # ================================================================
    # 实例获取（IPC 代理包装）
    # ================================================================

    def get_info(self):
        """获取 InfoData 代理（实例缓存在子进程）"""
        return self._get_proxy().get_info()

    def get_base_data(self):
        """获取 BaseData 代理（实例缓存在子进程）"""
        return self._get_proxy().get_base_data()

    def get_calendar(self):
        """获取交易日历"""
        return self._get_proxy().get_calendar()

    def get_market_data(self):
        """获取 MarketData 代理（实例缓存在子进程）"""
        return self._get_proxy().get_market_data()

    # ================================================================
    # 数据获取方法 — 所有调用通过 IPC 代理
    # ================================================================

    def _call_ipc(self, method_name: str, kwargs: dict, task_type: str = "query",
                  timeout: float = 30.0, logger=None, **log_context):
        """通过 IPC 代理调用 SDK 子进程，记录日志和统计"""
        if logger is None:
            logger = get_logger("sdk_manager")

        start = time.monotonic()
        try:
            proxy = self._get_proxy()
            proxy_method = getattr(proxy, method_name)
            result = proxy_method(**kwargs)
            duration_ms = (time.monotonic() - start) * 1000
            logger.log_sdk_call(method_name, duration_ms, task_type, "success", **log_context)
            self._record_sdk_call(method_name, self._count_result_rows(result), True)
            return result
        except ConnectionError as e:
            duration_ms = (time.monotonic() - start) * 1000
            reason = f"{method_name} IPC 断开: {e}"
            logger.log_sdk_call(method_name, duration_ms, task_type, "ipc_disconnect", error=reason, **log_context)
            # IPC 断开 → 子进程可能已死亡 → 触发重启
            try:
                proxy = SDKManager._proxy
                if proxy:
                    proxy.reset_instance()
                from services.common.sdk_proxy_client import get_subprocess_manager
                sub_mgr = get_subprocess_manager()
                if not sub_mgr.is_subprocess_alive():
                    sub_mgr.start_subprocess()
            except Exception:
                pass
            raise
        except Exception as e:
            duration_ms = (time.monotonic() - start) * 1000
            logger.log_sdk_call(method_name, duration_ms, task_type, "error", error=str(e), **log_context)
            raise

    def get_equity_structure(self, stock_codes: list) -> pd.DataFrame:
        try:
            result = self._call_ipc("get_equity_structure", {"stock_codes": stock_codes},
                                    "download", 60.0, stock_count=len(stock_codes) if stock_codes else 0)
            if isinstance(result, dict):
                dfs = [df for df in result.values() if isinstance(df, pd.DataFrame)]
                return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
            return result if isinstance(result, pd.DataFrame) else pd.DataFrame()
        except Exception:
            return pd.DataFrame()

    def get_income_statement(self, stock_codes: list) -> pd.DataFrame:
        try:
            result = self._call_ipc("get_income_statement", {"stock_codes": stock_codes},
                                    "download", 60.0, stock_count=len(stock_codes) if stock_codes else 0)
            if isinstance(result, dict):
                dfs = [df for df in result.values() if isinstance(df, pd.DataFrame)]
                return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
            return result if isinstance(result, pd.DataFrame) else pd.DataFrame()
        except Exception:
            return pd.DataFrame()

    def get_balance_sheet(self, stock_codes: list) -> pd.DataFrame:
        try:
            result = self._call_ipc("get_balance_sheet", {"stock_codes": stock_codes},
                                    "download", 60.0, stock_count=len(stock_codes) if stock_codes else 0)
            if isinstance(result, dict):
                dfs = [df for df in result.values() if isinstance(df, pd.DataFrame)]
                return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
            return result if isinstance(result, pd.DataFrame) else pd.DataFrame()
        except Exception:
            return pd.DataFrame()

    def get_cash_flow_statement(self, stock_codes: list) -> pd.DataFrame:
        try:
            result = self._call_ipc("get_cash_flow_statement", {"stock_codes": stock_codes},
                                    "download", 60.0, stock_count=len(stock_codes) if stock_codes else 0)
            if isinstance(result, dict):
                dfs = [df for df in result.values() if isinstance(df, pd.DataFrame)]
                return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
            return result if isinstance(result, pd.DataFrame) else pd.DataFrame()
        except Exception:
            return pd.DataFrame()

    def get_industry_base_info(self) -> pd.DataFrame:
        try:
            result = self._call_ipc("get_industry_base_info", {}, "download", 60.0)
            if isinstance(result, dict):
                dfs = [df for df in result.values() if isinstance(df, pd.DataFrame)]
                return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
            return result if isinstance(result, pd.DataFrame) else pd.DataFrame()
        except Exception:
            return pd.DataFrame()

    def get_code_info(self, security_type: str = 'EXTRA_STOCK_A') -> pd.DataFrame:
        try:
            result = self._call_ipc("get_code_info", {"security_type": security_type}, "query", 30.0)
            return result if isinstance(result, pd.DataFrame) else pd.DataFrame()
        except Exception:
            return pd.DataFrame()

    def get_code_list(self, security_type: str = 'EXTRA_STOCK_A') -> list:
        try:
            return self._call_ipc("get_code_list", {"security_type": security_type}, "query", 30.0) or []
        except Exception:
            return []

    def query_kline(self, code_list: list, begin_date: int, end_date: int,
                    period: int, task_type: str = "query") -> dict:
        stock_count = len(code_list) if isinstance(code_list, list) else 1
        if task_type == "query":
            timeout = 10.0 if stock_count <= 5 else (20.0 if stock_count <= 20 else (60.0 if stock_count <= 100 else 120.0))
        else:
            timeout = max(min(stock_count * 0.2 + 30, 180.0), 30.0)
        try:
            result = self._call_ipc("query_kline", {
                "code_list": code_list, "begin_date": begin_date,
                "end_date": end_date, "period": period,
            }, task_type, timeout, stock_count=stock_count, timeout=timeout)
            return result if isinstance(result, dict) else {}
        except Exception:
            return {}

    def query_snapshot(self, code_list: list, begin_date: int, end_date: int) -> dict:
        stock_count = len(code_list) if isinstance(code_list, list) else 1
        try:
            result = self._call_ipc("query_snapshot", {
                "code_list": code_list, "begin_date": begin_date, "end_date": end_date,
            }, "query", 30.0, stock_count=stock_count, timeout=30.0)
            return result if isinstance(result, dict) else {}
        except Exception:
            return {}

    def get_industry_daily(self, code_list: list) -> Dict[str, pd.DataFrame]:
        try:
            result = self._call_ipc("get_industry_daily", {"code_list": code_list},
                                    "download", 60.0, stock_count=len(code_list) if code_list else 0)
            return result if isinstance(result, dict) else {}
        except Exception:
            return {}

    def get_profit_notice(self, stock_codes: list) -> pd.DataFrame:
        try:
            result = self._call_ipc("get_profit_notice", {"stock_codes": stock_codes},
                                    "download", 60.0, stock_count=len(stock_codes) if stock_codes else 0)
            if isinstance(result, dict):
                dfs = [df for df in result.values() if isinstance(df, pd.DataFrame)]
                return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
            return result if isinstance(result, pd.DataFrame) else pd.DataFrame()
        except Exception:
            return pd.DataFrame()

    def get_profit_express(self, stock_codes: list) -> pd.DataFrame:
        try:
            result = self._call_ipc("get_profit_express", {"stock_codes": stock_codes},
                                    "download", 60.0, stock_count=len(stock_codes) if stock_codes else 0)
            if isinstance(result, dict):
                dfs = [df for df in result.values() if isinstance(df, pd.DataFrame)]
                return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
            return result if isinstance(result, pd.DataFrame) else pd.DataFrame()
        except Exception:
            return pd.DataFrame()

    def get_long_hu_bang(self, stock_codes: list, begin_date: int, end_date: int) -> pd.DataFrame:
        try:
            result = self._call_ipc("get_long_hu_bang", {
                "stock_codes": stock_codes, "begin_date": begin_date, "end_date": end_date,
            }, "download", 60.0, stock_count=len(stock_codes) if stock_codes else 0)
            return result if isinstance(result, pd.DataFrame) else pd.DataFrame()
        except Exception:
            return pd.DataFrame()

    def get_margin_summary(self, begin_date: int, end_date: int) -> pd.DataFrame:
        try:
            result = self._call_ipc("get_margin_summary", {
                "begin_date": begin_date, "end_date": end_date,
            }, "download", 60.0)
            return result if isinstance(result, pd.DataFrame) else pd.DataFrame()
        except Exception:
            return pd.DataFrame()

    def get_margin_detail(self, stock_codes: list, begin_date: int, end_date: int) -> pd.DataFrame:
        try:
            result = self._call_ipc("get_margin_detail", {
                "stock_codes": stock_codes, "begin_date": begin_date, "end_date": end_date,
            }, "download", 60.0, stock_count=len(stock_codes) if stock_codes else 0)
            if isinstance(result, dict):
                dfs = [df for df in result.values() if isinstance(df, pd.DataFrame)]
                return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
            return result if isinstance(result, pd.DataFrame) else pd.DataFrame()
        except Exception:
            return pd.DataFrame()

    def get_block_trading(self, stock_codes: list, begin_date: int, end_date: int) -> pd.DataFrame:
        try:
            result = self._call_ipc("get_block_trading", {
                "stock_codes": stock_codes, "begin_date": begin_date, "end_date": end_date,
            }, "download", 60.0, stock_count=len(stock_codes) if stock_codes else 0)
            return result if isinstance(result, pd.DataFrame) else pd.DataFrame()
        except Exception:
            return pd.DataFrame()

    def get_treasury_yield(self) -> pd.DataFrame:
        try:
            result = self._call_ipc("get_treasury_yield", {}, "download", 60.0)
            if isinstance(result, dict):
                dfs = [df for df in result.values() if isinstance(df, pd.DataFrame)]
                return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
            return result if isinstance(result, pd.DataFrame) else pd.DataFrame()
        except Exception:
            return pd.DataFrame()

    def get_industry_constituent(self, index_codes: list) -> pd.DataFrame:
        try:
            result = self._call_ipc("get_industry_constituent", {"index_codes": index_codes},
                                    "download", 60.0, stock_count=len(index_codes) if index_codes else 0)
            if isinstance(result, dict):
                dfs = [df for df in result.values() if isinstance(df, pd.DataFrame)]
                return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
            return result if isinstance(result, pd.DataFrame) else pd.DataFrame()
        except Exception:
            return pd.DataFrame()

    def get_index_constituent(self, index_codes: list) -> pd.DataFrame:
        try:
            result = self._call_ipc("get_index_constituent", {"index_codes": index_codes},
                                    "download", 60.0, stock_count=len(index_codes) if index_codes else 0)
            if isinstance(result, dict):
                dfs = [df for df in result.values() if isinstance(df, pd.DataFrame)]
                return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
            return result if isinstance(result, pd.DataFrame) else pd.DataFrame()
        except Exception:
            return pd.DataFrame()


# SDK 登录信息 - 从环境变量读取
import os
SDK_USERNAME = os.environ.get("SDK_USERNAME", "")
SDK_PASSWORD = os.environ.get("SDK_PASSWORD", "")
SDK_HOST = os.environ.get("SDK_HOST", "")
SDK_PORT = int(os.environ.get("SDK_PORT", "8600"))


# 全局单例
_sdk_manager: Optional[SDKManager] = None


def get_sdk_manager() -> SDKManager:
    global _sdk_manager
    if _sdk_manager is None:
        _sdk_manager = SDKManager()
    return _sdk_manager


def reset_sdk_manager():
    global _sdk_manager
    _sdk_manager = None
