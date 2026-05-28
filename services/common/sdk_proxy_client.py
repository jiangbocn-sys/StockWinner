"""
SDK 子进程代理客户端

替代原有 SDKManager 的位置，通过 Unix socket 与 SDK 子进程通信。
接口与原有 SDKManager 保持一致，调用方无需修改。

如果子进程死亡（segfault/崩溃），IPC 超时后标记连接不可用，
由 SDKConnectionManager 触发子进程重启。
"""

import os
import socket
import time
import json
import threading
from typing import Optional, Dict
import pandas as pd
from pathlib import Path

from services.common.sdk_ipc import SOCKET_PATH, encode_response, decode_response, send_message, recv_message
from services.common.structured_logger import get_logger


class SDKProxyClient:
    """SDK 子进程代理客户端（替代原 SDKManager 的数据查询部分）"""

    _instance: Optional['SDKProxyClient'] = None
    _lock = threading.Lock()

    def __init__(self):
        self._socket: Optional[socket.socket] = None
        self._subprocess_pid: Optional[int] = None
        self._connected = False
        self._lock = threading.RLock()  # 可重入锁：connect_to_subprocess 和 _call_ipc 可能嵌套调用

    @classmethod
    def get_instance(cls) -> 'SDKProxyClient':
        if cls._instance is None:
            cls._instance = SDKProxyClient()
        return cls._instance

    def reset_instance(self):
        """重置单例（重启后调用）"""
        with self._lock:
            self._close_socket()
            self._connected = False
            self._subprocess_pid = None

    def connect_to_subprocess(self, timeout: float = 5.0) -> bool:
        """连接 SDK 子进程"""
        logger = get_logger("sdk_proxy")
        with self._lock:
            self._close_socket()
            try:
                self._socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                self._socket.settimeout(timeout)
                self._socket.connect(SOCKET_PATH)
                self._connected = True
                logger.log_event("sdk_proxy_connected", f"已连接 SDK 子进程 @ {SOCKET_PATH}")
                return True
            except Exception as e:
                self._connected = False
                logger.log_event("sdk_proxy_connect_failed", f"连接 SDK 子进程失败: {e}")
                return False

    def _close_socket(self):
        """关闭 IPC socket"""
        if self._socket:
            try:
                self._socket.close()
            except Exception:
                pass
            self._socket = None
            self._connected = False

    def _call_ipc(self, method: str, kwargs: dict, priority: int = 1, timeout: float = 30.0) -> any:
        """通过 IPC 调用 SDK 子进程（支持优先级）

        Args:
            method: SDK 方法名
            kwargs: 方法参数
            priority: 优先级 (0=highest, 1=high, 2=medium, 3=low)
            timeout: IPC 超时时间

        Returns:
            SDK 调用结果
        """
        logger = get_logger("sdk_proxy")
        request_id = f"{time.monotonic()}_{os.getpid()}"
        request = {
            "method": method,
            "args": kwargs,
            "request_id": request_id,
            "priority": priority,  # 传递优先级
        }

        with self._lock:
            if not self._connected:
                # 自动重连：socket 文件存在说明子进程还在，只需重建 IPC 连接
                if os.path.exists(SOCKET_PATH):
                    if self.connect_to_subprocess(timeout=3.0):
                        logger.log_event("sdk_ipc_reconnect", "IPC 自动重连成功")
                    else:
                        raise ConnectionError("未连接 SDK 子进程（重连失败）")
                else:
                    raise ConnectionError("未连接 SDK 子进程（socket 不存在）")

            # 动态设置 socket 超时（不同方法需要不同超时时间）
            prev_timeout = self._socket.gettimeout()
            self._socket.settimeout(timeout)

            try:
                send_message(self._socket, request)
                response = recv_message(self._socket)
            except (ConnectionError, OSError, socket.timeout, BrokenPipeError) as e:
                self._connected = False
                logger.log_event("sdk_ipc_disconnected", f"IPC 连接断开: {e}")
                raise ConnectionError(f"SDK 子进程 IPC 断开: {e}")
            finally:
                self._socket.settimeout(prev_timeout)

        if not response.get("success"):
            error_msg = response.get("error", "未知错误")
            logger.log_event("sdk_method_error", f"{method} 子进程返回错误: {error_msg}")
            raise RuntimeError(error_msg)

        return decode_response(response.get("result"))

    # ================================================================
    # 公共接口（与原 SDKManager 一致）
    # ================================================================

    def connect(self) -> bool:
        """确保 SDK 连接可用"""
        try:
            return self._call_ipc("connect", {})
        except Exception as e:
            return False

    def disconnect(self):
        """断开连接"""
        try:
            self._call_ipc("disconnect", {})
        except Exception:
            pass
        self._close_socket()

    def is_connected(self) -> bool:
        """检查连接状态（仅检查，不重连——避免在主线程阻塞拿锁）"""
        if self._connected:
            if not os.path.exists(SOCKET_PATH):
                self._connected = False
                return False
            return True
        return False

    def get_info(self):
        """获取 InfoData 实例（缓存由子进程管理）"""
        return _IPCInfoData(self)

    def get_base_data(self):
        """获取 BaseData 实例（缓存由子进程管理）"""
        return _IPCBaseData(self)

    def get_calendar(self):
        """获取交易日历"""
        return self._call_ipc("get_calendar", {}, timeout=10.0)

    def get_market_data(self):
        """获取 MarketData 实例（缓存由子进程管理）"""
        return _IPCMarketData(self)

    # 直接调用方法（不通过实例缓存）
    def get_equity_structure(self, stock_codes: list, priority: int = 3) -> pd.DataFrame:
        """股权结构（后台任务，默认 low priority）"""
        try:
            result = self._call_ipc("get_equity_structure", {"stock_codes": stock_codes}, priority=priority, timeout=60.0)
            return _to_dataframe(result) if result is not None else pd.DataFrame()
        except Exception:
            return pd.DataFrame()

    def get_income_statement(self, stock_codes: list, priority: int = 3) -> pd.DataFrame:
        """利润表（后台任务，默认 low priority）"""
        try:
            result = self._call_ipc("get_income_statement", {"stock_codes": stock_codes}, priority=priority, timeout=60.0)
            return _to_dataframe(result) if result is not None else pd.DataFrame()
        except Exception:
            return pd.DataFrame()

    def get_balance_sheet(self, stock_codes: list, priority: int = 3) -> pd.DataFrame:
        """资产负债表（后台任务，默认 low priority）"""
        try:
            result = self._call_ipc("get_balance_sheet", {"stock_codes": stock_codes}, priority=priority, timeout=60.0)
            return _to_dataframe(result) if result is not None else pd.DataFrame()
        except Exception:
            return pd.DataFrame()

    def get_cash_flow_statement(self, stock_codes: list, priority: int = 3) -> pd.DataFrame:
        """现金流量表（后台任务，默认 low priority）"""
        try:
            result = self._call_ipc("get_cash_flow_statement", {"stock_codes": stock_codes}, priority=priority, timeout=60.0)
            return _to_dataframe(result) if result is not None else pd.DataFrame()
        except Exception:
            return pd.DataFrame()

    def get_industry_base_info(self, priority: int = 3) -> pd.DataFrame:
        """行业基本信息（后台任务，默认 low priority）"""
        try:
            result = self._call_ipc("get_industry_base_info", {}, priority=priority, timeout=60.0)
            return _to_dataframe(result) if result is not None else pd.DataFrame()
        except Exception:
            return pd.DataFrame()

    def get_code_info(self, security_type: str = 'EXTRA_STOCK_A', priority: int = 3) -> pd.DataFrame:
        """股票基本信息（后台任务，默认 low priority）"""
        try:
            result = self._call_ipc("get_code_info", {"security_type": security_type}, priority=priority, timeout=30.0)
            return _to_dataframe(result) if result is not None else pd.DataFrame()
        except Exception:
            return pd.DataFrame()

    def get_code_list(self, security_type: str = 'EXTRA_STOCK_A', priority: int = 3) -> list:
        """股票列表（后台任务，默认 low priority）"""
        try:
            return self._call_ipc("get_code_list", {"security_type": security_type}, priority=priority, timeout=30.0) or []
        except Exception:
            return []

    def query_kline(self, code_list: list, begin_date: int, end_date: int,
                    period: int, task_type: str = "query", priority: int = 1) -> dict:
        """K线查询

        Args:
            priority: 默认 high (1)，后台下载用 low (3)
        """
        try:
            result = self._call_ipc("query_kline", {
                "code_list": code_list,
                "begin_date": begin_date,
                "end_date": end_date,
                "period": period,
            }, priority=priority, timeout=120.0)
            return result if isinstance(result, dict) else {}
        except Exception:
            return {}

    def query_snapshot(self, code_list: list, begin_date: int, end_date: int, priority: int = 1) -> dict:
        """快照查询（默认 high priority）"""
        try:
            result = self._call_ipc("query_snapshot", {
                "code_list": code_list,
                "begin_date": begin_date,
                "end_date": end_date,
            }, priority=priority, timeout=30.0)
            return result if isinstance(result, dict) else {}
        except Exception:
            return {}

    def get_industry_daily(self, code_list: list, priority: int = 3) -> Dict[str, pd.DataFrame]:
        """行业日线（后台任务，默认 low priority）"""
        try:
            result = self._call_ipc("get_industry_daily", {"code_list": code_list}, priority=priority, timeout=60.0)
            return result if isinstance(result, dict) else {}
        except Exception:
            return {}

    def get_profit_notice(self, stock_codes: list, priority: int = 3) -> pd.DataFrame:
        """业绩预告（后台任务，默认 low priority）"""
        try:
            result = self._call_ipc("get_profit_notice", {"stock_codes": stock_codes}, priority=priority, timeout=60.0)
            return _to_dataframe(result) if result is not None else pd.DataFrame()
        except Exception:
            return pd.DataFrame()

    def get_profit_express(self, stock_codes: list, priority: int = 3) -> pd.DataFrame:
        """业绩快报（后台任务，默认 low priority）"""
        try:
            result = self._call_ipc("get_profit_express", {"stock_codes": stock_codes}, priority=priority, timeout=60.0)
            return _to_dataframe(result) if result is not None else pd.DataFrame()
        except Exception:
            return pd.DataFrame()

    def get_long_hu_bang(self, stock_codes: list, begin_date: int, end_date: int, priority: int = 3) -> pd.DataFrame:
        """龙虎榜（后台任务，默认 low priority）"""
        try:
            result = self._call_ipc("get_long_hu_bang", {
                "stock_codes": stock_codes,
                "begin_date": begin_date,
                "end_date": end_date,
            }, priority=priority, timeout=60.0)
            return _to_dataframe(result) if result is not None else pd.DataFrame()
        except Exception:
            return pd.DataFrame()

    def get_margin_summary(self, begin_date: int, end_date: int, priority: int = 3) -> pd.DataFrame:
        """融资融券汇总（后台任务，默认 low priority）"""
        try:
            result = self._call_ipc("get_margin_summary", {
                "begin_date": begin_date,
                "end_date": end_date,
            }, priority=priority, timeout=60.0)
            return _to_dataframe(result) if result is not None else pd.DataFrame()
        except Exception:
            return pd.DataFrame()

    def get_margin_detail(self, stock_codes: list, begin_date: int, end_date: int, priority: int = 3) -> pd.DataFrame:
        """融资融券明细（后台任务，默认 low priority）"""
        try:
            result = self._call_ipc("get_margin_detail", {
                "stock_codes": stock_codes,
                "begin_date": begin_date,
                "end_date": end_date,
            }, priority=priority, timeout=60.0)
            return _to_dataframe(result) if result is not None else pd.DataFrame()
        except Exception:
            return pd.DataFrame()

    def get_block_trading(self, stock_codes: list, begin_date: int, end_date: int, priority: int = 3) -> pd.DataFrame:
        """大宗交易（后台任务，默认 low priority）"""
        try:
            result = self._call_ipc("get_block_trading", {
                "stock_codes": stock_codes,
                "begin_date": begin_date,
                "end_date": end_date,
            }, priority=priority, timeout=60.0)
            return _to_dataframe(result) if result is not None else pd.DataFrame()
        except Exception:
            return pd.DataFrame()

    def get_treasury_yield(self, priority: int = 3) -> pd.DataFrame:
        """国债收益率（后台任务，默认 low priority）"""
        try:
            result = self._call_ipc("get_treasury_yield", {}, priority=priority, timeout=60.0)
            return _to_dataframe(result) if result is not None else pd.DataFrame()
        except Exception:
            return pd.DataFrame()

    def get_industry_constituent(self, index_codes: list, priority: int = 3) -> pd.DataFrame:
        """行业成分（后台任务，默认 low priority）"""
        try:
            result = self._call_ipc("get_industry_constituent", {"index_codes": index_codes}, priority=priority, timeout=60.0)
            return _to_dataframe(result) if result is not None else pd.DataFrame()
        except Exception:
            return pd.DataFrame()

    def get_index_constituent(self, index_codes: list, priority: int = 3) -> pd.DataFrame:
        """指数成分（后台任务，默认 low priority）"""
        try:
            result = self._call_ipc("get_index_constituent", {"index_codes": index_codes}, priority=priority, timeout=60.0)
            return _to_dataframe(result) if result is not None else pd.DataFrame()
        except Exception:
            return pd.DataFrame()


def _to_dataframe(obj) -> pd.DataFrame:
    """将反序列化结果转为 DataFrame"""
    if isinstance(obj, pd.DataFrame):
        return obj
    return pd.DataFrame()


# ================================================================
# IPC 包装的 SDK 实例（模拟原 InfoData/MarketData/BaseData）
# ================================================================

class _IPCInfoData:
    def __init__(self, client: SDKProxyClient, default_priority: int = 3):
        self._client = client
        self._default_priority = default_priority

    def get_equity_structure(self, stock_codes, priority=None, **kw):
        return self._client.get_equity_structure(stock_codes, priority=priority or self._default_priority)

    def get_income(self, stock_codes, priority=None, **kw):
        return self._client.get_income_statement(stock_codes, priority=priority or self._default_priority)

    def get_balance_sheet(self, stock_codes, priority=None, **kw):
        return self._client.get_balance_sheet(stock_codes, priority=priority or self._default_priority)

    def get_cash_flow(self, stock_codes, priority=None, **kw):
        return self._client.get_cash_flow_statement(stock_codes, priority=priority or self._default_priority)

    def get_industry_base_info(self, priority=None, **kw):
        return self._client.get_industry_base_info(priority=priority or self._default_priority)

    def get_profit_notice(self, code_list, priority=None, **kw):
        return self._client.get_profit_notice(code_list, priority=priority or self._default_priority)

    def get_profit_express(self, code_list, priority=None, **kw):
        return self._client.get_profit_express(code_list, priority=priority or self._default_priority)

    def get_long_hu_bang(self, code_list, begin_date=None, end_date=None, priority=None, **kw):
        return self._client.get_long_hu_bang(code_list, begin_date or 0, end_date or 0, priority=priority or self._default_priority)

    def get_margin_summary(self, begin_date=None, end_date=None, priority=None, **kw):
        return self._client.get_margin_summary(begin_date or 0, end_date or 0, priority=priority or self._default_priority)

    def get_margin_detail(self, code_list, begin_date=None, end_date=None, priority=None, **kw):
        return self._client.get_margin_detail(code_list, begin_date or 0, end_date or 0, priority=priority or self._default_priority)

    def get_block_trading(self, code_list, begin_date=None, end_date=None, priority=None, **kw):
        return self._client.get_block_trading(code_list, begin_date or 0, end_date or 0, priority=priority or self._default_priority)

    def get_treasury_yield(self, priority=None, **kw):
        return self._client.get_treasury_yield(priority=priority or self._default_priority)

    def get_industry_constituent(self, code_list, priority=None, **kw):
        return self._client.get_industry_constituent(code_list, priority=priority or self._default_priority)

    def get_index_constituent(self, code_list, priority=None, **kw):
        return self._client.get_index_constituent(code_list, priority=priority or self._default_priority)

    def get_industry_daily(self, code_list, priority=None, **kw):
        return self._client.get_industry_daily(code_list, priority=priority or self._default_priority)


class _IPCBaseData:
    def __init__(self, client: SDKProxyClient, default_priority: int = 3):
        self._client = client
        self._default_priority = default_priority

    def get_code_info(self, security_type='EXTRA_STOCK_A', priority=None, **kw):
        return self._client.get_code_info(security_type, priority=priority or self._default_priority)

    def get_code_list(self, security_type='EXTRA_STOCK_A', priority=None, **kw):
        return self._client.get_code_list(security_type, priority=priority or self._default_priority)

    def get_calendar(self, **kw):
        return self._client.get_calendar()


class _IPCMarketData:
    def __init__(self, client: SDKProxyClient, default_priority: int = 1):
        self._client = client
        self._default_priority = default_priority

    def query_kline(self, code_list, begin_date, end_date, period, priority=None, **kw):
        return self._client.query_kline(code_list, begin_date, end_date, period, priority=priority or self._default_priority)

    def query_snapshot(self, code_list, begin_date, end_date, priority=None, **kw):
        return self._client.query_snapshot(code_list, begin_date, end_date, priority=priority or self._default_priority)


# ================================================================
# 子进程管理
# ================================================================

class SDKSubprocessManager:
    """管理 SDK 子进程的生命周期"""

    def __init__(self):
        self._subprocess = None
        self._subprocess_pid: Optional[int] = None
        self._ready_event = threading.Event()
        self._lock = threading.Lock()

    def start_subprocess(self) -> bool:
        """启动 SDK 子进程"""
        import subprocess
        logger = get_logger("sdk_subprocess_mgr")

        with self._lock:
            if self._subprocess and self._subprocess.poll() is None:
                logger.log_event("sdk_subprocess_already_running",
                    f"子进程已运行 (PID: {self._subprocess_pid})")
                return True

            # 清理旧 socket
            if os.path.exists(SOCKET_PATH):
                try:
                    os.unlink(SOCKET_PATH)
                except Exception:
                    pass

            # 启动子进程
            python_path = sys.executable or "python3"
            self._subprocess = subprocess.Popen(
                [python_path, "-m", "services.common.sdk_subprocess_server"],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                cwd=str(Path(__file__).resolve().parents[2]),
            )
            self._subprocess_pid = self._subprocess.pid
            logger.log_event("sdk_subprocess_started",
                f"SDK 子进程已启动 PID: {self._subprocess_pid}")

            # 等待子进程就绪（读取 stdout 中的 sdk_ready 消息）
            self._ready_event.clear()
            self._wait_for_ready()

            if not self._ready_event.is_set():
                logger.log_event("sdk_subprocess_start_timeout",
                    "SDK 子进程启动超时")
                self._kill_subprocess()
                return False

            # 连接 IPC
            proxy = SDKProxyClient.get_instance()
            proxy.reset_instance()
            ok = proxy.connect_to_subprocess(timeout=5.0)
            if ok:
                logger.log_event("sdk_subprocess_ready",
                    f"SDK 子进程就绪 PID: {self._subprocess_pid}")
                return True
            else:
                logger.log_event("sdk_subprocess_connect_failed",
                    "SDK 子进程连接失败")
                self._kill_subprocess()
                return False

    def _wait_for_ready(self, timeout: float = 30.0):
        """等待子进程报告 ready"""
        import subprocess
        if not self._subprocess or not self._subprocess.stdout:
            return
        start = time.monotonic()
        while time.monotonic() - start < timeout:
            line = self._subprocess.stdout.readline()
            if not line:
                if self._subprocess.poll() is not None:
                    break  # 子进程已退出
                continue
            try:
                msg = json.loads(line.decode("utf-8").strip())
                if msg.get("event") == "sdk_ready":
                    self._ready_event.set()
                    return
            except Exception:
                continue
        self._ready_event.clear()

    def _kill_subprocess(self):
        """终止子进程

        直接 SIGKILL：让 TCP 发 RST 而非 FIN，TGW 服务端立即释放连接槽，
        避免 TIME_WAIT 导致下次 login 被"连接超限"拒绝。
        """
        logger = get_logger("sdk_subprocess_mgr")
        if self._subprocess:
            try:
                self._subprocess.kill()  # SIGKILL → TCP RST → TGW 立即释放
                self._subprocess.wait(timeout=5)
            except Exception:
                pass
            logger.log_event("sdk_subprocess_killed",
                f"子进程已终止 (PID: {self._subprocess_pid})")
            self._subprocess = None
            self._subprocess_pid = None

    def stop_subprocess(self):
        """停止子进程"""
        self._kill_subprocess()

    def is_subprocess_alive(self) -> bool:
        """检查子进程是否存活"""
        if self._subprocess is None:
            return False
        return self._subprocess.poll() is None


# ================================================================
# 全局单例
# ================================================================

_sdk_proxy: Optional[SDKProxyClient] = None
_subprocess_mgr: Optional[SDKSubprocessManager] = None


def get_sdk_proxy() -> SDKProxyClient:
    """获取 SDK 代理客户端"""
    global _sdk_proxy
    if _sdk_proxy is None:
        _sdk_proxy = SDKProxyClient.get_instance()
    return _sdk_proxy


def get_subprocess_manager() -> SDKSubprocessManager:
    """获取子进程管理器"""
    global _subprocess_mgr
    if _subprocess_mgr is None:
        _subprocess_mgr = SDKSubprocessManager()
    return _subprocess_mgr


import sys
