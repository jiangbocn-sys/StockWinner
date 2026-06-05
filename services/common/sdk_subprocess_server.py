"""
SDK 子进程服务端

运行在独立进程中，负责：
1. SDK 登录/登出
2. 处理主进程通过 IPC 发送的 SDK 查询请求
3. 如果发生 segfault，子进程死亡，主进程检测到 IPC 断开后重启

用法：
    python3 -m services.common.sdk_subprocess_server
"""

import os
import sys
import json
import time
import socket
import signal
import threading
from pathlib import Path
from typing import Optional

# pandas 2.x 兼容：'S' (大写) 频率别名被移除导致 snapshot DataFrame 构造失败
# monkeypatch to_offset() 将大写 'S' 转为小写 's'
try:
    import pandas as pd
    from pandas.tseries import frequencies as _freq
    _orig_to_offset = _freq.to_offset

    def _patched_to_offset(freq, *args, **kwargs):
        if isinstance(freq, str) and freq.upper() == 'S' and len(freq) == 1:
            freq = freq.lower()
        return _orig_to_offset(freq, *args, **kwargs)

    _freq.to_offset = _patched_to_offset
except Exception:
    pass

# 加载 .env
from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parents[2] / ".env")

from services.common.sdk_ipc import (
    SOCKET_PATH, encode_response, send_message, recv_message
)
from services.common.structured_logger import get_logger


# ============================================================
# SDK 登录
# ============================================================

def sdk_login(timeout: float = 20.0) -> bool:
    """SDK 登录（子进程启动时调用，带超时）

    timeout 默认 20 秒，加上其他初始化操作（约 5 秒），
    总启动时间控制在 25 秒内，留出 5 秒缓冲给主进程等待（30 秒）。
    """
    logger = get_logger("sdk_subprocess")
    username = os.environ.get("SDK_USERNAME", "")
    password = os.environ.get("SDK_PASSWORD", "")
    host = os.environ.get("SDK_HOST", "")
    port = int(os.environ.get("SDK_PORT", "8600"))

    if not username or not host:
        logger.error("sdk_subprocess", "SDK 凭证未配置")
        return False

    login_result = [None]  # 使用列表存储结果（线程间共享）
    login_error = [None]

    def _do_login():
        try:
            from AmazingData import login
            login_result[0] = login(username, password, host, port)
        except Exception as e:
            login_error[0] = e

    # 启动登录线程
    login_thread = threading.Thread(target=_do_login, daemon=True)
    login_thread.start()
    login_thread.join(timeout=timeout)

    if login_thread.is_alive():
        # 登录超时
        logger.log_event("sdk_login_timeout", f"SDK 登录超时 ({timeout}s)")
        print(json.dumps({"event": "sdk_login_timeout"}), flush=True)
        return False

    if login_error[0]:
        logger.error("sdk_subprocess_login_error", f"SDK 登录异常: {login_error[0]}")
        return False

    if login_result[0]:
        logger.log_event("sdk_subprocess_login_ok", "SDK 登录成功")
        return True
    else:
        logger.warn("sdk_subprocess_login_fail", "SDK 登录失败（返回 False）")
        return False


# ============================================================
# SDK 实例缓存
# ============================================================

_info_instance = None
_market_data_instance = None
_base_data_instance = None
_calendar = None


def get_info():
    global _info_instance
    if _info_instance is None:
        from AmazingData import InfoData
        _info_instance = InfoData()
    return _info_instance


def get_base_data():
    global _base_data_instance
    if _base_data_instance is None:
        from AmazingData import BaseData
        _base_data_instance = BaseData()
    return _base_data_instance


def get_calendar():
    global _calendar
    if _calendar is None:
        _calendar = get_base_data().get_calendar()
    return _calendar


def get_market_data():
    global _market_data_instance
    if _market_data_instance is None:
        from AmazingData import MarketData
        _market_data_instance = MarketData(get_calendar())
    return _market_data_instance


def clear_instances():
    global _info_instance, _market_data_instance, _base_data_instance, _calendar
    _info_instance = None
    _market_data_instance = None
    _base_data_instance = None
    _calendar = None


# ============================================================
# SDK 方法映射
# ============================================================

SDK_METHODS = {
    "get_equity_structure",
    "get_income_statement",
    "get_balance_sheet",
    "get_cash_flow_statement",
    "get_industry_base_info",
    "get_code_info",
    "get_code_list",
    "get_calendar",
    "query_kline",
    "query_snapshot",
    "get_industry_daily",
    "get_profit_notice",
    "get_profit_express",
    "get_long_hu_bang",
    "get_margin_summary",
    "get_margin_detail",
    "get_block_trading",
    "get_treasury_yield",
    "get_industry_constituent",
    "get_index_constituent",
    "get_adj_factor",      # 复权因子（前复权/后复权）
    "get_etf_pcf",         # ETF 申赎数据
    "get_fund_share",      # ETF 基金份额
    "get_fund_iopv",       # ETF IOPV 净值
    "connect",        # 等效于确保登录
    "disconnect",     # 清理实例
    "is_connected",   # 返回 True（如果已登录）
}


def _sanitize_result(result):
    """修复 DataFrame 中的 pandas 2.x 不兼容频率（'S' → 's'）"""
    try:
        import pandas as pd
        if isinstance(result, pd.DataFrame):
            for col in result.columns:
                col_data = result[col]
                if hasattr(col_data, 'dtype') and hasattr(col_data.dtype, 'freq'):
                    freq = col_data.dtype.freq
                    if freq is not None and hasattr(freq, 'freqstr'):
                        if freq.freqstr == 'S':
                            result[col] = col_data.asfreq(None)
        elif isinstance(result, dict):
            for key, val in result.items():
                if isinstance(val, pd.DataFrame):
                    result[key] = _sanitize_result(val)
    except Exception:
        pass
    return result


def execute_sdk_method(method: str, kwargs: dict):
    """执行 SDK 方法并返回结果"""
    logger = get_logger("sdk_subprocess")
    start = time.monotonic()

    try:
        if method == "connect":
            # 子进程启动时已 login，connect 只需确认
            return True

        elif method == "disconnect":
            clear_instances()
            return None

        elif method == "is_connected":
            return True

        elif method == "get_calendar":
            return get_calendar()

        elif method == "query_kline":
            md = get_market_data()
            return md.query_kline(
                code_list=kwargs.get("code_list"),
                begin_date=kwargs.get("begin_date"),
                end_date=kwargs.get("end_date"),
                period=kwargs.get("period"),
            )

        elif method == "query_snapshot":
            md = get_market_data()
            try:
                return md.query_snapshot(
                    code_list=kwargs.get("code_list"),
                    begin_date=kwargs.get("begin_date"),
                    end_date=kwargs.get("end_date"),
                )
            except ValueError as e:
                if "Invalid frequency" in str(e):
                    # pandas 2.x 不再支持 'S' 频率，snapshot 无法使用，快速返回空让 fallback 处理
                    logger.log_event("sdk_snapshot_skip", f"snapshot 频率不兼容，跳过")
                    return {}
                raise

        elif method == "get_equity_structure":
            return get_info().get_equity_structure(
                kwargs.get("stock_codes"), is_local=False
            )

        elif method == "get_income_statement":
            return get_info().get_income(
                kwargs.get("stock_codes"), is_local=False
            )

        elif method == "get_balance_sheet":
            return get_info().get_balance_sheet(
                kwargs.get("stock_codes"), is_local=False
            )

        elif method == "get_cash_flow_statement":
            return get_info().get_cash_flow(
                kwargs.get("stock_codes"), is_local=False
            )

        elif method == "get_industry_base_info":
            return get_info().get_industry_base_info(is_local=False)

        elif method == "get_code_info":
            return get_base_data().get_code_info(
                security_type=kwargs.get("security_type", "EXTRA_STOCK_A")
            )

        elif method == "get_code_list":
            return get_base_data().get_code_list(
                security_type=kwargs.get("security_type", "EXTRA_STOCK_A")
            )

        elif method == "get_industry_daily":
            return get_info().get_industry_daily(
                code_list=kwargs.get("code_list"), is_local=False
            )

        elif method == "get_profit_notice":
            return get_info().get_profit_notice(
                code_list=kwargs.get("stock_codes"), is_local=False
            )

        elif method == "get_profit_express":
            return get_info().get_profit_express(
                code_list=kwargs.get("stock_codes"), is_local=False
            )

        elif method == "get_long_hu_bang":
            return get_info().get_long_hu_bang(
                code_list=kwargs.get("stock_codes"),
                begin_date=kwargs.get("begin_date"),
                end_date=kwargs.get("end_date"),
                is_local=False,
            )

        elif method == "get_margin_summary":
            return get_info().get_margin_summary(
                begin_date=kwargs.get("begin_date"),
                end_date=kwargs.get("end_date"),
                is_local=False,
            )

        elif method == "get_margin_detail":
            return get_info().get_margin_detail(
                code_list=kwargs.get("stock_codes"),
                begin_date=kwargs.get("begin_date"),
                end_date=kwargs.get("end_date"),
                is_local=False,
            )

        elif method == "get_block_trading":
            return get_info().get_block_trading(
                code_list=kwargs.get("stock_codes"),
                begin_date=kwargs.get("begin_date"),
                end_date=kwargs.get("end_date"),
                is_local=False,
            )

        elif method == "get_treasury_yield":
            return get_info().get_treasury_yield(is_local=False)

        elif method == "get_industry_constituent":
            return get_info().get_industry_constituent(
                code_list=kwargs.get("index_codes"), is_local=False
            )

        elif method == "get_index_constituent":
            return get_info().get_index_constituent(
                code_list=kwargs.get("index_codes"), is_local=False
            )

        elif method == "get_adj_factor":
            # 获取复权因子（BaseData.get_adj_factor）
            # handbook: 需要 code_list, local_path, is_local 三个参数
            local_path = "/home/bobo/StockWinner/data/adj_factor/"
            import os
            os.makedirs(local_path, exist_ok=True)
            return get_base_data().get_adj_factor(
                code_list=kwargs.get("stock_codes", []),
                local_path=local_path,
                is_local=False  # 从服务端获取最新数据
            )

        elif method == "get_etf_pcf":
            # ETF 申赎数据（BaseData.get_etf_pcf）
            # 返回 (etf_pcf_info, etf_pcf_constituent_dict)
            # 注意：PCF 数据可能只在盘前公布，盘中查询可能返回 None
            result = get_base_data().get_etf_pcf(
                code_list=kwargs.get("etf_codes", [])
            )
            # 处理 SDK 返回 None 的情况（非盘前时间或无数据）
            if result is None:
                logger.log_event("etf_pcf_empty", "PCF 数据为空（可能非盘前时间）")
                return {"pcf_info": pd.DataFrame(), "constituents": {}}
            pcf_info, constituents = result
            return {"pcf_info": pcf_info, "constituents": constituents}

        elif method == "get_fund_share":
            # ETF 基金份额（InfoData.get_fund_share）
            return get_info().get_fund_share(
                code_list=kwargs.get("etf_codes", []), is_local=False
            )

        elif method == "get_fund_iopv":
            # ETF IOPV 净值（InfoData.get_fund_iopv）
            return get_info().get_fund_iopv(
                code_list=kwargs.get("etf_codes", []), is_local=False
            )

        else:
            raise ValueError(f"未知 SDK 方法: {method}")

    except Exception as e:
        duration_ms = (time.monotonic() - start) * 1000
        import traceback as _tb
        tb_str = _tb.format_exc()
        logger.log_event("sdk_method_error", f"{method} 异常: {e}\n{tb_str}")
        raise


# ============================================================
# IPC 服务循环
# ============================================================

# 全局锁：串行化所有 SDK 调用（TGW 单连接限制，不能并发调用）
_sdk_lock = threading.Lock()


class PriorityRequestQueue:
    """优先级请求队列

    四级优先级：
    - HIGHEST (0): 立即执行 - pending信号、止损触发
    - HIGH (1): 优先执行 - 用户查询、持仓刷新
    - MEDIUM (2): 常规执行 - 策略评估、批量查询
    - LOW (3): 空闲执行 - Watchlist刷新、后台下载
    """

    HIGHEST = 0
    HIGH = 1
    MEDIUM = 2
    LOW = 3

    def __init__(self):
        self._queues = {
            self.HIGHEST: [],
            self.HIGH: [],
            self.MEDIUM: [],
            self.LOW: [],
        }
        self._lock = threading.Lock()
        self._condition = threading.Condition(self._lock)

    def enqueue(self, request: dict, conn: socket.socket, priority: int):
        """按优先级入队"""
        priority = max(self.HIGHEST, min(self.LOW, priority))  # 确保范围有效
        with self._lock:
            self._queues[priority].append({
                "request": request,
                "conn": conn,
                "priority": priority,
            })
            self._condition.notify()  # 通知工作线程

    def dequeue(self, timeout: float = 0.5) -> Optional[dict]:
        """按优先级出队（高优先级优先）"""
        with self._condition:
            # 等待队列非空
            while not self._has_any():
                if not self._condition.wait(timeout):
                    return None  # 超时返回 None

            # 按优先级顺序取出
            for priority in [self.HIGHEST, self.HIGH, self.MEDIUM, self.LOW]:
                if self._queues[priority]:
                    return self._queues[priority].pop(0)
            return None

    def _has_any(self) -> bool:
        """检查是否有任何请求"""
        return any(len(q) > 0 for q in self._queues.values())

    def size(self) -> dict:
        """返回各队列大小"""
        with self._lock:
            return {
                "highest": len(self._queues[self.HIGHEST]),
                "high": len(self._queues[self.HIGH]),
                "medium": len(self._queues[self.MEDIUM]),
                "low": len(self._queues[self.LOW]),
            }


# 全局优先级队列
_priority_queue: Optional[PriorityRequestQueue] = None
_queue_worker_running = False


def _process_request(item: dict):
    """处理单个请求（SDK 调用）"""
    request = item["request"]
    conn = item["conn"]
    method = request.get("method")
    kwargs = request.get("args", {})
    request_id = request.get("request_id")
    priority = item.get("priority", PriorityRequestQueue.HIGH)

    logger = get_logger("sdk_subprocess")

    # 串行化 SDK 调用
    with _sdk_lock:
        try:
            result = execute_sdk_method(method, kwargs)
            response = {
                "request_id": request_id,
                "success": True,
                "result": encode_response(result),
            }
        except Exception as e:
            response = {
                "request_id": request_id,
                "success": False,
                "error": f"{type(e).__name__}: {e}",
                "result": encode_response(None),
            }

        try:
            send_message(conn, response)
        except (ConnectionError, OSError):
            # 客户端已断开，忽略
            pass


def _queue_worker():
    """优先级队列工作线程"""
    global _queue_worker_running
    logger = get_logger("sdk_subprocess")
    logger.log_event("sdk_queue_worker_started", "优先级队列工作线程启动")

    while _queue_worker_running:
        item = _priority_queue.dequeue(timeout=0.5)
        if item is None:
            continue  # 超时，继续循环检查

        try:
            _process_request(item)
        except Exception as e:
            logger.error("sdk_queue_process_error", f"处理请求异常: {e}")

    logger.log_event("sdk_queue_worker_stopped", "优先级队列工作线程停止")


def handle_client(conn: socket.socket):
    """处理一个客户端连接（接收请求并入队）"""
    logger = get_logger("sdk_subprocess")
    logger.log_event("sdk_ipc_client_connected", "IPC 客户端已连接")

    try:
        while True:
            try:
                request = recv_message(conn)
            except (ConnectionError, OSError):
                break  # 客户端断开

            # 获取优先级（默认 high）
            priority = request.get("priority", PriorityRequestQueue.HIGH)

            # 入队等待处理
            _priority_queue.enqueue(request, conn, priority)

    except Exception as e:
        logger.error("sdk_ipc_error", f"IPC 处理异常: {e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass
        logger.log_event("sdk_ipc_client_disconnected", "IPC 客户端已断开")


def start_server():
    """启动 IPC 服务端"""
    global _priority_queue, _queue_worker_running
    logger = get_logger("sdk_subprocess")

    # 初始化优先级队列
    _priority_queue = PriorityRequestQueue()
    _queue_worker_running = True

    # 启动工作线程
    worker_thread = threading.Thread(target=_queue_worker, daemon=True)
    worker_thread.start()

    # 清理旧 socket
    if os.path.exists(SOCKET_PATH):
        os.unlink(SOCKET_PATH)

    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(SOCKET_PATH)
    server.listen(8)  # 最多 8 个排队客户端
    server.settimeout(1.0)  # 允许定期检查信号

    logger.log_event("sdk_ipc_server_started", f"IPC 服务端监听: {SOCKET_PATH}")
    logger.log_event("sdk_ipc_server_pid", f"子进程 PID: {os.getpid()}")

    # 通知主进程已就绪（使用 stderr 避免与日志 stdout 混合）
    # 父进程捕获 stderr=STDOUT，所以还是能读到，但不会被异步日志队列干扰
    import sys
    sys.stderr.write(f"__SDK_SIGNAL__:{json.dumps({'event': 'sdk_ready', 'pid': os.getpid()})}\n")
    sys.stderr.flush()

    running = True

    def _shutdown(signum, frame):
        nonlocal running
        running = False
        _queue_worker_running = False
        logger.log_event("sdk_subprocess_shutdown", f"收到信号 {signum}，准备退出")

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    active_clients: list = []

    while running:
        try:
            conn, _ = server.accept()
            # 每个客户端在独立线程中接收请求并入队
            t = threading.Thread(target=handle_client, args=(conn,), daemon=True)
            t.start()
            active_clients.append(t)
        except socket.timeout:
            continue
        except Exception as e:
            if running:
                logger.error("sdk_ipc_accept_error", f"accept 异常: {e}")

    # 停止工作线程
    _queue_worker_running = False
    if _priority_queue:
        with _priority_queue._condition:
            _priority_queue._condition.notify_all()

    # 清理
    try:
        server.close()
    except Exception:
        pass
    if os.path.exists(SOCKET_PATH):
        os.unlink(SOCKET_PATH)
    logger.log_event("sdk_subprocess_exited", "子进程已退出")


# ============================================================
# 入口
# ============================================================

if __name__ == "__main__":
    # 先登录
    if not sdk_login():
        print(json.dumps({"event": "sdk_login_failed"}), flush=True)
        sys.exit(1)

    # 启动 IPC 服务端
    start_server()
