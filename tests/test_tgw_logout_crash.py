"""
验证假设 2: _sdk_login() 中的 logout() 在并发查询时导致崩溃

测试逻辑:
1. 启动一个 SDK 查询（持有 _serial_lock，使用 TGW socket）
2. 在查询进行中，触发 ensure_connected() → _test_connection() 失败 → _sdk_login() → logout()
3. 观察是否出现崩溃

关键: 这不是并发访问问题，而是 logout() 关闭 socket 时其他线程正在使用它。
"""

import sys
import os
import threading
import time
from pathlib import Path
from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / ".env")

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

def log(msg):
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def test_logout_during_query():
    """
    在查询进行中触发 logout + login 重连
    """
    log("=" * 60)
    log("TEST: 查询进行中触发 logout()")
    log("=" * 60)

    from services.common.sdk_manager import get_sdk_manager
    from services.common.sdk_connection_manager import get_connection_manager, ConnectionState, TaskType

    sdk_mgr = get_sdk_manager()
    conn_mgr = get_connection_manager()

    # 确保已连接
    log("确保 SDK 已连接...")
    if conn_mgr.get_state() != ConnectionState.CONNECTED:
        if not conn_mgr.ensure_connected():
            log("SDK 连接失败，跳过测试")
            return
    log(f"SDK 状态: {conn_mgr.get_state()}")

    # 创建 InfoData 实例
    _ = sdk_mgr.get_info()
    time.sleep(0.5)

    barrier = threading.Barrier(2)
    query_error = [None]
    query_done = [False]
    reconnect_result = [None]
    reconnect_error = [None]

    def _do_long_query():
        """执行一个较长的查询，持有 _serial_lock"""
        try:
            barrier.wait(timeout=10)
            log("  [查询线程] 开始获取 _serial_lock...")
            token = conn_mgr.acquire(task_type=TaskType.QUERY, task_id="test_query")
            if token:
                log("  [查询线程] 已持有 _serial_lock，开始查询...")
                md = sdk_mgr.get_market_data()
                # 使用较长的日期范围
                query_error[0] = md.query_kline(
                    ["600000.SH", "000001.SZ", "600519.SH"],
                    "20240101", "20251231", "day"
                )
                log(f"  [查询线程] 查询完成，返回 dict keys: {list(query_error[0].keys()) if isinstance(query_error[0], dict) else 'N/A'}")
                query_done[0] = True
                token.release()
            else:
                log("  [查询线程] 获取锁失败")
        except Exception as e:
            query_error[0] = e
            log(f"  [查询线程] 异常: {type(e).__name__}: {e}")

    def _do_reconnect():
        """触发重连（会调用 logout + login）"""
        try:
            barrier.wait(timeout=10)
            # 等待查询线程先拿到锁
            time.sleep(0.1)
            log("  [重连线程] 手动设置 last_success_time 为过去，触发重连...")
            conn_mgr._last_success_time = time.time() - 400
            log("  [重连线程] 调用 ensure_connected() ...")
            reconnect_result[0] = conn_mgr.ensure_connected()
            log(f"  [重连线程] ensure_connected() 返回: {reconnect_result[0]}")
            log(f"  [重连线程] 连接状态: {conn_mgr.get_state()}")
        except Exception as e:
            reconnect_error[0] = e
            log(f"  [重连线程] 异常: {type(e).__name__}: {e}")

    t_query = threading.Thread(target=_do_long_query, name="test_query")
    t_reconnect = threading.Thread(target=_do_reconnect, name="test_reconnect")

    t_query.start()
    t_reconnect.start()

    t_query.join(timeout=60)
    t_reconnect.join(timeout=45)

    if t_query.is_alive():
        log("  [查询线程] 超时未结束")
    if t_reconnect.is_alive():
        log("  [重连线程] 超时未结束")

    if query_done[0]:
        log("查询完成（可能是在重连之前或之后）")
    elif query_error[0] and not isinstance(query_error[0], dict):
        log(f"查询异常: {type(query_error[0]).__name__}: {query_error[0]}")
    else:
        log("查询未正常完成")

    log(f"重连结果: {reconnect_result[0]}")
    if reconnect_error[0]:
        log(f"重连异常: {type(reconnect_error[0]).__name__}: {reconnect_error[0]}")

    log("TEST 完成")


def test_direct_logout_during_query():
    """
    更激进的测试: 在查询进行中直接调用 logout()
    """
    log("=" * 60)
    log("TEST 2: 查询进行中直接调用 logout()")
    log("=" * 60)

    from services.common.sdk_manager import get_sdk_manager
    from services.common.sdk_connection_manager import get_connection_manager, ConnectionState, TaskType

    sdk_mgr = get_sdk_manager()
    conn_mgr = get_connection_manager()

    if conn_mgr.get_state() != ConnectionState.CONNECTED:
        if not conn_mgr.ensure_connected():
            log("SDK 连接失败，跳过测试")
            return

    _ = sdk_mgr.get_info()
    _ = sdk_mgr.get_market_data()
    time.sleep(0.5)

    barrier = threading.Barrier(2)
    query_error = [None]
    query_done = [False]
    logout_error = [None]

    def _do_query():
        try:
            barrier.wait(timeout=10)
            log("  [查询线程] 开始查询...")
            token = conn_mgr.acquire(task_type=TaskType.QUERY, task_id="test_query2")
            if token:
                log("  [查询线程] 持有锁，查询中...")
                md = sdk_mgr.get_market_data()
                result = md.query_kline(
                    ["600000.SH"], "20200101", "20251231", "day"
                )
                query_done[0] = True
                log(f"  [查询线程] 完成")
                token.release()
        except Exception as e:
            query_error[0] = e
            log(f"  [查询线程] 异常: {type(e).__name__}: {e}")

    def _do_logout():
        try:
            barrier.wait(timeout=10)
            time.sleep(0.2)
            log("  [logout 线程] 调用 logout()...")
            from AmazingData import logout
            logout(os.environ.get("SDK_USERNAME", ""))
            log("  [logout 线程] logout() 完成")
        except Exception as e:
            logout_error[0] = e
            log(f"  [logout 线程] 异常: {type(e).__name__}: {e}")

    t_query = threading.Thread(target=_do_query)
    t_logout = threading.Thread(target=_do_logout)

    t_query.start()
    t_logout.start()

    t_query.join(timeout=60)
    t_logout.join(timeout=15)

    if query_done[0]:
        log("查询完成")
    elif query_error[0] and not isinstance(query_error[0], dict):
        log(f"查询异常: {type(query_error[0]).__name__}: {query_error[0]}")
    else:
        log("查询未完成")

    if logout_error[0]:
        log(f"logout 异常: {type(logout_error[0]).__name__}: {logout_error[0]}")

    log("TEST 2 完成")


if __name__ == "__main__":
    log("启动 logout 并发测试")
    log(f"PID: {os.getpid()}")

    try:
        test_logout_during_query()
    except Exception as e:
        log(f"TEST 1 崩溃: {type(e).__name__}: {e}")

    time.sleep(3)

    try:
        test_direct_logout_during_query()
    except Exception as e:
        log(f"TEST 2 崩溃: {type(e).__name__}: {e}")

    log("=" * 60)
    log("所有测试完成，进程仍然存活")
    log("=" * 60)
