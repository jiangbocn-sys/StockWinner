"""
验证假设: _test_connection() 绕过 _serial_lock 并发访问 TGW socket 导致崩溃

测试逻辑:
1. 启动一个正常 SDK 查询（持有 _serial_lock，使用 TGW socket）
2. 同时触发 _test_connection()（不持有锁，使用同一 TGW socket）
3. 观察是否出现崩溃/segfault

如果进程在测试期间崩溃 → 假设成立
如果进程存活且 _test_connection() 返回结果 → 需要另找原因
"""

from pathlib import Path
from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / ".env")

import sys
import os
import threading
import time
import signal

# 项目根目录加入 sys.path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

result = {"test1_crashed": False, "test2_crashed": False, "logs": []}


def log(msg):
    ts = time.strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    result["logs"].append(line)
    print(line, flush=True)


def test_concurrent_access():
    """
    测试 1: 在正常查询持有锁期间，_test_connection() 是否能安全执行
    """
    log("=" * 60)
    log("TEST 1: 并发访问 TGW socket")
    log("=" * 60)

    from services.common.sdk_manager import get_sdk_manager
    from services.common.sdk_connection_manager import get_connection_manager, ConnectionState

    sdk_mgr = get_sdk_manager()
    conn_mgr = get_connection_manager()

    # 确保已连接
    log("确保 SDK 已连接...")
    if conn_mgr.get_state() != ConnectionState.CONNECTED:
        if not conn_mgr.ensure_connected():
            log("SDK 连接失败，跳过测试")
            return
    log(f"SDK 状态: {conn_mgr.get_state()}")

    # 获取 InfoData 实例
    if sdk_mgr._info_instance is None:
        # 触发实例创建
        _ = sdk_mgr.get_info()
        time.sleep(1)

    if sdk_mgr._info_instance is None:
        log("InfoData 实例创建失败，跳过测试")
        return

    # 场景: 持有 _serial_lock 做查询的同时，_test_connection() 并发调用
    barrier = threading.Barrier(2)
    test_conn_result = [None]
    test_conn_error = [None]
    query_result = [None]
    query_error = [None]

    def _do_query():
        """正常查询路径：持有 _serial_lock"""
        try:
            barrier.wait(timeout=10)
            log("  [查询线程] 开始获取 _serial_lock...")
            token = conn_mgr.acquire(task_type="query", task_id="test_query")
            if token:
                log("  [查询线程] 已持有 _serial_lock，执行 query_kline...")
                md = sdk_mgr.get_market_data()
                # 模拟一个较慢的查询（周K线数据量大）
                query_result[0] = md.query_kline(
                    ["600000.SH"], "20250101", "20251231", "week"
                )
                log(f"  [查询线程] 查询完成，结果 keys: {list(query_result[0].keys()) if query_result[0] else 'None'}")
                token.release()
            else:
                log("  [查询线程] 获取锁失败")
        except Exception as e:
            query_error[0] = e
            log(f"  [查询线程] 异常: {e}")

    def _do_test_connection():
        """健康检查路径：不持有 _serial_lock"""
        try:
            barrier.wait(timeout=10)
            # 等待查询线程先拿到锁
            time.sleep(0.2)
            log("  [HC 线程] 开始 _test_connection()（不持有锁）...")
            test_conn_result[0] = conn_mgr._test_connection()
            log(f"  [HC 线程] _test_connection() 返回: {test_conn_result[0]}")
        except Exception as e:
            test_conn_error[0] = e
            log(f"  [HC 线程] 异常: {e}")

    t_query = threading.Thread(target=_do_query, name="test_query")
    t_hc = threading.Thread(target=_do_test_connection, name="test_hc")

    t_query.start()
    t_hc.start()

    t_query.join(timeout=60)
    t_hc.join(timeout=15)

    if t_query.is_alive():
        log("  [查询线程] 超时未结束（可能挂起）")
    if t_hc.is_alive():
        log("  [HC 线程] 超时未结束（可能挂起）")

    if query_error[0]:
        log(f"  查询异常: {query_error[0]}")
    if test_conn_error[0]:
        log(f"  HC 异常: {test_conn_error[0]}")

    log(f"TEST 1 完成 - 查询: {'ok' if query_result[0] or query_error[0] else 'pending'}, HC: {test_conn_result[0]}")


def test_health_check_during_idle():
    """
    测试 2: 空闲状态下 _test_connection() 是否正常
    """
    log("=" * 60)
    log("TEST 2: 空闲状态下的 _test_connection()")
    log("=" * 60)

    from services.common.sdk_connection_manager import get_connection_manager, ConnectionState

    conn_mgr = get_connection_manager()

    # 确保连接已建立
    if conn_mgr.get_state() != ConnectionState.CONNECTED:
        conn_mgr.ensure_connected()
        time.sleep(2)

    # 手动设置 last_success_time 为过去，模拟超时
    conn_mgr._last_success_time = time.time() - 400

    log(f"模拟 last_success_time = {conn_mgr._last_success_time}s 前")
    log("调用 ensure_connected() 触发健康检查...")

    ok = conn_mgr.ensure_connected()
    log(f"ensure_connected() 返回: {ok}")
    log(f"连接状态: {conn_mgr.get_state()}")


if __name__ == "__main__":
    log("启动并发 TGW socket 访问测试")
    log(f"PID: {os.getpid()}")

    try:
        test_concurrent_access()
    except Exception as e:
        log(f"TEST 1 崩溃: {e}")

    time.sleep(2)

    try:
        test_health_check_during_idle()
    except Exception as e:
        log(f"TEST 2 崩溃: {e}")

    log("=" * 60)
    log("所有测试完成，进程仍然存活")
    log("=" * 60)

    # 输出日志
    for line in result["logs"]:
        pass  # 已实时输出
