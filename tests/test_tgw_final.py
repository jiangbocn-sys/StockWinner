"""
最终验证: _test_connection() 返回 False 后 _sdk_login() 中的 logout()
在查询进行中执行 → 崩溃

测试逻辑:
1. 启动长查询（持有 _serial_lock）
2. 同时设置 _last_success_time 为过去，让 _test_connection() 失败
3. ensure_connected() → _test_connection() 失败 → _sdk_login() → logout()
4. 由于 _serial_lock 被查询持有，logout() 会等待吗？还是会并发执行？

关键: _sdk_login() 不获取 _serial_lock，所以 logout() 会和查询并发执行！
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


def test_logout_waits_for_lock():
    """
    验证: _sdk_login() 中的 logout() 是否会等待 _serial_lock
    """
    log("=" * 60)
    log("TEST: 验证 logout() 是否等待 _serial_lock")
    log("=" * 60)

    from services.common.sdk_manager import get_sdk_manager
    from services.common.sdk_connection_manager import get_connection_manager, ConnectionState, TaskType

    sdk_mgr = get_sdk_manager()
    conn_mgr = get_connection_manager()

    if conn_mgr.get_state() != ConnectionState.CONNECTED:
        if not conn_mgr.ensure_connected():
            log("SDK 连接失败")
            return

    _ = sdk_mgr.get_info()
    _ = sdk_mgr.get_market_data()
    time.sleep(0.5)

    barrier = threading.Barrier(2)
    query_done = [False]
    query_error = [None]
    reconnect_timeline = []

    def _do_query():
        """长查询，持有 _serial_lock 10+ 秒"""
        try:
            barrier.wait(timeout=10)
            log("  [查询] 获取 _serial_lock...")
            token = conn_mgr.acquire(task_type=TaskType.QUERY, task_id="long_query")
            if token:
                log("  [查询] 持有 _serial_lock，开始查询...")
                md = sdk_mgr.get_market_data()
                # 查询较长的数据
                result = md.query_kline(
                    ["600000.SH"], "20200101", "20251231", "day"
                )
                query_done[0] = True
                log(f"  [查询] 完成，返回 {len(result)} 只股票")
                token.release()
            else:
                log("  [查询] 获取锁失败")
        except Exception as e:
            query_error[0] = e
            log(f"  [查询] 异常: {type(e).__name__}: {e}")

    def _do_reconnect():
        """触发重连，记录时间线"""
        try:
            barrier.wait(timeout=10)
            time.sleep(0.5)  # 让查询先拿到锁

            t0 = time.time()
            reconnect_timeline.append(f"t+{t0:.1f}s: 开始 ensure_connected()")
            log("  [重连] 设置 last_success_time 为过去...")
            conn_mgr._last_success_time = time.time() - 400

            log("  [重连] 调用 ensure_connected()...")
            t1 = time.time()
            reconnect_timeline.append(f"t+{t1-t0:.1f}s: 调用 ensure_connected")

            result = conn_mgr.ensure_connected()

            t2 = time.time()
            reconnect_timeline.append(f"t+{t2-t0:.1f}s: 返回 {result}")
            log(f"  [重连] ensure_connected() 返回 {result} (耗时 {t2-t1:.2f}s)")

            for line in reconnect_timeline:
                log(f"  时间线: {line}")
        except Exception as e:
            log(f"  [重连] 异常: {type(e).__name__}: {e}")

    t_query = threading.Thread(target=_do_query, name="long_query")
    t_reconnect = threading.Thread(target=_do_reconnect, name="reconnect")

    t_query.start()
    t_reconnect.start()

    t_query.join(timeout=90)
    t_reconnect.join(timeout=60)

    if t_query.is_alive():
        log("  [查询] 超时未结束")
    if t_reconnect.is_alive():
        log("  [重连] 超时未结束")

    if query_done[0]:
        log("查询完成")
    elif query_error[0]:
        log(f"查询异常: {type(query_error[0]).__name__}: {query_error[0]}")

    log(f"最终状态: {conn_mgr.get_state()}")
    log("TEST 完成")


if __name__ == "__main__":
    log(f"PID: {os.getpid()}")

    try:
        test_logout_waits_for_lock()
    except Exception as e:
        log(f"崩溃: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()

    log("=" * 60)
    log("测试完成，进程存活")
    log("=" * 60)
