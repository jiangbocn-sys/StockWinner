"""
复现真实崩溃场景

生产环境中:
1. SDK 连接在 TGW 端超时（token 过期）
2. _test_connection() 返回 False
3. ensure_connected() 调用 _sdk_login() → logout() → login()
4. 如果此时有其他查询正在排队等待 _serial_lock，logout() 会与之并发

但实际测试发现: _test_connection() 在连接正常时总是返回 True。
真正的问题可能是: TGW 端超时后，_test_connection() 的 get_income() 调用
本身就会导致 C-level 崩溃（不是 logout，而是 get_income 在死连接上操作）。

测试: 强制断开网络/关闭 socket 后调用 get_income
"""

import sys
import os
import threading
import time
import socket
from pathlib import Path
from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / ".env")

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

def log(msg):
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def test_stale_connection_get_income():
    """
    在连接已 stale 的情况下调用 _test_connection()
    模拟: 先 login，然后通过某种方式让连接失效，再调用 get_income
    """
    log("=" * 60)
    log("TEST: stale 连接上调用 get_income()")
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

    log(f"SDK 状态: {conn_mgr.get_state()}")

    # 方法 1: 在另一个进程中执行 login 抢占连接
    # 这会让当前连接的 token 失效
    log("步骤 1: 启动子进程抢占连接...")

    import subprocess
    proc = subprocess.Popen([
        "python3", "-c", """
import os, sys
from pathlib import Path
from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / ".env")
from AmazingData import login, logout
username = os.environ.get("SDK_USERNAME", "")
password = os.environ.get("SDK_PASSWORD", "")
host = os.environ.get("SDK_HOST", "")
port = int(os.environ.get("SDK_PORT", "8600"))
# 先 logout 再 login 抢占
try:
    logout(username)
except:
    pass
result = login(username, password, host, port)
print(f"子进程 login: {result}")
import time
time.sleep(5)  # 持有连接 5 秒
# 然后退出
sys.exit(0)
"""
    ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=str(PROJECT_ROOT))

    time.sleep(3)  # 等待子进程 login 完成
    log("步骤 2: 子进程已持有连接，当前连接应已失效")

    # 现在调用 _test_connection()
    log("步骤 3: 调用 _test_connection()...")
    result = conn_mgr._test_connection()
    log(f"_test_connection() 返回: {result}")

    # 尝试正常查询
    log("步骤 4: 尝试正常查询...")
    try:
        token = conn_mgr.acquire(task_type=TaskType.QUERY, task_id="test_query")
        if token:
            md = sdk_mgr.get_market_data()
            query_result = md.query_kline(["600000.SH"], "20250101", "20250131", "day")
            log(f"查询结果: {type(query_result)}")
            token.release()
        else:
            log("获取锁失败")
    except Exception as e:
        log(f"查询异常: {type(e).__name__}: {e}")

    # 等待子进程退出
    proc.wait()
    stdout, stderr = proc.communicate()
    log(f"子进程 stdout: {stdout.decode()[:200]}")
    log(f"子进程 stderr: {stderr.decode()[:200]}")

    log("TEST 完成")


if __name__ == "__main__":
    log(f"PID: {os.getpid()}")

    try:
        test_stale_connection_get_income()
    except Exception as e:
        log(f"崩溃: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()

    log("=" * 60)
    log("测试完成，进程存活")
    log("=" * 60)
