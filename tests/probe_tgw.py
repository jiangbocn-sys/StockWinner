"""
TGW 行为探测脚本

目的：
1. TGW 单用户最大连接数
2. 孤儿连接存活时间（TCP keepalive 超时）
3. logout() 是否真正清理 TGW 端连接
4. login() 成功后的连接寿命
5. 新 login() 是否能踢掉旧连接

用法：
    python3 tests/probe_tgw.py
"""

import os
import sys
import time
import threading
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

SDK_USERNAME = os.environ.get("SDK_USERNAME", "")
SDK_PASSWORD = os.environ.get("SDK_PASSWORD", "")
SDK_HOST = os.environ.get("SDK_HOST", "")
SDK_PORT = int(os.environ.get("SDK_PORT", "8600"))

from AmazingData import login, logout


def log(msg):
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def do_login(timeout=30):
    """执行 login，返回 (success, result, elapsed_ms)"""
    result_container = [None]
    error_container = [None]

    def _run():
        try:
            result_container[0] = login(SDK_USERNAME, SDK_PASSWORD, SDK_HOST, SDK_PORT)
        except Exception as e:
            error_container[0] = e

    t0 = time.monotonic()
    t = threading.Thread(target=_run, daemon=True)
    t.start()
    t.join(timeout=timeout)
    elapsed_ms = (time.monotonic() - t0) * 1000

    if t.is_alive():
        return False, "timeout", elapsed_ms
    if error_container[0]:
        return False, f"exception: {error_container[0]}", elapsed_ms
    if result_container[0]:
        return True, "success", elapsed_ms
    return False, "login returned False", elapsed_ms


def do_logout():
    """执行 logout"""
    try:
        result = logout(SDK_USERNAME)
        return True, str(result)
    except Exception as e:
        return False, str(e)


def probe_max_connections():
    """测试 TGW 单用户最大连接数"""
    log("=" * 60)
    log("TEST 1: 探测 TGW 最大连接数")
    log("=" * 60)

    sessions = []
    for i in range(10):
        success, result, elapsed = do_login()
        status = "OK" if success else f"FAIL ({result})"
        log(f"  连接 {i+1}: {status} ({elapsed:.0f}ms)")
        sessions.append((success, result, elapsed))
        if not success:
            log(f"\n  最大连接数 = {i}")
            break
        time.sleep(0.5)

    success_count = sum(1 for s, _, _ in sessions if s)
    log(f"\n  结论：TGW 允许 {success_count} 个并发连接")
    return success_count


def probe_logout_effect():
    """测试 logout() 是否真正清理 TGW 端连接"""
    log("")
    log("=" * 60)
    log("TEST 2: logout() 是否清理 TGW 端连接")
    log("=" * 60)

    # 先 login
    success, result, elapsed = do_login()
    log(f"  login: {result} ({elapsed:.0f}ms)")
    if not success:
        log("  SKIP: login 失败")
        return

    # 立即 logout
    logout_ok, logout_msg = do_logout()
    log(f"  logout: {logout_ok} ({logout_msg})")

    # 等 2 秒让 TGW 处理
    time.sleep(2)

    # 再次 login
    success2, result2, elapsed2 = do_login()
    log(f"  再次 login: {result2} ({elapsed2:.0f}ms)")

    if success2:
        log("\n  结论：logout() 有效，TGW 清理了旧连接")
    else:
        log(f"\n  结论：logout() 无效或 TGW 清理延迟 ({result2})")


def probe_connection_lifetime():
    """测试连接寿命：login 后多久 TGW 会自动断开"""
    log("")
    log("=" * 60)
    log("TEST 3: 连接寿命测试")
    log("=" * 60)

    success, result, elapsed = do_login()
    log(f"  login: {result} ({elapsed:.0f}ms)")
    if not success:
        log("  SKIP: login 失败")
        return

    # 每 30 秒尝试用 query 测试连接是否还活着
    from AmazingData import InfoData
    info = InfoData()

    for i in range(20):  # 最多 10 分钟
        time.sleep(30)
        try:
            # 用 get_income 测试连接（轻量）
            t0 = time.monotonic()
            result = info.get_income(["600000.SH"], is_local=False)
            elapsed = (time.monotonic() - t0) * 1000
            log(f"  T+{(i+1)*30}s: query OK ({elapsed:.0f}ms, rows={len(result) if isinstance(result, dict) else '?'})")
        except Exception as e:
            log(f"  T+{(i+1)*30}s: query FAIL ({type(e).__name__}: {e})")
            log(f"\n  结论：连接寿命约 {(i+1)*30} 秒")
            return

    log(f"\n  结论：连接至少存活 {20*30} 秒（测试结束）")


def probe_kick_off():
    """测试新 login() 是否能踢掉旧连接"""
    log("")
    log("=" * 60)
    log("TEST 4: 新 login() 是否踢掉旧连接")
    log("=" * 60)

    # 第一个 login
    s1, r1, t1 = do_login()
    log(f"  login #1: {r1} ({t1:.0f}ms)")
    if not s1:
        log("  SKIP: login #1 失败")
        return

    from AmazingData import InfoData
    info1 = InfoData()

    # 第二个 login（不 logout）
    time.sleep(1)
    s2, r2, t2 = do_login()
    log(f"  login #2 (无 logout): {r2} ({t2:.0f}ms)")

    if s2:
        # 测试旧连接
        time.sleep(0.5)
        try:
            result = info1.get_income(["600000.SH"], is_local=False)
            log(f"  旧连接 query: OK (rows={len(result) if isinstance(result, dict) else '?'})")
            log("\n  结论：新 login() 没有踢掉旧连接（旧连接仍可用）")
        except Exception as e:
            log(f"  旧连接 query: FAIL ({type(e).__name__})")
            log("\n  结论：新 login() 踢掉了旧连接")
    else:
        log(f"\n  结论：新 login() 被拒绝（TGW 拒绝重复连接），{r2}")


def probe_orphan_cleanup_time():
    """测试 orphan 连接清理时间"""
    log("")
    log("=" * 60)
    log("TEST 5: orphan 连接清理时间")
    log("=" * 60)

    # login 后不 logout，直接 kill 进程（模拟崩溃）
    # 这里我们用当前进程直接 exit 来模拟
    success, result, elapsed = do_login()
    log(f"  login: {result} ({elapsed:.0f}ms)")
    if not success:
        log("  SKIP: login 失败")
        return

    log("  不 logout，直接退出（模拟崩溃）...")
    log("  请等待 2-5 分钟后重新运行此脚本，观察 login 是否恢复")
    log("  （如果恢复，说明 TGW TCP keepalive 超时 = 等待的时间）")

    # 记录退出时间
    from services.common.timezone import get_china_time
    log(f"  退出时间: {get_china_time().strftime('%H:%M:%S')}")

    # 不等了，直接退出
    sys.exit(0)


def probe_cleanup_with_timeout():
    """测试不同等待时间后 login 的成功率"""
    log("")
    log("=" * 60)
    log("TEST 6: 不同等待时间后 login 测试")
    log("=" * 60)

    wait_times = [10, 30, 60, 120, 180, 300]

    for wait_time in wait_times:
        log(f"\n  等待 {wait_time}s 后尝试 login...")
        time.sleep(wait_time)
        success, result, elapsed = do_login()
        status = "OK" if success else f"FAIL ({result})"
        log(f"  -> {status} ({elapsed:.0f}ms)")
        if success:
            log(f"\n  结论：TGW orphan 清理时间 <= {wait_time}s")
            # logout 后退出
            do_logout()
            return

    log(f"\n  结论：等待 300s 后仍无法 login，TGW orphan 清理时间 > 300s")


if __name__ == "__main__":
    log(f"PID: {os.getpid()}")
    log(f"TGW: {SDK_HOST}:{SDK_PORT}")
    log(f"User: {SDK_USERNAME}")
    log("")

    # 先尝试清理现有连接
    log("步骤 0: 尝试 logout 清理现有连接")
    logout_ok, logout_msg = do_logout()
    log(f"  logout: {logout_ok} ({logout_msg})")
    time.sleep(2)

    # 测试 1: 最大连接数
    max_conn = probe_max_connections()

    # 清理所有连接
    log("\n等待 10 秒让 TGW 清理...")
    time.sleep(10)

    # 测试 2: logout 效果
    probe_logout_effect()

    # 清理
    time.sleep(5)
    do_logout()
    time.sleep(5)

    # 测试 3: 连接寿命（长测试，可选）
    # probe_connection_lifetime()

    # 测试 4: kick off
    probe_kick_off()

    log("")
    log("=" * 60)
    log("快速探测完成")
    log("=" * 60)
