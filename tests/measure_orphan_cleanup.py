"""
测量 TGW orphan 连接清理时间

模拟场景：进程崩溃（kill -9）后，TGW 多久自动清理 orphan 连接？

用法：python3 tests/measure_orphan_cleanup.py
"""
import os, sys, time, threading
from pathlib import Path
from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / ".env")

SDK_USERNAME = os.environ.get("SDK_USERNAME", "")
SDK_PASSWORD = os.environ.get("SDK_PASSWORD", "")
SDK_HOST = os.environ.get("SDK_HOST", "")
SDK_PORT = int(os.environ.get("SDK_PORT", "8600"))

from AmazingData import login

def do_login(timeout=30):
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

print(f"开始测量 orphan 清理时间... (当前时间: {time.strftime('%H:%M:%S')})")
print(f"等待 TGW 自动清理 kill -9 留下的 orphan 连接\n")

# 每 10 秒尝试 login，直到成功
attempt = 0
start_time = time.time()

while True:
    attempt += 1
    elapsed = time.time() - start_time
    ok, result, t_ms = do_login(timeout=15)
    status = "OK" if ok else result

    print(f"  T+{elapsed:6.0f}s (#{attempt:3d}): {status} ({t_ms:.0f}ms)")

    if ok:
        print(f"\n  结论：TGW orphan 清理时间 ≈ {elapsed:.0f} 秒")
        # 测试查询
        from AmazingData import InfoData
        info = InfoData()
        try:
            res = info.get_income(["600000.SH"], is_local=False)
            print(f"  查询验证：OK")
        except Exception as e:
            print(f"  查询验证：FAIL ({type(e).__name__})")
        break

    if elapsed > 600:
        print(f"\n  超时：10 分钟内未清理完成")
        break

    time.sleep(10)
