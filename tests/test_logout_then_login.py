"""
验证：logout() 后立即 login() 是否成功
"""
import os, sys, time, threading
from pathlib import Path
from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / ".env")

SDK_USERNAME = os.environ.get("SDK_USERNAME", "")
SDK_PASSWORD = os.environ.get("SDK_PASSWORD", "")
SDK_HOST = os.environ.get("SDK_HOST", "")
SDK_PORT = int(os.environ.get("SDK_PORT", "8600"))

from AmazingData import login, logout

def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

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

# 测试
log(f"TGW: {SDK_HOST}:{SDK_PORT}")
log(f"User: {SDK_USERNAME}")

# 1. 先尝试 logout 清理
log("\n步骤 1: logout 清理")
try:
    logout(SDK_USERNAME)
    log("  logout 完成")
except Exception as e:
    log(f"  logout 异常: {e}")
time.sleep(2)

# 2. login
log("\n步骤 2: login")
ok1, r1, t1 = do_login()
log(f"  login #1: {r1} ({t1:.0f}ms)")

if not ok1:
    log("FAILED: 第一次 login 就失败，说明 TGW 还有 orphan 连接")
    sys.exit(1)

# 3. 正常用一会儿
from AmazingData import InfoData
info = InfoData()
log("\n步骤 3: 测试查询（确认连接可用）")
try:
    t0 = time.monotonic()
    res = info.get_income(["600000.SH"], is_local=False)
    log(f"  query OK: {time.monotonic()-t0:.2f}s, rows={len(res) if isinstance(res, dict) else '?'}")
except Exception as e:
    log(f"  query FAIL: {type(e).__name__}: {e}")

# 4. logout
log("\n步骤 4: logout")
try:
    logout(SDK_USERNAME)
    log("  logout 完成")
except Exception as e:
    log(f"  logout 异常: {e}")
time.sleep(2)

# 5. 再次 login
log("\n步骤 5: 再次 login")
ok2, r2, t2 = do_login()
log(f"  login #2: {r2} ({t2:.0f}ms)")

if ok2:
    log("\nSUCCESS: logout 后 login 成功")
else:
    log(f"\nFAIL: logout 后 login 仍然失败: {r2}")
