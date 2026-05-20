#!/usr/bin/env python3
"""
TGW 最大数据量查询测试

从 100 只到全量逐步放大，找实际瓶颈
"""
import time
import requests
import sqlite3

BASE_URL = "http://localhost:8080"

def login():
    r = requests.post(f"{BASE_URL}/api/auth/login", json={
        "name": "bobo", "password": "123456"
    }, timeout=10)
    return r.json().get("token")

def get_all_stock_codes():
    """从 kline.db 获取所有股票代码（SH+SZ，不含BJ）"""
    conn = sqlite3.connect('/home/bobo/StockWinner/data/kline.db')
    c = conn.cursor()
    c.execute("SELECT DISTINCT stock_code FROM kline_data WHERE stock_code NOT LIKE '%.BJ' ORDER BY stock_code")
    codes = [r[0] for r in c.fetchall()]
    conn.close()
    return codes

def req(method, path, token, **kwargs):
    headers = {"X-Auth-Token": token}
    kwargs["headers"] = headers
    kwargs.setdefault("timeout", 600)
    r = requests.request(method, f"{BASE_URL}{path}", **kwargs)
    return r.status_code, r.json()

def test_batch_sizes(token, all_codes):
    """不同批量大小测试（API 层限制 50 只，测试到上限）"""
    print("=" * 70)
    print(f"TGW 批量查询测试（API 层）  [{time.strftime('%Y-%m-%d %H:%M:%S')}]")
    print(f"股票池: {len(all_codes)} 只 (SH+SZ)")
    print("=" * 70)

    # API 限制 50 只，测试边界值
    batch_sizes = [1, 10, 25, 50]

    for batch_size in batch_sizes:
        if batch_size > len(all_codes):
            break
        codes = all_codes[:batch_size]
        print(f"\n--- 批量 {batch_size:3d} 只 ---", end=" ", flush=True)
        start = time.time()
        try:
            code, data = req("POST", f"/api/v1/ui/8229DE7E/market/quotes",
                             token=token,
                             json={"stock_codes": codes})
            elapsed = time.time() - start
            if code == 200:
                count = data.get("data", {}).get("count", 0)
                failed = data.get("data", {}).get("failed_count", 0)
                print(f"耗时 {elapsed:.2f}s | 成功 {count}/{batch_size} | 失败 {failed}")
            else:
                msg = data.get("detail", data.get("message", "?"))
                print(f"HTTP {code} | {msg} ({elapsed:.2f}s)")
        except requests.exceptions.Timeout:
            elapsed = time.time() - start
            print(f"客户端超时 ({elapsed:.2f}s)")
        except Exception as e:
            elapsed = time.time() - start
            print(f"异常: {e} ({elapsed:.2f}s)")

        time.sleep(2)


def test_gateway_direct(token, all_codes):
    """通过 gateway.get_batch_kline_data 直接测试（绕过 API 50 只限制）"""
    print("\n" + "=" * 70)
    print(f"Gateway 层批量查询（绕过 API 限制）")
    print("=" * 70)

    # 通过 HTTP K 线接口间接测试（它走 gateway.get_batch_kline_data）
    # 但 K 线接口是单股的，我们直接调 SDK query_kline
    # 改为：通过 data_download 模块测试
    print(f"\n  股票总数: {len(all_codes)}")
    print("  测试方法: 通过 data_download.py 的下载函数（走 gateway → SDK）")

    batch_sizes = [100, 200, 500, 1000, 2000, 3000, 5782]

    for batch_size in batch_sizes:
        if batch_size > len(all_codes):
            break
        codes = all_codes[:batch_size]
        print(f"\n--- Gateway 批量 {batch_size:5d} 只 ---", end=" ", flush=True)

        # 直接调用 SDKManager（后端进程已登录，通过 gateway 接口测试）
        # 使用 data_download 模块的异步函数，通过 FastAPI 调用
        # 最简单的方式：用 requests 调后端的 K 线接口，但它是单股的
        # 所以改用 SDKManager 直接调用（需要后端事件循环上下文）
        # 改用：通过 scheduler API 触发一次小批量数据下载来测试

        # 直接调用 SDKManager.query_kline
        import sys
        sys.path.insert(0, '/home/bobo/StockWinner')
        from services.common.sdk_manager import get_sdk_manager
        from datetime import datetime, timedelta
        from services.common.timezone import get_china_time

        sdk = get_sdk_manager()
        end_dt = get_china_time()
        begin_dt = end_dt - timedelta(days=2)
        end_date = int((end_dt + timedelta(days=1)).strftime('%Y%m%d'))
        begin_date = int(begin_dt.strftime('%Y%m%d'))

        start = time.time()
        try:
            result = sdk.query_kline(
                code_list=codes,
                begin_date=begin_date,
                end_date=end_date,
                period=10008,  # day
                task_type="download"
            )
            elapsed = time.time() - start
            count = sum(len(df) if df is not None else 0 for df in result.values())
            success = len(result)
            print(f"耗时 {elapsed:.2f}s | 成功 {success}/{batch_size} | {count} 条记录")
        except Exception as e:
            elapsed = time.time() - start
            print(f"失败 ({elapsed:.2f}s): {type(e).__name__}: {e}")

        time.sleep(3)

def test_single_full_range(token):
    """全量 K 线查询（单日全量）"""
    print("\n" + "=" * 70)
    print("全量 K 线查询（单日全量股票）")
    print("=" * 70)

    # 获取所有代码
    all_codes = get_all_stock_codes()
    print(f"\n股票总数: {len(all_codes)}")

    # 直接通过 SDK manager 测试（绕过 API 层限制）
    print("\n  通过 SDKManager.query_kline 直接调用...", end=" ", flush=True)
    import sys
    sys.path.insert(0, '/home/bobo/StockWinner')
    from services.common.sdk_manager import get_sdk_manager
    from datetime import datetime, timedelta
    from services.common.timezone import get_china_time

    sdk = get_sdk_manager()
    end_dt = get_china_time()
    begin_dt = end_dt - timedelta(days=1)
    end_date = int((end_dt + timedelta(days=1)).strftime('%Y%m%d'))
    begin_date = int(begin_dt.strftime('%Y%m%d'))

    start = time.time()
    try:
        result = sdk.query_kline(
            code_list=all_codes,
            begin_date=begin_date,
            end_date=end_date,
            period=10008,  # day
            task_type="download"
        )
        elapsed = time.time() - start
        count = sum(len(df) if df is not None else 0 for df in result.values())
        print(f"耗时 {elapsed:.2f}s | 返回 {len(result)}/{len(all_codes)} 只, {count} 条记录")
    except Exception as e:
        elapsed = time.time() - start
        print(f"失败 ({elapsed:.2f}s): {e}")

if __name__ == "__main__":
    token = login()
    all_codes = get_all_stock_codes()
    test_batch_sizes(token, all_codes)
    test_gateway_direct(token, all_codes)
    print("\n" + "=" * 70)
    print("测试完成")
    print("=" * 70)
