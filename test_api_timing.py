#!/usr/bin/env python3
"""
行情 API 耗时测试

测试不同批量查询和下载的实际耗时，验证超时保护是否生效
"""
import time
import sys
import json
import requests

BASE_URL = "http://localhost:8080"
TOKEN = None

def login():
    """登录获取 token"""
    r = requests.post(f"{BASE_URL}/api/auth/login", json={
        "name": "bobo", "password": "123456"
    }, timeout=10)
    data = r.json()
    global TOKEN
    TOKEN = data.get("token")
    print(f"[登录] token={TOKEN[:20]}...")

def req(method, path, **kwargs):
    """发起请求（自动携带 token）"""
    headers = {"X-Auth-Token": TOKEN}
    kwargs["headers"] = headers
    kwargs.setdefault("timeout", 120)
    r = requests.request(method, f"{BASE_URL}{path}", **kwargs)
    return r.status_code, r.json()

def test_single_quote():
    """单股实时行情"""
    print("\n" + "=" * 60)
    print("1. 单股实时行情 /market/quote/600519.SH")
    print("=" * 60)
    times = []
    for i in range(3):
        start = time.time()
        code, data = req("GET", f"/api/v1/ui/8229DE7E/market/quote/600519.SH")
        elapsed = time.time() - start
        times.append(elapsed)
        ok = "OK" if code == 200 else f"FAIL(code={code})"
        name = data.get("data", {}).get("stock_name", "?") if code == 200 else data.get("message", "?")
        print(f"  #{i+1}: {elapsed:.3f}s [{ok}] {name}")
    print(f"  平均: {sum(times)/len(times):.3f}s | 最快: {min(times):.3f}s | 最慢: {max(times):.3f}s")

def test_batch_quotes(counts=[1, 5, 20, 50]):
    """批量行情"""
    print("\n" + "=" * 60)
    print("2. 批量实时行情 /market/quotes")
    print("=" * 60)

    all_codes = [
        "600519.SH", "000001.SZ", "000858.SZ", "601318.SH", "300750.SZ",
        "600036.SH", "601166.SH", "000333.SZ", "600276.SH", "002415.SZ",
        "600900.SH", "601012.SH", "002594.SZ", "601899.SH", "000568.SZ",
        "600030.SH", "601766.SH", "000725.SZ", "601288.SH", "002475.SZ",
        "601398.SH", "600585.SH", "000651.SZ", "601668.SH", "000002.SZ",
        "601601.SH", "601328.SH", "002230.SZ", "600887.SH", "002714.SZ",
        "601988.SH", "600809.SH", "000100.SZ", "601336.SH", "002304.SZ",
        "600196.SH", "601919.SH", "000776.SZ", "600050.SH", "002236.SZ",
        "601169.SH", "600745.SH", "002352.SZ", "601816.SH", "000977.SZ",
        "600690.SH", "601688.SH", "002027.SZ", "601985.SH", "002601.SZ"
    ]

    for count in counts:
        codes = all_codes[:count]
        start = time.time()
        code, data = req("POST", f"/api/v1/ui/8229DE7E/market/quotes",
                         json={"stock_codes": codes})
        elapsed = time.time() - start
        ok = "OK" if code == 200 else f"FAIL(code={code})"
        result_count = data.get("data", {}).get("count", 0) if code == 200 else 0
        print(f"  {count:2d} 只: {elapsed:.3f}s [{ok}] 成功 {result_count}/{count}")
        time.sleep(1)

def test_kline(limit=100):
    """K 线历史数据"""
    print("\n" + "=" * 60)
    print(f"3. K 线历史数据 /market/kline?limit={limit}")
    print("=" * 60)
    start = time.time()
    code, data = req("GET", f"/api/v1/ui/8229DE7E/market/kline?stock_code=600519.SH&period=day&limit={limit}")
    elapsed = time.time() - start
    ok = "OK" if code == 200 else f"FAIL(code={code})"
    kline_count = data.get("data", {}).get("count", 0) if code == 200 else 0
    print(f"  {limit:4d} 条: {elapsed:.3f}s [{ok}] 返回 {kline_count} 条")

def test_timeout_simulation():
    """验证超时保护：大量并发查询，验证不会互相阻塞"""
    print("\n" + "=" * 60)
    print("4. 并发测试：5 个单股查询同时发起")
    print("=" * 60)

    import concurrent.futures
    import threading

    results = []

    def do_query(idx):
        codes = ["600519.SH", "000001.SZ", "000858.SZ", "601318.SH", "300750.SZ"]
        start = time.time()
        code, data = req("POST", f"/api/v1/ui/8229DE7E/market/quotes",
                         json={"stock_codes": [codes[idx % len(codes)]]})
        elapsed = time.time() - start
        return idx, elapsed, code

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(do_query, i) for i in range(5)]
        for future in concurrent.futures.as_completed(futures):
            idx, elapsed, code = future.result()
            status = "OK" if code == 200 else f"FAIL({code})"
            results.append((idx, elapsed, status))

    for idx, elapsed, status in sorted(results):
        print(f"  请求 #{idx+1}: {elapsed:.3f}s [{status}]")

if __name__ == "__main__":
    login()
    test_single_quote()
    test_batch_quotes([1, 5, 20, 50])
    test_kline(100)
    test_kline(1000)
    test_timeout_simulation()

    print("\n" + "=" * 60)
    print("测试完成")
    print("=" * 60)
