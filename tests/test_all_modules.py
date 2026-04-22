#!/usr/bin/env python3
"""
StockWinner 模块完成度测试脚本
测试所有 API 端点和功能模块
"""

import requests
import json
from datetime import datetime

BASE_URL = "http://localhost:8080"

class TestResult:
    def __init__(self, name):
        self.name = name
        self.passed = 0
        self.failed = 0
        self.tests = []

    def add_pass(self, desc):
        self.passed += 1
        self.tests.append(("✅", desc))

    def add_fail(self, desc, error=""):
        self.failed += 1
        self.tests.append(("❌", f"{desc}: {error}"))

    def print_report(self):
        print(f"\n{'='*60}")
        print(f"模块：{self.name}")
        print(f"结果：{self.passed} 通过，{self.failed} 失败")
        print(f"{'='*60}")
        for status, desc in self.tests:
            print(f"  {status} {desc}")
        return self.failed == 0

def test_health_module():
    """测试健康检查模块"""
    result = TestResult("健康检查模块")

    try:
        # 基础健康检查
        r = requests.get(f"{BASE_URL}/api/v1/health", timeout=5)
        if r.status_code == 200:
            data = r.json()
            result.add_pass(f"基础健康检查 - status={data.get('status')}")
            result.add_pass(f"版本信息 - version={data.get('version')}")
        else:
            result.add_fail("基础健康检查", f"status={r.status_code}")
    except Exception as e:
        result.add_fail("基础健康检查", str(e))

    try:
        # 根路径
        r = requests.get(f"{BASE_URL}/", timeout=5)
        if r.status_code == 200:
            data = r.json()
            result.add_pass(f"根路径 - service={data.get('service')}")
        else:
            result.add_fail("根路径", f"status={r.status_code}")
    except Exception as e:
        result.add_fail("根路径", str(e))

    return result

def test_account_module():
    """测试账户管理模块"""
    result = TestResult("账户管理模块")

    # 测试账户列表
    try:
        r = requests.get(f"{BASE_URL}/api/v1/ui/accounts", timeout=5)
        if r.status_code == 200:
            data = r.json()
            accounts = data.get('accounts', [])
            result.add_pass(f"账户列表 API - 共{len(accounts)}个账户")
            for acc in accounts:
                result.add_pass(f"  - 账户：{acc.get('account_id')} ({acc.get('display_name')})")
        else:
            result.add_fail("账户列表 API", f"status={r.status_code}")
    except Exception as e:
        result.add_fail("账户列表 API", str(e))

    # 测试单个账户详情
    for account_id in ['bobo', 'haoge']:
        try:
            r = requests.get(f"{BASE_URL}/api/v1/ui/accounts/{account_id}", timeout=5)
            if r.status_code == 200:
                data = r.json()
                result.add_pass(f"账户详情 [{account_id}] - {data.get('account', {}).get('display_name')}")
            else:
                result.add_fail(f"账户详情 [{account_id}]", f"status={r.status_code}")
        except Exception as e:
            result.add_fail(f"账户详情 [{account_id}]", str(e))

    # 测试无效账户
    try:
        r = requests.get(f"{BASE_URL}/api/v1/ui/accounts/nonexistent", timeout=5)
        if r.status_code == 404:
            result.add_pass("无效账户返回 404")
        else:
            result.add_fail("无效账户返回 404", f"status={r.status_code}")
    except Exception as e:
        result.add_fail("无效账户返回 404", str(e))

    return result

def test_dashboard_module():
    """测试仪表盘模块"""
    result = TestResult("仪表盘模块")

    for account_id in ['bobo', 'haoge']:
        # 仪表盘数据
        try:
            r = requests.get(f"{BASE_URL}/api/v1/ui/{account_id}/dashboard", timeout=5)
            if r.status_code == 200:
                data = r.json()
                result.add_pass(f"[{account_id}] 仪表盘 - 系统状态={data.get('system_health', {}).get('status')}")

                # 检查数据结构
                if 'system_health' in data:
                    result.add_pass(f"  - system_health 字段存在")
                if 'today_trading' in data:
                    result.add_pass(f"  - today_trading 字段存在")
                if 'positions_summary' in data:
                    result.add_pass(f"  - positions_summary 字段存在")
                if 'resources' in data:
                    result.add_pass(f"  - resources 字段存在")
            else:
                result.add_fail(f"[{account_id}] 仪表盘", f"status={r.status_code}")
        except Exception as e:
            result.add_fail(f"[{account_id}] 仪表盘", str(e))

        # 健康检查
        try:
            r = requests.get(f"{BASE_URL}/api/v1/ui/{account_id}/health", timeout=5)
            if r.status_code == 200:
                data = r.json()
                result.add_pass(f"[{account_id}] 健康检查 - {data.get('status')}")
            else:
                result.add_fail(f"[{account_id}] 健康检查", f"status={r.status_code}")
        except Exception as e:
            result.add_fail(f"[{account_id}] 健康检查", str(e))

    return result

def test_positions_module():
    """测试持仓管理模块"""
    result = TestResult("持仓管理模块")

    for account_id in ['bobo', 'haoge']:
        # 持仓列表
        try:
            r = requests.get(f"{BASE_URL}/api/v1/ui/{account_id}/positions", timeout=5)
            if r.status_code == 200:
                data = r.json()
                result.add_pass(f"[{account_id}] 持仓列表 - {len(data.get('positions', []))}只股票")
            else:
                result.add_fail(f"[{account_id}] 持仓列表", f"status={r.status_code}")
        except Exception as e:
            result.add_fail(f"[{account_id}] 持仓列表", str(e))

        # 带参数查询
        try:
            r = requests.get(f"{BASE_URL}/api/v1/ui/{account_id}/positions?stock_code=600519.SH", timeout=5)
            if r.status_code == 200:
                result.add_pass(f"[{account_id}] 持仓筛选 API 正常")
            else:
                result.add_fail(f"[{account_id}] 持仓筛选", f"status={r.status_code}")
        except Exception as e:
            result.add_fail(f"[{account_id}] 持仓筛选", str(e))

    return result

def test_trades_module():
    """测试交易记录模块"""
    result = TestResult("交易记录模块")

    for account_id in ['bobo', 'haoge']:
        # 今日交易
        try:
            r = requests.get(f"{BASE_URL}/api/v1/ui/{account_id}/trades/today", timeout=5)
            if r.status_code == 200:
                data = r.json()
                result.add_pass(f"[{account_id}] 今日交易 - {len(data.get('trades', []))}条记录")
            else:
                result.add_fail(f"[{account_id}] 今日交易", f"status={r.status_code}")
        except Exception as e:
            result.add_fail(f"[{account_id}] 今日交易", str(e))

        # 交易记录列表
        try:
            r = requests.get(f"{BASE_URL}/api/v1/ui/{account_id}/trades", timeout=5)
            if r.status_code == 200:
                data = r.json()
                result.add_pass(f"[{account_id}] 交易记录列表 - {len(data.get('trades', []))}条记录")
            else:
                result.add_fail(f"[{account_id}] 交易记录列表", f"status={r.status_code}")
        except Exception as e:
            result.add_fail(f"[{account_id}] 交易记录列表", str(e))

        # 带参数筛选
        try:
            r = requests.get(f"{BASE_URL}/api/v1/ui/{account_id}/trades?limit=10", timeout=5)
            if r.status_code == 200:
                result.add_pass(f"[{account_id}] 交易筛选 API (limit=10) 正常")
            else:
                result.add_fail(f"[{account_id}] 交易筛选", f"status={r.status_code}")
        except Exception as e:
            result.add_fail(f"[{account_id}] 交易筛选", str(e))

    return result

def test_account_isolation():
    """测试多账户隔离"""
    result = TestResult("多账户隔离测试")

    # 获取两个账户的仪表盘数据
    try:
        r1 = requests.get(f"{BASE_URL}/api/v1/ui/bobo/dashboard", timeout=5)
        r2 = requests.get(f"{BASE_URL}/api/v1/ui/haoge/dashboard", timeout=5)

        if r1.status_code == 200 and r2.status_code == 200:
            d1 = r1.json()
            d2 = r2.json()

            # 验证账户 ID 不同
            if d1.get('account_id') == 'bobo' and d2.get('account_id') == 'haoge':
                result.add_pass("账户 ID 正确隔离")
            else:
                result.add_fail("账户 ID 隔离", "账户 ID 返回错误")

            # 验证账户名称不同
            if d1.get('account_name') != d2.get('account_name'):
                result.add_pass("账户名称正确隔离")
            else:
                result.add_fail("账户名称隔离", "账户名称相同")
        else:
            result.add_fail("多账户隔离", "API 请求失败")
    except Exception as e:
        result.add_fail("多账户隔离", str(e))

    # 测试无效账户访问
    try:
        r = requests.get(f"{BASE_URL}/api/v1/ui/invalid_account/health", timeout=5)
        if r.status_code == 404:
            result.add_pass("无效账户返回 404")
        else:
            result.add_fail("无效账户访问控制", f"status={r.status_code}")
    except Exception as e:
        result.add_fail("无效账户访问控制", str(e))

    return result

def test_database():
    """测试数据库"""
    result = TestResult("数据库模块")

    import sqlite3
    from pathlib import Path

    db_path = Path("data/stockwinner.db")

    if db_path.exists():
        result.add_pass(f"数据库文件存在 - {db_path}")

        try:
            conn = sqlite3.connect(str(db_path))
            cursor = conn.cursor()

            # 检查表结构
            tables = ['accounts', 'stock_positions', 'trade_records', 'orders', 'strategies', 'system_config']
            for table in tables:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                result.add_pass(f"表 [{table}] - {count}条记录")

            # 检查索引
            cursor.execute("SELECT name FROM sqlite_master WHERE type='index'")
            indexes = [row[0] for row in cursor.fetchall()]
            result.add_pass(f"索引数量 - {len(indexes)}个")

            conn.close()
        except Exception as e:
            result.add_fail("数据库查询", str(e))
    else:
        result.add_fail("数据库文件", "文件不存在")

    return result

def print_summary(results):
    """打印总结"""
    print("\n" + "="*60)
    print(" "*20 + "测试总结")
    print("="*60)

    total_passed = 0
    total_failed = 0

    for result in results:
        status = "✅ 通过" if result.failed == 0 else "❌ 失败"
        print(f"{status} - {result.name}: {result.passed}/{result.passed + result.failed}")
        total_passed += result.passed
        total_failed += result.failed

    print("="*60)
    total = total_passed + total_failed
    rate = (total_passed / total * 100) if total > 0 else 0
    print(f"总计：{total_passed}/{total} 测试通过 ({rate:.1f}%)")
    print("="*60)

    return total_failed == 0

def main():
    print("="*60)
    print("StockWinner v6.1.2 模块完成度测试")
    print(f"测试时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"测试地址：{BASE_URL}")
    print("="*60)

    results = []

    # 运行所有测试
    results.append(test_health_module())
    results.append(test_account_module())
    results.append(test_dashboard_module())
    results.append(test_positions_module())
    results.append(test_trades_module())
    results.append(test_account_isolation())
    results.append(test_database())

    # 打印总结
    success = print_summary(results)

    return 0 if success else 1

if __name__ == "__main__":
    exit(main())
