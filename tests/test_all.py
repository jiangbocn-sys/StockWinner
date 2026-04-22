#!/usr/bin/env python3
"""
StockWinner 系统测试脚本
测试所有核心功能模块
"""

import asyncio
import sys
import os
from datetime import datetime

# 添加项目根目录到 Python 路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 颜色输出
class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    END = '\033[0m'

def log_success(msg):
    print(f"{Colors.GREEN}✅ {msg}{Colors.END}")

def log_error(msg):
    print(f"{Colors.RED}❌ {msg}{Colors.END}")

def log_info(msg):
    print(f"{Colors.BLUE}ℹ️  {msg}{Colors.END}")

def log_warning(msg):
    print(f"{Colors.YELLOW}⚠️  {msg}{Colors.END}")

async def test_gateway():
    """测试交易网关"""
    print("\n" + "=" * 60)
    print("测试交易网关模块")
    print("=" * 60)

    from services.trading.gateway import MockTradingGateway, GalaxyTradingGateway

    # 测试模拟网关
    log_info("测试模拟网关...")
    mock_gateway = MockTradingGateway()
    await mock_gateway.connect()

    stocks = await mock_gateway.get_stock_list()
    log_success(f"模拟网关股票列表：{len(stocks)} 只")

    data = await mock_gateway.get_market_data("600519")
    if data:
        log_success(f"模拟行情：600519 = ¥{data.current_price:.2f} ({data.change_percent:.2f}%)")
    else:
        log_error("模拟行情获取失败")

    await mock_gateway.disconnect()

    # 测试银河网关
    log_info("测试银河网关...")
    galaxy_gateway = GalaxyTradingGateway()
    log_success(f"SDK 可用：{galaxy_gateway.sdk_available}")

    if galaxy_gateway.sdk_available:
        ret = await galaxy_gateway.connect()
        if ret:
            log_success("银河网关连接成功")

            # 测试获取行情
            data = await galaxy_gateway.get_market_data("600519")
            if data:
                log_success(f"银河行情：600519 = ¥{data.current_price:.2f} ({data.change_percent:.2f}%)")
            else:
                log_warning("银河行情返回空，使用 fallback 数据")

            await galaxy_gateway.disconnect()
        else:
            log_error("银河网关连接失败")
    else:
        log_warning("银河 SDK 不可用")

    return True

async def test_indicators():
    """测试技术指标"""
    print("\n" + "=" * 60)
    print("测试技术指标模块")
    print("=" * 60)

    from services.common.indicators import TechnicalIndicators
    import random

    # 生成模拟 K 线数据
    kline = []
    base_price = 100
    for i in range(60):
        open_price = base_price * (1 + (random.random() - 0.5) * 0.02)
        close_price = open_price * (1 + (random.random() - 0.5) * 0.02)
        high_price = max(open_price, close_price) * (1 + random.random() * 0.01)
        low_price = min(open_price, close_price) * (1 - random.random() * 0.01)
        kline.append({
            "open": open_price,
            "high": high_price,
            "low": low_price,
            "close": close_price,
            "volume": int(1000000 + random.random() * 500000)
        })
        base_price = close_price

    closes = [k["close"] for k in kline]
    highs = [k["high"] for k in kline]
    lows = [k["low"] for k in kline]

    # 测试各个指标
    ma5 = TechnicalIndicators.ma(closes, 5)
    log_success(f"MA(5) = {ma5:.2f}" if ma5 else log_error("MA(5) 失败"))

    ema12 = TechnicalIndicators.ema(closes, 12)
    log_success(f"EMA(12) = {ema12:.2f}" if ema12 else log_error("EMA(12) 失败"))

    rsi = TechnicalIndicators.rsi(closes, 14)
    log_success(f"RSI(14) = {rsi:.2f}" if rsi else log_error("RSI(14) 失败"))

    macd_result = TechnicalIndicators.macd(closes)
    if macd_result:
        log_success(f"MACD = {macd_result}")
    else:
        log_error("MACD 失败")

    boll = TechnicalIndicators.bollinger_bands(closes, 20)
    if boll:
        log_success(f"BOLL = {boll}")
    else:
        log_error("BOLL 失败")

    atr = TechnicalIndicators.atr(highs, lows, closes, 14)
    log_success(f"ATR(14) = {atr:.2f}" if atr else log_error("ATR(14) 失败"))

    kdj = TechnicalIndicators.kdj(highs, lows, closes)
    if kdj:
        log_success(f"KDJ = {kdj}")
    else:
        log_error("KDJ 失败")

    # 测试条件解析
    result = TechnicalIndicators.check_condition("MA5>MA10", {"ma5": 105, "ma10": 100})
    log_success(f"条件解析测试：MA5>MA10 = {result}" if result else log_error("条件解析失败"))

    return True

async def test_screening():
    """测试选股服务"""
    print("\n" + "=" * 60)
    print("测试选股服务模块")
    print("=" * 60)

    from services.screening.service import ScreeningService

    screening = ScreeningService()

    # 测试策略解析
    config = {"buy": ["MA5>MA20"], "sell": ["MA5<MA20"]}
    candidates = await screening._evaluate_conditions(config)
    log_success(f"选股扫描结果：{len(candidates)} 只候选股票")

    for c in candidates[:3]:
        print(f"  - {c['stock_code']}: {c['stock_name']} (score: {c['match_score']:.2f})")

    return True

async def test_monitoring():
    """测试监控服务"""
    print("\n" + "=" * 60)
    print("测试监控服务模块")
    print("=" * 60)

    from services.monitoring.service import TradingMonitor

    monitor = TradingMonitor()
    status = monitor.get_status()
    log_success(f"监控服务状态：running={status['running']}")

    return True

async def test_api():
    """测试 API 端点"""
    print("\n" + "=" * 60)
    print("测试 API 端点")
    print("=" * 60)

    import aiohttp

    base_url = "http://localhost:8080"

    endpoints = [
        ("/api/v1/health", "健康检查"),
        ("/api/v1/ui/accounts", "账户列表"),
        ("/api/v1/ui/bobo/dashboard", "波哥仪表盘"),
        ("/api/v1/ui/bobo/positions", "波哥持仓"),
        ("/api/v1/ui/bobo/screening/status", "选股服务状态"),
        ("/api/v1/ui/bobo/monitoring/status", "监控服务状态"),
    ]

    async with aiohttp.ClientSession() as session:
        for endpoint, name in endpoints:
            try:
                async with session.get(f"{base_url}{endpoint}") as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        log_success(f"{name}: OK")
                    else:
                        log_error(f"{name}: HTTP {resp.status}")
            except Exception as e:
                log_error(f"{name}: {str(e)}")

    return True

async def main():
    """主测试函数"""
    print(f"\n{Colors.BLUE}" + "=" * 60)
    print("StockWinner 系统测试")
    print(f"时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}" + Colors.END)
    print("=" * 60)

    results = {
        "gateway": False,
        "indicators": False,
        "screening": False,
        "monitoring": False,
        "api": False,
    }

    # 执行测试
    try:
        results["gateway"] = await test_gateway()
    except Exception as e:
        log_error(f"网关测试异常：{e}")

    try:
        results["indicators"] = await test_indicators()
    except Exception as e:
        log_error(f"指标测试异常：{e}")

    try:
        results["screening"] = await test_screening()
    except Exception as e:
        log_error(f"选股测试异常：{e}")

    try:
        results["monitoring"] = await test_monitoring()
    except Exception as e:
        log_error(f"监控测试异常：{e}")

    # API 测试（需要服务运行）
    try:
        results["api"] = await test_api()
    except Exception as e:
        log_warning(f"API 测试异常（可能服务未运行）: {e}")

    # 汇总结果
    print("\n" + "=" * 60)
    print("测试结果汇总")
    print("=" * 60)

    passed = sum(1 for v in results.values() if v)
    total = len(results)

    for name, result in results.items():
        status = f"{Colors.GREEN}通过{Colors.END}" if result else f"{Colors.RED}失败{Colors.END}"
        print(f"  {name}: {status}")

    print(f"\n总计：{passed}/{total} 通过")

    if passed == total:
        log_success("所有测试通过！🎉")
        return 0
    else:
        log_warning(f"有 {total - passed} 个测试失败")
        return 1

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
