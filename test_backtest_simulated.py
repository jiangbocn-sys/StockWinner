#!/usr/bin/env python3
"""
撮合模拟盘端到端测试 — 更真实的回测结果
浩哥短线股票池 + 尾盘买入逻辑 + 短线卖出策略
2025-01-01 ~ 2025-12-31
"""

import sys, os
sys.path.insert(0, os.path.dirname(__file__))

from services.backtest.modes.simulated_trading import SimulatedTradingEngine
from services.backtest.execution import FeeConfig, PositionLimits

STOCK_POOL = [
    "601012.SH", "002475.SZ", "300308.SZ", "600519.SH", "601318.SH",
    "600036.SH", "601398.SH", "300750.SZ", "300274.SZ", "600089.SH",
    "601166.SH", "603259.SH", "300760.SZ", "300059.SZ", "002594.SZ",
    "600276.SH", "300122.SZ", "600030.SH", "601888.SH", "600585.SH",
    "601668.SH", "600028.SH", "601857.SH", "600690.SH", "601186.SH",
    "600048.SH", "600837.SH", "601601.SH", "002027.SZ", "600009.SH",
    "600104.SH", "601766.SH", "601989.SH", "002371.SZ", "603986.SH",
    "688008.SH", "002049.SZ", "688012.SH", "002185.SZ", "600584.SH",
    "300496.SZ", "002230.SZ", "300024.SZ", "603019.SH", "002402.SZ",
    "688297.SH", "600893.SH", "002025.SZ", "600118.SH", "300159.SZ",
    "603256.SH", "600150.SH", "002389.SZ", "688148.SH", "002265.SZ",
]

BUY_CONDITIONS = [
    "PRICE > MA5",
    "MA5 > MA20",
    "RSI > 40",
    "RSI < 65",
]

STOP_LOSS_PCT = 0.025
TAKE_PROFIT_PCT = 0.02
TRAILING_STOP_PCT = 0.002


def test_simulated():
    print("=" * 60)
    print("撮合模拟盘端到端测试")
    print("股票池: 浩哥短线 (55只)")
    print("买入: PRICE>MA5 AND MA5>MA20 AND RSI 40~65")
    print("卖出: 止损2.5% / 止盈2% / 移动止盈0.2%")
    print("模式: 撮合模拟盘 (逐日推进)")
    print("时间: 2025-01-01 ~ 2025-12-31")
    print("初始资金: 1,000,000")
    print("=" * 60)

    strategy_config = {
        "buy_conditions": BUY_CONDITIONS,
        "stop_loss_pct": STOP_LOSS_PCT,
        "take_profit_pct": TAKE_PROFIT_PCT,
        "trailing_stop_pct": TRAILING_STOP_PCT,
    }

    engine = SimulatedTradingEngine(
        strategy_config=strategy_config,
        initial_capital=1000000,
        start_date="2025-01-01",
        end_date="2025-12-31",
        stock_pool=STOCK_POOL,
        fee_config=FeeConfig(),
        position_limits=PositionLimits(
            max_total_position_pct=0.80,
            max_single_position_pct=0.15,
            cash_reserve_pct=0.10,
        ),
        slippage_pct=0.0,
        stop_loss_pct=STOP_LOSS_PCT,
        take_profit_pct=TAKE_PROFIT_PCT,
        trailing_stop_pct=TRAILING_STOP_PCT,
    )

    print("\n开始回测...")
    result = engine.run()

    if "error" in result:
        print(f"\n回测失败: {result['error']}")
        return False

    r = result["result"]
    trades = result["trades"]
    nav = result["nav_series"]

    print("\n" + "=" * 60)
    print("回测结果（撮合模拟盘）")
    print("=" * 60)
    print(f"总收益率:       {r['total_return']:.2f}%")
    print(f"年化收益率:     {r['annualized_return']:.2f}%")
    print(f"最大回撤:       {r['max_drawdown']:.2f}% (起止: {r['max_drawdown_start']} ~ {r['max_drawdown_end']})")
    print(f"夏普比率:       {r['sharpe_ratio']:.2f}")
    print(f"卡玛比率:       {r['calmar_ratio']:.2f}")
    print(f"胜率:           {r['win_rate']:.2f}%")
    print(f"盈亏比:         {r['profit_factor']:.2f}")
    print(f"总交易次数:     {r['total_trades']}")
    print(f"平均持仓天数:   {r['avg_holding_days']:.1f}")
    print(f"最佳交易:       {r['best_trade']:.2f}%")
    print(f"最差交易:       {r['worst_trade']:.2f}%")
    print(f"总手续费:       {r['total_commission']:,.2f}")
    print(f"最终净值:       {r['final_nav']:.4f}")
    print(f"最终总资产:     {r['final_value']:,.2f}")

    # 净值曲线摘要
    if nav:
        print(f"\n净值曲线摘要:")
        print(f"  起始: {nav[0]['trade_date']} NAV={nav[0]['nav']:.4f} 总资产={nav[0]['total_value']:,.0f}")
        mid = nav[len(nav)//2]
        print(f"  年中: {mid['trade_date']} NAV={mid['nav']:.4f} 总资产={mid['total_value']:,.0f} 持仓={mid['position_count']}只")
        print(f"  结束: {nav[-1]['trade_date']} NAV={nav[-1]['nav']:.4f} 总资产={nav[-1]['total_value']:,.0f}")
        print(f"  交易日数: {len(nav)}")

    # 交易摘要
    if trades:
        complete = [t for t in trades if t.get("pnl") is not None and t["pnl"] != 0]
        win_trades = [t for t in complete if t.get("pnl", 0) > 0]
        lose_trades = [t for t in complete if t.get("pnl", 0) < 0]
        print(f"\n交易摘要:")
        print(f"  盈利笔数: {len(win_trades)}  亏损笔数: {len(lose_trades)}")
        print(f"  完整买卖: {len(complete)} 笔")

        # 按卖出原因统计
        by_reason = {}
        for t in complete:
            reason = t.get("reason", "unknown")
            by_reason.setdefault(reason, {"count": 0, "pnl": 0})
            by_reason[reason]["count"] += 1
            by_reason[reason]["pnl"] += t.get("pnl", 0)
        print(f"\n卖出原因统计:")
        for reason, info in sorted(by_reason.items(), key=lambda x: -x[1]["pnl"]):
            print(f"  {reason}: {info['count']}笔 累计盈亏={info['pnl']:,.2f}")

        print(f"\n前10笔交易:")
        for t in trades[:10]:
            pnl = t.get("pnl_pct", 0)
            sign = "+" if pnl >= 0 else ""
            print(f"  {t['stock_code']} {t['stock_name']} 买:{t.get('buy_date','?')}@{t.get('buy_price',0):.2f} → 卖:{t.get('date','?')}@{t.get('price',0):.2f} 盈亏:{sign}{pnl:.2f}% {t.get('reason','-')}")

    print("\n" + "=" * 60)
    print("测试通过")
    print("=" * 60)
    return True


if __name__ == "__main__":
    success = test_simulated()
    sys.exit(0 if success else 1)
