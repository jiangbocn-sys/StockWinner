"""
测试交易执行服务的核心功能
验证 5 个关键功能：
1. 计算买入数量（考虑可用资金）
2. 计算卖出数量（考虑持仓）
3. 计算交易手续费
4. 更新账户资金（买入减少，卖出增加）
5. 记录交易费用
"""

import asyncio
import sys
sys.path.insert(0, '/home/bobo/StockWinner')

from services.trading.execution_service import TradeExecutionService
from services.common.database import get_db_manager


async def test_execution_service():
    print("=" * 60)
    print("测试交易执行服务")
    print("=" * 60)

    # 使用 bobo 账户
    account_id = "8229DE7E"
    service = TradeExecutionService(account_id)
    db = get_db_manager()

    # 1. 获取初始账户信息
    print("\n【1】初始账户状态")
    account = await service.get_account_info()
    initial_cash = account.get('available_cash', 0)
    initial_balance = account.get('cash_balance', 0)
    print(f"  可用资金：{initial_cash:,.2f}")
    print(f"  资金余额：{initial_balance:,.2f}")

    # 2. 测试手续费计算
    print("\n【2】测试手续费计算")
    fees_buy = service._calculate_fees("600519.SH", 1685.0, 100, "buy")
    print(f"  买入 贵州茅台 100 股 @ 1685.0 元:")
    print(f"    佣金：{fees_buy['commission']:.2f} 元")
    print(f"    过户费：{fees_buy['transfer_fee']:.4f} 元")
    print(f"    印花税：{fees_buy['stamp_tax']:.2f} 元")
    print(f"    总费用：{fees_buy['total_fee']:.2f} 元")

    fees_sell = service._calculate_fees("600519.SH", 1700.0, 100, "sell")
    print(f"  卖出 贵州茅台 100 股 @ 1700.0 元:")
    print(f"    佣金：{fees_sell['commission']:.2f} 元")
    print(f"    过户费：{fees_sell['transfer_fee']:.4f} 元")
    print(f"    印花税：{fees_sell['stamp_tax']:.2f} 元")
    print(f"    总费用：{fees_sell['total_fee']:.2f} 元")

    # 3. 测试买入数量计算
    print("\n【3】测试买入数量计算")
    qty, total, fees = await service.calculate_buy_quantity("600519.SH", 1685.0, 100)
    print(f"  目标买入：100 股 @ 1685.0 元")
    print(f"  实际买入：{qty} 股")
    print(f"  总金额：{total:,.2f} 元")
    print(f"  费用：{fees['total_fee']:.2f} 元")

    # 4. 测试执行买入
    print("\n【4】测试执行买入")
    result = await service.execute_buy(
        stock_code="600519.SH",
        stock_name="贵州茅台",
        price=1685.0,
        target_quantity=100
    )
    print(f"  买入结果：{result['message']}")
    if result['success']:
        print(f"  成交数量：{result['quantity']} 股")
        print(f"  成交价格：{result['price']:.2f} 元")
        print(f"  总金额：{result['total_amount']:,.2f} 元")
        print(f"  费用明细：佣金={result['fees']['commission']:.2f}, "
              f"过户费={result['fees']['transfer_fee']:.4f}, "
              f"印花税={result['fees']['stamp_tax']:.2f}")

        # 检查账户资金变化
        account = await service.get_account_info()
        new_cash = account.get('available_cash', 0)
        print(f"  买入后可用资金：{new_cash:,.2f} 元")
        print(f"  资金变化：{new_cash - initial_cash:,.2f} 元")

        # 检查持仓状态
        position = await service.get_position("600519.SH")
        if position:
            print(f"  持仓数量：{position['quantity']} 股")
            print(f"  可用数量：{position['available_quantity']} 股 (T+1 规则：当日买入不可卖出)")
            print(f"  持仓成本：{position['avg_cost']:.2f} 元")

    # 4.5 模拟 T+1 解冻（次日）
    print("\n【4.5】模拟 T+1 解冻（次日可用）")
    await service.unfreeze_positions()
    position = await service.get_position("600519.SH")
    if position:
        print(f"  解冻后持仓数量：{position['quantity']} 股")
        print(f"  解冻后可用数量：{position['available_quantity']} 股")

    # 5. 测试执行卖出
    print("\n【5】测试执行卖出")
    result = await service.execute_sell(
        stock_code="600519.SH",
        stock_name="贵州茅台",
        price=1700.0,
        target_quantity=100
    )
    print(f"  卖出结果：{result['message']}")
    if result['success']:
        print(f"  成交数量：{result['quantity']} 股")
        print(f"  成交价格：{result['price']:.2f} 元")
        print(f"  净得金额：{result['net_amount']:,.2f} 元")
        print(f"  费用明细：佣金={result['fees']['commission']:.2f}, "
              f"过户费={result['fees']['transfer_fee']:.4f}, "
              f"印花税={result['fees']['stamp_tax']:.2f}")
        print(f"  盈亏：{result['profit_loss']:,.2f} 元")

        # 检查账户资金变化
        account = await service.get_account_info()
        final_cash = account.get('available_cash', 0)
        print(f"  卖出后可用资金：{final_cash:,.2f} 元")
        print(f"  相比买入前变化：{final_cash - initial_cash:,.2f} 元")

    # 6. 验证交易记录
    print("\n【6】验证交易记录")
    trades = await db.fetchall(
        "SELECT * FROM trade_records WHERE account_id = ? ORDER BY created_at DESC LIMIT 5",
        (account_id,)
    )
    print(f"  最近 {len(trades)} 条交易记录:")
    for trade in trades:
        print(f"    - {trade['stock_code']} {trade['trade_type']} "
              f"{trade['quantity']}股 @ {trade['price']:.2f} 元 "
              f"(佣金：{trade['commission']:.2f} 元)")

    print("\n" + "=" * 60)
    print("测试完成")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(test_execution_service())
