#!/usr/bin/env python3
"""
测试更新后的AmazingData SDK接口
"""

import sys
import os
sys.path.append('/home/bobo/StockWinner')

async def test_updated_gateway():
    try:
        from services.trading.gateway import AmazingDataTradingGateway
        import asyncio

        print("✅ 成功导入 AmazingDataTradingGateway")

        # 创建网关实例
        gateway = AmazingDataTradingGateway()

        print("尝试连接到AmazingData SDK...")
        connected = await gateway.connect()

        if connected:
            print("✅ 成功连接到AmazingData SDK")

            # 测试获取市场数据（使用K线方法）
            print("\n--- 测试获取市场数据 ---")
            try:
                market_data = await gateway.get_market_data('000001.SZ')
                if market_data:
                    print(f"✅ 成功获取市场数据!")
                    print(f"   股票代码: {market_data.stock_code}")
                    print(f"   股票名称: {market_data.stock_name}")
                    print(f"   当前价格: {market_data.current_price}")
                    print(f"   涨跌幅: {market_data.change_percent:.2f}%")
                else:
                    print("⚠️ 未能获取市场数据，但没有报错")
            except Exception as e:
                print(f"❌ 获取市场数据失败: {e}")
                import traceback
                traceback.print_exc()

            # 测试获取股票列表
            print("\n--- 测试获取股票列表 ---")
            try:
                stock_list = await gateway.get_stock_list()
                print(f"✅ 成功获取股票列表，共 {len(stock_list)} 只股票")
                for i, stock in enumerate(stock_list[:5]):  # 只显示前5只
                    print(f"   {i+1}. {stock['code']} - {stock['name']} ({stock['market']})")
                if len(stock_list) > 5:
                    print(f"   ... 还有 {len(stock_list)-5} 只股票")
            except Exception as e:
                print(f"❌ 获取股票列表失败: {e}")
                import traceback
                traceback.print_exc()

        else:
            print("❌ 连接AmazingData SDK失败")

        # 断开连接
        await gateway.disconnect()
        print("\n✅ 已断开连接")

    except Exception as e:
        print(f"❌ 测试过程中出现错误: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    import asyncio
    asyncio.run(test_updated_gateway())