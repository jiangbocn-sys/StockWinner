#!/usr/bin/env python3
"""
探索AmazingData SDK可用的方法
"""

import sys
import os
sys.path.append('/home/bobo/StockWinner')

def explore_amazingdata_methods():
    try:
        import AmazingData as ad
        print("✅ 成功导入 AmazingData SDK")

        # 查看AmazingData模块的所有属性
        print("\n--- AmazingData 模块属性 ---")
        for attr in dir(ad):
            if not attr.startswith('_'):
                print(f"  {attr}")

        # 测试登录
        username = 'REDACTED_SDK_USERNAME'
        password = 'REDACTED_SDK_PASSWORD'
        host = '101.230.159.234'
        port = 8600

        try:
            # 登录
            ad.login(username=username, password=password, host=host, port=port)
            print("\n✅ 登录成功")

            # 探索BaseData
            base_data = ad.BaseData()
            print(f"\n--- BaseData 属性 ---")
            for attr in dir(base_data):
                if not attr.startswith('_'):
                    print(f"  {attr}")

            # 获取交易日历
            calendar = base_data.get_calendar()
            print(f"✅ 成功获取交易日历，类型: {type(calendar)}")

            # 探索MarketData
            market_data = ad.MarketData(calendar)
            print(f"\n--- MarketData 属性 ---")
            for attr in dir(market_data):
                if not attr.startswith('_') and not callable(getattr(market_data, attr)):
                    print(f"  {attr}")

            for attr in dir(market_data):
                if not attr.startswith('_') and callable(getattr(market_data, attr)):
                    print(f"  {attr}()")

        except Exception as e:
            print(f"❌ 探索过程中出错: {e}")
            import traceback
            traceback.print_exc()

        # 尝试登出
        try:
            ad.logout(username=username)
            print("\n✅ 成功登出")
        except Exception as e:
            print(f"登出失败: {e}")

    except ImportError as e:
        print(f"❌ 无法导入 AmazingData SDK: {e}")

if __name__ == "__main__":
    explore_amazingdata_methods()