#!/usr/bin/env python3
"""
测试AmazingData SDK连接和基本功能
"""

import sys
import os
sys.path.append('/home/bobo/StockWinner')

def test_amazingdata():
    try:
        import AmazingData as ad
        print("✅ 成功导入 AmazingData SDK")

        # 获取版本信息（如果有的话）
        try:
            # 尝试获取版本信息
            if hasattr(ad, '__version__'):
                print(f"SDK 版本: {ad.__version__}")
            elif hasattr(ad, 'version'):
                print(f"SDK 版本: {ad.version}")
            else:
                print("SDK 版本: 未知")
        except Exception as e:
            print(f"获取版本信息失败: {e}")

        # 测试登录
        username = 'REDACTED_SDK_USERNAME'
        password = 'REDACTED_SDK_PASSWORD'
        host = '101.230.159.234'  # 电信 IP
        port = 8600

        print(f"尝试连接到服务器: {host}:{port}")

        try:
            # 登录
            ad.login(username=username, password=password, host=host, port=port)
            print("✅ 登录成功")

            # 获取交易日历
            try:
                base_data = ad.BaseData()
                calendar = base_data.get_calendar()
                print("✅ 成功获取交易日历")

                # 创建市场数据对象
                market_data = ad.MarketData(calendar)
                print("✅ 成功创建 MarketData 对象")

                # 测试获取实时行情
                try:
                    # 尝试获取一只股票的实时行情
                    stocks = ['000001.SZ']  # 平安银行
                    quote = market_data.query_realtime_quote(stocks)

                    if quote and '000001.SZ' in quote:
                        print("✅ 成功获取实时行情")
                        stock_data = quote['000001.SZ']
                        print(f"   股票数据: {stock_data}")
                    else:
                        print("⚠️ 未能获取实时行情数据，可能当前为非交易时间")

                except Exception as e:
                    print(f"❌ 获取实时行情失败: {e}")
                    import traceback
                    traceback.print_exc()

                # 测试获取快照数据
                try:
                    today = int(__import__('datetime').datetime.now().strftime('%Y%m%d'))
                    snapshot_result = market_data.query_snapshot(code_list=['000001.SZ'], begin_date=today, end_date=today)

                    if snapshot_result and today in snapshot_result and '000001.SZ' in snapshot_result[today]:
                        print("✅ 成功获取快照数据")
                        df = snapshot_result[today]['000001.SZ']
                        print(f"   快照数据长度: {len(df)}")
                    else:
                        print("⚠️ 未能获取快照数据，可能当前为非交易时间或数据不可用")

                except Exception as e:
                    print(f"❌ 获取快照数据失败: {e}")
                    import traceback
                    traceback.print_exc()

            except Exception as e:
                print(f"❌ 获取交易日历或创建MarketData对象失败: {e}")
                import traceback
                traceback.print_exc()

        except Exception as e:
            print(f"❌ 登录失败: {e}")
            import traceback
            traceback.print_exc()

        # 尝试登出
        try:
            ad.logout(username=username)
            print("✅ 成功登出")
        except Exception as e:
            print(f"登出失败: {e}")

    except ImportError as e:
        print(f"❌ 无法导入 AmazingData SDK: {e}")
        print("这表示 AmazingData SDK 未安装或不可用")
    except Exception as e:
        print(f"❌ 测试过程中出现意外错误: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    test_amazingdata()