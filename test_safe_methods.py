#!/usr/bin/env python3
"""
测试AmazingData SDK中不会触发pandas频率错误的方法
"""

import sys
import os
sys.path.append('/home/bobo/StockWinner')

def test_safe_amazingdata_methods():
    """测试不会触发pandas频率错误的AmazingData方法"""
    try:
        import AmazingData as ad
        print("✅ 成功导入 AmazingData SDK")

        # 测试登录
        username = 'REDACTED_SDK_USERNAME'
        password = 'REDACTED_SDK_PASSWORD'
        host = '101.230.159.234'  # 电信 IP
        port = 8600

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

                # 1. 测试获取实时行情 (这个应该不会触发pandas频率错误)
                try:
                    print("\n--- 测试实时行情 ---")
                    stocks = ['000001.SZ']  # 平安银行
                    quote = market_data.query_realtime_quote(stocks)

                    if quote and '000001.SZ' in quote:
                        print("✅ 成功获取实时行情")
                        stock_data = quote['000001.SZ']
                        print(f"   股票代码: {stock_data.get('code', 'N/A')}")
                        print(f"   最新价: {stock_data.get('last', 'N/A')}")
                        print(f"   涨跌幅: {((stock_data.get('last', 0) - stock_data.get('pre_close', 0)) / stock_data.get('pre_close', 1)) * 100 if stock_data.get('pre_close') else 0:.2f}%")
                    else:
                        print("⚠️ 未能获取实时行情数据，可能当前为非交易时间")

                except Exception as e:
                    print(f"❌ 获取实时行情失败: {e}")
                    import traceback
                    traceback.print_exc()

                # 2. 测试获取K线数据 (这个应该也不会触发频率错误)
                try:
                    print("\n--- 测试K线数据 ---")
                    import datetime
                    from AmazingData import constant

                    # 获取前几天的数据
                    end_dt = datetime.datetime.now()
                    begin_dt = end_dt - datetime.timedelta(days=10)
                    end_date = int(end_dt.strftime('%Y%m%d'))
                    begin_date = int(begin_dt.strftime('%Y%m%d'))

                    print(f"尝试获取K线数据: {begin_date} 到 {end_date}")

                    kline_data = market_data.query_kline(
                        code_list=['000001.SZ'],
                        begin_date=begin_date,
                        end_date=end_date,
                        period=constant.Period.day.value  # 使用日线
                    )

                    if kline_data and '00001.SZ' in kline_data:
                        print("✅ 成功获取K线数据")
                        df = kline_data['000001.SZ']
                        print(f"   K线数据长度: {len(df)}")
                        if len(df) > 0 and 'close' in df.columns:
                            print(f"   最新收盘价: {df.iloc[-1]['close']}")
                    else:
                        print("⚠️ 未能获取K线数据，可能数据源中没有此股票")

                except Exception as e:
                    print(f"❌ 获取K线数据失败: {e}")
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
            print("\n✅ 成功登出")
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
    test_safe_amazingdata_methods()