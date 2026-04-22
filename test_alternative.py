#!/usr/bin/env python3
"""
测试不同AmazingData SDK方法以避开频率解析错误
"""

import sys
import os
sys.path.append('/home/bobo/StockWinner')

def test_alternative_methods():
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

            # 尝试不同的方法来获取数据
            base_data = ad.BaseData()
            calendar = base_data.get_calendar()
            market_data = ad.MarketData(calendar)

            # 测试获取K线数据，这可能不会触发频率解析错误
            try:
                import datetime
                from AmazingData import constant

                # 获取几天前的数据，而不是今天的数据
                end_dt = datetime.datetime.now()
                begin_dt = end_dt - datetime.timedelta(days=5)
                end_date = int(end_dt.strftime('%Y%m%d'))
                begin_date = int(begin_dt.strftime('%Y%m%d'))

                print(f"尝试获取K线数据: {begin_date} 到 {end_date}")

                # 尝试获取K线数据，可能不会触发pandas频率解析错误
                kline_data = market_data.query_kline(
                    code_list=['000001.SZ'],
                    begin_date=begin_date,
                    end_date=end_date,
                    period=constant.Period.day.value  # 使用日线，避免秒级频率
                )

                if kline_data and '000001.SZ' in kline_data:
                    print("✅ 成功获取K线数据")
                    df = kline_data['000001.SZ']
                    print(f"   K线数据长度: {len(df)}")
                    if len(df) > 0:
                        print(f"   最新价格: {df.iloc[-1]['close'] if 'close' in df.columns else 'N/A'}")
                else:
                    print("⚠️ 未能获取K线数据")

            except Exception as e:
                print(f"❌ 获取K线数据失败: {e}")
                import traceback
                traceback.print_exc()

            # 测试获取历史数据而非实时数据
            try:
                print("尝试获取历史快照数据...")
                # 尝试使用过去某一天的数据，而非当天数据，以避开实时数据处理逻辑
                past_date = 20260325  # 假设是最近的交易日

                snapshot_result = market_data.query_snapshot(
                    code_list=['000001.SZ'],
                    begin_date=past_date,
                    end_date=past_date
                )

                if snapshot_result and past_date in snapshot_result:
                    print("✅ 成功获取历史快照数据")
                else:
                    print("⚠️ 未能获取历史快照数据")
            except Exception as e:
                print(f"❌ 获取历史快照数据失败: {e}")
                import traceback
                traceback.print_exc()

        except Exception as e:
            print(f"❌ 测试失败: {e}")
            import traceback
            traceback.print_exc()

        # 登出
        try:
            ad.logout(username=username)
            print("✅ 成功登出")
        except Exception as e:
            print(f"登出失败: {e}")

    except ImportError as e:
        print(f"❌ 无法导入 AmazingData SDK: {e}")

if __name__ == "__main__":
    test_alternative_methods()