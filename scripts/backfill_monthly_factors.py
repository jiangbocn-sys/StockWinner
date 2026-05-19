"""
补全历史月频因子数据

用途：补全 2021Q4、2022Q4、2023Q4 等年报缺失数据
用法：python3 scripts/backfill_monthly_factors.py
"""

import sys
import os
import json

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

from services.factors.monthly_factor_updater import MonthlyFactorUpdater

# 需要补全的所有季度报告期（2021Q2 到 2023Q4）
# 2024Q1 起的数据已是完整的，2021Q1 SDK 不提供
REPORT_PERIODS_TO_BACKFILL = [
    # 2021年
    "20210630",   # Q2
    "20210930",   # Q3
    "20211231",   # Q4
    # 2022年
    "20220331",   # Q1
    "20220630",   # Q2
    "20220930",   # Q3
    "20221231",   # Q4
    # 2023年
    "20230331",   # Q1
    "20230630",   # Q2
    "20230930",   # Q3
    "20231231",   # Q4
]


def main():
    print("=" * 60)
    print("月频因子历史数据补全")
    print("=" * 60)
    print(f"目标报告期: {REPORT_PERIODS_TO_BACKFILL}")
    print()

    updater = MonthlyFactorUpdater()

    # 获取所有非北交所股票
    stocks = updater.get_all_stocks(skip_bj=True)
    print(f"需要处理的股票数: {len(stocks)}")
    print()

    if len(stocks) == 0:
        print("没有需要处理的股票")
        return

    result = updater.batch_update_factors(
        stock_codes=stocks,
        batch_size=200,
        report_periods=REPORT_PERIODS_TO_BACKFILL,
        skip_bj=True,
    )

    print()
    print("=" * 60)
    print("补全完成")
    print(f"  更新: {result['updated']}")
    print(f"  插入: {result['inserted']}")
    print(f"  无数据: {result['missing']}")
    print(f"  失败: {result['failed']}")
    print(f"  跳过(已知无数据): {result['skipped']}")
    print("=" * 60)


if __name__ == "__main__":
    main()
