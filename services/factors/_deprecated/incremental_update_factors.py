"""
增量计算并更新因子数据 - 优化版本

只计算 stock_daily_factors 表中缺失的数据
支持断点续跑，每批提交一次
"""

import sqlite3
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Tuple, Dict, Any

from services.factors.daily_factor_calculator import DailyFactorCalculator

DB_PATH = Path(__file__).parent.parent.parent / "data" / "kline.db"


def get_missing_stock_dates(limit: int = 100) -> List[Tuple[str, str, str, str]]:
    """
    获取需要计算因子的股票和日期范围

    Returns:
        [(stock_code, start_date, end_date, reason), ...]
    """
    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()

    # 获取所有有 kline 数据的股票
    cursor.execute("""
        SELECT stock_code, MIN(trade_date), MAX(trade_date)
        FROM kline_data
        GROUP BY stock_code
    """)
    kline_ranges = {row[0]: (row[1], row[2]) for row in cursor.fetchall()}

    # 获取每只股票在因子表中 ma5 非 NULL 的最大日期
    cursor.execute("""
        SELECT stock_code, MAX(trade_date)
        FROM stock_daily_factors
        WHERE ma5 IS NOT NULL
        GROUP BY stock_code
    """)
    factor_ranges = {row[0]: row[1] for row in cursor.fetchall()}

    conn.close()

    result = []
    for stock_code, (kline_start, kline_end) in kline_ranges.items():
        if stock_code in factor_ranges and factor_ranges[stock_code]:
            # 已有部分数据，计算缺失的
            max_factor_date = factor_ranges[stock_code]
            if max_factor_date < kline_end:
                # 计算从已有日期之后的数据
                next_date = (datetime.strptime(max_factor_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
                if next_date <= kline_end:
                    result.append((stock_code, next_date, kline_end, 'incremental'))
        else:
            # 完全没有数据，计算全部
            result.append((stock_code, kline_start, kline_end, 'full'))

    return result[:limit]  # 限制每次处理的股票数量


def update_stock_factors_batch(
    stock_code: str,
    start_date: str,
    end_date: str,
    calculator: DailyFactorCalculator,
    conn: sqlite3.Connection
) -> int:
    """
    更新单只股票的因子数据

    Returns:
        更新的记录数
    """
    cursor = conn.cursor()

    try:
        # 计算所有因子
        df = calculator.calculate_all_daily_factors(stock_code, start_date, end_date)

        if df.empty:
            return 0

        stock_name = df['stock_name'].iloc[-1] if 'stock_name' in df.columns else ''

        # 准备批量数据
        batch_data = []
        for idx, row in df.iterrows():
            trade_date = row['trade_date']

            factor_data: Dict[str, Any] = {
                'stock_code': stock_code,
                'stock_name': stock_name,
                'trade_date': trade_date,
                # 基础因子（原有字段）
                'circ_market_cap': row.get('circ_market_cap'),
                'total_market_cap': row.get('total_market_cap'),
                'pe_inverse': row.get('pe_inverse'),
                'pb_inverse': row.get('pb_inverse'),
                'change_10d': row.get('change_10d'),
                'change_20d': row.get('change_20d'),
                'bias_5': row.get('bias_5'),
                'bias_10': row.get('bias_10'),
                'bias_20': row.get('bias_20'),
                'amplitude_5': row.get('amplitude_5'),
                'amplitude_10': row.get('amplitude_10'),
                'amplitude_20': row.get('amplitude_20'),
                'change_std_5': row.get('change_std_5'),
                'change_std_10': row.get('change_std_10'),
                'change_std_20': row.get('change_std_20'),
                'amount_std_5': row.get('amount_std_5'),
                'amount_std_10': row.get('amount_std_10'),
                'amount_std_20': row.get('amount_std_20'),
                'kdj_k': row.get('kdj_k'),
                'kdj_d': row.get('kdj_d'),
                'kdj_j': row.get('kdj_j'),
                'dif': row.get('dif'),
                'dea': row.get('dea'),
                'macd': row.get('macd'),
                'next_period_change': row.get('next_period_change'),
                'is_traded': row.get('is_traded'),
                # 技术面因子（新增）- 注意列名映射（ma_5 → ma5）
                'ma5': row.get('ma_5'),
                'ma10': row.get('ma_10'),
                'ma20': row.get('ma_20'),
                'ma60': row.get('ma_60'),
                'ema12': row.get('ema12'),
                'ema26': row.get('ema26'),
                'adx': row.get('adx'),
                'rsi_14': row.get('rsi_14'),
                'cci_20': row.get('cci_20'),
                'momentum_10d': row.get('momentum_10d'),
                'momentum_20d': row.get('momentum_20d'),
                'boll_upper': row.get('boll_upper'),
                'boll_middle': row.get('boll_middle'),
                'boll_lower': row.get('boll_lower'),
                'atr_14': row.get('atr_14'),
                'hv_20': row.get('hv_20'),
                'obv': row.get('obv'),
                'volume_ratio': row.get('volume_ratio'),
                'golden_cross': row.get('golden_cross', 0),
                'death_cross': row.get('death_cross', 0),
                # 特色因子（新增）
                'limit_up_count_10d': row.get('limit_up_count_10d'),
                'limit_up_count_20d': row.get('limit_up_count_20d'),
                'limit_up_count_30d': row.get('limit_up_count_30d'),
                'consecutive_limit_up': row.get('consecutive_limit_up'),
                'first_limit_up_days': row.get('first_limit_up_days'),
                'highest_board_10d': row.get('highest_board_10d'),
                'large_gain_5d_count': row.get('large_gain_5d_count'),
                'large_loss_5d_count': row.get('large_loss_5d_count'),
                'gap_up_ratio': row.get('gap_up_ratio'),
                'close_to_high_250d': row.get('close_to_high_250d'),
                'close_to_low_250d': row.get('close_to_low_250d'),
                # 基本面因子（暂时为空）
                'pe_ttm': None,
                'pb': None,
                'ps_ttm': None,
                'pcf': None,
                'roe': None,
                'roa': None,
                'gross_margin': None,
                'net_margin': None,
                'revenue_growth_yoy': None,
                'revenue_growth_qoq': None,
                'net_profit_growth_yoy': None,
                'net_profit_growth_qoq': None,
                'source': 'auto_calculated',
                'updated_at': datetime.now().isoformat()
            }
            batch_data.append(factor_data)

        if not batch_data:
            return 0

        # 批量插入/更新
        columns = list(batch_data[0].keys())
        placeholders = ', '.join(['?' for _ in columns])
        column_names = ', '.join(columns)

        insert_sql = f"""
            INSERT OR REPLACE INTO stock_daily_factors
            ({column_names})
            VALUES ({placeholders})
        """

        for data in batch_data:
            values = [data[col] for col in columns]
            cursor.execute(insert_sql, values)

        return len(batch_data)

    except Exception as e:
        print(f"  ❌ {stock_code} 更新失败：{e}")
        return 0


def main():
    """主函数"""
    print("=" * 80)
    print("增量计算并更新因子数据 - 优化版本")
    print("=" * 80)

    # 获取需要更新的股票列表（每次处理 100 只）
    print("\n正在分析缺失数据...")
    stocks_to_update = get_missing_stock_dates(limit=100)

    if not stocks_to_update:
        print("✓ 所有股票因子数据已完整，无需更新")
        return

    print(f"本次处理股票数：{len(stocks_to_update)}")

    # 创建计算器
    calculator = DailyFactorCalculator()

    # 创建数据库连接
    conn = sqlite3.connect(str(DB_PATH))

    total_updated = 0
    success_count = 0
    no_data_count = 0

    for i, (stock_code, start_date, end_date, reason) in enumerate(stocks_to_update):
        updated = update_stock_factors_batch(stock_code, start_date, end_date, calculator, conn)

        if updated > 0:
            total_updated += updated
            success_count += 1
            if (i + 1) % 10 == 0:
                conn.commit()
                print(f"  进度：{i + 1}/{len(stocks_to_update)}，已更新 {total_updated} 条记录")
        else:
            no_data_count += 1

    conn.commit()
    conn.close()

    print(f"\n{'=' * 80}")
    print(f"本次执行完成！")
    print(f"  处理股票：{len(stocks_to_update)} 只")
    print(f"  成功：{success_count} 只")
    print(f"  无数据：{no_data_count} 只")
    print(f"  总计更新：{total_updated} 条记录")
    print(f"\n提示：再次运行此脚本可继续处理剩余股票")
    print(f"{'=' * 80}")


if __name__ == "__main__":
    main()
