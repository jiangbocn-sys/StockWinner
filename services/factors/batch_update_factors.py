"""
批量计算并更新所有因子数据

对数据库中已有数据批量计算所有技术面、基本面、特色因子并更新
"""

import sqlite3
import pandas as pd
import numpy as np
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timedelta
import asyncio

from services.factors.daily_factor_calculator import DailyFactorCalculator
from services.factors.fundamental_factor_calculator import FundamentalFactorCalculator
from services.common.technical_indicators import add_all_extended_technical_indicators_to_df

DB_PATH = Path(__file__).parent.parent.parent / "data" / "kline.db"


def get_trading_dates_in_range(
    start_date: str,
    end_date: str
) -> List[str]:
    """获取日期范围内的所有交易日"""
    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()
    cursor.execute("""
        SELECT DISTINCT trade_date
        FROM kline_data
        WHERE trade_date >= ? AND trade_date <= ?
        ORDER BY trade_date
    """, (start_date, end_date))
    dates = [row[0] for row in cursor.fetchall()]
    conn.close()
    return dates


def get_stocks_with_data(trade_date: str) -> List[str]:
    """获取指定交易日有数据的所有股票"""
    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()
    cursor.execute("""
        SELECT DISTINCT stock_code
        FROM kline_data
        WHERE trade_date = ?
    """, (trade_date,))
    stocks = [row[0] for row in cursor.fetchall()]
    conn.close()
    return stocks


def check_factors_exist(
    stock_code: str,
    trade_date: str
) -> bool:
    """检查指定股票日期的因子数据是否已存在"""
    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()
    cursor.execute("""
        SELECT COUNT(*)
        FROM stock_daily_factors
        WHERE stock_code = ? AND trade_date = ?
        AND ma5 IS NOT NULL
    """, (stock_code, trade_date))
    count = cursor.fetchone()[0]
    conn.close()
    return count > 0


def update_factors_for_stock(
    stock_code: str,
    start_date: str,
    end_date: str,
    calculator: DailyFactorCalculator,
    fundamental_calculator: FundamentalFactorCalculator,
    update_existing: bool = False
) -> int:
    """
    更新单只股票的因子数据

    Returns:
        更新的记录数
    """
    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()

    try:
        # 计算所有技术面因子
        df = calculator.calculate_all_daily_factors(stock_code, start_date, end_date)

        if df.empty:
            print(f"  ⚠️  {stock_code}: 无数据")
            return 0

        # 获取股票名称
        stock_name = df['stock_name'].iloc[-1] if 'stock_name' in df.columns else ''

        updated = 0
        batch_size = 0

        for idx, row in df.iterrows():
            trade_date = row['trade_date']

            # 检查是否已存在
            cursor.execute("""
                SELECT id FROM stock_daily_factors
                WHERE stock_code = ? AND trade_date = ?
            """, (stock_code, trade_date))
            existing = cursor.fetchone()

            if existing and not update_existing:
                continue

            # 准备数据
            factor_data = {
                'stock_code': stock_code,
                'stock_name': stock_name,
                'trade_date': trade_date,
                # 基础因子（原有）
                'circ_market_cap': row.get('circ_market_cap'),
                'total_market_cap': row.get('total_market_cap'),
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
                # 技术面因子（新增）
                'ma5': row.get('ma5'),
                'ma10': row.get('ma10'),
                'ma20': row.get('ma20'),
                'ma60': row.get('ma60'),
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
                'golden_cross': row.get('golden_cross'),
                'death_cross': row.get('death_cross'),
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
                # 基本面因子（待计算）
                'pe_ttm': None,  # 需要 SDK 数据
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

            if existing:
                # 更新现有记录
                update_sql = """
                    UPDATE stock_daily_factors
                    SET ma5=?, ma10=?, ma20=?, ma60=?, ema12=?, ema26=?, adx=?,
                        rsi_14=?, cci_20=?, momentum_10d=?, momentum_20d=?,
                        boll_upper=?, boll_middle=?, boll_lower=?, atr_14=?, hv_20=?,
                        obv=?, volume_ratio=?, golden_cross=?, death_cross=?,
                        limit_up_count_10d=?, limit_up_count_20d=?, limit_up_count_30d=?,
                        consecutive_limit_up=?, first_limit_up_days=?, highest_board_10d=?,
                        large_gain_5d_count=?, large_loss_5d_count=?, gap_up_ratio=?,
                        close_to_high_250d=?, close_to_low_250d=?,
                        kdj_k=?, kdj_d=?, kdj_j=?, dif=?, dea=?, macd=?,
                        change_10d=?, change_20d=?, bias_5=?, bias_10=?, bias_20=?,
                        amplitude_5=?, amplitude_10=?, amplitude_20=?,
                        change_std_5=?, change_std_10=?, change_std_20=?,
                        amount_std_5=?, amount_std_10=?, amount_std_20=?,
                        next_period_change=?, is_traded=?,
                        source=?, updated_at=?
                    WHERE stock_code=? AND trade_date=?
                """
                params = [
                    factor_data['ma5'], factor_data['ma10'], factor_data['ma20'], factor_data['ma60'],
                    factor_data['ema12'], factor_data['ema26'], factor_data['adx'],
                    factor_data['rsi_14'], factor_data['cci_20'],
                    factor_data['momentum_10d'], factor_data['momentum_20d'],
                    factor_data['boll_upper'], factor_data['boll_middle'], factor_data['boll_lower'],
                    factor_data['atr_14'], factor_data['hv_20'],
                    factor_data['obv'], factor_data['volume_ratio'],
                    factor_data['golden_cross'], factor_data['death_cross'],
                    factor_data['limit_up_count_10d'], factor_data['limit_up_count_20d'],
                    factor_data['limit_up_count_30d'], factor_data['consecutive_limit_up'],
                    factor_data['first_limit_up_days'], factor_data['highest_board_10d'],
                    factor_data['large_gain_5d_count'], factor_data['large_loss_5d_count'],
                    factor_data['gap_up_ratio'],
                    factor_data['close_to_high_250d'], factor_data['close_to_low_250d'],
                    factor_data['kdj_k'], factor_data['kdj_d'], factor_data['kdj_j'],
                    factor_data['dif'], factor_data['dea'], factor_data['macd'],
                    factor_data['change_10d'], factor_data['change_20d'],
                    factor_data['bias_5'], factor_data['bias_10'], factor_data['bias_20'],
                    factor_data['amplitude_5'], factor_data['amplitude_10'], factor_data['amplitude_20'],
                    factor_data['change_std_5'], factor_data['change_std_10'], factor_data['change_std_20'],
                    factor_data['amount_std_5'], factor_data['amount_std_10'], factor_data['amount_std_20'],
                    factor_data['next_period_change'], factor_data['is_traded'],
                    factor_data['source'], factor_data['updated_at'],
                    stock_code, trade_date
                ]
                cursor.execute(update_sql, params)
            else:
                # 插入新记录
                columns = list(factor_data.keys())
                values = list(factor_data.values())
                placeholders = ', '.join(['?' for _ in values])
                insert_sql = f"""
                    INSERT INTO stock_daily_factors
                    ({', '.join(columns)})
                    VALUES ({placeholders})
                """
                cursor.execute(insert_sql, values)

            batch_size += 1
            if batch_size >= 50:
                conn.commit()
                batch_size = 0

            updated += 1

        conn.commit()
        return updated

    except Exception as e:
        print(f"  ❌ {stock_code} 更新失败：{e}")
        conn.rollback()
        return 0
    finally:
        conn.close()


def calculate_fundamental_factors_batch(
    stock_codes: List[str],
    trade_date: str,
    calculator: FundamentalFactorCalculator
) -> int:
    """批量计算基本面因子 - 更新到stock_monthly_factors表"""
    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()

    updated = 0
    for stock_code in stock_codes:
        try:
            factors = calculator.calculate_all_fundamental_factors(stock_code, trade_date)

            # 注意：估值/盈利/成长因子已迁移到stock_monthly_factors表
            update_sql = """
                UPDATE stock_monthly_factors
                SET pe_ttm=?, pb=?, ps_ttm=?, pcf=?,
                    pe_inverse=?, pb_inverse=?,
                    roe=?, roa=?, gross_margin=?, net_margin=?,
                    revenue_growth_yoy=?, revenue_growth_qoq=?,
                    net_profit_growth_yoy=?, net_profit_growth_qoq=?,
                    updated_at=?
                WHERE stock_code=? AND trade_date=?
            """
            params = [
                factors.get('pe_ttm'), factors.get('pb'),
                factors.get('ps_ttm'), factors.get('pcf'),
                factors.get('pe_inverse'), factors.get('pb_inverse'),
                factors.get('roe'), factors.get('roa'),
                factors.get('gross_margin'), factors.get('net_margin'),
                factors.get('revenue_growth_yoy'), factors.get('revenue_growth_qoq'),
                factors.get('net_profit_growth_yoy'), factors.get('net_profit_growth_qoq'),
                datetime.now().isoformat(),
                stock_code, trade_date
            ]
            cursor.execute(update_sql, params)
            updated += 1

        except Exception as e:
            print(f"  ⚠️  {stock_code} 基本面因子计算失败：{e}")
            continue

    conn.commit()
    conn.close()
    return updated


def main():
    """主函数"""
    print("=" * 80)
    print("批量计算并更新因子数据")
    print("=" * 80)

    # 获取日期范围
    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()
    cursor.execute("SELECT MIN(trade_date), MAX(trade_date) FROM kline_data")
    row = cursor.fetchone()
    start_date = row[0] or "2024-01-01"
    end_date = row[1] or datetime.now().strftime('%Y-%m-%d')
    conn.close()

    print(f"\n数据日期范围：{start_date} 至 {end_date}")

    # 创建计算器
    calculator = DailyFactorCalculator()
    fundamental_calculator = FundamentalFactorCalculator()

    # 获取所有股票
    all_stocks = get_stocks_with_data(end_date)
    print(f"总股票数：{len(all_stocks)}")

    # 分批处理
    BATCH_SIZE = 20
    total_updated = 0
    total_stocks = len(all_stocks)

    for i in range(0, len(all_stocks), BATCH_SIZE):
        batch_stocks = all_stocks[i:i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        print(f"\n处理批次 {batch_num}/{(total_stocks + BATCH_SIZE - 1) // BATCH_SIZE}: {len(batch_stocks)} 只股票")

        for stock_code in batch_stocks:
            updated = update_factors_for_stock(
                stock_code, start_date, end_date,
                calculator, fundamental_calculator,
                update_existing=False
            )
            total_updated += updated
            print(f"  ✓ {stock_code}: 更新 {updated} 条记录")

    print(f"\n{'=' * 80}")
    print(f"技术面因子计算完成！总计更新 {total_updated} 条记录")
    print(f"{'=' * 80}")

    # 基本面因子计算（单独处理，因为需要调用 SDK API）
    print(f"\n开始计算基本面因子...")
    print(f"注意：基本面因子需要调用 SDK API，计算速度较慢")

    # 只计算最新交易日的基本面因子
    fundamental_updated = 0
    for stock_code in all_stocks[:50]:  # 限制数量测试
        updated = calculate_fundamental_factors_batch(
            [stock_code], end_date, fundamental_calculator
        )
        fundamental_updated += updated
        print(f"  ✓ {stock_code}: 基本面因子更新完成")

    print(f"\n{'=' * 80}")
    print(f"全部完成！")
    print(f"  技术面因子：{total_updated} 条记录")
    print(f"  基本面因子：{fundamental_updated} 条记录 (仅最新交易日)")
    print(f"{'=' * 80}")


if __name__ == "__main__":
    main()
