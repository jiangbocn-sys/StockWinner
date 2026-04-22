"""
扩展 stock_daily_factors 表结构

添加所有技术面、基本面、特色因子字段
"""

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent.parent.parent / "data" / "kline.db"

# 需要添加的新字段
NEW_COLUMNS = [
    # ==================== 技术面因子 ====================
    # 趋势类
    ('ma5', 'REAL'),
    ('ma10', 'REAL'),
    ('ma20', 'REAL'),
    ('ma60', 'REAL'),
    ('ema12', 'REAL'),
    ('ema26', 'REAL'),
    ('adx', 'REAL'),

    # 动量类
    ('rsi_14', 'REAL'),
    ('cci_20', 'REAL'),
    ('momentum_10d', 'REAL'),
    ('momentum_20d', 'REAL'),

    # 波动类
    ('boll_upper', 'REAL'),
    ('boll_middle', 'REAL'),
    ('boll_lower', 'REAL'),
    ('atr_14', 'REAL'),
    ('hv_20', 'REAL'),  # 历史波动率

    # 成交量类
    ('obv', 'REAL'),
    ('volume_ratio', 'REAL'),  # 量比

    # 形态类
    ('golden_cross', 'INTEGER'),  # 金叉状态 (1=金叉，0=死叉，-1=无信号)
    ('death_cross', 'INTEGER'),

    # ==================== 特色因子 (A 股) ====================
    # 涨停类
    ('limit_up_count_10d', 'INTEGER'),
    ('limit_up_count_20d', 'INTEGER'),
    ('limit_up_count_30d', 'INTEGER'),
    ('consecutive_limit_up', 'INTEGER'),
    ('first_limit_up_days', 'INTEGER'),  # 首次涨停距今天数

    # 连板类
    ('highest_board_10d', 'INTEGER'),  # 10 日内最高连板数

    # 异动类
    ('large_gain_5d_count', 'INTEGER'),  # 5 日内大涨 (>5%) 次数
    ('large_loss_5d_count', 'INTEGER'),  # 5 日内大跌 (<-5%) 次数
    ('gap_up_ratio', 'REAL'),  # 跳空高开幅度

    # 筹码类
    ('close_to_high_250d', 'REAL'),  # 距 250 日新高距离
    ('close_to_low_250d', 'REAL'),   # 距 250 日新低距离

    # ==================== 基本面因子 ====================
    # 估值类
    ('pe_ttm', 'REAL'),
    ('pb', 'REAL'),
    ('ps_ttm', 'REAL'),
    ('pcf', 'REAL'),
    ('ev_ebitda', 'REAL'),  # EV/EBITDA

    # 盈利类
    ('roe', 'REAL'),
    ('roa', 'REAL'),
    ('gross_margin', 'REAL'),
    ('net_margin', 'REAL'),

    # 成长类
    ('revenue_growth_yoy', 'REAL'),
    ('revenue_growth_qoq', 'REAL'),
    ('net_profit_growth_yoy', 'REAL'),
    ('net_profit_growth_qoq', 'REAL'),
]


def extend_table():
    """扩展表结构"""
    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()

    # 获取当前所有列
    cursor.execute('PRAGMA table_info(stock_daily_factors)')
    existing_cols = {col[1] for col in cursor.fetchall()}

    print(f"当前字段数量：{len(existing_cols)}")
    print(f"计划添加字段数量：{len(NEW_COLUMNS)}")

    added = 0
    skipped = 0

    for col_name, col_type in NEW_COLUMNS:
        if col_name in existing_cols:
            print(f"  ⏭️  跳过已存在字段：{col_name}")
            skipped += 1
        else:
            try:
                cursor.execute(f'ALTER TABLE stock_daily_factors ADD COLUMN {col_name} {col_type}')
                print(f"  ✅ 添加字段：{col_name}")
                added += 1
            except Exception as e:
                print(f"  ❌ 添加字段 {col_name} 失败：{e}")

    conn.commit()
    conn.close()

    print(f"\n完成！新增 {added} 个字段，跳过 {skipped} 个字段")
    return added, skipped


if __name__ == "__main__":
    print("=" * 60)
    print("扩展 stock_daily_factors 表结构")
    print("=" * 60)
    extend_table()
