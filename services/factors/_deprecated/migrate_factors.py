"""
因子数据迁移工具

将 stock_factors 表的数据迁移到新的日频表和月频表：
1. stock_daily_factors - 日频因子表（与 kline_data 日期对齐）
2. stock_monthly_factors - 月频因子表（月末日期）

迁移规则：
- 只迁移 2021-04-02 之后的数据（与 kline_data 对齐）
- next_period_changes 数组展开为每日记录
- 原始 stock_factors 表保持不变
"""

import sqlite3
import pandas as pd
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional, Tuple
import asyncio

DB_PATH = Path(__file__).parent.parent.parent / "data" / "kline.db"


def get_connection(db_path: Path = DB_PATH) -> sqlite3.Connection:
    """获取数据库连接"""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def create_daily_factors_table(conn: sqlite3.Connection):
    """创建日频因子表"""
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stock_daily_factors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            stock_code TEXT NOT NULL,
            stock_name TEXT,
            trade_date DATE NOT NULL,

            -- 市值类因子
            circ_market_cap REAL,
            total_market_cap REAL,
            days_since_ipo INTEGER,

            -- 市场表现类因子
            change_10d REAL,
            change_20d REAL,
            bias_5 REAL,
            bias_10 REAL,
            bias_20 REAL,
            amplitude_5 REAL,
            amplitude_10 REAL,
            amplitude_20 REAL,
            change_std_5 REAL,
            change_std_10 REAL,
            change_std_20 REAL,
            amount_std_5 REAL,
            amount_std_10 REAL,
            amount_std_20 REAL,

            -- 技术指标类因子
            kdj_k REAL,
            kdj_d REAL,
            kdj_j REAL,
            dif REAL,
            dea REAL,
            macd REAL,

            -- 估值类因子
            pe_inverse REAL,
            pb_inverse REAL,

            -- 下期收益率
            next_period_change REAL,

            -- 标记
            is_traded INTEGER,

            -- 元数据
            source TEXT DEFAULT 'migrated',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

            UNIQUE(stock_code, trade_date)
        )
    """)

    # 创建索引
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_daily_stock_code
        ON stock_daily_factors(stock_code)
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_daily_trade_date
        ON stock_daily_factors(trade_date)
    """)
    cursor.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_daily_stock_date
        ON stock_daily_factors(stock_code, trade_date)
    """)

    conn.commit()
    print("日频因子表创建完成")


def create_monthly_factors_table(conn: sqlite3.Connection):
    """创建月频因子表"""
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stock_monthly_factors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            stock_code TEXT NOT NULL,
            stock_name TEXT,
            report_date DATE NOT NULL,

            -- 财报时间
            report_quarter INTEGER,
            report_year INTEGER,

            -- 利润类因子
            net_profit REAL,
            net_profit_ttm REAL,
            net_profit_ttm_yoy REAL,
            net_profit_single REAL,
            net_profit_single_yoy REAL,
            net_profit_single_qoq REAL,

            -- 现金流类因子
            operating_cash_flow REAL,
            operating_cash_flow_ttm REAL,
            operating_cash_flow_ttm_yoy REAL,
            operating_cash_flow_single REAL,
            operating_cash_flow_single_yoy REAL,
            operating_cash_flow_single_qoq REAL,

            -- 资产类因子
            net_assets REAL,

            -- 行业分类因子
            sw_level1 TEXT,
            sw_level2 TEXT,
            sw_level3 TEXT,

            -- 元数据
            source TEXT DEFAULT 'migrated',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

            UNIQUE(stock_code, report_date)
        )
    """)

    # 创建索引
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_monthly_stock_code
        ON stock_monthly_factors(stock_code)
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_monthly_report_date
        ON stock_monthly_factors(report_date)
    """)

    conn.commit()
    print("月频因子表创建完成")


def migrate_daily_factors(conn: sqlite3.Connection, start_date: str = "2021-04-02"):
    """
    迁移日频因子数据

    策略：
    1. 从 stock_factors 读取月频数据
    2. 对于 2021-04-02 之后的记录，从 kline 数据计算 next_period_change
    3. 其他日频因子在月内保持不变（向前填充）
    """
    cursor = conn.cursor()

    # 获取所有不同的月份
    cursor.execute("""
        SELECT DISTINCT strftime('%Y-%m', trade_date) as month
        FROM stock_factors
        WHERE trade_date >= ?
        ORDER BY month
    """, (start_date,))
    months = [row[0] for row in cursor.fetchall()]

    print(f"需要处理的月份数：{len(months)}")

    total_inserted = 0
    total_next_period_calculated = 0
    batch_data = []
    BATCH_SIZE = 5000

    for month_idx, month in enumerate(months):
        # 获取该月的所有股票数据
        cursor.execute("""
            SELECT
                stock_code, stock_name, trade_date,
                circ_market_cap, total_market_cap, days_since_ipo,
                change_10d, change_20d,
                bias_5, bias_10, bias_20,
                amplitude_5, amplitude_10, amplitude_20,
                change_std_5, change_std_10, change_std_20,
                amount_std_5, amount_std_10, amount_std_20,
                kdj_k, kdj_d, kdj_j,
                dif, dea, macd,
                pe_inverse, pb_inverse,
                next_period_changes,
                is_traded
            FROM stock_factors
            WHERE strftime('%Y-%m', trade_date) = ?
        """, (month,))

        monthly_records = cursor.fetchall()
        print(f"  处理月份 {month} ({len(monthly_records)} 只股票)...")

        for record in monthly_records:
            stock_code = record['stock_code']
            stock_name = record['stock_name']

            # 获取该股票在 kline 中的交易日期（用于确定哪些日期有交易）
            cursor.execute("""
                SELECT trade_date, close
                FROM kline_data
                WHERE stock_code = ?
                AND trade_date >= ?
                ORDER BY trade_date
            """, (stock_code, f"{month}-01"))
            kline_records = cursor.fetchall()

            if not kline_records:
                continue

            # 构建交易日期到收盘价的映射
            kline_map = {row[0]: row[1] for row in kline_records}
            trading_dates = list(kline_map.keys())

            # 向前填充的因子值（不包括 is_traded）
            factor_values = (
                record['circ_market_cap'],
                record['total_market_cap'],
                record['days_since_ipo'],
                record['change_10d'],
                record['change_20d'],
                record['bias_5'],
                record['bias_10'],
                record['bias_20'],
                record['amplitude_5'],
                record['amplitude_10'],
                record['amplitude_20'],
                record['change_std_5'],
                record['change_std_10'],
                record['change_std_20'],
                record['amount_std_5'],
                record['amount_std_10'],
                record['amount_std_20'],
                record['kdj_k'],
                record['kdj_d'],
                record['kdj_j'],
                record['dif'],
                record['dea'],
                record['macd'],
                record['pe_inverse'],
                record['pb_inverse'],
            )
            is_traded = record['is_traded']

            # 为每个交易日插入记录
            for i, trade_date in enumerate(trading_dates):
                # 从 kline 数据计算 next_period_change（次日收益率）
                next_period_change = None
                if i < len(trading_dates) - 1:
                    current_close = kline_map[trade_date]
                    next_close = kline_map[trading_dates[i + 1]]
                    next_period_change = (next_close - current_close) / current_close
                    total_next_period_calculated += 1

                # 准备插入数据
                batch_data.append((
                    stock_code, stock_name, trade_date,
                    *factor_values,
                    next_period_change,
                    is_traded,
                ))

                # 批量提交
                if len(batch_data) >= BATCH_SIZE:
                    cursor.executemany("""
                        INSERT OR REPLACE INTO stock_daily_factors (
                            stock_code, stock_name, trade_date,
                            circ_market_cap, total_market_cap, days_since_ipo,
                            change_10d, change_20d,
                            bias_5, bias_10, bias_20,
                            amplitude_5, amplitude_10, amplitude_20,
                            change_std_5, change_std_10, change_std_20,
                            amount_std_5, amount_std_10, amount_std_20,
                            kdj_k, kdj_d, kdj_j,
                            dif, dea, macd,
                            pe_inverse, pb_inverse,
                            next_period_change,
                            is_traded
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, batch_data)
                    conn.commit()
                    total_inserted += len(batch_data)
                    batch_data = []
                    print(f"    已插入 {total_inserted:,} 条记录...")

        # 每月结束后提交剩余数据
        if batch_data:
            cursor.executemany("""
                INSERT OR REPLACE INTO stock_daily_factors (
                    stock_code, stock_name, trade_date,
                    circ_market_cap, total_market_cap, days_since_ipo,
                    change_10d, change_20d,
                    bias_5, bias_10, bias_20,
                    amplitude_5, amplitude_10, amplitude_20,
                    change_std_5, change_std_10, change_std_20,
                    amount_std_5, amount_std_10, amount_std_20,
                    kdj_k, kdj_d, kdj_j,
                    dif, dea, macd,
                    pe_inverse, pb_inverse,
                    next_period_change,
                    is_traded
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, batch_data)
            conn.commit()
            total_inserted += len(batch_data)
            batch_data = []

    print(f"日频因子数据迁移完成，共插入 {total_inserted:,} 条记录")
    print(f"  其中从 kline 计算 next_period_change: {total_next_period_calculated:,} 条")


def migrate_monthly_factors(conn: sqlite3.Connection, start_date: str = "2021-04-02"):
    """
    迁移月频因子数据

    策略：
    1. 从 stock_factors 读取月频数据
    2. 直接插入到 stock_monthly_factors 表
    """
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            stock_code, stock_name, trade_date,
            report_quarter, report_year,
            net_profit, net_profit_ttm, net_profit_ttm_yoy,
            net_profit_single, net_profit_single_yoy, net_profit_single_qoq,
            operating_cash_flow, operating_cash_flow_ttm, operating_cash_flow_ttm_yoy,
            operating_cash_flow_single, operating_cash_flow_single_yoy, operating_cash_flow_single_qoq,
            net_assets,
            sw_level1, sw_level2, sw_level3
        FROM stock_factors
        WHERE trade_date >= ?
        ORDER BY trade_date
    """, (start_date,))

    records = cursor.fetchall()
    print(f"需要迁移 {len(records)} 条月频记录...")

    total_inserted = 0
    batch_data = []
    BATCH_SIZE = 1000

    for record in records:
        # Convert report_quarter to int, handling float strings
        rq = record['report_quarter']
        report_quarter = int(float(rq)) if rq else None

        batch_data.append((
            record['stock_code'],
            record['stock_name'],
            record['trade_date'],
            report_quarter,
            record['report_year'],
            record['net_profit'],
            record['net_profit_ttm'],
            record['net_profit_ttm_yoy'],
            record['net_profit_single'],
            record['net_profit_single_yoy'],
            record['net_profit_single_qoq'],
            record['operating_cash_flow'],
            record['operating_cash_flow_ttm'],
            record['operating_cash_flow_ttm_yoy'],
            record['operating_cash_flow_single'],
            record['operating_cash_flow_single_yoy'],
            record['operating_cash_flow_single_qoq'],
            record['net_assets'],
            record['sw_level1'],
            record['sw_level2'],
            record['sw_level3'],
            'migrated',  # source
        ))

        if len(batch_data) >= BATCH_SIZE:
            cursor.executemany("""
                INSERT OR REPLACE INTO stock_monthly_factors (
                    stock_code, stock_name, report_date,
                    report_quarter, report_year,
                    net_profit, net_profit_ttm, net_profit_ttm_yoy,
                    net_profit_single, net_profit_single_yoy, net_profit_single_qoq,
                    operating_cash_flow, operating_cash_flow_ttm, operating_cash_flow_ttm_yoy,
                    operating_cash_flow_single, operating_cash_flow_single_yoy, operating_cash_flow_single_qoq,
                    net_assets,
                    sw_level1, sw_level2, sw_level3,
                    source
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, batch_data)
            conn.commit()
            total_inserted += len(batch_data)
            batch_data = []
            print(f"  已处理 {total_inserted:,} 条记录...")

    # 提交剩余数据
    if batch_data:
        cursor.executemany("""
            INSERT OR REPLACE INTO stock_monthly_factors (
                stock_code, stock_name, report_date,
                report_quarter, report_year,
                net_profit, net_profit_ttm, net_profit_ttm_yoy,
                net_profit_single, net_profit_single_yoy, net_profit_single_qoq,
                operating_cash_flow, operating_cash_flow_ttm, operating_cash_flow_ttm_yoy,
                operating_cash_flow_single, operating_cash_flow_single_yoy, operating_cash_flow_single_qoq,
                net_assets,
                sw_level1, sw_level2, sw_level3,
                source
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, batch_data)
        conn.commit()
        total_inserted += len(batch_data)

    print(f"月频因子数据迁移完成，共插入 {total_inserted} 条记录")


def verify_migration(conn: sqlite3.Connection):
    """验证迁移结果"""
    cursor = conn.cursor()

    print("\n" + "=" * 60)
    print("迁移验证")
    print("=" * 60)

    # 日频表统计
    cursor.execute("""
        SELECT COUNT(*) as total, COUNT(DISTINCT stock_code) as stocks,
               MIN(trade_date) as min_date, MAX(trade_date) as max_date
        FROM stock_daily_factors
    """)
    row = cursor.fetchone()
    print(f"\nstock_daily_factors:")
    print(f"  总记录数：{row['total']:,}")
    print(f"  股票数量：{row['stocks']:,}")
    print(f"  日期范围：{row['min_date']} 至 {row['max_date']}")

    # 月频表统计
    cursor.execute("""
        SELECT COUNT(*) as total, COUNT(DISTINCT stock_code) as stocks,
               MIN(report_date) as min_date, MAX(report_date) as max_date
        FROM stock_monthly_factors
    """)
    row = cursor.fetchone()
    print(f"\nstock_monthly_factors:")
    print(f"  总记录数：{row['total']:,}")
    print(f"  股票数量：{row['stocks']:,}")
    print(f"  日期范围：{row['min_date']} 至 {row['max_date']}")

    # 原始表统计
    cursor.execute("""
        SELECT COUNT(*) as total FROM stock_factors
        WHERE trade_date >= '2021-04-02'
    """)
    row = cursor.fetchone()
    print(f"\nstock_factors (2021-04-02 至今):")
    print(f"  总记录数：{row['total']:,}")


def main():
    """主函数"""
    print("=" * 60)
    print("因子数据迁移工具")
    print("=" * 60)

    conn = get_connection()

    try:
        # 创建新表
        print("\n1. 创建新表...")
        create_daily_factors_table(conn)
        create_monthly_factors_table(conn)

        # 迁移数据
        print("\n2. 迁移日频因子数据...")
        migrate_daily_factors(conn)

        print("\n3. 迁移月频因子数据...")
        migrate_monthly_factors(conn)

        # 验证迁移
        print("\n4. 验证迁移结果...")
        verify_migration(conn)

        print("\n" + "=" * 60)
        print("迁移完成！")
        print("=" * 60)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
