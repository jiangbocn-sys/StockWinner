#!/usr/bin/env python3
"""
修复 kline_data 表中的股票名称

从 stock_monthly_factors 表获取正确的股票名称，更新 kline_data 表
"""

import sqlite3
from pathlib import Path
from datetime import datetime

DB_PATH = Path(__file__).parent.parent / "data" / "kline.db"

def fix_stock_names(batch_size: int = 1000):
    """批量更新股票名称"""

    conn = sqlite3.connect(str(DB_PATH), timeout=60)
    cursor = conn.cursor()

    # 启用 WAL 模式
    cursor.execute('PRAGMA journal_mode=WAL')

    # 获取所有需要更新的股票（名称等于代码的记录）
    cursor.execute("""
        SELECT DISTINCT stock_code FROM kline_data
        WHERE stock_name = stock_code OR stock_name = '' OR stock_name IS NULL
    """)
    stocks_to_fix = [row[0] for row in cursor.fetchall()]

    total_stocks = len(stocks_to_fix)
    print(f"[FixStockName] 发现 {total_stocks} 只股票需要修复名称")

    if total_stocks == 0:
        print("[FixStockName] 所有股票名称已正确")
        conn.close()
        return 0

    updated_count = 0
    processed = 0

    for i in range(0, len(stocks_to_fix), batch_size):
        batch_stocks = stocks_to_fix[i:i + batch_size]
        processed += len(batch_stocks)
        progress_pct = (processed / total_stocks) * 100 if total_stocks > 0 else 0
        print(f"[FixStockName] 进度：{progress_pct:.1f}% | 处理 {len(batch_stocks)} 只股票 ({processed}/{total_stocks})")

        for stock_code in batch_stocks:
            try:
                # 从 stock_monthly_factors 获取股票名称
                cursor.execute(
                    "SELECT stock_name FROM stock_monthly_factors WHERE stock_code = ? LIMIT 1",
                    (stock_code,)
                )
                row = cursor.fetchone()

                if row and row[0]:
                    stock_name = row[0]
                else:
                    # 如果 stock_monthly_factors 中没有，使用股票代码
                    stock_name = stock_code

                # 更新 kline_data 表
                cursor.execute(
                    "UPDATE kline_data SET stock_name = ?, updated_at = ? WHERE stock_code = ? AND (stock_name = stock_code OR stock_name = '' OR stock_name IS NULL)",
                    (stock_name, datetime.now().isoformat(), stock_code)
                )

                updated = cursor.rowcount
                if updated > 0:
                    updated_count += updated

            except Exception as e:
                print(f"[FixStockName] 更新 {stock_code} 失败：{e}")
                continue

        # 每批次提交一次
        conn.commit()
        print(f"  本批次更新：{updated_count} 条记录")

    conn.commit()

    # 统计结果
    cursor.execute("SELECT COUNT(*) FROM kline_data WHERE stock_name = stock_code OR stock_name = '' OR stock_name IS NULL")
    remaining = cursor.fetchone()[0]

    print(f"\n[FixStockName] ====== 修复完成 ======")
    print(f"[FixStockName] 更新记录数：{updated_count}")
    print(f"[FixStockName] 剩余未修复：{remaining}")

    conn.close()
    return updated_count

if __name__ == "__main__":
    fix_stock_names()
