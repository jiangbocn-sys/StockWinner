"""
历史数据关联脚本

将已有的 trade_records 买入记录与 watchlist 选出记录关联，
回填 trade_records.strategy_id/signal_id 和 watchlist.bought/buy_trade_id。

关联规则：按 stock_code + 时间窗口（±3天）匹配
"""

import sqlite3
from pathlib import Path
from datetime import datetime

DB_PATH = Path(__file__).parent.parent / "data" / "stockwinner.db"


def main():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # 1. 获取所有买入记录（尚无 strategy_id 的）
    cursor.execute("""
        SELECT id, account_id, stock_code, stock_name, trade_time
        FROM trade_records
        WHERE trade_type = 'buy' AND (strategy_id IS NULL OR strategy_id = 0)
        ORDER BY trade_time ASC
    """)
    buy_records = cursor.fetchall()
    print(f"找到 {len(buy_records)} 条未关联的买入记录")

    matched = 0
    unmatched = 0

    for br in buy_records:
        trade_time = br["trade_time"]
        # 解析时间（支持 ISO 格式和简单字符串格式）
        try:
            t = datetime.fromisoformat(trade_time.replace("Z", "+00:00"))
        except Exception:
            try:
                t = datetime.strptime(trade_time, "%Y-%m-%d %H:%M:%S")
            except Exception:
                unmatched += 1
                continue

        # 在 watchlist 中查找匹配（stock_code 相同，时间在 ±3 天内）
        cursor.execute("""
            SELECT id, strategy_id FROM watchlist
            WHERE account_id = ? AND stock_code = ?
              AND bought = 0
              AND selected_at BETWEEN ? AND ?
            ORDER BY selected_at DESC
            LIMIT 1
        """, (
            br["account_id"],
            br["stock_code"],
            (t.replace(hour=0, minute=0, second=0)).isoformat(),
            (t.replace(hour=23, minute=59, second=59)).isoformat(),
        ))

        wl = cursor.fetchone()
        if wl:
            # 关联成功
            cursor.execute(
                "UPDATE trade_records SET strategy_id = ?, signal_id = ? WHERE id = ?",
                (wl["strategy_id"], wl["id"], br["id"])
            )
            cursor.execute(
                "UPDATE watchlist SET bought = 1, buy_trade_id = ? WHERE id = ?",
                (br["id"], wl["id"])
            )
            matched += 1
            print(f"  关联: {br['stock_code']} {br['stock_name']} 买入于 {trade_time[:10]} -> watchlist#{wl['id']} strategy#{wl['strategy_id']}")
        else:
            # 扩大时间窗口到 ±7 天
            from datetime import timedelta
            start = (t - timedelta(days=7)).isoformat()
            end = (t + timedelta(days=7)).isoformat()
            cursor.execute("""
                SELECT id, strategy_id FROM watchlist
                WHERE account_id = ? AND stock_code = ?
                  AND bought = 0
                  AND selected_at BETWEEN ? AND ?
                ORDER BY ABS(julianday(selected_at) - julianday(?))
                LIMIT 1
            """, (br["account_id"], br["stock_code"], start, end, trade_time))
            wl2 = cursor.fetchone()
            if wl2:
                cursor.execute(
                    "UPDATE trade_records SET strategy_id = ?, signal_id = ? WHERE id = ?",
                    (wl2["strategy_id"], wl2["id"], br["id"])
                )
                cursor.execute(
                    "UPDATE watchlist SET bought = 1, buy_trade_id = ? WHERE id = ?",
                    (br["id"], wl2["id"])
                )
                matched += 1
                print(f"  关联(宽窗口): {br['stock_code']} -> watchlist#{wl2['id']}")
            else:
                unmatched += 1

    conn.commit()
    conn.close()
    print(f"\n关联完成: 成功 {matched} 条, 未匹配 {unmatched} 条")


if __name__ == "__main__":
    main()
