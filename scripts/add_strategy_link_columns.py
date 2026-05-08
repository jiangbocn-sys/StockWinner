#!/usr/bin/env python3
"""
数据库迁移：为 strategies 表添加 buy_strategy_id 和 sell_strategy_id 列
"""
import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "stockwinner.db"


def main():
    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()

    # 检查列是否已存在
    columns = [row[1] for row in cursor.execute("PRAGMA table_info(strategies)")]

    if "buy_strategy_id" not in columns:
        cursor.execute("ALTER TABLE strategies ADD COLUMN buy_strategy_id INTEGER REFERENCES strategies(id)")
        print("已添加 buy_strategy_id 列")
    else:
        print("buy_strategy_id 列已存在")

    if "sell_strategy_id" not in columns:
        cursor.execute("ALTER TABLE strategies ADD COLUMN sell_strategy_id INTEGER REFERENCES strategies(id)")
        print("已添加 sell_strategy_id 列")
    else:
        print("sell_strategy_id 列已存在")

    conn.commit()
    conn.close()
    print("迁移完成")


if __name__ == "__main__":
    main()
