#!/usr/bin/env python3
"""
从 JSON 备份恢复 watchlist 分组数据

用法：python3 scripts/restore_watchlist_from_backup.py [备份文件路径]
默认读取 data/watchlist_backup_YYYYMMDD.json 中最新的备份
"""
import sqlite3
import json
import os
import sys
import glob

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

STOCK_DB = "data/stockwinner.db"
BACKUP_DIR = "data"

def find_latest_backup():
    files = glob.glob(os.path.join(BACKUP_DIR, "watchlist_backup_*.json"))
    if not files:
        print("未找到备份文件")
        sys.exit(1)
    return sorted(files)[-1]

def main():
    backup_path = sys.argv[1] if len(sys.argv) > 1 else find_latest_backup()

    if not os.path.exists(backup_path):
        print(f"备份文件不存在: {backup_path}")
        sys.exit(1)

    print(f"从备份恢复: {backup_path}")

    with open(backup_path, 'r') as f:
        groups = json.load(f)

    stock_conn = sqlite3.connect(STOCK_DB)
    stock_cur = stock_conn.cursor()

    total_stocks = 0
    for g in groups:
        name = g['name']
        account_id = g['account_id']
        print(f"\n恢复分组: {name} ({len(g['stocks'])} 只)")

        # 查找或创建分组
        stock_cur.execute(
            "SELECT id FROM candidate_groups WHERE name = ? AND account_id = ?",
            (name, account_id)
        )
        existing = stock_cur.fetchone()

        if existing:
            group_id = existing[0]
            stock_cur.execute("DELETE FROM watchlist WHERE group_id = ?", (group_id,))
        else:
            stock_cur.execute(
                "INSERT INTO candidate_groups (account_id, name, group_type, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (account_id, name, g.get('group_type', 'manual'),
                 g.get('created_at', ''), g.get('updated_at', ''))
            )
            group_id = stock_cur.lastrowid

        for s in g['stocks']:
            stock_cur.execute('''
                INSERT INTO watchlist (
                    account_id, group_id, source_type, stock_code, stock_name,
                    reason, status, target_quantity, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, ?)
            ''', (
                account_id, group_id,
                s.get('source_type', 'screening'),
                s['stock_code'], s['stock_name'],
                s.get('reason', ''),
                s.get('status', 'watching'),
                s.get('created_at', ''),
                s.get('updated_at', ''),
            ))
            total_stocks += 1

    stock_conn.commit()
    stock_conn.close()
    print(f"\n恢复完成：共 {len(groups)} 个分组，{total_stocks} 只股票")

if __name__ == "__main__":
    main()
