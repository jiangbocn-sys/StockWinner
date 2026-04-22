#!/usr/bin/env python3
"""
清理 accounts 表中未使用的字段：
- cash_balance (与 available_cash 重复)
- frozen_cash (从未使用)
- total_asset (从未使用)
"""
import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "stockwinner.db"

def cleanup_accounts_table():
    """清理 accounts 表未使用的字段"""

    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()

    # 1. 备份现有数据
    print("备份现有数据...")
    cursor.execute("""
        SELECT account_id, available_cash FROM accounts
    """)
    backup_data = cursor.fetchall()
    print(f"已备份 {len(backup_data)} 条账户数据")

    # 2. 检查哪些字段存在
    cursor.execute("PRAGMA table_info(accounts)")
    columns = [row[1] for row in cursor.fetchall()]
    print(f"\n当前字段列表：{columns}")

    # 3. 移除未使用的字段（通过重建表）
    columns_to_keep = [
        'id', 'account_id', 'name', 'username', 'password_hash',
        'display_name', 'is_active', 'created_at', 'updated_at',
        'broker_account', 'broker_password', 'broker_company',
        'broker_server_ip', 'broker_server_port', 'broker_status', 'notes',
        'available_cash'  # 只保留这个资金字段
    ]

    # 检查哪些字段需要保留且当前存在
    existing_columns = set(columns)
    keep_columns = [col for col in columns_to_keep if col in existing_columns]
    print(f"\n保留字段：{keep_columns}")

    # 4. 创建临时表
    print("\n创建临时表...")
    columns_def = {
        'id': 'INTEGER PRIMARY KEY AUTOINCREMENT',
        'account_id': 'TEXT NOT NULL UNIQUE',
        'name': 'TEXT NOT NULL',
        'username': 'TEXT NOT NULL UNIQUE',
        'password_hash': 'TEXT NOT NULL',
        'display_name': 'TEXT',
        'is_active': 'INTEGER DEFAULT 1',
        'created_at': 'DATETIME NOT NULL DEFAULT (datetime(\'now\'))',
        'updated_at': 'DATETIME NOT NULL DEFAULT (datetime(\'now\'))',
        'broker_account': 'TEXT',
        'broker_password': 'TEXT',
        'broker_company': 'TEXT',
        'broker_server_ip': 'TEXT',
        'broker_server_port': 'INTEGER',
        'broker_status': 'TEXT',
        'notes': 'TEXT',
        'available_cash': 'REAL DEFAULT 0.0'
    }

    create_cols = ', '.join([f"{col} {columns_def[col]}" for col in keep_columns if col != 'id'])
    create_cols_with_id = f"id INTEGER PRIMARY KEY AUTOINCREMENT, {create_cols}" if 'id' in keep_columns else create_cols

    cursor.execute("DROP TABLE IF EXISTS accounts_backup")
    cursor.execute("ALTER TABLE accounts RENAME TO accounts_backup")

    cursor.execute(f"""
        CREATE TABLE accounts (
            {create_cols_with_id}
        )
    """)

    # 5. 迁移数据
    print("迁移数据...")
    select_cols = ', '.join([col for col in keep_columns if col != 'id'])
    insert_cols = ', '.join([col for col in keep_columns if col != 'id'])

    cursor.execute(f"""
        INSERT INTO accounts ({select_cols})
        SELECT {select_cols} FROM accounts_backup
    """)

    # 6. 删除备份表
    cursor.execute("DROP TABLE accounts_backup")

    # 7. 提交
    conn.commit()

    # 8. 验证结果
    cursor.execute("PRAGMA table_info(accounts)")
    new_columns = [row[1] for row in cursor.fetchall()]
    print(f"\n清理后字段：{new_columns}")

    cursor.execute("SELECT account_id, available_cash FROM accounts")
    for row in cursor.fetchall():
        print(f"  {row[0]}: available_cash = {row[1]}")

    conn.close()
    print("\n✅ 清理完成!")

if __name__ == "__main__":
    cleanup_accounts_table()
