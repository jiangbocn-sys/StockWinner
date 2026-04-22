#!/usr/bin/env python3
"""
添加证券公司账户信息字段到 accounts 表
"""
import aiosqlite
import asyncio
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "stockwinner.db"

async def add_broker_columns():
    """添加银河证券账户信息字段"""

    async with aiosqlite.connect(DB_PATH) as db:
        # 检查并添加字段
        columns_to_add = [
            ("broker_account", "TEXT", "资金账号"),
            ("broker_password", "TEXT", "资金密码"),
            ("broker_company", "TEXT", "开户券商"),
            ("broker_server_ip", "TEXT", "服务器 IP"),
            ("broker_server_port", "INTEGER", "服务器端口"),
            ("broker_status", "TEXT", "账户状态"),
            ("notes", "TEXT", "备注"),
        ]

        # 获取现有列名
        cursor = await db.execute("PRAGMA table_info(accounts)")
        existing_columns = [row[1] for row in await cursor.fetchall()]

        for col_name, col_type, col_comment in columns_to_add:
            if col_name not in existing_columns:
                try:
                    await db.execute(f"ALTER TABLE accounts ADD COLUMN {col_name} {col_type}")
                    print(f"✅ 添加字段：{col_name} ({col_comment})")
                except Exception as e:
                    print(f"❌ 添加字段 {col_name} 失败：{e}")
            else:
                print(f"⚠️  字段 {col_name} 已存在")

        await db.commit()
        print("\n数据库结构更新完成！")

if __name__ == "__main__":
    asyncio.run(add_broker_columns())