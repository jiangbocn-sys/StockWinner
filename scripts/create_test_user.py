#!/usr/bin/env python3
"""
更新测试账户密码
"""
import asyncio
import hashlib
from datetime import datetime
import aiosqlite

DB_PATH = "data/stockwinner.db"

async def update_test_password():
    """添加/更新测试账户"""

    # 测试账户信息
    test_user = "test"
    test_password = "test123"  # 明文密码
    password_hash = hashlib.sha256(test_password.encode()).hexdigest()

    async with aiosqlite.connect(DB_PATH) as db:
        # 检查是否已存在
        cursor = await db.execute("SELECT * FROM accounts WHERE username = ?", (test_user,))
        existing = await cursor.fetchone()

        if existing:
            print(f"更新用户 {test_user} 的密码...")
            await db.execute(
                "UPDATE accounts SET password_hash = ?, updated_at = ? WHERE username = ?",
                (password_hash, datetime.now().isoformat(), test_user)
            )
            print(f"✅ 用户 {test_user} 密码已更新为：{test_password}")
        else:
            print(f"创建测试用户 {test_user}...")
            import uuid
            account_id = str(uuid.uuid4())[:8].upper()
            await db.execute(
                """INSERT INTO accounts
                (account_id, name, username, password_hash, display_name, is_active, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, 1, ?, ?)""",
                (
                    account_id,
                    f"测试账户",
                    test_user,
                    password_hash,
                    f"测试账户 {test_user}",
                    datetime.now().isoformat(),
                    datetime.now().isoformat()
                )
            )
            print(f"✅ 测试用户已创建：{test_user} / {test_password}")
            print(f"   账户 ID: {account_id}")

        await db.commit()

        # 显示所有账户
        cursor = await db.execute("SELECT username, display_name, is_active FROM accounts")
        accounts = await cursor.fetchall()
        print("\n当前所有账户:")
        for acc in accounts:
            status = "激活" if acc[2] else "禁用"
            print(f"  - {acc[0]} ({acc[1]}) - {status}")

if __name__ == "__main__":
    asyncio.run(update_test_password())
