"""
多账户管理器
负责账户信息管理和验证（基于数据库）
"""

from typing import Dict, List, Optional
from datetime import datetime, timezone, timedelta

# 中国时区
CHINA_TZ = timezone(timedelta(hours=8))

def get_china_time():
    """获取中国时区时间"""
    return datetime.now(CHINA_TZ).replace(tzinfo=None)


class AccountManager:
    """账户管理器 - 基于数据库验证"""

    async def get_account(self, account_id: str) -> Optional[Dict]:
        """从数据库获取账户信息"""
        try:
            from services.common.database import get_db_manager
            db = get_db_manager()
            return await db.fetchone(
                "SELECT * FROM accounts WHERE account_id = ?",
                (account_id,)
            )
        except Exception as e:
            print(f"[AccountManager] 获取账户失败：{e}")
            return None

    async def get_active_accounts(self) -> List[Dict]:
        """从数据库获取所有激活的账户"""
        try:
            from services.common.database import get_db_manager
            db = get_db_manager()
            return await db.fetchall(
                "SELECT * FROM accounts WHERE is_active = 1 ORDER BY account_id"
            )
        except Exception as e:
            print(f"[AccountManager] 获取账户列表失败：{e}")
            return []

    async def list_accounts(self) -> List[Dict]:
        """从数据库获取所有账户"""
        try:
            from services.common.database import get_db_manager
            db = get_db_manager()
            return await db.fetchall(
                "SELECT * FROM accounts ORDER BY account_id"
            )
        except Exception as e:
            print(f"[AccountManager] 获取账户列表失败：{e}")
            return []

    async def validate_account(self, account_id: str) -> bool:
        """验证账户是否存在且激活（基于数据库）"""
        try:
            from services.common.database import get_db_manager
            db = get_db_manager()

            db_account = await db.fetchone(
                "SELECT account_id FROM accounts WHERE account_id = ? AND is_active = 1",
                (account_id,)
            )
            return db_account is not None
        except Exception as e:
            print(f"[AccountManager] 验证账户失败：{e}")
            return False

    async def get_account_display_name(self, account_id: str) -> str:
        """获取账户显示名称"""
        account = await self.get_account(account_id)
        if account:
            return account.get('display_name', account_id)
        return account_id


# 全局单例
_account_manager: Optional[AccountManager] = None


def get_account_manager() -> AccountManager:
    """获取账户管理器单例"""
    global _account_manager
    if _account_manager is None:
        _account_manager = AccountManager()
    return _account_manager


def reset_account_manager():
    """重置账户管理器（用于测试）"""
    global _account_manager
    _account_manager = None
