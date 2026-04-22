"""
账户管理模块
- 支持账户的增删改查
- 管理账户的各种字段内容
"""
import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any
from services.common.database import get_db_manager
import hashlib

# 中国时区
CHINA_TZ = timezone(timedelta(hours=8))

def get_china_time():
    """获取中国时区时间"""
    return datetime.now(CHINA_TZ).replace(tzinfo=None)

logger = logging.getLogger(__name__)


class AccountManagerService:
    """账户管理服务"""

    def __init__(self):
        self.db = get_db_manager()

    def _hash_password(self, password: str) -> str:
        """对密码进行哈希处理"""
        return hashlib.sha256(password.encode()).hexdigest()

    async def create_account(
        self,
        name: str,
        username: str,
        password: str,
        display_name: str = "",
        is_active: int = 1,
        **extra_fields
    ) -> Dict[str, Any]:
        """
        创建新账户

        Args:
            name: 账户名称（唯一标识）
            username: 用户名
            password: 密码（会自动哈希）
            display_name: 显示名称
            is_active: 是否激活 (1=激活，0=禁用)
            **extra_fields: 其他字段

        Returns:
            创建的账户信息
        """
        try:
            # 检查账户名是否已存在
            existing_accounts = await self.get_accounts_by_name(name)
            if existing_accounts:
                return {
                    "success": False,
                    "message": f"账户名 '{name}' 已存在",
                    "data": None
                }

            # 生成账户 ID
            import uuid
            account_id = str(uuid.uuid4())[:8].upper()

            # 准备账户数据
            account_data = {
                "account_id": account_id,
                "name": name,
                "username": username,
                "password_hash": self._hash_password(password),
                "display_name": display_name or name,
                "is_active": is_active,
                "created_at": get_china_time().isoformat(),
                "updated_at": get_china_time().isoformat()
            }

            # 添加额外字段
            for key, value in extra_fields.items():
                if key not in account_data:
                    account_data[key] = value

            # 插入数据库
            inserted_id = await self.db.insert("accounts", account_data)

            logger.info(f"创建账户成功：{name} (ID: {account_id})")

            # 获取完整账户信息
            full_account = await self.get_account_by_db_id(inserted_id)

            return {
                "success": True,
                "message": "账户创建成功",
                "data": full_account
            }

        except Exception as e:
            logger.error(f"创建账户失败：{e}")
            return {
                "success": False,
                "message": f"创建账户失败：{str(e)}",
                "data": None
            }

    async def get_account_by_db_id(self, db_id: int) -> Optional[Dict[str, Any]]:
        """根据数据库 ID 获取账户信息"""
        try:
            account = await self.db.fetchone(
                "SELECT * FROM accounts WHERE id = ?",
                (db_id,)
            )
            return account
        except Exception as e:
            logger.error(f"获取账户失败：{e}")
            return None

    async def get_account_by_id(self, account_id: str) -> Optional[Dict[str, Any]]:
        """根据账户 ID 获取账户信息"""
        try:
            account = await self.db.fetchone(
                "SELECT * FROM accounts WHERE account_id = ?",
                (account_id,)
            )
            return account
        except Exception as e:
            logger.error(f"获取账户失败：{e}")
            return None

    async def get_accounts_by_name(self, name: str) -> List[Dict[str, Any]]:
        """根据名称获取账户信息"""
        try:
            accounts = await self.db.fetchall(
                "SELECT * FROM accounts WHERE name = ?",
                (name,)
            )
            return accounts
        except Exception as e:
            logger.error(f"获取账户失败：{e}")
            return []

    async def get_all_accounts(self) -> List[Dict[str, Any]]:
        """获取所有账户"""
        try:
            accounts = await self.db.fetchall(
                "SELECT * FROM accounts ORDER BY created_at DESC"
            )
            return accounts
        except Exception as e:
            logger.error(f"获取所有账户失败：{e}")
            return []

    async def update_account(self, account_id: str, **updates) -> Dict[str, Any]:
        """
        更新账户信息

        Args:
            account_id: 账户 ID (account_id 字段)
            **updates: 要更新的字段

        Returns:
            更新结果
        """
        try:
            # 检查账户是否存在
            existing_account = await self.get_account_by_id(account_id)
            if not existing_account:
                return {
                    "success": False,
                    "message": f"账户 ID '{account_id}' 不存在",
                    "data": None
                }

            # 准备更新数据
            update_data = {
                "updated_at": get_china_time().isoformat()
            }

            # 如果要更新密码，需要哈希
            if "password" in updates:
                update_data["password_hash"] = self._hash_password(updates["password"])
                del updates["password"]

            # 添加需要更新的字段（排除保护字段）
            protected_fields = ["id", "account_id", "created_at"]
            for key, value in updates.items():
                if key not in protected_fields:
                    update_data[key] = value

            # 执行更新
            await self.db.update(
                "accounts",
                update_data,
                "account_id = ?",
                (account_id,)
            )

            logger.info(f"更新账户成功：{account_id}")

            # 获取更新后的完整账户信息
            updated_account = await self.get_account_by_id(account_id)

            return {
                "success": True,
                "message": "账户更新成功",
                "data": updated_account
            }

        except Exception as e:
            logger.error(f"更新账户失败：{e}")
            return {
                "success": False,
                "message": f"更新账户失败：{str(e)}",
                "data": None
            }

    async def update_account_by_db_id(self, db_id: int, **updates) -> Dict[str, Any]:
        """
        根据数据库 ID 更新账户信息

        Args:
            db_id: 数据库自增 ID
            **updates: 要更新的字段

        Returns:
            更新结果
        """
        try:
            # 检查账户是否存在
            existing_account = await self.get_account_by_db_id(db_id)
            if not existing_account:
                return {
                    "success": False,
                    "message": f"账户 ID '{db_id}' 不存在",
                    "data": None
                }

            # 准备更新数据
            update_data = {
                "updated_at": get_china_time().isoformat()
            }

            # 如果要更新密码，需要哈希
            if "password" in updates:
                update_data["password_hash"] = self._hash_password(updates["password"])
                del updates["password"]

            # 添加需要更新的字段（排除保护字段）
            protected_fields = ["id", "account_id", "created_at"]
            for key, value in updates.items():
                if key not in protected_fields:
                    update_data[key] = value

            # 执行更新
            await self.db.update(
                "accounts",
                update_data,
                "id = ?",
                (db_id,)
            )

            logger.info(f"更新账户成功：{db_id}")

            # 获取更新后的完整账户信息
            updated_account = await self.get_account_by_db_id(db_id)

            return {
                "success": True,
                "message": "账户更新成功",
                "data": updated_account
            }

        except Exception as e:
            logger.error(f"更新账户失败：{e}")
            return {
                "success": False,
                "message": f"更新账户失败：{str(e)}",
                "data": None
            }

    async def delete_account(self, account_id: str) -> Dict[str, Any]:
        """
        删除账户（物理删除）

        Args:
            account_id: 账户 ID

        Returns:
            删除结果
        """
        try:
            # 检查账户是否存在
            existing_account = await self.get_account_by_id(account_id)
            if not existing_account:
                return {
                    "success": False,
                    "message": f"账户 ID '{account_id}' 不存在",
                    "data": None
                }

            # 物理删除
            await self.db.delete(
                "accounts",
                "account_id = ?",
                (account_id,)
            )

            logger.info(f"删除账户成功：{account_id}")

            return {
                "success": True,
                "message": "账户删除成功",
                "data": {"account_id": account_id}
            }

        except Exception as e:
            logger.error(f"删除账户失败：{e}")
            return {
                "success": False,
                "message": f"删除账户失败：{str(e)}",
                "data": None
            }

    async def search_accounts(self, **criteria) -> List[Dict[str, Any]]:
        """
        根据条件搜索账户

        Args:
            **criteria: 搜索条件

        Returns:
            匹配的账户列表
        """
        try:
            # 构建查询条件
            where_parts = []
            params = []

            for key, value in criteria.items():
                if key == "is_active":
                    where_parts.append(f"is_active = ?")
                    params.append(value)
                elif key == "name":
                    where_parts.append(f"name LIKE ?")
                    params.append(f"%{value}%")
                elif key == "username":
                    where_parts.append(f"username LIKE ?")
                    params.append(f"%{value}%")
                elif key == "display_name":
                    where_parts.append(f"display_name LIKE ?")
                    params.append(f"%{value}%")

            where_clause = " AND ".join(where_parts) if where_parts else "1=1"
            query = f"SELECT * FROM accounts WHERE {where_clause} ORDER BY created_at DESC"

            accounts = await self.db.fetchall(query, params)
            return accounts

        except Exception as e:
            logger.error(f"搜索账户失败：{e}")
            return []

    async def get_account_statistics(self) -> Dict[str, Any]:
        """获取账户统计信息"""
        try:
            # 总账户数
            total_count = await self.db.fetchval(
                "SELECT COUNT(*) as count FROM accounts"
            )

            # 按激活状态统计
            active_count = await self.db.fetchval(
                "SELECT COUNT(*) as count FROM accounts WHERE is_active = 1"
            )

            stats = {
                "total_accounts": total_count or 0,
                "active_accounts": active_count or 0,
                "inactive_accounts": (total_count or 0) - (active_count or 0)
            }

            return {
                "success": True,
                "message": "获取统计信息成功",
                "data": stats
            }

        except Exception as e:
            logger.error(f"获取账户统计失败：{e}")
            return {
                "success": False,
                "message": f"获取统计信息失败：{str(e)}",
                "data": None
            }


# 全局账户管理服务实例
_account_service: Optional[AccountManagerService] = None


def get_account_management_service() -> AccountManagerService:
    """获取账户管理服务单例"""
    global _account_service
    if _account_service is None:
        _account_service = AccountManagerService()
    return _account_service


def reset_account_management_service():
    """重置账户管理服务（用于测试）"""
    global _account_service
    _account_service = None