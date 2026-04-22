"""
用户认证服务
- 支持基于数据库 accounts 表的登录验证
- 管理用户会话（内存存储）
- 提供当前登录用户的券商 credentials
"""
import hashlib
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any
from services.common.database import get_db_manager

# 会话过期时间（秒）
SESSION_EXPIRY = 3600  # 1 小时

# 中国时区
CHINA_TZ = timezone(timedelta(hours=8))

def get_china_time():
    """获取中国时区时间"""
    return datetime.now(CHINA_TZ).replace(tzinfo=None)


class AuthService:
    """认证服务"""

    def __init__(self):
        self.db = get_db_manager()
        # 内存存储会话：token -> account info
        self._sessions: Dict[str, Dict[str, Any]] = {}

    def _hash_password(self, password: str) -> str:
        """SHA256 密码哈希"""
        return hashlib.sha256(password.encode()).hexdigest()

    async def login(self, name: str, password: str) -> Dict[str, Any]:
        """
        用户登录

        Args:
            name: 用户名（账户名）
            password: 密码（明文）

        Returns:
            登录结果：{success: bool, message: str, token: str, account: dict}
        """
        try:
            # 查询用户
            account = await self.db.fetchone(
                "SELECT * FROM accounts WHERE name = ? AND is_active = 1",
                (name,)
            )

            if not account:
                return {
                    "success": False,
                    "message": "用户名或密码错误",
                    "token": None,
                    "account": None
                }

            # 验证密码
            password_hash = self._hash_password(password)
            if account.get("password_hash") != password_hash:
                return {
                    "success": False,
                    "message": "用户名或密码错误",
                    "token": None,
                    "account": None
                }

            # 生成会话 token
            token = str(uuid.uuid4())

            # 存储会话（排除密码）
            account_data = dict(account)
            account_data.pop("password_hash", None)

            self._sessions[token] = {
                "account": account_data,
                "expires_at": get_china_time() + timedelta(seconds=SESSION_EXPIRY)
            }

            return {
                "success": True,
                "message": "登录成功",
                "token": token,
                "account": account_data
            }

        except Exception as e:
            return {
                "success": False,
                "message": f"登录失败：{str(e)}",
                "token": None,
                "account": None
            }

    async def logout(self, token: str) -> bool:
        """用户登出"""
        if token in self._sessions:
            del self._sessions[token]
            return True
        return False

    def validate_token(self, token: str) -> Optional[Dict[str, Any]]:
        """
        验证会话 token

        Returns:
            有效的账户信息，无效则返回 None
        """
        if token not in self._sessions:
            return None

        session = self._sessions[token]
        if get_china_time() > session["expires_at"]:
            del self._sessions[token]
            return None

        return session["account"]

    def get_broker_credentials(self, token: str) -> Optional[Dict[str, str]]:
        """
        获取当前用户的券商 credentials

        Returns:
            券商账号信息：{broker_account, broker_password, ...}
        """
        account = self.validate_token(token)
        if not account:
            return None

        return {
            "broker_account": account.get("broker_account", ""),
            "broker_password": account.get("broker_password", ""),
            "broker_company": account.get("broker_company", ""),
            "broker_server_ip": account.get("broker_server_ip", ""),
            "broker_server_port": account.get("broker_server_port", 8600),
            "broker_status": account.get("broker_status", "normal")
        }

    def cleanup_expired(self):
        """清理过期会话"""
        now = get_china_time()
        expired = [k for k, v in self._sessions.items() if v["expires_at"] < now]
        for k in expired:
            del self._sessions[k]


# 全局服务实例
_auth_service: Optional[AuthService] = None


def get_auth_service() -> AuthService:
    """获取认证服务单例"""
    global _auth_service
    if _auth_service is None:
        _auth_service = AuthService()
    return _auth_service


async def get_current_account(token: str) -> Optional[Dict[str, Any]]:
    """
    根据 token 获取当前账户信息
    用于依赖注入
    """
    service = get_auth_service()
    return service.validate_token(token)
