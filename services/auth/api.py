"""
认证 API 路由
"""
from fastapi import APIRouter, HTTPException, Header, Depends
from typing import Dict, Any, Optional
from services.auth.service import get_auth_service
import logging
from datetime import datetime, timezone, timedelta

# 中国时区
CHINA_TZ = timezone(timedelta(hours=8))

def get_china_time():
    """获取中国时区时间"""
    return datetime.now(CHINA_TZ).replace(tzinfo=None)


logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/auth", tags=["auth"])


def get_token_from_header(x_auth_token: Optional[str] = Header(None, alias="X-Auth-Token")) -> str:
    """从请求头获取 token"""
    if not x_auth_token:
        raise HTTPException(status_code=401, detail="缺少认证 token")
    return x_auth_token


@router.post("/login")
async def login(credentials: Dict[str, str]):
    """
    用户登录

    请求体:
    - name: 用户名（账户名）
    - password: 密码
    """
    name = credentials.get("name", "")
    password = credentials.get("password", "")

    if not name or not password:
        raise HTTPException(status_code=400, detail="用户名和密码不能为空")

    service = get_auth_service()
    result = await service.login(name, password)

    if result["success"]:
        return result
    else:
        raise HTTPException(status_code=401, detail=result["message"])


@router.post("/logout")
async def logout(token: str = Depends(get_token_from_header)):
    """用户登出"""
    service = get_auth_service()
    await service.logout(token)
    return {"success": True, "message": "已登出"}


@router.get("/me")
async def get_current_user(token: str = Depends(get_token_from_header)):
    """获取当前登录用户信息"""
    service = get_auth_service()
    account = service.validate_token(token)

    if account:
        return {
            "success": True,
            "data": account
        }
    else:
        raise HTTPException(status_code=401, detail="认证失败或会话已过期")


@router.get("/broker-credentials")
async def get_broker_credentials(token: str = Depends(get_token_from_header)):
    """获取当前用户的券商账户 credentials"""
    service = get_auth_service()
    credentials = service.get_broker_credentials(token)

    if credentials:
        return {
            "success": True,
            "data": credentials
        }
    else:
        raise HTTPException(status_code=401, detail="认证失败或会话已过期")


@router.put("/password")
async def change_password(
    password_data: Dict[str, str],
    token: str = Depends(get_token_from_header)
):
    """
    修改当前用户的密码

    请求体:
    - old_password: 旧密码
    - new_password: 新密码（至少 6 位）
    """
    from services.common.database import get_db_manager
    import hashlib
    from datetime import datetime, timezone, timedelta

    old_password = password_data.get("old_password", "")
    new_password = password_data.get("new_password", "")

    if not old_password or not new_password:
        raise HTTPException(status_code=400, detail="旧密码和新密码不能为空")

    if len(new_password) < 6:
        raise HTTPException(status_code=400, detail="新密码至少需要 6 位")

    service = get_auth_service()
    account = service.validate_token(token)

    if not account:
        raise HTTPException(status_code=401, detail="认证失败或会话已过期")

    # 验证旧密码
    db = get_db_manager()
    old_password_hash = hashlib.sha256(old_password.encode()).hexdigest()

    db_account = await db.fetchone(
        "SELECT * FROM accounts WHERE id = ?",
        (account.get("id"),)
    )

    if not db_account or db_account.get("password_hash") != old_password_hash:
        raise HTTPException(status_code=400, detail="旧密码错误")

    # 更新密码
    new_password_hash = hashlib.sha256(new_password.encode()).hexdigest()
    await db.execute(
        "UPDATE accounts SET password_hash = ?, updated_at = ? WHERE id = ?",
        (new_password_hash, get_china_time().isoformat(), account.get("id"))
    )
    await db.commit()

    return {
        "success": True,
        "message": "密码修改成功"
    }
