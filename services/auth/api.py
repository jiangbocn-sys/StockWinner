"""
认证 API 路由
"""
from fastapi import APIRouter, HTTPException, Header, Depends
from typing import Dict, Any, Optional
from services.auth.service import get_auth_service
import logging
from services.common.timezone import get_china_time
from services.common.database import get_db_manager


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


# ============================================================
# Agent 绑定 API — 用户通过 UI token 为自己创建/管理 Agent
# ============================================================

@router.post("/agent/bind")
async def bind_agent(
    agent_data: Dict[str, str],
    token: str = Depends(get_token_from_header),
):
    """为当前登录用户创建并绑定 Agent

    请求体:
    - name: Agent 名称（如 "OpenClaw-Agent"）
    - agent_type: Agent 类型（openclaw / hermes / claude_code / generic）
    - role: 角色（viewer / strategist / operator / admin，默认 viewer）

    返回:
    - agent_id, api_key, role 等
    - api_key 仅返回一次，不会再次显示
    """
    import uuid
    import hashlib
    import secrets
    import json

    service = get_auth_service()
    account = service.validate_token(token)
    if not account:
        raise HTTPException(status_code=401, detail="认证失败或会话已过期")

    name = agent_data.get("name", "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="Agent 名称不能为空")

    agent_type = agent_data.get("agent_type", "generic")
    role = agent_data.get("role", "viewer")
    allowed_roles = ["viewer", "strategist", "operator", "admin"]
    if role not in allowed_roles:
        raise HTTPException(status_code=400, detail=f"角色必须是 {allowed_roles} 之一")

    api_key = f"sk-agent-{secrets.token_hex(24)}"
    key_hash = hashlib.sha256(api_key.encode()).hexdigest()
    agent_id = str(uuid.uuid4())
    user_id = account["account_id"]
    now = get_china_time().strftime("%Y-%m-%dT%H:%M:%S")

    db = get_db_manager()
    await db.execute("""
        INSERT INTO agent_accounts
        (agent_id, user_id, name, agent_type, api_key_hash, role, allowed_account_ids, rate_limit_per_min, enabled, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, 60, 1, ?)
    """, (agent_id, user_id, name, agent_type, key_hash, role, json.dumps(["*"]), now))

    return {
        "success": True,
        "agent_id": agent_id,
        "name": name,
        "agent_type": agent_type,
        "role": role,
        "user_id": user_id,
        "api_key": api_key,
        "warning": "请妥善保存 api_key，系统不会再次显示",
    }


@router.get("/agents")
async def list_my_agents(token: str = Depends(get_token_from_header)):
    """获取当前登录用户的所有 Agent"""
    service = get_auth_service()
    account = service.validate_token(token)
    if not account:
        raise HTTPException(status_code=401, detail="认证失败或会话已过期")

    db = get_db_manager()
    agents = await db.fetchall(
        "SELECT agent_id, name, agent_type, role, rate_limit_per_min, enabled, created_at, last_used_at "
        "FROM agent_accounts WHERE user_id = ? ORDER BY created_at DESC",
        (account["account_id"],)
    )

    return {"success": True, "user_id": account["account_id"], "agents": agents}


@router.delete("/agent/{agent_id}")
async def unbind_agent(
    agent_id: str,
    token: str = Depends(get_token_from_header),
):
    """解绑（删除）当前用户的某个 Agent"""
    service = get_auth_service()
    account = service.validate_token(token)
    if not account:
        raise HTTPException(status_code=401, detail="认证失败或会话已过期")

    db = get_db_manager()
    result = await db.execute(
        "DELETE FROM agent_accounts WHERE agent_id = ? AND user_id = ?",
        (agent_id, account["account_id"])
    )
    if result.rowcount == 0:
        raise HTTPException(status_code=404, detail="Agent 不存在或不属于你")

    return {"success": True, "message": f"Agent {agent_id} 已解绑"}


@router.post("/agent/{agent_id}/rotate-key")
async def rotate_agent_key(
    agent_id: str,
    token: str = Depends(get_token_from_header),
):
    """重置 Agent 的 API Key（旧 key 立即失效）"""
    import secrets
    import hashlib

    service = get_auth_service()
    account = service.validate_token(token)
    if not account:
        raise HTTPException(status_code=401, detail="认证失败或会话已过期")

    db = get_db_manager()
    existing = await db.fetchone(
        "SELECT * FROM agent_accounts WHERE agent_id = ? AND user_id = ?",
        (agent_id, account["account_id"])
    )
    if not existing:
        raise HTTPException(status_code=404, detail="Agent 不存在或不属于你")

    new_key = f"sk-agent-{secrets.token_hex(24)}"
    new_hash = hashlib.sha256(new_key.encode()).hexdigest()
    await db.execute(
        "UPDATE agent_accounts SET api_key_hash = ? WHERE agent_id = ?",
        (new_hash, agent_id)
    )

    return {
        "success": True,
        "agent_id": agent_id,
        "api_key": new_key,
        "warning": "请妥善保存新 api_key，旧 key 已失效",
    }


# ============================================================
# 用户管理 API — 管理员专用
# ============================================================

def require_admin_role(token: str = Depends(get_token_from_header)) -> Dict[str, Any]:
    """要求管理员角色"""
    service = get_auth_service()
    account = service.validate_token(token)
    if not account:
        raise HTTPException(status_code=401, detail="认证失败或会话已过期")
    if account.get("role") != "admin":
        raise HTTPException(status_code=403, detail="需要管理员权限")
    return account


@router.get("/users")
async def list_users(admin: Dict[str, Any] = Depends(require_admin_role)):
    """列出所有用户（管理员专用）"""
    db = get_db_manager()
    users = await db.fetchall(
        "SELECT account_id, name, display_name, role, is_active, created_at FROM accounts ORDER BY created_at DESC"
    )
    return {"success": True, "users": users}


@router.post("/users")
async def create_user(
    user_data: Dict[str, str],
    admin: Dict[str, Any] = Depends(require_admin_role),
):
    """创建用户（管理员专用）

    请求体:
    - name: 用户名（账户名）
    - password: 密码（至少 6 位）
    - display_name: 显示名称（可选）
    - role: 角色（admin / user，默认 user）
    """
    import hashlib
    import uuid

    name = user_data.get("name", "").strip()
    password = user_data.get("password", "")
    display_name = user_data.get("display_name", name)
    role = user_data.get("role", "user")

    if not name or not password:
        raise HTTPException(status_code=400, detail="用户名和密码不能为空")
    if len(password) < 6:
        raise HTTPException(status_code=400, detail="密码至少需要 6 位")
    if role not in ("admin", "user"):
        raise HTTPException(status_code=400, detail="角色必须是 admin 或 user")

    db = get_db_manager()
    existing = await db.fetchone("SELECT * FROM accounts WHERE name = ?", (name,))
    if existing:
        raise HTTPException(status_code=400, detail="用户名已存在")

    password_hash = hashlib.sha256(password.encode()).hexdigest()
    account_id = str(uuid.uuid4())[:8].upper()
    now = get_china_time().strftime("%Y-%m-%dT%H:%M:%S")

    await db.execute("""
        INSERT INTO accounts (account_id, name, password_hash, display_name, role, is_active, created_at)
        VALUES (?, ?, ?, ?, ?, 1, ?)
    """, (account_id, name, password_hash, display_name, role, now))

    return {
        "success": True,
        "account_id": account_id,
        "name": name,
        "role": role,
        "message": "用户创建成功"
    }


@router.put("/users/{account_id}/role")
async def update_user_role(
    account_id: str,
    role_data: Dict[str, str],
    admin: Dict[str, Any] = Depends(require_admin_role),
):
    """修改用户角色（管理员专用）

    请求体:
    - role: 新角色（admin / user）
    """
    new_role = role_data.get("role", "")
    if new_role not in ("admin", "user"):
        raise HTTPException(status_code=400, detail="角色必须是 admin 或 user")

    # 防止修改自己的角色
    if account_id == admin.get("account_id"):
        raise HTTPException(status_code=400, detail="不能修改自己的角色")

    db = get_db_manager()
    result = await db.execute(
        "UPDATE accounts SET role = ?, updated_at = ? WHERE account_id = ?",
        (new_role, get_china_time().strftime("%Y-%m-%dT%H:%M:%S"), account_id)
    )
    if result.rowcount == 0:
        raise HTTPException(status_code=404, detail="用户不存在")

    return {"success": True, "account_id": account_id, "role": new_role}


@router.delete("/users/{account_id}")
async def delete_user(
    account_id: str,
    admin: Dict[str, Any] = Depends(require_admin_role),
):
    """删除用户（管理员专用）"""
    # 防止删除自己
    if account_id == admin.get("account_id"):
        raise HTTPException(status_code=400, detail="不能删除自己")

    db = get_db_manager()

    # 检查是否有持仓等关联数据
    positions = await db.fetchone(
        "SELECT COUNT(*) as cnt FROM stock_positions WHERE account_id = ?",
        (account_id,)
    )
    if positions and positions.get("cnt", 0) > 0:
        raise HTTPException(status_code=400, detail="用户有持仓记录，无法删除")

    result = await db.execute("DELETE FROM accounts WHERE account_id = ?", (account_id,))
    if result.rowcount == 0:
        raise HTTPException(status_code=404, detail="用户不存在")

    return {"success": True, "account_id": account_id, "message": "用户已删除"}


@router.put("/users/{account_id}/status")
async def toggle_user_status(
    account_id: str,
    status_data: Dict[str, int],
    admin: Dict[str, Any] = Depends(require_admin_role),
):
    """锁定/解锁账户（管理员专用）

    请求体:
    - is_active: 1=激活, 0=锁定
    """
    is_active = status_data.get("is_active")
    if is_active not in (0, 1):
        raise HTTPException(status_code=400, detail="is_active 必须是 0 或 1")

    # 防止锁定自己
    if account_id == admin.get("account_id"):
        raise HTTPException(status_code=400, detail="不能锁定自己的账户")

    db = get_db_manager()
    result = await db.execute(
        "UPDATE accounts SET is_active = ?, updated_at = ? WHERE account_id = ?",
        (is_active, get_china_time().strftime("%Y-%m-%dT%H:%M:%S"), account_id)
    )
    if result.rowcount == 0:
        raise HTTPException(status_code=404, detail="用户不存在")

    status_text = "激活" if is_active else "锁定"
    return {"success": True, "account_id": account_id, "is_active": is_active, "message": f"账户已{status_text}"}


@router.put("/me")
async def update_my_profile(
    profile_data: Dict[str, Any],
    token: str = Depends(get_token_from_header),
):
    """更新个人信息（所有用户）

    可更新字段:
    - display_name: 显示名称
    - broker_account: 券商资金账号
    - broker_password: 券商密码
    - broker_company: 开户券商
    - broker_server_ip: 服务器 IP
    - broker_server_port: 服务器端口
    - commission_rate: 佣金费率
    - stamp_tax: 印花税
    - transfer_fee: 过户费
    - min_commission: 最低佣金
    - available_cash: 可用资金
    """
    service = get_auth_service()
    account = service.validate_token(token)
    if not account:
        raise HTTPException(status_code=401, detail="认证失败或会话已过期")

    db = get_db_manager()

    # 可更新字段列表
    updatable_fields = [
        "display_name", "broker_account", "broker_password", "broker_company",
        "broker_server_ip", "broker_server_port", "commission_rate",
        "stamp_tax", "transfer_fee", "min_commission", "available_cash", "notes",
        "notifications_paused"
    ]

    updates = {}
    for field in updatable_fields:
        if field in profile_data:
            updates[field] = profile_data[field]

    if not updates:
        return {"success": True, "message": "无需更新"}

    updates["updated_at"] = get_china_time().strftime("%Y-%m-%dT%H:%M:%S")

    set_clause = ", ".join([f"{k} = ?" for k in updates.keys()])
    values = list(updates.values()) + [account["account_id"]]

    await db.execute(f"UPDATE accounts SET {set_clause} WHERE account_id = ?", values)

    return {"success": True, "message": "个人信息已更新"}
