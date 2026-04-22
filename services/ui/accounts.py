"""
账户管理 API
"""

from fastapi import APIRouter, HTTPException, Path, Body
from typing import List, Optional
from datetime import datetime, timezone, timedelta
from services.common.database import get_db_manager
from services.common.account_manager import get_account_manager

# 中国时区
CHINA_TZ = timezone(timedelta(hours=8))

def get_china_time():
    """获取中国时区时间"""
    return datetime.now(CHINA_TZ).replace(tzinfo=None)

router = APIRouter()


@router.get("/api/v1/ui/accounts")
async def list_accounts():
    """获取账户列表（包含可用资金）"""
    db = get_db_manager()

    accounts = await db.fetchall(
        "SELECT * FROM accounts ORDER BY account_id"
    )

    return {
        "success": True,
        "data": [
            {
                "account_id": acc["account_id"],
                "name": acc["name"],
                "display_name": acc["display_name"],
                "is_active": bool(acc["is_active"]),
                "available_cash": float(acc["available_cash"] or 0),
                "broker_account": acc.get("broker_account", ""),
                "broker_company": acc.get("broker_company", ""),
                "broker_server_ip": acc.get("broker_server_ip", ""),
                "broker_server_port": acc.get("broker_server_port", ""),
                "broker_status": acc.get("broker_status", ""),
                "notes": acc.get("notes", ""),
                "created_at": acc.get("created_at", ""),
                "updated_at": acc.get("updated_at", "")
            }
            for acc in accounts
        ]
    }


@router.get("/api/v1/ui/accounts/statistics")
async def get_statistics():
    """获取账户统计信息"""
    db = get_db_manager()

    # 统计总数
    total = await db.fetchone("SELECT COUNT(*) as cnt FROM accounts")
    active = await db.fetchone("SELECT COUNT(*) as cnt FROM accounts WHERE is_active = 1")
    inactive = await db.fetchone("SELECT COUNT(*) as cnt FROM accounts WHERE is_active = 0")

    # 统计总资金
    total_assets = await db.fetchone("SELECT SUM(available_cash) as total FROM accounts WHERE is_active = 1")

    return {
        "success": True,
        "data": {
            "total_accounts": total["cnt"] if total else 0,
            "active_accounts": active["cnt"] if active else 0,
            "inactive_accounts": inactive["cnt"] if inactive else 0,
            "total_available_cash": float(total_assets["total"] or 0)
        }
    }


@router.get("/api/v1/ui/accounts/{account_id}")
async def get_account(account_id: str = Path(..., description="账户 ID")):
    """获取账户详情"""
    db = get_db_manager()

    # 检查账户是否存在
    existing = await db.fetchone(
        "SELECT 1 FROM accounts WHERE account_id = ?",
        (account_id,)
    )

    if not existing:
        raise HTTPException(status_code=404, detail="账户不存在")

    account = await db.fetchone(
        "SELECT * FROM accounts WHERE account_id = ?",
        (account_id,)
    )

    return {
        "success": True,
        "data": {
            "account_id": account["account_id"],
            "name": account["name"],
            "display_name": account["display_name"],
            "is_active": bool(account["is_active"]),
            "available_cash": float(account["available_cash"] or 0),
            "broker_account": account.get("broker_account", ""),
            "broker_password": account.get("broker_password", ""),
            "broker_company": account.get("broker_company", ""),
            "broker_server_ip": account.get("broker_server_ip", ""),
            "broker_server_port": account.get("broker_server_port", ""),
            "broker_status": account.get("broker_status", ""),
            "notes": account.get("notes", ""),
            "created_at": account.get("created_at", ""),
            "updated_at": account.get("updated_at", "")
        }
    }


@router.post("/api/v1/ui/accounts/create")
async def create_account(account_data: dict = Body(...)):
    """创建账户"""
    db = get_db_manager()

    # 检查账户 ID 是否已存在
    existing = await db.fetchone(
        "SELECT 1 FROM accounts WHERE account_id = ? OR name = ?",
        (account_data.get("name"), account_data.get("name"))
    )

    if existing:
        raise HTTPException(status_code=400, detail="账户名称已存在")

    # 加密密码
    import hashlib
    password_hash = hashlib.sha256(account_data.get("password", "").encode()).hexdigest()

    # 插入数据库
    await db.execute(
        """INSERT INTO accounts
           (account_id, name, password_hash, display_name, is_active,
            broker_account, broker_password, broker_company, broker_server_ip,
            broker_server_port, broker_status, notes, available_cash)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            account_data.get("name"),
            account_data.get("name"),
            password_hash,
            account_data.get("display_name", ""),
            account_data.get("is_active", 1),
            account_data.get("broker_account", ""),
            account_data.get("broker_password", ""),
            account_data.get("broker_company", ""),
            account_data.get("broker_server_ip", ""),
            account_data.get("broker_server_port", 8600),
            account_data.get("broker_status", "normal"),
            account_data.get("notes", ""),
            float(account_data.get("available_cash", 0))
        )
    )

    return {"success": True, "message": "账户创建成功"}


@router.put("/api/v1/ui/accounts/{account_id}")
async def update_account(account_id: str, account_data: dict = Body(...)):
    """更新账户"""
    db = get_db_manager()

    # 检查账户是否存在
    existing = await db.fetchone(
        "SELECT 1 FROM accounts WHERE account_id = ?",
        (account_id,)
    )

    if not existing:
        raise HTTPException(status_code=404, detail="账户不存在")

    # 更新数据库
    update_fields = []
    params = []

    # 构建动态更新语句
    updatable_fields = [
        "name", "display_name", "is_active", "available_cash",
        "broker_account", "broker_password", "broker_company",
        "broker_server_ip", "broker_server_port", "broker_status", "notes"
    ]

    for field in updatable_fields:
        if field in account_data:
            update_fields.append(f"{field} = ?")
            params.append(account_data[field])

    # 处理密码更新
    if account_data.get("password"):
        import hashlib
        password_hash = hashlib.sha256(account_data["password"].encode()).hexdigest()
        update_fields.append("password_hash = ?")
        params.append(password_hash)

    params.append(get_china_time().isoformat())
    params.append(account_id)

    await db.execute(
        f"""UPDATE accounts
            SET {", ".join(update_fields)}, updated_at = ?
            WHERE account_id = ?""",
        params
    )

    return {"success": True, "message": "账户更新成功"}


@router.delete("/api/v1/ui/accounts/{account_id}")
async def delete_account(account_id: str = Path(..., description="账户 ID")):
    """删除账户"""
    db = get_db_manager()

    # 检查账户是否存在
    existing = await db.fetchone(
        "SELECT 1 FROM accounts WHERE account_id = ?",
        (account_id,)
    )

    if not existing:
        raise HTTPException(status_code=404, detail="账户不存在")

    # 删除账户
    await db.execute("DELETE FROM accounts WHERE account_id = ?", (account_id,))

    return {"success": True, "message": "账户已删除"}


@router.get("/api/v1/ui/accounts/statistics")
async def get_statistics():
    """获取账户统计信息"""
    db = get_db_manager()

    # 统计总数
    total = await db.fetchone("SELECT COUNT(*) as cnt FROM accounts")
    active = await db.fetchone("SELECT COUNT(*) as cnt FROM accounts WHERE is_active = 1")
    inactive = await db.fetchone("SELECT COUNT(*) as cnt FROM accounts WHERE is_active = 0")

    # 统计总资金
    total_assets = await db.fetchone("SELECT SUM(available_cash) as total FROM accounts WHERE is_active = 1")

    return {
        "success": True,
        "data": {
            "total_accounts": total["cnt"] if total else 0,
            "active_accounts": active["cnt"] if active else 0,
            "inactive_accounts": inactive["cnt"] if inactive else 0,
            "total_available_cash": float(total_assets["total"] or 0)
        }
    }
