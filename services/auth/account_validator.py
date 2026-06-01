"""
账户验证服务

封装账户状态检查逻辑，避免重复代码分散在 20+ 处。
"""

from fastapi import HTTPException
from services.common.database import get_db_manager


async def validate_account_active(account_id: str) -> dict:
    """验证账户存在且活跃

    Args:
        account_id: 账户 ID

    Returns:
        账户完整信息（包含 available_cash, max_single_position_pct 等）

    Raises:
        HTTPException(404): 贡献不存在
        HTTPException(403): 贡献已禁用
    """
    db = await get_db_manager()
    account = await db.fetchone(
        "SELECT * FROM accounts WHERE account_id = ?",
        (account_id,)
    )
    if not account:
        raise HTTPException(status_code=404, detail=f"账户 {account_id} 不存在")
    if not account.get("is_active"):
        raise HTTPException(status_code=403, detail=f"账户 {account_id} 已禁用")
    return account


async def validate_account_exists(account_id: str) -> dict:
    """验证账户存在（不检查活跃状态）

    Args:
        account_id: 账户 ID

    Returns:
        账户完整信息

    Raises:
        HTTPException(404): 账户不存在
    """
    db = await get_db_manager()
    account = await db.fetchone(
        "SELECT * FROM accounts WHERE account_id = ?",
        (account_id,)
    )
    if not account:
        raise HTTPException(status_code=404, detail=f"账户 {account_id} 不存在")
    return account


async def get_active_account_ids() -> list:
    """获取所有活跃账户 ID"""
    db = await get_db_manager()
    rows = await db.fetchall(
        "SELECT account_id FROM accounts WHERE is_active = 1"
    )
    return [r["account_id"] for r in rows]


async def get_account_display_name(account_id: str) -> str:
    """获取账户显示名称"""
    db = await get_db_manager()
    account = await db.fetchone(
        "SELECT display_name, name FROM accounts WHERE account_id = ?",
        (account_id,)
    )
    if not account:
        return account_id
    return account.get("display_name") or account.get("name") or account_id