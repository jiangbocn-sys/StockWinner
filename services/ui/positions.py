"""
持仓管理 API
"""

from fastapi import APIRouter, HTTPException, Path, Query
from typing import Optional
from services.common.database import get_db_manager

router = APIRouter()


@router.get("/api/v1/ui/{account_id}/positions")
async def get_positions(
    account_id: str = Path(..., description="账户 ID"),
    stock_code: Optional[str] = Query(None, description="股票代码过滤")
):
    """获取持仓列表（包含可用资金）"""
    db = get_db_manager()

    # 从数据库验证账户
    account = await db.fetchone(
        "SELECT 1 FROM accounts WHERE account_id = ? AND is_active = 1",
        (account_id,)
    )
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    if stock_code:
        positions = await db.fetchall(
            "SELECT * FROM stock_positions WHERE account_id = ? AND stock_code = ? AND quantity > 0",
            (account_id, stock_code)
        )
    else:
        positions = await db.fetchall(
            "SELECT * FROM stock_positions WHERE account_id = ? AND quantity > 0 ORDER BY stock_code",
            (account_id,)
        )

    # 获取账户可用资金
    account_data = await db.fetchone(
        "SELECT available_cash FROM accounts WHERE account_id = ?",
        (account_id,)
    )
    available_cash = float(account_data["available_cash"]) if account_data else 0.0

    return {
        "account_id": account_id,
        "positions": positions,
        "available_cash": available_cash
    }


@router.get("/api/v1/ui/{account_id}/positions/{stock_code}")
async def get_position(
    account_id: str = Path(..., description="账户 ID"),
    stock_code: str = Path(..., description="股票代码")
):
    """获取单只股票持仓"""
    db = get_db_manager()

    # 从数据库验证账户
    account = await db.fetchone(
        "SELECT 1 FROM accounts WHERE account_id = ? AND is_active = 1",
        (account_id,)
    )
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在：{account_id}")

    position = await db.fetchone(
        "SELECT * FROM stock_positions WHERE account_id = ? AND stock_code = ?",
        (account_id, stock_code)
    )

    if not position:
        raise HTTPException(status_code=404, detail=f"持仓不存在：{stock_code}")

    return {"position": position}
