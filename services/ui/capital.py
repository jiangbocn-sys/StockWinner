"""
策略资金 API (Strategy Capital API)
- 资金总览
- 策略现金详情
- 借用记录管理
"""

from fastapi import APIRouter, Request, HTTPException, Body
from typing import Optional
from services.trading.strategy_cash_service import get_strategy_cash_service
from services.common.structured_logger import get_logger

logger = get_logger("capital_api")
router = APIRouter()


@router.get("/api/v1/ui/{account_id}/capital/overview")
async def get_capital_overview(request: Request, account_id: str):
    """
    资金总览：账户现金 + 各策略资产

    Returns:
        {
            account_cash: float,
            strategies: [
                {strategy_id, name, allocated_capital, strategy_cash, positions_mv, total_asset},
            ],
            total_strategy_assets: float,
        }
    """
    # 验证 account_id 与认证一致
    auth_account_id = request.state.account_id
    if account_id != auth_account_id:
        raise HTTPException(status_code=403, detail="账户ID不匹配")

    cash_svc = get_strategy_cash_service(account_id)

    # 获取账户实际现金
    from services.common.database import get_db_manager
    db = get_db_manager()
    account = await db.fetchone(
        "SELECT available_cash FROM accounts WHERE account_id = ?",
        (account_id,)
    )
    account_cash = account.get("available_cash", 0) if account else 0

    # 获取各策略资产汇总
    strategies = await cash_svc.get_all_strategies_summary()
    total_strategy_assets = sum(s.get("total_asset", 0) for s in strategies)

    return {
        "success": True,
        "account_cash": account_cash,
        "strategies": strategies,
        "total_strategy_assets": total_strategy_assets,
    }


@router.get("/api/v1/ui/{account_id}/capital/strategies/{strategy_id}")
async def get_strategy_capital(request: Request, account_id: str, strategy_id: int):
    """
    单策略资金详情

    Returns:
        {strategy_id, name, allocated_capital, strategy_cash, positions_mv, total_asset, available}
    """
    auth_account_id = request.state.account_id
    if account_id != auth_account_id:
        raise HTTPException(status_code=403, detail="账户ID不匹配")

    cash_svc = get_strategy_cash_service(account_id)

    summary = await cash_svc.get_strategy_total_asset(strategy_id)
    if not summary:
        raise HTTPException(status_code=404, detail="策略不存在")

    return {
        "success": True,
        "data": summary,
    }


@router.put("/api/v1/ui/{account_id}/capital/strategies/{strategy_id}/allocation")
async def adjust_strategy_allocation(
    request: Request,
    account_id: str,
    strategy_id: int,
    allocated_capital: float,
):
    """
    调整策略分配上限

    Note: allocated_capital 是参考值/统计值，不是买入硬约束
    """
    auth_account_id = request.state.account_id
    if account_id != auth_account_id:
        raise HTTPException(status_code=403, detail="账户ID不匹配")

    cash_svc = get_strategy_cash_service(account_id)

    success, reason = await cash_svc.adjust_allocation(strategy_id, allocated_capital)
    if not success:
        raise HTTPException(status_code=400, detail=reason)

    return {
        "success": True,
        "message": reason,
    }


@router.get("/api/v1/ui/{account_id}/capital/borrows")
async def get_borrow_records(
    request: Request,
    account_id: str,
    strategy_id: Optional[int] = None,
    status: Optional[str] = None,
):
    """
    获取借用记录列表

    Query params:
        strategy_id: 筛选特定策略
        status: borrowed / returned
    """
    auth_account_id = request.state.account_id
    if account_id != auth_account_id:
        raise HTTPException(status_code=403, detail="账户ID不匹配")

    cash_svc = get_strategy_cash_service(account_id)

    records = await cash_svc.get_borrow_records(strategy_id=strategy_id, status=status)

    return {
        "success": True,
        "records": records,
    }


@router.post("/api/v1/ui/{account_id}/capital/borrows/{borrow_id}/return")
async def return_borrowed_cash(
    request: Request,
    account_id: str,
    borrow_id: int,
    amount: float,
):
    """
    归还借用的现金

    Body:
        amount: 归还金额
    """
    auth_account_id = request.state.account_id
    if account_id != auth_account_id:
        raise HTTPException(status_code=403, detail="账户ID不匹配")

    cash_svc = get_strategy_cash_service(account_id)

    success, reason = await cash_svc.return_cash(borrow_id, amount)
    if not success:
        raise HTTPException(status_code=400, detail=reason)

    return {
        "success": True,
        "message": reason,
    }


@router.post("/api/v1/ui/{account_id}/capital/strategies/{strategy_id}/adjust-cash")
async def adjust_strategy_cash(
    request: Request,
    account_id: str,
    strategy_id: int,
    delta: float = Body(..., embed=True),
):
    """
    直接调整策略现金（手动增减）

    Body:
        delta: 增减金额（正数为增加，负数为扣减）
    """
    auth_account_id = request.state.account_id
    if account_id != auth_account_id:
        raise HTTPException(status_code=403, detail="账户ID不匹配")

    from services.common.database import get_db_manager
    from services.common.timezone import format_china_time
    from services.common.structured_logger import get_logger

    db = get_db_manager()
    logger = get_logger("capital_api")

    # 获取当前现金
    strategy = await db.fetchone(
        "SELECT strategy_cash FROM strategies WHERE id = ? AND account_id = ?",
        (strategy_id, account_id)
    )
    if not strategy:
        raise HTTPException(status_code=404, detail="策略不存在")

    current_cash = strategy.get("strategy_cash") or 0
    new_cash = current_cash + delta

    # 检查调整后现金不能为负
    if new_cash < 0:
        raise HTTPException(status_code=400, detail=f"调整后现金为负：当前 ¥{current_cash:.2f}，调整 -¥{abs(delta):.2f}")

    # 检查账户可用资金约束（策略现金总和不能超过账户可用资金）
    strategies = await db.fetchall(
        "SELECT id, strategy_cash FROM strategies WHERE account_id = ? AND is_active = 1",
        (account_id,)
    )
    total_strategy_cash = sum(s.get("strategy_cash") or 0 for s in strategies)
    total_strategy_cash += delta  # 加上本次调整

    account = await db.fetchone(
        "SELECT available_cash FROM accounts WHERE account_id = ?",
        (account_id,)
    )
    account_cash = account.get("available_cash") or 0 if account else 0

    if total_strategy_cash > account_cash:
        raise HTTPException(
            status_code=400,
            detail=f"策略现金总和超限：调整后 ¥{total_strategy_cash:.2f} > 账户可用 ¥{account_cash:.2f}"
        )

    # 更新策略现金
    await db.execute(
        "UPDATE strategies SET strategy_cash = ?, updated_at = ? WHERE id = ? AND account_id = ?",
        (new_cash, format_china_time(), strategy_id, account_id)
    )

    # 记录交易
    await db.insert("strategy_cash_transactions", {
        "account_id": account_id,
        "strategy_id": strategy_id,
        "transaction_type": "manual_adjust",
        "amount": delta,
        "stock_code": None,
        "trade_record_id": None,
        "balance_before": current_cash,
        "balance_after": new_cash,
        "reason": "手动调整现金",
        "created_at": format_china_time(),
    })

    logger.log_event("strategy_cash_adjust",
                     f"策略 #{strategy_id} 现金调整：¥{current_cash:.2f} → ¥{new_cash:.2f}",
                     strategy_id=strategy_id, delta=delta)

    return {
        "success": True,
        "strategy_id": strategy_id,
        "old_cash": current_cash,
        "new_cash": new_cash,
        "delta": delta,
    }


@router.post("/api/v1/ui/{account_id}/capital/strategies/{strategy_id}/recalc")
async def recalculate_strategy_cash(request: Request, account_id: str, strategy_id: int):
    """
    从交易记录重新计算策略现金余额（数据修复）
    """
    auth_account_id = request.state.account_id
    if account_id != auth_account_id:
        raise HTTPException(status_code=403, detail="账户ID不匹配")

    cash_svc = get_strategy_cash_service(account_id)

    calculated_cash = await cash_svc.recalculate_strategy_cash(strategy_id)

    return {
        "success": True,
        "strategy_id": strategy_id,
        "calculated_cash": calculated_cash,
    }