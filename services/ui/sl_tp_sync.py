"""
止盈止损手动同步 API — 用户手动触发单只/批量同步
"""
from fastapi import APIRouter, Path, Body
from typing import Optional, List

from services.auth.account_validator import validate_account_active
from services.monitoring.sl_tp_sync import (
    calculate_effective_sl_tp,
    sync_to_watchlist,
    batch_sync_positions,
)

router = APIRouter()


@router.post("/api/v1/ui/{account_id}/sl-tp-sync/sync/{stock_code}")
async def manual_sync_single(
    account_id: str = Path(..., description="账户 ID"),
    stock_code: str = Path(..., description="股票代码"),
):
    """
    手动触发单只股票的止盈止损同步

    用途：用户在 watchlist 页面点击"同步止盈止损"按钮
    """
    await validate_account_active(account_id)

    result = await calculate_effective_sl_tp(account_id, stock_code)

    if not result["config_exists"]:
        return {
            "success": False,
            "message": "该股票未设置 trading_strategies 风控配置",
            "stock_code": stock_code,
        }

    updated = await sync_to_watchlist(
        account_id, stock_code,
        result["stop_loss_price"],
        result["take_profit_price"],
        log_reason="manual"
    )

    return {
        "success": True,
        "message": "同步成功" if updated else "无活跃 watchlist 记录",
        "stock_code": stock_code,
        "stop_loss_price": result["stop_loss_price"],
        "take_profit_price": result["take_profit_price"],
        "source_sl": result["source_sl"],
        "source_tp": result["source_tp"],
        "avg_cost": result["avg_cost_used"],
        "highest_price": result["highest_price_used"],
    }


@router.post("/api/v1/ui/{account_id}/sl-tp-sync/batch")
async def manual_sync_batch(
    account_id: str = Path(..., description="账户 ID"),
    stock_codes: List[str] = Body(..., description="股票代码列表"),
):
    """
    手动批量同步多只股票的止盈止损

    用途：用户选中多只股票后点击"批量同步"
    """
    await validate_account_active(account_id)

    if not stock_codes:
        return {"success": False, "message": "股票列表为空"}

    result = await batch_sync_positions(account_id, stock_codes)

    return {
        "success": True,
        "synced": result["synced"],
        "skipped": result["skipped"],
        "errors": result["errors"],
    }


@router.post("/api/v1/ui/{account_id}/sl-tp-sync/sync-all")
async def sync_all_positions(
    account_id: str = Path(..., description="账户 ID"),
):
    """
    同步账户所有持仓的止盈止损

    用途：用户点击"同步所有持仓"或在启动时自动调用
    """
    await validate_account_active(account_id)

    result = await batch_sync_positions(account_id)

    return {
        "success": True,
        "message": f"已同步 {result['synced']} 只，跳过 {result['skipped']} 只",
        "synced": result["synced"],
        "skipped": result["skipped"],
        "errors": result["errors"],
    }