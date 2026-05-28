"""
个股交易策略 API（trading_strategies 表）- 止损止盈配置

条件触发策略的 CRUD API 在 trades.py 中定义。
"""

from fastapi import APIRouter, HTTPException, Path, Body
from typing import Optional, List
from datetime import datetime
from services.common.database import get_db_manager
from services.common.timezone import get_china_time, format_china_time

router = APIRouter()


# ============================================================
# 个股交易策略 API（trading_strategies 表）- 止损止盈配置
# ============================================================

@router.get("/api/v1/ui/{account_id}/trading-strategies/stock-list")
async def list_trading_strategies(
    account_id: str = Path(..., description="账户 ID"),
):
    """列出个股交易策略"""
    db = get_db_manager()
    account = await db.fetchone(
        "SELECT 1 FROM accounts WHERE account_id = ? AND is_active = 1",
        (account_id,)
    )
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    strategies = await db.fetchall(
        "SELECT * FROM trading_strategies WHERE account_id = ? ORDER BY updated_at DESC",
        (account_id,)
    )
    return {"success": True, "strategies": strategies}


@router.post("/api/v1/ui/{account_id}/trading-strategies/stock")
async def upsert_stock_trading_strategy(
    account_id: str = Path(..., description="账户 ID"),
    stock_code: str = Body(..., description="股票代码"),
    stock_name: str = Body("", description="股票名称"),
    strategy_type: str = Body("fixed", description="策略类型：fixed/trailing_stop"),
    config: Optional[str] = Body(None, description="策略配置 JSON"),
    entry_price: Optional[float] = Body(None, description="建仓价"),
    stop_loss_price: Optional[float] = Body(None, description="止损价"),
    take_profit_price: Optional[float] = Body(None, description="止盈价"),
    stop_loss_pct: Optional[float] = Body(None, description="止损比例"),
    take_profit_pct: Optional[float] = Body(None, description="止盈比例"),
    max_trade_quantity: Optional[int] = Body(None, description="单次买卖最大数量")
):
    """创建或更新个股交易策略（账户+股票唯一）"""
    db = get_db_manager()

    # 验证账户
    account = await db.fetchone(
        "SELECT 1 FROM accounts WHERE account_id = ? AND is_active = 1",
        (account_id,)
    )
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    # 验证参数范围
    if stop_loss_pct is not None and not (0.0 <= stop_loss_pct <= 1.0):
        raise HTTPException(status_code=400, detail="止损比例必须在0-1之间")
    if take_profit_pct is not None and not (0.0 <= take_profit_pct <= 1.0):
        raise HTTPException(status_code=400, detail="止盈比例必须在0-1之间")

    # 检查是否已存在
    existing = await db.fetchone(
        "SELECT id FROM trading_strategies WHERE account_id = ? AND stock_code = ?",
        (account_id, stock_code)
    )

    if existing:
        # 更新已有策略
        update_fields = ["updated_at = ?"]
        params = [format_china_time()]

        if entry_price is not None:
            update_fields.append("entry_price = ?")
            params.append(entry_price)
        if stop_loss_price is not None:
            update_fields.append("stop_loss_price = ?")
            params.append(stop_loss_price)
        if take_profit_price is not None:
            update_fields.append("take_profit_price = ?")
            params.append(take_profit_price)
        if stop_loss_pct is not None:
            update_fields.append("stop_loss_pct = ?")
            params.append(stop_loss_pct)
        if take_profit_pct is not None:
            update_fields.append("take_profit_pct = ?")
            params.append(take_profit_pct)
        if max_trade_quantity is not None:
            update_fields.append("max_trade_quantity = ?")
            params.append(max_trade_quantity)
        if stock_name is not None and stock_name != "":
            update_fields.append("stock_name = ?")
            params.append(stock_name)
        if strategy_type is not None:
            update_fields.append("strategy_type = ?")
            params.append(strategy_type)
        if config is not None:
            update_fields.append("config = ?")
            params.append(config)

        params.append(existing["id"])
        await db.execute(
            f"""UPDATE trading_strategies SET {", ".join(update_fields)} WHERE id = ?""",
            params
        )

        strategy_id = existing["id"]
        message = "交易策略更新成功"
    else:
        # 创建新策略
        strategy_id = await db.insert(
            "trading_strategies",
            {
                "account_id": account_id,
                "stock_code": stock_code,
                "stock_name": stock_name or stock_code,
                "strategy_type": strategy_type or "fixed",
                "config": config or "{}",
                "entry_price": entry_price or 0,
                "stop_loss_price": stop_loss_price or 0,
                "take_profit_price": take_profit_price or 0,
                "stop_loss_pct": stop_loss_pct or 0,
                "take_profit_pct": take_profit_pct or 0,
                "max_trade_quantity": max_trade_quantity or 0,
                "updated_at": format_china_time()
            }
        )
        message = "交易策略创建成功"

    # 同步到 watchlist：计算有效止损止盈价
    try:
        pos = await db.fetchone(
            "SELECT avg_cost FROM stock_positions WHERE account_id = ? AND stock_code = ? AND quantity > 0",
            (account_id, stock_code)
        )
        avg_cost = float(pos["avg_cost"]) if pos and pos.get("avg_cost", 0) > 0 else 0

        sl = stop_loss_price if (stop_loss_price and stop_loss_price > 0) else (
            round(avg_cost * (1 - (stop_loss_pct or 0)), 2) if avg_cost > 0 and (stop_loss_pct or 0) > 0 else 0
        )
        tp = take_profit_price if (take_profit_price and take_profit_price > 0) else (
            round(avg_cost * (1 + (take_profit_pct or 0)), 2) if avg_cost > 0 and (take_profit_pct or 0) > 0 else 0
        )

        existing_wl = await db.fetchone(
            "SELECT id FROM watchlist WHERE account_id = ? AND stock_code = ? AND status IN ('pending', 'watching', 'bought')",
            (account_id, stock_code)
        )
        if existing_wl and (sl > 0 or tp > 0):
            await db.execute(
                "UPDATE watchlist SET stop_loss_price = ?, take_profit_price = ?, updated_at = ? WHERE id = ?",
                (sl, tp, format_china_time(), existing_wl["id"])
            )
    except Exception:
        pass

    return {
        "success": True,
        "message": message,
        "strategy_id": strategy_id
    }


@router.post("/api/v1/ui/{account_id}/trading-strategies/copy")
async def copy_trading_strategy(
    account_id: str = Path(..., description="账户 ID"),
    source_stock_code: str = Body(..., description="源股票代码"),
    target_stock_codes: List[str] = Body(..., description="目标股票代码列表")
):
    """复制交易策略到其他股票"""
    db = get_db_manager()

    # 获取源策略
    source = await db.fetchone(
        "SELECT * FROM trading_strategies WHERE account_id = ? AND stock_code = ?",
        (account_id, source_stock_code)
    )

    if not source:
        raise HTTPException(status_code=404, detail="源交易策略不存在")

    # 复制到目标股票
    copied_count = 0
    for target_code in target_stock_codes:
        if target_code == source_stock_code:
            continue

        # 检查目标是否已存在
        existing = await db.fetchone(
            "SELECT id FROM trading_strategies WHERE account_id = ? AND stock_code = ?",
            (account_id, target_code)
        )

        if existing:
            # 更新
            await db.execute(
                """UPDATE trading_strategies
                   SET entry_price = ?, stop_loss_price = ?, take_profit_price = ?,
                       stop_loss_pct = ?, take_profit_pct = ?, max_trade_quantity = ?, updated_at = ?
                   WHERE account_id = ? AND stock_code = ?""",
                (
                    source["entry_price"],
                    source["stop_loss_price"],
                    source["take_profit_price"],
                    source["stop_loss_pct"],
                    source["take_profit_pct"],
                    source["max_trade_quantity"],
                    format_china_time(),
                    account_id,
                    target_code
                )
            )
        else:
            # 创建
            await db.insert(
                "trading_strategies",
                {
                    "account_id": account_id,
                    "stock_code": target_code,
                    "entry_price": source["entry_price"],
                    "stop_loss_price": source["stop_loss_price"],
                    "take_profit_price": source["take_profit_price"],
                    "stop_loss_pct": source["stop_loss_pct"],
                    "take_profit_pct": source["take_profit_pct"],
                    "max_trade_quantity": source["max_trade_quantity"],
                    "updated_at": format_china_time()
                }
            )
        copied_count += 1

    return {
        "success": True,
        "message": f"已复制策略到 {copied_count} 只股票"
    }


@router.get("/api/v1/ui/{account_id}/trading-strategies/stock/{stock_code}")
async def get_trading_strategy(
    account_id: str = Path(..., description="账户 ID"),
    stock_code: str = Path(..., description="股票代码")
):
    """获取指定股票的交易策略"""
    db = get_db_manager()

    strategy = await db.fetchone(
        """SELECT * FROM trading_strategies
           WHERE account_id = ? AND stock_code = ?""",
        (account_id, stock_code)
    )

    if not strategy:
        return {
            "success": True,
            "strategy": None,
            "message": "该股票尚未设置交易策略"
        }

    return {
        "success": True,
        "strategy": {
            "id": strategy["id"],
            "account_id": strategy["account_id"],
            "stock_code": strategy["stock_code"],
            "stock_name": strategy["stock_code"],
            "strategy_type": strategy.get("strategy_type", "fixed"),
            "config": strategy.get("config", "{}"),
            "entry_price": float(strategy.get("entry_price") or 0),
            "stop_loss_price": float(strategy.get("stop_loss_price") or 0),
            "take_profit_price": float(strategy.get("take_profit_price") or 0),
            "stop_loss_pct": float(strategy.get("stop_loss_pct") or 0),
            "take_profit_pct": float(strategy.get("take_profit_pct") or 0),
            "max_trade_quantity": int(strategy.get("max_trade_quantity") or 0),
            "updated_at": strategy.get("updated_at", "")
        }
    }


@router.put("/api/v1/ui/{account_id}/trading-strategies/stock/{stock_code}")
async def update_trading_strategy_per_stock(
    account_id: str = Path(..., description="账户 ID"),
    stock_code: str = Path(..., description="股票代码"),
    strategy_type: str = Body(None, description="策略类型：fixed/trailing_stop"),
    config: Optional[str] = Body(None, description="策略配置 JSON"),
    entry_price: Optional[float] = Body(None, description="建仓价"),
    stop_loss_price: Optional[float] = Body(None, description="止损价"),
    take_profit_price: Optional[float] = Body(None, description="止盈价"),
    stop_loss_pct: Optional[float] = Body(None, description="止损比例"),
    take_profit_pct: Optional[float] = Body(None, description="止盈比例"),
    max_trade_quantity: Optional[int] = Body(None, description="单次买卖最大数量")
):
    """更新交易策略"""
    db = get_db_manager()

    # 检查策略是否存在
    existing = await db.fetchone(
        "SELECT id FROM trading_strategies WHERE account_id = ? AND stock_code = ?",
        (account_id, stock_code)
    )

    if not existing:
        raise HTTPException(status_code=404, detail="交易策略不存在")

    # 验证参数范围
    if stop_loss_pct is not None and not (0.0 <= stop_loss_pct <= 1.0):
        raise HTTPException(status_code=400, detail="止损比例必须在0-1之间")
    if take_profit_pct is not None and not (0.0 <= take_profit_pct <= 1.0):
        raise HTTPException(status_code=400, detail="止盈比例必须在0-1之间")

    # 更新
    update_fields = ["updated_at = ?"]
    params = [format_china_time()]

    if entry_price is not None:
        update_fields.append("entry_price = ?")
        params.append(entry_price)
    if stop_loss_price is not None:
        update_fields.append("stop_loss_price = ?")
        params.append(stop_loss_price)
    if take_profit_price is not None:
        update_fields.append("take_profit_price = ?")
        params.append(take_profit_price)
    if stop_loss_pct is not None:
        update_fields.append("stop_loss_pct = ?")
        params.append(stop_loss_pct)
    if take_profit_pct is not None:
        update_fields.append("take_profit_pct = ?")
        params.append(take_profit_pct)
    if max_trade_quantity is not None:
        update_fields.append("max_trade_quantity = ?")
        params.append(max_trade_quantity)
    if strategy_type is not None:
        update_fields.append("strategy_type = ?")
        params.append(strategy_type)
    if config is not None:
        update_fields.append("config = ?")
        params.append(config)

    params.append(existing["id"])
    await db.execute(
        f"""UPDATE trading_strategies SET {", ".join(update_fields)} WHERE id = ?""",
        params
    )

    return {
        "success": True,
        "message": "交易策略更新成功"
    }


@router.delete("/api/v1/ui/{account_id}/trading-strategies/stock/{stock_code}")
async def delete_trading_strategy(
    account_id: str = Path(..., description="账户 ID"),
    stock_code: str = Path(..., description="股票代码")
):
    """删除交易策略"""
    db = get_db_manager()

    # 检查策略是否存在
    existing = await db.fetchone(
        "SELECT id FROM trading_strategies WHERE account_id = ? AND stock_code = ?",
        (account_id, stock_code)
    )

    if not existing:
        raise HTTPException(status_code=404, detail="交易策略不存在")

    await db.execute(
        "DELETE FROM trading_strategies WHERE account_id = ? AND stock_code = ?",
        (account_id, stock_code)
    )

    return {
        "success": True,
        "message": "交易策略已删除"
    }
