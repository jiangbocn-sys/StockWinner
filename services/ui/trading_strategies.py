"""
交易策略 API
每只股票的交易策略管理
"""

from fastapi import APIRouter, HTTPException, Path, Body
from typing import Optional, List
from datetime import datetime, timezone, timedelta
from services.common.database import get_db_manager

# 中国时区
CHINA_TZ = timezone(timedelta(hours=8))

def get_china_time():
    """获取中国时区时间"""
    return datetime.now(CHINA_TZ).replace(tzinfo=None)

router = APIRouter()


@router.get("/api/v1/ui/{account_id}/trading-strategies")
async def list_trading_strategies(account_id: str = Path(..., description="账户 ID")):
    """获取账户的所有交易策略"""
    db = get_db_manager()

    # 验证账户
    account = await db.fetchone(
        "SELECT 1 FROM accounts WHERE account_id = ? AND is_active = 1",
        (account_id,)
    )
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    strategies = await db.fetchall(
        """SELECT * FROM trading_strategies
           WHERE account_id = ?
           ORDER BY updated_at DESC""",
        (account_id,)
    )

    return {
        "success": True,
        "strategies": [
            {
                "id": s["id"],
                "account_id": s["account_id"],
                "stock_code": s["stock_code"],
                "stock_name": s["stock_code"],  # 暂时用代码代替名称
                "entry_price": float(s.get("entry_price") or 0),
                "stop_loss_price": float(s.get("stop_loss_price") or 0),
                "take_profit_price": float(s.get("take_profit_price") or 0),
                "stop_loss_pct": float(s.get("stop_loss_pct") or 0),
                "take_profit_pct": float(s.get("take_profit_pct") or 0),
                "max_trade_quantity": int(s.get("max_trade_quantity") or 0),
                "updated_at": s.get("updated_at", "")
            }
            for s in strategies
        ]
    }


@router.get("/api/v1/ui/{account_id}/trading-strategies/{stock_code}")
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
            "stock_name": strategy["stock_code"],  # 暂时用代码代替名称
            "entry_price": float(strategy.get("entry_price") or 0),
            "stop_loss_price": float(strategy.get("stop_loss_price") or 0),
            "take_profit_price": float(strategy.get("take_profit_price") or 0),
            "stop_loss_pct": float(strategy.get("stop_loss_pct") or 0),
            "take_profit_pct": float(strategy.get("take_profit_pct") or 0),
            "max_trade_quantity": int(strategy.get("max_trade_quantity") or 0),
            "updated_at": strategy.get("updated_at", "")
        }
    }


@router.post("/api/v1/ui/{account_id}/trading-strategies")
async def create_trading_strategy(
    account_id: str = Path(..., description="账户 ID"),
    stock_code: str = Body(..., description="股票代码"),
    entry_price: Optional[float] = Body(None, description="建仓价"),
    stop_loss_price: Optional[float] = Body(None, description="止损价"),
    take_profit_price: Optional[float] = Body(None, description="止盈价"),
    stop_loss_pct: Optional[float] = Body(None, description="止损比例"),
    take_profit_pct: Optional[float] = Body(None, description="止盈比例"),
    max_trade_quantity: Optional[int] = Body(None, description="单次买卖最大数量")
):
    """创建或更新交易策略（账户+股票唯一）"""
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
        params = [get_china_time().isoformat()]

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
                "entry_price": entry_price or 0,
                "stop_loss_price": stop_loss_price or 0,
                "take_profit_price": take_profit_price or 0,
                "stop_loss_pct": stop_loss_pct or 0,
                "take_profit_pct": take_profit_pct or 0,
                "max_trade_quantity": max_trade_quantity or 0,
                "updated_at": get_china_time().isoformat()
            }
        )
        message = "交易策略创建成功"

    return {
        "success": True,
        "message": message,
        "strategy_id": strategy_id
    }


@router.put("/api/v1/ui/{account_id}/trading-strategies/{stock_code}")
async def update_trading_strategy(
    account_id: str = Path(..., description="账户 ID"),
    stock_code: str = Path(..., description="股票代码"),
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
    params = [get_china_time().isoformat()]

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

    params.append(existing["id"])
    await db.execute(
        f"""UPDATE trading_strategies SET {", ".join(update_fields)} WHERE id = ?""",
        params
    )

    return {
        "success": True,
        "message": "交易策略更新成功"
    }


@router.delete("/api/v1/ui/{account_id}/trading-strategies/{stock_code}")
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
                    get_china_time().isoformat(),
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
                    "updated_at": get_china_time().isoformat()
                }
            )
        copied_count += 1

    return {
        "success": True,
        "message": f"已复制策略到 {copied_count} 只股票"
    }