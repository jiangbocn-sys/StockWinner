"""
策略效能评估 API

提供策略表现的统计、明细和图表数据
"""

from fastapi import APIRouter, HTTPException, Path, Query
from typing import Optional
from services.common.database import get_db_manager
from services.common.account_manager import get_account_manager

router = APIRouter()


@router.get("/api/v1/ui/{account_id}/performance/summary")
async def get_performance_summary(
    account_id: str = Path(..., description="账户 ID"),
    strategy_id: Optional[int] = Query(None, description="策略 ID，不传则返回所有策略汇总"),
):
    """策略效能汇总：胜率、总盈亏、执行率、交易次数"""
    account_manager = get_account_manager()
    if not await account_manager.validate_account(account_id):
        raise HTTPException(status_code=404, detail=f"账户不存在：{account_id}")

    db = get_db_manager()

    # 策略列表（用于下拉选择）
    strategies = await db.fetchall(
        "SELECT id, name, strategy_type, code_scope, status FROM strategies WHERE account_id = ? ORDER BY id",
        (account_id,)
    )

    # 单策略汇总 or 全部汇总
    if strategy_id:
        strategy = await db.fetchone(
            "SELECT id, name, strategy_type, code_scope FROM strategies WHERE id = ? AND account_id = ?",
            (strategy_id, account_id)
        )
        if not strategy:
            raise HTTPException(status_code=404, detail="策略不存在")

        # 选股记录统计
        selections = await db.fetchall(
            "SELECT * FROM watchlist WHERE strategy_id = ? AND account_id = ?",
            (strategy_id, account_id)
        )
        total_selections = len(selections)
        bought_count = sum(1 for s in selections if s.get("bought"))

        # 交易记录统计
        sell_trades = await db.fetchall(
            "SELECT * FROM trade_records WHERE strategy_id = ? AND account_id = ? AND trade_type = 'sell'",
            (strategy_id, account_id)
        )

        win_count = sum(1 for t in sell_trades if (t.get("profit_loss") or 0) > 0)
        lose_count = sum(1 for t in sell_trades if (t.get("profit_loss") or 0) <= 0)
        total_pnl = sum(t.get("profit_loss") or 0 for t in sell_trades)

        # 平均持仓天数（一次 SQL 查出所有买卖配对）
        buy_sell_pairs = await db.fetchall("""
            SELECT s.stock_code, s.trade_time as sell_time,
                   b.trade_time as buy_time
            FROM trade_records s
            LEFT JOIN trade_records b ON b.account_id = s.account_id
                AND b.stock_code = s.stock_code
                AND b.trade_type = 'buy'
                AND b.trade_time < s.trade_time
                AND b.strategy_id = s.strategy_id
            WHERE s.strategy_id = ? AND s.account_id = ? AND s.trade_type = 'sell'
            ORDER BY s.trade_time DESC
        """, (strategy_id, account_id))
        holding_days = []
        for pair in buy_sell_pairs:
            try:
                from datetime import datetime
                bt = datetime.fromisoformat(pair["buy_time"])
                st = datetime.fromisoformat(pair["sell_time"])
                holding_days.append((st - bt).days)
            except Exception:
                pass
        avg_holding_days = sum(holding_days) / len(holding_days) if holding_days else 0

        # 盈亏比
        total_profit = sum(t.get("profit_loss") or 0 for t in sell_trades if (t.get("profit_loss") or 0) > 0)
        total_loss = abs(sum(t.get("profit_loss") or 0 for t in sell_trades if (t.get("profit_loss") or 0) < 0))
        profit_factor = total_profit / total_loss if total_loss > 0 else float('inf') if total_profit > 0 else 0

        win_rate = win_count / (win_count + lose_count) if (win_count + lose_count) > 0 else 0
        execution_rate = bought_count / total_selections if total_selections > 0 else 0

        return {
            "success": True,
            "strategy": strategy,
            "stats": {
                "total_selections": total_selections,
                "bought_count": bought_count,
                "execution_rate": round(execution_rate * 100, 1),
                "total_trades": len(sell_trades),
                "win_count": win_count,
                "lose_count": lose_count,
                "win_rate": round(win_rate * 100, 1),
                "total_pnl": round(total_pnl, 2),
                "avg_holding_days": round(avg_holding_days, 1),
                "profit_factor": round(profit_factor, 2) if profit_factor != float('inf') else "∞",
                "total_profit": round(total_profit, 2),
                "total_loss": round(total_loss, 2),
            },
            "strategies": strategies,
        }

    # 全部策略汇总（排行榜）
    all_stats = []
    for s in strategies:
        sid = s["id"]
        sell_trades = await db.fetchall(
            "SELECT profit_loss FROM trade_records WHERE strategy_id = ? AND account_id = ? AND trade_type = 'sell'",
            (sid, account_id)
        )
        selections = await db.fetchall(
            "SELECT bought FROM watchlist WHERE strategy_id = ? AND account_id = ?",
            (sid, account_id)
        )
        win_count = sum(1 for t in sell_trades if (t.get("profit_loss") or 0) > 0)
        total_sell = len(sell_trades)
        total_pnl = sum(t.get("profit_loss") or 0 for t in sell_trades)
        bought_count = sum(1 for sel in selections if sel.get("bought"))
        total_sel = len(selections)

        all_stats.append({
            "strategy_id": sid,
            "name": s["name"],
            "strategy_type": s["strategy_type"],
            "code_scope": s.get("code_scope"),
            "total_selections": total_sel,
            "bought_count": bought_count,
            "total_trades": total_sell,
            "win_count": win_count,
            "win_rate": round(win_count / total_sell * 100, 1) if total_sell > 0 else 0,
            "total_pnl": round(total_pnl, 2),
            "execution_rate": round(bought_count / total_sel * 100, 1) if total_sel > 0 else 0,
        })

    return {"success": True, "strategies": strategies, "all_stats": all_stats}


@router.get("/api/v1/ui/{account_id}/performance/{strategy_id}/selections")
async def get_strategy_selections(
    account_id: str = Path(...),
    strategy_id: int = Path(...),
    limit: int = Query(200, description="返回数量"),
):
    """选股明细：选出日期、价格、是否买入"""
    account_manager = get_account_manager()
    if not await account_manager.validate_account(account_id):
        raise HTTPException(status_code=404, detail=f"账户不存在：{account_id}")

    db = get_db_manager()

    # 验证策略
    strategy = await db.fetchone(
        "SELECT id, name FROM strategies WHERE id = ? AND account_id = ?",
        (strategy_id, account_id)
    )
    if not strategy:
        raise HTTPException(status_code=404, detail="策略不存在")

    selections = await db.fetchall("""
        SELECT w.stock_code, w.stock_name, w.selected_at, w.buy_price, w.bought,
               t.price as buy_price_actual, t.trade_time as buy_time,
               s.price as sell_price, s.trade_time as sell_time, s.profit_loss
        FROM watchlist w
        LEFT JOIN trade_records t ON t.id = w.buy_trade_id
        LEFT JOIN trade_records s ON s.id = (
            SELECT t2.id FROM trade_records t2
            WHERE t2.stock_code = w.stock_code
              AND t2.trade_type = 'sell'
              AND t2.strategy_id = w.strategy_id
              AND t2.trade_time > COALESCE(t.trade_time, w.selected_at)
            ORDER BY t2.trade_time ASC
            LIMIT 1
        )
        WHERE w.strategy_id = ? AND w.account_id = ?
        ORDER BY w.selected_at DESC
        LIMIT ?
    """, (strategy_id, account_id, limit))

    return {"success": True, "strategy": strategy, "selections": selections}


@router.get("/api/v1/ui/{account_id}/performance/{strategy_id}/trades")
async def get_strategy_trades(
    account_id: str = Path(...),
    strategy_id: int = Path(...),
    limit: int = Query(200, description="返回数量"),
):
    """交易明细：买卖价格、盈亏、持仓天数"""
    account_manager = get_account_manager()
    if not await account_manager.validate_account(account_id):
        raise HTTPException(status_code=404, detail=f"账户不存在：{account_id}")

    db = get_db_manager()

    strategy = await db.fetchone(
        "SELECT id, name FROM strategies WHERE id = ? AND account_id = ?",
        (strategy_id, account_id)
    )
    if not strategy:
        raise HTTPException(status_code=404, detail="策略不存在")

    trades = await db.fetchall("""
        SELECT t.id, t.stock_code, t.stock_name, t.trade_type, t.quantity,
               t.price, t.amount, t.commission, t.profit_loss, t.trade_time,
               t.trigger_source
        FROM trade_records t
        WHERE t.strategy_id = ? AND t.account_id = ?
        ORDER BY t.trade_time DESC
        LIMIT ?
    """, (strategy_id, account_id, limit))

    return {"success": True, "strategy": strategy, "trades": trades}


@router.get("/api/v1/ui/{account_id}/performance/equity-curve")
async def get_equity_curve(
    account_id: str = Path(...),
    strategy_id: Optional[int] = Query(None, description="策略 ID，不传则返回全部"),
):
    """权益曲线：按日期累计盈亏"""
    account_manager = get_account_manager()
    if not await account_manager.validate_account(account_id):
        raise HTTPException(status_code=404, detail=f"账户不存在：{account_id}")

    db = get_db_manager()

    if strategy_id:
        sell_trades = await db.fetchall("""
            SELECT DATE(trade_time) as trade_date,
                   SUM(profit_loss) as daily_pnl
            FROM trade_records
            WHERE strategy_id = ? AND account_id = ? AND trade_type = 'sell' AND profit_loss IS NOT NULL
            GROUP BY DATE(trade_time)
            ORDER BY trade_date ASC
        """, (strategy_id, account_id))
    else:
        sell_trades = await db.fetchall("""
            SELECT DATE(trade_time) as trade_date,
                   SUM(profit_loss) as daily_pnl
            FROM trade_records
            WHERE account_id = ? AND trade_type = 'sell' AND profit_loss IS NOT NULL
            GROUP BY DATE(trade_time)
            ORDER BY trade_date ASC
        """, (account_id,))

    # 计算累计盈亏
    cumulative = 0
    curve = []
    for row in sell_trades:
        cumulative += row["daily_pnl"] or 0
        curve.append({
            "date": row["trade_date"],
            "daily_pnl": round(row["daily_pnl"] or 0, 2),
            "cumulative_pnl": round(cumulative, 2),
        })

    return {"success": True, "curve": curve}
