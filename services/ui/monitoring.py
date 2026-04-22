"""
交易监控 API
"""

from fastapi import APIRouter, HTTPException, Path, Body, Query
from typing import List, Optional
from datetime import datetime, timezone, timedelta
from services.common.database import get_db_manager
from services.monitoring.service import get_trading_monitor

# 中国时区
CHINA_TZ = timezone(timedelta(hours=8))

def get_china_time():
    """获取中国时区时间"""
    return datetime.now(CHINA_TZ).replace(tzinfo=None)

router = APIRouter()


# ============== 交易监控服务控制 ==============

@router.post("/api/v1/ui/{account_id}/monitoring/start")
async def start_monitoring(
    account_id: str = Path(..., description="账户 ID"),
    interval: int = Body(30, embed=True, description="监控间隔（秒）", ge=10, le=300)
):
    """启动交易监控服务"""
    db = get_db_manager()

    # 从数据库验证账户
    account = await db.fetchone("SELECT * FROM accounts WHERE account_id = ? AND is_active = 1", (account_id,))
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    monitor = get_trading_monitor()
    result = await monitor.start_monitoring(account_id, interval)

    return result


@router.post("/api/v1/ui/{account_id}/monitoring/stop")
async def stop_monitoring(account_id: str = Path(..., description="账户 ID")):
    """停止交易监控服务"""
    db = get_db_manager()

    # 从数据库验证账户
    account = await db.fetchone("SELECT * FROM accounts WHERE account_id = ? AND is_active = 1", (account_id,))
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    monitor = get_trading_monitor()
    result = await monitor.stop_monitoring()

    return result


@router.get("/api/v1/ui/{account_id}/monitoring/status")
async def get_monitoring_status(account_id: str = Path(..., description="账户 ID")):
    """获取交易监控服务状态"""
    db = get_db_manager()

    # 从数据库验证账户
    account = await db.fetchone("SELECT * FROM accounts WHERE account_id = ? AND is_active = 1", (account_id,))
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    monitor = get_trading_monitor()
    status = monitor.get_status()

    return {
        "account_id": account_id,
        "monitoring": status
    }


# ============== 交易信号管理 ==============

@router.get("/api/v1/ui/{account_id}/signals")
async def get_signals(
    account_id: str = Path(..., description="账户 ID"),
    status: Optional[str] = Query(None, description="状态筛选：pending/executed/cancelled"),
    signal_type: Optional[str] = Query(None, description="信号类型：buy/sell_stop_loss/sell_take_profit")
):
    """获取交易信号列表"""
    db = get_db_manager()

    # 从数据库验证账户
    account = await db.fetchone("SELECT * FROM accounts WHERE account_id = ? AND is_active = 1", (account_id,))
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    conditions = ["account_id = ?"]
    params = [account_id]

    if status:
        conditions.append("status = ?")
        params.append(status)
    if signal_type:
        conditions.append("signal_type = ?")
        params.append(signal_type)

    where = " AND ".join(conditions)
    signals = await db.fetchall(
        f"SELECT * FROM trading_signals WHERE {where} ORDER BY created_at DESC LIMIT 100",
        params
    )

    return {
        "account_id": account_id,
        "signals": signals,
        "count": len(signals)
    }


@router.get("/api/v1/ui/{account_id}/signals/{signal_id}")
async def get_signal(
    account_id: str = Path(..., description="账户 ID"),
    signal_id: int = Path(..., description="信号 ID")
):
    """获取单个交易信号详情"""
    db = get_db_manager()

    # 从数据库验证账户
    account = await db.fetchone("SELECT * FROM accounts WHERE account_id = ? AND is_active = 1", (account_id,))
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    signal = await db.fetchone(
        "SELECT * FROM trading_signals WHERE id = ? AND account_id = ?",
        (signal_id, account_id)
    )

    if not signal:
        raise HTTPException(status_code=404, detail="信号不存在")

    return {"signal": signal}


@router.post("/api/v1/ui/{account_id}/signals/{signal_id}/execute")
async def execute_signal(
    account_id: str = Path(..., description="账户 ID"),
    signal_id: int = Path(..., description="信号 ID")
):
    """执行交易信号"""
    db = get_db_manager()

    # 从数据库验证账户
    account = await db.fetchone("SELECT * FROM accounts WHERE account_id = ? AND is_active = 1", (account_id,))
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    # 检查信号是否存在
    signal = await db.fetchone(
        "SELECT * FROM trading_signals WHERE id = ? AND account_id = ?",
        (signal_id, account_id)
    )

    if not signal:
        raise HTTPException(status_code=404, detail="信号不存在")

    # 使用交易执行服务
    from services.trading.execution_service import get_trade_execution_service

    execution = get_trade_execution_service(account_id)
    stock_code = signal.get('stock_code')
    signal_type = signal.get('signal_type')

    if signal_type == 'buy':
        result = await execution.execute_buy(
            stock_code=stock_code,
            stock_name=signal.get('stock_name', ''),
            price=signal.get('price', 0),
            target_quantity=signal.get('quantity', 100)
        )
    elif signal_type in ('sell_stop_loss', 'sell_take_profit'):
        result = await execution.execute_sell(
            stock_code=stock_code,
            stock_name=signal.get('stock_name', ''),
            price=signal.get('price', 0),
            target_quantity=signal.get('quantity', 100)
        )
    else:
        return {"success": False, "message": "未知的信号类型"}

    if result["success"]:
        # 更新信号状态
        await db.update(
            "trading_signals",
            {"status": "executed", "executed_at": get_china_time(), "result": str(result)},
            "id = ?",
            (signal_id,)
        )

        # 更新 watchlist 状态
        if signal_type == 'buy':
            await db.update(
                "watchlist",
                {"status": "watching", "updated_at": get_china_time()},
                "account_id = ? AND stock_code = ?",
                (account_id, stock_code)
            )
        else:
            await db.update(
                "watchlist",
                {"status": "sold", "updated_at": get_china_time()},
                "account_id = ? AND stock_code = ?",
                (account_id, stock_code)
            )

    return {
        "success": result["success"],
        "message": result.get("message", ""),
        "details": result
    }


@router.post("/api/v1/ui/{account_id}/signals/{signal_id}/cancel")
async def cancel_signal(
    account_id: str = Path(..., description="账户 ID"),
    signal_id: int = Path(..., description="信号 ID")
):
    """取消交易信号"""
    db = get_db_manager()

    # 从数据库验证账户
    account = await db.fetchone("SELECT * FROM accounts WHERE account_id = ? AND is_active = 1", (account_id,))
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    signal = await db.fetchone(
        "SELECT * FROM trading_signals WHERE id = ? AND account_id = ?",
        (signal_id, account_id)
    )

    if not signal:
        raise HTTPException(status_code=404, detail="信号不存在")

    await db.update(
        "trading_signals",
        {"status": "cancelled"},
        "id = ?",
        (signal_id,)
    )

    return {
        "success": True,
        "message": "交易信号已取消"
    }


@router.post("/api/v1/ui/{account_id}/signals/clear")
async def clear_signals(
    account_id: str = Path(..., description="账户 ID"),
    status: Optional[str] = Body(None, description="可选，只清除指定状态的信号")
):
    """清空交易信号"""
    db = get_db_manager()

    # 从数据库验证账户
    account = await db.fetchone("SELECT * FROM accounts WHERE account_id = ? AND is_active = 1", (account_id,))
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    if status:
        await db.delete("trading_signals", "account_id = ? AND status = ?", (account_id, status))
    else:
        await db.delete("trading_signals", "account_id = ?", (account_id,))

    return {
        "success": True,
        "message": "交易信号已清空"
    }
