"""
仪表盘 API
"""

from fastapi import APIRouter, HTTPException, Path
from datetime import datetime, timezone, timedelta
from services.common.account_manager import get_account_manager
from services.common.database import get_db_manager
from services.screening.service import get_screening_service
from services.monitoring.service import get_trading_monitor

# 中国时区
CHINA_TZ = timezone(timedelta(hours=8))

def get_china_time():
    """获取中国时区时间"""
    return datetime.now(CHINA_TZ).replace(tzinfo=None)

router = APIRouter()


@router.get("/api/v1/ui/{account_id}/dashboard")
async def get_dashboard(account_id: str = Path(..., description="账户 ID")):
    """仪表盘总览数据"""
    account_manager = get_account_manager()

    # 验证账户
    if not await account_manager.validate_account(account_id):
        raise HTTPException(status_code=404, detail=f"账户不存在：{account_id}")

    db = get_db_manager()

    # 获取今日交易统计
    today = get_china_time().strftime("%Y-%m-%d")
    trade_stats = await db.fetchone("""
        SELECT
            COUNT(*) as total_count,
            SUM(CASE WHEN trade_type = 'buy' THEN 1 ELSE 0 END) as buy_count,
            SUM(CASE WHEN trade_type = 'sell' THEN 1 ELSE 0 END) as sell_count,
            SUM(amount) as total_amount
        FROM trade_records
        WHERE account_id = ? AND DATE(trade_time) = ?
    """, (account_id, today))

    # 获取持仓概览
    positions_summary = await db.fetchone("""
        SELECT
            COUNT(*) as position_count,
            SUM(market_value) as total_market_value,
            SUM(profit_loss) as total_pnl
        FROM stock_positions
        WHERE account_id = ? AND quantity > 0
    """, (account_id,))

    # 获取账户显示名称
    account_name = await account_manager.get_account_display_name(account_id)

    return {
        "account_id": account_id,
        "account_name": account_name,
        "timestamp": get_china_time().isoformat(),
        "system_health": {
            "status": "healthy",
            "uptime_hours": get_uptime_hours(),
            "services": get_service_status()
        },
        "today_trading": {
            "trade_count": trade_stats.get("total_count", 0) if trade_stats else 0,
            "buy_count": trade_stats.get("buy_count", 0) if trade_stats else 0,
            "sell_count": trade_stats.get("sell_count", 0) if trade_stats else 0,
            "total_amount": float(trade_stats.get("total_amount", 0)) if trade_stats and trade_stats.get("total_amount") else 0,
            "total_pnl": float(trade_stats.get("total_pnl", 0)) if trade_stats and trade_stats.get("total_pnl") else 0
        },
        "positions_summary": {
            "position_count": positions_summary.get("position_count", 0) if positions_summary else 0,
            "total_market_value": float(positions_summary.get("total_market_value", 0)) if positions_summary and positions_summary.get("total_market_value") else 0,
            "total_pnl": float(positions_summary.get("total_pnl", 0)) if positions_summary and positions_summary.get("total_pnl") else 0
        },
        "resources": get_resource_usage()
    }


@router.get("/api/v1/ui/{account_id}/health")
async def health_check(account_id: str = Path(..., description="账户 ID")):
    """健康检查"""
    account_manager = get_account_manager()

    if not await account_manager.validate_account(account_id):
        raise HTTPException(status_code=404, detail=f"账户不存在：{account_id}")

    account_name = await account_manager.get_account_display_name(account_id)

    return {
        "status": "healthy",
        "account_id": account_id,
        "account_name": account_name,
        "timestamp": get_china_time().isoformat(),
        "version": "6.2.5"
    }


def get_uptime_hours() -> float:
    """获取运行时长（小时）"""
    # TODO: 记录启动时间并计算
    return 0.0


def get_service_status() -> dict:
    """获取服务状态"""
    screening_service = get_screening_service()
    trading_monitor = get_trading_monitor()

    screening_status = screening_service.get_status()
    monitoring_status = trading_monitor.get_status()

    return {
        "galaxy_api": "disconnected",  # TODO: 接入银河 SDK
        "screening": "running" if screening_status.get("running") else "stopped",
        "monitoring": "running" if monitoring_status.get("running") else "stopped",
        "notification": "ok"
    }


def get_resource_usage() -> dict:
    """获取资源使用情况"""
    import os
    import psutil

    try:
        process = psutil.Process(os.getpid())
        return {
            "cpu_percent": psutil.cpu_percent(interval=0.1),
            "memory_mb": round(process.memory_info().rss / 1024 / 1024, 1),
            "disk_percent": psutil.disk_usage('/').percent
        }
    except Exception:
        return {
            "cpu_percent": 0,
            "memory_mb": 0,
            "disk_percent": 0
        }
