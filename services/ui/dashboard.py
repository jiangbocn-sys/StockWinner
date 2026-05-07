"""
仪表盘 API
"""

from fastapi import APIRouter, HTTPException, Path
from services.common.account_manager import get_account_manager
from services.common.database import get_db_manager
from services.common.timezone import get_china_time
from services._version import VERSION, get_start_time

router = APIRouter()


def get_uptime_text() -> str:
    """获取运行时长文本"""
    start = get_start_time()
    if start:
        delta = get_china_time() - start
        total_seconds = int(delta.total_seconds())
        if total_seconds < 0:
            total_seconds = 0
        days = total_seconds // 86400
        hours = (total_seconds % 86400) // 3600
        minutes = (total_seconds % 3600) // 60
        seconds = total_seconds % 60
        return f"{days}天{hours}小时{minutes}分{seconds}秒"
    return "0天0小时0分0秒"


def check_sdk_connection() -> str:
    """检测 Galaxy SDK 连接状态"""
    try:
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        if sdk_mgr._ensure_login():
            return "connected"
        else:
            return "login_failed"
    except ImportError:
        return "disconnected"
    except Exception:
        return "login_failed"


@router.get("/api/v1/ui/{account_id}/dashboard")
async def get_dashboard(account_id: str = Path(..., description="账户 ID")):
    """仪表盘总览数据"""
    account_manager = get_account_manager()
    if not await account_manager.validate_account(account_id):
        raise HTTPException(status_code=404, detail=f"账户不存在：{account_id}")

    db = get_db_manager()

    # 获取账户资金
    account = await db.fetchone(
        "SELECT available_cash, display_name FROM accounts WHERE account_id = ?",
        (account_id,)
    )
    available_cash = float(account["available_cash"] if account and account.get("available_cash") else 0)
    account_name = account["display_name"] if account else account_id

    # 持仓概览
    positions = await db.fetchone("""
        SELECT
            COUNT(*) as position_count,
            SUM(market_value) as total_market_value,
            SUM(profit_loss) as total_pnl
        FROM stock_positions
        WHERE account_id = ? AND quantity > 0
    """, (account_id,))

    # 今日交易统计
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

    # 今日盈亏 = 今日卖出的盈亏之和
    daily_pnl_row = await db.fetchone("""
        SELECT SUM(profit_loss) as daily_pnl
        FROM trade_records
        WHERE account_id = ? AND DATE(trade_time) = ? AND trade_type = 'sell'
    """, (account_id, today))

    # 今日任务执行统计
    today_tasks = await db.fetchone("""
        SELECT
            COUNT(*) as total_count,
            SUM(CASE WHEN last_status = 'success' THEN 1 ELSE 0 END) as success_count,
            SUM(CASE WHEN last_status = 'error' OR last_status = 'failed' THEN 1 ELSE 0 END) as fail_count
        FROM strategy_tasks
        WHERE account_id = ? AND DATE(last_run_at) = ?
    """, (account_id, today))

    # 资源使用情况
    resources = get_resource_usage()

    return {
        "account_id": account_id,
        "account_name": account_name,
        "timestamp": get_china_time().isoformat(),
        "system_health": {
            "status": "healthy",
            "version": VERSION,
            "uptime_text": get_uptime_text(),
            "galaxy_api": check_sdk_connection(),
            "cpu_percent": resources["cpu_percent"],
            "memory_mb": resources["memory_mb"],
            "disk_percent": resources["disk_percent"],
        },
        "today_trading": {
            "trade_count": trade_stats.get("total_count", 0) if trade_stats else 0,
            "buy_count": trade_stats.get("buy_count", 0) if trade_stats else 0,
            "sell_count": trade_stats.get("sell_count", 0) if trade_stats else 0,
            "total_amount": float(trade_stats.get("total_amount", 0)) if trade_stats and trade_stats.get("total_amount") else 0,
        },
        "positions_summary": {
            "available_cash": available_cash,
            "position_count": positions.get("position_count", 0) if positions else 0,
            "total_market_value": float(positions.get("total_market_value", 0)) if positions and positions.get("total_market_value") else 0,
            "total_pnl": float(positions.get("total_pnl", 0)) if positions and positions.get("total_pnl") else 0,
            "daily_pnl": float(daily_pnl_row.get("daily_pnl", 0)) if daily_pnl_row and daily_pnl_row.get("daily_pnl") else 0,
        },
        "today_tasks": {
            "task_count": today_tasks.get("total_count", 0) if today_tasks else 0,
            "success_count": today_tasks.get("success_count", 0) if today_tasks else 0,
            "fail_count": today_tasks.get("fail_count", 0) if today_tasks else 0,
        },
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
        "version": VERSION
    }


def get_resource_usage() -> dict:
    """获取资源使用情况"""
    import os, psutil
    try:
        process = psutil.Process(os.getpid())
        return {
            "cpu_percent": psutil.cpu_percent(interval=0.1),
            "memory_mb": round(process.memory_info().rss / 1024 / 1024, 1),
            "disk_percent": psutil.disk_usage('/').percent
        }
    except Exception:
        return {"cpu_percent": 0, "memory_mb": 0, "disk_percent": 0}
