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
        if sdk_mgr.connect():
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
            COALESCE(SUM(CASE WHEN trade_type = 'buy' THEN 1 ELSE 0 END), 0) as buy_count,
            COALESCE(SUM(CASE WHEN trade_type = 'sell' THEN 1 ELSE 0 END), 0) as sell_count,
            COALESCE(SUM(amount), 0) as total_amount
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
            COALESCE(SUM(CASE WHEN last_status = 'success' THEN 1 ELSE 0 END), 0) as success_count,
            COALESCE(SUM(CASE WHEN last_status = 'error' OR last_status = 'failed' THEN 1 ELSE 0 END), 0) as fail_count
        FROM strategy_tasks
        WHERE account_id = ? AND DATE(last_run_at) = ?
    """, (account_id, today))

    # 资源使用情况
    resources = get_resource_usage()

    # 数据库状态
    import sqlite3
    from pathlib import Path
    kline_db_path = Path(__file__).parent.parent.parent / "data" / "kline.db"
    db_stats = {"kline_latest_date": None, "kline_latest_count": 0, "kline_total_count": 0,
                "factor_latest_date": None, "factor_latest_count": 0, "factor_total_count": 0,
                "weekly_latest_date": None, "weekly_total_count": 0,
                "base_info_count": 0}
    try:
        if kline_db_path.exists():
            kconn = sqlite3.connect(str(kline_db_path))
            kcursor = kconn.cursor()

            # 日K线
            kcursor.execute("SELECT MAX(trade_date) FROM kline_data WHERE stock_code NOT LIKE '801%.SI'")
            r = kcursor.fetchone()
            db_stats["kline_latest_date"] = r[0] if r else None
            if db_stats["kline_latest_date"]:
                kcursor.execute("SELECT COUNT(*) FROM kline_data WHERE trade_date = ? AND stock_code NOT LIKE '801%.SI'", (db_stats["kline_latest_date"],))
                db_stats["kline_latest_count"] = kcursor.fetchone()[0]
            kcursor.execute("SELECT COUNT(*) FROM kline_data WHERE stock_code NOT LIKE '801%.SI'")
            db_stats["kline_total_count"] = kcursor.fetchone()[0]

            # 日频因子
            kcursor.execute("SELECT MAX(trade_date) FROM stock_daily_factors")
            r = kcursor.fetchone()
            db_stats["factor_latest_date"] = r[0] if r else None
            if db_stats["factor_latest_date"]:
                kcursor.execute("SELECT COUNT(*) FROM stock_daily_factors WHERE trade_date = ?", (db_stats["factor_latest_date"],))
                db_stats["factor_latest_count"] = kcursor.fetchone()[0]
            kcursor.execute("SELECT COUNT(*) FROM stock_daily_factors")
            db_stats["factor_total_count"] = kcursor.fetchone()[0]

            # 周K线
            kcursor.execute("SELECT MAX(week_end_date) FROM weekly_kline_data")
            r = kcursor.fetchone()
            db_stats["weekly_latest_date"] = r[0] if r else None
            if db_stats["weekly_latest_date"]:
                kcursor.execute("SELECT COUNT(*) FROM weekly_kline_data WHERE week_end_date = ?", (db_stats["weekly_latest_date"],))
                db_stats["weekly_latest_count"] = kcursor.fetchone()[0]
            kcursor.execute("SELECT COUNT(*) FROM weekly_kline_data")
            db_stats["weekly_total_count"] = kcursor.fetchone()[0]

            # stock_base_info - 按市场分类统计
            kcursor.execute("SELECT COUNT(*) FROM stock_base_info")
            db_stats["base_info_count"] = kcursor.fetchone()[0]
            kcursor.execute("SELECT COUNT(*) FROM stock_base_info WHERE stock_code LIKE '%.SH'")
            db_stats["base_info_sh"] = kcursor.fetchone()[0]
            kcursor.execute("SELECT COUNT(*) FROM stock_base_info WHERE stock_code LIKE '%.SZ'")
            db_stats["base_info_sz"] = kcursor.fetchone()[0]
            kcursor.execute("SELECT COUNT(*) FROM stock_base_info WHERE stock_code LIKE '%.BJ'")
            db_stats["base_info_bj"] = kcursor.fetchone()[0]
            kcursor.execute("SELECT COUNT(*) FROM stock_base_info WHERE stock_code LIKE '8%' OR stock_code LIKE '4%'")
            db_stats["base_info_neeq"] = kcursor.fetchone()[0]

            kconn.close()
    except Exception:
        pass

    return {
        "account_id": account_id,
        "account_name": account_name,
        "timestamp": get_china_time().isoformat(),
        "system_health": {
            "status": "healthy",
            "version": VERSION,
            "uptime_text": get_uptime_text(),
            "server_start": get_start_time().isoformat() if get_start_time() else None,
            "galaxy_api": check_sdk_connection(),
            "cpu_percent": resources["cpu_percent"],
            "memory_mb": resources["memory_mb"],
            "disk_percent": resources["disk_percent"],
        },
        "today_trading": {
            "trade_count": trade_stats["total_count"],
            "buy_count": trade_stats["buy_count"],
            "sell_count": trade_stats["sell_count"],
            "total_amount": float(trade_stats["total_amount"]),
        },
        "positions_summary": {
            "available_cash": available_cash,
            "position_count": positions.get("position_count", 0) if positions else 0,
            "total_market_value": float(positions.get("total_market_value", 0)) if positions and positions.get("total_market_value") else 0,
            "total_pnl": float(positions.get("total_pnl", 0)) if positions and positions.get("total_pnl") else 0,
            "daily_pnl": float(daily_pnl_row.get("daily_pnl", 0)) if daily_pnl_row and daily_pnl_row.get("daily_pnl") else 0,
        },
        "today_tasks": {
            "task_count": today_tasks["total_count"],
            "success_count": today_tasks["success_count"],
            "fail_count": today_tasks["fail_count"],
        },
        "db_stats": db_stats,
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
