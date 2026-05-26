"""
仪表盘 API
"""

from pathlib import Path as FilePath
from fastapi import APIRouter, HTTPException, Path
from services.common.account_manager import get_account_manager
from services.common.database import get_db_manager, get_sync_connection
from services.common.timezone import get_china_time
from services._version import VERSION, get_start_time

router = APIRouter()

# 数据源状态缓存：避免阻塞仪表盘返回，由后台任务异步刷新
_data_sources_cache: list = []


def get_sdk_metrics() -> dict:
    """获取 SDK 调用统计（只读状态，不触发登录）"""
    try:
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        return sdk_mgr.get_sdk_metrics()
    except Exception:
        return {
            "recent_60s": {"calls": 0, "rows": 0, "success_rate": 0, "active_methods": []},
            "session": {"total_calls": 0, "success_calls": 0, "total_rows": 0},
        }


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


async def _get_data_sources_status() -> list:
    """返回数据源状态缓存（不阻塞，首次调用触发后台刷新）"""
    await _ensure_data_sources_init()
    return list(_data_sources_cache)


async def _refresh_data_sources_background():
    """并行检测所有数据源，完成一个立即更新缓存"""
    from services.data.channel.config_manager import get_all_provider_configs
    from services.data.channel.router import get_channel_router
    from services.common.timezone import get_china_time
    import asyncio

    try:
        configs = await get_all_provider_configs()
    except Exception:
        return

    try:
        router = get_channel_router()
        registered = router.get_providers()
    except Exception:
        registered = {}

    global _data_sources_cache

    async def check_one(cfg):
        pid = cfg["provider_id"]
        provider = registered.get(pid)
        now = get_china_time().isoformat()

        if provider:
            try:
                hc = await provider.health_check()
                status = "connected" if hc.get("ok") else "error"
                error_msg = None if hc.get("ok") else hc.get("message", "")
                latency_ms = hc.get("latency_ms", -1)
            except Exception as e:
                status = "error"
                error_msg = str(e)
                latency_ms = -1
        elif cfg.get("is_enabled"):
            status = "disconnected"
            error_msg = None
            latency_ms = -1
        else:
            status = "not_configured"
            error_msg = None
            latency_ms = -1

        return {
            "provider_id": pid,
            "display_name": cfg.get("display_name", pid),
            "is_enabled": bool(cfg.get("is_enabled", False)),
            "status": status,
            "error_message": error_msg,
            "latency_ms": latency_ms,
            "last_check_time": now,
        }

    # 并行启动所有检测，完成一个就更新缓存
    tasks = [check_one(cfg) for cfg in configs]
    for coro in asyncio.as_completed(tasks):
        result = await coro
        # 即时更新缓存中对应 provider 的状态
        for i, item in enumerate(_data_sources_cache):
            if item.get("provider_id") == result["provider_id"]:
                _data_sources_cache[i] = result
                break
        else:
            _data_sources_cache.append(result)


_data_sources_refresh_started = False


async def start_data_sources_refresh(interval: int = 60):
    """启动定期刷新数据源状态的后台任务（每60秒）"""
    global _data_sources_refresh_started
    if _data_sources_refresh_started:
        return
    _data_sources_refresh_started = True

    import asyncio
    async def _loop():
        while True:
            await asyncio.sleep(interval)
            try:
                await _refresh_data_sources_background()
            except Exception:
                pass

    asyncio.create_task(_loop())


async def _ensure_data_sources_init():
    """首次调用返回占位符并触发后台刷新；后续调用触发异步刷新"""
    global _data_sources_cache
    if not _data_sources_cache:
        # 首次：构建占位符（checking 状态）
        from services.data.channel.config_manager import get_all_provider_configs
        from services.data.channel.router import get_channel_router

        try:
            configs = await get_all_provider_configs()
            registered = {}
            try:
                registered = get_channel_router().get_providers()
            except Exception:
                pass

            placeholders = []
            for cfg in configs:
                pid = cfg["provider_id"]
                placeholders.append({
                    "provider_id": pid,
                    "display_name": cfg.get("display_name", pid),
                    "is_enabled": bool(cfg.get("is_enabled", False)),
                    "status": "checking",
                    "error_message": None,
                    "latency_ms": -1,
                    "last_check_time": None,
                })
            _data_sources_cache = placeholders
        except Exception:
            pass

        # 触发后台异步刷新，不阻塞
        import asyncio
        asyncio.create_task(_refresh_data_sources_background())
    else:
        # 已有缓存，也触发异步刷新（不阻塞返回）
        import asyncio
        asyncio.create_task(_refresh_data_sources_background())


def check_sdk_connection() -> str:
    """检测 Galaxy SDK 连接状态（检查子进程 + socket + IPC flag）"""
    import os
    try:
        from services.common.sdk_manager import get_sdk_manager
        from services.common.sdk_proxy_client import get_subprocess_manager
        from services.common.sdk_ipc import SOCKET_PATH

        sdk_mgr = get_sdk_manager()
        if sdk_mgr.is_connected():
            return "connected"

        # flag 为 False 但 socket 存在 + 子进程存活 → 下次 IPC 自动重连，视为已连接
        sub_mgr = get_subprocess_manager()
        if sub_mgr.is_subprocess_alive() and os.path.exists(SOCKET_PATH):
            return "connected"

        if sub_mgr.is_subprocess_alive():
            return "connecting"
        return "disconnected"
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
    kline_db_path = FilePath(__file__).parent.parent.parent / "data" / "kline.db"
    db_stats = {"kline_latest_date": None, "kline_latest_count": 0, "kline_total_count": 0,
                "factor_latest_date": None, "factor_latest_count": 0, "factor_total_count": 0,
                "weekly_latest_date": None, "weekly_total_count": 0,
                "base_info_count": 0}
    try:
        kline_db_path = FilePath(__file__).parent.parent.parent / "data" / "kline.db"
        if kline_db_path.exists():
            kconn = get_sync_connection("kline")
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
    except Exception:
        pass

    # 获取交易监控 SDK 健康状态
    try:
        from services.monitoring.service import get_trading_monitor
        monitor = get_trading_monitor()
        monitor_status = monitor.get_status()
        monitor_sdk_healthy = monitor_status.get("sdk_healthy", True)
        monitor_sdk_error_time = monitor_status.get("sdk_error_time", "")
        monitor_sdk_error_msg = monitor_status.get("sdk_error_msg", "")
        monitor_running = monitor_status.get("running", False)
        monitor_data_stale = monitor_status.get("data_stale", False)
        monitor_last_data_time = monitor_status.get("last_data_time", "")
    except Exception:
        monitor_sdk_healthy = True
        monitor_sdk_error_time = ""
        monitor_sdk_error_msg = ""
        monitor_running = False

    # 综合健康状态：SDK + Galaxy API + 服务运行状态
    sdk_connection_ok = check_sdk_connection()
    overall_healthy = sdk_connection_ok == "connected" and monitor_sdk_healthy and not monitor_data_stale

    # 确定异常原因
    health_issues = []
    if sdk_connection_ok != "connected":
        health_issues.append(f"SDK连接异常: {sdk_connection_ok}")
    if not monitor_sdk_healthy and monitor_running:
        health_issues.append(f"行情获取失败: {monitor_sdk_error_msg}")
    if monitor_data_stale and monitor_running:
        health_issues.append(f"行情数据过期: 最近成功获取时间={monitor_last_data_time or '无'}")

    return {
        "account_id": account_id,
        "account_name": account_name,
        "timestamp": get_china_time().isoformat(),
        "system_health": {
            "status": "healthy" if overall_healthy else "unhealthy",
            "issues": health_issues,
            "version": VERSION,
            "uptime_text": get_uptime_text(),
            "server_start": get_start_time().isoformat() if get_start_time() else None,
            "galaxy_api": sdk_connection_ok,
            "monitor_sdk_healthy": monitor_sdk_healthy,
            "monitor_sdk_error_time": monitor_sdk_error_time,
            "monitor_sdk_error_msg": monitor_sdk_error_msg,
            "monitor_running": monitor_running,
            "monitor_data_stale": monitor_data_stale,
            "monitor_last_data_time": monitor_last_data_time,
            "cpu_percent": resources["cpu_percent"],
            "memory_mb": resources["memory_mb"],
            "disk_percent": resources["disk_percent"],
        },
        "data_sources_status": await _get_data_sources_status(),
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
        "db_throughput": db.get_throughput(),
        "sdk_metrics": get_sdk_metrics(),
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
