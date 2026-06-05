"""
仪表盘 API
"""

from pathlib import Path as FilePath
from fastapi import APIRouter, HTTPException, Path, Query
from services.common.account_manager import get_account_manager
from services.common.database import get_db_manager, get_sync_connection
from services.common.timezone import get_china_time
from services._version import VERSION, get_start_time
from services.auth.account_validator import validate_account_active
from services.common.events import subscribe, EVENT_PROVIDER_STATUS

router = APIRouter()

# 数据源状态缓存：避免阻塞仪表盘返回，由后台任务异步刷新
_data_sources_cache: list = []

# 策略筛选进度缓存（内存中跟踪正在运行的筛选）
_screening_progress: dict = {}  # key: f"{account_id}:{strategy_id}"


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
    """返回数据源状态缓存（仅读，不触发检测）"""
    if not _data_sources_cache:
        await _ensure_data_sources_init()
    return list(_data_sources_cache)


def update_provider_status(provider_id: str, ok: bool, message: str = ""):
    """数据流驱动：实际数据通过某 provider 后更新其状态"""
    global _data_sources_cache
    for item in _data_sources_cache:
        if item.get("provider_id") == provider_id:
            item["status"] = "connected" if ok else "error"
            item["error_message"] = None if ok else message
            item["last_check_time"] = get_china_time().isoformat()
            return


def _handle_provider_status_event(data: dict):
    """事件处理器：接收 provider 状态变化事件"""
    update_provider_status(
        data.get("provider_id", ""),
        data.get("ok", False),
        data.get("message", "")
    )


def init_dashboard_events():
    """初始化 dashboard 事件订阅（服务启动时调用）"""
    from services.common.events import subscribe, EVENT_PROVIDER_STATUS
    subscribe(EVENT_PROVIDER_STATUS, _handle_provider_status_event)


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


async def _ensure_data_sources_init():
    """仅首次调用时初始化缓存+检测，后续不触发"""
    global _data_sources_cache
    if _data_sources_cache:
        return

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

    # 启动后做一次完整检测
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


@router.get("/api/v1/public/system-status")
async def get_public_system_status():
    """公开系统状态（无需认证）— 供管理员快速检查"""
    from services.monitoring.service import get_trading_monitor
    from services.common.scheduler_service import get_scheduler
    from services.common.price_cache import get_price_cache
    from services.trading.trading_hours import is_today_trading_day, can_trade

    # Monitor status
    try:
        monitor = get_trading_monitor()
        monitor_status = monitor.get_status()
    except Exception:
        monitor_status = {"running": False, "error": "无法获取监控状态"}

    # Scheduler status
    try:
        scheduler = get_scheduler()
        scheduler_status = scheduler.get_status()
    except Exception:
        scheduler_status = {"running": False, "error": "无法获取调度状态"}

    # PriceCache status
    try:
        cache = get_price_cache()
        cache_stats = cache.get_stats()
    except Exception:
        cache_stats = {"cache_total": 0, "cache_valid": 0}

    # SDK connection
    sdk_status = check_sdk_connection()

    # Trading hours
    try:
        trading_day = is_today_trading_day()
        trading_hours = can_trade()
    except Exception:
        trading_day = False
        trading_hours = False

    return {
        "timestamp": get_china_time().isoformat(),
        "version": VERSION,
        "uptime": get_uptime_text(),
        "trading_day": trading_day,
        "trading_hours": trading_hours,
        "sdk_connection": sdk_status,
        "monitor": {
            "running": monitor_status.get("running", False),
            "account_ids": monitor_status.get("account_ids", []),
            "heartbeat_age": monitor_status.get("heartbeat_age", 0),
            "is_zombie": monitor_status.get("is_zombie", False),
            "sdk_healthy": monitor_status.get("sdk_healthy", True),
            "data_stale": monitor_status.get("data_stale", False),
        },
        "scheduler": {
            "running": scheduler_status.get("running", False),
            "scheduler_running": scheduler_status.get("scheduler_running", False),
            "jobs_count": len(scheduler_status.get("jobs", [])),
        },
        "price_cache": {
            "total": cache_stats.get("cache_total", 0),
            "valid": cache_stats.get("cache_valid", 0),
            "ttl_seconds": cache_stats.get("ttl_seconds", 600),
        },
        "resources": get_resource_usage(),
    }


@router.get("/api/v1/ui/{account_id}/dashboard")
async def get_dashboard(account_id: str = Path(..., description="账户 ID")):
    """仪表盘总览数据"""

    await validate_account_active(account_id)

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
        monitor_heartbeat_age = monitor_status.get("heartbeat_age", 0)
        monitor_is_zombie = monitor_status.get("is_zombie", False)
    except Exception:
        monitor_sdk_healthy = True
        monitor_sdk_error_time = ""
        monitor_sdk_error_msg = ""
        monitor_running = False
        monitor_data_stale = False
        monitor_last_data_time = ""
        monitor_heartbeat_age = 0
        monitor_is_zombie = False

    # 获取调度器状态
    try:
        from services.common.scheduler_service import get_scheduler
        scheduler = get_scheduler()
        scheduler_status = scheduler.get_status()
        scheduler_running = scheduler_status.get("running", False)
        scheduler_jobs_count = len(scheduler_status.get("jobs", []))
    except Exception:
        scheduler_running = False
        scheduler_jobs_count = 0

    # 获取 PriceCache 状态
    try:
        from services.common.price_cache import get_price_cache
        cache = get_price_cache()
        cache_stats = cache.get_stats()
        price_cache_total = cache_stats.get("cache_total", 0)
        price_cache_valid = cache_stats.get("cache_valid", 0)
        price_cache_ttl = cache_stats.get("ttl_seconds", 600)
    except Exception:
        price_cache_total = 0
        price_cache_valid = 0
        price_cache_ttl = 600

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
            "monitor": {
                "running": monitor_running,
                "sdk_healthy": monitor_sdk_healthy,
                "data_stale": monitor_data_stale,
                "heartbeat_age": monitor_heartbeat_age,
                "is_zombie": monitor_is_zombie,
                "sdk_error_time": monitor_sdk_error_time,
                "sdk_error_msg": monitor_sdk_error_msg,
                "last_data_time": monitor_last_data_time,
            },
            "scheduler": {
                "running": scheduler_running,
                "jobs_count": scheduler_jobs_count,
            },
            "price_cache": {
                "total": price_cache_total,
                "valid": price_cache_valid,
                "ttl_seconds": price_cache_ttl,
            },
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

    await validate_account_active(account_id)

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


# ================================================================
# 任务状态 API
# ================================================================

def update_screening_progress(account_id: str, strategy_id: int, progress: dict):
    """更新策略筛选进度（由 screening service 调用）"""
    key = f"{account_id}:{strategy_id}"
    _screening_progress[key] = {
        "account_id": account_id,
        "strategy_id": strategy_id,
        "progress": progress.get("percent", 0),
        "message": progress.get("message", ""),
        "processed": progress.get("processed", 0),
        "total": progress.get("total_stocks", 0),
        "matched": progress.get("matched", 0),
        "current_stock": progress.get("current_stock", ""),
        "start_time": progress.get("start_time"),
        "updated_at": get_china_time().isoformat(),
    }


def clear_screening_progress(account_id: str, strategy_id: int):
    """清除策略筛选进度（筛选完成时调用）"""
    key = f"{account_id}:{strategy_id}"
    if key in _screening_progress:
        del _screening_progress[key]


def get_screening_progress(account_id: str, strategy_id: int) -> dict:
    """获取策略筛选进度"""
    key = f"{account_id}:{strategy_id}"
    return _screening_progress.get(key)


@router.get("/api/v1/ui/{account_id}/tasks/running")
async def get_running_tasks(account_id: str = Path(..., description="账户 ID")):
    """获取当前正在执行的所有任务

    返回：
    - 系统级后台任务（因子计算、数据下载）
    - 策略筛选任务
    - 回测任务
    """

    await validate_account_active(account_id)

    db = get_db_manager()
    tasks = []

    # 1. 系统级后台任务（TaskManager）
    try:
        from services.common.task_manager import get_task_manager
        task_mgr = get_task_manager()
        system_tasks = task_mgr.get_all_status()

        for task_type, info in system_tasks.items():
            if info.get("status") == "running":
                # 任务名称映射
                name_map = {
                    "data_download": "K线数据下载",
                    "daily_factor_calc": "日频因子计算",
                    "daily_factor_fill": "因子空值填充",
                    "monthly_factor_update": "月频因子更新",
                    "weekly_kline_download": "周K线下载",
                }
                tasks.append({
                    "type": "system",
                    "task_type": task_type,
                    "name": name_map.get(task_type, task_type),
                    "account_id": None,  # 系统级任务不关联账户
                    "progress": info.get("progress", {}).get("percent", 0),
                    "message": info.get("progress", {}).get("message", ""),
                    "started_at": info.get("start_time"),
                    "elapsed_seconds": info.get("elapsed_seconds", 0),
                    "can_cancel": False,  # 系统任务不允许取消
                })
    except Exception:
        pass

    # 2. 策略筛选任务（内存缓存 + 数据库）
    # 先检查内存缓存中的运行中筛选
    for key, prog in _screening_progress.items():
        if prog.get("account_id") == account_id:
            # 获取策略名称
            strategy = await db.fetchone(
                "SELECT name FROM strategies WHERE id = ?",
                (prog.get("strategy_id"),)
            )
            tasks.append({
                "type": "screening",
                "task_type": "strategy_screening",
                "id": prog.get("strategy_id"),
                "name": strategy.get("name", "未知策略") if strategy else "未知策略",
                "account_id": account_id,
                "progress": prog.get("progress", 0),
                "message": prog.get("message", f"已处理 {prog.get('processed', 0)}/{prog.get('total', 0)}"),
                "started_at": prog.get("start_time"),
                "elapsed_seconds": 0,
                "current_stock": prog.get("current_stock", ""),
                "matched": prog.get("matched", 0),
                "can_cancel": True,
            })

    # 也检查数据库中 last_status='running' 的任务（可能是之前崩溃遗留）
    running_strategy_tasks = await db.fetchall("""
        SELECT st.id, st.strategy_id, st.last_run_at, st.last_output,
               s.name as strategy_name
        FROM strategy_tasks st
        JOIN strategies s ON st.strategy_id = s.id
        WHERE st.account_id = ? AND st.last_status = 'running'
    """, (account_id,))

    for t in running_strategy_tasks:
        # 检查是否已在内存缓存中（避免重复）
        key = f"{account_id}:{t['strategy_id']}"
        if key in _screening_progress:
            continue

        # 解析 last_output 获取进度
        import json
        output = {}
        try:
            output = json.loads(t.get("last_output") or "{}")
        except Exception:
            pass

        tasks.append({
            "type": "screening",
            "task_type": "strategy_screening",
            "id": t["strategy_id"],
            "name": t["strategy_name"],
            "account_id": account_id,
            "progress": 50,  # 无法确定进度
            "message": "正在筛选（进度未知）",
            "started_at": t.get("last_run_at"),
            "elapsed_seconds": 0,
            "can_cancel": True,
            "stale": True,  # 标记为可能遗留
        })

    # 3. 回测任务
    running_backtests = await db.fetchall("""
        SELECT br.id, br.name, br.strategy_id, br.status, br.progress, br.started_at,
               br.current_trade_date, br.start_date, br.end_date,
               s.name as strategy_name
        FROM backtest_runs br
        LEFT JOIN strategies s ON br.strategy_id = s.id
        WHERE br.account_id = ? AND br.status = 'running'
        ORDER BY br.started_at DESC
    """, (account_id,))

    for b in running_backtests:
        # 计算进度百分比（基于日期范围）
        progress_pct = b.get("progress", 0)
        if not progress_pct and b.get("start_date") and b.get("end_date") and b.get("current_trade_date"):
            # 简单估算：当前日期 / 总日期范围
            try:
                from datetime import datetime
                start = datetime.strptime(b["start_date"], "%Y-%m-%d")
                end = datetime.strptime(b["end_date"], "%Y-%m-%d")
                current = datetime.strptime(b["current_trade_date"], "%Y-%m-%d")
                total_days = (end - start).days + 1
                passed_days = (current - start).days + 1
                progress_pct = round(passed_days / total_days * 100, 1)
            except Exception:
                progress_pct = 0

        message = f"回测进度 {progress_pct:.0f}%"
        if b.get("current_trade_date"):
            message += f"（当前日期: {b['current_trade_date']}）"

        tasks.append({
            "type": "backtest",
            "task_type": "backtest",
            "id": b["id"],
            "name": b.get("name") or b.get("strategy_name") or "回测",
            "strategy_name": b.get("strategy_name"),
            "account_id": account_id,
            "progress": progress_pct,
            "message": message,
            "started_at": b.get("started_at"),
            "elapsed_seconds": 0,
            "current_date": b.get("current_trade_date"),
            "can_cancel": True,
        })

    # 按启动时间排序
    tasks.sort(key=lambda x: x.get("started_at") or "", reverse=True)

    return {
        "account_id": account_id,
        "timestamp": get_china_time().isoformat(),
        "tasks": tasks,
        "total": len(tasks),
        "system_busy": len([t for t in tasks if t["type"] == "system"]) > 0,
    }


@router.get("/api/v1/ui/{account_id}/tasks/history")
async def get_task_history(
    account_id: str = Path(..., description="账户 ID"),
    limit: int = Query(20, description="返回数量"),
    task_type: str = Query(None, description="任务类型过滤"),
):
    """获取最近完成的任务历史"""

    await validate_account_active(account_id)

    db = get_db_manager()
    history = []

    # 最近完成的策略任务
    if not task_type or task_type == "screening":
        strategy_tasks = await db.fetchall("""
            SELECT st.id, st.strategy_id, st.last_run_at, st.last_status, st.last_output,
                   s.name as strategy_name
            FROM strategy_tasks st
            JOIN strategies s ON st.strategy_id = s.id
            WHERE st.account_id = ? AND st.last_status IN ('success', 'error', 'failed')
            ORDER BY st.last_run_at DESC
            LIMIT ?
        """, (account_id, limit))

        for t in strategy_tasks:
            import json
            output = {}
            try:
                output = json.loads(t.get("last_output") or "{}")
            except Exception:
                pass

            history.append({
                "type": "screening",
                "task_type": "strategy_screening",
                "id": t["strategy_id"],
                "name": t["strategy_name"],
                "status": t["last_status"],
                "completed_at": t["last_run_at"],
                "result": output,
                "matched_count": output.get("added", 0) if t["last_status"] == "success" else 0,
            })

    # 最近完成的回测
    if not task_type or task_type == "backtest":
        backtests = await db.fetchall("""
            SELECT br.id, br.name, br.strategy_id, br.status, br.progress, br.completed_at,
                   br.result_summary, br.error_message,
                   s.name as strategy_name
            FROM backtest_runs br
            LEFT JOIN strategies s ON br.strategy_id = s.id
            WHERE br.account_id = ? AND br.status IN ('completed', 'failed', 'cancelled')
            ORDER BY br.completed_at DESC
            LIMIT ?
        """, (account_id, limit))

        for b in backtests:
            import json
            result = {}
            try:
                result = json.loads(b.get("result_summary") or "{}")
            except Exception:
                pass

            history.append({
                "type": "backtest",
                "task_type": "backtest",
                "id": b["id"],
                "name": b.get("name") or b.get("strategy_name") or "回测",
                "status": b["status"],
                "completed_at": b["completed_at"],
                "result": result,
                "error": b.get("error_message"),
            })

    # 按完成时间排序
    history.sort(key=lambda x: x.get("completed_at") or "", reverse=True)

    return {
        "account_id": account_id,
        "history": history[:limit],
        "total": len(history),
    }
