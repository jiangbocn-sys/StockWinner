"""
系统任务状态检查 API

提供统一的接口查询系统中正在运行的任务：
- 数据下载
- 因子计算
- 回测任务
- 交易监控
- 策略执行
"""

from fastapi import APIRouter, HTTPException
from typing import Dict, List, Any
from services.common.database import get_db_manager

router = APIRouter()


@router.get("/api/v1/ui/system/running-tasks")
async def get_running_tasks() -> Dict:
    """
    获取系统中正在运行的任务列表

    Returns:
        {
            "has_running": bool,
            "tasks": [
                {"type": "backtest", "id": 149, "name": "...", "progress": 50.0},
                {"type": "data_download", "status": "running", "progress": {...}},
                ...
            ],
            "blocking_count": int,
            "message": str
        }
    """
    tasks = []
    blocking_count = 0

    # 1. 检查回测任务
    db = get_db_manager()
    running_backtests = await db.fetchall(
        "SELECT id, name, progress, current_trade_date, started_at FROM backtest_runs WHERE status = 'running'"
    )
    for bt in running_backtests:
        tasks.append({
            "type": "backtest",
            "id": bt["id"],
            "name": bt["name"],
            "progress": bt["progress"],
            "current_date": bt["current_trade_date"],
            "started_at": bt["started_at"],
            "blocking": True,  # 回测任务是阻塞型
        })
        blocking_count += 1

    # 2. 检查 TaskManager 任务（下载、因子计算等）
    try:
        from services.common.task_manager import get_task_manager, TaskStatus
        tm = get_task_manager()
        all_status = tm.get_all_status()
        for task_type, info in all_status.items():
            if info.get("status") == "running":
                tasks.append({
                    "type": task_type,
                    "name": _get_task_display_name(task_type),
                    "progress": info.get("progress", {}),
                    "start_time": info.get("start_time"),
                    "elapsed_seconds": info.get("elapsed_seconds", 0),
                    "blocking": True,  # 数据下载、因子计算都是阻塞型
                })
                blocking_count += 1
    except Exception:
        pass

    # 3. 检查交易监控
    try:
        from services.monitoring.service import get_trading_monitor
        monitor = get_trading_monitor()
        if monitor._running:
            tasks.append({
                "type": "trading_monitor",
                "name": "交易监控",
                "status": "running",
                "blocking": False,  # 监控不是阻塞型，重启会自动停止
            })
    except Exception:
        pass

    # 4. 检查策略任务（strategy_tasks 表）
    try:
        running_strategy_tasks = await db.fetchall(
            "SELECT id, strategy_id, module, last_status, last_output FROM strategy_tasks WHERE last_status = 'running'"
        )
        for st in running_strategy_tasks:
            # 获取策略/任务名称
            name = st.get("module") or "策略执行"
            if st.get("strategy_id"):
                strategy = await db.fetchone(
                    "SELECT name FROM strategies WHERE id = ?", (st["strategy_id"],)
                )
                if strategy:
                    name = strategy["name"]
            tasks.append({
                "type": "strategy_task",
                "id": st["id"],
                "name": name,
                "blocking": True,
            })
            blocking_count += 1
    except Exception:
        pass

    # 5. 检查回测子进程
    try:
        from services.backtest.subprocess_worker import get_backtest_process_manager
        bt_mgr = get_backtest_process_manager()
        active_count = bt_mgr.get_active_count()
        if active_count > 0:
            # 子进程回测已在上面的数据库查询中统计，这里只是补充说明
            pass
    except Exception:
        pass

    # 构建消息
    if blocking_count > 0:
        message = f"系统中有 {blocking_count} 个正在执行的任务，重启将中断这些任务。请确认后再重启。"
    else:
        message = "系统中无正在执行的任务，可以安全重启。"

    return {
        "has_running": blocking_count > 0,
        "tasks": tasks,
        "blocking_count": blocking_count,
        "message": message,
    }


@router.post("/api/v1/ui/system/confirm-restart")
async def confirm_restart(force: bool = False) -> Dict:
    """
    确认重启（记录用户确认）

    如果 force=True，即使有任务运行也允许重启。
    否则需要先检查是否有阻塞任务。

    Returns:
        {
            "allowed": bool,
            "message": str,
            "blocking_tasks": list
        }
    """
    running_info = await get_running_tasks()

    if force:
        # 强制重启，记录警告
        from services.common.structured_logger import get_logger
        log = get_logger("system")
        log.warn("force_restart", f"用户强制重启，将中断 {running_info['blocking_count']} 个任务")
        return {
            "allowed": True,
            "message": f"强制重启已确认，将中断 {running_info['blocking_count']} 个任务",
            "blocking_tasks": running_info["tasks"],
        }

    if running_info["blocking_count"] > 0:
        return {
            "allowed": False,
            "message": running_info["message"],
            "blocking_tasks": running_info["tasks"],
        }

    return {
        "allowed": True,
        "message": "可以安全重启",
        "blocking_tasks": [],
    }


def _get_task_display_name(task_type: str) -> str:
    """任务类型中文名称"""
    names = {
        "data_download": "K线数据下载",
        "daily_factor_calc": "日频因子计算",
        "daily_factor_fill": "因子空值填充",
        "monthly_factor_update": "月频因子更新",
        "weekly_kline_download": "周K线下载",
    }
    return names.get(task_type, task_type)