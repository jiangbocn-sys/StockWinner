"""
调度服务 API 接口

提供调度状态查询和手动触发功能
"""

from fastapi import APIRouter, Query
from typing import Dict

router = APIRouter()


@router.get("/api/v1/ui/scheduler/status")
async def get_scheduler_status() -> Dict:
    """
    获取调度服务状态

    Returns:
        调度服务运行状态、任务列表、执行历史
    """
    from services.common.scheduler_service import get_scheduler

    scheduler = get_scheduler()
    return scheduler.get_status()


@router.post("/api/v1/ui/scheduler/start")
async def start_scheduler() -> Dict:
    """
    启动调度服务

    Returns:
        启动结果
    """
    from services.common.scheduler_service import start_scheduler

    scheduler = start_scheduler()
    return {
        'success': True,
        'message': '调度服务已启动',
        'status': scheduler.get_status()
    }


@router.post("/api/v1/ui/scheduler/stop")
async def stop_scheduler() -> Dict:
    """
    停止调度服务

    Returns:
        停止结果
    """
    from services.common.scheduler_service import stop_scheduler

    stop_scheduler()
    return {'success': True, 'message': '调度服务已停止'}


@router.post("/api/v1/ui/scheduler/kline/check")
async def manual_kline_check(full: bool = Query(False, description="是否全量下载")) -> Dict:
    """
    手动触发K线数据检查

    默认增量检查，full=true 时执行全量下载

    Returns:
        任务启动状态
    """
    from services.common.scheduler_service import get_scheduler

    scheduler = get_scheduler()
    return scheduler.run_manual_kline_check(full=full)


@router.post("/api/v1/ui/scheduler/weekly/kline")
async def manual_weekly_kline_download() -> Dict:
    """
    手动触发周K线数据下载

    Returns:
        任务启动状态
    """
    from services.common.scheduler_service import get_scheduler

    scheduler = get_scheduler()
    return scheduler.run_manual_weekly_kline_download()


@router.post("/api/v1/ui/scheduler/monthly/check")
async def manual_monthly_check() -> Dict:
    """
    手动触发月频因子更新

    检查月频因子是否需要更新，如需要则启动更新任务

    Returns:
        任务启动状态
    """
    from services.common.scheduler_service import get_scheduler

    scheduler = get_scheduler()
    return scheduler.run_manual_monthly_check()


@router.post("/api/v1/ui/scheduler/industry/download")
async def manual_industry_indices_download() -> Dict:
    """
    手动触发申万行业指数下载

    使用SDK的InfoData.get_industry_daily方法下载31个申万一级行业指数数据

    Returns:
        任务启动状态
    """
    from services.common.scheduler_service import get_scheduler

    scheduler = get_scheduler()
    return scheduler.run_manual_industry_indices_download()


@router.get("/api/v1/ui/data/status")
async def get_data_status() -> Dict:
    """
    获取各数据表最新日期

    Returns:
        kline_data、weekly_kline_data、stock_daily_factors、stock_monthly_factors 最新日期
    """
    import sqlite3
    from pathlib import Path

    db_path = Path(__file__).parent.parent.parent / "data" / "kline.db"

    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    status = {}

    # kline_data 最新日期
    cursor.execute("SELECT MAX(trade_date) FROM kline_data")
    status['kline_latest'] = cursor.fetchone()[0]

    # weekly_kline_data 最新日期
    cursor.execute("SELECT MAX(week_end_date) FROM weekly_kline_data")
    status['weekly_kline_latest'] = cursor.fetchone()[0]

    # stock_daily_factors 最新日期
    cursor.execute("SELECT MAX(trade_date) FROM stock_daily_factors")
    status['daily_factors_latest'] = cursor.fetchone()[0]

    # stock_monthly_factors 最新报告期
    cursor.execute("SELECT MAX(report_date) FROM stock_monthly_factors")
    status['monthly_factors_latest'] = cursor.fetchone()[0]

    conn.close()
    return status