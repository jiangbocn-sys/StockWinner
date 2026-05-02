"""
调度服务 API 接口

提供调度状态查询和手动触发功能
"""

from fastapi import APIRouter
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
async def manual_kline_check() -> Dict:
    """
    手动触发K线数据检查

    检查K线数据是否最新，如落后则启动增量下载和因子计算

    Returns:
        任务启动状态
    """
    from services.common.scheduler_service import get_scheduler

    scheduler = get_scheduler()
    return scheduler.run_manual_kline_check()


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