"""
因子计算 API 接口

支持后台任务状态管理，防止重复调用
"""

from fastapi import APIRouter, Body
from typing import Optional
import asyncio
import threading

router = APIRouter()


@router.post("/api/v1/ui/factors/monthly/update")
async def update_monthly_factors(
    mode: str = Body("fill_empty", embed=True, description="更新模式：fill_empty=填充空值，fill_quarters=补充缺失季度，all=更新全部")
):
    """
    更新月频因子

    从SDK获取财务报表数据，计算估值因子和盈利因子

    Args:
        mode: 更新模式
            - 'fill_empty': 只填充pe_ttm为空的记录（推荐，速度较快）
            - 'fill_quarters': 补充缺失的季度报告记录（如2024Q1/Q2）
            - 'all': 更新所有股票（耗时较长）

    Returns:
        更新统计信息或任务状态
    """
    from services.common.task_manager import get_task_manager, TaskType, TaskStatus

    task_manager = get_task_manager()

    # 检查是否已有任务运行
    if task_manager.is_running(TaskType.MONTHLY_FACTOR_UPDATE):
        task_info = task_manager.get_status(TaskType.MONTHLY_FACTOR_UPDATE)
        return {
            "success": False,
            "message": "月频因子更新任务已在运行中，请等待完成",
            "task_status": task_info.to_dict()
        }

    # 启动任务
    task_manager.start_task(TaskType.MONTHLY_FACTOR_UPDATE)

    # 在后台线程执行（不阻塞API）
    def run_update():
        from services.factors.monthly_factor_updater import run_monthly_factor_update
        try:
            # 更新进度
            task_manager.update_progress(TaskType.MONTHLY_FACTOR_UPDATE, 5, "正在初始化...")

            result = run_monthly_factor_update(mode)

            # 完成
            task_manager.complete_task(TaskType.MONTHLY_FACTOR_UPDATE, result)
        except Exception as e:
            task_manager.fail_task(TaskType.MONTHLY_FACTOR_UPDATE, str(e))

    # 启动后台线程
    thread = threading.Thread(target=run_update, daemon=True)
    thread.start()

    return {
        "success": True,
        "message": "月频因子更新任务已启动",
        "task_status": task_manager.get_status(TaskType.MONTHLY_FACTOR_UPDATE).to_dict()
    }


@router.get("/api/v1/ui/factors/monthly/status")
async def get_monthly_factors_task_status():
    """
    获取月频因子更新任务状态

    Returns:
        任务状态信息（是否运行、进度百分比、开始时间等）
    """
    from services.common.task_manager import get_task_manager, TaskType

    task_manager = get_task_manager()
    task_info = task_manager.get_status(TaskType.MONTHLY_FACTOR_UPDATE)

    return task_info.to_dict()


@router.post("/api/v1/ui/factors/monthly/reset")
async def reset_monthly_factors_task():
    """
    重置月频因子更新任务状态

    仅在任务失败或需要重新启动时使用
    """
    from services.common.task_manager import get_task_manager, TaskType

    task_manager = get_task_manager()
    task_manager.reset_task(TaskType.MONTHLY_FACTOR_UPDATE)

    return {
        "success": True,
        "message": "月频因子更新任务状态已重置"
    }


@router.get("/api/v1/ui/factors/monthly/stats")
async def get_monthly_factors_stats():
    """
    获取月频因子统计信息

    Returns:
        空值比例、数据量等统计信息
    """
    import sqlite3
    from pathlib import Path

    db_path = Path(__file__).parent.parent.parent / "data" / "kline.db"

    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    # 总记录数
    cursor.execute("SELECT COUNT(*) FROM stock_monthly_factors")
    total = cursor.fetchone()[0]

    # 统计各因子空值比例
    factor_fields = [
        'pe_ttm', 'pb', 'ps_ttm', 'pcf', 'roe', 'roa',
        'gross_margin', 'net_margin', 'operating_margin',
        'revenue_growth_yoy', 'net_profit_growth_yoy',
        'total_market_cap', 'circ_market_cap'
    ]

    stats = {'total_records': total, 'factors': {}}

    for field in factor_fields:
        cursor.execute(f"SELECT COUNT(*) FROM stock_monthly_factors WHERE {field} IS NOT NULL AND {field} > 0")
        valid = cursor.fetchone()[0]
        stats['factors'][field] = {
            'valid': valid,
            'empty': total - valid,
            'fill_rate': round(valid / total * 100, 2) if total > 0 else 0
        }

    # 最新报告期
    cursor.execute("SELECT MAX(report_date) FROM stock_monthly_factors")
    stats['latest_report_date'] = cursor.fetchone()[0]

    # 已更新股票数
    cursor.execute("SELECT COUNT(DISTINCT stock_code) FROM stock_monthly_factors WHERE pe_ttm > 0")
    stats['stocks_updated'] = cursor.fetchone()[0]

    # 待更新股票数
    cursor.execute("SELECT COUNT(DISTINCT stock_code) FROM stock_monthly_factors WHERE pe_ttm IS NULL OR pe_ttm = 0")
    stats['stocks_pending'] = cursor.fetchone()[0]

    conn.close()

    return stats


@router.get("/api/v1/ui/tasks/status")
async def get_all_tasks_status():
    """
    获取所有后台任务状态

    Returns:
        所有任务的状态信息（数据下载、因子计算等）
    """
    from services.common.task_manager import get_task_manager

    task_manager = get_task_manager()
    return task_manager.get_all_status()


@router.post("/api/v1/ui/factors/monthly/fill-inherit")
async def fill_monthly_factors_inherit():
    """
    将季度财务数据沿用填充到月度记录

    月度记录使用最近一期季度的财务数据：
    - 1月、2月、3月 → 去年12月(Q4)
    - 4月、5月、6月 → 3月(Q1)
    - 7月、8月、9月 → 6月(Q2)
    - 10月、11月、12月 → 9月(Q3)

    Returns:
        填充统计信息
    """
    from services.factors.monthly_factor_filler import run_monthly_factor_fill

    result = run_monthly_factor_fill()

    return {
        "success": True,
        "message": f"月度因子填充完成：成功 {result['filled']} 条",
        "result": result
    }


@router.post("/api/v1/ui/factors/daily/calculate")
async def calculate_daily_factors(
    lookback_days: int = Body(5, embed=True, description="前溯天数，默认5天")
):
    """
    智能补算日频因子

    检测最近 N 个交易日的因子覆盖率，对缺失的股票进行计算

    Returns:
        计算结果统计
    """
    from services.common.task_manager import get_task_manager, TaskType

    task_manager = get_task_manager()

    if task_manager.is_running(TaskType.DAILY_FACTOR_CALC):
        task_info = task_manager.get_status(TaskType.DAILY_FACTOR_CALC)
        return {
            "success": False,
            "message": "日频因子计算任务已在运行中",
            "task_status": task_info.to_dict()
        }

    task_manager.start_task(TaskType.DAILY_FACTOR_CALC)

    def run_calc():
        try:
            from services.data.local_data_service import calculate_and_save_factors_for_dates
            from services.common.timezone import get_china_time
            from datetime import timedelta

            end_date = get_china_time().strftime('%Y-%m-%d')
            start_date = (get_china_time() - timedelta(days=lookback_days)).strftime('%Y-%m-%d')

            task_manager.update_progress(TaskType.DAILY_FACTOR_CALC, 10, f"正在计算 {start_date} 至 {end_date}...")

            inserted = calculate_and_save_factors_for_dates(
                start_date=start_date,
                end_date=end_date,
                only_new_dates=True,
                show_progress=True
            )

            task_manager.complete_task(TaskType.DAILY_FACTOR_CALC, {
                "inserted": inserted,
                "date_range": f"{start_date} 至 {end_date}"
            })
        except Exception as e:
            task_manager.fail_task(TaskType.DAILY_FACTOR_CALC, str(e))

    thread = threading.Thread(target=run_calc, daemon=True)
    thread.start()

    return {
        "success": True,
        "message": f"日频因子计算任务已启动（前溯 {lookback_days} 天）",
        "task_status": task_manager.get_status(TaskType.DAILY_FACTOR_CALC).to_dict()
    }


@router.post("/api/v1/ui/factors/daily/fill-empty")
async def fill_daily_factors_empty():
    """
    补算日频因子空值

    对已有记录但因子字段为空值的记录进行填充计算

    Returns:
        计算结果统计
    """
    from services.common.task_manager import get_task_manager, TaskType

    task_manager = get_task_manager()

    if task_manager.is_running(TaskType.DAILY_FACTOR_FILL):
        task_info = task_manager.get_status(TaskType.DAILY_FACTOR_FILL)
        return {
            "success": False,
            "message": "日频因子补算空值任务已在运行中",
            "task_status": task_info.to_dict()
        }

    task_manager.start_task(TaskType.DAILY_FACTOR_FILL)

    def run_fill():
        try:
            from services.data.local_data_service import fill_empty_factor_values

            task_manager.update_progress(TaskType.DAILY_FACTOR_FILL, 10, "正在补算空值...")

            result = fill_empty_factor_values(show_progress=True)

            task_manager.complete_task(TaskType.DAILY_FACTOR_FILL, result)
        except Exception as e:
            task_manager.fail_task(TaskType.DAILY_FACTOR_FILL, str(e))

    thread = threading.Thread(target=run_fill, daemon=True)
    thread.start()

    return {
        "success": True,
        "message": "日频因子补算空值任务已启动",
        "task_status": task_manager.get_status(TaskType.DAILY_FACTOR_FILL).to_dict()
    }