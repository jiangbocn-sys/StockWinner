# ---
# name: 周K线全量下载
# description: 下载所有股票的周K线数据，用于中长期技术分析
# category: 数据下载
# ---
"""
周K线全量下载任务

下载完整的周K线数据到weekly_kline_data表，
用于中期趋势分析和周频因子计算。

重要：必须通过 run_manual_weekly_kline_check() 在独立线程中执行，
不能直接调用 _weekly_kline_check_job()，否则会阻塞事件循环。
"""


async def execute(task_id: int = None, **kwargs):
    """执行周K线全量下载

    使用 run_manual_weekly_kline_check() 在独立线程中执行，避免阻塞主事件循环。
    """
    from services.common.scheduler_service import get_scheduler
    from services.common.database import get_db_manager
    from services.common.timezone import get_china_time
    import json

    scheduler = get_scheduler()

    # 通过独立线程执行，不阻塞事件循环
    result = scheduler.run_manual_weekly_kline_check()

    # 更新数据库任务状态
    if task_id is not None:
        db = get_db_manager()
        output = json.dumps(result, ensure_ascii=False)
        await db.execute(
            "UPDATE strategy_tasks SET last_status = 'running', last_output = ?, updated_at = ? WHERE id = ?",
            (output, get_china_time().isoformat(), task_id)
        )

    return result
