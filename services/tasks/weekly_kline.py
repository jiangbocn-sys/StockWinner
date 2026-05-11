# ---
# name: 周K线全量下载
# description: 下载所有股票的周K线数据，用于中长期技术分析
# category: 数据下载
# ---
"""
周K线全量下载任务

下载完整的周K线数据到weekly_kline_data表，
用于中期趋势分析和周频因子计算。
"""


async def execute(task_id: int = None, **kwargs):
    """执行周K线全量下载"""
    from services.common.scheduler_service import get_scheduler
    from services.common.database import get_db_manager
    from services.common.timezone import get_china_time
    import json

    scheduler = get_scheduler()
    result = scheduler._weekly_kline_check_job()

    status = 'success' if result and result.get('success') else 'error'

    if task_id is not None:
        db = get_db_manager()
        output = json.dumps(result or {'success': False, 'message': '任务未返回结果'}, ensure_ascii=False)
        await db.execute(
            "UPDATE strategy_tasks SET last_status = ?, last_output = ?, updated_at = ? WHERE id = ?",
            (status, output, get_china_time().isoformat(), task_id)
        )

    return result
