# ---
# name: K线增量检查
# description: 检查kline_data表最新日期，如缺失则下载当日数据
# category: 数据下载
# ---
"""
K线增量检查任务

检查本地数据库中kline_data表的最新交易日期，
如果发现缺失，自动下载最新K线数据并计算因子。
"""


async def execute(task_id: int = None, **kwargs):
    """执行K线增量检查

    直接同步执行，确保数据库状态能正确更新。
    """
    from services.common.scheduler_service import get_scheduler
    from services.common.database import get_db_manager
    from services.common.timezone import get_china_time
    import json

    scheduler = get_scheduler()

    # 直接执行，等待完成，并根据实际结果更新状态
    try:
        scheduler._daily_kline_check_job()
        result = {'success': True, 'message': 'K线增量检查完成'}
        status = 'success'
    except Exception as e:
        result = {'success': False, 'message': f'K线增量检查异常: {e}'}
        status = 'error'

    # 更新数据库任务状态
    if task_id is not None:
        db = get_db_manager()
        output = json.dumps(result, ensure_ascii=False)
        await db.execute(
            "UPDATE strategy_tasks SET last_status = ?, last_output = ?, updated_at = ? WHERE id = ?",
            (status, output, get_china_time().isoformat(), task_id)
        )

    return result
