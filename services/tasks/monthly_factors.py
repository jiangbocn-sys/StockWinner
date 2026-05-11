# ---
# name: 月频因子更新
# description: 检查月频因子是否需要更新，如需要则启动更新任务
# category: 因子计算
# ---
"""
月频因子更新任务

检查stock_monthly_factors表的最新报告期，
对比最新财报数据，如有新的财报则启动因子更新。
"""


async def execute(task_id: int = None, **kwargs):
    """执行月频因子更新检查"""
    from services.common.scheduler_service import get_scheduler
    from services.common.database import get_db_manager
    from services.common.timezone import get_china_time
    import json

    scheduler = get_scheduler()
    result = scheduler._monthly_factor_check_job()

    status = 'success' if result and result.get('success') else 'error'

    if task_id is not None:
        db = get_db_manager()
        output = json.dumps(result or {'success': False, 'message': '任务未返回结果'}, ensure_ascii=False)
        await db.execute(
            "UPDATE strategy_tasks SET last_status = ?, last_output = ?, updated_at = ? WHERE id = ?",
            (status, output, get_china_time().isoformat(), task_id)
        )

    return result
