# ---
# name: 月频因子更新
# description: 检查月频因子是否需要更新，如需要则启动更新任务
# category: 因子计算
# ---
"""
月频因子更新任务

检查stock_monthly_factors表的最新报告期，
对比最新财报数据，如有新的财报则启动因子更新。

重要：必须通过 run_manual_monthly_check() 在独立线程中执行，
不能直接调用 _monthly_factor_check_job()，否则会阻塞事件循环。
"""


async def execute(task_id: int = None, **kwargs):
    """执行月频因子更新检查

    使用 run_manual_monthly_check() 在独立线程中执行，避免阻塞主事件循环。
    """
    from services.common.scheduler_service import get_scheduler
    from services.common.database import get_db_manager
    from services.common.timezone import get_china_time
    import json

    scheduler = get_scheduler()

    # 通过独立线程执行，不阻塞事件循环
    result = scheduler.run_manual_monthly_check()

    # 更新数据库任务状态
    if task_id is not None:
        db = get_db_manager()
        output = json.dumps(result, ensure_ascii=False)
        await db.execute(
            "UPDATE strategy_tasks SET last_status = 'running', last_output = ?, updated_at = ? WHERE id = ?",
            (output, get_china_time().isoformat(), task_id)
        )

    return result
