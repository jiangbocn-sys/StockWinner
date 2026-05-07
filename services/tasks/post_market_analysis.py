# ---
# name: 盘后分析
# description: 收盘后对每只持仓股调用DSA进行分析，结果通过飞书发送
# category: 分析通知
# ---
"""
盘后分析任务

每天收盘后，逐一对持仓股调用 DSA 服务进行分析，
分析结果通过飞书通知发送给对应账户用户。
"""


async def execute(task_id: int = None, **kwargs):
    """执行盘后分析"""
    from services.common.scheduler_service import get_scheduler
    from services.common.database import get_db_manager
    from services.common.timezone import get_china_time
    import json

    scheduler = get_scheduler()
    result = scheduler._post_market_analysis_job()

    if task_id is not None:
        db = get_db_manager()
        output = json.dumps(result, ensure_ascii=False)
        await db.execute(
            "UPDATE strategy_tasks SET last_status = 'success', last_output = ?, updated_at = ? WHERE id = ?",
            (output, get_china_time().isoformat(), task_id)
        )

    return result
