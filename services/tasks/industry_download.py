# ---
# name: 行业指数下载
# description: 使用SDK下载31个申万一级行业指数日线数据
# category: 数据下载
# ---
"""
申万行业指数下载任务

通过SDK的InfoData.get_industry_daily方法，
下载全部31个申万一级行业指数的日线数据。
"""


async def execute(task_id: int = None, **kwargs):
    """执行行业指数下载"""
    from services.common.scheduler_service import get_scheduler
    from services.common.database import get_db_manager
    from services.common.timezone import get_china_time
    import json

    scheduler = get_scheduler()
    scheduler._run_industry_indices_download()

    result = {'success': True, 'message': '行业指数下载完成'}

    if task_id is not None:
        db = get_db_manager()
        output = json.dumps(result, ensure_ascii=False)
        await db.execute(
            "UPDATE strategy_tasks SET last_status = 'success', last_output = ?, updated_at = ? WHERE id = ?",
            (output, get_china_time().isoformat(), task_id)
        )

    return result
