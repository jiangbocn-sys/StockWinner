# ---
# name: 行业指数下载
# description: 使用SDK下载31个申万一级行业指数日线数据
# category: 数据下载
# ---
"""
申万行业指数下载任务

通过SDK的InfoData.get_industry_daily方法，
下载全部31个申万一级行业指数的日线数据。

重要：必须通过 run_manual_industry_indices_download() 在独立线程中执行，
不能直接调用 _run_industry_indices_download()，否则会阻塞事件循环。
"""


async def execute(task_id: int = None, **kwargs):
    """执行行业指数下载

    使用 run_manual_industry_indices_download() 在独立线程中执行，避免阻塞主事件循环。
    """
    from services.common.scheduler_service import get_scheduler
    from services.common.database import get_db_manager
    from services.common.timezone import get_china_time
    import json

    scheduler = get_scheduler()

    # 通过独立线程执行，不阻塞事件循环
    result = scheduler.run_manual_industry_indices_download()

    # 更新数据库任务状态
    if task_id is not None:
        db = get_db_manager()
        output = json.dumps(result, ensure_ascii=False)
        await db.execute(
            "UPDATE strategy_tasks SET last_status = 'running', last_output = ?, updated_at = ? WHERE id = ?",
            (output, get_china_time().isoformat(), task_id)
        )

    return result
