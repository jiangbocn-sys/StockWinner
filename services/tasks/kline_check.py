# ---
# name: K线增量检查
# description: 检查kline_data表最新日期，如缺失则下载当日数据
# category: 数据下载
# ---
"""
K线增量检查任务

检查本地数据库中kline_data表的最新交易日期，
如果发现缺失，自动下载最新K线数据并计算因子。

交易时段拒绝下载（09:15-15:00），避免影响实时行情稳定性。

重要：必须通过 run_manual_kline_check() 在独立线程中执行，
不能直接调用 _daily_kline_check_job()，否则会阻塞事件循环。
"""


async def execute(task_id: int = None, **kwargs):
    """执行K线增量检查

    使用 run_manual_kline_check() 在独立线程中执行，避免阻塞主事件循环。
    交易时段拒绝下载并返回错误信息。
    """
    from services.common.scheduler_service import get_scheduler
    from services.common.database import get_db_manager
    from services.common.timezone import get_china_time
    import json

    scheduler = get_scheduler()

    # 通过独立线程执行，不阻塞事件循环
    result = scheduler.run_manual_kline_check(full=False)

    # 更新数据库任务状态
    if task_id is not None:
        db = get_db_manager()
        output = json.dumps(result, ensure_ascii=False)
        status = 'running' if result.get('success') else 'aborted'
        await db.execute(
            "UPDATE strategy_tasks SET last_status = ?, last_output = ?, updated_at = ? WHERE id = ?",
            (status, output, get_china_time().isoformat(), task_id)
        )

    return result
