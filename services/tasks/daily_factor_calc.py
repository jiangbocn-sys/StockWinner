# ---
# name: 日频因子智能补算
# description: 检测最近交易日因子覆盖率，对缺失股票进行计算（手动启动）
# category: 因子计算
# ---
"""
日频因子智能补算任务

手动启动任务，检测最近 N 个交易日的因子覆盖率，
对缺失的股票进行因子计算。

重要：此任务无 cron 表达式，仅通过手动触发执行。
"""


async def execute(task_id: int = None, lookback_days: int = 5, **kwargs):
    """执行日频因子智能补算

    Args:
        task_id: 任务 ID（用于更新状态）
        lookback_days: 前溯天数，默认 5 天
    """
    from services.common.scheduler_service import get_scheduler
    from services.common.database import get_db_manager
    from services.common.timezone import get_china_time
    import json

    scheduler = get_scheduler()

    # 使用独立线程执行，避免阻塞主事件循环
    result = scheduler.run_manual_daily_factor_calc(lookback_days=lookback_days)

    # 更新数据库任务状态
    if task_id is not None:
        db = get_db_manager()
        output = json.dumps(result, ensure_ascii=False)
        await db.execute(
            "UPDATE strategy_tasks SET last_status = ?, last_output = ?, updated_at = ? WHERE id = ?",
            (result.get('status', 'running'), output, get_china_time().isoformat(), task_id)
        )

    return result