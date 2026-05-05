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


async def execute(**kwargs):
    """执行月频因子更新检查"""
    from services.common.scheduler_service import get_scheduler

    scheduler = get_scheduler()
    result = scheduler.run_manual_monthly_check()
    return result
