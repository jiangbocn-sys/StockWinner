# ---
# name: K线增量检查
# description: 检查kline_data表最新日期，如缺失则下载当日数据
# category: 数据下载
# ---
"""
K线增量检查任务

检查本地数据库中kline_data表的最新交易日期，
如果发现缺失，自动下载最新K线数据。
"""


async def execute(full: bool = False, **kwargs):
    """执行K线增量检查

    Args:
        full: True=全量下载，False=增量检查
        **kwargs: 额外参数（保留扩展）
    """
    from services.common.scheduler_service import get_scheduler

    scheduler = get_scheduler()
    result = scheduler.run_manual_kline_check(full=full)
    return result
