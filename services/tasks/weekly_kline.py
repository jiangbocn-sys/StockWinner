# ---
# name: 周K线全量下载
# description: 下载所有股票的周K线数据，用于中长期技术分析
# category: 数据下载
# ---
"""
周K线全量下载任务

下载完整的周K线数据到weekly_kline_data表，
用于中期趋势分析和周频因子计算。
"""


async def execute(**kwargs):
    """执行周K线全量下载"""
    from services.common.scheduler_service import get_scheduler

    scheduler = get_scheduler()
    result = scheduler.run_manual_weekly_kline_download()
    return result
