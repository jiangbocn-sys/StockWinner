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


async def execute(**kwargs):
    """执行行业指数下载"""
    from services.common.scheduler_service import get_scheduler

    scheduler = get_scheduler()
    result = scheduler.run_manual_industry_indices_download()
    return result
