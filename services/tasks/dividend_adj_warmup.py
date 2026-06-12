# ---
# name: 复权因子盘前预热
# description: 使用分红数据快速更新活跃股票复权因子，交易时段零SDK开销
# category: 数据预热
# ---
"""
复权因子盘前预热任务

使用 get_dividend（0.3秒）替代 get_adj_factor（30秒超时），
更新活跃股票（持仓 + watchlist）的复权因子数据。

执行时间：每日 8:50（开盘前 5 分钟）
"""


async def execute(task_id: int = None, **kwargs):
    """执行复权因子预热"""
    from services.data.dividend_adj_service import update_adj_factor_from_dividend, get_missing_adj_factor_stocks
    from services.common.structured_logger import get_logger

    log = get_logger("dividend_adj_task")

    log.info("dividend_adj_task", "开始复权因子盘前预热")

    # 1. 更新活跃股票的复权因子
    result = update_adj_factor_from_dividend()

    # 2. 检查缺失复权因子的股票（首次预热）
    missing = get_missing_adj_factor_stocks()
    if missing:
        log.info("dividend_adj_task", f"发现 {len(missing)} 只缺失复权因子的股票，补充更新")
        result2 = update_adj_factor_from_dividend(missing)
        result['saved'] += result2['saved']

    log.info("dividend_adj_task", f"预热完成: {result['message']}")

    return {
        'success': result['success'],
        'stocks': result['stocks'],
        'saved': result['saved'],
        'message': result['message']
    }