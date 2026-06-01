# ---
# name: 复权因子更新
# description: 从SDK获取最新复权因子数据，更新本地数据库，用于除权计算
# category: 数据下载
# ---
"""
复权因子更新任务

从 SDK 获取持仓和 watchlist 股票的复权因子数据，
保存到本地数据库 stock_adj_factor 表，用于计算除权后的涨跌幅。
"""


async def execute(task_id: int = None, **kwargs):
    """执行复权因子更新"""
    from services.common.database import get_sync_connection
    from services.common.structured_logger import get_logger
    from services.data.adj_factor_service import update_adj_factor_from_sdk
    from pathlib import Path

    log = get_logger("adj_factor_task")

    # 获取持仓+watchlist股票代码
    db_path = Path(__file__).parent.parent.parent / "data" / "stockwinner.db"
    conn = get_sync_connection(path=db_path)
    positions = conn.execute(
        "SELECT DISTINCT stock_code FROM stock_positions WHERE quantity > 0"
    ).fetchall()
    watchlist = conn.execute(
        "SELECT DISTINCT stock_code FROM watchlist WHERE status IN ('pending', 'watching', 'bought')"
    ).fetchall()

    codes = [p['stock_code'] for p in positions] + [w['stock_code'] for w in watchlist]
    codes = list(set(codes))

    if not codes:
        return {'success': True, 'message': '无持仓和watchlist股票，跳过更新', 'count': 0}

    log.info(f"开始更新 {len(codes)} 只股票的复权因子")

    # 调用新服务：从 SDK 获取并保存到数据库
    result = update_adj_factor_from_sdk(codes)

    log.info(f"复权因子更新完成：{result['message']}")

    return {
        'success': result['success'],
        'message': result['message'],
        'stocks_updated': result['stocks'],
        'records_saved': result['saved']
    }