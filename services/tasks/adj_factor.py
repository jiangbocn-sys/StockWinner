# ---
# name: 复权因子更新
# description: 从SDK获取最新复权因子数据，更新本地h5文件，用于除权计算
# category: 数据下载
# ---
"""
复权因子更新任务

从 SDK 获取持仓和 watchlist 股票的复权因子数据，
保存到本地 h5 文件，用于计算除权后的涨跌幅。
"""


async def execute(task_id: int = None, **kwargs):
    """执行复权因子更新"""
    from services.common.sdk_proxy_client import SDKProxyClient
    from services.common.database import get_sync_connection
    from services.common.structured_logger import get_logger
    from pathlib import Path
    import pandas as pd

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
    conn.close()

    codes = [p[0] for p in positions] + [w[0] for w in watchlist]
    codes = list(set(codes))

    if not codes:
        return {'success': True, 'message': '无持仓和watchlist股票，跳过更新', 'count': 0}

    log.info(f"开始更新 {len(codes)} 只股票的复权因子")

    # 调用 SDK 获取复权因子
    proxy = SDKProxyClient.get_instance()
    if not proxy.connect_to_subprocess(timeout=10.0):
        return {'success': False, 'message': 'SDK 子进程连接失败', 'count': 0}

    result = proxy._call_ipc("get_adj_factor", {"stock_codes": codes}, priority=1, timeout=300.0)

    if result is not None and isinstance(result, pd.DataFrame) and not result.empty:
        count = len(result.columns)
        log.info(f"复权因子更新完成，覆盖 {count} 只股票")
        return {'success': True, 'message': f'复权因子更新完成', 'count': count}
    else:
        log.warning("复权因子更新返回空数据")
        return {'success': False, 'message': '复权因子更新返回空数据', 'count': 0}