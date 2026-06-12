# ---
# name: 复权因子更新
# description: 从SDK获取全量A股复权因子数据，更新本地数据库，用于除权计算
# category: 数据下载
# ---
"""
复权因子更新任务

从 SDK 获取所有A股股票的复权因子数据，
保存到本地数据库 stock_adj_factor 表，用于计算除权后的涨跌幅。

执行时间：凌晨 3 点（券商盘后数据已更新完成）
"""


async def execute(task_id: int = None, **kwargs):
    """执行复权因子全量更新"""
    from services.common.database import get_sync_connection
    from services.common.structured_logger import get_logger
    from services.data.adj_factor_service import update_adj_factor_from_sdk
    from services.factors.kline_manager import get_kline_manager
    from pathlib import Path
    import time

    log = get_logger("adj_factor_task")

    # 获取所有A股股票代码（全量更新）
    km = get_kline_manager()
    all_codes = km.get_all_stocks()

    if not all_codes:
        return {'success': True, 'message': '无股票代码', 'stocks': 0, 'saved': 0}

    log.info("adj_factor", f"开始全量更新 {len(all_codes)} 只股票的复权因子")

    # 分批更新（每批 50 只，避免 SDK 单次调用过大）
    batch_size = 50
    total_updated = 0
    total_saved = 0
    failed_batches = []

    for i in range(0, len(all_codes), batch_size):
        batch = all_codes[i:i + batch_size]
        batch_num = i // batch_size + 1

        try:
            result = update_adj_factor_from_sdk(batch)
            if result['success']:
                total_updated += result['stocks']
                total_saved += result['saved']
                log.info("adj_factor", f"批次 {batch_num}/{len(all_codes)//batch_size + 1}: {result['message']}")
            else:
                failed_batches.append(batch_num)
                log.warn("adj_factor", f"批次 {batch_num} 失败: {result['message']}")
        except Exception as e:
            failed_batches.append(batch_num)
            log.error("adj_factor", f"批次 {batch_num} 异常: {e}")

        # 批次间短暂间隔（避免 SDK 队列拥堵）
        time.sleep(0.3)

    result = {
        'success': len(failed_batches) < len(all_codes) // batch_size // 2,  # 允许少量失败
        'stocks_updated': total_updated,
        'records_saved': total_saved,
        'failed_batches': failed_batches,
        'message': f'更新 {total_updated} 只股票，保存 {total_saved} 条除权记录'
    }

    log.info("adj_factor", f"复权因子全量更新完成: {result['message']}")

    return result