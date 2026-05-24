"""
周K线数据下载模块
通过 SDK 下载周K线数据并保存到 weekly_kline_data 表

下载策略：
- 增量下载：检查每只股票已有周K线的最新日期，只下载缺失部分
- 无历史数据时下载近 10 年作为初始数据
"""
import sys
sys.path.insert(0, '/home/bobo/StockWinner')

from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any, Tuple

from services.factors.kline_manager import get_kline_manager, KlineManager


def _delete_incomplete_week(km: KlineManager, cutoff_date: str):
    """删除指定日期之后的周数据（含本周未完成的部分）"""
    deleted = km.delete_incomplete_week(cutoff_date)
    if deleted > 0:
        print(f"[WeeklyKline] 删除不完整周数据 {deleted} 条（week_end > {cutoff_date}）")


async def download_weekly_kline_data(
    years: int = 10,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    broker_account: str = "",
    broker_password: str = "",
    batch_size: int = 0,
    market_filter: Optional[List[str]] = ['SH', 'SZ']
):
    """
    通过 SDK 下载周K线数据（增量模式）

    重要：
    - 只下载到最近一个完整交易周，不创建本周未完成的数据
    - 下载前自动清理超过 cutoff 的不完整周数据
    - 动态计算批次大小：初始下载按数据量自动调整（100~500只/批），增量下载按 200 只/批
    """
    from services.trading.gateway import get_gateway_for_account
    from services.common.database import get_sync_connection, KLINE_DB_PATH

    # 连接网关
    broker_creds = {
        "broker_account": broker_account,
        "broker_password": broker_password,
    }
    gateway = await get_gateway_for_account(broker_creds)

    if not gateway or not hasattr(gateway, 'sdk_available') or not gateway.sdk_available:
        print("[WeeklyKline] 错误：交易网关不可用")
        return False

    if not gateway.connected:
        print("[WeeklyKline] 错误：交易网关未连接")
        return False

    gateway_name = type(gateway).__name__
    print(f"[WeeklyKline] 使用交易网关：{gateway_name}")

    km = get_kline_manager()

    # 计算最近一个完整交易周
    last_complete_week = km.get_last_completed_week_end()
    cutoff_date = last_complete_week.strftime('%Y-%m-%d')
    print(f"[WeeklyKline] 最近完整交易周截止: {cutoff_date}")

    # 清理不完整周数据
    _delete_incomplete_week(km, cutoff_date)

    # 获取股票列表
    all_stocks = km.get_all_stocks()
    if not all_stocks:
        print("[WeeklyKline] 错误：无股票数据")
        return False

    # 获取股票名称映射（优先 stock_base_info，名称最准确）
    conn = get_sync_connection("kline", path=KLINE_DB_PATH)
    cursor = conn.cursor()
    # 优先查 stock_base_info（每日 SDK 同步，名称最新）
    cursor.execute("SELECT stock_code, stock_name FROM stock_base_info")
    base_rows = cursor.fetchall()
    stock_names = {row[0]: row[1] or '' for row in base_rows}
    # 补充：stock_base_info 没有的，从 kline_data 取
    cursor.execute("SELECT DISTINCT stock_code, stock_name FROM kline_data")
    kline_rows = cursor.fetchall()
    for row in kline_rows:
        if row[0] not in stock_names and row[1]:
            stock_names[row[0]] = row[1]

    stock_list = []
    for code in all_stocks:
        name = stock_names.get(code, '')
        if market_filter:
            if code.endswith('.SH') and 'SH' not in market_filter:
                continue
            if code.endswith('.SZ') and 'SZ' not in market_filter:
                continue
            if code.endswith('.BJ') and 'BJ' not in market_filter:
                continue
            if code.endswith('.SI') and 'SI' not in market_filter:
                continue
        stock_list.append({'stock_code': code, 'stock_name': name})

    if not stock_list:
        print("[WeeklyKline] 错误：过滤后无股票数据")
        return False

    # 获取每只股票已有周K线的最新日期和最早日期
    latest_map = km.get_weekly_latest()
    earliest_map = km.get_weekly_earliest()

    # 使用 cutoff_date 作为下载截止日期
    end_dt = last_complete_week
    cutoff_10y = (end_dt - timedelta(days=years * 365)).strftime('%Y-%m-%d')

    # 分类：无数据 → 初始下载；有数据但不足10年 → 初始下载（回溯补全）；有数据且足10年 → 增量更新
    needs_update = []
    needs_initial = []
    for s in stock_list:
        code = s['stock_code']
        latest = latest_map.get(code)
        earliest = earliest_map.get(code)

        # 已有数据超过 cutoff，跳过
        if latest and latest > cutoff_date:
            continue

        # 无数据，或最早日期晚于 cutoff_10y（不足10年覆盖），需要初始下载
        if not earliest or earliest > cutoff_10y:
            needs_initial.append(s)
        else:
            needs_update.append(s)

    total_stocks = len(stock_list)
    print(f"[WeeklyKline] 共 {total_stocks} 只股票")
    print(f"[WeeklyKline]   需全量下载（无数据或不足{years}年）: {len(needs_initial)} 只")
    print(f"[WeeklyKline]   需增量更新: {len(needs_update)} 只")

    # 先处理初始下载
    if needs_initial:
        initial_start = end_dt - timedelta(days=years * 365) if not start_date else datetime.strptime(start_date, '%Y-%m-%d')
        end_date_int = int((end_dt + timedelta(days=7)).strftime('%Y%m%d'))
        start_date_int = int(initial_start.strftime('%Y%m%d'))

        # 动态计算批次大小：初始下载数据量大（10年×52周），减少每批股票数
        # 目标：每批返回不超过 ~25,000 条（避免 SDK 超时）
        weeks_per_stock = years * 52  # ~520
        max_records_per_batch = 25000
        dynamic_batch_size = max(20, min(500, max_records_per_batch // weeks_per_stock))
        actual_batch_size = batch_size if batch_size > 0 else dynamic_batch_size

        print(f"[WeeklyKline] 初始下载: {len(needs_initial)} 只股票, {initial_start.strftime('%Y-%m-%d')} ~ {cutoff_date}")
        print(f"[WeeklyKline]   每批 {actual_batch_size} 只（动态计算：{dynamic_batch_size}），每只约 {weeks_per_stock} 条周K线")
        await _download_batch(gateway, needs_initial, start_date_int, end_date_int, years, actual_batch_size)

    # 再处理增量更新
    if needs_update:
        end_date_int = int((end_dt + timedelta(days=7)).strftime('%Y%m%d'))

        # 找到最早的起始日期用于打印
        earliest_start_str = None
        for s in needs_update:
            latest = latest_map.get(s['stock_code'])
            if latest:
                if earliest_start_str is None or latest < earliest_start_str:
                    earliest_start_str = latest

        print(f"[WeeklyKline] 增量更新: {len(needs_update)} 只股票, 从 {earliest_start_str or 'N/A'} 开始")

        # 增量下载：每只股票数据量小（通常只有几周到几个月），可以批量处理更多股票
        incremental_batch_size = batch_size if batch_size > 0 else 200
        print(f"[WeeklyKline]   每批 {incremental_batch_size} 只")
        await _download_incremental(gateway, needs_update, latest_map, end_date_int, incremental_batch_size)

    print(f"[WeeklyKline] ====== 下载完成 ======")
    return True


async def _download_batch(
    gateway, stocks: List[dict],
    start_date_int: int, end_date_int: int, years: int,
    batch_size: int
):
    """批量下载（相同日期范围）"""
    from services.factors.kline_manager import KlineManager
    km = get_kline_manager()
    total_stocks = len(stocks)
    total_saved = 0

    for i in range(0, total_stocks, batch_size):
        batch = stocks[i:i + batch_size]
        batch_codes = [s.get('stock_code') for s in batch]

        processed = i + len(batch)
        progress_pct = processed / total_stocks * 100

        print(f"[WeeklyKline] 进度：{progress_pct:.1f}% ({processed}/{total_stocks})")

        try:
            from services.common.task_manager import get_task_manager, TaskType
            task_manager = get_task_manager()
            if task_manager.is_running(TaskType.WEEKLY_KLINE_DOWNLOAD):
                task_manager.update_progress(TaskType.WEEKLY_KLINE_DOWNLOAD, round(progress_pct, 1),
                    message=f"下载批次 {i//batch_size + 1}/{(total_stocks-1)//batch_size + 1} ({processed}/{total_stocks})")
        except Exception:
            pass

        try:
            kline_data = await gateway.get_batch_kline_data(
                stock_codes=batch_codes,
                period="week",
                start_date=str(start_date_int),
                end_date=str(end_date_int),
                limit=years * 52 + 200  # 额外缓冲，避免日历对齐损失
            )

            for code, data in kline_data.items():
                if data is None or len(data) == 0:
                    continue

                stock_name = ''
                for s in batch:
                    if s.get('stock_code') == code:
                        stock_name = s.get('stock_name', '')
                        break

                saved = km.save_weekly_kline_data(code, stock_name, data)
                total_saved += saved

        except Exception as e:
            print(f"[WeeklyKline] 批次下载失败：{e}")
            continue

    print(f"[WeeklyKline] 初始下载完成，保存 {total_saved} 条记录")


async def _download_incremental(
    gateway, stocks: List[dict],
    latest_map: Dict[str, str], end_date_int: int,
    batch_size: int
):
    """批量增量下载（每批多只股票，从各自的最新日期开始）"""
    from services.factors.kline_manager import KlineManager
    km = get_kline_manager()
    total_stocks = len(stocks)
    total_saved = 0

    # 计算增量下载的起止日期
    end_dt_str = str(end_date_int).zfill(8)
    end_dt_str = f"{end_dt_str[:4]}-{end_dt_str[4:6]}-{end_dt_str[6:8]}"

    for i in range(0, total_stocks, batch_size):
        batch = stocks[i:i + batch_size]

        processed = i + len(batch)
        progress_pct = processed / total_stocks * 100

        print(f"[WeeklyKline] 增量进度：{progress_pct:.1f}% ({processed}/{total_stocks})")

        try:
            from services.common.task_manager import get_task_manager, TaskType
            task_manager = get_task_manager()
            if task_manager.is_running(TaskType.WEEKLY_KLINE_DOWNLOAD):
                task_manager.update_progress(TaskType.WEEKLY_KLINE_DOWNLOAD, round(progress_pct, 1),
                    message=f"增量更新 {processed}/{total_stocks}")
        except Exception:
            pass

        # 批量获取该批所有股票的周K线（仅查询最新日期之后 + 90天缓冲）
        batch_codes = [s['stock_code'] for s in batch]
        # 找到该批最早的起始日期
        earliest_latest = None
        for s in batch:
            latest = latest_map.get(s['stock_code'])
            if latest:
                if earliest_latest is None or latest < earliest_latest:
                    earliest_latest = latest
        # 从最早起始日期前 90 天开始查（覆盖可能的周历偏移）
        if earliest_latest:
            start_from = (datetime.strptime(earliest_latest, '%Y-%m-%d') - timedelta(days=90)).strftime('%Y%m%d')
        else:
            start_from = str(end_date_int - 9000)
        try:
            kline_data = await gateway.get_batch_kline_data(
                stock_codes=batch_codes,
                period="week",
                start_date=start_from,
                end_date=str(end_date_int),
                limit=200  # 增量通常只需几周，200条足够
            )

            for s in batch:
                code = s['stock_code']
                stock_name = s['stock_name']
                latest = latest_map.get(code)

                if not latest:
                    continue

                # 如果起始日期已经超过结束日期，跳过
                start_dt = datetime.strptime(latest, '%Y-%m-%d') + timedelta(days=7)
                if start_dt > datetime.strptime(end_dt_str, '%Y-%m-%d'):
                    continue

                data = kline_data.get(code)
                if data is not None and len(data) > 0:
                    saved = km.save_weekly_kline_data(
                        code, stock_name, data, skip_existing_before=latest
                    )
                    total_saved += saved

        except Exception as e:
            print(f"[WeeklyKline] 批次 {i//batch_size + 1} 增量下载失败: {e}")
            continue

    print(f"[WeeklyKline] 增量更新完成，保存 {total_saved} 条记录")


def download_weekly_kline_sync(
    years: int = 10,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    broker_account: str = "",
    broker_password: str = "",
    batch_size: int = 50,
    market_filter: Optional[List[str]] = ['SH', 'SZ']
):
    """同步版本的周K线下载函数（显式创建新事件循环）"""
    import asyncio
    loop = asyncio.new_event_loop()
    try:
        asyncio.set_event_loop(loop)
        return loop.run_until_complete(download_weekly_kline_data(
            years=years, start_date=start_date, end_date=end_date,
            broker_account=broker_account, broker_password=broker_password,
            batch_size=batch_size, market_filter=market_filter
        ))
    finally:
        loop.close()


if __name__ == "__main__":
    import os
    # 测试下载 - 从环境变量读取凭证
    download_weekly_kline_sync(
        years=10,
        broker_account=os.environ.get("SDK_USERNAME", ""),
        broker_password=os.environ.get("SDK_PASSWORD", ""),
        batch_size=50
    )
