"""
数据下载模块

统一 K 线数据下载（日K线/周K线/行业指数）

设计原则：
- 每个下载函数是独立可顺序调用的单元，适配后续 NightTaskQueue 链式执行
- SDK 调用经过 SDKConnectionManager 排队保护
- 数据库写入使用 get_sync_connection() (WAL + busy_timeout)
"""

import asyncio
import sqlite3
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import List, Dict, Optional, Tuple

from services.common.timezone import CHINA_TZ, get_china_time
from services.common.download_progress import get_progress_tracker, DownloadStatus

# 数据库路径
DB_PATH = Path(__file__).parent.parent.parent / "data" / "kline.db"


# ================================================================
# 交易日期工具
# ================================================================

def get_trading_day_end_date(current_time: Optional[datetime] = None,
                              use_sdk_calendar: bool = True) -> Tuple[str, str]:
    """
    根据当前时间确定下载结束日期

    规则：
    - 使用 SDK 交易日日历（推荐）：
      - 当前时间在交易日 16:00 前 -> 结束日期 = 前一交易日
      - 当前时间在交易日 16:00 后 -> 结束日期 = 当前交易日
    - 不使用 SDK 日历（降级方案）：
      - 工作日（周一-周五）且时间 < 16:00 -> 结束日期 = 前一日
      - 工作日（周一-周五）且时间 >= 16:00 -> 结束日期 = 当日
      - 周末（周六、周日）-> 结束日期 = 周五

    参数：
    - current_time: 当前时间，默认使用当前中国时区时间
    - use_sdk_calendar: 是否使用 SDK 交易日日历（默认 True）

    返回：
    - (end_date, status_msg): end_date 格式为 'YYYY-MM-DD'，status_msg 说明日期选择原因
    """
    if current_time is None:
        current_time = datetime.now(CHINA_TZ)

    today_str = current_time.strftime('%Y-%m-%d')
    today_int = int(today_str.replace('-', ''))
    current_hour = current_time.hour

    if use_sdk_calendar:
        try:
            from services.common.sdk_manager import get_sdk_manager
            sdk_manager = get_sdk_manager()
            sdk_manager.connect()

            calendar = sdk_manager.get_calendar()

            if calendar:
                last_trading_day = None
                prev_trading_day = None
                for i, day in enumerate(calendar):
                    if day == today_int:
                        last_trading_day = day
                        if i > 0:
                            prev_trading_day = calendar[i - 1]
                        break
                    elif day < today_int:
                        prev_trading_day = day
                        last_trading_day = day
                    else:
                        break

                if last_trading_day == today_int:
                    if current_hour < 16:
                        end_date = str(prev_trading_day) if prev_trading_day else (current_time - timedelta(days=1)).strftime('%Y%m%d')
                        end_date = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:8]}"
                        return end_date, f"交易日 {current_hour}:00 < 16:00，下载截止至 {end_date}"
                    else:
                        return today_str, f"交易日 {current_hour}:00 >= 16:00，下载包含今日 {today_str}"
                else:
                    end_date = str(last_trading_day) if last_trading_day else (current_time - timedelta(days=1)).strftime('%Y%m%d')
                    end_date = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:8]}"
                    return end_date, f"今日非交易日，下载截止至最近交易日 {end_date}"
        except Exception as e:
            print(f"[LocalData] SDK 日历查询失败：{e}，使用降级方案")

    # 降级方案：不使用 SDK 日历
    weekday = current_time.weekday()

    if weekday < 5:
        if current_hour < 16:
            end_date = (current_time - timedelta(days=1)).strftime('%Y-%m-%d')
            return end_date, f"工作日 {current_hour}:00 < 16:00，下载截止至 {end_date}"
        else:
            return today_str, f"工作日 {current_hour}:00 >= 16:00，下载包含今日 {today_str}"
    else:
        days_since_friday = weekday - 4
        friday = current_time - timedelta(days=days_since_friday)
        end_date = friday.strftime('%Y-%m-%d')
        day_name = "周六" if weekday == 5 else "周日"
        return end_date, f"周末（{day_name}），使用周五 {end_date}"


# ================================================================
# 动态批次计算
# ================================================================

# TGW 测试结论：返回数据量 ≈ 股票数 × 天数，单次上限 ~10000 条
# 安全阈值：股票数 × 天数 ≤ 5000
_MAX_DATA_POINTS = 5000


def calculate_batch_size(stock_count: int, trading_days: int) -> int:
    """
    根据 stock_count × trading_days ≤ 5000 公式计算最优批次大小

    Args:
        stock_count: 总股票数
        trading_days: 交易天数
    Returns:
        最优批次大小（每批股票数）
    """
    if trading_days <= 0:
        trading_days = 1

    # batch × days ≤ 5000 → batch ≤ 5000 / days
    optimal = _MAX_DATA_POINTS // trading_days

    # 上限：SDK 单次批量请求最多 3000 只
    optimal = min(optimal, 3000)
    # 下限：至少 50 只
    optimal = max(optimal, 50)

    return optimal


# ================================================================
# K 线全量下载
# ================================================================

async def download_all_kline_data(
    batch_size: int = 500,
    months: int = 24,
    start_date: str = None,
    end_date: str = None,
    broker_account: str = "",
    broker_password: str = "",
    calculate_factors: bool = True,
    market_filter: Optional[List[str]] = None,
    download_industry: bool = True
) -> bool:
    """
    异步下载全量 K 线数据

    按市场类型（SH/SZ/BJ）分开下载，避免不同类型股票混入批次导致整批失败。
    SI（行业指数）使用专用接口 get_industry_daily，不在本函数中下载。

    Args:
        batch_size: 每批次下载的股票数量（默认 20）
        months: 下载的月数（默认 24 个月）
        start_date: 开始日期（YYYY-MM-DD）
        end_date: 结束日期（YYYY-MM-DD）
        broker_account: 券商账户
        broker_password: 券商密码
        calculate_factors: 是否计算因子
        market_filter: 市场筛选列表 ['SH', 'SZ', 'BJ']
        download_industry: 是否下载行业指数

    Returns:
        下载是否成功
    """
    from services.trading.gateway import get_gateway_for_account
    from services.data.local_data_service import get_local_data_service

    tracker = get_progress_tracker()
    tracker.set_status_sync(DownloadStatus.IDLE)
    tracker._total_stocks = 0
    tracker._processed_stocks = 0
    tracker._total_tasks = 0
    tracker._processed_tasks = 0
    tracker._downloaded_records = 0
    tracker._current_stock = ""
    tracker._message = ""
    tracker._error = ""
    tracker._start_time = None
    tracker._end_time = None

    gateway = await get_gateway_for_account({
        'broker_account': broker_account,
        'broker_password': broker_password
    })

    if not gateway:
        tracker.set_status_sync(DownloadStatus.ERROR, "网关初始化失败")
        return False

    if not start_date or not end_date:
        current_time = datetime.now(CHINA_TZ)
        if not end_date:
            end_date = current_time.strftime('%Y-%m-%d')
        if not start_date:
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
            start_dt = end_dt - timedelta(days=months * 30)
            start_date = start_dt.strftime('%Y-%m-%d')

    print(f"[LocalData] 下载日期范围：{start_date} 至 {end_date}")

    tracker.set_status_sync(DownloadStatus.DOWNLOADING, "获取股票列表...")
    stock_list = await gateway.get_stock_list()

    # 按市场分组（排除 SI，SI 用专用接口）
    markets: Dict[str, List[dict]] = {}
    for s in stock_list:
        m = s.get('market', '')
        if market_filter and m not in market_filter:
            continue
        if m == 'SI':
            continue  # 行业指数走专用接口
        if m not in markets:
            markets[m] = []
        markets[m].append(s)

    # 市场顺序：SH → SZ → BJ
    market_order = [m for m in ['SH', 'SZ', 'BJ'] if m in markets]
    total_stocks = sum(len(v) for v in markets.values())
    print(f"[LocalData] 获取到 {total_stocks} 只股票（按市场: "
          f"{', '.join(f'{m}={len(markets[m])}' for m in market_order)}）")

    # 计算交易天数和动态批次大小
    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(end_date, '%Y-%m-%d')
    calendar_days = (end_dt - start_dt).days
    trading_days = max(1, int(calendar_days * 5 / 7))  # 约 5/7 是交易日

    tracker._total_stocks = total_stocks
    tracker._total_tasks = total_stocks
    tracker._start_time = get_china_time()
    tracker.set_status_sync(DownloadStatus.DOWNLOADING, f"开始下载 {total_stocks} 只股票K线...")

    local_service = get_local_data_service()
    downloaded_count = 0
    failed_count = 0
    global_progress = 0

    for market in market_order:
        mkt_stocks = markets[market]
        mkt_batch_size = min(batch_size, calculate_batch_size(len(mkt_stocks), trading_days))
        print(f"[LocalData] 市场 {market}: {len(mkt_stocks)} 只, 批次大小 {mkt_batch_size}")

        for i in range(0, len(mkt_stocks), mkt_batch_size):
            batch = mkt_stocks[i:i + mkt_batch_size]
            batch_codes = [f"{s.get('code')}.{s.get('market')}" for s in batch]

            global_progress = min(global_progress + len(batch), total_stocks)
            progress_pct = global_progress / total_stocks * 100
            tracker.update_sync(
                processed=global_progress,
                current_stock=batch_codes[0] if batch_codes else "",
                message=f"[{market}] 批次 {i//mkt_batch_size + 1}/{(len(mkt_stocks)-1)//mkt_batch_size + 1}"
            )

            try:
                from services.common.task_manager import get_task_manager, TaskType
                task_manager = get_task_manager()
                if task_manager.is_running(TaskType.DATA_DOWNLOAD):
                    task_manager.update_progress(TaskType.DATA_DOWNLOAD, round(progress_pct, 1),
                        message=f"[{market}] 批次 {i//mkt_batch_size + 1}/{(len(mkt_stocks)-1)//mkt_batch_size + 1} ({global_progress}/{total_stocks})")
            except Exception:
                pass

            try:
                start_date_int = start_date.replace('-', '') if start_date else None
                end_date_int = end_date.replace('-', '') if end_date else None
                kline_data = await gateway.get_batch_kline_data(
                    stock_codes=batch_codes,
                    start_date=start_date_int,
                    end_date=end_date_int,
                    task_type="download"
                )

                save_batch = []
                for stock_info in batch:
                    full_code = f"{stock_info.get('code')}.{stock_info.get('market')}"
                    name = stock_info.get('name', full_code)
                    df = kline_data.get(full_code)
                    if df is not None and len(df) > 0:
                        save_batch.append((full_code, name, df))
                        downloaded_count += 1
                    else:
                        # 停牌/无交易正常，不计入失败
                        pass

                if save_batch:
                    saved = local_service.save_kline_data_batch(save_batch)
                    print(f"[LocalData] [{market}] 批次 {i//mkt_batch_size + 1}: 保存 {saved} 条记录 ({len(save_batch)}/{len(batch)} 有数据)")
                else:
                    print(f"[LocalData] [{market}] 批次 {i//mkt_batch_size + 1}: 无数据返回 ({len(batch)} 只)")

            except Exception as e:
                print(f"[LocalData] [{market}] 批次 {i//mkt_batch_size + 1} 下载失败：{e}")
                failed_count += len(batch)

    # 计算因子（可选，后续 NightTaskQueue 会将此作为独立步骤）
    if calculate_factors and downloaded_count > 0:
        tracker.set_status_sync(DownloadStatus.CALCULATING_FACTORS, "开始计算因子...")
        try:
            from services.data.factor_service import calculate_and_save_factors_for_dates
            factor_count = calculate_and_save_factors_for_dates(
                start_date=start_date,
                end_date=end_date,
                only_new_dates=True,
                show_progress=True
            )
            print(f"[LocalData] 因子计算完成：插入 {factor_count} 条记录")
        except Exception as e:
            print(f"[LocalData] 因子计算失败：{e}")

    # 下载行业指数（可选，后续 NightTaskQueue 会将此作为独立步骤）
    if download_industry:
        tracker.set_status_sync(DownloadStatus.DOWNLOADING, "下载申万行业指数数据...")
        try:
            industry_result = download_industry_indices()
            if industry_result.get('success'):
                print(f"[LocalData] 行业指数下载完成：{industry_result.get('saved', 0)} 条记录")
            else:
                print(f"[LocalData] 行业指数下载失败：{industry_result.get('message', '未知错误')}")
        except Exception as e:
            print(f"[LocalData] 行业指数下载失败：{e}")

    tracker.complete_sync()

    return failed_count < total_stocks * 0.5


def download_all_kline_data_sync(
    batch_size: int = 500,
    months: int = 24,
    start_date: str = None,
    end_date: str = None,
    broker_account: str = "",
    broker_password: str = "",
    calculate_factors: bool = True,
    market_filter: Optional[List[str]] = None,
    download_industry: bool = False
) -> bool:
    """同步版本的下载函数，用于后台任务"""
    async def _async_download():
        return await download_all_kline_data(
            batch_size=batch_size,
            months=months,
            start_date=start_date,
            end_date=end_date,
            broker_account=broker_account,
            broker_password=broker_password,
            calculate_factors=calculate_factors,
            market_filter=market_filter,
            download_industry=download_industry
        )

    loop = asyncio.new_event_loop()
    try:
        asyncio.set_event_loop(loop)
        return loop.run_until_complete(_async_download())
    finally:
        loop.close()


# ================================================================
# K 线增量下载
# ================================================================

async def download_incremental_kline_data(
    batch_size: int = 500,
    months: int = 6,
    broker_account: str = "",
    broker_password: str = "",
    calculate_factors: bool = True,
    use_trading_time_rule: bool = True,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    download_industry: bool = True
) -> bool:
    """
    增量下载 K 线数据（带交易时间检查）

    下载时间逻辑：
    - 如果指定了 start_date 和 end_date：直接使用用户指定的日期范围
    - use_trading_time_rule=True 时：
      - 交易日早于 16:00 -> 结束日期 = 前一交易日
      - 交易日晚于 16:00 -> 结束日期 = 当日
      - 非交易日 -> 结束日期 = 前一个交易日
    - use_trading_time_rule=False 时：
      - 使用当前日期作为结束日期（忽略交易时间）
    """
    current_time = datetime.now(CHINA_TZ)

    if start_date and end_date:
        status_msg = f"使用用户指定的日期范围：{start_date} 至 {end_date}"
        print(f"[LocalData] {status_msg}")
        print(f"[LocalData] 当前时间：{current_time.strftime('%Y-%m-%d %H:%M:%S')}")
    else:
        end_date, status_msg = get_trading_day_end_date(current_time, use_sdk_calendar=use_trading_time_rule)
        print(f"[LocalData] {status_msg}")
        print(f"[LocalData] 当前时间：{current_time.strftime('%Y-%m-%d %H:%M:%S')}")

        end_date_obj = datetime.strptime(end_date, '%Y-%m-%d')
        start_date_obj = end_date_obj - timedelta(days=months * 30)
        start_date = start_date_obj.strftime('%Y-%m-%d')

    print(f"[LocalData] 下载日期范围：{start_date} 至 {end_date}")

    return await download_all_kline_data(
        batch_size=batch_size,
        months=months,
        start_date=start_date,
        end_date=end_date,
        broker_account=broker_account,
        broker_password=broker_password,
        calculate_factors=calculate_factors,
        download_industry=download_industry
    )


def download_incremental_kline_data_sync(
    batch_size: int = 500,
    months: int = 6,
    broker_account: str = "",
    broker_password: str = "",
    calculate_factors: bool = True,
    use_trading_time_rule: bool = True,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    download_industry: bool = True
) -> bool:
    """同步版本的增量下载函数"""
    async def _async_download():
        return await download_incremental_kline_data(
            batch_size=batch_size,
            months=months,
            broker_account=broker_account,
            broker_password=broker_password,
            calculate_factors=calculate_factors,
            use_trading_time_rule=use_trading_time_rule,
            start_date=start_date,
            end_date=end_date,
            download_industry=download_industry
        )

    loop = asyncio.new_event_loop()
    try:
        asyncio.set_event_loop(loop)
        return loop.run_until_complete(_async_download())
    finally:
        loop.close()


# ================================================================
# 行业指数下载
# ================================================================

def download_industry_indices() -> Dict:
    """
    下载申万行业指数K线数据

    使用SDK的InfoData.get_industry_daily方法获取行业指数数据，
    注意：这与普通股票/指数的query_kline方法不同，SI类型需要专用接口。

    Returns:
        下载统计信息
    """
    from services.common.sdk_manager import get_sdk_manager
    import pandas as pd

    print("[LocalData] [行业指数] 开始初始化 SDK...")
    sdk = get_sdk_manager()
    print("[LocalData] [行业指数] SDK 初始化完成，获取行业指数代码列表...")

    # 1. 获取行业指数代码列表
    industry_info = sdk.get_industry_base_info()
    print(f"[LocalData] [行业指数] 获取到行业指数基本信息，共 {len(industry_info) if industry_info is not None else 0} 条")
    if industry_info is None or len(industry_info) == 0:
        return {'success': False, 'message': '无法获取行业指数基本信息'}

    # 只获取一级分类（申万31个行业）
    level1 = industry_info[industry_info['LEVEL_TYPE'] == 1]
    print(f"[LocalData] [行业指数] 找到 {len(level1)} 个申万一级行业指数")

    # 提取代码列表和名称映射
    codes = []
    code_to_name = {}
    for idx, row in level1.iterrows():
        index_code = str(row.get('INDEX_CODE', ''))
        level1_name = row.get('LEVEL1_NAME', '')
        if index_code:
            codes.append(index_code)
            code_to_name[index_code] = level1_name

    # 2. 批量获取行业日行情数据
    print(f"[LocalData] [行业指数] 开始下载 {len(codes)} 个行业指数数据...")
    result = sdk.get_industry_daily(codes)
    print(f"[LocalData] [行业指数] SDK 返回结果，共 {len(result) if result else 0} 个指数")

    if not result:
        return {'success': False, 'message': 'SDK下载失败'}

    # 3. 保存到数据库
    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()

    total_saved = 0
    success_count = 0
    latest_dates = []
    for code, df in result.items():
        if df is None or len(df) == 0:
            print(f"[LocalData]   {code}: 无数据")
            continue

        # 将索引转换为列（TRADE_DATE是索引）
        original_index_name = df.index.name
        if original_index_name is not None:
            df = df.reset_index()
            print(f"[LocalData]   {code}: 索引 '{original_index_name}' 转换为列")

        # 转换列名小写
        df.columns = df.columns.str.lower()
        print(f"[LocalData]   {code}: 列名 {list(df.columns)}")

        # 确认trade_date存在
        if 'trade_date' not in df.columns:
            print(f"[LocalData]   {code}: 无trade_date列，跳过")
            continue

        # 插入数据
        saved_count = 0
        for _, row in df.iterrows():
            try:
                trade_date = row['trade_date']
                if pd.isna(trade_date):
                    continue
                if isinstance(trade_date, pd.Timestamp):
                    trade_date = trade_date.strftime('%Y-%m-%d')
                elif isinstance(trade_date, str) and len(trade_date) == 10:
                    pass
                else:
                    trade_date = str(trade_date)[:10]

                cursor.execute('''
                    INSERT OR REPLACE INTO kline_data
                    (stock_code, stock_name, trade_date, open, high, low, close, volume, amount)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    code,
                    code_to_name.get(code, ''),
                    trade_date,
                    float(row.get('open', 0) or 0),
                    float(row.get('high', 0) or 0),
                    float(row.get('low', 0) or 0),
                    float(row.get('close', 0) or 0),
                    int(row.get('volume', 0) or 0),
                    float(row.get('amount', 0) or 0)
                ))
                saved_count += 1
                total_saved += 1
            except Exception:
                pass

        conn.commit()
        success_count += 1
        latest_date = df['trade_date'].max()
        latest_dates.append(latest_date)
        print(f"[LocalData]   {code} ({code_to_name.get(code, '')}): 保存 {saved_count} 条，最新 {latest_date}")

    conn.close()

    print(f"[LocalData] 行业指数下载完成：{success_count}/{len(codes)} 个指数，{total_saved} 条记录")
    return {
        'success': True,
        'indices_count': success_count,
        'total_records': total_saved,
        'latest_date': str(max(latest_dates) if latest_dates else 'N/A')
    }
