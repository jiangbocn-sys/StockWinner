"""
周K线数据下载模块
通过 SDK 下载周K线数据并保存到 weekly_kline_data 表

下载策略：
- 增量下载：检查每只股票已有周K线的最新日期，只下载缺失部分
- 无历史数据时下载近 10 年作为初始数据
"""
import sys
sys.path.insert(0, '/home/bobo/StockWinner')

import asyncio
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, Dict, Any, Tuple

# 时区配置
from zoneinfo import ZoneInfo
CHINA_TZ = ZoneInfo("Asia/Shanghai")

# 数据库路径
DB_PATH = Path("/home/bobo/StockWinner/data/kline.db")


def _get_last_completed_week_end(now: datetime) -> datetime:
    """
    计算最近一个完整的交易周结束日期（周五）

    规则：
    - 周一到周五 16:00 前：本周未完成，返回上周五
    - 周五 16:00 后 或 周末：本周已完成，返回本周五
    """
    weekday = now.weekday()  # 0=周一, 4=周五, 5=周六, 6=周日

    if weekday <= 3:
        # 周一到周四 → 本周未完成，返回上周五
        return now - timedelta(days=weekday + 3)
    elif weekday == 4:
        # 周五
        if now.hour < 16:
            # 16:00 前 → 返回上周五
            return now - timedelta(days=7)
        else:
            # 16:00 后 → 返回本周五
            return now
    else:
        # 周六或周日 → 本周五已完成
        return now - timedelta(days=weekday - 4)


def _get_weekly_kline_latest(conn: sqlite3.Connection) -> Dict[str, str]:
    """获取每只股票已有周K线的最新日期

    Returns:
        {stock_code: latest_week_end_date}
    """
    cursor = conn.cursor()
    cursor.execute("""
        SELECT stock_code, MAX(week_end_date) FROM weekly_kline_data GROUP BY stock_code
    """)
    return {row[0]: row[1] for row in cursor.fetchall()}


def _delete_incomplete_week(conn: sqlite3.Connection, cutoff_date: str):
    """删除指定日期之后的周数据（含本周未完成的部分）

    注意：cutoff_date 是最近一个完整交易周的结束日期。
    删除 week_end_date > cutoff_date 的记录（而非 week_start_date），
    确保同一周内即使 week_start <= cutoff 也会被清理。
    """
    cursor = conn.cursor()
    cursor.execute("DELETE FROM weekly_kline_data WHERE week_end_date > ?", (cutoff_date,))
    deleted = cursor.rowcount
    if deleted > 0:
        conn.commit()
        print(f"[WeeklyKline] 删除不完整周数据 {deleted} 条（week_end > {cutoff_date}）")


async def download_weekly_kline_data(
    years: int = 10,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    broker_account: str = "",
    broker_password: str = "",
    batch_size: int = 50,
    market_filter: Optional[List[str]] = ['SH', 'SZ']
):
    """
    通过 SDK 下载周K线数据（增量模式）

    重要：
    - 只下载到最近一个完整交易周，不创建本周未完成的数据
    - 下载前自动清理超过 cutoff 的不完整周数据

    Args:
        years: 无历史数据时下载的年数（默认 10 年）
        start_date: 开始日期（YYYY-MM-DD），指定则覆盖增量逻辑
        end_date: 结束日期（YYYY-MM-DD），可选
        broker_account: 券商账户
        broker_password: 券商密码
        batch_size: 每批次下载的股票数量
        market_filter: 市场筛选（默认沪深）
    """
    from services.trading.gateway import get_gateway_for_account

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

    now = datetime.now(CHINA_TZ)

    # 计算最近一个完整交易周
    last_complete_week = _get_last_completed_week_end(now)
    cutoff_date = last_complete_week.strftime('%Y-%m-%d')
    print(f"[WeeklyKline] 最近完整交易周截止: {cutoff_date}")

    # 连接数据库
    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()

    # 清理不完整周数据
    _delete_incomplete_week(conn, cutoff_date)

    # 从数据库获取股票列表
    cursor.execute("SELECT DISTINCT stock_code, stock_name FROM kline_data ORDER BY stock_code")
    stock_rows = cursor.fetchall()

    stock_list = []
    for row in stock_rows:
        code = row[0]
        name = row[1] or ''
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
        print("[WeeklyKline] 错误：无股票数据")
        conn.close()
        return False

    # 增量逻辑：获取每只股票已有周K线的最新日期
    latest_map = _get_weekly_kline_latest(conn)

    # 过滤掉已有数据超过 cutoff 的股票（防止重复）
    for s in stock_list:
        code = s['stock_code']
        latest = latest_map.get(code)
        if latest and latest > cutoff_date:
            # 理论上不会发生，因为上面已经清理了
            del latest_map[code]

    # 分组：有历史数据 vs 无历史数据
    needs_update = []
    needs_initial = []
    for s in stock_list:
        code = s['stock_code']
        if code in latest_map:
            needs_update.append(s)
        else:
            needs_initial.append(s)

    total_stocks = len(stock_list)
    print(f"[WeeklyKline] 共 {total_stocks} 只股票")
    print(f"[WeeklyKline]   需增量更新: {len(needs_update)} 只")
    print(f"[WeeklyKline]   需初始下载: {len(needs_initial)} 只")

    # 使用 cutoff_date 作为下载截止日期
    end_dt = last_complete_week

    # 先处理初始下载
    if needs_initial:
        initial_start = now.replace(tzinfo=None) - timedelta(days=years * 365) if not start_date else datetime.strptime(start_date, '%Y-%m-%d')
        end_date_int = int((end_dt + timedelta(days=7)).strftime('%Y%m%d'))
        start_date_int = int(initial_start.strftime('%Y%m%d'))

        print(f"[WeeklyKline] 初始下载: {len(needs_initial)} 只股票, {initial_start.strftime('%Y-%m-%d')} ~ {cutoff_date}")
        await _download_batch(gateway, conn, needs_initial, start_date_int, end_date_int, years, batch_size, 0)

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

        offset = len(needs_initial) * 1
        await _download_incremental(gateway, conn, needs_update, latest_map, end_date_int, batch_size, offset)

    conn.close()

    print(f"[WeeklyKline] ====== 下载完成 ======")
    return True


async def _download_batch(
    gateway, conn: sqlite3.Connection, stocks: List[dict],
    start_date_int: int, end_date_int: int, years: int,
    batch_size: int, progress_offset: int
):
    """批量下载（相同日期范围）"""
    cursor = conn.cursor()
    total_stocks = len(stocks)
    total_saved = 0

    # 构建市场级交易周参考表（一次构建，所有股票共用）
    start_date_str = f"{start_date_int:08d}"
    start_date_str = f"{start_date_str[:4]}-{start_date_str[4:6]}-{start_date_str[6:8]}"
    end_date_str = f"{end_date_int:08d}"
    end_date_str = f"{end_date_str[:4]}-{end_date_str[4:6]}-{end_date_str[6:8]}"
    week_calendar = _build_market_week_calendar(
        cursor, datetime.strptime(start_date_str, '%Y-%m-%d'), datetime.strptime(end_date_str, '%Y-%m-%d')
    )
    if week_calendar:
        print(f"[WeeklyKline] 交易周参考: {week_calendar[0][2]} ~ {week_calendar[-1][3]} 共 {len(week_calendar)} 周")

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
                limit=years * 52
            )

            for code, data in kline_data.items():
                if data is None or len(data) == 0:
                    continue

                stock_name = ''
                for s in batch:
                    if s.get('stock_code') == code:
                        stock_name = s.get('stock_name', '')
                        break

                saved = _save_weekly_data(cursor, code, stock_name, data, week_calendar)
                total_saved += saved

            conn.commit()

        except Exception as e:
            print(f"[WeeklyKline] 批次下载失败：{e}")
            continue

    print(f"[WeeklyKline] 初始下载完成，保存 {total_saved} 条记录")


async def _download_incremental(
    gateway, conn: sqlite3.Connection, stocks: List[dict],
    latest_map: Dict[str, str], end_date_int: int,
    batch_size: int, progress_offset: int
):
    """增量下载（每只股票从自己的最新日期开始）"""
    cursor = conn.cursor()
    total_stocks = len(stocks)
    total_saved = 0

    # 计算增量下载的起止日期
    end_dt_str = str(end_date_int).zfill(8)
    end_dt_str = f"{end_dt_str[:4]}-{end_dt_str[4:6]}-{end_dt_str[6:8]}"

    # 找到所有股票中最早的起始日期，用于构建交易周参考表
    earliest_start = None
    for s in stocks:
        latest = latest_map.get(s['stock_code'])
        if latest:
            start_dt = datetime.strptime(latest, '%Y-%m-%d') + timedelta(days=7)
            if earliest_start is None or start_dt < earliest_start:
                earliest_start = start_dt
    if earliest_start is None:
        return
    earliest_start_str = earliest_start.strftime('%Y-%m-%d')

    week_calendar = _build_market_week_calendar(
        cursor, earliest_start, datetime.strptime(end_dt_str, '%Y-%m-%d')
    )

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

        for s in batch:
            code = s['stock_code']
            stock_name = s['stock_name']
            latest = latest_map.get(code)

            if not latest:
                continue

            # 计算该股票的起始日期
            start_dt = datetime.strptime(latest, '%Y-%m-%d') + timedelta(days=7)

            # 如果起始日期已经超过结束日期，跳过
            if start_dt > datetime.strptime(end_dt_str, '%Y-%m-%d'):
                continue

            start_date_int = int(start_dt.strftime('%Y%m%d'))

            try:
                kline_data = await gateway.get_batch_kline_data(
                    stock_codes=[code],
                    period="week",
                    start_date=str(start_date_int),
                    end_date=str(end_date_int),
                    limit=520  # 最多10年
                )

                data = kline_data.get(code)
                if data is not None and len(data) > 0:
                    saved = _save_weekly_data(cursor, code, stock_name, data, week_calendar, latest)
                    total_saved += saved

            except Exception as e:
                print(f"[WeeklyKline] {code} 增量下载失败: {e}")
                continue

        conn.commit()

    print(f"[WeeklyKline] 增量更新完成，保存 {total_saved} 条记录")


def _build_market_week_calendar(cursor, start_dt: datetime, end_dt: datetime) -> list:
    """从 kline_data 表构建市场级交易周参考表

    按 ISO 周分组所有交易日，返回 [(iso_year, iso_week, week_start, week_end), ...]
    其中 week_start/end 是市场实际最早/最晚交易日（非固定周一/周五）。

    个股可能停牌，但市场整体一定有足够多的交易日来标定每周范围。
    """
    cursor.execute("""
        SELECT DISTINCT trade_date FROM kline_data
        WHERE trade_date >= ? AND trade_date <= ?
        ORDER BY trade_date
    """, (start_dt.strftime('%Y-%m-%d'), end_dt.strftime('%Y-%m-%d')))
    trade_days = [r[0] for r in cursor.fetchall()]

    if not trade_days:
        return []

    # 按 ISO 周分组
    weeks = {}
    for day_str in trade_days:
        d = datetime.strptime(day_str, '%Y-%m-%d')
        iso_year, iso_week, _ = d.isocalendar()
        key = (iso_year, iso_week)
        if key not in weeks:
            weeks[key] = [day_str]
        else:
            weeks[key].append(day_str)

    result = []
    for key in sorted(weeks.keys()):
        days = weeks[key]
        result.append((key[0], key[1], days[0], days[-1]))
    return result


def _save_weekly_data(cursor, code: str, stock_name: str, data,
                      week_calendar: list = None,
                      skip_existing_before: str = None) -> int:
    """保存单只股票的周K线数据

    SDK 返回的周线不含 trade_date（为空字符串），因此不能直接推算周起止日期。

    解决方案：
    1. 从 kline_data 表构建市场级交易周参考表（按 ISO 周分组）
    2. SDK 周线按时间顺序返回，逐条匹配到对应的 ISO 周
    3. 对已有历史数据的股票，skip_existing_before 指定该股票已有数据的最新日期，
       跳过该日期之前的周线，只写入新增周

    支持场景：
    - 正常周：周一 ~ 周五
    - 节假日开头：周二/三 ~ 周五（如清明后 04-07~04-10）
    - 节假日结尾：周一 ~ 周四（如五一前 04-27~04-30）
    - 个股停牌：用市场整体日历，不依赖个股自身日K
    """
    import pandas as pd

    if isinstance(data, pd.DataFrame):
        df = data
    else:
        df = pd.DataFrame(data)

    if df.empty:
        return 0

    saved = 0
    bar_index = 0
    total_bars = len(df)

    # 找到第一周需要写入的位置
    # 如果指定了 skip_existing_before，跳过该日期所在周及之前的所有周
    cal_index = 0
    if week_calendar:
        if skip_existing_before:
            for i, (iso_y, iso_w, ws, we) in enumerate(week_calendar):
                if we > skip_existing_before:
                    cal_index = i
                    break
                # 消耗一个 SDK bar（该周已有数据，不需要写入）
                bar_index += 1

    for _, row in df.iterrows():
        if bar_index >= total_bars:
            break

        try:
            if week_calendar and cal_index < len(week_calendar):
                _, _, week_start_str, week_end_str = week_calendar[cal_index]
                cal_index += 1
            else:
                # 无日历参考，跳过此 bar
                bar_index += 1
                continue

            cursor.execute("""
                INSERT OR REPLACE INTO weekly_kline_data
                (stock_code, stock_name, week_start_date, week_end_date, open, high, low, close, volume, amount)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                code,
                stock_name,
                week_start_str,
                week_end_str,
                float(row.get('open', 0)) if not pd.isna(row.get('open')) else None,
                float(row.get('high', 0)) if not pd.isna(row.get('high')) else None,
                float(row.get('low', 0)) if not pd.isna(row.get('low')) else None,
                float(row.get('close', 0)) if not pd.isna(row.get('close')) else None,
                int(row.get('volume', 0)) if not pd.isna(row.get('volume')) else None,
                float(row.get('amount', 0)) if not pd.isna(row.get('amount')) else None
            ))
            saved += 1
            bar_index += 1
        except Exception:
            bar_index += 1
            continue

    return saved


def download_weekly_kline_sync(
    years: int = 10,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    broker_account: str = "",
    broker_password: str = "",
    batch_size: int = 50,
    market_filter: Optional[List[str]] = ['SH', 'SZ']
):
    """同步版本的周K线下载函数"""
    import asyncio

    async def _download():
        return await download_weekly_kline_data(
            years=years,
            start_date=start_date,
            end_date=end_date,
            broker_account=broker_account,
            broker_password=broker_password,
            batch_size=batch_size,
            market_filter=market_filter
        )

    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop.run_until_complete(_download())
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