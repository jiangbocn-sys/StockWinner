"""
因子计算服务

每日/智能因子计算、空值填充

设计原则：
- 所有函数是独立可顺序调用的单元，适配 NightTaskQueue 链式执行
- 数据库写入使用 get_sync_connection("kline") (WAL + busy_timeout)
- 因子计算通过 factor_pipeline 统一管道
"""

import asyncio
import time
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional

from services.common.timezone import CHINA_TZ, get_china_time
from services.common.download_progress import get_progress_tracker, DownloadStatus
from services.common.structured_logger import get_logger
from services.common.database import get_sync_connection
from services.factors.factor_pipeline import calculate_technical_factors, add_signal_indicators


# ================================================================
# 每日因子计算
# ================================================================

def calculate_and_save_factors_for_dates(
    start_date: str,
    end_date: str,
    stock_codes: Optional[List[str]] = None,
    only_new_dates: bool = True,
    show_progress: bool = True,
    tracker=None  # 下载进度跟踪器
) -> int:
    """
    对指定日期范围内的新增 k 线数据计算并保存因子

    策略：
    1. 获取日期范围内有数据的所有股票
    2. 对于每只股票，只计算 stock_daily_factors 表中缺失的日期
    3. 批量读取每只股票的 k 线数据
    4. 通过 factor_pipeline 计算所有技术指标 + 信号指标
    5. 计算市值和上市天数
    6. 批量插入到 stock_daily_factors 表

    参数：
    - start_date: 开始日期 YYYY-MM-DD
    - end_date: 结束日期 YYYY-MM-DD
    - stock_codes: 股票代码列表，如不传则自动获取范围内所有股票
    - only_new_dates: 是否只计算新增日期（默认 True）
    - show_progress: 是否显示进度（默认 True）
    - tracker: 下载进度跟踪器（可选）

    返回：
    - 成功计算的记录数
    """
    log = get_logger("factor")

    conn = get_sync_connection("kline")
    cursor = conn.cursor()
    cursor.execute("PRAGMA synchronous=NORMAL")

    start_time = time.monotonic()

    # 获取日期范围内的所有股票
    if stock_codes is None:
        cursor.execute("""
            SELECT DISTINCT stock_code FROM kline_data
            WHERE trade_date >= ? AND trade_date <= ?
        """, (start_date, end_date))
        stock_codes = [row[0] for row in cursor.fetchall()]

    if not stock_codes:
        log.log_event("factor_calc_no_data", f"日期范围 [{start_date}, {end_date}] 内无数据",
                      start_date=start_date, end_date=end_date)
        return 0

    total_stocks = len(stock_codes)
    log.log_event("factor_calc_start", f"为 {total_stocks} 只股票计算因子 ({start_date} 至 {end_date})",
                  total_stocks=total_stocks, start_date=start_date, end_date=end_date)

    total_inserted = 0
    BATCH_SIZE = 50
    processed = 0

    if tracker:
        tracker.update_sync(total_tasks=total_stocks, message="开始计算因子...")

    for i in range(0, len(stock_codes), BATCH_SIZE):
        batch_stocks = stock_codes[i:i + BATCH_SIZE]
        processed += len(batch_stocks)

        if show_progress:
            progress_pct = (processed / total_stocks) * 100 if total_stocks > 0 else 0
            batch_num = i // BATCH_SIZE + 1
            num_batches = (total_stocks + BATCH_SIZE - 1) // BATCH_SIZE
            log.log_event("factor_batch_progress", f"因子计算进度 {progress_pct:.1f}%",
                          progress_pct=round(progress_pct, 1), batch_num=batch_num, num_batches=num_batches,
                          batch_size=len(batch_stocks), processed=processed, total_stocks=total_stocks)

        if tracker:
            tracker.update_sync(
                processed=processed,
                current_stock=batch_stocks[0] if batch_stocks else "",
                message=f"计算因子：批次 {batch_num}/{num_batches}"
            )

        for stock_code in batch_stocks:
            try:
                stock_name = ''
                cursor.execute(
                    "SELECT DISTINCT stock_name FROM kline_data WHERE stock_code = ? LIMIT 1",
                    (stock_code,)
                )
                row = cursor.fetchone()
                if row and row[0]:
                    stock_name = row[0]

                # 确定需要计算的日期范围
                if only_new_dates:
                    cursor.execute("""
                        SELECT MIN(trade_date), MAX(trade_date)
                        FROM stock_daily_factors
                        WHERE stock_code = ?
                    """, (stock_code,))
                    row = cursor.fetchone()

                    existing_min = row[0] if row and row[0] else None
                    existing_max = row[1] if row and row[1] else None

                    calc_start = start_date
                    calc_end = end_date

                    if existing_max:
                        calc_start_dt = datetime.strptime(existing_max, '%Y-%m-%d') + timedelta(days=1)
                        calc_end_dt = datetime.strptime(end_date, '%Y-%m-%d')
                        if calc_start_dt > calc_end_dt:
                            continue
                        calc_start = calc_start_dt.strftime('%Y-%m-%d')
                    elif existing_min:
                        calc_end_dt = datetime.strptime(existing_min, '%Y-%m-%d') - timedelta(days=1)
                        calc_start_dt = datetime.strptime(start_date, '%Y-%m-%d')
                        if calc_end_dt < calc_start_dt:
                            continue
                        calc_end = calc_end_dt.strftime('%Y-%m-%d')

                # 获取K线数据（前溯120天）
                kline_query_start = calc_start if only_new_dates else start_date
                if only_new_dates:
                    query_start_dt = datetime.strptime(kline_query_start, '%Y-%m-%d') - timedelta(days=120)
                    cursor.execute("SELECT MIN(trade_date) FROM kline_data WHERE stock_code = ?", (stock_code,))
                    min_kline = cursor.fetchone()[0]
                    if min_kline:
                        kline_query_start = max(min_kline, query_start_dt.strftime('%Y-%m-%d'))

                cursor.execute("""
                    SELECT trade_date, open, high, low, close, volume, amount
                    FROM kline_data
                    WHERE stock_code = ? AND trade_date >= ? AND trade_date <= ?
                    ORDER BY trade_date
                """, (stock_code, kline_query_start, calc_end if only_new_dates else end_date))
                kline_rows = cursor.fetchall()

                if not kline_rows:
                    continue

                df = pd.DataFrame(kline_rows, columns=['trade_date', 'open', 'high', 'low', 'close', 'volume', 'amount'])
                df['stock_code'] = stock_code
                df['change_pct'] = df['close'].pct_change()

                if only_new_dates and i == 0 and stock_code == batch_stocks[0]:
                    log.debug("factor", f"{stock_code}: calc={calc_start}~{calc_end}, K线={df['trade_date'].min()}~{df['trade_date'].max()}",
                              context={"stock_code": stock_code, "calc_start": calc_start, "calc_end": calc_end})

                df = calculate_technical_factors(df)
                df = add_signal_indicators(df)

                # 获取股本数据
                float_share = None
                total_share = None
                try:
                    cursor.execute("SELECT float_share, total_share FROM stock_base_info WHERE stock_code = ?", (stock_code,))
                    result = cursor.fetchone()
                    if result:
                        float_share = result[0]
                        total_share = result[1]
                except:
                    pass

                # 获取上市日期
                ipo_date = None
                try:
                    cursor.execute("SELECT list_date FROM stock_base_info WHERE stock_code = ?", (stock_code,))
                    result = cursor.fetchone()
                    if result and result[0]:
                        list_val = result[0]
                        if isinstance(list_val, int) and list_val > 19000000:
                            ipo_date = f"{list_val // 10000}-{(list_val % 10000) // 100:02d}-{list_val % 100:02d}"
                        else:
                            ipo_date = str(list_val)
                except:
                    pass

                # 批量插入
                insert_count = 0
                insert_start = calc_start if only_new_dates else start_date
                insert_end = calc_end if only_new_dates else end_date

                if show_progress and insert_count == 0:
                    log.debug("factor", f"{stock_code}: insert范围 {insert_start} ~ {insert_end}, df日期 {df['trade_date'].min()} ~ {df['trade_date'].max()}",
                              context={"stock_code": stock_code, "insert_start": insert_start, "insert_end": insert_end})

                def to_python_value(val):
                    if val is None:
                        return None
                    if isinstance(val, (int, float)):
                        return val
                    import numpy as np
                    if isinstance(val, np.integer):
                        return int(val)
                    if isinstance(val, np.floating):
                        return float(val)
                    return val

                for _, row in df.iterrows():
                    trade_date = row['trade_date']
                    if trade_date < insert_start or trade_date > insert_end:
                        continue

                    close_price = row['close']
                    circ_market_cap = None
                    total_market_cap = None
                    if close_price and close_price > 0:
                        if float_share and float_share > 0:
                            circ_market_cap = round(close_price * float_share / 10000, 2)
                        if total_share and total_share > 0:
                            total_market_cap = round(close_price * total_share / 10000, 2)

                    days_since_ipo = None
                    if ipo_date:
                        try:
                            ipo_dt = datetime.strptime(ipo_date, '%Y-%m-%d')
                            trade_dt = datetime.strptime(trade_date, '%Y-%m-%d')
                            days_since_ipo = (trade_dt - ipo_dt).days
                        except:
                            pass

                    try:
                        cursor.execute("""
                            INSERT OR REPLACE INTO stock_daily_factors (
                                stock_code, stock_name, trade_date,
                                circ_market_cap, total_market_cap, days_since_ipo,
                                change_5d, change_10d, change_20d,
                                bias_5, bias_10, bias_20,
                                amplitude_5, amplitude_10, amplitude_20,
                                change_std_5, change_std_10, change_std_20,
                                amount_std_5, amount_std_10, amount_std_20,
                                kdj_k, kdj_d, kdj_j,
                                dif, dea, macd,
                                ma5, ma10, ma20, ma60,
                                ema12, ema26, adx,
                                rsi_14, cci_20, atr_14,
                                boll_upper, boll_middle, boll_lower, hv_20,
                                obv, volume_ratio,
                                momentum_10d, momentum_20d,
                                golden_cross, death_cross,
                                limit_up_count_10d, limit_up_count_20d, limit_up_count_30d,
                                consecutive_limit_up,
                                large_gain_5d_count, large_loss_5d_count,
                                close_to_high_250d, close_to_low_250d,
                                gap_up_ratio,
                                next_period_change, is_traded,
                                source, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            stock_code,
                            stock_name,
                            trade_date,
                            circ_market_cap,
                            total_market_cap,
                            days_since_ipo,
                            to_python_value(row.get('change_5d')),
                            to_python_value(row.get('change_10d')),
                            to_python_value(row.get('change_20d')),
                            to_python_value(row.get('bias_5')),
                            to_python_value(row.get('bias_10')),
                            to_python_value(row.get('bias_20')),
                            to_python_value(row.get('amplitude_5')),
                            to_python_value(row.get('amplitude_10')),
                            to_python_value(row.get('amplitude_20')),
                            to_python_value(row.get('change_std_5')),
                            to_python_value(row.get('change_std_10')),
                            to_python_value(row.get('change_std_20')),
                            to_python_value(row.get('amount_std_5')),
                            to_python_value(row.get('amount_std_10')),
                            to_python_value(row.get('amount_std_20')),
                            to_python_value(row.get('kdj_k')),
                            to_python_value(row.get('kdj_d')),
                            to_python_value(row.get('kdj_j')),
                            to_python_value(row.get('dif')),
                            to_python_value(row.get('dea')),
                            to_python_value(row.get('macd')),
                            to_python_value(row.get('ma5')),
                            to_python_value(row.get('ma10')),
                            to_python_value(row.get('ma20')),
                            to_python_value(row.get('ma60')),
                            to_python_value(row.get('ema12')),
                            to_python_value(row.get('ema26')),
                            to_python_value(row.get('adx')),
                            to_python_value(row.get('rsi_14')),
                            to_python_value(row.get('cci_20')),
                            to_python_value(row.get('atr_14')),
                            to_python_value(row.get('boll_upper')),
                            to_python_value(row.get('boll_middle')),
                            to_python_value(row.get('boll_lower')),
                            to_python_value(row.get('hv_20')),
                            to_python_value(row.get('obv')),
                            to_python_value(row.get('volume_ratio')),
                            to_python_value(row.get('momentum_10d')),
                            to_python_value(row.get('momentum_20d')),
                            to_python_value(row.get('golden_cross')),
                            to_python_value(row.get('death_cross')),
                            to_python_value(row.get('limit_up_count_10d')),
                            to_python_value(row.get('limit_up_count_20d')),
                            to_python_value(row.get('limit_up_count_30d')),
                            to_python_value(row.get('consecutive_limit_up')),
                            to_python_value(row.get('large_gain_5d_count')),
                            to_python_value(row.get('large_loss_5d_count')),
                            to_python_value(row.get('close_to_high_250d')),
                            to_python_value(row.get('close_to_low_250d')),
                            to_python_value(row.get('gap_up_ratio')),
                            to_python_value(row.get('next_period_change')),
                            to_python_value(row.get('is_traded')),
                            'auto_calculated',
                            datetime.now(CHINA_TZ).isoformat(),
                            datetime.now(CHINA_TZ).isoformat()
                        ))
                        insert_count += 1
                    except Exception as e:
                        log.error("factor", f"插入 {stock_code} {trade_date} 失败: {e}",
                                  context={"stock_code": stock_code, "trade_date": trade_date, "error": str(e)})
                        continue

                if insert_count > 0:
                    total_inserted += insert_count
                    if show_progress:
                        log.log_event("factor_insert", f"{stock_code} 插入 {insert_count} 条记录",
                                      stock_code=stock_code, stock_name=stock_name, insert_count=insert_count)

                if (processed % BATCH_SIZE == 0) and total_inserted > 0:
                    conn.commit()

            except Exception as e:
                log.error("factor", f"{stock_code} 处理失败: {e}",
                          context={"stock_code": stock_code, "error": str(e)})
                continue

    if total_inserted > 0:
        conn.commit()

    duration_ms = (time.monotonic() - start_time) * 1000
    log.log_event("factor_calc_complete", f"因子计算完成，总计插入 {total_inserted} 条记录",
                  total_inserted=total_inserted)
    log.log_duration("factor_calc_total", duration_ms,
                     total_stocks=total_stocks, total_inserted=total_inserted,
                     start_date=start_date, end_date=end_date)

    if tracker:
        tracker.complete_sync()

    return total_inserted


async def calculate_and_save_factors_for_dates_async(
    start_date: str,
    end_date: str,
    stock_codes: Optional[List[str]] = None,
    only_new_dates: bool = True,
    show_progress: bool = True
) -> int:
    """异步版本：对指定日期范围内的新增 k 线数据计算并保存因子"""
    tracker = get_progress_tracker()

    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(
        None,
        lambda: calculate_and_save_factors_for_dates(
            start_date, end_date, stock_codes, only_new_dates, show_progress, tracker
        )
    )
    return result


# ================================================================
# 空值因子填充
# ================================================================

def fill_empty_factor_values(
    start_date: str,
    end_date: str,
    stock_codes: Optional[List[str]] = None,
    show_progress: bool = True,
    tracker: Optional['DownloadProgressTracker'] = None
) -> int:
    """
    填充因子数据：处理缺失记录 + 更新空值字段

    Step 1: 调用 calculate_and_save_factors_for_dates 处理缺失记录
    Step 2: 更新已有记录中的空值字段
    """
    log = get_logger("factor")

    start_time = time.monotonic()

    log.log_event("fill_empty_start", f"开始填充因子数据，日期范围：{start_date} ~ {end_date}",
                  start_date=start_date, end_date=end_date)

    # Step 1: 处理缺失记录
    log.log_event("fill_empty_step1", "Step 1: 检查缺失因子记录...")

    conn_check = get_sync_connection("kline")
    cursor_check = conn_check.cursor()

    if stock_codes is None:
        cursor_check.execute("""
            SELECT DISTINCT k.stock_code
            FROM kline_data k
            LEFT JOIN stock_daily_factors f ON k.stock_code = f.stock_code AND k.trade_date = f.trade_date
            WHERE k.trade_date >= ? AND k.trade_date <= ? AND f.stock_code IS NULL
        """, (start_date, end_date))
        missing_stock_codes = [row[0] for row in cursor_check.fetchall()]
    else:
        placeholders = ','.join(['?' for _ in stock_codes])
        cursor_check.execute(f"""
            SELECT DISTINCT k.stock_code
            FROM kline_data k
            LEFT JOIN stock_daily_factors f ON k.stock_code = f.stock_code AND k.trade_date = f.trade_date
            WHERE k.trade_date >= ? AND k.trade_date <= ? AND f.stock_code IS NULL
            AND k.stock_code IN ({placeholders})
        """, [start_date, end_date] + stock_codes)
        missing_stock_codes = [row[0] for row in cursor_check.fetchall()]

    missing_count = 0
    if missing_stock_codes:
        log.log_event("fill_empty_missing_found", f"发现 {len(missing_stock_codes)} 只股票有缺失因子记录",
                      missing_count=len(missing_stock_codes))
        missing_count = calculate_and_save_factors_for_dates(
            start_date=start_date,
            end_date=end_date,
            stock_codes=missing_stock_codes,
            only_new_dates=False,
            show_progress=show_progress,
            tracker=tracker
        )
    else:
        log.log_event("fill_empty_no_missing", "无缺失因子记录，跳过Step 1")

    log.log_event("fill_empty_step1_done", f"Step 1 完成: 插入 {missing_count} 条缺失记录",
                  missing_count=missing_count)

    # Step 2: 更新空值记录
    log.log_event("fill_empty_step2", "Step 2: 更新空值记录...")

    conn = get_sync_connection("kline")
    cursor = conn.cursor()
    cursor.execute("PRAGMA synchronous=NORMAL")

    # 检测所有因子字段的空值（与 INSERT 语句中的字段保持一致）
    ALL_FACTOR_FIELDS = [
        'ma5', 'ma10', 'ma20', 'ma60', 'ema12', 'ema26',
        'kdj_k', 'kdj_d', 'kdj_j', 'dif', 'dea', 'macd',
        'rsi_14', 'cci_20', 'adx', 'atr_14',
        'boll_upper', 'boll_middle', 'boll_lower',
        'hv_20', 'obv', 'volume_ratio',
        'change_5d', 'change_10d', 'change_20d',
        'bias_5', 'bias_10', 'bias_20',
        'amplitude_5', 'amplitude_10', 'amplitude_20',
        'change_std_5', 'change_std_10', 'change_std_20',
        'amount_std_5', 'amount_std_10', 'amount_std_20',
        'momentum_10d', 'momentum_20d',
        'golden_cross', 'death_cross',
        'limit_up_count_10d', 'limit_up_count_20d', 'limit_up_count_30d',
        'consecutive_limit_up',
        'large_gain_5d_count', 'large_loss_5d_count',
        'close_to_high_250d', 'close_to_low_250d',
        'gap_up_ratio', 'next_period_change', 'is_traded'
    ]
    null_condition = " OR ".join([f"{f} IS NULL" for f in ALL_FACTOR_FIELDS])

    if stock_codes is None:
        cursor.execute(f"""
            SELECT DISTINCT stock_code FROM stock_daily_factors
            WHERE trade_date >= ? AND trade_date <= ?
            AND ({null_condition})
        """, (start_date, end_date))
        stock_codes = [row[0] for row in cursor.fetchall()]

    total_stocks = len(stock_codes)
    log.log_event("fill_empty_stocks", f"需处理空值：{total_stocks} 只股票",
                  total_stocks=total_stocks)

    if total_stocks == 0:
        log.log_event("fill_empty_nothing", "无需处理的空值因子")
        duration_ms = (time.monotonic() - start_time) * 1000
        log.log_duration("fill_empty_total", duration_ms,
                         missing_count=missing_count, total_updated=0, total=missing_count)
        return missing_count

    total_updated = 0
    BATCH_SIZE = 20

    for i in range(0, total_stocks, BATCH_SIZE):
        batch_stocks = stock_codes[i:i + BATCH_SIZE]
        processed = min(i + BATCH_SIZE, total_stocks)

        if show_progress:
            progress_pct = (processed / total_stocks) * 100
            log.log_event("fill_empty_progress", f"填充进度 {progress_pct:.1f}%",
                          progress_pct=round(progress_pct, 1), batch_size=len(batch_stocks),
                          processed=processed, total_stocks=total_stocks)

        for stock_code in batch_stocks:
            try:
                cursor.execute(f"""
                    SELECT trade_date FROM stock_daily_factors
                    WHERE stock_code = ? AND trade_date >= ? AND trade_date <= ?
                    AND ({null_condition})
                """, (stock_code, start_date, end_date))
                null_dates = [row[0] for row in cursor.fetchall()]

                if not null_dates:
                    continue

                earliest_date = min(null_dates)
                cursor.execute("SELECT MIN(trade_date) FROM kline_data WHERE stock_code = ?", (stock_code,))
                kline_start = cursor.fetchone()[0] or earliest_date

                calc_start_dt = datetime.strptime(earliest_date, '%Y-%m-%d') - timedelta(days=120)
                calc_start = max(kline_start, calc_start_dt.strftime('%Y-%m-%d'))

                cursor.execute("""
                    SELECT trade_date, open, high, low, close, volume, amount
                    FROM kline_data WHERE stock_code = ? AND trade_date >= ? AND trade_date <= ?
                    ORDER BY trade_date
                """, (stock_code, calc_start, end_date))
                kline_rows = cursor.fetchall()

                if not kline_rows:
                    continue

                df = pd.DataFrame(kline_rows, columns=['trade_date', 'open', 'high', 'low', 'close', 'volume', 'amount'])
                df['stock_code'] = stock_code
                df['change_pct'] = df['close'].pct_change()

                df = calculate_technical_factors(df)

                factor_fields = [
                    'ma5', 'ma10', 'ma20', 'ma60', 'ema12', 'ema26',
                    'kdj_k', 'kdj_d', 'kdj_j', 'dif', 'dea', 'macd',
                    'rsi_14', 'cci_20', 'adx', 'atr_14',
                    'boll_upper', 'boll_middle', 'boll_lower',
                    'hv_20', 'obv', 'volume_ratio',
                    'change_5d', 'change_10d', 'change_20d',
                    'bias_5', 'bias_10', 'bias_20',
                    'amplitude_5', 'amplitude_10', 'amplitude_20',
                    'change_std_5', 'change_std_10', 'change_std_20',
                    'amount_std_5', 'amount_std_10', 'amount_std_20',
                    'momentum_10d', 'momentum_20d',
                    'golden_cross', 'death_cross',
                    'limit_up_count_10d', 'limit_up_count_20d', 'limit_up_count_30d',
                    'consecutive_limit_up',
                    'large_gain_5d_count', 'large_loss_5d_count',
                    'close_to_high_250d', 'close_to_low_250d',
                    'gap_up_ratio', 'next_period_change', 'is_traded'
                ]

                update_count = 0
                for trade_date in null_dates:
                    row_data = df[df['trade_date'] == trade_date]
                    if row_data.empty:
                        continue
                    row = row_data.iloc[0]

                    update_fields = []
                    update_values = []
                    for field in factor_fields:
                        if field in row and pd.notna(row.get(field)):
                            update_fields.append(f"{field} = ?")
                            update_values.append(row.get(field))

                    if update_fields:
                        update_values.extend(['fill_empty_update', datetime.now(CHINA_TZ).isoformat(), stock_code, trade_date])
                        sql = f"UPDATE stock_daily_factors SET {', '.join(update_fields)}, source = ?, updated_at = ? WHERE stock_code = ? AND trade_date = ?"
                        cursor.execute(sql, update_values)
                        update_count += 1

                if update_count > 0:
                    total_updated += update_count
                    if show_progress:
                        log.log_event("fill_empty_update", f"{stock_code} 更新 {update_count} 条空值记录",
                                      stock_code=stock_code, update_count=update_count)

                if processed % BATCH_SIZE == 0:
                    conn.commit()

            except Exception as e:
                log.error("factor", f"{stock_code} 处理失败: {e}",
                          context={"stock_code": stock_code, "error": str(e)})
                continue

    conn.commit()

    if tracker:
        tracker.complete_sync()

    duration_ms = (time.monotonic() - start_time) * 1000
    total = missing_count + total_updated
    log.log_event("fill_empty_complete", f"填充完成，缺失记录: {missing_count} 条，空值更新: {total_updated} 条，总计: {total} 条",
                  missing_count=missing_count, total_updated=total_updated, total=total)
    log.log_duration("fill_empty_total", duration_ms,
                     missing_count=missing_count, total_updated=total_updated, total=total,
                     start_date=start_date, end_date=end_date)

    return missing_count + total_updated


# ================================================================
# 智能因子更新
# ================================================================

def smart_update_factors(
    start_date: str,
    end_date: str,
    stock_codes: Optional[List[str]] = None,
    show_progress: bool = True,
    tracker=None
) -> dict:
    """
    智能更新因子：只处理缺失记录和空值字段

    相比原 fill_empty 模式的改进：
    - 不触发全量重算，只计算缺失/空值的日期
    - 批量检测缺失日期，避免逐只股票查询
    - 合并 INSERT 和 UPDATE 操作
    """
    def to_python_value(val):
        if val is None:
            return None
        if isinstance(val, (int, float)):
            return val
        import numpy as np
        if isinstance(val, np.integer):
            return int(val)
        if isinstance(val, np.floating):
            return float(val)
        return val

    log = get_logger("factor")

    start_time = time.monotonic()

    log.log_event("smart_update_start", f"开始智能因子更新，日期范围：{start_date} ~ {end_date}",
                  start_date=start_date, end_date=end_date)

    conn = get_sync_connection("kline")
    cursor = conn.cursor()
    cursor.execute("PRAGMA synchronous=NORMAL")

    # ========== Step 1: 批量检测缺失/空值日期 ==========
    log.log_event("smart_update_step1", "Step 1: 批量检测缺失/空值日期...")

    if stock_codes is None:
        cursor.execute("""
            SELECT k.stock_code, k.trade_date
            FROM kline_data k
            LEFT JOIN stock_daily_factors f ON k.stock_code = f.stock_code AND k.trade_date = f.trade_date
            WHERE k.trade_date >= ? AND k.trade_date <= ? AND f.stock_code IS NULL
        """, (start_date, end_date))
    else:
        placeholders = ','.join(['?' for _ in stock_codes])
        cursor.execute(f"""
            SELECT k.stock_code, k.trade_date
            FROM kline_data k
            LEFT JOIN stock_daily_factors f ON k.stock_code = f.stock_code AND k.trade_date = f.trade_date
            WHERE k.trade_date >= ? AND k.trade_date <= ? AND f.stock_code IS NULL
            AND k.stock_code IN ({placeholders})
        """, [start_date, end_date] + stock_codes)

    missing_records = cursor.fetchall()

    # 检测所有因子字段的空值（与 INSERT 语句中的字段保持一致）
    ALL_FACTOR_FIELDS = [
        'ma5', 'ma10', 'ma20', 'ma60', 'ema12', 'ema26',
        'kdj_k', 'kdj_d', 'kdj_j', 'dif', 'dea', 'macd',
        'rsi_14', 'cci_20', 'adx', 'atr_14',
        'boll_upper', 'boll_middle', 'boll_lower',
        'hv_20', 'obv', 'volume_ratio',
        'change_5d', 'change_10d', 'change_20d',
        'bias_5', 'bias_10', 'bias_20',
        'amplitude_5', 'amplitude_10', 'amplitude_20',
        'change_std_5', 'change_std_10', 'change_std_20',
        'amount_std_5', 'amount_std_10', 'amount_std_20',
        'momentum_10d', 'momentum_20d',
        'golden_cross', 'death_cross',
        'limit_up_count_10d', 'limit_up_count_20d', 'limit_up_count_30d',
        'consecutive_limit_up',
        'large_gain_5d_count', 'large_loss_5d_count',
        'close_to_high_250d', 'close_to_low_250d',
        'gap_up_ratio', 'next_period_change', 'is_traded'
    ]
    null_condition = " OR ".join([f"{f} IS NULL" for f in ALL_FACTOR_FIELDS])

    if stock_codes is None:
        cursor.execute(f"""
            SELECT stock_code, trade_date
            FROM stock_daily_factors
            WHERE trade_date >= ? AND trade_date <= ?
            AND ({null_condition})
        """, (start_date, end_date))
    else:
        cursor.execute(f"""
            SELECT stock_code, trade_date
            FROM stock_daily_factors
            WHERE trade_date >= ? AND trade_date <= ?
            AND ({null_condition})
            AND stock_code IN ({placeholders})
        """, [start_date, end_date] + stock_codes)

    null_records = cursor.fetchall()

    missing_by_stock: Dict[str, List[str]] = {}
    null_by_stock: Dict[str, List[str]] = {}

    for stock_code, trade_date in missing_records:
        if stock_code not in missing_by_stock:
            missing_by_stock[stock_code] = []
        missing_by_stock[stock_code].append(trade_date)

    for stock_code, trade_date in null_records:
        if stock_code not in null_by_stock:
            null_by_stock[stock_code] = []
        null_by_stock[stock_code].append(trade_date)

    all_stocks = set(missing_by_stock.keys()) | set(null_by_stock.keys())

    total_stocks = len(all_stocks)
    log.log_event("smart_update_discovery", f"发现 {len(missing_records)} 条缺失记录，{len(null_records)} 条空值记录，涉及 {total_stocks} 只股票",
                  missing_records=len(missing_records), null_records=len(null_records), total_stocks=total_stocks)

    if total_stocks == 0:
        log.log_event("smart_update_nothing", "无需处理，直接返回")
        duration_ms = (time.monotonic() - start_time) * 1000
        log.log_duration("smart_update_total", duration_ms, inserted=0, updated=0, skipped=0)
        return {'inserted': 0, 'updated': 0, 'skipped': 0}

    # ========== Step 2: 计算因子 ==========
    log.log_event("smart_update_step2", "Step 2: 计算因子...")

    total_inserted = 0
    total_updated = 0

    BATCH_SIZE = 50
    all_stocks_list = list(all_stocks)

    if tracker:
        tracker.start(total_stocks=total_stocks, total_batches=(total_stocks + BATCH_SIZE - 1) // BATCH_SIZE)

    for batch_idx in range(0, total_stocks, BATCH_SIZE):
        batch_stocks = all_stocks_list[batch_idx:batch_idx + BATCH_SIZE]
        processed = min(batch_idx + BATCH_SIZE, total_stocks)

        if show_progress:
            progress_pct = (processed / total_stocks) * 100
            log.log_event("smart_update_progress", f"智能更新进度 {progress_pct:.1f}%",
                          progress_pct=round(progress_pct, 1), batch_size=len(batch_stocks),
                          processed=processed, total_stocks=total_stocks)

        if tracker:
            tracker.update_sync(processed=processed, message=f"计算因子：批次 {batch_idx//BATCH_SIZE + 1}")

        for stock_code in batch_stocks:
            try:
                stock_name = ''
                cursor.execute(
                    "SELECT DISTINCT stock_name FROM kline_data WHERE stock_code = ? LIMIT 1",
                    (stock_code,)
                )
                row = cursor.fetchone()
                if row and row[0]:
                    stock_name = row[0]

                missing_dates = missing_by_stock.get(stock_code, [])
                null_dates = null_by_stock.get(stock_code, [])
                all_dates = set(missing_dates) | set(null_dates)

                if not all_dates:
                    continue

                earliest_date = min(all_dates)
                query_start_dt = datetime.strptime(earliest_date, '%Y-%m-%d') - timedelta(days=120)
                cursor.execute("SELECT MIN(trade_date) FROM kline_data WHERE stock_code = ?", (stock_code,))
                kline_start = cursor.fetchone()[0] or earliest_date
                calc_start = max(kline_start, query_start_dt.strftime('%Y-%m-%d'))

                cursor.execute("""
                    SELECT trade_date, open, high, low, close, volume, amount
                    FROM kline_data WHERE stock_code = ? AND trade_date >= ? AND trade_date <= ?
                    ORDER BY trade_date
                """, (stock_code, calc_start, end_date))
                kline_rows = cursor.fetchall()

                if not kline_rows:
                    continue

                df = pd.DataFrame(kline_rows, columns=['trade_date', 'open', 'high', 'low', 'close', 'volume', 'amount'])
                df['stock_code'] = stock_code
                df['change_pct'] = df['close'].pct_change()

                df = calculate_technical_factors(df)
                df = add_signal_indicators(df)

                float_share = None
                total_share = None
                ipo_date = None
                try:
                    cursor.execute("SELECT float_share, total_share, list_date FROM stock_base_info WHERE stock_code = ?", (stock_code,))
                    result = cursor.fetchone()
                    if result:
                        float_share = result[0]
                        total_share = result[1]
                        list_val = result[2]
                        if list_val:
                            if isinstance(list_val, int) and list_val > 19000000:
                                ipo_date = f"{list_val // 10000}-{(list_val % 10000) // 100:02d}-{list_val % 100:02d}"
                            else:
                                ipo_date = str(list_val)
                except:
                    pass

                # 处理缺失记录（INSERT）
                for trade_date in missing_dates:
                    row_data = df[df['trade_date'] == trade_date]
                    if row_data.empty:
                        continue
                    row = row_data.iloc[0]

                    close_price = row['close']
                    circ_market_cap = None
                    total_market_cap = None
                    if close_price and close_price > 0:
                        if float_share and float_share > 0:
                            circ_market_cap = round(close_price * float_share / 10000, 2)
                        if total_share and total_share > 0:
                            total_market_cap = round(close_price * total_share / 10000, 2)

                    days_since_ipo = None
                    if ipo_date:
                        try:
                            ipo_dt = datetime.strptime(ipo_date, '%Y-%m-%d')
                            trade_dt = datetime.strptime(trade_date, '%Y-%m-%d')
                            days_since_ipo = (trade_dt - ipo_dt).days
                        except:
                            pass

                    try:
                        cursor.execute("""
                            INSERT INTO stock_daily_factors (
                                stock_code, stock_name, trade_date,
                                circ_market_cap, total_market_cap, days_since_ipo,
                                change_5d, change_10d, change_20d,
                                bias_5, bias_10, bias_20,
                                amplitude_5, amplitude_10, amplitude_20,
                                change_std_5, change_std_10, change_std_20,
                                amount_std_5, amount_std_10, amount_std_20,
                                kdj_k, kdj_d, kdj_j,
                                dif, dea, macd,
                                ma5, ma10, ma20, ma60,
                                ema12, ema26, adx,
                                rsi_14, cci_20, atr_14,
                                boll_upper, boll_middle, boll_lower, hv_20,
                                obv, volume_ratio,
                                momentum_10d, momentum_20d,
                                golden_cross, death_cross,
                                limit_up_count_10d, limit_up_count_20d, limit_up_count_30d,
                                consecutive_limit_up,
                                large_gain_5d_count, large_loss_5d_count,
                                close_to_high_250d, close_to_low_250d,
                                gap_up_ratio,
                                next_period_change, is_traded,
                                source, created_at, updated_at
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            stock_code, stock_name, trade_date,
                            circ_market_cap, total_market_cap, days_since_ipo,
                            to_python_value(row.get('change_5d')),
                            to_python_value(row.get('change_10d')),
                            to_python_value(row.get('change_20d')),
                            to_python_value(row.get('bias_5')),
                            to_python_value(row.get('bias_10')),
                            to_python_value(row.get('bias_20')),
                            to_python_value(row.get('amplitude_5')),
                            to_python_value(row.get('amplitude_10')),
                            to_python_value(row.get('amplitude_20')),
                            to_python_value(row.get('change_std_5')),
                            to_python_value(row.get('change_std_10')),
                            to_python_value(row.get('change_std_20')),
                            to_python_value(row.get('amount_std_5')),
                            to_python_value(row.get('amount_std_10')),
                            to_python_value(row.get('amount_std_20')),
                            to_python_value(row.get('kdj_k')),
                            to_python_value(row.get('kdj_d')),
                            to_python_value(row.get('kdj_j')),
                            to_python_value(row.get('dif')),
                            to_python_value(row.get('dea')),
                            to_python_value(row.get('macd')),
                            to_python_value(row.get('ma5')),
                            to_python_value(row.get('ma10')),
                            to_python_value(row.get('ma20')),
                            to_python_value(row.get('ma60')),
                            to_python_value(row.get('ema12')),
                            to_python_value(row.get('ema26')),
                            to_python_value(row.get('adx')),
                            to_python_value(row.get('rsi_14')),
                            to_python_value(row.get('cci_20')),
                            to_python_value(row.get('atr_14')),
                            to_python_value(row.get('boll_upper')),
                            to_python_value(row.get('boll_middle')),
                            to_python_value(row.get('boll_lower')),
                            to_python_value(row.get('hv_20')),
                            to_python_value(row.get('obv')),
                            to_python_value(row.get('volume_ratio')),
                            to_python_value(row.get('momentum_10d')),
                            to_python_value(row.get('momentum_20d')),
                            to_python_value(row.get('golden_cross')),
                            to_python_value(row.get('death_cross')),
                            to_python_value(row.get('limit_up_count_10d')),
                            to_python_value(row.get('limit_up_count_20d')),
                            to_python_value(row.get('limit_up_count_30d')),
                            to_python_value(row.get('consecutive_limit_up')),
                            to_python_value(row.get('large_gain_5d_count')),
                            to_python_value(row.get('large_loss_5d_count')),
                            to_python_value(row.get('close_to_high_250d')),
                            to_python_value(row.get('close_to_low_250d')),
                            to_python_value(row.get('gap_up_ratio')),
                            to_python_value(row.get('next_period_change')),
                            to_python_value(row.get('is_traded')),
                            'smart_update',
                            datetime.now(CHINA_TZ).isoformat(),
                            datetime.now(CHINA_TZ).isoformat()
                        ))
                        total_inserted += 1
                    except Exception as e:
                        log.error("factor", f"INSERT {stock_code} {trade_date} 失败: {e}",
                                  context={"stock_code": stock_code, "trade_date": trade_date, "error": str(e)})
                        continue

                # 处理空值记录（UPDATE）
                for trade_date in null_dates:
                    row_data = df[df['trade_date'] == trade_date]
                    if row_data.empty:
                        continue
                    row = row_data.iloc[0]

                    factor_fields = [
                        'ma5', 'ma10', 'ma20', 'ma60', 'ema12', 'ema26',
                        'kdj_k', 'kdj_d', 'kdj_j', 'dif', 'dea', 'macd',
                        'rsi_14', 'cci_20', 'adx', 'atr_14',
                        'boll_upper', 'boll_middle', 'boll_lower',
                        'hv_20', 'obv', 'volume_ratio',
                        'change_5d', 'change_10d', 'change_20d',
                        'bias_5', 'bias_10', 'bias_20',
                        'amplitude_5', 'amplitude_10', 'amplitude_20',
                        'change_std_5', 'change_std_10', 'change_std_20',
                        'amount_std_5', 'amount_std_10', 'amount_std_20',
                        'momentum_10d', 'momentum_20d',
                        'golden_cross', 'death_cross',
                        'limit_up_count_10d', 'limit_up_count_20d', 'limit_up_count_30d',
                        'consecutive_limit_up',
                        'large_gain_5d_count', 'large_loss_5d_count',
                        'close_to_high_250d', 'close_to_low_250d',
                        'gap_up_ratio', 'next_period_change', 'is_traded'
                    ]

                    update_fields = []
                    update_values = []
                    for field in factor_fields:
                        if field in row and pd.notna(row.get(field)):
                            update_fields.append(f"{field} = ?")
                            update_values.append(to_python_value(row.get(field)))

                    if update_fields:
                        update_values.extend(['smart_update', datetime.now(CHINA_TZ).isoformat(), stock_code, trade_date])
                        try:
                            cursor.execute(f"""
                                UPDATE stock_daily_factors
                                SET {', '.join(update_fields)}, source = ?, updated_at = ?
                                WHERE stock_code = ? AND trade_date = ?
                            """, update_values)
                            total_updated += 1
                        except Exception as e:
                            log.error("factor", f"UPDATE {stock_code} {trade_date} 失败: {e}",
                                      context={"stock_code": stock_code, "trade_date": trade_date, "error": str(e)})
                            continue

                if processed % BATCH_SIZE == 0:
                    conn.commit()

            except Exception as e:
                log.error("factor", f"{stock_code} 处理失败: {e}",
                          context={"stock_code": stock_code, "error": str(e)})
                continue

    conn.commit()

    if tracker:
        tracker.complete_sync(inserted=total_inserted, updated=total_updated)

    result = {
        'inserted': total_inserted,
        'updated': total_updated,
        'skipped': 0
    }

    duration_ms = (time.monotonic() - start_time) * 1000
    total = total_inserted + total_updated
    log.log_event("smart_update_complete", f"智能因子更新完成，新增 {total_inserted} 条，更新 {total_updated} 条，总计 {total} 条",
                  inserted=total_inserted, updated=total_updated, total=total)
    log.log_duration("smart_update_total", duration_ms,
                     inserted=total_inserted, updated=total_updated, skipped=0,
                     start_date=start_date, end_date=end_date)

    return result
