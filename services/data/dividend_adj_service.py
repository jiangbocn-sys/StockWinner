"""
分红数据转复权因子服务

使用 get_dividend 快速获取除权数据（0.3秒），替代 get_adj_factor（30秒超时）。

核心逻辑：
1. get_dividend 获取除权日期、送股率、现金分红
2. 计算单次复权因子：
   - 送股：复权因子 = 1 + 送股率
   - 现金分红：复权因子 ≈ 1（价格调整在查询时处理）
3. 计算累计复权因子（按时间正向累积）
4. 写入 stock_adj_factor 表

执行方式：
- 盘前预热任务（8:50）更新活跃股票
- 交易时段零 SDK 开销
"""

import pandas as pd
from datetime import datetime
from typing import List, Dict, Optional
from pathlib import Path

from services.common.database import get_sync_connection
from services.common.structured_logger import get_logger
from services.common.timezone import get_china_time, format_china_time

log = get_logger("dividend_adj_service")


def get_active_stock_codes() -> List[str]:
    """获取活跃股票列表（持仓 + watchlist）"""
    codes = set()

    # 从 stockwinner.db 获取持仓和 watchlist
    try:
        conn = get_sync_connection("stockwinner")
        cursor = conn.cursor()

        # 持仓
        cursor.execute(
            "SELECT DISTINCT stock_code FROM stock_positions WHERE quantity > 0"
        )
        for row in cursor.fetchall():
            codes.add(row[0])

        # watchlist（活跃状态）
        cursor.execute(
            "SELECT DISTINCT stock_code FROM watchlist WHERE status IN ('pending', 'watching', 'bought')"
        )
        for row in cursor.fetchall():
            codes.add(row[0])

    except Exception as e:
        log.error("dividend_adj", f"获取活跃股票失败: {e}")

    return list(codes)


def fetch_dividend_data(stock_codes: List[str]) -> pd.DataFrame:
    """从 SDK 获取分红数据（0.3秒）"""
    if not stock_codes:
        return pd.DataFrame()

    try:
        from services.common.sdk_manager import get_sdk_manager

        sdk_mgr = get_sdk_manager()
        # 使用 InfoData.get_dividend
        df = sdk_mgr.get_dividend(stock_codes, timeout=30.0)

        if df is None or df.empty:
            return pd.DataFrame()

        return df

    except Exception as e:
        log.error("dividend_adj", f"获取分红数据失败: {e}")
        return pd.DataFrame()


def calculate_adj_factor_from_dividend(dividend_df: pd.DataFrame) -> List[Dict]:
    """从分红数据计算复权因子

    计算逻辑：
    - 送股（DVD_PER_SHARE_STK > 0）：复权因子 = 1 + 送股率
    - 现金分红：复权因子 ≈ 1.0（价格调整在 K 线查询时处理）

    Args:
        dividend_df: SDK 返回的分红数据

    Returns:
        [{stock_code, trade_date, adj_factor, source}, ...]
    """
    records = []

    if dividend_df is None or dividend_df.empty:
        return records

    # 过滤有效除权记录（DATE_EX 不为空）
    valid_df = dividend_df[dividend_df['DATE_EX'].notna()].copy()

    for _, row in valid_df.iterrows():
        stock_code = row.get('MARKET_CODE', '')
        date_ex = str(row.get('DATE_EX', ''))

        if not stock_code or not date_ex or len(date_ex) < 8:
            continue

        # 格式化日期
        trade_date = f"{date_ex[:4]}-{date_ex[4:6]}-{date_ex[6:8]}"

        # 送股率（每股送股数）
        stk_rate = float(row.get('DVD_PER_SHARE_STK', 0) or 0)

        # 计算复权因子
        # 送股：复权因子 = 1 + 送股率
        # 现金分红：复权因子保持 1.0（价格调整在前复权计算时处理）
        adj_factor = 1.0 + stk_rate

        # 如果只有现金分红（无送股），复权因子为 1.0
        # 但仍然记录除权日期，用于 K 线价格调整
        cash_div = float(row.get('DVD_PER_SHARE_AFTER_TAX_CASH', 0) or 0)

        # 只记录有实际除权的记录（送股或现金分红 > 0）
        if adj_factor > 1.0 or cash_div > 0:
            records.append({
                'stock_code': stock_code,
                'trade_date': trade_date,
                'adj_factor': adj_factor,
                'cash_dividend': cash_div,  # 记录现金分红用于价格调整
                'source': 'dividend',
            })

    return records


def save_adj_factor_records(records: List[Dict]) -> int:
    """保存复权因子到数据库

    策略：
    - INSERT OR REPLACE 覆盖已有记录
    - 计算累计复权因子（按时间正向累积）

    Args:
        records: 复权因子记录列表

    Returns:
        保存的记录数
    """
    if not records:
        return 0

    conn = get_sync_connection("kline")
    cursor = conn.cursor()
    now = format_china_time()

    saved = 0

    # 按股票分组，计算累计因子
    from collections import defaultdict
    stock_records = defaultdict(list)
    for r in records:
        stock_records[r['stock_code']].append(r)

    for stock_code, recs in stock_records.items():
        # 按日期排序（正向，计算累计因子）
        recs.sort(key=lambda x: x['trade_date'])

        cumulative = 1.0
        for rec in recs:
            # 累积复权因子
            cumulative *= rec['adj_factor']

            try:
                cursor.execute("""
                    INSERT OR REPLACE INTO stock_adj_factor
                    (stock_code, trade_date, adj_factor, cumulative_factor, source, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    stock_code,
                    rec['trade_date'],
                    rec['adj_factor'],
                    round(cumulative, 6),
                    rec['source'],
                    now
                ))
                saved += 1
            except Exception as e:
                log.error("dividend_adj", f"保存复权因子失败: {stock_code} {rec['trade_date']}, {e}")

    conn.commit()
    log.info("dividend_adj", f"保存 {saved} 条复权因子记录")
    return saved


def update_adj_factor_from_dividend(stock_codes: List[str] = None) -> Dict:
    """从分红数据更新复权因子

    Args:
        stock_codes: 指定股票列表，None 表示更新所有活跃股票

    Returns:
        {'success': bool, 'stocks': int, 'saved': int, 'message': str}
    """
    if stock_codes is None:
        stock_codes = get_active_stock_codes()

    if not stock_codes:
        return {'success': True, 'stocks': 0, 'saved': 0, 'message': '无活跃股票'}

    log.info("dividend_adj", f"开始更新 {len(stock_codes)} 只股票的复权因子（分红数据）")

    # 1. 获取分红数据（0.3秒）
    dividend_df = fetch_dividend_data(stock_codes)

    if dividend_df.empty:
        log.warn("dividend_adj", "分红数据为空")
        return {'success': True, 'stocks': len(stock_codes), 'saved': 0, 'message': '无分红数据'}

    # 2. 计算复权因子
    records = calculate_adj_factor_from_dividend(dividend_df)

    # 3. 保存到数据库
    saved = save_adj_factor_records(records)

    return {
        'success': True,
        'stocks': len(stock_codes),
        'saved': saved,
        'message': f'成功更新 {len(stock_codes)} 只股票，保存 {saved} 条除权记录'
    }


def get_missing_adj_factor_stocks() -> List[str]:
    """获取缺失复权因子数据的活跃股票"""
    active_codes = set(get_active_stock_codes())

    if not active_codes:
        return []

    conn = get_sync_connection("kline")
    cursor = conn.cursor()

    cursor.execute(
        "SELECT DISTINCT stock_code FROM stock_adj_factor"
    )
    existing_codes = set(row[0] for row in cursor.fetchall())

    missing = [c for c in active_codes if c not in existing_codes]
    return missing