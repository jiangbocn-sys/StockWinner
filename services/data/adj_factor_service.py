"""
复权因子数据库服务

管理 stock_adj_factor 表：
- 从 SDK 批量获取复权因子数据
- 保存到数据库（只保存除权记录，adj_factor != 1.0）
- 计算累计复权因子
- 新鲜度检测与自动更新
"""

import pandas as pd
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from pathlib import Path

from services.common.database import get_sync_connection, KLINE_DB_PATH
from services.common.structured_logger import get_logger
from services.common.timezone import get_china_time

log = get_logger("adj_factor_service")


def init_adj_factor_table():
    """初始化 stock_adj_factor 表（若不存在）"""
    conn = get_sync_connection("kline")
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stock_adj_factor (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            stock_code TEXT NOT NULL,
            trade_date TEXT NOT NULL,
            adj_factor REAL NOT NULL DEFAULT 1.0,
            cumulative_factor REAL,
            source TEXT DEFAULT 'sdk',
            created_at TEXT,
            updated_at TEXT,
            UNIQUE(stock_code, trade_date)
        )
    """)

    # 索引
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_adj_factor_stock ON stock_adj_factor(stock_code)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_adj_factor_date ON stock_adj_factor(trade_date)")
    cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_adj_factor_stock_date ON stock_adj_factor(stock_code, trade_date)")

    conn.commit()
    log.log_event("adj_factor_table_init", "复权因子表初始化完成")


def save_adj_factor_batch(df: pd.DataFrame, source: str = 'sdk') -> int:
    """批量保存复权因子到数据库

    SDK 返回格式：索引=DatetimeIndex（日期），列=股票代码，值=单次复权因子
    策略：只保存除权记录（adj_factor != 1.0），减少数据量

    Args:
        df: SDK 返回的 DataFrame
        source: 数据来源标记

    Returns:
        保存的记录数
    """
    if df is None or df.empty:
        return 0

    conn = get_sync_connection("kline")
    records = []
    now = get_china_time().strftime('%Y-%m-%d %H:%M:%S')

    for stock_code in df.columns:
        col = df[stock_code]
        # 找所有非 1.0 的除权日期
        adj_dates = col[col != 1.0]

        for date_ts, adj_factor in adj_dates.items():
            trade_date = pd.Timestamp(date_ts).strftime('%Y-%m-%d')
            records.append((
                stock_code,
                trade_date,
                float(adj_factor),
                None,  # cumulative_factor 后续计算
                source,
                now,
                now
            ))

    if not records:
        conn.commit()
        return 0

    # 批量插入（UPSERT）
    cursor = conn.cursor()
    cursor.executemany("""
        INSERT INTO stock_adj_factor (stock_code, trade_date, adj_factor, cumulative_factor, source, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(stock_code, trade_date) DO UPDATE SET
            adj_factor = excluded.adj_factor,
            updated_at = excluded.updated_at
    """, records)
    conn.commit()

    # 计算累计因子
    _calculate_all_cumulative_factors(conn)

    log.log_event("adj_factor_saved", f"保存 {len(records)} 条复权因子记录", stocks=len(df.columns))
    return len(records)


def _calculate_all_cumulative_factors(conn):
    """计算所有股票的累计复权因子

    累计因子 = 从最早开始累乘单次因子
    用于前复权计算：前复权价格 = 原价 × (当日累计因子 / 最新累计因子)
    """
    cursor = conn.cursor()

    # 获取所有股票代码
    cursor.execute("SELECT DISTINCT stock_code FROM stock_adj_factor")
    codes = [row['stock_code'] for row in cursor.fetchall()]

    for stock_code in codes:
        # 获取该股票所有除权记录（按日期升序）
        cursor.execute("""
            SELECT trade_date, adj_factor FROM stock_adj_factor
            WHERE stock_code = ? ORDER BY trade_date ASC
        """, (stock_code,))
        records = cursor.fetchall()

        if not records:
            continue

        # 计算累计因子（从最早开始累乘）
        cumulative = 1.0
        updates = []
        for r in records:
            cumulative *= float(r['adj_factor'])
            updates.append((cumulative, stock_code, r['trade_date']))

        # 批量更新
        cursor.executemany("""
            UPDATE stock_adj_factor SET cumulative_factor = ?
            WHERE stock_code = ? AND trade_date = ?
        """, updates)

    conn.commit()


def get_adj_factor_for_stock(stock_code: str,
                              start_date: str = None,
                              end_date: str = None) -> List[Dict]:
    """获取单只股票的复权因子历史

    Args:
        stock_code: 股票代码
        start_date: 开始日期（可选）
        end_date: 结束日期（可选）

    Returns:
        [{'trade_date': 'YYYY-MM-DD', 'adj_factor': float, 'cumulative_factor': float}, ...]
    """
    conn = get_sync_connection("kline")
    cursor = conn.cursor()

    sql = "SELECT trade_date, adj_factor, cumulative_factor FROM stock_adj_factor WHERE stock_code = ?"
    params = [stock_code]

    if start_date:
        sql += " AND trade_date >= ?"
        params.append(start_date)
    if end_date:
        sql += " AND trade_date <= ?"
        params.append(end_date)

    sql += " ORDER BY trade_date ASC"

    cursor.execute(sql, params)
    rows = cursor.fetchall()

    return [{'trade_date': r['trade_date'],
             'adj_factor': r['adj_factor'],
             'cumulative_factor': r['cumulative_factor']} for r in rows]


def get_latest_cumulative_factor(stock_code: str) -> float:
    """获取某股票的最新累计复权因子

    用于前复权基准
    """
    conn = get_sync_connection("kline")
    cursor = conn.cursor()

    cursor.execute("""
        SELECT cumulative_factor FROM stock_adj_factor
        WHERE stock_code = ? AND cumulative_factor IS NOT NULL
        ORDER BY trade_date DESC LIMIT 1
    """, (stock_code,))
    row = cursor.fetchone()

    return float(row['cumulative_factor']) if row else 1.0


def get_adj_factor_batch(stock_codes: List[str], date: str = None) -> Dict[str, Dict]:
    """批量获取多只股票在某日期的复权因子

    Args:
        stock_codes: 股票代码列表
        date: 查询日期（可选，默认取最新）

    Returns:
        {stock_code: {'adj_factor': float, 'cumulative_factor': float, 'trade_date': str}, ...}
    """
    if not stock_codes:
        return {}

    conn = get_sync_connection("kline")
    cursor = conn.cursor()

    result = {}

    if date:
        # 查询指定日期
        placeholders = ','.join(['?' for _ in stock_codes])
        cursor.execute(f"""
            SELECT stock_code, trade_date, adj_factor, cumulative_factor FROM stock_adj_factor
            WHERE stock_code IN ({placeholders}) AND trade_date <= ?
            GROUP BY stock_code HAVING trade_date = MAX(trade_date)
        """, stock_codes + [date])
    else:
        # 查询最新
        placeholders = ','.join(['?' for _ in stock_codes])
        cursor.execute(f"""
            SELECT stock_code, trade_date, adj_factor, cumulative_factor FROM stock_adj_factor
            WHERE stock_code IN ({placeholders})
            GROUP BY stock_code HAVING trade_date = MAX(trade_date)
        """, stock_codes)

    for row in cursor.fetchall():
        result[row['stock_code']] = {
            'trade_date': row['trade_date'],
            'adj_factor': row['adj_factor'],
            'cumulative_factor': row['cumulative_factor']
        }

    return result


def check_freshness(stock_codes: List[str]) -> Tuple[List[str], Dict[str, str]]:
    """检查哪些股票的复权因子数据需要更新

    检测条件：
    1. 股票无数据 - 需要更新
    2. 当天未检查 - 需要检查（但只查一次）

    Args:
        stock_codes: 待检查的股票代码列表

    Returns:
        (needs_update, reasons)
        needs_update: 需要更新的股票列表
        reasons: {stock_code: reason} 原因字典
    """
    if not stock_codes:
        return [], {}

    conn = get_sync_connection("kline")
    cursor = conn.cursor()

    needs_update = []
    reasons = {}

    today = get_china_time().strftime('%Y-%m-%d')

    for code in stock_codes:
        # 检查1: 股票是否无数据
        cursor.execute("SELECT COUNT(*) as cnt FROM stock_adj_factor WHERE stock_code = ?", (code,))
        count = cursor.fetchone()['cnt']
        if count == 0:
            needs_update.append(code)
            reasons[code] = "无数据"
            continue

        # 检查2: 当天是否已检查过（通过 updated_at 判断）
        cursor.execute("SELECT MAX(updated_at) as last_update FROM stock_adj_factor WHERE stock_code = ?", (code,))
        row = cursor.fetchone()
        if row and row['last_update']:
            last_update_date = row['last_update'][:10]  # 取日期部分
            if last_update_date == today:
                # 当天已更新，不需要再查
                continue

        # 当天未检查，需要查询 SDK
        needs_update.append(code)
        reasons[code] = "当天未检查"

    return needs_update, reasons


def update_adj_factor_from_sdk(stock_codes: List[str]) -> Dict:
    """从 SDK 获取复权因子并更新数据库

    Args:
        stock_codes: 股票代码列表

    Returns:
        {'success': bool, 'saved': int, 'stocks': int, 'message': str}
    """
    if not stock_codes:
        return {'success': True, 'saved': 0, 'stocks': 0, 'message': '无股票需更新'}

    try:
        from services.common.sdk_manager import get_sdk_manager

        sdk_mgr = get_sdk_manager()
        df = sdk_mgr.get_adj_factor(stock_codes, priority=1)

        if df is None or df.empty:
            # 即使返回空，也更新检查时间戳（标记当天已检查）
            conn = get_sync_connection("kline")
            cursor = conn.cursor()
            now = get_china_time().strftime('%Y-%m-%d %H:%M:%S')

            for code in stock_codes:
                # 检查是否有该股票的记录
                cursor.execute("SELECT id FROM stock_adj_factor WHERE stock_code = ? LIMIT 1", (code,))
                row = cursor.fetchone()
                if row:
                    # 更新已有记录的 updated_at
                    cursor.execute("UPDATE stock_adj_factor SET updated_at = ? WHERE stock_code = ? AND id = ?",
                                   (now, code, row['id']))
                else:
                    # 无记录，插入一条标记（adj_factor=1.0，表示无除权）
                    cursor.execute("""
                        INSERT INTO stock_adj_factor (stock_code, trade_date, adj_factor, cumulative_factor, source, created_at, updated_at)
                        VALUES (?, '1990-01-01', 1.0, 1.0, 'check_mark', ?, ?)
                    """, (code, now, now))
            conn.commit()

            return {'success': True, 'saved': 0, 'stocks': len(stock_codes), 'message': 'SDK 返回空数据，已标记当天已检查'}

        saved = save_adj_factor_batch(df)

        return {
            'success': True,
            'saved': saved,
            'stocks': len(stock_codes),
            'message': f'成功更新 {len(stock_codes)} 只股票，保存 {saved} 条除权记录'
        }
    except Exception as e:
        log.error(f"复权因子更新失败: {e}")
        return {'success': False, 'saved': 0, 'stocks': 0, 'message': str(e)}


def update_adj_factor_if_needed(stock_codes: List[str]) -> Dict:
    """检查新鲜度并按需更新

    Args:
        stock_codes: 股票代码列表

    Returns:
        {'success': bool, 'updated': List[str], 'saved': int, 'message': str}
    """
    needs_update, reasons = check_freshness(stock_codes)

    if not needs_update:
        return {'success': True, 'updated': [], 'saved': 0, 'message': '所有数据新鲜'}

    log.log_event("adj_factor_stale", f"{len(needs_update)} 只股票需更新", reasons=reasons)

    result = update_adj_factor_from_sdk(needs_update)

    return {
        'success': result['success'],
        'updated': needs_update,
        'saved': result['saved'],
        'message': result['message']
    }


def get_adj_factor_count() -> Dict:
    """获取复权因子统计信息"""
    conn = get_sync_connection("kline")
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) as total FROM stock_adj_factor")
    total = cursor.fetchone()['total']

    cursor.execute("SELECT COUNT(DISTINCT stock_code) as stocks FROM stock_adj_factor")
    stocks = cursor.fetchone()['stocks']

    cursor.execute("SELECT COUNT(*) as adj_events FROM stock_adj_factor WHERE adj_factor != 1.0")
    adj_events = cursor.fetchone()['adj_events']

    return {'total_records': total, 'stocks': stocks, 'adj_events': adj_events}