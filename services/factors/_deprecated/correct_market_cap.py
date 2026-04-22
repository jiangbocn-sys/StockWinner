"""
修正流通市值和总市值

策略：
1. 从 stock_factors 表获取每个股票每个报告期的股本数据（如果有的话）
2. 对于没有股本数据的日期，从 SDK 按月获取
3. 结合 kline_data 的收盘价计算市值

市值计算公式：
- 流通市值 (亿元) = 流通股本 (万股) × 收盘价 (元) / 10000
- 总市值 (亿元) = 总股本 (万股) × 收盘价 (元) / 10000

优化：
- 按月批量获取 SDK 数据（股数不频繁变化）
- 优先使用 stock_factors 表已有的股本数据
- 进度持久化，支持中断恢复
"""

import sqlite3
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional, Tuple
import json
import sys
import time

DB_PATH = Path(__file__).parent.parent.parent / "data" / "kline.db"
PROGRESS_FILE = Path(__file__).parent.parent.parent / "data" / "market_cap_progress.json"

# 排除 2026-04-03 的数据
EXCLUDE_DATE = "2026-04-03"


def get_connection() -> sqlite3.Connection:
    """获取数据库连接"""
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def load_progress() -> Dict:
    """加载进度"""
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE, 'r') as f:
            return json.load(f)
    return {"processed_months": [], "current_month_index": 0, "start_time": None}


def save_progress(processed_months: List[str], current_index: int, start_time: str = None):
    """保存进度"""
    progress = {
        "processed_months": processed_months,
        "current_month_index": current_index,
        "start_time": start_time,
        "last_updated": datetime.now().isoformat()
    }
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(progress, f, indent=2)


def get_month_range() -> List[Tuple[str, str, str]]:
    """
    获取需要处理的月份范围

    返回：[(month, start_date, end_date), ...]
    """
    conn = get_connection()
    cursor = conn.cursor()

    # 获取日期范围
    cursor.execute("""
        SELECT MIN(trade_date), MAX(trade_date)
        FROM stock_daily_factors
        WHERE trade_date < ?
    """, (EXCLUDE_DATE,))
    row = cursor.fetchone()
    min_date = row[0]
    max_date = row[1]

    conn.close()

    # 生成月份列表
    months = []
    current = datetime.strptime(min_date[:7] + '-01', '%Y-%m-%d')
    end = datetime.strptime(max_date[:7] + '-01', '%Y-%m-%d')

    while current <= end:
        month = current.strftime('%Y-%m')
        start_date = current.strftime('%Y-%m-%d')

        # 月末日期
        if current.month == 12:
            next_month = current.replace(year=current.year + 1, month=1, day=1)
        else:
            next_month = current.replace(month=current.month + 1, day=1)
        end_date = (next_month - timedelta(days=1)).strftime('%Y-%m-%d')

        months.append((month, start_date, end_date))
        current = next_month

    return months


def get_stocks_in_month(month_start: str, month_end: str) -> List[str]:
    """获取指定月份有交易的所有股票"""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT DISTINCT stock_code
        FROM stock_daily_factors
        WHERE trade_date >= ? AND trade_date <= ?
        ORDER BY stock_code
    """, (month_start, month_end))

    stocks = [row[0] for row in cursor.fetchall()]
    conn.close()
    return stocks


def get_equity_from_sdk(stock_codes: List[str], query_date: str) -> Dict[str, Dict]:
    """
    从 SDK 批量获取股本数据

    参数：
    - stock_codes: 股票代码列表
    - query_date: 查询日期（用于获取最接近的股本数据）

    返回：{stock_code: {'total_shares': x, 'circ_shares': y}, ...}
    """
    import sys
    sys.path.insert(0, '/home/bobo/StockWinner')

    from services.factors.sdk_api import AmazingDataAPI

    api = AmazingDataAPI()
    result = {}
    query_dt = pd.to_datetime(query_date)

    # SDK 批量查询
    try:
        equity_df = api.get_equity_structure(stock_codes)

        if not equity_df.empty:
            # 转换 ANN_DATE
            equity_df['ANN_DATE'] = pd.to_datetime(equity_df['ANN_DATE'], format='%Y%m%d', errors='coerce')

            # 为每个股票获取最接近 query_date 的数据
            for stock_code in stock_codes:
                stock_data = equity_df[equity_df['MARKET_CODE'] == stock_code]
                valid_data = stock_data[stock_data['ANN_DATE'] <= query_dt]

                if not valid_data.empty:
                    latest = valid_data.sort_values('ANN_DATE', ascending=False).iloc[0]
                    result[stock_code] = {
                        'total_shares': latest.get('TOT_SHARE'),
                        'circ_shares': latest.get('FLOAT_SHARE')
                    }
                else:
                    result[stock_code] = {'total_shares': None, 'circ_shares': None}
        else:
            for stock_code in stock_codes:
                result[stock_code] = {'total_shares': None, 'circ_shares': None}

    except Exception as e:
        print(f"SDK 查询失败：{e}")
        for stock_code in stock_codes:
            result[stock_code] = {'total_shares': None, 'circ_shares': None}

    return result


def update_month_market_cap(
    month: str,
    month_start: str,
    month_end: str
) -> Tuple[int, int, int]:
    """
    更新指定月份的市值数据

    策略：
    1. 获取该月所有股票
    2. 从 SDK 获取所有股票的股本数据（用月末日期查询）
    3. 获取该月所有交易日的收盘价
    4. 批量更新市值数据

    返回：(更新记录数，使用 SDK 记录数，失败记录数)
    """
    conn = get_connection()
    cursor = conn.cursor()

    # 获取该月所有股票
    stocks = get_stocks_in_month(month_start, month_end)
    if not stocks:
        return 0, 0, 0

    print(f"  月份 {month}: {len(stocks)} 只股票")

    # 从 SDK 获取股本数据（使用月末日期）
    equity_data = get_equity_from_sdk(stocks, month_end)

    # 统计
    updated = 0
    has_sdk_data = 0
    failed = 0

    # 按股票更新
    batch_data = []
    BATCH_SIZE = 500

    for stock_code in stocks:
        equity = equity_data.get(stock_code, {})
        total_shares = equity.get('total_shares')
        circ_shares = equity.get('circ_shares')

        if total_shares or circ_shares:
            has_sdk_data += 1

        # 获取该股票该月的所有交易日和收盘价
        cursor.execute("""
            SELECT trade_date, close
            FROM kline_data
            WHERE stock_code = ? AND trade_date >= ? AND trade_date <= ?
        """, (stock_code, month_start, month_end))

        kline_records = cursor.fetchall()

        for trade_date, close in kline_records:
            circ_market_cap = None
            total_market_cap = None

            if circ_shares and close:
                # SDK 返回的股本单位是万股
                # 计算：万股 × 元 = 万元，万元 × 10000 = 元
                circ_market_cap = circ_shares * close * 10000  # 单位：元

            if total_shares and close:
                total_market_cap = total_shares * close * 10000  # 单位：元

            batch_data.append((
                circ_market_cap, total_market_cap, datetime.now().isoformat(), stock_code, trade_date
            ))

            if len(batch_data) >= BATCH_SIZE:
                cursor.executemany("""
                    UPDATE stock_daily_factors SET
                        circ_market_cap = ?,
                        total_market_cap = ?,
                        updated_at = ?
                    WHERE stock_code = ? AND trade_date = ?
                """, batch_data)
                conn.commit()
                updated += len(batch_data)
                batch_data = []

    # 提交剩余
    if batch_data:
        cursor.executemany("""
            UPDATE stock_daily_factors SET
                circ_market_cap = ?,
                total_market_cap = ?,
                updated_at = ?
            WHERE stock_code = ? AND trade_date = ?
        """, batch_data)
        conn.commit()
        updated += len(batch_data)

    conn.close()

    return updated, has_sdk_data, failed


def main(reset: bool = False, sdk_batch_size: int = 100):
    """
    主函数

    参数：
    - reset: 是否重置进度从头开始
    - sdk_batch_size: SDK 批量查询大小
    """
    print("=" * 70)
    print("修正流通市值和总市值")
    print("=" * 70)
    print(f"数据库：{DB_PATH}")
    print(f"进度文件：{PROGRESS_FILE}")
    print(f"排除日期：{EXCLUDE_DATE}")
    print()

    # 获取月份范围
    print("步骤 1: 获取月份范围...")
    months = get_month_range()
    total_months = len(months)
    print(f"  共 {total_months} 个月份：{months[0][0]} 至 {months[-1][0]}")
    print()

    # 加载进度
    if reset:
        print("步骤 2: 重置进度，从头开始...")
        processed_months = []
        current_index = 0
        start_time = datetime.now().isoformat()
    else:
        print("步骤 2: 加载进度...")
        progress = load_progress()
        processed_months = progress.get("processed_months", [])
        current_index = progress.get("current_month_index", 0)
        start_time = progress.get("start_time") or datetime.now().isoformat()
        print(f"  已处理 {len(processed_months)} 个月份")
    print()

    # 开始处理
    print("步骤 3: 开始处理...")
    print(f"  预计剩余：{total_months - current_index} 个月份")
    print(f"  SDK 批量大小：{sdk_batch_size}")
    print()

    total_updated = 0
    total_sdk_data = 0
    total_failed = 0
    start_time_dt = datetime.now()

    for i, (month, month_start, month_end) in enumerate(months[current_index:], start=current_index):
        if month in processed_months:
            continue

        try:
            # 更新该月市值数据
            updated, sdk_data, failed = update_month_market_cap(month, month_start, month_end)
            total_updated += updated
            total_sdk_data += sdk_data
            total_failed += failed

            processed_months.append(month)

            # 保存进度
            if (i + 1) % 5 == 0:
                save_progress(processed_months, i + 1, start_time)
                elapsed = (datetime.now() - start_time_dt).total_seconds() / 60
                remaining = total_months - (i + 1)
                eta_minutes = remaining * (elapsed / max(1, len(processed_months)))
                print(f"  [{i+1}/{total_months}] {month}: 更新 {updated:,} 条，累计 {total_updated:,} 条 | "
                      f"已运行 {elapsed:.1f}分钟 | 剩余 {eta_minutes:.1f}分钟")

        except Exception as e:
            print(f"  [{i+1}/{total_months}] {month}: 错误 - {e}")
            total_failed += 1
            processed_months.append(month)
            if (i + 1) % 5 == 0:
                save_progress(processed_months, i + 1, start_time)

    # 保存最终进度
    save_progress(processed_months, total_months, start_time)

    # 总结
    print()
    print("=" * 70)
    print("处理完成!")
    print("=" * 70)
    print(f"  处理月份数：{len(processed_months)}/{total_months}")
    print(f"  更新记录数：{total_updated:,}")
    print(f"  使用 SDK 数据：{total_sdk_data:,}")
    print(f"  失败记录数：{total_failed}")
    elapsed = (datetime.now() - start_time_dt).total_seconds() / 60
    print(f"  总耗时：{elapsed:.1f} 分钟")
    print()

    # 验证结果
    print("数据验证:")
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT COUNT(*) as total,
               SUM(CASE WHEN circ_market_cap IS NOT NULL AND circ_market_cap > 0 THEN 1 ELSE 0 END) as has_circ_cap,
               SUM(CASE WHEN total_market_cap IS NOT NULL AND total_market_cap > 0 THEN 1 ELSE 0 END) as has_total_cap
        FROM stock_daily_factors
    """)
    row = cursor.fetchone()
    print(f"  总记录数：{row['total']:,}")
    print(f"  有流通市值：{row['has_circ_cap']:,} ({row['has_circ_cap']/row['total']*100:.1f}%)")
    print(f"  有总市值：{row['has_total_cap']:,} ({row['has_total_cap']/row['total']*100:.1f}%)")

    # 随机抽查
    print("\n随机抽查:")
    cursor.execute("""
        SELECT stock_code, trade_date,
               ROUND(circ_market_cap, 2) as circ_cap,
               ROUND(total_market_cap, 2) as total_cap
        FROM stock_daily_factors
        WHERE circ_market_cap IS NOT NULL
        ORDER BY RANDOM()
        LIMIT 5
    """)
    for row in cursor.fetchall():
        print(f"  {row[0]} {row[1]}: 流通市值={row[2]}亿，总市值={row[3]}亿")

    conn.close()


if __name__ == "__main__":
    reset = "--reset" in sys.argv
    main(reset=reset)
