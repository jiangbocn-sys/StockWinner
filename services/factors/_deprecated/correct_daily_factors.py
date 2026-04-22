"""
stock_daily_factors 数据校正工具

问题分析：
- 原迁移脚本只计算了 next_period_change 从 kline 数据
- 其他字段（change_10d, bias_5, kdj_k 等）直接从 stock_factors 复制
- 但这些字段应该根据 kline 数据重新计算

校正策略：
1. 按股票处理 - 一次性读取单只股票全部 kline 数据
2. 使用 pandas 递归计算所有日频因子
3. 批量更新到 stock_daily_factors 表
4. 进度持久化 - 支持中断和恢复
5. 排除 2026-04-03 数据（未收盘，数据为 0）

预计处理时间：
- 4,944 只股票 × 每只约 1,153 条记录
- 单只股票处理时间约 0.5-2 秒
- 总时间约 1.5-3 小时
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional, Tuple
import json
import sys

DB_PATH = Path(__file__).parent.parent.parent / "data" / "kline.db"
PROGRESS_FILE = Path(__file__).parent.parent.parent / "data" / "correction_progress.json"

# 排除 2026-04-03 的数据（未收盘）
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
    return {"processed_stocks": [], "current_stock_index": 0, "start_time": None}


def save_progress(processed_stocks: List[str], current_index: int, start_time: str = None):
    """保存进度"""
    progress = {
        "processed_stocks": processed_stocks,
        "current_stock_index": current_index,
        "start_time": start_time,
        "last_updated": datetime.now().isoformat()
    }
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(progress, f, indent=2)


def get_all_stocks() -> List[str]:
    """获取所有需要处理的股票代码"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT DISTINCT stock_code FROM stock_daily_factors
        WHERE trade_date < ?
        ORDER BY stock_code
    """, (EXCLUDE_DATE,))
    stocks = [row[0] for row in cursor.fetchall()]
    conn.close()
    return stocks


def get_stock_kline_data(stock_code: str) -> pd.DataFrame:
    """
    获取单只股票的全部 kline 数据（排除 2026-04-03）
    """
    conn = get_connection()
    query = """
        SELECT
            trade_date as trade_date,
            open, high, low, close, volume, amount
        FROM kline_data
        WHERE stock_code = ? AND trade_date < ?
        ORDER BY trade_date
    """
    df = pd.read_sql_query(query, conn, params=(stock_code, EXCLUDE_DATE))
    conn.close()
    return df


def calculate_all_factors(df: pd.DataFrame) -> pd.DataFrame:
    """
    计算所有日频因子

    输入 df 需要包含：trade_date, open, high, low, close, volume, amount

    输出包含：
    - change_10d, change_20d: N 日涨跌幅
    - bias_5, bias_10, bias_20: N 日乖离率
    - amplitude_5, amplitude_10, amplitude_20: N 日振幅
    - change_std_5, change_std_10, change_std_20: N 日涨跌幅标准差
    - amount_std_5, amount_std_10, amount_std_20: N 日成交额标准差
    - kdj_k, kdj_d, kdj_j: KDJ 指标
    - dif, dea, macd: MACD 指标
    - next_period_change: 下期收益率
    - is_traded: 是否交易
    """
    df = df.copy()
    df = df.sort_values('trade_date').reset_index(drop=True)

    # 计算涨跌幅（用于后续计算）
    df['change_pct'] = df['close'].pct_change()

    # ========== 市场表现类因子 ==========
    windows = [5, 10, 20]

    for window in windows:
        # N 日涨跌幅
        df[f'change_{window}d'] = df['close'].pct_change(window)

        # N 日乖离率 = (收盘价 - N 日均线) / N 日均线
        ma_n = df['close'].rolling(window=window).mean()
        df[f'bias_{window}'] = (df['close'] - ma_n) / ma_n

        # N 日振幅 = (N 日最高价 - N 日最低价) / 前一日收盘价
        highest = df['high'].rolling(window=window).max()
        lowest = df['low'].rolling(window=window).min()
        prev_close = df['close'].shift(1)  # 使用前一日收盘价
        df[f'amplitude_{window}'] = (highest - lowest) / prev_close

        # N 日涨跌幅标准差
        df[f'change_std_{window}'] = df['change_pct'].rolling(window=window).std()

        # N 日成交额标准差
        df[f'amount_std_{window}'] = df['amount'].rolling(window=window).std()

    # ========== KDJ 指标 ==========
    n, m1, m2 = 9, 3, 3

    # 计算 N 日最高价和最低价
    lowest_low = df['low'].rolling(window=n).min()
    highest_high = df['high'].rolling(window=n).max()

    # 计算 RSV
    rsv = (df['close'] - lowest_low) / (highest_high - lowest_low) * 100

    # 计算 K、D、J
    df['kdj_k'] = rsv.ewm(com=m1-1, adjust=False).mean()
    df['kdj_d'] = df['kdj_k'].ewm(com=m2-1, adjust=False).mean()
    df['kdj_j'] = 3 * df['kdj_k'] - 2 * df['kdj_d']

    # ========== MACD 指标 ==========
    fast, slow, signal = 12, 26, 9

    ema_fast = df['close'].ewm(span=fast, adjust=False).mean()
    ema_slow = df['close'].ewm(span=slow, adjust=False).mean()

    df['dif'] = ema_fast - ema_slow
    df['dea'] = df['dif'].ewm(span=signal, adjust=False).mean()
    df['macd'] = 2 * (df['dif'] - df['dea'])

    # ========== 下期收益率 ==========
    df['next_period_change'] = df['change_pct'].shift(-1)

    # ========== 是否交易 ==========
    df['is_traded'] = (df['volume'] > 0).astype(int)

    return df


def update_stock_factors(stock_code: str, df: pd.DataFrame) -> Tuple[int, int]:
    """
    更新单只股票的因子数据

    返回：(成功更新记录数，失败记录数)
    """
    conn = get_connection()
    cursor = conn.cursor()

    updated = 0
    failed = 0

    for _, row in df.iterrows():
        trade_date = row['trade_date']

        try:
            cursor.execute("""
                UPDATE stock_daily_factors SET
                    change_10d = ?, change_20d = ?,
                    bias_5 = ?, bias_10 = ?, bias_20 = ?,
                    amplitude_5 = ?, amplitude_10 = ?, amplitude_20 = ?,
                    change_std_5 = ?, change_std_10 = ?, change_std_20 = ?,
                    amount_std_5 = ?, amount_std_10 = ?, amount_std_20 = ?,
                    kdj_k = ?, kdj_d = ?, kdj_j = ?,
                    dif = ?, dea = ?, macd = ?,
                    next_period_change = ?,
                    is_traded = ?,
                    source = 'recalculated',
                    updated_at = ?
                WHERE stock_code = ? AND trade_date = ?
            """, (
                row.get('change_10d'), row.get('change_20d'),
                row.get('bias_5'), row.get('bias_10'), row.get('bias_20'),
                row.get('amplitude_5'), row.get('amplitude_10'), row.get('amplitude_20'),
                row.get('change_std_5'), row.get('change_std_10'), row.get('change_std_20'),
                row.get('amount_std_5'), row.get('amount_std_10'), row.get('amount_std_20'),
                row.get('kdj_k'), row.get('kdj_d'), row.get('kdj_j'),
                row.get('dif'), row.get('dea'), row.get('macd'),
                row.get('next_period_change'),
                row.get('is_traded'),
                datetime.now().isoformat(),
                stock_code, trade_date
            ))
            updated += 1
        except Exception as e:
            print(f"    更新失败 {stock_code} {trade_date}: {e}")
            failed += 1

    conn.commit()
    conn.close()
    return updated, failed


def delete_april_03_data():
    """删除 2026-04-03 的数据"""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("DELETE FROM stock_daily_factors WHERE trade_date = ?", (EXCLUDE_DATE,))
    deleted = cursor.rowcount
    conn.commit()
    conn.close()

    print(f"已删除 {deleted} 条 2026-04-03 的数据")
    return deleted


def verify_data_quality(stock_code: str, df: pd.DataFrame) -> Dict:
    """验证计算结果质量"""
    issues = []

    # 检查 NaN 比例
    nan_ratio = df.isna().sum().sum() / (len(df) * len(df.columns))
    if nan_ratio > 0.3:
        issues.append(f"NaN 比例过高：{nan_ratio:.2%}")

    # 检查异常值
    if df['kdj_k'].abs().max() > 200:
        issues.append("KDJ_K 存在异常值")
    if df['bias_5'].abs().max() > 10:
        issues.append("BIAS_5 存在异常值")

    return {"issues": issues, "record_count": len(df)}


def main(reset: bool = False):
    """
    主函数

    参数：
    - reset: 是否重置进度从头开始
    """
    print("=" * 70)
    print("stock_daily_factors 数据校正工具")
    print("=" * 70)
    print(f"数据库：{DB_PATH}")
    print(f"进度文件：{PROGRESS_FILE}")
    print(f"排除日期：{EXCLUDE_DATE}")
    print()

    # 第一步：删除 2026-04-03 的数据
    print("步骤 1: 删除 2026-04-03 的数据...")
    delete_april_03_data()
    print()

    # 第二步：获取所有股票
    print("步骤 2: 获取所有需要处理的股票...")
    all_stocks = get_all_stocks()
    total_stocks = len(all_stocks)
    print(f"  共 {total_stocks} 只股票")
    print()

    # 第三步：加载进度
    if reset:
        print("步骤 3: 重置进度，从头开始...")
        processed_stocks = []
        current_index = 0
        start_time = datetime.now().isoformat()
    else:
        print("步骤 3: 加载进度...")
        progress = load_progress()
        processed_stocks = progress.get("processed_stocks", [])
        current_index = progress.get("current_stock_index", 0)
        start_time = progress.get("start_time") or datetime.now().isoformat()
        print(f"  已处理 {len(processed_stocks)} 只股票")
        print(f"  当前索引：{current_index}")
    print()

    # 第四步：开始处理
    print("步骤 4: 开始处理...")
    print(f"  预计剩余：{total_stocks - current_index} 只股票")
    print(f"  预计时间：{(total_stocks - current_index) * 1.5 / 60:.1f} - {(total_stocks - current_index) * 3 / 60:.1f} 分钟")
    print()

    total_updated = 0
    total_failed = 0
    start_time_dt = datetime.now()

    for i, stock_code in enumerate(all_stocks[current_index:], start=current_index):
        # 检查是否已处理
        if stock_code in processed_stocks:
            continue

        try:
            # 获取 kline 数据
            df = get_stock_kline_data(stock_code)

            if df.empty:
                print(f"  [{i+1}/{total_stocks}] {stock_code}: 无 kline 数据，跳过")
                processed_stocks.append(stock_code)
                if (i + 1) % 10 == 0:
                    save_progress(processed_stocks, i + 1, start_time)
                continue

            # 计算所有因子
            df = calculate_all_factors(df)

            # 验证数据质量
            quality = verify_data_quality(stock_code, df)
            if quality["issues"]:
                print(f"  [{i+1}/{total_stocks}] {stock_code}: 警告 - {', '.join(quality['issues'])}")

            # 更新数据库
            updated, failed = update_stock_factors(stock_code, df)
            total_updated += updated
            total_failed += failed

            processed_stocks.append(stock_code)

            # 每 10 只股票保存一次进度
            if (i + 1) % 10 == 0:
                save_progress(processed_stocks, i + 1, start_time)
                elapsed = (datetime.now() - start_time_dt).total_seconds()
                avg_time = elapsed / (i + 1 - current_index)
                remaining = total_stocks - (i + 1)
                eta_minutes = remaining * avg_time / 60
                print(f"  [{i+1}/{total_stocks}] {stock_code}: 更新 {updated} 条，累计 {total_updated:,} 条 | "
                      f"平均 {avg_time:.2f}秒/只 | 剩余 {eta_minutes:.1f}分钟")

        except Exception as e:
            print(f"  [{i+1}/{total_stocks}] {stock_code}: 错误 - {e}")
            total_failed += 1
            processed_stocks.append(stock_code)
            if (i + 1) % 10 == 0:
                save_progress(processed_stocks, i + 1, start_time)

    # 保存最终进度
    save_progress(processed_stocks, total_stocks, start_time)

    # 第五步：总结
    print()
    print("=" * 70)
    print("处理完成!")
    print("=" * 70)
    print(f"  处理股票数：{len(processed_stocks)}/{total_stocks}")
    print(f"  更新记录数：{total_updated:,}")
    print(f"  失败记录数：{total_failed}")
    elapsed = (datetime.now() - start_time_dt).total_seconds()
    print(f"  总耗时：{elapsed / 60:.1f} 分钟")
    print(f"  平均速度：{elapsed / len(processed_stocks):.2f} 秒/只股票")
    print()

    # 验证结果
    print("数据验证:")
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT COUNT(*) as total, COUNT(DISTINCT stock_code) as stocks,
               MIN(trade_date) as min_date, MAX(trade_date) as max_date,
               SUM(CASE WHEN source = 'recalculated' THEN 1 ELSE 0 END) as recalculated
        FROM stock_daily_factors
    """)
    row = cursor.fetchone()
    print(f"  总记录数：{row['total']:,}")
    print(f"  股票数量：{row['stocks']:,}")
    print(f"  日期范围：{row['min_date']} 至 {row['max_date']}")
    print(f"  已重新计算：{row['recalculated']:,} ({row['recalculated']/row['total']*100:.1f}%)")

    conn.close()


if __name__ == "__main__":
    reset = "--reset" in sys.argv
    main(reset=reset)
