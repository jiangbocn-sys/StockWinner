"""
周K线数据下载模块
通过 SDK 下载周K线数据并保存到 weekly_kline_data 表
"""
import sys
sys.path.insert(0, '/home/bobo/StockWinner')

import asyncio
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, Dict, Any

# 时区配置
from zoneinfo import ZoneInfo
CHINA_TZ = ZoneInfo("Asia/Shanghai")

# 数据库路径
DB_PATH = Path("/home/bobo/StockWinner/data/kline.db")


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
    通过 SDK 下载周K线数据

    Args:
        years: 下载的年数（默认 10 年）
        start_date: 开始日期（YYYY-MM-DD），可选
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

    # 计算日期范围
    if start_date and end_date:
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
    else:
        end_dt = datetime.now(CHINA_TZ)
        start_dt = end_dt - timedelta(days=years * 365)

    start_date_int = int(start_dt.strftime('%Y%m%d'))
    end_date_int = int((end_dt + timedelta(days=7)).strftime('%Y%m%d'))  # SDK 半开区间

    print(f"[WeeklyKline] 目标日期范围：{years}年 ({start_dt.strftime('%Y-%m-%d')} ~ {end_dt.strftime('%Y-%m-%d')})")

    # 连接数据库
    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()

    # 从数据库获取股票列表（更可靠）
    cursor.execute("SELECT DISTINCT stock_code, stock_name FROM kline_data ORDER BY stock_code")
    stock_rows = cursor.fetchall()

    stock_list = []
    for row in stock_rows:
        code = row[0]
        name = row[1] or ''
        # 市场筛选
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
        return False

    total_stocks = len(stock_list)
    print(f"[WeeklyKline] 共 {total_stocks} 只股票需要下载")

    # 分批下载
    total_saved = 0
    failed_count = 0
    processed = 0

    for i in range(0, total_stocks, batch_size):
        batch = stock_list[i:i + batch_size]
        batch_codes = [s.get('stock_code') for s in batch]

        processed += len(batch)
        progress = processed / total_stocks * 100

        print(f"[WeeklyKline] 进度：{progress:.1f}% ({processed}/{total_stocks}) - 下载 {len(batch_codes)} 只股票")

        try:
            # 批量获取周K线数据
            kline_data = await gateway.get_batch_kline_data(
                stock_codes=batch_codes,
                period="week",
                start_date=str(start_date_int),
                end_date=str(end_date_int),
                limit=years * 52  # 约52周/年
            )

            # 保存数据
            for code, data in kline_data.items():
                if data is None or len(data) == 0:
                    continue

                # 获取股票名称
                stock_name = ''
                for s in batch:
                    if s.get('stock_code') == code:
                        stock_name = s.get('stock_name', '')
                        break

                # 处理 DataFrame 数据
                import pandas as pd
                if isinstance(data, pd.DataFrame):
                    df = data
                else:
                    # 如果是 list，转换为 DataFrame
                    df = pd.DataFrame(data)

                # 插入数据
                saved = 0
                for _, row in df.iterrows():
                    try:
                        trade_date = row.get('trade_date', '')
                        if not trade_date or pd.isna(trade_date):
                            continue

                        # SDK周K线的 trade_date 是该周的自然周一
                        # week_start_date = trade_date（周一）
                        # week_end_date = trade_date + 4天（自然周五）
                        if isinstance(trade_date, str):
                            dt = datetime.strptime(trade_date, '%Y-%m-%d')
                        elif isinstance(trade_date, pd.Timestamp):
                            dt = trade_date.to_pydatetime()
                        else:
                            dt = trade_date

                        week_start_str = dt.strftime('%Y-%m-%d')
                        week_end = dt + timedelta(days=4)  # 周一 + 4天 = 周五
                        week_end_str = week_end.strftime('%Y-%m-%d')

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
                    except Exception as e:
                        pass

                if saved > 0:
                    total_saved += saved
                    if processed % 200 == 0 or processed == total_stocks:
                        print(f"[WeeklyKline] {code} ({stock_name}): 保存 {saved} 条周K线")

            conn.commit()

        except Exception as e:
            print(f"[WeeklyKline] 批次下载失败：{e}")
            failed_count += len(batch)
            continue

    conn.close()

    print(f"[WeeklyKline] ====== 下载完成 ======")
    print(f"[WeeklyKline] 总计保存 {total_saved} 条周K线数据")
    print(f"[WeeklyKline] 失败股票数：{failed_count}")

    return True


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
    # 测试下载
    download_weekly_kline_sync(
        years=10,
        broker_account="REDACTED_SDK_USERNAME",
        broker_password="REDACTED_SDK_PASSWORD",
        batch_size=50
    )