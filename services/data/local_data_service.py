"""
本地 K 线数据服务
- 从 AmazingData SDK 批量下载历史 K 线数据到本地 SQLite 数据库
- 支持增量更新（只下载新数据）
- 提供本地 K 线数据查询接口（无需调用 SDK）
"""

import asyncio
import sqlite3
import pandas as pd
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional, Tuple
from pathlib import Path

# 导入统一时区模块
from services.common.timezone import CHINA_TZ, get_china_time
# 导入下载进度跟踪器
from services.common.download_progress import get_progress_tracker, DownloadStatus

# 统一因子计算管道（替代本地重复实现）
from services.factors.factor_pipeline import calculate_technical_factors, add_signal_indicators


def get_trading_day_end_date(current_time: Optional[datetime] = None,
                              use_sdk_calendar: bool = True) -> Tuple[str, str]:
    """
    根据当前时间确定下载结束日期

    规则：
    - 使用 SDK 交易日日历（推荐）：
      - 当前时间在交易日 16:00 前 → 结束日期 = 前一交易日
      - 当前时间在交易日 16:00 后 → 结束日期 = 当前交易日
    - 不使用 SDK 日历（降级方案）：
      - 工作日（周一 - 周五）且时间 < 16:00 → 结束日期 = 前一日
      - 工作日（周一 - 周五）且时间 >= 16:00 → 结束日期 = 当日
      - 周末（周六、周日）→ 结束日期 = 周五

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
            # 确保 SDK 已登录
            sdk_manager._ensure_login()

            # 使用缓存的 BaseData 实例，避免 TGW 连接数超限
            calendar = sdk_manager.get_calendar()  # 返回 [19901219, 19901220, ...] 格式的列表

            if calendar:
                # 找到当前日期之前（包含今天）的最近一个交易日
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
                    # 今天是交易日
                    if current_hour < 16:
                        # 16 点前，下载截止到前一交易日
                        end_date = str(prev_trading_day) if prev_trading_day else (current_time - timedelta(days=1)).strftime('%Y%m%d')
                        end_date = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:8]}"
                        return end_date, f"交易日 {current_hour}:00 < 16:00，下载截止至 {end_date}"
                    else:
                        # 16 点后，可以下载当日数据
                        return today_str, f"交易日 {current_hour}:00 >= 16:00，下载包含今日 {today_str}"
                else:
                    # 今天不是交易日，使用最近一个交易日
                    end_date = str(last_trading_day) if last_trading_day else (current_time - timedelta(days=1)).strftime('%Y%m%d')
                    end_date = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:8]}"
                    return end_date, f"今日非交易日，下载截止至最近交易日 {end_date}"
        except Exception as e:
            print(f"[LocalData] SDK 日历查询失败：{e}，使用降级方案")
            # 降级到不使用 SDK 日历的逻辑

    # 降级方案：不使用 SDK 日历
    weekday = current_time.weekday()  # 0=周一，6=周日

    # 判断是否是工作日（周一 - 周五）
    if weekday < 5:
        # 工作日
        if current_hour < 16:
            # 16 点前，不下载当日数据
            end_date = (current_time - timedelta(days=1)).strftime('%Y-%m-%d')
            return end_date, f"工作日 {current_hour}:00 < 16:00，下载截止至 {end_date}"
        else:
            # 16 点后，可以下载当日数据
            return today_str, f"工作日 {current_hour}:00 >= 16:00，下载包含今日 {today_str}"
    else:
        # 周末（周六、周日），计算周五的日期
        days_since_friday = weekday - 4  # 周六=1, 周日=2
        friday = current_time - timedelta(days=days_since_friday)
        end_date = friday.strftime('%Y-%m-%d')
        day_name = "周六" if weekday == 5 else "周日"
        return end_date, f"周末（{day_name}），使用周五 {end_date}"

# 数据库路径
DB_PATH = Path(__file__).parent.parent.parent / "data" / "kline.db"


class LocalKlineDataService:
    """本地 K 线数据服务"""

    def __init__(self):
        self.db_path = DB_PATH
        self._init_db()

    def _init_db(self):
        """初始化数据库表"""
        self.db_path.parent.mkdir(exist_ok=True)
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()

        # 启用 WAL 模式以提高并发性能
        cursor.execute('PRAGMA journal_mode=WAL')
        cursor.execute('PRAGMA synchronous=NORMAL')
        cursor.execute('PRAGMA cache_size=-64000')  # 64MB 缓存
        cursor.execute('PRAGMA temp_store=MEMORY')

        # 创建 K 线数据表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS kline_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_code TEXT NOT NULL,
                stock_name TEXT,
                trade_date DATE NOT NULL,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume INTEGER,
                amount REAL,
                UNIQUE(stock_code, trade_date)
            )
        ''')

        # 创建索引加速查询
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_stock_code ON kline_data(stock_code)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_trade_date ON kline_data(trade_date)')
        cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_stock_time ON kline_data(stock_code, trade_date)')

        # 创建股票元数据表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS stock_metadata (
                stock_code TEXT PRIMARY KEY,
                stock_name TEXT,
                market TEXT,
                last_update_time TIMESTAMP,
                kline_count INTEGER DEFAULT 0
            )
        ''')

        conn.commit()
        conn.close()
        print(f"[LocalData] 数据库初始化完成：{self.db_path}")

    def get_latest_date(self, stock_code: str) -> Optional[str]:
        """获取某只股票最新的 K 线日期"""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute(
            'SELECT MAX(trade_date) FROM kline_data WHERE stock_code = ?',
            (stock_code,)
        )
        result = cursor.fetchone()[0]
        conn.close()
        return result

    def get_kline_count(self, stock_code: str) -> int:
        """获取某只股票的 K 线数据条数"""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute(
            'SELECT COUNT(*) FROM kline_data WHERE stock_code = ?',
            (stock_code,)
        )
        result = cursor.fetchone()[0]
        conn.close()
        return result

    def save_kline_data(self, stock_code: str, stock_name: str, df: pd.DataFrame):
        """
        保存 K 线数据到数据库

        Args:
            stock_code: 股票代码（带 .SH/.SZ 后缀）
            stock_name: 股票名称
            df: pandas DataFrame，包含 trade_date, open, high, low, close, volume, amount 列
        """
        if df is None or len(df) == 0:
            return 0

        conn = sqlite3.connect(str(self.db_path), timeout=30)
        cursor = conn.cursor()

        saved_count = 0
        for _, row in df.iterrows():
            try:
                # 处理日期格式 - SDK 已统一转换为 trade_date 字符串字段
                trade_date = row.get('trade_date')

                # 处理空日期
                if not trade_date or trade_date == '':
                    print(f"[LocalData] 警告：{stock_code} 数据缺少日期字段，跳过此行")
                    continue

                # 如果是 Timestamp 类型，转换为字符串
                if isinstance(trade_date, pd.Timestamp):
                    trade_date = trade_date.strftime('%Y-%m-%d')
                # 如果是整数日期格式 20260408，转换为字符串
                elif isinstance(trade_date, int):
                    trade_date = str(trade_date)
                    if len(trade_date) == 8:
                        trade_date = f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:8]}"

                cursor.execute('''
                    INSERT OR REPLACE INTO kline_data
                    (stock_code, stock_name, trade_date, open, high, low, close, volume, amount)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    stock_code,
                    stock_name,
                    trade_date,
                    float(row.get('open', 0)),
                    float(row.get('high', 0)),
                    float(row.get('low', 0)),
                    float(row.get('close', 0)),
                    int(row.get('volume', 0)),
                    float(row.get('amount', 0))
                ))
                saved_count += 1
            except Exception as e:
                print(f"[LocalData] 保存 {stock_code} 数据失败：{e}")
                continue

        # 提交事务
        conn.commit()

        # 查询当前记录数（在同一连接上）
        cursor.execute('SELECT COUNT(*) FROM kline_data WHERE stock_code = ?', (stock_code,))
        kline_count = cursor.fetchone()[0]

        # 更新股票元数据
        cursor.execute('''
            INSERT OR REPLACE INTO stock_metadata
            (stock_code, stock_name, market, last_update_time, kline_count)
            VALUES (?, ?, ?, ?, ?)
        ''', (
            stock_code,
            stock_name,
            'SH' if '.SH' in stock_code else 'SZ',
            get_china_time().isoformat(),
            kline_count
        ))

        conn.commit()
        conn.close()
        return saved_count

    def save_kline_data_batch(self, kline_batch: List[Tuple[str, str, pd.DataFrame]]) -> int:
        """
        批量保存多只股票的 K 线数据到数据库（单个事务）

        Args:
            kline_batch: 列表，每项为 (stock_code, stock_name, df) 元组

        Returns:
            保存的记录总数
        """
        if not kline_batch:
            return 0

        conn = sqlite3.connect(str(self.db_path), timeout=60)
        cursor = conn.cursor()
        total_saved = 0

        try:
            # 提前通过 AmazingData SDK 的 get_code_info 获取所有股票名称缓存
            from services.common.sdk_manager import get_sdk_manager
            sdk_mgr = get_sdk_manager()
            stock_names_cache = {}

            try:
                code_info = sdk_mgr.get_code_info(security_type='EXTRA_STOCK_A')
                if code_info is not None:
                    for idx, row in code_info.iterrows():
                        stock_names_cache[idx] = row.get('symbol', idx)
            except Exception as e:
                print(f"[LocalData] 获取股票名称失败：{e}，使用传入的股票名称")

            for stock_code, stock_name, df in kline_batch:
                if df is None or len(df) == 0:
                    continue

                # 行业指数数据列名转换（SDK返回大写，表结构需要小写）
                # 同时处理索引（TRADE_DATE）转换为列
                if 'OPEN' in df.columns or df.index.name == 'TRADE_DATE':
                    # 将索引转换为列
                    if df.index.name == 'TRADE_DATE':
                        df = df.reset_index()
                    # 列名转小写
                    df.columns = df.columns.str.lower()

                # 如果股票名称为空或等于股票代码，从缓存获取
                if not stock_name or stock_name == stock_code:
                    stock_name = stock_names_cache.get(stock_code, stock_code)

                for _, row in df.iterrows():
                    try:
                        # 处理日期格式 - SDK 已统一转换为 trade_date 字符串字段
                        trade_date = row.get('trade_date')

                        # 处理空日期
                        if not trade_date or trade_date == '':
                            print(f"[LocalData] 警告：{stock_code} 数据缺少日期字段，跳过此行")
                            continue

                        # 如果是 Timestamp 类型，转换为字符串
                        if isinstance(trade_date, pd.Timestamp):
                            trade_date = trade_date.strftime('%Y-%m-%d')
                        # 如果是整数日期格式 20260408，转换为字符串
                        elif isinstance(trade_date, int):
                            trade_date = str(trade_date)
                            if len(trade_date) == 8:
                                trade_date = f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:8]}"

                        cursor.execute('''
                            INSERT OR REPLACE INTO kline_data
                            (stock_code, stock_name, trade_date, open, high, low, close, volume, amount)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            stock_code,
                            stock_name,
                            trade_date,
                            float(row.get('open', 0)),
                            float(row.get('high', 0)),
                            float(row.get('low', 0)),
                            float(row.get('close', 0)),
                            int(row.get('volume', 0)),
                            float(row.get('amount', 0))
                        ))
                        total_saved += 1
                    except Exception as e:
                        print(f"[LocalData] 保存 {stock_code} 数据失败：{e}")
                        continue

            # 统一提交事务
            conn.commit()

            # 批量更新股票元数据
            for stock_code, stock_name, df in kline_batch:
                if df is None or len(df) == 0:
                    continue
                cursor.execute('SELECT COUNT(*) FROM kline_data WHERE stock_code = ?', (stock_code,))
                kline_count = cursor.fetchone()[0]
                cursor.execute('''
                    INSERT OR REPLACE INTO stock_metadata
                    (stock_code, stock_name, market, last_update_time, kline_count)
                    VALUES (?, ?, ?, ?, ?)
                ''', (
                    stock_code,
                    stock_name,
                    'SI' if '.SI' in stock_code else ('SH' if '.SH' in stock_code else 'SZ'),
                    get_china_time().isoformat(),
                    kline_count
                ))

            conn.commit()
            print(f"[LocalData] 批量保存完成：{len(kline_batch)} 只股票，{total_saved} 条记录")

        except Exception as e:
            print(f"[LocalData] 批量保存失败：{e}")
            conn.rollback()
            raise
        finally:
            conn.close()

        return total_saved

    def get_kline_data(
        self,
        stock_code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: Optional[int] = None
    ) -> List[Dict]:
        """
        从本地数据库获取 K 线数据

        Args:
            stock_code: 股票代码
            months: 下载的月数（默认 6 个月）（YYYY-MM-DD）
            （YYYY-MM-DD）
            limit: 返回条数限制

        Returns:
            K 线数据列表
        """
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        query = 'SELECT * FROM kline_data WHERE stock_code = ?'
        params = [stock_code]

        if start_date:
            query += ' AND trade_date >= ?'
            params.append(start_date)
        if end_date:
            query += ' AND trade_date <= ?'
            params.append(end_date)

        query += ' ORDER BY trade_date ASC'

        if limit:
            query += ' LIMIT ?'
            params.append(limit)

        cursor.execute(query, params)
        rows = cursor.fetchall()

        result = [dict(row) for row in rows]
        conn.close()
        return result

    def get_all_stocks(self) -> List[str]:
        """获取数据库中所有股票代码"""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute('SELECT DISTINCT stock_code FROM kline_data')
        stocks = [row[0] for row in cursor.fetchall()]
        conn.close()
        return stocks

    def get_stocks_date_ranges_batch(self, stock_codes: List[str], start_date: str, end_date: str) -> Dict[str, Dict]:
        """
        批量获取多只股票在指定日期范围内的数据统计（避免 N+1 查询）

        Args:
            stock_codes: 股票代码列表
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)

        Returns:
            Dict[stock_code, {'latest_date': str, 'earliest_date': str, 'count': int}]
        """
        if not stock_codes:
            return {}

        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()

        # 使用 GROUP BY 一次性查询所有股票的日期范围
        cursor.execute('''
            SELECT stock_code, MAX(trade_date), MIN(trade_date), COUNT(*)
            FROM kline_data
            WHERE stock_code IN ({}) AND trade_date >= ? AND trade_date <= ?
            GROUP BY stock_code
        '''.format(','.join(['?' for _ in stock_codes])), stock_codes + [start_date, end_date])

        results = {}
        for row in cursor.fetchall():
            results[row[0]] = {
                'latest_date': row[1],
                'earliest_date': row[2],
                'count': row[3]
            }
        conn.close()
        return results

    def get_stocks_existing_dates_batch(self, stock_codes: List[str], start_date: str, end_date: str) -> Dict[str, List[str]]:
        """
        批量获取每只股票在目标日期范围内实际存在的交易日期列表

        用于精确识别缺失日期（适用于短日期范围的完整性检查）

        Args:
            stock_codes: 股票代码列表
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)

        Returns:
            Dict[stock_code, List[trade_date]] - 每只股票已存在的日期列表
        """
        if not stock_codes:
            return {}

        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()

        # 批量查询所有股票的日期列表
        cursor.execute('''
            SELECT stock_code, trade_date
            FROM kline_data
            WHERE stock_code IN ({}) AND trade_date >= ? AND trade_date <= ?
            ORDER BY stock_code, trade_date
        '''.format(','.join(['?' for _ in stock_codes])), stock_codes + [start_date, end_date])

        results = {}
        for row in cursor.fetchall():
            stock_code = row[0]
            trade_date = row[1]
            if stock_code not in results:
                results[stock_code] = []
            results[stock_code].append(trade_date)
        conn.close()
        return results

    def get_stock_date_range(self, stock_code: str) -> tuple:
        """获取某只股票的日期范围（最新日期，最早日期）"""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute('''
            SELECT MAX(trade_date), MIN(trade_date)
            FROM kline_data
            WHERE stock_code = ?
        ''', (stock_code,))
        result = cursor.fetchone()
        conn.close()
        if result and result[0]:
            return (result[0], result[1])
        return (None, None)

    def check_data_integrity(self, stock_code: str, expected_start: str, expected_end: str) -> Dict:
        """
        检查某只股票的数据完整性

        Args:
            stock_code: 股票代码
            expected_start: 期望起始日期（YYYY-MM-DD）
            expected_end: 期望结束日期（YYYY-MM-DD）

        Returns:
            完整性检查结果：
            {
                'is_complete': bool,      # 数据是否完整
                'actual_start': str,      # 实际起始日期
                'actual_end': str,        # 实际结束日期
                'expected_count': int,    # 期望记录数（交易日天数）
                'actual_count': int,      # 实际记录数
                'missing_ranges': list    # 缺失的日期范围列表
            }
        """
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()

        # 计算期望的记录数（交易日天数，约 242 天/年）
        start_dt = datetime.strptime(expected_start, '%Y-%m-%d')
        end_dt = datetime.strptime(expected_end, '%Y-%m-%d')
        total_days = (end_dt - start_dt).days + 1
        # 估算交易日天数（约 70% 的工作日）
        expected_count = int(total_days * 0.7)

        # 获取目标日期范围内的实际记录数（修复：添加日期范围限制）
        cursor.execute('''
            SELECT MAX(trade_date), MIN(trade_date), COUNT(*)
            FROM kline_data
            WHERE stock_code = ? AND trade_date >= ? AND trade_date <= ?
        ''', (stock_code, expected_start, expected_end))
        result = cursor.fetchone()
        conn.close()

        if not result or not result[0]:
            # 目标日期范围内完全没有数据
            return {
                'is_complete': False,
                'actual_start': None,
                'actual_end': None,
                'expected_count': expected_count,
                'actual_count': 0,
                'missing_ranges': [(expected_start, expected_end)]
            }

        actual_start = result[1]
        actual_end = result[0]
        actual_count = result[2]

        # 检查是否有显著缺失（允许 10% 的误差，因为节假日等）
        # 当实际记录数 >= 期望记录数时，说明数据充足
        if actual_count >= expected_count:
            missing_ratio = 0
        else:
            missing_count = expected_count - actual_count
            missing_ratio = missing_count / expected_count if expected_count > 0 else 0

        # 检查结束日期 - 动态容差逻辑
        actual_end_dt = datetime.strptime(actual_end, '%Y-%m-%d')
        expected_end_dt = datetime.strptime(expected_end, '%Y-%m-%d')
        end_days_diff = (expected_end_dt - actual_end_dt).days

        # 动态容差：考虑期望日期和当前时间
        # T+1 规则只在交易日 16:00 前适用（当天数据还未生成）
        # 如果当前时间 >= 16:00，当天数据应该已经可用，不允许容差
        today = datetime.now(CHINA_TZ)
        today_str = today.strftime('%Y-%m-%d')
        current_hour = today.hour

        if expected_end >= today_str:
            # 期望日期是今天或未来
            if current_hour >= 16:
                # 已经 16:00 以后，当天数据应该可用，严格要求
                end_date_ok = end_days_diff <= 0
            else:
                # 16:00 之前，允许 1 天容差（T+1 规则）
                end_date_ok = end_days_diff <= 1
        else:
            # 期望日期是过去的日期，不允许容差
            end_date_ok = end_days_diff <= 0

        # 如果缺失超过 10% 或结束日期不符合要求，认为数据不完整
        is_complete = missing_ratio < 0.1 and end_date_ok

        # 计算缺失范围
        missing_ranges = []
        if not is_complete:
            if actual_start and actual_start > expected_start:
                missing_ranges.append((expected_start, actual_start))
            # 检查晚期缺失 - 根据期望日期和当前时间决定是否使用容差
            if actual_end and actual_end < expected_end:
                if expected_end >= today_str:
                    # 期望日期是今天或未来
                    if current_hour >= 16:
                        # 16:00 以后，严格要求
                        if end_days_diff > 0:
                            missing_ranges.append((actual_end, expected_end))
                    else:
                        # 16:00 之前，允许 1 天容差
                        if end_days_diff > 1:
                            missing_ranges.append((actual_end, expected_end))
                else:
                    # 期望日期是过去的日期，严格要求
                    if end_days_diff > 0:
                        missing_ranges.append((actual_end, expected_end))

        return {
            'is_complete': is_complete,
            'actual_start': actual_start,
            'actual_end': actual_end,
            'expected_count': expected_count,
            'actual_count': actual_count,
            'missing_ranges': missing_ranges
        }

    def cleanup_old_data(self, months: int = 6):
        """清理超过指定月数的旧数据"""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()

        # 计算截止日期
        cutoff_date = datetime.now(CHINA_TZ) - timedelta(days=months * 30)
        cutoff_date_str = cutoff_date.strftime('%Y-%m-%d')

        # 删除旧数据
        cursor.execute('DELETE FROM kline_data WHERE trade_date < ?', (cutoff_date_str,))
        deleted = cursor.rowcount

        conn.commit()
        conn.close()

        print(f"[LocalData] 清理了 {deleted} 条旧数据")
        return deleted

    def get_download_stats(self) -> Dict:
        """获取下载统计信息"""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()

        # 总股票数
        cursor.execute('SELECT COUNT(DISTINCT stock_code) FROM kline_data')
        total_stocks = cursor.fetchone()[0]

        # 总数据条数
        cursor.execute('SELECT COUNT(*) FROM kline_data')
        total_records = cursor.fetchone()[0]

        # 最新数据日期
        cursor.execute('SELECT MAX(trade_date) FROM kline_data')
        latest_date = cursor.fetchone()[0]

        # 最早数据日期
        cursor.execute('SELECT MIN(trade_date) FROM kline_data')
        earliest_date = cursor.fetchone()[0]

        conn.close()

        return {
            "total_stocks": total_stocks,
            "total_records": total_records,
            "latest_date": latest_date,
            "earliest_date": earliest_date,
            "database_path": str(self.db_path)
        }
    def get_batch_kline(
        self,
        stock_codes: List[str],
        limit: int = 100,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> Dict[str, List[Dict]]:
        """
        批量从本地 kline.db 获取 K 线历史数据（不经过 TGW）

        Args:
            stock_codes: 股票代码列表
            limit: 每只股票最多返回条数（默认 100）
            start_date: 开始日期 YYYY-MM-DD（传入后 limit 不生效）
            end_date: 结束日期 YYYY-MM-DD（需配合 start_date 使用）

        Returns:
            Dict[stock_code, [{trade_date, open, high, low, close, volume, amount}]]
        """
        if not stock_codes:
            return {}

        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        placeholders = ','.join(['?' for _ in stock_codes])

        if start_date:
            query = f'''
                SELECT stock_code, trade_date, open, high, low, close, volume, amount
                FROM kline_data
                WHERE stock_code IN ({placeholders}) AND trade_date >= ?
            '''
            params = stock_codes + [start_date]
            if end_date:
                query += ' AND trade_date <= ?'
                params.append(end_date)
            query += ' ORDER BY stock_code, trade_date ASC'
        else:
            # 使用 LIMIT 方式：对每只股票取最近 N 条
            # SQLite 不支持每组的 LIMIT，用子查询
            query = f'''
                SELECT k.stock_code, k.trade_date, k.open, k.high, k.low, k.close, k.volume, k.amount
                FROM kline_data k
                WHERE k.stock_code IN ({placeholders})
                  AND (
                      SELECT COUNT(*) FROM kline_data k2
                      WHERE k2.stock_code = k.stock_code AND k2.trade_date >= k.trade_date
                  ) <= ?
                ORDER BY k.stock_code, k.trade_date ASC
            '''
            params = stock_codes + [limit]

        cursor.execute(query, params)
        rows = cursor.fetchall()
        conn.close()

        # 按 stock_code 分组
        result: Dict[str, List[Dict]] = {}
        for row in rows:
            code = row['stock_code']
            if code not in result:
                result[code] = []
            result[code].append(dict(row))

        return result

    def get_daily_factors(
        self,
        stock_code: str,
        date: str,
    ) -> Optional[Dict]:
        """
        从 stock_daily_factors 表获取指定日期的因子数据

        Args:
            stock_code: 股票代码
            date: 日期 YYYY-MM-DD

        Returns:
            因子数据 Dict，不存在返回 None
        """
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute(
            'SELECT * FROM stock_daily_factors WHERE stock_code = ? AND trade_date = ?',
            (stock_code, date)
        )
        row = cursor.fetchone()
        conn.close()

        return dict(row) if row else None

    def get_daily_factors_batch(
        self,
        stock_codes: List[str],
        date: str,
    ) -> Dict[str, Dict]:
        """
        批量获取多只股票在指定日期的因子数据

        Args:
            stock_codes: 股票代码列表
            date: 日期 YYYY-MM-DD

        Returns:
            Dict[stock_code, {factor_data}]
        """
        if not stock_codes:
            return {}

        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        placeholders = ','.join(['?' for _ in stock_codes])
        query = f'SELECT * FROM stock_daily_factors WHERE stock_code IN ({placeholders}) AND trade_date = ?'
        cursor.execute(query, stock_codes + [date])

        result: Dict[str, Dict] = {}
        for row in cursor.fetchall():
            result[row['stock_code']] = dict(row)

        conn.close()
        return result

    def get_kline_spliced(
        self,
        stock_codes: List[str],
        lookback: int = 100,
        realtime_quotes: Optional[Dict[str, Dict]] = None,
    ) -> Dict[str, pd.DataFrame]:
        """
        获取完整 K 线序列：本地历史 (lookback-1) 天 + 当日实时 OHLCV

        用于 Kronos 等需要包含当日数据的模型预测场景。

        Args:
            stock_codes: 股票代码列表
            lookback: 总需要的 K 线数量（默认 100）
            realtime_quotes: 当日实时行情 {stock_code: {open, high, low, close, volume, amount}}
                           如果不传，自动从 kline.db 取最新日期的数据作为当日近似

        Returns:
            Dict[stock_code, pd.DataFrame] — 每只股票 lookback 条 K 线
        """
        if not stock_codes:
            return {}

        # 1. 从本地获取 lookback-1 条历史数据
        history = self.get_batch_kline(stock_codes, limit=lookback - 1)

        # 2. 如果没有传入实时行情，用本地最新数据近似当日
        if realtime_quotes is None:
            realtime_quotes = {}
            # 获取每只股票最新一条数据作为当日近似
            conn = sqlite3.connect(str(self.db_path))
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            placeholders = ','.join(['?' for _ in stock_codes])
            cursor.execute(f'''
                SELECT k.stock_code, k.trade_date, k.open, k.high, k.low, k.close, k.volume, k.amount
                FROM kline_data k
                WHERE k.stock_code IN ({placeholders})
                  AND (
                      SELECT COUNT(*) FROM kline_data k2
                      WHERE k2.stock_code = k.stock_code AND k2.trade_date >= k.trade_date
                  ) <= 1
                ORDER BY k.stock_code
            ''', stock_codes)
            for row in cursor.fetchall():
                code = row['stock_code']
                realtime_quotes[code] = {
                    'open': row['open'], 'high': row['high'],
                    'low': row['low'], 'close': row['close'],
                    'volume': row['volume'], 'amount': row['amount'],
                }
            conn.close()

        # 3. 拼接历史 + 当日
        result: Dict[str, pd.DataFrame] = {}
        today = get_china_time().strftime('%Y-%m-%d')

        for code in stock_codes:
            rows = history.get(code, [])

            # 追加当日实时数据（去重：如果历史已包含今日数据则跳过）
            if realtime_quotes and code in realtime_quotes:
                latest_date = rows[-1].get('trade_date') if rows else None
                if latest_date != today:
                    q = realtime_quotes[code]
                    rows.append({
                        'stock_code': code,
                        'trade_date': today,
                        'open': q.get('open', 0),
                        'high': q.get('high', 0),
                        'low': q.get('low', 0),
                        'close': q.get('close', 0),
                        'volume': q.get('volume', 0),
                        'amount': q.get('amount', 0),
                    })

            # 确保总条数不超过 lookback
            if len(rows) > lookback:
                rows = rows[-lookback:]

            if len(rows) > 0:
                df = pd.DataFrame(rows)
                result[code] = df

        return result

    def get_kline_with_realtime(
        self,
        stock_codes: List[str],
        lookback: int = 100,
        fetch_realtime_fn = None,
    ) -> Dict[str, pd.DataFrame]:
        """
        智能获取 K 线数据：根据交易时段自动选择数据源

        数据路由规则：
        - 盘中（09:00-16:00 交易日）：本地 lookback-1 天 + TGW 当日实时拼接
        - 盘后（收盘后/非交易日）：本地 lookback 天（已包含当日数据）

        Args:
            stock_codes: 股票代码列表
            lookback: 总需要的 K 线数量（默认 100）
            fetch_realtime_fn: 可选，异步函数用于获取当日实时行情
                             签名: async fn(stock_code) -> {open, high, low, close, volume, amount}
                             如果不传，盘中时段不会获取实时数据

        Returns:
            Dict[stock_code, pd.DataFrame] — 每只股票 lookback 条 K 线
        """
        trading = is_trading_hours()

        if not trading:
            # 盘后：本地数据已包含当日完整数据，直接返回
            return self.get_batch_kline(stock_codes, limit=lookback)

        # 盘中：需要拼接当日实时数据
        realtime_quotes = {}
        if fetch_realtime_fn is not None:
            import asyncio
            loop = asyncio.new_event_loop()
            try:
                for code in stock_codes:
                    try:
                        quote = loop.run_until_complete(fetch_realtime_fn(code))
                        if quote:
                            realtime_quotes[code] = quote
                    except Exception:
                        pass
            finally:
                loop.close()

        return self.get_kline_spliced(stock_codes, lookback=lookback, realtime_quotes=realtime_quotes)


# 全局单例
_local_data_service: Optional[LocalKlineDataService] = None


def is_trading_hours() -> bool:
    """
    判断当前是否处于交易时段（用于决定数据源策略）

    规则：
    - 交易日 09:00 - 16:00 → True（需要实时数据拼接）
    - 其他时间 → False（可用本地因子表）

    注意：此处简化处理，不考虑节假日精确判断。
    如需精确交易日判断，调用方应使用 SDK 日历。
    """
    now = get_china_time()
    weekday = now.weekday()  # 0=周一, 6=周日
    if weekday >= 5:
        return False
    hour = now.hour
    return 9 <= hour < 16


def get_local_data_service() -> LocalKlineDataService:
    """获取本地数据服务单例"""
    global _local_data_service
    if _local_data_service is None:
        _local_data_service = LocalKlineDataService()
    return _local_data_service


def reset_local_data_service():
    """重置本地数据服务（用于测试）"""
    global _local_data_service
    _local_data_service = None


def get_ipo_date_for_stock(stock_code: str) -> Optional[str]:
    """
    从 kline_data 表推算股票的 IPO 日期（近似值）

    方法：使用 kline_data 表中最小的交易日期作为 IPO 日期的参考
    注意：由于 kline 数据可能不完整，这只是一个近似值

    参数：
    - stock_code: 股票代码

    返回：
    - IPO 日期字符串 YYYY-MM-DD，如无法确定返回 None
    """
    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()

    # 从 kline_data 表获取最早的交易日期
    cursor.execute("""
        SELECT MIN(trade_date)
        FROM kline_data
        WHERE stock_code = ?
    """, (stock_code,))
    row = cursor.fetchone()
    conn.close()

    if row and row[0]:
        return row[0]

    return None


def calculate_days_since_ipo(stock_code: str, trade_date: str) -> Optional[int]:
    """
    计算指定日期的上市天数

    参数：
    - stock_code: 股票代码
    - trade_date: 交易日期 YYYY-MM-DD

    返回：
    - 上市天数，如无法计算返回 None
    """
    ipo_date = get_ipo_date_for_stock(stock_code)
    if not ipo_date:
        return None

    from datetime import datetime
    ipo_dt = datetime.strptime(ipo_date, '%Y-%m-%d')
    trade_dt = datetime.strptime(trade_date, '%Y-%m-%d')

    return (trade_dt - ipo_dt).days + 1  # IPO 首日为第 1 天


# ================================================================
# 因子计算（已迁移至 factor_service.py，保留 re-export 兼容）
# ================================================================

from services.data.factor_service import (
    calculate_and_save_factors_for_dates,
    calculate_and_save_factors_for_dates_async,
    fill_empty_factor_values,
    smart_update_factors,
)

__all__ = [
    "LocalDataService",
    "get_local_data_service",
    "get_trading_day_end_date",
    "download_industry_indices",
    "calculate_and_save_factors_for_dates",
    "calculate_and_save_factors_for_dates_async",
    "fill_empty_factor_values",
    "smart_update_factors",
    "download_all_kline_data",
    "download_all_kline_data_sync",
    "download_incremental_kline_data",
    "download_incremental_kline_data_sync",
    "download_industry_indices",
    "get_trading_day_end_date",
]


# ================================================================
# 下载函数（已迁移至 data_download.py，保留 re-export 兼容）
# 使用延迟导入避免循环引用（data_download.py -> get_local_data_service）
# ================================================================

def download_all_kline_data(*args, **kwargs):
    from services.data.data_download import download_all_kline_data as _fn
    return _fn(*args, **kwargs)


def download_all_kline_data_sync(*args, **kwargs):
    from services.data.data_download import download_all_kline_data_sync as _fn
    return _fn(*args, **kwargs)


def download_incremental_kline_data(*args, **kwargs):
    from services.data.data_download import download_incremental_kline_data as _fn
    return _fn(*args, **kwargs)


def download_incremental_kline_data_sync(*args, **kwargs):
    from services.data.data_download import download_incremental_kline_data_sync as _fn
    return _fn(*args, **kwargs)


def download_industry_indices(*args, **kwargs):
    from services.data.data_download import download_industry_indices as _fn
    return _fn(*args, **kwargs)
