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
import asyncio

# 导入统一时区模块
from services.common.timezone import CHINA_TZ, get_china_time
# 导入下载进度跟踪器
from services.common.download_progress import get_progress_tracker, DownloadStatus


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
            bd = sdk_manager.get_base_data()
            calendar = bd.get_calendar()  # 返回 [19901219, 19901220, ...] 格式的列表

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


# 全局单例
_local_data_service: Optional[LocalKlineDataService] = None


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
    4. 调用 DailyFactorCalculator 计算所有因子
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
    from services.factors.daily_factor_calculator import DailyFactorCalculator

    conn = sqlite3.connect(str(DB_PATH), timeout=60)
    cursor = conn.cursor()
    # 启用WAL模式减少锁竞争，设置busy_timeout为60秒
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA busy_timeout = 60000")
    cursor.execute("PRAGMA synchronous=NORMAL")

    # 获取日期范围内的所有股票
    if stock_codes is None:
        cursor.execute("""
            SELECT DISTINCT stock_code FROM kline_data
            WHERE trade_date >= ? AND trade_date <= ?
        """, (start_date, end_date))
        stock_codes = [row[0] for row in cursor.fetchall()]

    if not stock_codes:
        print(f"[FactorCalc] 日期范围 [{start_date}, {end_date}] 内无数据")
        conn.close()
        return 0

    total_stocks = len(stock_codes)
    print(f"[FactorCalc] 将为 {total_stocks} 只股票计算因子 ({start_date} 至 {end_date})")

    calculator = DailyFactorCalculator()
    total_inserted = 0
    BATCH_SIZE = 50  # 每批处理的股票数
    processed = 0

    # 初始化进度跟踪器 - 设置总任务数为股票总数
    if tracker:
        tracker.update_sync(total_tasks=total_stocks, message="开始计算因子...")

    # 分批处理
    for i in range(0, len(stock_codes), BATCH_SIZE):
        batch_stocks = stock_codes[i:i + BATCH_SIZE]
        processed += len(batch_stocks)

        if show_progress:
            progress_pct = (processed / total_stocks) * 100 if total_stocks > 0 else 0
            batch_num = i // BATCH_SIZE + 1
            num_batches = (total_stocks + BATCH_SIZE - 1) // BATCH_SIZE
            print(f"\n[FactorCalc] 进度：{progress_pct:.1f}% | 批次 {batch_num}/{num_batches} | 处理 {len(batch_stocks)} 只股票 ({processed}/{total_stocks})")

        # 更新进度跟踪器（使用同步方法）
        if tracker:
            tracker.update_sync(
                processed=processed,
                current_stock=batch_stocks[0] if batch_stocks else "",
                message=f"计算因子：批次 {batch_num}/{num_batches}"
            )

        for stock_code in batch_stocks:
            try:
                # 从 kline_data 获取股票名称
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
                    # 查询该股票在 stock_daily_factors 表中已有哪些日期
                    cursor.execute("""
                        SELECT MIN(trade_date), MAX(trade_date)
                        FROM stock_daily_factors
                        WHERE stock_code = ?
                    """, (stock_code,))
                    row = cursor.fetchone()

                    existing_min = row[0] if row and row[0] else None
                    existing_max = row[1] if row and row[1] else None

                    # 计算需要补充的日期范围
                    calc_start = start_date
                    calc_end = end_date

                    if existing_max:
                        # 已有数据，只计算之后的日期
                        calc_start_dt = datetime.strptime(existing_max, '%Y-%m-%d') + timedelta(days=1)
                        calc_end_dt = datetime.strptime(end_date, '%Y-%m-%d')
                        if calc_start_dt > calc_end_dt:
                            # 没有需要补充的日期
                            continue
                        calc_start = calc_start_dt.strftime('%Y-%m-%d')
                    elif existing_min:
                        # 有数据但只有部分，计算之前的日期
                        calc_end_dt = datetime.strptime(existing_min, '%Y-%m-%d') - timedelta(days=1)
                        calc_start_dt = datetime.strptime(start_date, '%Y-%m-%d')
                        if calc_end_dt < calc_start_dt:
                            continue
                        calc_end = calc_end_dt.strftime('%Y-%m-%d')

                # 获取K线数据用于计算技术指标
                # 注意：需要获取前溯足够数据用于计算MA60、RSI_14等指标（至少120天）
                kline_query_start = calc_start if only_new_dates else start_date
                if only_new_dates:
                    # 前溯120天获取足够的历史数据
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

                # 转换为 DataFrame（指定列名）
                import pandas as pd
                df = pd.DataFrame(kline_rows, columns=['trade_date', 'open', 'high', 'low', 'close', 'volume', 'amount'])
                df['stock_code'] = stock_code
                df['change_pct'] = df['close'].pct_change()

                # 调试：打印第一批次的第一只股票
                if only_new_dates and i == 0 and stock_code == batch_stocks[0]:
                    print(f"  [DEBUG] {stock_code}: calc={calc_start}~{calc_end}, K线={df['trade_date'].min()}~{df['trade_date'].max()}")

                # 计算技术指标因子
                df = calculator.calculate_price_performance(df)
                df = calculator.calculate_kdj(df)
                df = calculator.calculate_macd(df)

                # 计算扩展技术指标（MA5/10/20/60, RSI14, 布林带等）
                from services.common.technical_indicators import add_all_extended_technical_indicators_to_df
                from services.common.sdk_column_mapping import map_tech_columns
                df = add_all_extended_technical_indicators_to_df(df)
                # 转换列名：ma_5 -> ma5 等
                df = map_tech_columns(df)

                # 计算补充技术信号指标
                # 1. 金叉死叉信号
                df['golden_cross'] = ((df['ma5'] > df['ma10']) & (df['ma5'].shift(1) <= df['ma10'].shift(1))).astype(int)
                df['death_cross'] = ((df['ma5'] < df['ma10']) & (df['ma5'].shift(1) >= df['ma10'].shift(1))).astype(int)

                # 2. 涨停统计（涨幅>9.5%视为涨停）
                df['is_limit_up'] = (df['change_pct'] * 100 > 9.5).astype(int)
                df['limit_up_count_10d'] = df['is_limit_up'].rolling(10, min_periods=1).sum()
                df['limit_up_count_20d'] = df['is_limit_up'].rolling(20, min_periods=1).sum()
                df['limit_up_count_30d'] = df['is_limit_up'].rolling(30, min_periods=1).sum()

                # 3. 连续涨停天数
                df['consecutive_limit_up'] = 0
                consec_count = 0
                for i in range(len(df)):
                    if df['is_limit_up'].iloc[i] == 1:
                        consec_count += 1
                    else:
                        consec_count = 0
                    df.loc[df.index[i], 'consecutive_limit_up'] = consec_count

                # 4. 大涨大跌统计（涨幅>5%或跌幅>5%）
                df['large_gain_5d_count'] = (df['change_pct'] * 100 > 5).rolling(5, min_periods=1).sum().astype(int)
                df['large_loss_5d_count'] = (df['change_pct'] * 100 < -5).rolling(5, min_periods=1).sum().astype(int)

                # 5. 价格位置（距250日高低点比例）
                high_250 = df['high'].rolling(250, min_periods=1).max()
                low_250 = df['low'].rolling(250, min_periods=1).min()
                range_val = high_250 - low_250
                df['close_to_high_250d'] = (df['close'] - low_250) / range_val * 100
                df['close_to_low_250d'] = (high_250 - df['close']) / range_val * 100

                # 6. 缺口比例（跳空高开统计）
                df['gap_up'] = ((df['open'] > df['high'].shift(1)) & (df['open'] > df['low'].shift(1))).astype(int)
                df['gap_up_ratio'] = df['gap_up'].rolling(20, min_periods=1).sum() / 20 * 100

                df['next_period_change'] = df['close'].pct_change().shift(-1)
                df['is_traded'] = (df['volume'] > 0).astype(int)

                # 从 stock_base_info 获取股本数据用于计算市值
                float_share = None  # 流通股本（万股）
                total_share = None  # 总股本（万股）
                try:
                    cursor.execute("SELECT float_share, total_share FROM stock_base_info WHERE stock_code = ?", (stock_code,))
                    result = cursor.fetchone()
                    if result:
                        float_share = result[0]  # 万股
                        total_share = result[1]  # 万股
                except:
                    pass

                # 计算上市天数（从 stock_base_info 获取上市日期）
                ipo_date = None
                try:
                    cursor.execute("SELECT list_date FROM stock_base_info WHERE stock_code = ?", (stock_code,))
                    result = cursor.fetchone()
                    if result and result[0]:
                        # list_date 可能是整数格式如 20260415
                        list_val = result[0]
                        if isinstance(list_val, int) and list_val > 19000000:
                            ipo_date = f"{list_val // 10000}-{(list_val % 10000) // 100:02d}-{list_val % 100:02d}"
                        else:
                            ipo_date = str(list_val)
                except:
                    pass

                # 批量插入（只插入calc_start到calc_end范围内的日期）
                insert_count = 0
                # 确定需要插入的日期范围
                insert_start = calc_start if only_new_dates else start_date
                insert_end = calc_end if only_new_dates else end_date

                # 调试日志：显示日期范围
                if show_progress and insert_count == 0:
                    print(f"  [DEBUG] {stock_code}: insert范围 {insert_start} ~ {insert_end}, df日期 {df['trade_date'].min()} ~ {df['trade_date'].max()}")

                # numpy类型转换函数：避免numpy.int64/float64被SQLite存储为blob
                def to_python_value(val):
                    if val is None:
                        return None
                    if isinstance(val, (int, float)):
                        return val
                    # numpy类型转换
                    import numpy as np
                    if isinstance(val, np.integer):
                        return int(val)
                    if isinstance(val, np.floating):
                        return float(val)
                    return val

                for _, row in df.iterrows():
                    trade_date = row['trade_date']

                    # 只处理需要计算的日期范围内的记录
                    if trade_date < insert_start or trade_date > insert_end:
                        continue

                    # 计算市值：收盘价(元) * 股本(万股) = 万元，转换为亿元
                    close_price = row['close']
                    circ_market_cap = None
                    total_market_cap = None
                    if close_price and close_price > 0:
                        if float_share and float_share > 0:
                            circ_market_cap = round(close_price * float_share / 10000, 2)  # 亿元
                        if total_share and total_share > 0:
                            total_market_cap = round(close_price * total_share / 10000, 2)  # 亿元

                    # 计算上市天数
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
                        print(f"[FactorCalc] 插入 {stock_code} {trade_date} 失败：{e}")
                        continue

                if insert_count > 0:
                    total_inserted += insert_count
                    if show_progress:
                        print(f"  {stock_code} ({stock_name}): 插入 {insert_count} 条记录")

                # 批量提交（每处理一批股票后commit，减少锁竞争）
                if (processed % BATCH_SIZE == 0) and total_inserted > 0:
                    conn.commit()

            except Exception as e:
                print(f"[FactorCalc] {stock_code} 处理失败：{e}")
                continue

    # 最终提交
    if total_inserted > 0:
        conn.commit()

    conn.close()
    print(f"\n[FactorCalc] ====== 因子计算完成 ======")
    print(f"[FactorCalc] 总计插入 {total_inserted} 条记录")

    # 更新进度跟踪器为完成状态
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
    """
    异步版本：对指定日期范围内的新增 k 线数据计算并保存因子

    参数：
    - start_date: 开始日期 YYYY-MM-DD
    - end_date: 结束日期 YYYY-MM-DD
    - stock_codes: 股票代码列表，如不传则自动获取范围内所有股票
    - only_new_dates: 是否只计算新增日期（默认 True）
    - show_progress: 是否显示进度（默认 True）

    返回：
    - 成功计算的记录数
    """
    # 获取进度跟踪器
    tracker = get_progress_tracker()

    # 调用同步版本并传入 tracker
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(
        None,
        lambda: calculate_and_save_factors_for_dates(
            start_date,
            end_date,
            stock_codes,
            only_new_dates,
            show_progress,
            tracker
        )
    )
    return result


def fill_empty_factor_values(
    start_date: str,
    end_date: str,
    stock_codes: Optional[List[str]] = None,
    show_progress: bool = True,
    tracker: Optional['DownloadProgressTracker'] = None
) -> int:
    """
    填充因子数据：处理缺失记录 + 更新空值字段
    
    Step 1: 调用calculate_and_save_factors_for_dates处理缺失记录
    Step 2: 更新已有记录中的空值字段

    参数：
    - start_date: 开始日期 YYYY-MM-DD
    - end_date: 结束日期 YYYY-MM-DD
    - stock_codes: 股票代码列表，如不传则自动获取范围内所有股票
    - show_progress: 是否显示进度（默认 True）
    - tracker: 进度跟踪器（可选）

    返回：
    - 成功更新/插入的记录数
    """
    print(f"\n[FillEmpty] ====== 开始填充因子数据 ======")
    print(f"[FillEmpty] 日期范围：{start_date} ~ {end_date}")

    # Step 1: 处理缺失记录 - 只计算有缺失因子的股票
    print("\n[FillEmpty] Step 1: 检查缺失因子记录...")

    conn_check = sqlite3.connect(str(DB_PATH), timeout=60)
    cursor_check = conn_check.cursor()
    cursor_check.execute("PRAGMA journal_mode=WAL")
    cursor_check.execute("PRAGMA busy_timeout = 60000")

    # 获取日期范围内有K线但没有因子的股票列表
    if stock_codes is None:
        cursor_check.execute("""
            SELECT DISTINCT k.stock_code
            FROM kline_data k
            LEFT JOIN stock_daily_factors f ON k.stock_code = f.stock_code AND k.trade_date = f.trade_date
            WHERE k.trade_date >= ? AND k.trade_date <= ? AND f.stock_code IS NULL
        """, (start_date, end_date))
        missing_stock_codes = [row[0] for row in cursor_check.fetchall()]
    else:
        # 如果指定了股票列表，只检查这些股票
        placeholders = ','.join(['?' for _ in stock_codes])
        cursor_check.execute(f"""
            SELECT DISTINCT k.stock_code
            FROM kline_data k
            LEFT JOIN stock_daily_factors f ON k.stock_code = f.stock_code AND k.trade_date = f.trade_date
            WHERE k.trade_date >= ? AND k.trade_date <= ? AND f.stock_code IS NULL
            AND k.stock_code IN ({placeholders})
        """, [start_date, end_date] + stock_codes)
        missing_stock_codes = [row[0] for row in cursor_check.fetchall()]

    conn_check.close()

    missing_count = 0
    if missing_stock_codes:
        # 有缺失记录，只计算这些股票
        print(f"[FillEmpty] 发现 {len(missing_stock_codes)} 只股票有缺失因子记录")
        missing_count = calculate_and_save_factors_for_dates(
            start_date=start_date,
            end_date=end_date,
            stock_codes=missing_stock_codes,  # 只计算缺失的股票
            only_new_dates=False,  # 全量模式处理这些股票
            show_progress=show_progress,
            tracker=tracker
        )
    else:
        print("[FillEmpty] 无缺失因子记录，跳过Step 1")

    print(f"[FillEmpty] Step 1 完成: 插入 {missing_count} 条缺失记录")

    # Step 2: 更新空值记录
    print("\n[FillEmpty] Step 2: 更新空值记录...")

    conn = sqlite3.connect(str(DB_PATH), timeout=60)
    cursor = conn.cursor()
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA busy_timeout = 60000")
    cursor.execute("PRAGMA synchronous=NORMAL")

    # 关键因子字段列表（用于判断是否为空）
    KEY_FACTOR_FIELDS = ['ma5', 'ema12', 'kdj_k', 'rsi_14', 'boll_upper', 'atr_14']
    null_condition = " OR ".join([f"{f} IS NULL" for f in KEY_FACTOR_FIELDS])

    # 获取有空值因子的股票列表
    if stock_codes is None:
        cursor.execute(f"""
            SELECT DISTINCT stock_code FROM stock_daily_factors
            WHERE trade_date >= ? AND trade_date <= ?
            AND ({null_condition})
        """, (start_date, end_date))
        stock_codes = [row[0] for row in cursor.fetchall()]

    total_stocks = len(stock_codes)
    print(f"[FillEmpty] 需处理空值：{total_stocks} 只股票")

    if total_stocks == 0:
        conn.close()
        print("[FillEmpty] 无需处理的空值因子")
        return missing_count

    total_updated = 0
    BATCH_SIZE = 20

    for i in range(0, total_stocks, BATCH_SIZE):
        batch_stocks = stock_codes[i:i + BATCH_SIZE]
        processed = min(i + BATCH_SIZE, total_stocks)

        if show_progress:
            progress_pct = (processed / total_stocks) * 100
            print(f"\n[FillEmpty] 进度：{progress_pct:.1f}% | 处理 {len(batch_stocks)} 只股票 ({processed}/{total_stocks})")

        for stock_code in batch_stocks:
            try:
                # 获取有空值因子的日期
                cursor.execute(f"""
                    SELECT trade_date FROM stock_daily_factors
                    WHERE stock_code = ? AND trade_date >= ? AND trade_date <= ?
                    AND ({null_condition})
                """, (stock_code, start_date, end_date))
                null_dates = [row[0] for row in cursor.fetchall()]

                if not null_dates:
                    continue

                # 获取K线数据计算因子
                earliest_date = min(null_dates)
                cursor.execute("SELECT MIN(trade_date) FROM kline_data WHERE stock_code = ?", (stock_code,))
                kline_start = cursor.fetchone()[0] or earliest_date

                from datetime import datetime, timedelta
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

                import pandas as pd
                df = pd.DataFrame(kline_rows, columns=['trade_date', 'open', 'high', 'low', 'close', 'volume', 'amount'])
                df['stock_code'] = stock_code
                df['change_pct'] = df['close'].pct_change()

                from services.factors.daily_factor_calculator import DailyFactorCalculator
                from services.common.technical_indicators import add_all_extended_technical_indicators_to_df
                from services.common.sdk_column_mapping import map_tech_columns

                calculator = DailyFactorCalculator()
                df = calculator.calculate_price_performance(df)
                df = calculator.calculate_kdj(df)
                df = calculator.calculate_macd(df)
                df = add_all_extended_technical_indicators_to_df(df)
                df = map_tech_columns(df)

                # 更新空值字段
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
                        print(f"  {stock_code}: 更新 {update_count} 条空值记录")

                if processed % BATCH_SIZE == 0:
                    conn.commit()

            except Exception as e:
                print(f"[FillEmpty] {stock_code} 处理失败：{e}")
                continue

    conn.commit()
    conn.close()

    # 通知进度跟踪器完成
    if tracker:
        tracker.complete_sync()

    print(f"\n[FillEmpty] ====== 填充完成 ======")
    print(f"[FillEmpty] 缺失记录: {missing_count} 条")
    print(f"[FillEmpty] 空值更新: {total_updated} 条")
    print(f"[FillEmpty] 总计: {missing_count + total_updated} 条")

    return missing_count + total_updated


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

    参数：
    - start_date: 开始日期 YYYY-MM-DD
    - end_date: 结束日期 YYYY-MM-DD
    - stock_codes: 股票代码列表，如不传则自动获取范围内所有股票
    - show_progress: 是否显示进度（默认 True）
    - tracker: 进度跟踪器（可选）

    返回：
    - {
        'inserted': int,    # 新增记录数
        'updated': int,     # 更新空值数
        'skipped': int      # 跳过已有完整记录数
      }
    """
    from services.factors.daily_factor_calculator import DailyFactorCalculator
    from services.common.technical_indicators import add_all_extended_technical_indicators_to_df
    from services.common.sdk_column_mapping import map_tech_columns

    # numpy类型转换函数：避免numpy.int64/float64被SQLite存储为blob
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

    print(f"\n[SmartUpdate] ====== 开始智能因子更新 ======")
    print(f"[SmartUpdate] 日期范围：{start_date} ~ {end_date}")

    conn = sqlite3.connect(str(DB_PATH), timeout=60)
    cursor = conn.cursor()
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA busy_timeout = 60000")
    cursor.execute("PRAGMA synchronous=NORMAL")

    # ========== Step 1: 批量检测需要处理的日期 ==========
    print("\n[SmartUpdate] Step 1: 批量检测缺失/空值日期...")

    # 1.1 获取缺失记录（有K线无因子）
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

    missing_records = cursor.fetchall()  # [(stock_code, trade_date), ...]

    # 1.2 获取有空值字段的记录
    KEY_FACTOR_FIELDS = ['ma5', 'ema12', 'kdj_k', 'rsi_14', 'boll_upper', 'atr_14']
    null_condition = " OR ".join([f"{f} IS NULL" for f in KEY_FACTOR_FIELDS])

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

    null_records = cursor.fetchall()  # [(stock_code, trade_date), ...]

    # 1.3 合并并按股票分组
    # 缺失记录 -> 需要INSERT
    # 空值记录 -> 需要UPDATE
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

    # 合并所有需要处理的股票
    all_stocks = set(missing_by_stock.keys()) | set(null_by_stock.keys())

    total_stocks = len(all_stocks)
    print(f"[SmartUpdate] 发现 {len(missing_records)} 条缺失记录，{len(null_records)} 条空值记录")
    print(f"[SmartUpdate] 涉及 {total_stocks} 只股票需要处理")

    if total_stocks == 0:
        conn.close()
        print("[SmartUpdate] 无需处理，直接返回")
        return {'inserted': 0, 'updated': 0, 'skipped': 0}

    # ========== Step 2: 计算因子 ==========
    print("\n[SmartUpdate] Step 2: 计算因子...")

    total_inserted = 0
    total_updated = 0
    calculator = DailyFactorCalculator()

    BATCH_SIZE = 50
    all_stocks_list = list(all_stocks)
    total_batches = (total_stocks + BATCH_SIZE - 1) // BATCH_SIZE

    if tracker:
        tracker.start(total_stocks=total_stocks, total_batches=total_batches)

    for batch_idx in range(0, total_stocks, BATCH_SIZE):
        batch_stocks = all_stocks_list[batch_idx:batch_idx + BATCH_SIZE]
        processed = min(batch_idx + BATCH_SIZE, total_stocks)

        if show_progress:
            progress_pct = (processed / total_stocks) * 100
            print(f"\n[SmartUpdate] 进度：{progress_pct:.1f}% | 处理 {len(batch_stocks)} 只股票 ({processed}/{total_stocks})")

        if tracker:
            tracker.update_sync(processed=processed, message=f"计算因子：批次 {batch_idx//BATCH_SIZE + 1}")

        for stock_code in batch_stocks:
            try:
                # 获取股票名称
                stock_name = ''
                cursor.execute(
                    "SELECT DISTINCT stock_name FROM kline_data WHERE stock_code = ? LIMIT 1",
                    (stock_code,)
                )
                row = cursor.fetchone()
                if row and row[0]:
                    stock_name = row[0]

                # 获取该股票需要处理的所有日期
                missing_dates = missing_by_stock.get(stock_code, [])
                null_dates = null_by_stock.get(stock_code, [])
                all_dates = set(missing_dates) | set(null_dates)

                if not all_dates:
                    continue

                # 获取最早的日期，前溯120天获取历史数据用于计算
                earliest_date = min(all_dates)
                query_start_dt = datetime.strptime(earliest_date, '%Y-%m-%d') - timedelta(days=120)
                cursor.execute("SELECT MIN(trade_date) FROM kline_data WHERE stock_code = ?", (stock_code,))
                kline_start = cursor.fetchone()[0] or earliest_date
                calc_start = max(kline_start, query_start_dt.strftime('%Y-%m-%d'))

                # 获取K线数据
                cursor.execute("""
                    SELECT trade_date, open, high, low, close, volume, amount
                    FROM kline_data WHERE stock_code = ? AND trade_date >= ? AND trade_date <= ?
                    ORDER BY trade_date
                """, (stock_code, calc_start, end_date))
                kline_rows = cursor.fetchall()

                if not kline_rows:
                    continue

                # 转换为 DataFrame
                df = pd.DataFrame(kline_rows, columns=['trade_date', 'open', 'high', 'low', 'close', 'volume', 'amount'])
                df['stock_code'] = stock_code
                df['change_pct'] = df['close'].pct_change()

                # 计算技术指标
                df = calculator.calculate_price_performance(df)
                df = calculator.calculate_kdj(df)
                df = calculator.calculate_macd(df)
                df = add_all_extended_technical_indicators_to_df(df)
                df = map_tech_columns(df)

                # 计算补充技术信号指标
                df['golden_cross'] = ((df['ma5'] > df['ma10']) & (df['ma5'].shift(1) <= df['ma10'].shift(1))).astype(int)
                df['death_cross'] = ((df['ma5'] < df['ma10']) & (df['ma5'].shift(1) >= df['ma10'].shift(1))).astype(int)
                df['is_limit_up'] = (df['change_pct'] * 100 > 9.5).astype(int)
                df['limit_up_count_10d'] = df['is_limit_up'].rolling(10, min_periods=1).sum()
                df['limit_up_count_20d'] = df['is_limit_up'].rolling(20, min_periods=1).sum()
                df['limit_up_count_30d'] = df['is_limit_up'].rolling(30, min_periods=1).sum()
                df['consecutive_limit_up'] = 0
                consec_count = 0
                for idx in range(len(df)):
                    if df['is_limit_up'].iloc[idx] == 1:
                        consec_count += 1
                    else:
                        consec_count = 0
                    df.loc[df.index[idx], 'consecutive_limit_up'] = consec_count
                df['large_gain_5d_count'] = (df['change_pct'] * 100 > 5).rolling(5, min_periods=1).sum().astype(int)
                df['large_loss_5d_count'] = (df['change_pct'] * 100 < -5).rolling(5, min_periods=1).sum().astype(int)
                high_250 = df['high'].rolling(250, min_periods=1).max()
                low_250 = df['low'].rolling(250, min_periods=1).min()
                range_val = high_250 - low_250
                df['close_to_high_250d'] = (df['close'] - low_250) / range_val * 100
                df['close_to_low_250d'] = (high_250 - df['close']) / range_val * 100
                df['gap_up'] = ((df['open'] > df['high'].shift(1)) & (df['open'] > df['low'].shift(1))).astype(int)
                df['gap_up_ratio'] = df['gap_up'].rolling(20, min_periods=1).sum() / 20 * 100
                df['next_period_change'] = df['close'].pct_change().shift(-1)
                df['is_traded'] = (df['volume'] > 0).astype(int)

                # 获取市值数据
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

                    # 计算市值
                    close_price = row['close']
                    circ_market_cap = None
                    total_market_cap = None
                    if close_price and close_price > 0:
                        if float_share and float_share > 0:
                            circ_market_cap = round(close_price * float_share / 10000, 2)
                        if total_share and total_share > 0:
                            total_market_cap = round(close_price * total_share / 10000, 2)

                    # 计算上市天数
                    days_since_ipo = None
                    if ipo_date:
                        try:
                            ipo_dt = datetime.strptime(ipo_date, '%Y-%m-%d')
                            trade_dt = datetime.strptime(trade_date, '%Y-%m-%d')
                            days_since_ipo = (trade_dt - ipo_dt).days
                        except:
                            pass

                    # INSERT
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
                        print(f"[SmartUpdate] INSERT {stock_code} {trade_date} 失败：{e}")
                        continue

                # 处理空值记录（UPDATE）
                for trade_date in null_dates:
                    row_data = df[df['trade_date'] == trade_date]
                    if row_data.empty:
                        continue
                    row = row_data.iloc[0]

                    # 构建UPDATE语句，只更新空值字段
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
                            print(f"[SmartUpdate] UPDATE {stock_code} {trade_date} 失败：{e}")
                            continue

                if processed % BATCH_SIZE == 0:
                    conn.commit()

            except Exception as e:
                print(f"[SmartUpdate] {stock_code} 处理失败：{e}")
                continue

    conn.commit()
    conn.close()

    if tracker:
        tracker.complete_sync(inserted=total_inserted, updated=total_updated)

    result = {
        'inserted': total_inserted,
        'updated': total_updated,
        'skipped': 0
    }

    print(f"\n[SmartUpdate] ====== 智能因子更新完成 ======")
    print(f"[SmartUpdate] 新增记录: {total_inserted} 条")
    print(f"[SmartUpdate] 更新空值: {total_updated} 条")
    print(f"[SmartUpdate] 总计处理: {total_inserted + total_updated} 条")

    return result


async def download_all_kline_data(
    batch_size: int = 20,
    months: int = 24,
    start_date: str = None,
    end_date: str = None,
    broker_account: str = "",
    broker_password: str = "",
    calculate_factors: bool = True,
    market_filter: Optional[List[str]] = None
):
    """
    异步下载全量 K 线数据

    Args:
        batch_size: 每批次下载的股票数量（默认 20）
        months: 下载的月数（默认 24 个月）
        start_date: 开始日期（YYYY-MM-DD）
        end_date: 结束日期（YYYY-MM-DD）
        broker_account: 券商账户
        broker_password: 券商密码
        calculate_factors: 是否计算因子
        market_filter: 市场筛选列表 ['SH', 'SZ', 'BJ']

    Returns:
        下载是否成功
    """
    from services.trading.gateway import get_gateway_for_account
    from datetime import datetime, timedelta

    # 获取进度跟踪器
    tracker = get_progress_tracker()
    # 使用同步方法重置（reset是async，在后台任务中可能不生效）
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

    # 获取网关
    gateway = await get_gateway_for_account({
        'broker_account': broker_account,
        'broker_password': broker_password
    })

    if not gateway:
        tracker.set_status_sync(DownloadStatus.ERROR, "网关初始化失败")
        return False

    # 确定日期范围
    if not start_date or not end_date:
        current_time = datetime.now(CHINA_TZ)
        if not end_date:
            end_date = current_time.strftime('%Y-%m-%d')
        if not start_date:
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
            start_dt = end_dt - timedelta(days=months * 30)
            start_date = start_dt.strftime('%Y-%m-%d')

    print(f"[LocalData] 下载日期范围：{start_date} 至 {end_date}")

    # 获取股票列表
    tracker.set_status_sync(DownloadStatus.DOWNLOADING, "获取股票列表...")
    stock_list = await gateway.get_stock_list()

    # 市场筛选
    if market_filter:
        stock_list = [s for s in stock_list if s.get('market') in market_filter]

    total_stocks = len(stock_list)
    print(f"[LocalData] 获取到 {total_stocks} 只股票")

    # 初始化进度（设置总数和开始时间）
    tracker._total_stocks = total_stocks
    tracker._total_tasks = total_stocks
    tracker._start_time = get_china_time()
    tracker.set_status_sync(DownloadStatus.DOWNLOADING, f"开始下载 {total_stocks} 只股票K线...")

    # 本地数据服务
    local_service = get_local_data_service()
    downloaded_count = 0
    failed_count = 0

    # 分批下载
    for i in range(0, total_stocks, batch_size):
        batch = stock_list[i:i + batch_size]
        # SDK需要完整股票代码格式：CODE.MARKET（如 600000.SH）
        batch_codes = [f"{s.get('code')}.{s.get('market')}" for s in batch]

        progress = min(i + batch_size, total_stocks)
        tracker.update_sync(
            processed=progress,
            current_stock=batch_codes[0] if batch_codes else "",
            message=f"下载批次 {i//batch_size + 1}/{(total_stocks-1)//batch_size + 1}"
        )

        try:
            # 批量获取K线数据（日期需要转换为 YYYYMMDD 格式）
            start_date_int = start_date.replace('-', '') if start_date else None
            end_date_int = end_date.replace('-', '') if end_date else None
            kline_data = await gateway.get_batch_kline_data(
                stock_codes=batch_codes,
                start_date=start_date_int,
                end_date=end_date_int
            )

            # 准备批量保存的数据
            save_batch = []
            for stock_info in batch:
                # 完整股票代码格式：CODE.MARKET
                full_code = f"{stock_info.get('code')}.{stock_info.get('market')}"
                name = stock_info.get('name', full_code)
                df = kline_data.get(full_code)  # kline_data 键是完整格式
                if df is not None and len(df) > 0:
                    save_batch.append((full_code, name, df))
                    downloaded_count += 1
                else:
                    # BJ市场无数据不计入失败
                    market = stock_info.get('market')
                    if market != 'BJ':
                        failed_count += 1

            # 批量保存
            if save_batch:
                saved = local_service.save_kline_data_batch(save_batch)
                print(f"[LocalData] 批次 {i//batch_size + 1}: 保存 {saved} 条记录")

        except Exception as e:
            print(f"[LocalData] 批次下载失败：{e}")
            failed_count += len(batch)

    # 计算因子
    if calculate_factors and downloaded_count > 0:
        tracker.set_status_sync(DownloadStatus.CALCULATING_FACTORS, "开始计算因子...")
        try:
            factor_count = calculate_and_save_factors_for_dates(
                start_date=start_date,
                end_date=end_date,
                only_new_dates=True,
                show_progress=True
            )
            print(f"[LocalData] 因子计算完成：插入 {factor_count} 条记录")
        except Exception as e:
            print(f"[LocalData] 因子计算失败：{e}")

    tracker.complete_sync()

    return failed_count < total_stocks * 0.5  # 失败不超过50%视为成功


def download_all_kline_data_sync(
    batch_size: int = 20,
    months: int = 24,
    start_date: str = None,
    end_date: str = None,
    broker_account: str = "",
    broker_password: str = "",
    calculate_factors: bool = True,
    market_filter: Optional[List[str]] = None
):
    """同步版本的下载函数，用于后台任务

    Args:
        batch_size: 每批次下载的股票数量（默认 20，避免 SDK 连接数超限）
        months: 下载的月数（默认 24 个月=2 年）
        start_date: 开始日期（YYYY-MM-DD），可选
        end_date: 结束日期（YYYY-MM-DD），可选
        broker_account: 银河证券资金账号
        broker_password: 银河证券资金密码
        calculate_factors: 下载完成后是否自动计算因子
        market_filter: 市场筛选列表，如 ['SH', 'SZ'] 只下载沪深 A 股，['BJ'] 只下载北交所股票
    """
    import asyncio

    async def _async_download():
        return await download_all_kline_data(
            batch_size=batch_size,
            months=months,
            start_date=start_date,
            end_date=end_date,
            broker_account=broker_account,
            broker_password=broker_password,
            calculate_factors=calculate_factors,
            market_filter=market_filter
        )

    # 创建新的事件循环来运行异步函数
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop.run_until_complete(_async_download())
    finally:
        loop.close()


async def download_incremental_kline_data(
    batch_size: int = 20,
    months: int = 6,
    broker_account: str = "",
    broker_password: str = "",
    calculate_factors: bool = True,
    use_trading_time_rule: bool = True,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
):
    """
    增量下载 K 线数据（带交易时间检查）

    下载时间逻辑：
    - 如果指定了 start_date 和 end_date：直接使用用户指定的日期范围
    - use_trading_time_rule=True 时：
      - 交易日早于 16:00 → 结束日期 = 前一交易日
      - 交易日晚于 16:00 → 结束日期 = 当日
      - 非交易日 → 结束日期 = 前一个交易日
    - use_trading_time_rule=False 时：
      - 使用当前日期作为结束日期（忽略交易时间）

    Args:
        batch_size: 每批次下载的股票数量（默认 20，避免 SDK 连接数超限）
        months: 下载的月数（默认 6 个月，用于增量下载）
        broker_account: 银河证券资金账号
        broker_password: 银河证券资金密码
        calculate_factors: 下载完成后是否自动计算因子
        use_trading_time_rule: 是否应用交易时间规则（默认 True）
        start_date: 开始日期（YYYY-MM-DD），指定则直接使用
        end_date: 结束日期（YYYY-MM-DD），指定则直接使用

    Returns:
        是否成功完成下载
    """
    # 获取当前中国时区时间
    current_time = datetime.now(CHINA_TZ)

    # 确定下载结束日期
    if start_date and end_date:
        # 用户指定了日期范围，直接使用
        status_msg = f"使用用户指定的日期范围：{start_date} 至 {end_date}"
        print(f"[LocalData] {status_msg}")
        print(f"[LocalData] 当前时间：{current_time.strftime('%Y-%m-%d %H:%M:%S')}")
    else:
        # 自动计算日期范围
        end_date, status_msg = get_trading_day_end_date(current_time, use_sdk_calendar=use_trading_time_rule)
        print(f"[LocalData] {status_msg}")
        print(f"[LocalData] 当前时间：{current_time.strftime('%Y-%m-%d %H:%M:%S')}")

        # 计算开始日期
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
        calculate_factors=calculate_factors
    )


def download_incremental_kline_data_sync(
    batch_size: int = 20,
    months: int = 6,
    broker_account: str = "",
    broker_password: str = "",
    calculate_factors: bool = True,
    use_trading_time_rule: bool = True,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
):
    """同步版本的增量下载函数

    Args:
        batch_size: 每批次下载的股票数量（默认 20，避免 SDK 连接数超限）
        months: 下载的月数（默认 6 个月）
        broker_account: 银河证券资金账号
        broker_password: 银河证券资金密码
        calculate_factors: 下载完成后是否自动计算因子
        use_trading_time_rule: 是否应用交易时间规则（默认 True）
        start_date: 开始日期（YYYY-MM-DD），指定则直接使用
        end_date: 结束日期（YYYY-MM-DD），指定则直接使用
    """
    import asyncio

    async def _async_download():
        return await download_incremental_kline_data(
            batch_size=batch_size,
            months=months,
            broker_account=broker_account,
            broker_password=broker_password,
            calculate_factors=calculate_factors,
            use_trading_time_rule=use_trading_time_rule,
            start_date=start_date,
            end_date=end_date
        )

    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop.run_until_complete(_async_download())
    finally:
        loop.close()
