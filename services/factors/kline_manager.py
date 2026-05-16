"""
统一 K 线数据管理

从 local_data_service.py 和 download_weekly_kline.py 提取单一实现：
1. kline.db 路径管理
2. 日K线 / 周K线 数据库读写
3. 个股交易周日历构建
4. 日期格式标准化
5. 批量股票日期范围查询

使用示例:
    from services.factors.kline_manager import KlineManager, get_kline_manager

    km = get_kline_manager()

    # 查询
    df = km.get_kline_data("600000.SH", start_date="2026-01-01")
    stocks = km.get_all_stocks()
    ranges = km.get_stocks_date_ranges_batch(["600000.SH"], "2026-01-01", "2026-04-01")

    # 写入
    saved = km.save_kline_data("600000.SH", "浦发银行", sdk_df)
    saved = km.save_weekly_kline_data("600000.SH", "浦发银行", weekly_df, week_calendar)

    # 周日历
    week_cal = km.build_stock_week_calendar("600000.SH", "2026-01-01", "2026-04-01")
"""

import sqlite3
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple

from services.common.database import KLINE_DB_PATH, get_sync_connection
from services.common.timezone import get_china_time


class KlineManager:
    """K 线数据统一管理"""

    def __init__(self, db_path: Optional[Path] = None):
        self.db_path = db_path or KLINE_DB_PATH

    def _conn(self, timeout: int = 30) -> sqlite3.Connection:
        """获取预配置的 kline.db 连接（WAL mode, busy_timeout=10000）"""
        return get_sync_connection("kline", path=self.db_path)

    # ================================================================
    # 数据库查询
    # ================================================================

    def get_global_latest_date(self) -> Optional[str]:
        """获取 kline_data 表中全局最新的交易日期"""
        conn = self._conn()
        cursor = conn.cursor()
        cursor.execute('SELECT MAX(trade_date) FROM kline_data')
        result = cursor.fetchone()
        return result[0] if result and result[0] else None

    def get_stock_count_on_date(self, trade_date: str) -> int:
        """获取指定交易日期有数据的股票数量"""
        conn = self._conn()
        cursor = conn.cursor()
        cursor.execute(
            'SELECT COUNT(DISTINCT stock_code) FROM kline_data WHERE trade_date = ?',
            (trade_date,)
        )
        result = cursor.fetchone()
        return result[0] if result else 0

    def get_total_stock_count(self) -> int:
        """获取 kline_data 表中总的股票数量"""
        conn = self._conn()
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(DISTINCT stock_code) FROM kline_data')
        result = cursor.fetchone()
        return result[0] if result else 0

    def get_latest_date(self, stock_code: str) -> Optional[str]:
        """获取某只股票的最新交易日期"""
        conn = self._conn()
        cursor = conn.cursor()
        cursor.execute(
            'SELECT MAX(trade_date) FROM kline_data WHERE stock_code = ?',
            (stock_code,)
        )
        result = cursor.fetchone()
        return result[0] if result and result[0] else None

    def get_kline_count(self, stock_code: str) -> int:
        """获取某只股票的 K 线记录数"""
        conn = self._conn()
        cursor = conn.cursor()
        cursor.execute(
            'SELECT COUNT(*) FROM kline_data WHERE stock_code = ?',
            (stock_code,)
        )
        result = cursor.fetchone()
        return result[0] if result else 0

    def get_kline_data(
        self, stock_code: str, start_date: Optional[str] = None,
        end_date: Optional[str] = None, limit: Optional[int] = None
    ) -> pd.DataFrame:
        """
        从数据库获取 K 线数据。

        Returns:
            DataFrame with columns: stock_code, stock_name, trade_date,
            open, high, low, close, volume, amount
        """
        conn = self._conn()
        query = 'SELECT * FROM kline_data WHERE stock_code = ?'
        params: list = [stock_code]

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

        df = pd.read_sql_query(query, conn, params=params)
        return df

    def get_all_stocks(self) -> List[str]:
        """获取数据库中所有股票代码"""
        conn = self._conn()
        cursor = conn.cursor()
        cursor.execute('SELECT DISTINCT stock_code FROM kline_data')
        stocks = [row[0] for row in cursor.fetchall()]
        return stocks

    def get_all_trade_dates(self, start_date: Optional[str] = None,
                             end_date: Optional[str] = None) -> List[str]:
        """获取所有交易日期列表"""
        conn = self._conn()
        query = 'SELECT DISTINCT trade_date FROM kline_data'
        params: list = []
        if start_date and end_date:
            query += ' WHERE trade_date >= ? AND trade_date <= ?'
            params.extend([start_date, end_date])
        query += ' ORDER BY trade_date ASC'
        cursor = conn.cursor()
        cursor.execute(query, params)
        return [row[0] for row in cursor.fetchall()]

    def get_stocks_date_ranges_batch(
        self, stock_codes: List[str], start_date: str, end_date: str
    ) -> Dict[str, Dict]:
        """
        批量获取多只股票在指定日期范围内的数据统计。

        Returns:
            {stock_code: {'latest_date': str, 'earliest_date': str, 'count': int}}
        """
        if not stock_codes:
            return {}

        conn = self._conn()
        cursor = conn.cursor()
        placeholders = ','.join(['?' for _ in stock_codes])
        cursor.execute(f'''
            SELECT stock_code, MAX(trade_date), MIN(trade_date), COUNT(*)
            FROM kline_data
            WHERE stock_code IN ({placeholders})
              AND trade_date >= ? AND trade_date <= ?
            GROUP BY stock_code
        ''', stock_codes + [start_date, end_date])

        results = {}
        for row in cursor.fetchall():
            results[row[0]] = {
                'latest_date': row[1],
                'earliest_date': row[2],
                'count': row[3]
            }
        return results

    def get_stocks_existing_dates_batch(
        self, stock_codes: List[str], start_date: str, end_date: str
    ) -> Dict[str, List[str]]:
        """
        批量获取每只股票在目标日期范围内实际存在的交易日期列表。

        Returns:
            {stock_code: [trade_date1, trade_date2, ...]}
        """
        if not stock_codes:
            return {}

        conn = self._conn()
        cursor = conn.cursor()
        placeholders = ','.join(['?' for _ in stock_codes])
        cursor.execute(f'''
            SELECT stock_code, trade_date
            FROM kline_data
            WHERE stock_code IN ({placeholders})
              AND trade_date >= ? AND trade_date <= ?
            ORDER BY stock_code, trade_date
        ''', stock_codes + [start_date, end_date])

        results: Dict[str, List[str]] = {}
        for row in cursor.fetchall():
            results.setdefault(row[0], []).append(row[1])
        return results

    def get_stocks_earliest_dates_batch(
        self, stock_codes: List[str]
    ) -> Dict[str, str]:
        """
        批量获取每只股票在数据库中的最早交易日期。

        Returns:
            {stock_code: 'YYYY-MM-DD'}
        """
        if not stock_codes:
            return {}

        conn = self._conn()
        cursor = conn.cursor()
        placeholders = ','.join(['?' for _ in stock_codes])
        cursor.execute(f'''
            SELECT stock_code, MIN(trade_date)
            FROM kline_data
            WHERE stock_code IN ({placeholders})
            GROUP BY stock_code
        ''', stock_codes)

        results = {}
        for row in cursor.fetchall():
            results[row[0]] = row[1]
        return results

    def get_stock_date_range(self, stock_code: str) -> Tuple[Optional[str], Optional[str]]:
        """获取某只股票的日期范围（最新日期，最早日期）"""
        conn = self._conn()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT MAX(trade_date), MIN(trade_date)
            FROM kline_data WHERE stock_code = ?
        ''', (stock_code,))
        result = cursor.fetchone()
        if result and result[0]:
            return (result[0], result[1])
        return (None, None)

    def get_batch_kline(
        self, stock_codes: List[str], start_date: str, end_date: str,
        limit_per_stock: int = 100
    ) -> Dict[str, pd.DataFrame]:
        """
        批量获取多只股票的 K 线数据，每只股票最多 limit_per_stock 条。

        Returns:
            {stock_code: DataFrame}
        """
        if not stock_codes:
            return {}

        conn = self._conn()
        placeholders = ','.join(['?' for _ in stock_codes])

        query = f'''
            SELECT * FROM kline_data k1
            WHERE stock_code IN ({placeholders})
              AND trade_date >= ? AND trade_date <= ?
              AND (
                  SELECT COUNT(*) FROM kline_data k2
                  WHERE k2.stock_code = k1.stock_code
                    AND k2.trade_date >= k1.trade_date
                    AND k2.trade_date <= ?
              ) <= ?
            ORDER BY k1.stock_code, k1.trade_date ASC
        '''
        params = stock_codes + [start_date, end_date, end_date, limit_per_stock]
        df = pd.read_sql_query(query, conn, params=params)

        if df.empty:
            return {}

        result = {}
        for code, group in df.groupby('stock_code'):
            result[code] = group
        return result

    # ================================================================
    # 周K线查询
    # ================================================================

    def get_weekly_latest(self) -> Dict[str, str]:
        """获取每只股票已有周K线的最新 week_end_date。

        Returns:
            {stock_code: latest_week_end_date}
        """
        conn = self._conn()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT stock_code, MAX(week_end_date)
            FROM weekly_kline_data
            GROUP BY stock_code
        """)
        result = {row[0]: row[1] for row in cursor.fetchall()}
        return result

    def get_weekly_earliest(self) -> Dict[str, str]:
        """获取每只股票已有周K线的最早 week_end_date。

        Returns:
            {stock_code: earliest_week_end_date}
        """
        conn = self._conn()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT stock_code, MIN(week_end_date)
            FROM weekly_kline_data
            GROUP BY stock_code
        """)
        result = {row[0]: row[1] for row in cursor.fetchall()}
        return result

    def get_weekly_data(
        self, stock_code: str, limit: Optional[int] = None
    ) -> pd.DataFrame:
        """获取某只股票的周K线数据"""
        conn = self._conn()
        query = 'SELECT * FROM weekly_kline_data WHERE stock_code = ? ORDER BY week_end_date ASC'
        params: list = [stock_code]
        if limit:
            query = query.replace('ASC', 'ASC LIMIT ?')
            params.append(limit)

        df = pd.read_sql_query(query, conn, params=params)
        return df

    def get_weekly_stock_count(self) -> int:
        """获取周K线表中覆盖的股票数量"""
        conn = self._conn()
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(DISTINCT stock_code) FROM weekly_kline_data')
        result = cursor.fetchone()
        return result[0] if result else 0

    def get_weekly_latest_date(self) -> Optional[str]:
        """获取周K线表中最晚的 week_end_date"""
        conn = self._conn()
        cursor = conn.cursor()
        cursor.execute('SELECT MAX(week_end_date) FROM weekly_kline_data')
        result = cursor.fetchone()
        return result[0] if result and result[0] else None

    def delete_by_date(self, trade_date: str) -> int:
        """删除指定交易日的全部数据。

        Returns:
            删除的记录数
        """
        conn = self._conn()
        cursor = conn.cursor()
        cursor.execute(
            "DELETE FROM kline_data WHERE trade_date = ?",
            (trade_date,)
        )
        deleted = cursor.rowcount
        conn.commit()
        return deleted

    def delete_incomplete_week(self, cutoff_date: str) -> int:
        """删除 cutoff_date 之后的不完整周数据。

        Returns:
            删除的记录数
        """
        conn = self._conn()
        cursor = conn.cursor()
        cursor.execute(
            "DELETE FROM weekly_kline_data WHERE week_end_date > ?",
            (cutoff_date,)
        )
        deleted = cursor.rowcount
        conn.commit()
        return deleted

    # ================================================================
    # 数据库写入
    # ================================================================

    def save_kline_data(
        self, stock_code: str, stock_name: str, df: pd.DataFrame
    ) -> int:
        """
        保存日K线数据到数据库（INSERT OR REPLACE）。

        Returns:
            保存的记录数
        """
        if df is None or len(df) == 0:
            return 0

        conn = self._conn(timeout=30)
        cursor = conn.cursor()
        saved_count = 0

        try:
            for _, row in df.iterrows():
                trade_date = self._normalize_date(row.get('trade_date'))
                if not trade_date:
                    continue

                cursor.execute('''
                    INSERT OR REPLACE INTO kline_data
                    (stock_code, stock_name, trade_date, open, high, low, close, volume, amount)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    stock_code, stock_name, trade_date,
                    float(row.get('open', 0)),
                    float(row.get('high', 0)),
                    float(row.get('low', 0)),
                    float(row.get('close', 0)),
                    int(row.get('volume', 0)),
                    float(row.get('amount', 0))
                ))
                saved_count += 1
            conn.commit()
        except Exception:
            conn.rollback()
            raise

        return saved_count

    def save_kline_data_batch(
        self, kline_batch: List[Tuple[str, str, pd.DataFrame]]
    ) -> int:
        """
        批量保存多只股票的日K线数据（单事务）。

        Args:
            kline_batch: [(stock_code, stock_name, df), ...]

        Returns:
            保存的记录总数
        """
        if not kline_batch:
            return 0

        conn = self._conn(timeout=60)
        cursor = conn.cursor()
        total_saved = 0

        try:
            for stock_code, stock_name, df in kline_batch:
                if df is None or len(df) == 0:
                    continue

                # 处理行业指数的大写列名 + 索引
                if 'OPEN' in df.columns or df.index.name == 'TRADE_DATE':
                    if df.index.name == 'TRADE_DATE':
                        df = df.reset_index()
                    df.columns = df.columns.str.lower()

                for _, row in df.iterrows():
                    trade_date = self._normalize_date(row.get('trade_date'))
                    if not trade_date:
                        continue

                    cursor.execute('''
                        INSERT OR REPLACE INTO kline_data
                        (stock_code, stock_name, trade_date, open, high, low, close, volume, amount)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        stock_code, stock_name, trade_date,
                        float(row.get('open', 0)),
                        float(row.get('high', 0)),
                        float(row.get('low', 0)),
                        float(row.get('close', 0)),
                        int(row.get('volume', 0)),
                        float(row.get('amount', 0))
                    ))
                    total_saved += 1

            conn.commit()
        except Exception:
            conn.rollback()
            raise

        return total_saved

    def save_weekly_kline_data(
        self, stock_code: str, stock_name: str, data,
        week_calendar: Optional[List[Tuple[int, int, str, str]]] = None,
        skip_existing_before: Optional[str] = None
    ) -> int:
        """
        保存单只股票的周K线数据。

        使用 SDK 返回的 trade_date 直接计算周边界（周一至周五）。
        不再依赖预构建日历，避免日历覆盖不足导致数据丢失。

        Args:
            stock_code: 股票代码
            stock_name: 股票名称
            data: SDK 返回的周K线 DataFrame
            week_calendar: 已废弃，保留兼容性
            skip_existing_before: 跳过此日期之前的周（增量更新用）

        Returns:
            保存的记录数
        """
        if isinstance(data, pd.DataFrame):
            df = data
        else:
            df = pd.DataFrame(data)

        if df.empty:
            return 0

        conn = self._conn()
        cursor = conn.cursor()
        saved = 0

        for _, row in df.iterrows():
            # 从 trade_date 获取周结束日期（ISO周五）
            trade_date_raw = row.get('trade_date', '')
            if not trade_date_raw or trade_date_raw == '':
                continue

            week_ref_str = self._normalize_date(trade_date_raw)
            if not week_ref_str:
                continue

            # SDK 周线的 trade_date 通常是该周第一个交易日（可能为周一），
            # 需要计算该 ISO 周的周五作为 week_end_date
            try:
                ref_dt = datetime.strptime(week_ref_str, '%Y-%m-%d')
                iso_y, iso_w, _ = ref_dt.isocalendar()
                # ISO 周五 = ISO Monday + 4 days
                monday_dt = datetime.fromisocalendar(iso_y, iso_w, 1)
                friday_dt = monday_dt + timedelta(days=4)
                week_start_str = monday_dt.strftime('%Y-%m-%d')
                week_end_str = friday_dt.strftime('%Y-%m-%d')
            except (ValueError, TypeError):
                continue

            # 跳过已有数据
            if skip_existing_before and week_end_str <= skip_existing_before:
                continue

            try:
                cursor.execute("""
                    INSERT OR REPLACE INTO weekly_kline_data
                    (stock_code, stock_name, week_start_date, week_end_date,
                     open, high, low, close, volume, amount)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    stock_code, stock_name, week_start_str, week_end_str,
                    float(row.get('open', 0)) if not pd.isna(row.get('open')) else None,
                    float(row.get('high', 0)) if not pd.isna(row.get('high')) else None,
                    float(row.get('low', 0)) if not pd.isna(row.get('low')) else None,
                    float(row.get('close', 0)) if not pd.isna(row.get('close')) else None,
                    int(row.get('volume', 0)) if not pd.isna(row.get('volume')) else None,
                    float(row.get('amount', 0)) if not pd.isna(row.get('amount')) else None
                ))
                saved += 1
            except Exception:
                continue

        conn.commit()
        return saved

    # ================================================================
    # 交易周日历构建
    # ================================================================

    def build_stock_week_calendar(
        self, stock_code: str, start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> List[Tuple[int, int, str, str]]:
        """
        从个股日K线数据构建交易周参考表。

        按 ISO 周分组该股票的交易日，返回
        [(iso_year, iso_week, week_start, week_end), ...]
        其中 week_start/end 是该股票实际最早/最晚交易日。

        关键：用个股自身交易日构建日历，停牌周不会计入。
        """
        conn = self._conn()
        cursor = conn.cursor()

        if start_date and end_date:
            cursor.execute("""
                SELECT DISTINCT trade_date FROM kline_data
                WHERE stock_code = ? AND trade_date >= ? AND trade_date <= ?
                ORDER BY trade_date
            """, (stock_code, start_date, end_date))
        else:
            cursor.execute("""
                SELECT DISTINCT trade_date FROM kline_data
                WHERE stock_code = ? ORDER BY trade_date
            """, (stock_code,))

        trade_days = [r[0] for r in cursor.fetchall()]

        if not trade_days:
            return []

        # 按 ISO 周分组
        weeks: Dict[Tuple[int, int], List[str]] = {}
        for day_str in trade_days:
            d = datetime.strptime(day_str, '%Y-%m-%d')
            iso_year, iso_week, _ = d.isocalendar()
            key = (iso_year, iso_week)
            weeks.setdefault(key, []).append(day_str)

        return [
            (iso_y, iso_w, days[0], days[-1])
            for (iso_y, iso_w), days in sorted(weeks.items())
        ]

    # ================================================================
    # 工具方法
    # ================================================================

    @staticmethod
    def get_last_completed_week_end(now: Optional[datetime] = None) -> datetime:
        """
        计算最近一个完整的交易周结束日期（周五）。

        规则：
        - 周一到周五 16:00 前：本周未完成，返回上周五
        - 周五 16:00 后 或 周末：本周已完成，返回本周五
        """
        now = now or get_china_time()
        weekday = now.weekday()  # 0=周一, 4=周五, 5=周六, 6=周日

        if weekday <= 3:
            return now - timedelta(days=weekday + 3)
        elif weekday == 4:
            if now.hour < 16:
                return now - timedelta(days=7)
            else:
                return now
        else:
            return now - timedelta(days=weekday - 4)

    @staticmethod
    def _normalize_date(date_val) -> Optional[str]:
        """
        标准化日期格式为 YYYY-MM-DD。

        支持：pd.Timestamp, int(20260408), str(YYYY-MM-DD), str(YYYYMMDD)
        """
        if not date_val or date_val == '':
            return None

        if isinstance(date_val, pd.Timestamp):
            return date_val.strftime('%Y-%m-%d')
        elif isinstance(date_val, int):
            s = str(date_val)
            if len(s) == 8:
                return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
            return None
        elif isinstance(date_val, str):
            if len(date_val) == 10 and date_val[4] == '-':
                return date_val  # already YYYY-MM-DD
            elif len(date_val) == 8 and date_val.isdigit():
                return f"{date_val[:4]}-{date_val[4:6]}-{date_val[6:8]}"
            return date_val
        return str(date_val)


# 全局单例
_kline_manager: Optional[KlineManager] = None


def get_kline_manager() -> KlineManager:
    """获取全局 KlineManager 实例"""
    global _kline_manager
    if _kline_manager is None:
        _kline_manager = KlineManager()
    return _kline_manager
