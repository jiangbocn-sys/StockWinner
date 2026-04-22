"""
因子数据对齐和补全模块

功能：
1. 检测 kline_data 和 stock_daily_factors 表的数据差异
2. 对齐两个表的记录（股票代码 + 交易日期为键值）
3. 补全缺失的因子记录
4. 智能计算因子（考虑数据量要求）
5. 清理孤儿记录（kline_data 中不存在的记录）
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import List, Dict, Optional, Tuple
import sys

# 中国时区
CHINA_TZ = timezone(timedelta(hours=8))

def get_china_time():
    """获取中国时区时间"""
    return datetime.now(CHINA_TZ).replace(tzinfo=None)

DB_PATH = Path(__file__).parent.parent.parent / "data" / "kline.db"


class FactorAlignment:
    """因子数据对齐器"""

    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path

    def _get_connection(self) -> sqlite3.Connection:
        """获取数据库连接"""
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        return conn

    def analyze_data_gap(self) -> Dict:
        """
        分析 kline_data 和 stock_daily_factors 表的数据差距

        返回：
        {
            'kline_total': int,           # kline_data 总记录数
            'factor_total': int,          # stock_daily_factors 总记录数
            'kline_stocks': int,          # kline_data 股票数
            'factor_stocks': int,         # stock_daily_factors 股票数
            'missing_records': int,       # 缺失的因子记录数
            'orphan_records': int,        # 孤儿因子记录数（kline_data 中不存在）
            'missing_by_stock': list,     # 每只股票缺失的记录数（前 20）
            'latest_kline_date': str,     # kline_data 最新日期
            'latest_factor_date': str,    # stock_daily_factors 最新日期
        }
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        # 基础统计
        cursor.execute('SELECT COUNT(*) FROM kline_data')
        kline_total = cursor.fetchone()[0]

        cursor.execute('SELECT COUNT(*) FROM stock_daily_factors')
        factor_total = cursor.fetchone()[0]

        cursor.execute('SELECT COUNT(DISTINCT stock_code) FROM kline_data')
        kline_stocks = cursor.fetchone()[0]

        cursor.execute('SELECT COUNT(DISTINCT stock_code) FROM stock_daily_factors')
        factor_stocks = cursor.fetchone()[0]

        # 最新日期
        cursor.execute('SELECT MAX(trade_date) FROM kline_data')
        latest_kline_date = cursor.fetchone()[0]

        cursor.execute('SELECT MAX(trade_date) FROM stock_daily_factors')
        latest_factor_date = cursor.fetchone()[0]

        # 缺失记录数（kline_data 有但 stock_daily_factors 没有）
        cursor.execute("""
            SELECT COUNT(*) as cnt
            FROM kline_data k
            LEFT JOIN stock_daily_factors f
                ON k.stock_code = f.stock_code AND k.trade_date = f.trade_date
            WHERE f.trade_date IS NULL
        """)
        missing_records = cursor.fetchone()[0]

        # 孤儿记录数（stock_daily_factors 有但 kline_data 没有）
        cursor.execute("""
            SELECT COUNT(*) as cnt
            FROM stock_daily_factors f
            LEFT JOIN kline_data k
                ON f.stock_code = k.stock_code AND f.trade_date = k.trade_date
            WHERE k.trade_date IS NULL
        """)
        orphan_records = cursor.fetchone()[0]

        # 每只股票缺失的记录数（前 20）
        cursor.execute("""
            SELECT k.stock_code, COUNT(*) as missing_count
            FROM kline_data k
            LEFT JOIN stock_daily_factors f
                ON k.stock_code = f.stock_code AND k.trade_date = f.trade_date
            WHERE f.trade_date IS NULL
            GROUP BY k.stock_code
            ORDER BY missing_count DESC
            LIMIT 20
        """)
        missing_by_stock = [(row[0], row[1]) for row in cursor.fetchall()]

        conn.close()

        return {
            'kline_total': kline_total,
            'factor_total': factor_total,
            'kline_stocks': kline_stocks,
            'factor_stocks': factor_stocks,
            'missing_records': missing_records,
            'orphan_records': orphan_records,
            'missing_by_stock': missing_by_stock,
            'latest_kline_date': latest_kline_date,
            'latest_factor_date': latest_factor_date,
        }

    def get_missing_records(self, stock_code: Optional[str] = None, limit: int = 100) -> List[Dict]:
        """
        获取缺失的因子记录列表

        参数：
        - stock_code: 股票代码，可选，不传则获取所有
        - limit: 返回数量限制

        返回：
        缺失记录列表，每项包含 stock_code, trade_date
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        if stock_code:
            cursor.execute("""
                SELECT k.stock_code, k.trade_date as trade_date
                FROM kline_data k
                LEFT JOIN stock_daily_factors f
                    ON k.stock_code = f.stock_code AND k.trade_date = f.trade_date
                WHERE f.trade_date IS NULL AND k.stock_code = ?
                ORDER BY k.trade_date
                LIMIT ?
            """, (stock_code, limit))
        else:
            cursor.execute("""
                SELECT k.stock_code, k.trade_date as trade_date
                FROM kline_data k
                LEFT JOIN stock_daily_factors f
                    ON k.stock_code = f.stock_code AND k.trade_date = f.trade_date
                WHERE f.trade_date IS NULL
                ORDER BY k.stock_code, k.trade_date
                LIMIT ?
            """, (limit,))

        results = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return results

    def get_orphan_records(self, limit: int = 100) -> List[Dict]:
        """
        获取孤儿因子记录（kline_data 中不存在）

        参数：
        - limit: 返回数量限制

        返回：
        孤儿记录列表
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT f.stock_code, f.trade_date, f.stock_name
            FROM stock_daily_factors f
            LEFT JOIN kline_data k
                ON f.stock_code = k.stock_code AND f.trade_date = k.trade_date
            WHERE k.trade_date IS NULL
            ORDER BY f.trade_date DESC
            LIMIT ?
        """, (limit,))

        results = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return results

    def delete_orphan_records(self, dry_run: bool = True) -> int:
        """
        删除孤儿因子记录

        参数：
        - dry_run: 是否只是预演（不实际删除）

        返回：
        删除或即将删除的记录数
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT COUNT(*) as cnt
            FROM stock_daily_factors f
            LEFT JOIN kline_data k
                ON f.stock_code = k.stock_code AND f.trade_date = k.trade_date
            WHERE k.trade_date IS NULL
        """)
        count = cursor.fetchone()[0]

        if not dry_run:
            cursor.execute("""
                DELETE FROM stock_daily_factors
                WHERE trade_date NOT IN (SELECT trade_date FROM kline_data)
            """)
            deleted = cursor.rowcount
            conn.commit()
            print(f"[FactorAlignment] 已删除 {deleted} 条孤儿记录")
        else:
            print(f"[FactorAlignment] 预演：将删除 {count} 条孤儿记录")

        conn.close()
        return count

    def insert_missing_records(self, stock_codes: Optional[List[str]] = None) -> int:
        """
        插入缺失的因子记录（只插入空记录，不计算因子值）

        参数：
        - stock_codes: 股票代码列表，可选

        返回：
        插入的记录数
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        # 获取缺失的记录
        if stock_codes:
            placeholders = ','.join(['?' for _ in stock_codes])
            cursor.execute(f"""
                SELECT k.stock_code, k.trade_date, k.stock_name
                FROM kline_data k
                LEFT JOIN stock_daily_factors f
                    ON k.stock_code = f.stock_code AND k.trade_date = f.trade_date
                WHERE f.trade_date IS NULL AND k.stock_code IN ({placeholders})
                ORDER BY k.stock_code, k.trade_date
            """, stock_codes)
        else:
            cursor.execute("""
                SELECT k.stock_code, k.trade_date, k.stock_name
                FROM kline_data k
                LEFT JOIN stock_daily_factors f
                    ON k.stock_code = f.stock_code AND k.trade_date = f.trade_date
                WHERE f.trade_date IS NULL
                ORDER BY k.stock_code, k.trade_date
            """)

        missing = cursor.fetchall()
        inserted = 0

        current_time = get_china_time().isoformat()

        for row in missing:
            try:
                cursor.execute("""
                    INSERT OR IGNORE INTO stock_daily_factors (
                        stock_code, stock_name, trade_date, source, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                """, (row[0], row[2], row[1], 'auto_inserted', current_time, current_time))
                inserted += 1
            except Exception as e:
                print(f"[FactorAlignment] 插入 {row[0]} {row[1]} 失败：{e}")

        conn.commit()
        conn.close()

        print(f"[FactorAlignment] 插入了 {inserted} 条缺失记录")
        return inserted


class FactorCalculator:
    """因子计算器 - 支持智能计算和数据量检查"""

    # 各因子所需的最小数据量
    MIN_DATA_REQUIREMENTS = {
        'ma5': 5,
        'ma10': 10,
        'ma20': 20,
        'ma60': 60,
        'ema12': 12,
        'ema26': 26,
        'kdj_k': 9,
        'kdj_d': 9,
        'kdj_j': 9,
        'dif': 26,
        'dea': 26,
        'macd': 26,
        'adx': 14,
        'rsi_14': 14,
        'cci_20': 20,
        'atr_14': 14,
        'boll_upper': 20,
        'boll_middle': 20,
        'boll_lower': 20,
        'change_10d': 10,
        'change_20d': 20,
        'bias_5': 5,
        'bias_10': 10,
        'bias_20': 20,
        'momentum_10d': 10,
        'momentum_20d': 20,
        'change_std_5': 5,
        'change_std_10': 10,
        'change_std_20': 20,
        'amount_std_5': 5,
        'amount_std_10': 10,
        'amount_std_20': 20,
        'hv_20': 20,
    }

    # 不需要计算的字段（键值、系统字段、外部数据字段）
    SKIP_CALCULATION_FIELDS = {
        'id', 'stock_code', 'stock_name', 'trade_date',  # 键值
        'source', 'created_at', 'updated_at',  # 系统字段
        'circ_market_cap', 'total_market_cap', 'days_since_ipo',  # 外部数据字段
        'pe_inverse', 'pb_inverse',  # 财务数据
        'pe_ttm', 'pb', 'ps_ttm', 'pcf', 'ev_ebitda',  # 估值数据
        'roe', 'roa', 'gross_margin', 'net_margin',  # 财务指标
        'revenue_growth_yoy', 'revenue_growth_qoq',  # 增长数据
        'net_profit_growth_yoy', 'net_profit_growth_qoq',  # 利润增长
        'golden_cross', 'death_cross',  # 信号字段
        'limit_up_count_10d', 'limit_up_count_20d', 'limit_up_count_30d',
        'consecutive_limit_up', 'first_limit_up_days', 'highest_board_10d',
        'large_gain_5d_count', 'large_loss_5d_count',
        'gap_up_ratio', 'close_to_high_250d', 'close_to_low_250d',
        'obv', 'volume_ratio',  # 成交量指标（暂未计算）
    }

    # 需要计算的因子字段
    CALCULATED_FIELDS = set(MIN_DATA_REQUIREMENTS.keys()) | {
        'amplitude_5', 'amplitude_10', 'amplitude_20',
        'next_period_change', 'is_traded',
    }

    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path
        self.table_columns = None  # 缓存表结构

    def _get_table_columns(self) -> List[str]:
        """获取 stock_daily_factors 表的所有列名"""
        if self.table_columns is not None:
            return self.table_columns

        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('PRAGMA table_info(stock_daily_factors)')
        self.table_columns = [row[1] for row in cursor.fetchall()]
        conn.close()
        return self.table_columns

    def _get_connection(self) -> sqlite3.Connection:
        """获取数据库连接"""
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        return conn

    def get_stock_kline_data(self, stock_code: str, end_date: str, max_days: int = 6000) -> pd.DataFrame:
        """
        获取股票的 K 线数据（用于计算因子）

        参数：
        - stock_code: 股票代码
        - end_date: 截止日期
        - max_days: 最多获取的天数（默认 6000 天，覆盖 20+ 年数据）

        返回：
        pandas DataFrame
        """
        conn = self._get_connection()

        query = """
            SELECT trade_date as trade_date, open, high, low, close, volume, amount
            FROM kline_data
            WHERE stock_code = ? AND trade_date <= ?
            ORDER BY trade_date DESC
            LIMIT ?
        """

        df = pd.read_sql_query(query, conn, params=(stock_code, end_date, max_days))
        conn.close()

        if not df.empty:
            df = df.iloc[::-1].reset_index(drop=True)  # 按时间正序排列
            df['trade_date'] = pd.to_datetime(df['trade_date'])

        return df

    def calculate_all_factors(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算所有因子（自动处理数据量不足的情况）

        参数：
        - df: K 线数据 DataFrame

        返回：
        包含因子数据的 DataFrame
        """
        if df.empty:
            return df

        df = df.copy()

        # 基础价格数据
        df['close'] = pd.to_numeric(df['close'], errors='coerce')
        df['open'] = pd.to_numeric(df['open'], errors='coerce')
        df['high'] = pd.to_numeric(df['high'], errors='coerce')
        df['low'] = pd.to_numeric(df['low'], errors='coerce')
        df['volume'] = pd.to_numeric(df['volume'], errors='coerce')
        df['amount'] = pd.to_numeric(df['amount'], errors='coerce')

        # 计算涨跌幅
        df['pct_change'] = df['close'].pct_change()

        # 计算 MA
        for period in [5, 10, 20, 60]:
            if len(df) >= period:
                df[f'ma{period}'] = df['close'].rolling(window=period).mean()
            else:
                df[f'ma{period}'] = np.nan

        # 计算 EMA
        if len(df) >= 26:
            df['ema12'] = df['close'].ewm(span=12, adjust=False).mean()
            df['ema26'] = df['close'].ewm(span=26, adjust=False).mean()
        else:
            df['ema12'] = np.nan
            df['ema26'] = np.nan

        # 计算 MACD
        if len(df) >= 26:
            df['dif'] = df['ema12'] - df['ema26']
            df['dea'] = df['dif'].ewm(span=9, adjust=False).mean()
            df['macd'] = 2 * (df['dif'] - df['dea'])
        else:
            df['dif'] = np.nan
            df['dea'] = np.nan
            df['macd'] = np.nan

        # 计算 KDJ
        if len(df) >= 9:
            low_9 = df['low'].rolling(window=9).min()
            high_9 = df['high'].rolling(window=9).max()
            rsv = (df['close'] - low_9) / (high_9 - low_9) * 100
            df['kdj_k'] = rsv.ewm(com=2, adjust=False).mean()
            df['kdj_d'] = df['kdj_k'].ewm(com=1, adjust=False).mean()
            df['kdj_j'] = 3 * df['kdj_k'] - 2 * df['kdj_d']
        else:
            df['kdj_k'] = np.nan
            df['kdj_d'] = np.nan
            df['kdj_j'] = np.nan

        # 计算振幅
        df['amplitude'] = (df['high'] - df['low']) / df['close'].shift(1) * 100

        # 计算振幅标准差
        for period in [5, 10, 20]:
            if len(df) >= period:
                df[f'amplitude_std_{period}'] = df['amplitude'].rolling(window=period).std()
            else:
                df[f'amplitude_std_{period}'] = np.nan

        # 计算乖离率
        for period in [5, 10, 20]:
            if len(df) >= period:
                ma = df['close'].rolling(window=period).mean()
                df[f'bias_{period}'] = (df['close'] - ma) / ma * 100
            else:
                df[f'bias_{period}'] = np.nan

        # 计算涨跌幅标准差
        for period in [5, 10, 20]:
            if len(df) >= period:
                df[f'change_std_{period}'] = df['pct_change'].rolling(window=period).std()
            else:
                df[f'change_std_{period}'] = np.nan

        # 计算成交额标准差
        for period in [5, 10, 20]:
            if len(df) >= period:
                df[f'amount_std_{period}'] = df['amount'].rolling(window=period).std()
            else:
                df[f'amount_std_{period}'] = np.nan

        # 计算 10 日/20 日涨跌幅
        if len(df) >= 10:
            df['change_10d'] = df['close'].pct_change(periods=10) * 100
        else:
            df['change_10d'] = np.nan

        if len(df) >= 20:
            df['change_20d'] = df['close'].pct_change(periods=20) * 100
        else:
            df['change_20d'] = np.nan

        # 计算 BOLL 布林带
        if len(df) >= 20:
            df['boll_middle'] = df['close'].rolling(window=20).mean()
            rolling_std = df['close'].rolling(window=20).std()
            df['boll_upper'] = df['boll_middle'] + 2 * rolling_std
            df['boll_lower'] = df['boll_middle'] - 2 * rolling_std
        else:
            df['boll_upper'] = np.nan
            df['boll_middle'] = np.nan
            df['boll_lower'] = np.nan

        # 计算 ATR (平均真实波幅)
        if len(df) >= 14:
            high = df['high']
            low = df['low']
            close_prev = df['close'].shift(1)
            tr1 = high - low
            tr2 = abs(high - close_prev)
            tr3 = abs(low - close_prev)
            tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
            df['atr_14'] = tr.rolling(window=14).mean()
        else:
            df['atr_14'] = np.nan

        # 计算 RSI (相对强弱指标)
        if len(df) >= 14:
            delta = df['close'].diff()
            gain = delta.where(delta > 0, 0)
            loss = -delta.where(delta < 0, 0)
            avg_gain = gain.rolling(window=14).mean()
            avg_loss = loss.rolling(window=14).mean()
            rs = avg_gain / avg_loss
            df['rsi_14'] = 100 - (100 / (1 + rs))
        else:
            df['rsi_14'] = np.nan

        # 计算 CCI (商品通道指标)
        if len(df) >= 20:
            tp = (df['high'] + df['low'] + df['close']) / 3
            sma_tp = tp.rolling(window=20).mean()
            mad = tp.rolling(window=20).apply(lambda x: np.abs(x - x.mean()).mean(), raw=True)
            df['cci_20'] = (tp - sma_tp) / (0.015 * mad)
        else:
            df['cci_20'] = np.nan

        # 计算 ADX (平均趋向指标)
        if len(df) >= 14:
            high = df['high']
            low = df['low']
            close = df['close']

            # +DM 和 -DM
            plus_dm = high.diff()
            minus_dm = -low.diff()

            plus_dm = plus_dm.where((plus_dm > minus_dm) & (plus_dm > 0), 0)
            minus_dm = minus_dm.where((minus_dm > plus_dm) & (minus_dm > 0), 0)

            # TR
            tr1 = high - low
            tr2 = abs(high - close.shift(1))
            tr3 = abs(low - close.shift(1))
            tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

            # 平滑
            tr14 = tr.rolling(window=14).sum()
            plus_di14 = plus_dm.rolling(window=14).sum() / tr14 * 100
            minus_di14 = minus_dm.rolling(window=14).sum() / tr14 * 100

            # DX
            dx = abs(plus_di14 - minus_di14) / (plus_di14 + minus_di14) * 100
            df['adx'] = dx.rolling(window=14).mean()
        else:
            df['adx'] = np.nan

        # 计算动量
        if len(df) >= 10:
            df['momentum_10d'] = df['close'] - df['close'].shift(10)
        else:
            df['momentum_10d'] = np.nan

        if len(df) >= 20:
            df['momentum_20d'] = df['close'] - df['close'].shift(20)
        else:
            df['momentum_20d'] = np.nan

        # 计算 HV (历史波动率)
        if len(df) >= 20:
            df['hv_20'] = df['pct_change'].rolling(window=20).std() * np.sqrt(252) * 100
        else:
            df['hv_20'] = np.nan

        # 计算下期收益率
        df['next_period_change'] = df['pct_change'].shift(-1)

        # 是否交易
        df['is_traded'] = (df['volume'] > 0).astype(int)

        return df

    def calculate_factors_for_stock(self, stock_code: str, recalculate: bool = False, fill_empty_only: bool = False) -> Dict:
        """
        为单只股票计算并更新因子

        策略：
        - 读取记录，检查哪些字段为空
        - 只计算填充空白字段
        - 已有数值的直接保留或跳过

        参数：
        - stock_code: 股票代码
        - recalculate: 是否重新计算已有数据的记录
        - fill_empty_only: 是否仅填充空字段（不跳过已有数据的记录）

        返回：
        计算结果统计
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        # 获取表的所有列名
        all_columns = self._get_table_columns()

        # 获取该股票的 K 线数据日期范围
        cursor.execute("""
            SELECT MIN(trade_date), MAX(trade_date) FROM kline_data WHERE stock_code = ?
        """, (stock_code,))
        date_range = cursor.fetchone()

        if not date_range or not date_range[0]:
            conn.close()
            return {'status': 'no_data', 'message': f'{stock_code} 无 K 线数据'}

        end_date = date_range[1]

        # 获取 K 线数据（默认 6000 天，覆盖 20+ 年数据）
        df = self.get_stock_kline_data(stock_code, end_date)

        if df.empty:
            conn.close()
            return {'status': 'no_data', 'message': f'{stock_code} K 线数据为空'}

        # 获取股票名称
        cursor.execute("SELECT DISTINCT stock_name FROM kline_data WHERE stock_code = ? LIMIT 1", (stock_code,))
        row = cursor.fetchone()
        stock_name = row[0] if row else stock_code

        # 计算因子
        df = self.calculate_all_factors(df)

        updated = 0
        skipped = 0
        current_time = get_china_time().isoformat()

        for idx, row in df.iterrows():
            trade_date = row['trade_date'].strftime('%Y-%m-%d')

            # 读取现有记录
            cursor.execute("""
                SELECT * FROM stock_daily_factors
                WHERE stock_code = ? AND trade_date = ?
            """, (stock_code, trade_date))

            existing = cursor.fetchone()
            existing_dict = dict(existing) if existing else None

            # 判断是否需要更新
            if existing_dict and not recalculate and not fill_empty_only:
                # 检查计算字段是否已有数据
                has_factor_data = False
                for field in self.CALCULATED_FIELDS:
                    if field in existing_dict and existing_dict[field] is not None:
                        has_factor_data = True
                        break

                if has_factor_data:
                    skipped += 1
                    continue

            # 构建要更新的字段：只填充空白的计算字段
            values_dict = {
                'stock_code': stock_code,
                'stock_name': stock_name,
                'trade_date': trade_date,
                'source': 'auto_calculated',
                'updated_at': current_time,
            }

            # 如果有现有记录，保留已有数值的字段
            if existing_dict:
                for field in self.CALCULATED_FIELDS:
                    if field in all_columns:
                        if existing_dict.get(field) is not None:
                            # 已有数值，直接使用
                            values_dict[field] = existing_dict[field]
                        else:
                            # 空白字段，计算填充
                            values_dict[field] = row.get(field)
            else:
                # 新记录，填充所有计算字段
                for field in self.CALCULATED_FIELDS:
                    if field in all_columns:
                        values_dict[field] = row.get(field)

            # 构建 INSERT 语句
            columns = list(values_dict.keys())
            placeholders = ','.join(['?' for _ in columns])
            columns_str = ','.join(columns)
            values = [values_dict[col] for col in columns]

            try:
                cursor.execute(f"""
                    INSERT OR REPLACE INTO stock_daily_factors ({columns_str})
                    VALUES ({placeholders})
                """, values)
                updated += 1

                if updated % 500 == 0:
                    conn.commit()
                    print(f"  {stock_code}: 已更新 {updated} 条记录")

            except Exception as e:
                print(f"[FactorCalculator] 更新 {stock_code} {trade_date} 失败：{e}")

        conn.commit()
        conn.close()

        return {
            'status': 'success',
            'stock_code': stock_code,
            'updated': updated,
            'skipped': skipped,
            'total': len(df)
        }


def run_factor_alignment(
    delete_orphans: bool = False,
    insert_missing: bool = True,
    calculate_factors: bool = True,
    stock_codes: Optional[List[str]] = None,
    recalculate: bool = False,
    fill_empty_only: bool = False,
    show_progress: bool = True
) -> Dict:
    """
    运行因子数据对齐和补全

    参数：
    - delete_orphans: 是否删除孤儿记录
    - insert_missing: 是否插入缺失记录
    - calculate_factors: 是否计算因子
    - stock_codes: 股票代码列表，可选
    - recalculate: 是否重新计算已有数据的记录
    - show_progress: 是否显示进度

    返回：
    执行结果统计
    """
    print("=" * 60)
    print("因子数据对齐和补全")
    print("=" * 60)

    alignment = FactorAlignment()
    calculator = FactorCalculator()

    # 1. 分析数据差距
    print("\n[步骤 1/4] 分析数据差距...")
    gap = alignment.analyze_data_gap()

    print(f"  kline_data 记录数：{gap['kline_total']:,}")
    print(f"  stock_daily_factors 记录数：{gap['factor_total']:,}")
    print(f"  kline_data 股票数：{gap['kline_stocks']}")
    print(f"  stock_daily_factors 股票数：{gap['factor_stocks']}")
    print(f"  缺失记录数：{gap['missing_records']:,}")
    print(f"  孤儿记录数：{gap['orphan_records']:,}")
    print(f"  kline_data 最新日期：{gap['latest_kline_date']}")
    print(f"  stock_daily_factors 最新日期：{gap['latest_factor_date']}")

    if gap['missing_by_stock']:
        print(f"\n  缺失最多的前 5 只股票:")
        for code, count in gap['missing_by_stock'][:5]:
            print(f"    {code}: {count:,} 条")

    # 2. 删除孤儿记录
    if delete_orphans:
        print(f"\n[步骤 2/4] 删除孤儿记录...")
        deleted = alignment.delete_orphan_records(dry_run=False)
        print(f"  已删除 {deleted:,} 条孤儿记录")
    else:
        print(f"\n[步骤 2/4] 跳过删除孤儿记录 (--delete-orphans 启用删除)")

    # 3. 插入缺失记录
    if insert_missing:
        print(f"\n[步骤 3/4] 插入缺失记录...")
        inserted = alignment.insert_missing_records(stock_codes)
        print(f"  已插入 {inserted:,} 条缺失记录")
    else:
        print(f"\n[步骤 3/4] 跳过插入缺失记录")

    # 4. 计算因子
    if calculate_factors:
        print(f"\n[步骤 4/4] 计算因子...")

        if stock_codes:
            stocks_to_process = stock_codes
            print(f"  计算指定 {len(stocks_to_process)} 只股票的因子")
        elif fill_empty_only:
            # 获取所有股票（遍历所有记录检查空字段）
            conn = alignment._get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT DISTINCT stock_code FROM kline_data ORDER BY stock_code")
            stocks_to_process = [row[0] for row in cursor.fetchall()]
            conn.close()
            print(f"  计算所有 {len(stocks_to_process)} 只股票的因子（仅填充空字段）")
        else:
            # 获取所有有缺失或空记录的股票
            conn = alignment._get_connection()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT DISTINCT k.stock_code
                FROM kline_data k
                LEFT JOIN stock_daily_factors f
                    ON k.stock_code = f.stock_code AND k.trade_date = f.trade_date
                WHERE f.trade_date IS NULL OR (
                    f.dif IS NULL AND f.macd IS NULL AND f.ma5 IS NULL
                )
                ORDER BY k.stock_code
            """)
            stocks_to_process = [row[0] for row in cursor.fetchall()]
            conn.close()
            print(f"  计算 {len(stocks_to_process)} 只股票的因子")

        total_updated = 0
        total_skipped = 0

        for i, stock_code in enumerate(stocks_to_process):
            if show_progress and (i + 1) % 100 == 0:
                print(f"  进度：{i + 1}/{len(stocks_to_process)} ({(i + 1) / len(stocks_to_process) * 100:.1f}%)")

            result = calculator.calculate_factors_for_stock(stock_code, recalculate, fill_empty_only)

            if result['status'] == 'success':
                total_updated += result['updated']
                total_skipped += result['skipped']

        print(f"\n  因子计算完成:")
        print(f"    更新记录数：{total_updated:,}")
        print(f"    跳过记录数：{total_skipped:,}")
    else:
        print(f"\n[步骤 4/4] 跳过计算因子")

    print("\n" + "=" * 60)
    print("因子数据对齐完成")
    print("=" * 60)

    return {
        'gap_analysis': gap,
        'deleted_orphans': gap['orphan_records'] if delete_orphans else 0,
        'inserted_missing': gap['missing_records'] if insert_missing else 0,
        'updated_factors': total_updated if calculate_factors else 0,
        'skipped_factors': total_skipped if calculate_factors else 0
    }


# CLI 入口
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='因子数据对齐和补全工具')
    parser.add_argument('--delete-orphans', action='store_true', help='删除孤儿记录')
    parser.add_argument('--no-insert', action='store_true', help='不插入缺失记录')
    parser.add_argument('--no-calculate', action='store_true', help='不计算因子')
    parser.add_argument('--recalculate', action='store_true', help='重新计算已有数据的记录')
    parser.add_argument('--fill-empty', action='store_true', help='仅填充空字段（遍历所有记录）')
    parser.add_argument('--stocks', nargs='+', help='指定股票代码列表')
    parser.add_argument('--no-progress', action='store_true', help='不显示进度')

    args = parser.parse_args()

    run_factor_alignment(
        delete_orphans=args.delete_orphans,
        insert_missing=not args.no_insert,
        calculate_factors=not args.no_calculate,
        stock_codes=args.stocks,
        recalculate=args.recalculate,
        fill_empty_only=args.fill_empty,
        show_progress=not args.no_progress
    )
