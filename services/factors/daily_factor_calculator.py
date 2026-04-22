"""
日频因子计算器

基于 kline_data 表计算所有日频因子，支持：
1. 市值类因子（集成 SDK 获取股本数据）
2. 市场表现类因子（涨跌幅、乖离率、振幅、波动率）
3. 技术指标类因子（KDJ、MACD）- 使用统一技术模块
4. 估值类因子（PE、PB 倒数，集成 SDK 获取财务数据）
5. 下期收益率（next_period_change）
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Dict, Optional, Tuple
import aiosqlite
import asyncio

# 中国时区
CHINA_TZ = timezone(timedelta(hours=8))

# 导入统一技术指标模块
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from common.technical_indicators import add_price_performance_to_df, add_kdj_to_df, add_macd_to_df

DB_PATH = Path(__file__).parent.parent.parent / "data" / "kline.db"


class DailyFactorCalculator:
    """日频因子计算器"""

    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path

    def _get_connection(self) -> sqlite3.Connection:
        """获取数据库连接"""
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        return conn

    def get_trading_dates(self, start_date: str, end_date: str) -> List[str]:
        """获取指定范围内的所有交易日"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT trade_date
            FROM kline_data
            WHERE trade_date >= ? AND trade_date <= ?
            ORDER BY trade_date
        """, (start_date, end_date))
        dates = [row[0] for row in cursor.fetchall()]
        conn.close()
        return dates

    def get_all_stocks_on_date(self, trade_date: str) -> List[str]:
        """获取指定交易日有数据的所有股票代码"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT stock_code
            FROM kline_data
            WHERE trade_date = ?
        """, (trade_date,))
        stocks = [row[0] for row in cursor.fetchall()]
        conn.close()
        return stocks

    def get_stock_kline_data(
        self,
        stock_code: str,
        start_date: str,
        end_date: str,
        with_name: bool = False
    ) -> pd.DataFrame:
        """获取单只股票的 K 线数据"""
        conn = self._get_connection()

        if with_name:
            query = """
                SELECT trade_date, open, high, low, close, volume, amount, stock_name
                FROM kline_data
                WHERE stock_code = ? AND trade_date >= ? AND trade_date <= ?
                ORDER BY trade_date
            """
        else:
            query = """
                SELECT trade_date, open, high, low, close, volume, amount
                FROM kline_data
                WHERE stock_code = ? AND trade_date >= ? AND trade_date <= ?
                ORDER BY trade_date
            """

        df = pd.read_sql_query(query, conn, params=(stock_code, start_date, end_date))
        conn.close()

        if not df.empty:
            df['stock_code'] = stock_code
            # 计算涨跌幅
            df['change_pct'] = df['close'].pct_change()

        return df

    # ==================== 市值类因子 ====================

    def calculate_market_cap(
        self,
        stock_code: str,
        trade_date: str,
        total_shares: Optional[float] = None,
        circ_shares: Optional[float] = None
    ) -> Optional[Dict]:
        """
        计算市值类因子

        参数：
        - stock_code: 股票代码
        - trade_date: 交易日期
        - total_shares: 总股本（万股），如不传则从 SDK 获取
        - circ_shares: 流通股本（万股），如不传则从 SDK 获取

        返回：
        - circ_market_cap: 流通市值（亿元）= 流通股本 * 收盘价 / 10000
        - total_market_cap: 总市值（亿元）= 总股本 * 收盘价 / 10000
        - days_since_ipo: 上市天数
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        # 获取当日收盘价和股票名称
        cursor.execute("""
            SELECT close, stock_name FROM kline_data
            WHERE stock_code = ? AND trade_date = ?
        """, (stock_code, trade_date))
        row = cursor.fetchone()
        conn.close()

        if not row:
            return None

        close = row[0]
        stock_name = row[1]

        # 如果没有传入股本数据，尝试从 SDK 获取
        if total_shares is None or circ_shares is None:
            try:
                from services.common.sdk_manager import get_sdk_manager
                sdk_manager = get_sdk_manager()
                equity_df = sdk_manager.get_equity_structure([stock_code])
                if not equity_df.empty:
                    # 获取最接近 trade_date 的数据
                    equity_df['ANN_DATE'] = pd.to_datetime(equity_df['ANN_DATE'], format='%Y%m%d')
                    trade_dt = pd.to_datetime(trade_date)
                    valid_equity = equity_df[equity_df['ANN_DATE'] <= trade_dt]
                    if not valid_equity.empty:
                        latest = valid_equity.sort_values('ANN_DATE', ascending=False).iloc[0]
                        if total_shares is None:
                            total_shares = latest.get('TOT_SHARE')
                        if circ_shares is None:
                            circ_shares = latest.get('FLOAT_SHARE')
            except Exception as e:
                print(f"获取股本数据失败 {stock_code}: {e}")

        # 计算市值（单位：亿元）
        # SDK 返回的股本单位是万股，收盘价单位是元
        # 市值 = 股本 * 收盘价 / 10000 （亿股 * 元 = 亿元）
        circ_market_cap = None
        total_market_cap = None

        if circ_shares is not None and close is not None:
            circ_market_cap = circ_shares * close / 10000  # 流通市值（亿元）

        if total_shares is not None and close is not None:
            total_market_cap = total_shares * close / 10000  # 总市值（亿元）

        # 计算上市天数（需要 IPO 日期，暂时返回 None）
        days_since_ipo = None

        return {
            'stock_code': stock_code,
            'stock_name': stock_name,
            'trade_date': trade_date,
            'close_price': close,
            'circ_market_cap': circ_market_cap,
            'total_market_cap': total_market_cap,
            'days_since_ipo': days_since_ipo,
        }

    # ==================== 市场表现类因子 ====================

    def calculate_price_performance(
        self,
        df: pd.DataFrame,
        windows: List[int] = [5, 10, 20]
    ) -> pd.DataFrame:
        """
        计算市场表现类因子

        输入：
        - df: K 线数据，包含 trade_date, close, high, low, amount 列

        输出：
        - change_Nd: N 日涨跌幅
        - bias_N: N 日乖离率（收盘价与 N 日均线的偏离度）
        - amplitude_N: N 日振幅（最高价与最低价之差除以前收盘）
        - change_std_N: N 日涨跌幅标准差
        - amount_std_N: N 日成交额标准差
        """
        return add_price_performance_to_df(df, windows)

    # ==================== 技术指标类因子 ====================

    def calculate_kdj(
        self,
        df: pd.DataFrame,
        n: int = 9,
        m1: int = 3,
        m2: int = 3
    ) -> pd.DataFrame:
        """
        计算 KDJ 指标

        参数：
        - n: RSV 计算周期（默认 9 日）
        - m1: K 值平滑周期（默认 3）
        - m2: D 值平滑周期（默认 3）

        公式：
        - RSV = (收盘价 - N 日最低价) / (N 日最高价 - N 日最低价) * 100
        - K = RSV 的 M1 日指数移动平均
        - D = K 的 M2 日指数移动平均
        - J = 3 * K - 2 * D
        """
        return add_kdj_to_df(df, n, m1, m2)

    def calculate_macd(
        self,
        df: pd.DataFrame,
        fast: int = 12,
        slow: int = 26,
        signal: int = 9
    ) -> pd.DataFrame:
        """
        计算 MACD 指标

        参数：
        - fast: 快线周期（默认 12）
        - slow: 慢线周期（默认 26）
        - signal: 信号线周期（默认 9）

        公式：
        - DIF = EMA(fast) - EMA(slow)
        - DEA = DIF 的 signal 日指数移动平均
        - MACD = 2 * (DIF - DEA)
        """
        return add_macd_to_df(df, fast, slow, signal)

    # ==================== 估值类因子 ====================

    def calculate_valuation(
        self,
        stock_code: str,
        trade_date: str,
        net_profit_ttm: Optional[float] = None,
        net_assets: Optional[float] = None,
        total_shares: Optional[float] = None
    ) -> Optional[Dict]:
        """
        计算估值类因子

        参数：
        - stock_code: 股票代码
        - trade_date: 交易日期
        - net_profit_ttm: 净利润 TTM（亿元），如不传则从 SDK 获取
        - net_assets: 净资产（亿元），如不传则从 SDK 获取
        - total_shares: 总股本（万股），如不传则从 SDK 获取

        返回：
        - pe_inverse: PE 倒数 = 净利润 TTM / 市值 = 1/PE
        - pb_inverse: PB 倒数 = 净资产 / 市值 = 1/PB
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        # 获取当日收盘价
        cursor.execute("""
            SELECT close FROM kline_data
            WHERE stock_code = ? AND trade_date = ?
        """, (stock_code, trade_date))
        row = cursor.fetchone()
        conn.close()

        if not row:
            return None

        close = row[0]

        # 如果没有传入财务数据，尝试从 SDK 获取
        if net_profit_ttm is None or net_assets is None or total_shares is None:
            try:
                from services.common.sdk_manager import get_sdk_manager
                sdk_manager = get_sdk_manager()

                # 获取股本数据
                if total_shares is None:
                    equity_df = sdk_manager.get_equity_structure([stock_code])
                    if not equity_df.empty:
                        # 获取最接近 trade_date 的数据
                        equity_df['ANN_DATE'] = pd.to_datetime(equity_df['ANN_DATE'], format='%Y%m%d', errors='coerce')
                        trade_dt = pd.to_datetime(trade_date)
                        valid_equity = equity_df[equity_df['ANN_DATE'] <= trade_dt]
                        if not valid_equity.empty:
                            latest = valid_equity.sort_values('ANN_DATE', ascending=False).iloc[0]
                            total_shares = latest.get('TOT_SHARE')

                # 获取利润表数据
                if net_profit_ttm is None:
                    income_df = sdk_manager.get_income_statement([stock_code])
                    if not income_df.empty:
                        # 获取最新的年报数据（REPORT_TYPE=1）
                        annual = income_df[income_df['REPORT_TYPE'] == '1']
                        if not annual.empty:
                            latest = annual.sort_values('REPORTING_PERIOD', ascending=False).iloc[0]
                            # NET_PRO_INCL_MIN_INT_INC 是净利润（包含少数股东权益）
                            net_profit_ttm = latest.get('NET_PRO_INCL_MIN_INT_INC')

                # 获取资产负债表数据
                if net_assets is None:
                    balance_df = sdk_manager.get_balance_sheet([stock_code])
                    if not balance_df.empty:
                        # 获取最新的年报数据
                        annual = balance_df[balance_df['REPORT_TYPE'] == '1']
                        if not annual.empty:
                            latest = annual.sort_values('REPORTING_PERIOD', ascending=False).iloc[0]
                            # TOT_SHARE_EQUITY_EXCL_MIN_INT 是归属母公司股东权益
                            net_assets = latest.get('TOT_SHARE_EQUITY_EXCL_MIN_INT')

            except Exception as e:
                print(f"获取财务数据失败 {stock_code}: {e}")

        # 计算估值指标
        # 注意：SDK 返回的财务数据单位是元，需要转换为亿元
        # SDK 返回的股本单位是万股
        # 市值 = total_shares * close / 10000 （亿元）
        # 财务数据（元）转换为亿元：除以 100,000,000

        pe_inverse = None
        pb_inverse = None

        if total_shares is not None:
            market_cap = total_shares * close / 10000  # 市值（亿元）

            if net_profit_ttm is not None and market_cap > 0:
                # SDK 返回的净利润单位是元，转换为亿元
                net_profit_ttm_yi = net_profit_ttm / 100000000
                pe_inverse = net_profit_ttm_yi / market_cap  # PE 倒数

            if net_assets is not None and market_cap > 0:
                # SDK 返回的净资产单位是元，转换为亿元
                net_assets_yi = net_assets / 100000000
                pb_inverse = net_assets_yi / market_cap  # PB 倒数

        return {
            'stock_code': stock_code,
            'trade_date': trade_date,
            'pe_inverse': pe_inverse,
            'pb_inverse': pb_inverse,
        }

    # ==================== 下期收益率 ====================

    def calculate_next_period_change(
        self,
        stock_code: str,
        trade_date: str
    ) -> Optional[float]:
        """
        计算下一交易日的收益率

        公式：
        - next_period_change = (次日收盘价 - 当日收盘价) / 当日收盘价
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        # 获取当日和次日的收盘价
        cursor.execute("""
            SELECT trade_date, close FROM kline_data
            WHERE stock_code = ? AND trade_date >= ?
            ORDER BY trade_date
            LIMIT 2
        """, (stock_code, trade_date))
        rows = cursor.fetchall()
        conn.close()

        if len(rows) < 2:
            return None

        current_close = rows[0][1]
        next_close = rows[1][1]

        return (next_close - current_close) / current_close

    # ==================== 特色因子 (A 股) ====================

    def calculate_limit_up_stats(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算涨停相关统计

        A 股涨停规则：
        - 主板：涨跌幅 >= 10%
        - 创业板/科创板：涨跌幅 >= 20%
        - ST 股：涨跌幅 >= 5%

        为简化处理，统一使用 9.5% 作为涨停阈值（考虑数据精度）
        """
        df = df.copy()

        # 判断涨停（简化版：统一使用 9.5% 阈值）
        df['is_limit_up'] = (df['change_pct'] >= 0.095).astype(int)

        # N 日涨停次数
        df['limit_up_count_10d'] = df['is_limit_up'].rolling(10, min_periods=1).sum()
        df['limit_up_count_20d'] = df['is_limit_up'].rolling(20, min_periods=1).sum()
        df['limit_up_count_30d'] = df['is_limit_up'].rolling(30, min_periods=1).sum()

        # 连续涨停天数
        def count_consecutive(x):
            count = 0
            result = []
            for val in x:
                if val == 1:
                    count += 1
                else:
                    count = 0
                result.append(count)
            return result

        df['consecutive_limit_up'] = count_consecutive(df['is_limit_up'].tolist())

        # 首次涨停距今天数（最近一次涨停距今的天数）
        def days_since_last_limit_up(x):
            result = []
            last_pos = None
            for i, val in enumerate(x):
                if val == 1:
                    last_pos = i
                    result.append(0)
                elif last_pos is not None:
                    result.append(i - last_pos)
                else:
                    result.append(np.nan)
            return result

        df['first_limit_up_days'] = days_since_last_limit_up(df['is_limit_up'].tolist())

        # 10 日内最高连板数
        def highest_board_in_window(x, window=10):
            result = []
            for i in range(len(x)):
                start = max(0, i - window + 1)
                window_data = x[start:i+1]
                max_board = 0
                current_board = 0
                for val in window_data:
                    if val == 1:
                        current_board += 1
                        max_board = max(max_board, current_board)
                    else:
                        current_board = 0
                result.append(max_board)
            return result

        df['highest_board_10d'] = highest_board_in_window(df['is_limit_up'].tolist(), 10)

        # 清理临时列
        if 'is_limit_up' in df.columns:
            df.drop(columns=['is_limit_up'], inplace=True)

        return df

    def calculate_large_move_stats(self, df: pd.DataFrame, threshold: float = 0.05) -> pd.DataFrame:
        """
        计算异动统计（大涨/大跌次数）

        Args:
            df: K 线数据 DataFrame
            threshold: 涨跌幅阈值（默认 5%）
        """
        df = df.copy()

        # 大涨 (>5%)
        df['is_large_gain'] = (df['change_pct'] > threshold).astype(int)
        df['large_gain_5d_count'] = df['is_large_gain'].rolling(5, min_periods=1).sum()

        # 大跌 (<-5%)
        df['is_large_loss'] = (df['change_pct'] < -threshold).astype(int)
        df['large_loss_5d_count'] = df['is_large_loss'].rolling(5, min_periods=1).sum()

        # 跳空高开幅度（当日开盘价 vs 前日收盘价）
        df['gap_up_ratio'] = (df['open'] - df['close'].shift(1)) / df['close'].shift(1)

        # 清理临时列
        df.drop(columns=['is_large_gain', 'is_large_loss'], inplace=True, errors='ignore')

        return df

    def calculate_price_position(self, df: pd.DataFrame, window: int = 250) -> pd.DataFrame:
        """
        计算筹码位置（距新高/新低距离）

        Args:
            df: K 线数据 DataFrame
            window: 周期窗口（默认 250 日）
        """
        df = df.copy()

        # N 日最高价和最低价
        df['highest_' + str(window)] = df['high'].rolling(window=window, min_periods=1).max()
        df['lowest_' + str(window)] = df['low'].rolling(window=window, min_periods=1).min()

        # 距新高距离 (%)
        df['close_to_high_250d'] = (df['close'] - df['highest_' + str(window)]) / df['highest_' + str(window)] * 100

        # 距新低距离 (%)
        df['close_to_low_250d'] = (df['close'] - df['lowest_' + str(window)]) / df['lowest_' + str(window)] * 100

        # 清理临时列
        df.drop(columns=['highest_' + str(window), 'lowest_' + str(window)], inplace=True)

        return df

    # ==================== 综合计算 ====================

    def calculate_all_daily_factors(
        self,
        stock_code: str,
        start_date: str,
        end_date: str
    ) -> pd.DataFrame:
        """
        计算所有日频因子

        返回包含所有日频因子的 DataFrame
        """
        from services.common.technical_indicators import add_all_extended_technical_indicators_to_df

        # 获取 K 线数据
        df = self.get_stock_kline_data(stock_code, start_date, end_date, with_name=True)

        if df.empty:
            return df

        # ========== 计算市值类因子 ==========
        # 获取股本数据并计算市值
        market_cap_data = []
        for _, row in df.iterrows():
            trade_date = row['trade_date']
            cap_result = self.calculate_market_cap(stock_code, trade_date)
            if cap_result:
                market_cap_data.append({
                    'trade_date': trade_date,
                    'circ_market_cap': cap_result['circ_market_cap'],
                    'total_market_cap': cap_result['total_market_cap'],
                    'days_since_ipo': cap_result['days_since_ipo']
                })

        # 将市值数据合并到主 DataFrame
        if market_cap_data:
            cap_df = pd.DataFrame(market_cap_data)
            df = df.merge(cap_df, on='trade_date', how='left')
        else:
            df['circ_market_cap'] = np.nan
            df['total_market_cap'] = np.nan
            df['days_since_ipo'] = np.nan

        # ========== 计算市场表现类因子 ==========
        df = self.calculate_price_performance(df)

        # ========== 计算 KDJ 指标 ==========
        df = self.calculate_kdj(df)

        # ========== 计算 MACD 指标 ==========
        df = self.calculate_macd(df)

        # ========== 计算扩展技术指标 ==========
        df = add_all_extended_technical_indicators_to_df(df)

        # ========== 计算特色因子 ==========
        df = self.calculate_limit_up_stats(df)
        df = self.calculate_large_move_stats(df)
        df = self.calculate_price_position(df)

        # ========== 计算下期收益率 ==========
        df['next_period_change'] = df['change_pct'].shift(-1)

        # ========== 计算 is_traded ==========
        df['is_traded'] = (df['volume'] > 0).astype(int)

        return df

    def batch_calculate(
        self,
        stock_codes: List[str],
        start_date: str,
        end_date: str
    ) -> pd.DataFrame:
        """
        批量计算多只股票的日频因子

        返回：
        - 包含所有股票所有日频因子的 DataFrame
        """
        all_data = []

        for stock_code in stock_codes:
            df = self.calculate_all_daily_factors(stock_code, start_date, end_date)
            if not df.empty:
                all_data.append(df)

        if all_data:
            return pd.concat(all_data, ignore_index=True)
        else:
            return pd.DataFrame()


# ==================== 异步版本（用于后台任务） ====================

async def async_calculate_daily_factors(
    stock_code: str,
    start_date: str,
    end_date: str,
    db_path: Path = DB_PATH
) -> pd.DataFrame:
    """异步计算日频因子"""
    calculator = DailyFactorCalculator(db_path)
    loop = asyncio.get_event_loop()

    # 在线程池中执行同步计算
    result = await loop.run_in_executor(
        None,
        calculator.calculate_all_daily_factors,
        stock_code, start_date, end_date
    )

    return result


if __name__ == "__main__":
    # 测试代码
    calculator = DailyFactorCalculator()

    # 测试单只股票
    test_stock = "689009.SH"
    print(f"测试股票：{test_stock}")

    df = calculator.calculate_all_daily_factors(
        test_stock,
        "2026-02-01",
        "2026-03-31"
    )

    if not df.empty:
        print(f"\n数据条数：{len(df)}")
        print(f"\n列名：{df.columns.tolist()}")
        print(f"\n最新数据:")
        print(df.tail(3)[['trade_date', 'close', 'change_pct', 'change_10d', 'bias_20', 'kdj_k', 'dif', 'next_period_change']])
