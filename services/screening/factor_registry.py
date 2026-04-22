"""
因子注册表 - 选股程序优化核心模块

功能：
1. 管理所有可用因子（数据库因子 + 动态计算因子）
2. 从选股条件中自动提取需要的指标
3. 批量从数据库获取预计算因子
4. 按需调用计算器计算缺失因子

优势：
- 优先使用数据库因子，减少重复计算
- 新增选股条件只需注册新因子，无需修改选股逻辑
- 支持平滑降级：数据库查询失败时自动切换到动态计算
"""

import re
import sqlite3
import pandas as pd
from pathlib import Path
from typing import Dict, List, Set, Tuple, Optional, Any, Callable
from datetime import datetime

# 数据库路径
KLINE_DB = Path(__file__).parent.parent.parent / "data" / "kline.db"

# 因子映射配置：选股条件中的指标名 → 数据库字段
# 注意：数据库因子必须存在于 stock_daily_factors 表中
FACTOR_MAPPING = {
    # ==================== 均线类（数据库已有） ====================
    'MA5': {'table': 'stock_daily_factors', 'column': 'ma5', 'source': 'db'},
    'MA10': {'table': 'stock_daily_factors', 'column': 'ma10', 'source': 'db'},
    'MA20': {'table': 'stock_daily_factors', 'column': 'ma20', 'source': 'db'},
    'MA60': {'table': 'stock_daily_factors', 'column': 'ma60', 'source': 'db'},

    # ==================== 指数均线类（数据库已有） ====================
    'EMA12': {'table': 'stock_daily_factors', 'column': 'ema12', 'source': 'db'},
    'EMA26': {'table': 'stock_daily_factors', 'column': 'ema26', 'source': 'db'},

    # ==================== KDJ 类（数据库已有） ====================
    'K': {'table': 'stock_daily_factors', 'column': 'kdj_k', 'source': 'db'},
    'D': {'table': 'stock_daily_factors', 'column': 'kdj_d', 'source': 'db'},
    'J': {'table': 'stock_daily_factors', 'column': 'kdj_j', 'source': 'db'},

    # ==================== MACD 类（数据库已有） ====================
    'DIF': {'table': 'stock_daily_factors', 'column': 'dif', 'source': 'db'},
    'DEA': {'table': 'stock_daily_factors', 'column': 'dea', 'source': 'db'},
    'MACD': {'table': 'stock_daily_factors', 'column': 'macd', 'source': 'db'},

    # ==================== 涨跌幅类（数据库只有 change_10d/20d） ====================
    'CHANGE_5D': {'calculator': 'calculate_change_5d', 'source': 'calc'},
    'CHANGE_10D': {'table': 'stock_daily_factors', 'column': 'change_10d', 'source': 'db'},
    'CHANGE_20D': {'table': 'stock_daily_factors', 'column': 'change_20d', 'source': 'db'},

    # ==================== 动量类（数据库已有） ====================
    'MOMENTUM_10D': {'table': 'stock_daily_factors', 'column': 'momentum_10d', 'source': 'db'},
    'MOMENTUM_20D': {'table': 'stock_daily_factors', 'column': 'momentum_20d', 'source': 'db'},

    # ==================== 乖离率类（数据库已有） ====================
    'BIAS_5': {'table': 'stock_daily_factors', 'column': 'bias_5', 'source': 'db'},
    'BIAS_10': {'table': 'stock_daily_factors', 'column': 'bias_10', 'source': 'db'},
    'BIAS_20': {'table': 'stock_daily_factors', 'column': 'bias_20', 'source': 'db'},

    # ==================== 振幅类（数据库已有） ====================
    'AMPLITUDE_5': {'table': 'stock_daily_factors', 'column': 'amplitude_5', 'source': 'db'},
    'AMPLITUDE_10': {'table': 'stock_daily_factors', 'column': 'amplitude_10', 'source': 'db'},
    'AMPLITUDE_20': {'table': 'stock_daily_factors', 'column': 'amplitude_20', 'source': 'db'},

    # ==================== 波动率类（数据库已有） ====================
    'CHANGE_STD_5': {'table': 'stock_daily_factors', 'column': 'change_std_5', 'source': 'db'},
    'CHANGE_STD_10': {'table': 'stock_daily_factors', 'column': 'change_std_10', 'source': 'db'},
    'CHANGE_STD_20': {'table': 'stock_daily_factors', 'column': 'change_std_20', 'source': 'db'},

    # ==================== 成交额波动类（数据库已有） ====================
    'AMOUNT_STD_5': {'table': 'stock_daily_factors', 'column': 'amount_std_5', 'source': 'db'},
    'AMOUNT_STD_10': {'table': 'stock_daily_factors', 'column': 'amount_std_10', 'source': 'db'},
    'AMOUNT_STD_20': {'table': 'stock_daily_factors', 'column': 'amount_std_20', 'source': 'db'},

    # ==================== 估值类（已移入 monthly_factors 表） ====================
    # 注意：以下因子数据在 stock_monthly_factors 表中，按月更新
    # 选股时需要查询该股票最近一个月的因子数据
    'PE_INV': {'table': 'stock_monthly_factors', 'column': 'pe_inverse', 'source': 'db'},
    'PB_INV': {'table': 'stock_monthly_factors', 'column': 'pb_inverse', 'source': 'db'},
    'PE_TTM': {'table': 'stock_monthly_factors', 'column': 'pe_ttm', 'source': 'db'},
    'PB': {'table': 'stock_monthly_factors', 'column': 'pb', 'source': 'db'},
    'PS_TTM': {'table': 'stock_monthly_factors', 'column': 'ps_ttm', 'source': 'db'},
    'PCF': {'table': 'stock_monthly_factors', 'column': 'pcf', 'source': 'db'},

    # ==================== 市值类（数据库已有） ====================
    'CIRC_MARKET_CAP': {'table': 'stock_daily_factors', 'column': 'circ_market_cap', 'source': 'db'},
    'TOTAL_MARKET_CAP': {'table': 'stock_daily_factors', 'column': 'total_market_cap', 'source': 'db'},

    # ==================== 其他（数据库已有） ====================
    'DAYS_SINCE_IPO': {'table': 'stock_daily_factors', 'column': 'days_since_ipo', 'source': 'db'},
    'NEXT_PERIOD_CHANGE': {'table': 'stock_daily_factors', 'column': 'next_period_change', 'source': 'db'},

    # ==================== 需要动态计算的指标 ====================
    'RSI': {'calculator': 'calculate_rsi', 'source': 'calc', 'params': {'period': 14}},

    # ==================== RSI 类（数据库已有） ====================
    'RSI_14': {'table': 'stock_daily_factors', 'column': 'rsi_14', 'source': 'db'},

    # ==================== 布林带类（数据库已有） ====================
    'BOLL_UPPER': {'table': 'stock_daily_factors', 'column': 'boll_upper', 'source': 'db'},
    'BOLL_MIDDLE': {'table': 'stock_daily_factors', 'column': 'boll_middle', 'source': 'db'},
    'BOLL_LOWER': {'table': 'stock_daily_factors', 'column': 'boll_lower', 'source': 'db'},

    # ==================== ATR 类（数据库已有） ====================
    'ATR_14': {'table': 'stock_daily_factors', 'column': 'atr_14', 'source': 'db'},

    # ==================== CCI 类（数据库已有） ====================
    'CCI': {'table': 'stock_daily_factors', 'column': 'cci_20', 'source': 'db'},

    # ==================== ADX 类（数据库已有） ====================
    'ADX': {'table': 'stock_daily_factors', 'column': 'adx', 'source': 'db'},

    # ==================== 历史波动率（数据库已有） ====================
    'HV_20': {'table': 'stock_daily_factors', 'column': 'hv_20', 'source': 'db'},

    # ==================== 成交量类（数据库已有） ====================
    'OBV': {'table': 'stock_daily_factors', 'column': 'obv', 'source': 'db'},
    'VOLUME_RATIO': {'table': 'stock_daily_factors', 'column': 'volume_ratio', 'source': 'db'},

    # ==================== 涨停特色因子（数据库已有） ====================
    'LIMIT_UP_COUNT_10D': {'table': 'stock_daily_factors', 'column': 'limit_up_count_10d', 'source': 'db'},
    'LIMIT_UP_COUNT_20D': {'table': 'stock_daily_factors', 'column': 'limit_up_count_20d', 'source': 'db'},
    'LIMIT_UP_COUNT_30D': {'table': 'stock_daily_factors', 'column': 'limit_up_count_30d', 'source': 'db'},
    'CONSECUTIVE_LIMIT_UP': {'table': 'stock_daily_factors', 'column': 'consecutive_limit_up', 'source': 'db'},
    # 注：first_limit_up_days 和 highest_board_10d 已移除，暂不提供

    # ==================== 异动类（数据库已有） ====================
    'LARGE_GAIN_5D_COUNT': {'table': 'stock_daily_factors', 'column': 'large_gain_5d_count', 'source': 'db'},
    'LARGE_LOSS_5D_COUNT': {'table': 'stock_daily_factors', 'column': 'large_loss_5d_count', 'source': 'db'},
    'GAP_UP_RATIO': {'table': 'stock_daily_factors', 'column': 'gap_up_ratio', 'source': 'db'},

    # ==================== 筹码类（数据库已有） ====================
    'CLOSE_TO_HIGH_250D': {'table': 'stock_daily_factors', 'column': 'close_to_high_250d', 'source': 'db'},
    'CLOSE_TO_LOW_250D': {'table': 'stock_daily_factors', 'column': 'close_to_low_250d', 'source': 'db'},

    # ==================== 盈利类（已移入 monthly_factors 表） ====================
    'ROE': {'table': 'stock_monthly_factors', 'column': 'roe', 'source': 'db'},
    'ROA': {'table': 'stock_monthly_factors', 'column': 'roa', 'source': 'db'},
    'GROSS_MARGIN': {'table': 'stock_monthly_factors', 'column': 'gross_margin', 'source': 'db'},
    'NET_MARGIN': {'table': 'stock_monthly_factors', 'column': 'net_margin', 'source': 'db'},

    # ==================== 成长类（已移入 monthly_factors 表） ====================
    'REVENUE_GROWTH_YOY': {'table': 'stock_monthly_factors', 'column': 'revenue_growth_yoy', 'source': 'db'},
    'REVENUE_GROWTH_QOQ': {'table': 'stock_monthly_factors', 'column': 'revenue_growth_qoq', 'source': 'db'},
    'NET_PROFIT_GROWTH_YOY': {'table': 'stock_monthly_factors', 'column': 'net_profit_growth_yoy', 'source': 'db'},
    'NET_PROFIT_GROWTH_QOQ': {'table': 'stock_monthly_factors', 'column': 'net_profit_growth_qoq', 'source': 'db'},

    # ==================== 基础数据（从 kline_data 读取） ====================
    'PRICE': {'table': 'kline_data', 'column': 'close', 'source': 'latest'},
    'OPEN': {'table': 'kline_data', 'column': 'open', 'source': 'latest'},
    'HIGH': {'table': 'kline_data', 'column': 'high', 'source': 'latest'},
    'LOW': {'table': 'kline_data', 'column': 'low', 'source': 'latest'},
    'VOLUME': {'table': 'kline_data', 'column': 'volume', 'source': 'latest'},
    'AMOUNT': {'table': 'kline_data', 'column': 'amount', 'source': 'latest'},
}

# 条件解析正则表达式
INDICATOR_PATTERN = re.compile(r'\b([A-Z_]+(?:\d+)?)\b')


class FactorRegistry:
    """因子注册表 - 管理所有可用因子"""

    def __init__(self, db_path: Path = KLINE_DB):
        self.db_path = db_path
        self._mapping = FACTOR_MAPPING.copy()
        self._calculators: Dict[str, Callable] = {}
        self._register_builtin_calculators()

    def _register_builtin_calculators(self):
        """注册内置计算器"""
        from services.common.technical_indicators import (
            calculate_rsi, calculate_ema, calculate_ma,
            add_kdj_to_df, calculate_kdj
        )
        self._calculators['calculate_rsi'] = calculate_rsi
        self._calculators['calculate_ema'] = calculate_ema
        self._calculators['calculate_ma'] = calculate_ma
        self._calculators['calculate_kdj'] = calculate_kdj

        # 注册 KDJ 分量计算器
        def calculate_kdj_k(closes, highs=None, lows=None):
            """计算 KDJ 的 K 值"""
            if highs is None or lows is None:
                return None
            result = calculate_kdj(highs, lows, closes)
            return result.get('k') if result else None

        def calculate_kdj_d(closes, highs=None, lows=None):
            """计算 KDJ 的 D 值"""
            if highs is None or lows is None:
                return None
            result = calculate_kdj(highs, lows, closes)
            return result.get('d') if result else None

        def calculate_kdj_j(closes, highs=None, lows=None):
            """计算 KDJ 的 J 值"""
            if highs is None or lows is None:
                return None
            result = calculate_kdj(highs, lows, closes)
            return result.get('j') if result else None

        def calculate_change_5d(closes):
            """计算 5 日涨跌幅"""
            if len(closes) < 6:
                return None
            return (closes[-1] - closes[-6]) / closes[-6] * 100

        self._calculators['calculate_kdj_k'] = calculate_kdj_k
        self._calculators['calculate_kdj_d'] = calculate_kdj_d
        self._calculators['calculate_kdj_j'] = calculate_kdj_j
        self._calculators['calculate_change_5d'] = calculate_change_5d

    def register_factor(
        self,
        name: str,
        table: Optional[str] = None,
        column: Optional[str] = None,
        calculator: Optional[Callable] = None,
        source: str = 'db',
        params: Optional[Dict] = None
    ):
        """
        注册新因子

        Args:
            name: 因子名称（用于条件表达式中）
            table: 数据库表名（source='db' 时必需）
            column: 数据库列名（source='db' 时必需）
            calculator: 计算函数（source='calc' 时必需）
            source: 数据来源 'db' | 'calc' | 'latest'
            params: 计算器参数
        """
        self._mapping[name] = {
            'table': table,
            'column': column,
            'calculator': calculator.__name__ if calculator else None,
            'source': source,
            'params': params or {}
        }
        if calculator:
            self._calculators[calculator.__name__] = calculator

    def extract_required_factors(self, conditions: List[str]) -> Set[str]:
        """
        从条件列表中提取需要的指标名称

        Args:
            conditions: 条件列表，如 ["MA5 > MA20", "RSI < 30"]

        Returns:
            需要的指标名称集合
        """
        factors = set()
        for condition in conditions:
            matches = INDICATOR_PATTERN.findall(condition)
            for match in matches:
                if match in self._mapping:
                    factors.add(match)
        return factors

    def classify_factors(
        self,
        factors: Set[str]
    ) -> Tuple[Set[str], Set[str]]:
        """
        将因子分类为数据库因子和需要计算的因子

        Returns:
            (db_factors, calc_factors)
        """
        db_factors = set()
        calc_factors = set()

        for factor in factors:
            config = self._mapping.get(factor)
            if not config:
                # 未知因子，默认需要计算
                calc_factors.add(factor)
            elif config['source'] in ('db', 'latest'):
                db_factors.add(factor)
            else:
                calc_factors.add(factor)

        return db_factors, calc_factors

    def fetch_db_factors(
        self,
        factors: Set[str],
        trade_date: str
    ) -> pd.DataFrame:
        """
        批量从数据库获取因子数据

        Args:
            factors: 需要的因子名称集合
            trade_date: 交易日期

        Returns:
            DataFrame，索引为 stock_code，列为各因子值
        """
        # 按表分组
        table_factors: Dict[str, List[str]] = {}
        for factor in factors:
            config = self._mapping.get(factor)
            if not config or config['source'] not in ('db', 'latest'):
                continue

            table = config['table']
            if table not in table_factors:
                table_factors[table] = []
            table_factors[table].append(factor)

        all_data = {}

        for table, factor_list in table_factors.items():
            # 构建查询列
            columns = ['stock_code']
            column_map = {}  # 列名 → 因子名

            for factor in factor_list:
                config = self._mapping[factor]
                if config['source'] == 'latest':
                    # 最新数据需要特殊处理
                    continue
                columns.append(config['column'])
                column_map[config['column']] = factor

            if not columns:
                continue

            # 查询数据库
            query = f"""
                SELECT {', '.join(columns)}
                FROM {table}
                WHERE trade_date = ?
            """

            try:
                conn = sqlite3.connect(str(self.db_path), timeout=60)
                conn.execute("PRAGMA busy_timeout=60000")
                df = pd.read_sql_query(query, conn, params=(trade_date,))
                conn.close()

                if not df.empty:
                    df = df.set_index('stock_code')
                    # 重命名列
                    for col, factor in column_map.items():
                        if col in df.columns:
                            all_data[factor] = df[col]

            except Exception as e:
                print(f"[FactorRegistry] 查询 {table} 失败：{e}")
                # 查询失败，返回空 DataFrame
                return pd.DataFrame()

        # 处理 latest 类型的因子（从 kline_data 获取最新数据）
        latest_factors = [f for f in factors
                         if self._mapping.get(f, {}).get('source') == 'latest']
        if latest_factors:
            try:
                conn = sqlite3.connect(str(self.db_path), timeout=60)
                conn.execute("PRAGMA busy_timeout=60000")
                # 获取每个股票的最新数据
                query = """
                    SELECT stock_code, close, open, high, low, volume, amount
                    FROM (
                        SELECT stock_code, close, open, high, low, volume, amount,
                               ROW_NUMBER() OVER (PARTITION BY stock_code ORDER BY trade_date DESC) as rn
                        FROM kline_data
                    )
                    WHERE rn = 1
                """
                df = pd.read_sql_query(query, conn)
                conn.close()

                if not df.empty:
                    df = df.set_index('stock_code')
                    col_map = {'close': 'PRICE', 'open': 'OPEN', 'high': 'HIGH',
                               'low': 'LOW', 'volume': 'VOLUME', 'amount': 'AMOUNT'}
                    for col, factor in col_map.items():
                        if factor in latest_factors and col in df.columns:
                            all_data[factor] = df[col]

            except Exception as e:
                print(f"[FactorRegistry] 查询最新数据失败：{e}")

        if not all_data:
            return pd.DataFrame()

        # 合并所有数据
        result = pd.DataFrame(all_data)
        return result

    def get_calculator(self, name: str) -> Optional[Callable]:
        """获取计算器函数"""
        return self._calculators.get(name)

    def get_factor_config(self, name: str) -> Optional[Dict]:
        """获取因子配置"""
        return self._mapping.get(name)

    def has_factor(self, name: str) -> bool:
        """检查因子是否已注册"""
        return name in self._mapping

    def get_all_factors(self) -> List[str]:
        """获取所有已注册的因子名称"""
        return list(self._mapping.keys())


# 全局单例
_registry: Optional[FactorRegistry] = None


def get_factor_registry() -> FactorRegistry:
    """获取全局因子注册表实例"""
    global _registry
    if _registry is None:
        _registry = FactorRegistry()
    return _registry
