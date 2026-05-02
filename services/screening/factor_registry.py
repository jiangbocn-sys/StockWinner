"""
因子注册表 - 从数据库动态加载因子元数据

功能：
1. 从 factor_metadata 表动态加载因子配置
2. 从选股条件中自动提取需要的指标
3. 批量从数据库获取预计算因子
4. 按需调用计算器计算缺失因子
"""

import re
import sqlite3
import pandas as pd
from pathlib import Path
from typing import Dict, List, Set, Tuple, Optional, Any, Callable
from datetime import datetime

# 数据库路径
KLINE_DB = Path(__file__).parent.parent.parent / "data" / "kline.db"

# 条件解析正则表达式
INDICATOR_PATTERN = re.compile(r'\b([A-Z_]+(?:\d+)?)\b')


class FactorRegistry:
    """因子注册表 - 从数据库动态加载因子元数据"""

    def __init__(self, db_path: Path = KLINE_DB):
        self.db_path = db_path
        self._mapping: Dict[str, Dict] = {}  # 因子ID → 配置
        self._calculators: Dict[str, Callable] = {}
        self._load_from_db()
        self._register_builtin_calculators()

    def _load_from_db(self):
        """从 factor_metadata 表加载因子配置"""
        try:
            conn = sqlite3.connect(str(self.db_path), timeout=60)
            conn.execute("PRAGMA busy_timeout=60000")
            cursor = conn.cursor()

            cursor.execute("""
                SELECT factor_id, factor_name, category, data_table, data_column,
                       update_freq, is_filterable, unit, description
                FROM factor_metadata WHERE is_enabled = 1
            """)

            for row in cursor.fetchall():
                factor_id = row[0]
                self._mapping[factor_id] = {
                    'factor_name': row[1],
                    'category': row[2],
                    'data_table': row[3],
                    'data_column': row[4],
                    'update_freq': row[5],
                    'is_filterable': row[6],
                    'unit': row[7],
                    'description': row[8],
                    'source': 'db'
                }

            conn.close()
            print(f"[FactorRegistry] 从数据库加载 {len(self._mapping)} 个因子")

        except Exception as e:
            print(f"[FactorRegistry] 加载因子元数据失败: {e}")
            # 如果数据库加载失败，使用备用映射
            self._load_fallback_mapping()

    def _load_fallback_mapping(self):
        """备用映射（数据库加载失败时使用）"""
        # 最基本的因子映射
        self._mapping = {
            'TOTAL_MARKET_CAP': {'data_table': 'stock_daily_factors', 'data_column': 'total_market_cap', 'is_filterable': 1},
            'CIRC_MARKET_CAP': {'data_table': 'stock_daily_factors', 'data_column': 'circ_market_cap', 'is_filterable': 1},
            'MA5': {'data_table': 'stock_daily_factors', 'data_column': 'ma5', 'is_filterable': 0},
            'MA10': {'data_table': 'stock_daily_factors', 'data_column': 'ma10', 'is_filterable': 0},
            'DIF': {'data_table': 'stock_daily_factors', 'data_column': 'dif', 'is_filterable': 0},
            'DEA': {'data_table': 'stock_daily_factors', 'data_column': 'dea', 'is_filterable': 0},
            'RSI_14': {'data_table': 'stock_daily_factors', 'data_column': 'rsi_14', 'is_filterable': 0},
            'VOLUME_RATIO': {'data_table': 'stock_daily_factors', 'data_column': 'volume_ratio', 'is_filterable': 0},
            'PE_TTM': {'data_table': 'stock_monthly_factors', 'data_column': 'pe_ttm', 'is_filterable': 1},
            'PB': {'data_table': 'stock_monthly_factors', 'data_column': 'pb', 'is_filterable': 1},
            'ROE': {'data_table': 'stock_monthly_factors', 'data_column': 'roe', 'is_filterable': 1},
        }

    def _register_builtin_calculators(self):
        """注册内置计算器（用于动态计算缺失因子）"""
        from services.common.technical_indicators import (
            calculate_rsi, calculate_ema, calculate_ma,
            calculate_kdj
        )
        self._calculators['calculate_rsi'] = calculate_rsi
        self._calculators['calculate_ema'] = calculate_ema
        self._calculators['calculate_ma'] = calculate_ma
        self._calculators['calculate_kdj'] = calculate_kdj

    def reload(self):
        """重新从数据库加载因子配置"""
        self._mapping.clear()
        self._load_from_db()

    def get_filterable_factors(self) -> List[Dict]:
        """获取可用于静态筛选的因子列表"""
        return [
            {
                'factor_id': fid,
                'factor_name': cfg.get('factor_name', fid),
                'category': cfg.get('category', ''),
                'unit': cfg.get('unit', ''),
                'update_freq': cfg.get('update_freq', 'daily')
            }
            for fid, cfg in self._mapping.items()
            if cfg.get('is_filterable', 0) == 1
        ]

    def extract_required_factors(self, conditions: Any) -> Set[str]:
        """
        从条件中提取需要的指标名称（支持嵌套格式）

        Args:
            conditions: 可以是：
                - 字符串列表：["MA5 > MA20", "RSI_14 < 30"]
                - 嵌套字典：{"logic": "AND", "conditions": [...]}
                - 单个字符串："RSI_14 < 30"

        Returns:
            需要的指标名称集合
        """
        factors = set()

        # 处理嵌套字典格式
        if isinstance(conditions, dict):
            if 'conditions' in conditions:
                # 递归处理嵌套条件
                for c in conditions['conditions']:
                    factors.update(self.extract_required_factors(c))
            return factors

        # 处理列表格式
        if isinstance(conditions, list):
            for condition in conditions:
                factors.update(self.extract_required_factors(condition))
            return factors

        # 处理单个字符串条件
        if isinstance(conditions, str):
            matches = INDICATOR_PATTERN.findall(conditions)
            for match in matches:
                if match in self._mapping:
                    factors.add(match)
            # 也处理特殊条件名（如 DIF_CROSS_UP_DEA）
            special_conditions = ['DIF_CROSS_UP_DEA', 'DIF_CROSS_DOWN_DEA',
                                   'MA5_CROSS_UP_MA10', 'MA5_CROSS_DOWN_MA10',
                                   'MA10_CROSS_UP_MA20']
            for special in special_conditions:
                if special in conditions:
                    # 提取相关因子
                    if 'DIF' in special or 'DEA' in special:
                        factors.add('DIF')
                        factors.add('DEA')
                    if 'MA5' in special:
                        factors.add('MA5')
                    if 'MA10' in special:
                        factors.add('MA10')
                    if 'MA20' in special:
                        factors.add('MA20')
            return factors

        return factors

    def classify_factors(self, factors: Set[str]) -> Tuple[Set[str], Set[str]]:
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
            elif config.get('source') == 'db':
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
            if not config:
                continue

            table = config.get('data_table')
            if not table:
                continue

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
                col = config.get('data_column')
                if col:
                    columns.append(col)
                    column_map[col] = factor

            if len(columns) <= 1:
                continue

            # 查询数据库
            # 根据表类型确定日期条件
            if table == 'stock_monthly_factors':
                # 月频因子：获取最近一期的数据
                date_condition = "report_date = (SELECT MAX(report_date) FROM stock_monthly_factors)"
            else:
                # 日频因子：使用指定日期
                date_condition = f"trade_date = '{trade_date}'"

            query = f"""
                SELECT {', '.join(columns)}
                FROM {table}
                WHERE {date_condition}
            """

            try:
                conn = sqlite3.connect(str(self.db_path), timeout=60)
                conn.execute("PRAGMA busy_timeout=60000")
                df = pd.read_sql_query(query, conn)
                conn.close()

                if not df.empty:
                    df = df.set_index('stock_code')
                    # 重命名列
                    for col, factor in column_map.items():
                        if col in df.columns:
                            all_data[factor] = df[col]

            except Exception as e:
                print(f"[FactorRegistry] 查询 {table} 失败：{e}")

        if not all_data:
            return pd.DataFrame()

        # 合并所有数据
        result = pd.DataFrame(all_data)
        return result

    def get_factor_config(self, name: str) -> Optional[Dict]:
        """获取因子配置"""
        return self._mapping.get(name)

    def has_factor(self, name: str) -> bool:
        """检查因子是否已注册"""
        return name in self._mapping

    def get_all_factors(self) -> List[str]:
        """获取所有已注册的因子名称"""
        return list(self._mapping.keys())

    def get_factors_by_category(self, category: str) -> List[str]:
        """获取指定类别的因子"""
        return [
            fid for fid, cfg in self._mapping.items()
            if cfg.get('category') == category
        ]

    def get_factors_by_freq(self, freq: str) -> List[str]:
        """获取指定更新频率的因子"""
        return [
            fid for fid, cfg in self._mapping.items()
            if cfg.get('update_freq') == freq
        ]


# 全局单例
_registry: Optional[FactorRegistry] = None


def get_factor_registry() -> FactorRegistry:
    """获取全局因子注册表实例"""
    global _registry
    if _registry is None:
        _registry = FactorRegistry()
    return _registry


def reload_factor_registry():
    """重新加载因子注册表"""
    global _registry
    if _registry:
        _registry.reload()
    else:
        _registry = FactorRegistry()