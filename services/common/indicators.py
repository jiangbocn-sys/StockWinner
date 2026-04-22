"""
技术指标计算模块 - 兼容层

此模块保留用于向后兼容，所有技术指标计算已委托给
services/common/technical_indicators.py 统一实现

新代码应该直接使用 technical_indicators 模块
"""

from typing import List, Dict, Optional
from dataclasses import dataclass
from datetime import datetime

# 导入统一实现
from .technical_indicators import (
    calculate_ma as ma,
    calculate_ema as ema,
    calculate_rsi as rsi,
    calculate_macd as macd,
    calculate_bollinger_bands as bollinger_bands,
    calculate_atr as atr,
    calculate_kdj as kdj,
    calculate_indicators_for_screening,
)


@dataclass
class PriceData:
    """价格数据"""
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int


class TechnicalIndicators:
    """技术指标计算器 - 兼容原有接口"""

    @staticmethod
    def ma(prices: List[float], period: int) -> Optional[float]:
        """简单移动平均线"""
        return ma(prices, period)

    @staticmethod
    def ema(prices: List[float], period: int) -> Optional[float]:
        """指数移动平均线"""
        return ema(prices, period)

    @staticmethod
    def rsi(prices: List[float], period: int = 14) -> Optional[float]:
        """相对强弱指数"""
        return rsi(prices, period)

    @staticmethod
    def macd(prices: List[float], fast: int = 12, slow: int = 26, signal: int = 9) -> Optional[Dict]:
        """MACD 指标"""
        return macd(prices, fast, slow, signal)

    @staticmethod
    def bollinger_bands(prices: List[float], period: int = 20, std_dev: float = 2.0) -> Optional[Dict]:
        """布林带"""
        return bollinger_bands(prices, period, std_dev)

    @staticmethod
    def atr(highs: List[float], lows: List[float], closes: List[float], period: int = 14) -> Optional[float]:
        """平均真实波幅"""
        return atr(highs, lows, closes, period)

    @staticmethod
    def kdj(highs: List[float], lows: List[float], closes: List[float],
            n: int = 9, m1: int = 3, m2: int = 3) -> Optional[Dict]:
        """KDJ 指标"""
        return kdj(highs, lows, closes, n, m1, m2)

    @staticmethod
    def check_condition(condition: str, indicators: Dict[str, float]) -> bool:
        """
        检查条件表达式

        Args:
            condition: 条件字符串，如 "MA5>MA20", "RSI<30"
            indicators: 指标值字典

        Returns:
            条件是否成立
        """
        try:
            # 支持的指标映射
            available = {
                'MA5': indicators.get('ma5'),
                'MA10': indicators.get('ma10'),
                'MA20': indicators.get('ma20'),
                'MA60': indicators.get('ma60'),
                'RSI': indicators.get('rsi'),
                'MACD': indicators.get('macd'),
                'K': indicators.get('kdj_k'),
                'D': indicators.get('kdj_d'),
                'J': indicators.get('kdj_j'),
                'PRICE': indicators.get('price'),
                'VOLUME': indicators.get('volume'),
                'MA_VOL': indicators.get('ma_vol'),
                'MA_VOL5': indicators.get('ma_vol5'),
                'MA_VOL10': indicators.get('ma_vol10'),
            }

            # 替换条件中的指标名称
            expr = condition

            # 先处理 MA(VOL, X) 形式的条件，将其替换为对应的值
            import re
            ma_vol_pattern = r'MA\(VOL\s*,\s*(\d+)\)'
            for match in re.finditer(ma_vol_pattern, expr):
                period = int(match.group(1))
                key = f'MA_VOL{period}' if period != 5 else 'MA_VOL'
                value = available.get(key) or indicators.get(f'ma_vol{period}') or indicators.get('ma_vol')
                if value is not None:
                    expr = expr.replace(match.group(0), str(value))

            # 替换其他指标名称
            for key, value in available.items():
                if value is not None:
                    expr = expr.replace(key, str(value))

            # 安全评估表达式
            # 只允许基本的比较运算
            allowed_chars = set('0123456789.+-*/<>=%() ')
            if all(c in allowed_chars for c in expr):
                return eval(expr)

            return False
        except Exception:
            return False


class StockScreener:
    """股票筛选器"""

    def __init__(self):
        self.indicators = TechnicalIndicators()

    def evaluate(self, stock_data: Dict, conditions: List[str]) -> Dict:
        """
        评估股票是否符合条件

        Args:
            stock_data: 股票数据，包含 prices、highs、lows、volumes 等
            conditions: 条件列表

        Returns:
            评估结果
        """
        prices = stock_data.get('prices', [])
        highs = stock_data.get('highs', prices)
        lows = stock_data.get('lows', prices)

        # 计算指标
        indicators = {
            'ma5': self.indicators.ma(prices, 5),
            'ma10': self.indicators.ma(prices, 10),
            'ma20': self.indicators.ma(prices, 20),
            'ma60': self.indicators.ma(prices, 60),
            'rsi': self.indicators.rsi(prices, 14),
            'price': prices[-1] if prices else None,
            'volume': stock_data.get('volume', 0),
        }

        # 计算 KDJ
        kdj = self.indicators.kdj(highs, lows, prices)
        if kdj:
            indicators['kdj_k'] = kdj['k']
            indicators['kdj_d'] = kdj['d']
            indicators['kdj_j'] = kdj['j']

        # 计算 MACD
        macd = self.indicators.macd(prices)
        if macd:
            indicators['macd'] = macd['macd']

        # 检查条件
        matched_conditions = []
        unmatched_conditions = []

        for condition in conditions:
            if self.indicators.check_condition(condition, indicators):
                matched_conditions.append(condition)
            else:
                unmatched_conditions.append(condition)

        all_matched = len(unmatched_conditions) == 0

        return {
            "matched": all_matched,
            "matched_conditions": matched_conditions,
            "unmatched_conditions": unmatched_conditions,
            "indicators": indicators,
            "score": len(matched_conditions) / len(conditions) if conditions else 0
        }


# 使用示例
if __name__ == "__main__":
    # 测试数据
    prices = [100 + i * 0.5 for i in range(30)]

    print("MA5:", TechnicalIndicators.ma(prices, 5))
    print("MA20:", TechnicalIndicators.ma(prices, 20))
    print("RSI:", TechnicalIndicators.rsi(prices, 14))
    print("MACD:", TechnicalIndicators.macd(prices))
    print("布林带:", TechnicalIndicators.bollinger_bands(prices, 20))
