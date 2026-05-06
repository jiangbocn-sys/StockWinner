"""
交易策略条件评估器注册表

条件评估器采用插件注册表模式，一种监控循环统一处理多种触发类型：
- price: 价格监测（突破/跌破/等于目标价）
- change_pct: 涨跌幅监测（涨/跌超过阈值）
- volume: 交易量监测（量比/绝对量）
"""

from abc import ABC, abstractmethod
from typing import Any, Dict


class TriggerEvaluator(ABC):
    """条件评估器基类"""

    type: str = ""

    @abstractmethod
    async def evaluate(
        self,
        stock_code: str,
        market_data: Dict[str, Any],
        kline_data: Dict[str, Any],
        condition: Dict[str, Any],
    ) -> bool:
        """
        评估条件是否触发

        Args:
            stock_code: 股票代码
            market_data: 实时行情数据（现价、涨跌幅、成交量等）
            kline_data: K线历史数据（用于计算均量等）
            condition: 条件参数

        Returns:
            True 表示条件触发
        """
        pass


class PriceMonitorEvaluator(TriggerEvaluator):
    """价格监测评估器

    condition 参数：
        direction: "above" | "below" | "equals"
        target_price: 目标价格
        tolerance: 容差（用于 equals，默认 0.01）
    """

    type = "price"

    async def evaluate(
        self,
        stock_code: str,
        market_data: Dict[str, Any],
        kline_data: Dict[str, Any],
        condition: Dict[str, Any],
    ) -> bool:
        current_price = market_data.get("current_price") or market_data.get("price")
        if current_price is None:
            return False

        target_price = condition.get("target_price")
        if target_price is None:
            return False

        direction = condition.get("direction", "above")
        tolerance = condition.get("tolerance", 0.01)

        if direction == "above":
            return current_price >= target_price
        elif direction == "below":
            return current_price <= target_price
        elif direction == "equals":
            return abs(current_price - target_price) <= tolerance
        return False


class ChangePctMonitorEvaluator(TriggerEvaluator):
    """涨跌幅监测评估器

    condition 参数：
        direction: "up" | "down"
        threshold: 阈值（百分比，如 5.0 表示 5%）
    """

    type = "change_pct"

    async def evaluate(
        self,
        stock_code: str,
        market_data: Dict[str, Any],
        kline_data: Dict[str, Any],
        condition: Dict[str, Any],
    ) -> bool:
        change_pct = market_data.get("change_pct") or market_data.get("change_percent")
        if change_pct is None:
            return False

        direction = condition.get("direction", "up")
        threshold = condition.get("threshold", 5.0)

        if direction == "up":
            return change_pct >= threshold
        elif direction == "down":
            return change_pct <= -threshold
        return False


class VolumeMonitorEvaluator(TriggerEvaluator):
    """交易量监测评估器

    condition 参数：
        mode: "ratio" | "absolute"
        threshold: 阈值
            ratio 模式：当前量 / N日均量 > threshold（如 2.0 表示放量 2 倍）
            absolute 模式：成交量 > threshold（手）
        avg_days: 均量计算天数（ratio 模式，默认 5）
    """

    type = "volume"

    async def evaluate(
        self,
        stock_code: str,
        market_data: Dict[str, Any],
        kline_data: Dict[str, Any],
        condition: Dict[str, Any],
    ) -> bool:
        mode = condition.get("mode", "ratio")
        threshold = condition.get("threshold")
        if threshold is None:
            return False

        current_volume = market_data.get("volume")
        if current_volume is None:
            return False

        if mode == "absolute":
            return current_volume >= threshold
        elif mode == "ratio":
            # 从 K 线数据计算 N 日均量
            avg_days = condition.get("avg_days", 5)
            klines = kline_data.get("klines", []) if isinstance(kline_data, dict) else []

            if not klines or len(klines) < avg_days:
                return False

            # 取最近 avg_days 天的成交量计算均值（不含今日）
            volumes = []
            for k in klines[:avg_days]:
                vol = k.get("volume") or k.get("vol")
                if vol is not None:
                    volumes.append(vol)

            if not volumes:
                return False

            avg_volume = sum(volumes) / len(volumes)
            if avg_volume <= 0:
                return False

            volume_ratio = current_volume / avg_volume
            return volume_ratio >= threshold

        return False


# 评估器注册表
EVALUATOR_REGISTRY: Dict[str, TriggerEvaluator] = {
    "price": PriceMonitorEvaluator(),
    "change_pct": ChangePctMonitorEvaluator(),
    "volume": VolumeMonitorEvaluator(),
}


def get_evaluator(type_name: str) -> TriggerEvaluator:
    """根据类型获取评估器"""
    evaluator = EVALUATOR_REGISTRY.get(type_name)
    if evaluator is None:
        raise ValueError(f"未知的评估器类型: {type_name}")
    return evaluator


def register_evaluator(type_name: str, evaluator: TriggerEvaluator):
    """注册自定义评估器"""
    EVALUATOR_REGISTRY[type_name] = evaluator
