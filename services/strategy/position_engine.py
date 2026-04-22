"""
持仓策略引擎 (Position Strategy Engine)

根据市场环境决定持仓比例、个股份额占比，在不同市场环境下做出仓位决策

核心功能：
1. 市场环境监测 - 指数走势、市场情绪、涨跌家数比
2. 仓位控制 - 满仓/半仓/空仓决策
3. 持仓周期管理 - 短线/中线/长线策略
4. 分散度控制 - 单只股票最大持仓比例
"""

from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from enum import Enum


# 中国时区
CHINA_TZ = timezone(timedelta(hours=8))

def get_china_time():
    """获取中国时区时间"""
    return datetime.now(CHINA_TZ).replace(tzinfo=None)


class MarketCondition(Enum):
    """市场环境状态"""
    STRONG_BULL = "strong_bull"  # 强势牛市
    WEAK_BULL = "weak_bull"  # 弱势牛市
    CONSOLIDATION = "consolidation"  # 震荡盘整
    WEAK_BEAR = "weak_bear"  # 弱势熊市
    STRONG_BEAR = "strong_bear"  # 强势熊市


class PositionLevel(Enum):
    """仓位等级"""
    FULL = "full"  # 满仓 (80-100%)
    HEAVY = "heavy"  # 重仓 (60-80%)
    HALF = "half"  # 半仓 (40-60%)
    LIGHT = "light"  # 轻仓 (20-40%)
    VERY_LIGHT = "very_light"  # 很轻仓 (10-20%)
    EMPTY = "empty"  # 空仓 (0-10%)


class HoldingPeriod(Enum):
    """持仓周期类型"""
    ULTRA_SHORT = "ultra_short"  # 超短线 (1-3 天)
    SHORT = "short"  # 短线 (3-10 天)
    MEDIUM = "medium"  # 中线 (10-30 天)
    LONG = "long"  # 长线 (30 天以上)


@dataclass
class MarketIndicator:
    """市场指标数据"""
    # 指数数据
    index_code: str
    index_name: str
    current_price: float
    change_pct: float  # 涨跌幅
    volume: int  # 成交量
    amount: float  # 成交额

    # 趋势指标
    ma5: Optional[float] = None
    ma10: Optional[float] = None
    ma20: Optional[float] = None
    ma60: Optional[float] = None

    # 市场情绪
    advance_count: int = 0  # 上涨家数
    decline_count: int = 0  # 下跌家数
    limit_up_count: int = 0  # 涨停家数
    limit_down_count: int = 0  # 跌停家数

    # 其他
    trading_date: str = field(default_factory=lambda: get_china_time().strftime('%Y-%m-%d'))


@dataclass
class MarketAnalysis:
    """市场分析结果"""
    market_condition: MarketCondition  # 市场环境判断
    confidence: float  # 置信度 (0-1)
    trend_score: float  # 趋势得分 (-1 到 1，负为空头，正为多头)
    sentiment_score: float  # 情绪得分 (0-1)
    risk_level: str  # 风险等级：low/medium/high/very_high

    # 详细分析
    analysis_details: Dict[str, Any] = field(default_factory=dict)
    analyzed_at: str = field(default_factory=lambda: get_china_time().isoformat())


@dataclass
class PositionRecommendation:
    """仓位建议"""
    position_level: PositionLevel  # 建议仓位等级
    position_pct: float  # 建议仓位百分比 (0-1)
    max_single_stock_pct: float  # 单只股票最大仓位
    max_holding_count: int  # 最大持仓数量
    recommended_period: HoldingPeriod  # 建议持仓周期

    # 配置参数
    stop_loss_pct: float  # 止损比例
    take_profit_pct: float  # 止盈比例
    trailing_stop_pct: float  # 移动止损比例

    # 依据说明
    reasoning: List[str] = field(default_factory=list)
    risk_warnings: List[str] = field(default_factory=list)
    generated_at: str = field(default_factory=lambda: get_china_time().isoformat())


@dataclass
class PositionConfig:
    """持仓配置"""
    # 基础配置
    strategy_id: int
    strategy_name: str

    # 仓位控制
    base_position_pct: float = 0.6  # 基础仓位 (默认 60%)
    max_position_pct: float = 0.8  # 最大仓位 (默认 80%)
    min_position_pct: float = 0.2  # 最小仓位 (默认 20%)

    # 个股限制
    max_single_stock_pct: float = 0.2  # 单只股票最大仓位 (默认 20%)
    min_single_stock_pct: float = 0.05  # 单只股票最小仓位 (默认 5%)
    max_holding_count: int = 10  # 最大持仓数量

    # 市场环境调整
    adjust_by_market: bool = True  # 是否根据市场环境调整
    bull_position_pct: float = 0.9  # 牛市仓位
    bear_position_pct: float = 0.1  # 熊市仓位

    # 止损止盈
    stop_loss_pct: float = 0.05  # 止损比例 (默认 5%)
    take_profit_pct: float = 0.15  # 止盈比例 (默认 15%)
    trailing_stop_pct: float = 0.08  # 移动止损比例 (默认 8%)

    # 持仓周期
    default_period: HoldingPeriod = HoldingPeriod.SHORT


class PositionStrategyEngine:
    """持仓策略引擎"""

    def __init__(self):
        self._position_config: Optional[PositionConfig] = None
        self._market_cache: Dict[str, MarketIndicator] = {}

    def get_position_config(self, strategy_id: int) -> Optional[PositionConfig]:
        """获取持仓配置"""
        # TODO: 从数据库读取配置
        if self._position_config:
            return self._position_config

        # 返回默认配置
        return PositionConfig(
            strategy_id=strategy_id,
            strategy_name="默认持仓策略"
        )

    def save_position_config(self, config: PositionConfig):
        """保存持仓配置"""
        self._position_config = config
        # TODO: 保存到数据库

    async def analyze_market(
        self,
        index_data: MarketIndicator,
        additional_indices: Optional[List[MarketIndicator]] = None
    ) -> MarketAnalysis:
        """
        分析市场环境

        Args:
            index_data: 主要指数数据（如上证指数）
            additional_indices: 其他指数数据（如深证成指、创业板指）

        Returns:
            市场分析结果
        """
        # 1. 计算趋势得分
        trend_score = self._calculate_trend_score(index_data)

        # 2. 计算情绪得分
        sentiment_score = self._calculate_sentiment_score(index_data)

        # 3. 判断市场环境
        market_condition, confidence = self._determine_market_condition(
            trend_score, sentiment_score, index_data
        )

        # 4. 评估风险等级
        risk_level = self._evaluate_risk_level(market_condition, trend_score)

        # 5. 详细分析
        analysis_details = {
            "index_name": index_data.index_name,
            "index_change": index_data.change_pct,
            "trend_analysis": {
                "ma5_signal": "bullish" if index_data.ma5 and index_data.current_price > index_data.ma5 else "bearish",
                "ma20_signal": "bullish" if index_data.ma20 and index_data.current_price > index_data.ma20 else "bearish",
                "golden_cross": self._check_golden_cross(index_data),
            },
            "sentiment_analysis": {
                "advance_decline_ratio": self._calc_advance_decline_ratio(index_data),
                "limit_up_ratio": index_data.limit_up_count / max(index_data.advance_count + index_data.decline_count, 1),
            }
        }

        return MarketAnalysis(
            market_condition=market_condition,
            confidence=confidence,
            trend_score=trend_score,
            sentiment_score=sentiment_score,
            risk_level=risk_level,
            analysis_details=analysis_details
        )

    def _calculate_trend_score(self, index: MarketIndicator) -> float:
        """
        计算趋势得分 (-1 到 1)

        负分表示空头趋势，正分表示多头趋势
        """
        score = 0.0
        current = index.current_price

        # 均线位置评分（每项 -0.25 到 0.25）
        if index.ma5:
            score += 0.25 * (1 if current > index.ma5 else -1)
        if index.ma10:
            score += 0.25 * (1 if current > index.ma10 else -1)
        if index.ma20:
            score += 0.25 * (1 if current > index.ma20 else -1)
        if index.ma60:
            score += 0.25 * (1 if current > index.ma60 else -1)

        # 均线排列评分
        ma_aligned = True
        if index.ma5 and index.ma10 and index.ma20:
            if index.ma5 > index.ma10 > index.ma20:
                score += 0.25  # 多头排列
            elif index.ma5 < index.ma10 < index.ma20:
                score -= 0.25  # 空头排列
            else:
                ma_aligned = False

        # 涨跌幅影响
        if index.change_pct > 2:
            score += 0.1
        elif index.change_pct < -2:
            score -= 0.1

        return max(-1.0, min(1.0, score))

    def _calculate_sentiment_score(self, index: MarketIndicator) -> float:
        """
        计算市场情绪得分 (0-1)

        0 表示极度悲观，1 表示极度乐观
        """
        score = 0.5  # 基准分

        # 涨跌家数比
        adv_dec_ratio = self._calc_advance_decline_ratio(index)
        if adv_dec_ratio > 3:
            score += 0.3
        elif adv_dec_ratio > 2:
            score += 0.2
        elif adv_dec_ratio > 1:
            score += 0.1
        elif adv_dec_ratio < 0.3:
            score -= 0.3
        elif adv_dec_ratio < 0.5:
            score -= 0.2
        elif adv_dec_ratio < 1:
            score -= 0.1

        # 涨停跌停比
        if index.limit_up_count > 0 and index.limit_down_count >= 0:
            limit_ratio = index.limit_up_count / max(index.limit_down_count, 1)
            if limit_ratio > 5:
                score += 0.2
            elif limit_ratio > 3:
                score += 0.15
            elif limit_ratio > 2:
                score += 0.1
            elif limit_ratio < 0.5:
                score -= 0.2
            elif limit_ratio < 1:
                score -= 0.1

        # 涨跌幅影响
        if index.change_pct > 1:
            score += 0.1
        elif index.change_pct > 2:
            score += 0.2
        elif index.change_pct < -1:
            score -= 0.1
        elif index.change_pct < -2:
            score -= 0.2

        return max(0.0, min(1.0, score))

    def _calc_advance_decline_ratio(self, index: MarketIndicator) -> float:
        """计算涨跌家数比"""
        if index.decline_count == 0:
            return float('inf') if index.advance_count > 0 else 1.0
        return index.advance_count / index.decline_count

    def _check_golden_cross(self, index: MarketIndicator) -> bool:
        """检查是否金叉"""
        if index.ma5 and index.ma10:
            return index.ma5 > index.ma10
        return False

    def _determine_market_condition(
        self,
        trend_score: float,
        sentiment_score: float,
        index: MarketIndicator
    ) -> Tuple[MarketCondition, float]:
        """
        判断市场环境

        Returns:
            (市场环境状态，置信度)
        """
        # 综合得分
        combined_score = trend_score * 0.6 + (sentiment_score - 0.5) * 0.4

        # 置信度计算（基于指标一致性）
        confidence = 0.5
        if trend_score > 0.5 and sentiment_score > 0.6:
            confidence += 0.3  # 趋势和情绪一致向上
        elif trend_score < -0.5 and sentiment_score < 0.4:
            confidence += 0.3  # 趋势和情绪一致向下
        else:
            confidence += 0.1  # 趋势和情绪不一致

        confidence = min(1.0, confidence)

        # 判断市场环境
        if combined_score > 0.6:
            if trend_score > 0.7 and sentiment_score > 0.7:
                return MarketCondition.STRONG_BULL, confidence
            else:
                return MarketCondition.WEAK_BULL, confidence
        elif combined_score > 0.2:
            return MarketCondition.WEAK_BULL, confidence
        elif combined_score > -0.2:
            return MarketCondition.CONSOLIDATION, confidence
        elif combined_score > -0.6:
            return MarketCondition.WEAK_BEAR, confidence
        else:
            if trend_score < -0.7 and sentiment_score < 0.3:
                return MarketCondition.STRONG_BEAR, confidence
            else:
                return MarketCondition.WEAK_BEAR, confidence

    def _evaluate_risk_level(
        self,
        market_condition: MarketCondition,
        trend_score: float
    ) -> str:
        """评估风险等级"""
        if market_condition == MarketCondition.STRONG_BEAR:
            return "very_high"
        elif market_condition == MarketCondition.WEAK_BEAR:
            return "high"
        elif market_condition == MarketCondition.CONSOLIDATION:
            return "medium"
        elif market_condition == MarketCondition.WEAK_BULL:
            return "low"
        else:
            return "low"

    def generate_position_recommendation(
        self,
        market_analysis: MarketAnalysis,
        config: Optional[PositionConfig] = None
    ) -> PositionRecommendation:
        """
        生成仓位建议

        Args:
            market_analysis: 市场分析结果
            config: 持仓配置（可选）

        Returns:
            仓位建议
        """
        config = config or self._position_config or PositionConfig(
            strategy_id=0,
            strategy_name="默认"
        )

        reasoning = []
        risk_warnings = []

        # 根据市场环境决定基础仓位
        market = market_analysis.market_condition
        if market == MarketCondition.STRONG_BULL:
            position_pct = config.bull_position_pct
            position_level = PositionLevel.FULL
            reasoning.append("强势牛市，建议满仓操作")
        elif market == MarketCondition.WEAK_BULL:
            position_pct = (config.bull_position_pct + config.base_position_pct) / 2
            position_level = PositionLevel.HEAVY
            reasoning.append("弱势牛市，建议重仓但保持谨慎")
        elif market == MarketCondition.CONSOLIDATION:
            position_pct = config.base_position_pct
            position_level = PositionLevel.HALF
            reasoning.append("震荡盘整，建议半仓观望")
        elif market == MarketCondition.WEAK_BEAR:
            position_pct = (config.base_position_pct + config.bear_position_pct) / 2
            position_level = PositionLevel.LIGHT
            reasoning.append("弱势熊市，建议轻仓防御")
            risk_warnings.append("市场趋势向下，注意控制风险")
        else:  # STRONG_BEAR
            position_pct = config.bear_position_pct
            position_level = PositionLevel.EMPTY
            reasoning.append("强势熊市，建议空仓等待")
            risk_warnings.append("市场处于下跌趋势，建议保持空仓")

        # 根据趋势得分微调仓位
        trend_adjustment = market_analysis.trend_score * 0.1
        position_pct += trend_adjustment
        position_pct = max(config.min_position_pct, min(config.max_position_pct, position_pct))

        # 根据风险等级调整个股限制
        if market_analysis.risk_level == "very_high":
            max_single_pct = config.max_single_stock_pct * 0.5
            max_count = max(1, config.max_holding_count // 3)
            risk_warnings.append("高风险环境，大幅降低个股集中度")
        elif market_analysis.risk_level == "high":
            max_single_pct = config.max_single_stock_pct * 0.7
            max_count = max(2, config.max_holding_count // 2)
            risk_warnings.append("高风险环境，降低个股集中度")
        elif market_analysis.risk_level == "medium":
            max_single_pct = config.max_single_stock_pct
            max_count = config.max_holding_count
        else:
            max_single_pct = config.max_single_stock_pct * 1.2
            max_count = config.max_holding_count + 2

        # 确定建议持仓周期
        if market in [MarketCondition.STRONG_BULL, MarketCondition.WEAK_BULL]:
            period = HoldingPeriod.MEDIUM
            reasoning.append("上涨趋势中适合中线持有")
        elif market == MarketCondition.CONSOLIDATION:
            period = HoldingPeriod.SHORT
            reasoning.append("震荡市适合短线操作")
        else:
            period = HoldingPeriod.ULTRA_SHORT
            reasoning.append("下跌趋势中仅适合超短线博弈")

        # 止损止盈配置
        stop_loss = config.stop_loss_pct
        take_profit = config.take_profit_pct
        trailing_stop = config.trailing_stop_pct

        # 高风险环境下收紧止损
        if market_analysis.risk_level in ["high", "very_high"]:
            stop_loss = min(stop_loss, 0.03)
            take_profit = max(take_profit, 0.10)
            reasoning.append("高风险环境，收紧止损至 3%")

        return PositionRecommendation(
            position_level=position_level,
            position_pct=round(position_pct, 2),
            max_single_stock_pct=round(max_single_pct, 2),
            max_holding_count=max_count,
            recommended_period=period,
            stop_loss_pct=stop_loss,
            take_profit_pct=take_profit,
            trailing_stop_pct=trailing_stop,
            reasoning=reasoning,
            risk_warnings=risk_warnings
        )

    def calculate_position_size(
        self,
        available_cash: float,
        current_price: float,
        position_pct: float,
        max_single_pct: float
    ) -> int:
        """
        计算单只股票的买入数量

        Args:
            available_cash: 可用资金
            current_price: 当前股价
            position_pct: 建议仓位比例
            max_single_pct: 单只股票最大仓位比例

        Returns:
            买入数量（100 的整数倍）
        """
        # 计算可投入的总金额
        total_amount = available_cash * position_pct

        # 单只股票限制
        max_amount = available_cash * max_single_pct
        buy_amount = min(total_amount / 10, max_amount)  # 假设分散到 10 只股票

        # 计算可买数量（100 股的整数倍）
        if current_price <= 0:
            return 0

        quantity = int((buy_amount / current_price) // 100) * 100

        # 至少 100 股
        return max(100, quantity)


# 全局单例
_position_engine: Optional[PositionStrategyEngine] = None


def get_position_engine() -> PositionStrategyEngine:
    """获取持仓策略引擎单例"""
    global _position_engine
    if _position_engine is None:
        _position_engine = PositionStrategyEngine()
    return _position_engine


def reset_position_engine():
    """重置持仓策略引擎"""
    global _position_engine
    _position_engine = None
