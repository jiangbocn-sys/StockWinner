"""
交易策略引擎 (Trading Strategy Engine)

根据个股走势形态，决定是否及时止损或止盈，优化交易策略以最大化利润，
同时在下跌市场趋势中尽量减小损失

核心功能：
1. 止损策略 - 固定比例止损、移动止损、技术位止损
2. 止盈策略 - 固定比例止盈、分批止盈、趋势跟踪止盈
3. 买入时机 - 形态识别、指标金叉、突破买入
4. 卖出时机 - 形态破位、指标死叉、基本面恶化
"""

from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from enum import Enum
from services.common.indicators import TechnicalIndicators


# 中国时区
CHINA_TZ = timezone(timedelta(hours=8))

def get_china_time():
    """获取中国时区时间"""
    return datetime.now(CHINA_TZ).replace(tzinfo=None)


class SignalType(Enum):
    """信号类型"""
    BUY = "buy"  # 买入
    SELL = "sell"  # 卖出
    HOLD = "hold"  # 持有
    STOP_LOSS = "stop_loss"  # 止损
    TAKE_PROFIT = "take_profit"  # 止盈
    TRAILING_STOP = "trailing_stop"  # 移动止损
    ADD_POSITION = "add_position"  # 加仓
    REDUCE_POSITION = "reduce_position"  # 减仓


class SignalStrength(Enum):
    """信号强度"""
    VERY_STRONG = "very_strong"  # 非常强 (90-100%)
    STRONG = "strong"  # 强 (70-90%)
    MODERATE = "moderate"  # 中等 (50-70%)
    WEAK = "weak"  # 弱 (30-50%)
    VERY_WEAK = "very_weak"  # 非常弱 (0-30%)


class PatternType(Enum):
    """K 线形态类型"""
    # 看涨形态
    BULLISH_ENGULFING = "bullish_engulfing"  # 阳包阴
    HAMMER = "hammer"  # 锤头线
    MORNING_STAR = "morning_star"  # 早晨之星
    THREE_WHITE_SOLDIERS = "three_white_soldiers"  # 三阳开泰
    GOLDEN_CROSS = "golden_cross"  # 金叉
    BREAKOUT = "breakout"  # 突破

    # 看跌形态
    BEARISH_ENGULFING = "bearish_engulfing"  # 阴包阳
    SHOOTING_STAR = "shooting_star"  # 射击之星
    EVENING_STAR = "evening_star"  # 黄昏之星
    THREE_BLACK_CROWS = "three_black_crows"  # 三只乌鸦
    DEAD_CROSS = "dead_cross"  # 死叉
    BREAKDOWN = "breakdown"  # 破位


@dataclass
class PositionData:
    """持仓数据"""
    stock_code: str
    stock_name: str
    quantity: int
    avg_cost: float  # 平均成本
    current_price: float
    market_value: float
    profit_loss: float  # 盈亏金额
    profit_loss_pct: float  # 盈亏比例
    highest_price_since_buy: float  # 买入后最高价
    lowest_price_since_buy: float  # 买入后最低价
    buy_date: str
    holding_days: int


@dataclass
class TradingSignal:
    """交易信号"""
    signal_type: SignalType
    strength: SignalStrength
    stock_code: str
    stock_name: str

    # 价格信息
    current_price: float
    suggested_price: float  # 建议价格

    # 数量信息
    suggested_quantity: int  # 建议数量
    position_pct: float  # 占仓位比例

    # 信号依据
    pattern: Optional[PatternType]  # 识别到的形态
    indicators: Dict[str, Any]  # 指标数据
    reasons: List[str]  # 信号原因

    # 风控参数
    stop_loss_price: Optional[float] = None
    take_profit_price: Optional[float] = None

    # 其他
    confidence: float = 0.5  # 置信度
    generated_at: str = field(default_factory=lambda: get_china_time().isoformat())


@dataclass
class StopLossConfig:
    """止损配置"""
    # 固定比例止损
    fixed_stop_loss_pct: float = 0.05  # 固定止损比例 (5%)

    # 移动止损
    trailing_stop_pct: float = 0.08  # 从最高点回撤 8% 止损
    trailing_stop_enabled: bool = True

    # 技术位止损
    technical_stop_loss: bool = True  # 技术位止损
    ma20_stop_loss: bool = True  # 跌破 20 日线止损
    support_level_stop: bool = True  # 跌破支撑位止损

    # 时间止损
    time_stop_days: int = 10  # 持有超过 N 天且未盈利则止损

    # 最大亏损限制
    max_loss_per_stock_pct: float = 0.10  # 单只股票最大亏损 10%
    max_total_loss_pct: float = 0.20  # 总账户最大亏损 20%


@dataclass
class TakeProfitConfig:
    """止盈配置"""
    # 固定比例止盈
    fixed_take_profit_pct: float = 0.15  # 固定止盈比例 (15%)

    # 分批止盈
    staged_take_profit: bool = True
    staged_levels: List[Tuple[float, float]] = field(default_factory=lambda: [
        (0.10, 0.3),  # 涨 10% 卖出 30%
        (0.20, 0.3),  # 涨 20% 卖出 30%
        (0.30, 0.4),  # 涨 30% 卖出剩余 40%
    ])

    # 趋势跟踪止盈
    trend_tracking: bool = True
    trend_ma_period: int = 10  # 用 10 日线作为趋势跟踪基准

    # 技术指标止盈
    rsi_overbought: float = 80  # RSI 超买线
    macd_dead_cross: bool = True  # MACD 死叉止盈


@dataclass
class TradingConfig:
    """交易配置"""
    strategy_id: int
    strategy_name: str

    # 买入配置
    buy_on_breakout: bool = True  # 突破买入
    buy_on_golden_cross: bool = True  # 金叉买入
    buy_on_pattern: bool = True  # 形态买入

    # 仓位限制
    max_position_per_stock: float = 0.2  # 单只股票最大仓位
    max_total_positions: int = 10  # 最大持仓数量

    # 止损止盈配置
    stop_loss: StopLossConfig = field(default_factory=StopLossConfig)
    take_profit: TakeProfitConfig = field(default_factory=TakeProfitConfig)

    # 其他
    enable_trailing_stop: bool = True
    enable_time_stop: bool = True


class TradingStrategyEngine:
    """交易策略引擎"""

    def __init__(self):
        self._config: Optional[TradingConfig] = None
        self._price_cache: Dict[str, List[float]] = {}  # 价格缓存

    def get_config(self, strategy_id: int) -> Optional[TradingConfig]:
        """获取交易配置"""
        if self._config:
            return self._config

        return TradingConfig(
            strategy_id=strategy_id,
            strategy_name="默认交易策略"
        )

    def save_config(self, config: TradingConfig):
        """保存交易配置"""
        self._config = config

    async def evaluate_position(
        self,
        position: PositionData,
        kline_data: List[Dict],
        config: Optional[TradingConfig] = None
    ) -> Optional[TradingSignal]:
        """
        评估持仓，生成交易信号

        Args:
            position: 持仓数据
            kline_data: K 线数据（至少 60 日）
            config: 交易配置

        Returns:
            交易信号（如有）
        """
        config = config or self._config

        if not kline_data or len(kline_data) < 30:
            return None

        # 提取价格数据
        closes = [k['close'] for k in kline_data]
        highs = [k.get('high', k['close']) for k in kline_data]
        lows = [k.get('low', k['close']) for k in kline_data]
        opens = [k.get('open', k['close']) for k in kline_data]
        volumes = [k.get('volume', 0) for k in kline_data]

        current_price = closes[-1]
        profit_pct = position.profit_loss_pct

        # 计算技术指标
        ma5 = TechnicalIndicators.ma(closes, 5)
        ma10 = TechnicalIndicators.ma(closes, 10)
        ma20 = TechnicalIndicators.ma(closes, 20)
        rsi = TechnicalIndicators.rsi(closes, 14)
        macd_data = TechnicalIndicators.macd(closes)

        indicators = {
            "ma5": ma5, "ma10": ma10, "ma20": ma20,
            "rsi": rsi,
            "macd": macd_data.get("macd", 0) if macd_data else 0,
            "macd_hist": macd_data.get("histogram", 0) if macd_data else 0,
            "current_price": current_price,
        }

        # 1. 检查止损条件
        stop_loss_signal = self._check_stop_loss(
            position, indicators, closes, highs, lows, config.stop_loss if config else StopLossConfig()
        )
        if stop_loss_signal:
            return stop_loss_signal

        # 2. 检查止盈条件
        take_profit_signal = self._check_take_profit(
            position, indicators, closes, volumes, config.take_profit if config else TakeProfitConfig()
        )
        if take_profit_signal:
            return take_profit_signal

        # 3. 检查减仓条件
        reduce_signal = self._check_reduce_position(position, indicators, closes)
        if reduce_signal:
            return reduce_signal

        return None  # 无信号，继续持有

    def _check_stop_loss(
        self,
        position: PositionData,
        indicators: Dict,
        closes: List[float],
        highs: List[float],
        lows: List[float],
        config: StopLossConfig
    ) -> Optional[TradingSignal]:
        """检查止损条件"""
        reasons = []
        trigger_condition = False

        current_price = indicators["current_price"]
        profit_pct = position.profit_loss_pct

        # 1. 固定比例止损
        if profit_pct <= -config.fixed_stop_loss_pct:
            trigger_condition = True
            reasons.append(f"固定止损：亏损{profit_pct*100:.1f}% >= {config.fixed_stop_loss_pct*100:.0f}%")

        # 2. 移动止损（从最高点回撤）
        if config.trailing_stop_enabled and position.highest_price_since_buy > 0:
            highest = position.highest_price_since_buy
            drawdown = (highest - current_price) / highest
            if drawdown >= config.trailing_stop_pct:
                trigger_condition = True
                reasons.append(f"移动止损：从高点回撤{drawdown*100:.1f}% >= {config.trailing_stop_pct*100:.0f}%")

        # 3. 技术位止损 - 跌破 20 日线
        if config.ma20_stop_loss and indicators.get("ma20"):
            if current_price < indicators["ma20"]:
                # 且 20 日线趋势向下
                ma20_prev = TechnicalIndicators.ma(closes[:-1], 20) if len(closes) > 20 else indicators["ma20"]
                if ma20_prev and indicators["ma20"] < ma20_prev:
                    trigger_condition = True
                    reasons.append(f"技术止损：跌破 20 日线 ({indicators['ma20']:.2f}) 且趋势向下")

        # 4. 时间止损
        if config.time_stop_days > 0 and position.holding_days > config.time_stop_days:
            if profit_pct <= 0:
                trigger_condition = True
                reasons.append(f"时间止损：持有{position.holding_days}天未盈利")

        # 5. 最大亏损限制
        if profit_pct <= -config.max_loss_per_stock_pct:
            trigger_condition = True
            reasons.append(f"最大亏损：亏损{profit_pct*100:.1f}% >= {config.max_loss_per_stock_pct*100:.0f}%")

        if trigger_condition:
            stop_loss_price = current_price * 0.98  # 建议卖出价

            # 确定信号强度
            if profit_pct <= -config.max_loss_per_stock_pct:
                strength = SignalStrength.VERY_STRONG
            elif profit_pct <= -config.fixed_stop_loss_pct:
                strength = SignalStrength.STRONG
            else:
                strength = SignalStrength.MODERATE

            return TradingSignal(
                signal_type=SignalType.STOP_LOSS,
                strength=strength,
                stock_code=position.stock_code,
                stock_name=position.stock_name,
                current_price=current_price,
                suggested_price=stop_loss_price,
                suggested_quantity=position.quantity,  # 全部卖出
                position_pct=1.0,
                pattern=None,
                indicators=indicators,
                reasons=reasons,
                stop_loss_price=stop_loss_price,
                confidence=0.9 if len(reasons) > 1 else 0.7
            )

        return None

    def _check_take_profit(
        self,
        position: PositionData,
        indicators: Dict,
        closes: List[float],
        volumes: List[float],
        config: TakeProfitConfig
    ) -> Optional[TradingSignal]:
        """检查止盈条件"""
        reasons = []
        trigger_condition = False
        sell_ratio = 1.0  # 卖出比例

        current_price = indicators["current_price"]
        profit_pct = position.profit_loss_pct

        # 1. 固定比例止盈
        if profit_pct >= config.fixed_take_profit_pct:
            trigger_condition = True
            reasons.append(f"固定止盈：盈利{profit_pct*100:.1f}% >= {config.fixed_take_profit_pct*100:.0f}%")

        # 2. 分批止盈
        if config.staged_take_profit:
            for level_pct, level_ratio in config.staged_levels:
                if profit_pct >= level_pct:
                    sell_ratio = level_ratio
                    trigger_condition = True
                    reasons.append(f"分批止盈：盈利{profit_pct*100:.1f}% 达到第{level_pct*100:.0f}%档位")
                    break

        # 3. RSI 超买止盈
        if indicators.get("rsi") and indicators["rsi"] >= config.rsi_overbought:
            trigger_condition = True
            reasons.append(f"RSI 超买：RSI={indicators['rsi']:.1f} >= {config.rsi_overbought}")

        # 4. MACD 死叉止盈
        if config.macd_dead_cross and indicators.get("macd_hist"):
            if indicators["macd_hist"] < 0:
                # 检查是否刚死叉
                macd_prev = TechnicalIndicators.macd(closes[:-1])
                if macd_prev and macd_prev.get("histogram", 0) >= 0:
                    trigger_condition = True
                    reasons.append("MACD 死叉：红柱转绿")

        # 5. 趋势跟踪止盈
        if config.trend_tracking and indicators.get("ma10"):
            if current_price < indicators["ma10"]:
                # 跌破趋势线
                ma10_prev = TechnicalIndicators.ma(closes[:-1], 10) if len(closes) > 10 else indicators["ma10"]
                if ma10_prev and indicators["ma10"] < ma10_prev:
                    trigger_condition = True
                    reasons.append(f"趋势转弱：跌破 10 日线 ({indicators['ma10']:.2f})")

        if trigger_condition and profit_pct > 0:
            sell_quantity = int(position.quantity * sell_ratio)
            if sell_quantity < 100:
                sell_quantity = 100  # 至少 100 股

            # 确定信号强度
            if profit_pct >= 0.30 or indicators.get("rsi", 0) >= 85:
                strength = SignalStrength.VERY_STRONG
            elif profit_pct >= 0.20 or indicators.get("rsi", 0) >= 80:
                strength = SignalStrength.STRONG
            else:
                strength = SignalStrength.MODERATE

            return TradingSignal(
                signal_type=SignalType.TAKE_PROFIT,
                strength=strength,
                stock_code=position.stock_code,
                stock_name=position.stock_name,
                current_price=current_price,
                suggested_price=current_price * 0.99,  # 略低于市价
                suggested_quantity=sell_quantity,
                position_pct=sell_ratio,
                pattern=None,
                indicators=indicators,
                reasons=reasons,
                take_profit_price=current_price,
                confidence=0.85 if len(reasons) > 1 else 0.65
            )

        return None

    def _check_reduce_position(
        self,
        position: PositionData,
        indicators: Dict,
        closes: List[float]
    ) -> Optional[TradingSignal]:
        """检查减仓条件（非止损止盈）"""
        reasons = []

        current_price = indicators["current_price"]

        # 高位放量滞涨
        ma5 = indicators.get("ma5")
        ma10 = indicators.get("ma10")

        if ma5 and ma10:
            # 5 日线下穿 10 日线
            if ma5 < ma10:
                ma5_prev = TechnicalIndicators.ma(closes[:-1], 5) if len(closes) > 5 else ma5
                ma10_prev = TechnicalIndicators.ma(closes[:-1], 10) if len(closes) > 10 else ma10
                if ma5_prev > ma10_prev:  # 刚死叉
                    reasons.append("5 日线下穿 10 日线，形成死叉")

        # RSI 高位拐头
        rsi = indicators.get("rsi")
        if rsi and rsi > 70:
            reasons.append(f"RSI 高位 ({rsi:.1f})，注意回调风险")

        if len(reasons) >= 2:
            return TradingSignal(
                signal_type=SignalType.REDUCE_POSITION,
                strength=SignalStrength.WEAK,
                stock_code=position.stock_code,
                stock_name=position.stock_name,
                current_price=current_price,
                suggested_price=current_price * 0.99,
                suggested_quantity=int(position.quantity * 0.3),  # 减仓 30%
                position_pct=0.3,
                pattern=None,
                indicators=indicators,
                reasons=reasons,
                confidence=0.5
            )

        return None

    async def scan_buy_opportunities(
        self,
        stock_data: Dict[str, Dict],
        config: Optional[TradingConfig] = None
    ) -> List[TradingSignal]:
        """
        扫描买入机会

        Args:
            stock_data: {stock_code: {kline_data: [...], current_price: x}}
            config: 交易配置

        Returns:
            买入信号列表
        """
        config = config or self._config
        signals = []

        for stock_code, data in stock_data.items():
            kline_data = data.get("kline_data", [])
            if not kline_data or len(kline_data) < 30:
                continue

            signal = self._evaluate_buy_signal(stock_code, kline_data, config)
            if signal:
                signals.append(signal)

        return signals

    def _evaluate_buy_signal(
        self,
        stock_code: str,
        kline_data: List[Dict],
        config: TradingConfig
    ) -> Optional[TradingSignal]:
        """评估买入信号"""
        reasons = []
        patterns_detected = []
        confidence = 0.5

        # 提取数据
        closes = [k['close'] for k in kline_data]
        highs = [k.get('high', k['close']) for k in kline_data]
        lows = [k.get('low', k['close']) for k in kline_data]
        opens = [k.get('open', k['close']) for k in kline_data]
        volumes = [k.get('volume', 0) for k in kline_data]

        current_price = closes[-1]

        # 计算指标
        ma5 = TechnicalIndicators.ma(closes, 5)
        ma10 = TechnicalIndicators.ma(closes, 10)
        ma20 = TechnicalIndicators.ma(closes, 20)
        rsi = TechnicalIndicators.rsi(closes, 14)
        macd_data = TechnicalIndicators.macd(closes)
        kdj_data = TechnicalIndicators.kdj(highs, lows, closes)

        indicators = {
            "ma5": ma5, "ma10": ma10, "ma20": ma20,
            "rsi": rsi,
            "macd": macd_data.get("macd", 0) if macd_data else 0,
            "macd_hist": macd_data.get("histogram", 0) if macd_data else 0,
            "kdj_k": kdj_data.get("k", 50) if kdj_data else 50,
            "kdj_d": kdj_data.get("d", 50) if kdj_data else 50,
            "current_price": current_price,
        }

        # 1. 金叉买入
        if config.buy_on_golden_cross:
            if ma5 and ma10 and ma5 > ma10:
                ma5_prev = TechnicalIndicators.ma(closes[:-1], 5) if len(closes) > 5 else ma5
                ma10_prev = TechnicalIndicators.ma(closes[:-1], 10) if len(closes) > 10 else ma10
                if ma5_prev <= ma10_prev:  # 刚金叉
                    reasons.append("MA5 上穿 MA10，形成金叉")
                    patterns_detected.append(PatternType.GOLDEN_CROSS)
                    confidence += 0.15

            if macd_data and macd_data.get("histogram", 0) > 0:
                macd_prev = TechnicalIndicators.macd(closes[:-1])
                if macd_prev and macd_prev.get("histogram", 0) <= 0:
                    reasons.append("MACD 金叉")
                    patterns_detected.append(PatternType.GOLDEN_CROSS)
                    confidence += 0.15

        # 2. 突破买入
        if config.buy_on_breakout:
            high_20 = max(closes[-20:]) if len(closes) >= 20 else current_price
            if current_price >= high_20 * 0.98:
                reasons.append(f"突破 20 日新高 ({high_20:.2f})")
                patterns_detected.append(PatternType.BREAKOUT)
                confidence += 0.2

        # 3. 形态买入
        if config.buy_on_pattern:
            # 阳包阴
            if (closes[-1] > opens[-1] and closes[-1] > closes[-2] and
                closes[-2] < opens[-2]):
                reasons.append("阳包阴形态")
                patterns_detected.append(PatternType.BULLISH_ENGULFING)
                confidence += 0.1

            # KDJ 金叉
            if kdj_data:
                if kdj_data.get("k", 0) > kdj_data.get("d", 0) and kdj_data.get("j", 0) > 50:
                    reasons.append(f"KDJ 金叉 (K={kdj_data['k']:.1f}, D={kdj_data['d']:.1f})")
                    confidence += 0.1

        # 4. RSI 条件
        if rsi and 40 < rsi < 70:
            reasons.append(f"RSI={rsi:.1f} 健康区间")
            confidence += 0.05

        # 5. 成交量条件
        ma_vol5 = TechnicalIndicators.ma(volumes, 5)
        if ma_vol5 and volumes[-1] > ma_vol5 * 1.5:
            reasons.append("放量上涨")
            confidence += 0.1

        # 判断是否生成信号
        if len(reasons) >= 2 and confidence >= 0.6:
            # 确定信号强度
            if confidence >= 0.85:
                strength = SignalStrength.VERY_STRONG
            elif confidence >= 0.7:
                strength = SignalStrength.STRONG
            else:
                strength = SignalStrength.MODERATE

            # 计算止损止盈价
            stop_loss = current_price * 0.95
            take_profit = current_price * 1.15

            return TradingSignal(
                signal_type=SignalType.BUY,
                strength=strength,
                stock_code=stock_code,
                stock_name=stock_code,
                current_price=current_price,
                suggested_price=current_price * 1.01,  # 略高于市价确保成交
                suggested_quantity=100,  # 基础数量，实际由仓位引擎计算
                position_pct=0.1,
                pattern=patterns_detected[0] if patterns_detected else None,
                indicators=indicators,
                reasons=reasons,
                stop_loss_price=stop_loss,
                take_profit_price=take_profit,
                confidence=confidence
            )

        return None

    def detect_patterns(
        self,
        kline_data: List[Dict]
    ) -> List[PatternType]:
        """
        检测 K 线形态

        Args:
            kline_data: K 线数据

        Returns:
            检测到的形态列表
        """
        patterns = []

        if not kline_data or len(kline_data) < 10:
            return patterns

        closes = [k['close'] for k in kline_data]
        opens = [k.get('open', k['close']) for k in kline_data]
        highs = [k.get('high', k['close']) for k in kline_data]
        lows = [k.get('low', k['close']) for k in kline_data]

        # 1. 阳包阴
        if (closes[-1] > opens[-1] and closes[-1] > closes[-2] and
            closes[-2] < opens[-2]):
            patterns.append(PatternType.BULLISH_ENGULFING)

        # 2. 阴包阳
        if (closes[-1] < opens[-1] and closes[-1] < closes[-2] and
            closes[-2] > opens[-2]):
            patterns.append(PatternType.BEARISH_ENGULFING)

        # 3. 锤头线
        body = abs(closes[-1] - opens[-1])
        lower_shadow = min(closes[-1], opens[-1]) - lows[-1]
        upper_shadow = highs[-1] - max(closes[-1], opens[-1])
        if lower_shadow > body * 2 and upper_shadow < body * 0.5:
            patterns.append(PatternType.HAMMER)

        # 4. 射击之星
        if upper_shadow > body * 2 and lower_shadow < body * 0.5:
            patterns.append(PatternType.SHOOTING_STAR)

        # 5. 突破
        high_20 = max(closes[-20:]) if len(closes) >= 20 else closes[-1]
        if closes[-1] >= high_20:
            patterns.append(PatternType.BREAKOUT)

        # 6. 金叉/死叉
        ma5 = TechnicalIndicators.ma(closes, 5)
        ma10 = TechnicalIndicators.ma(closes, 10)
        if ma5 and ma10:
            if ma5 > ma10:
                ma5_prev = TechnicalIndicators.ma(closes[:-1], 5)
                if ma5_prev and ma5_prev <= ma10:
                    patterns.append(PatternType.GOLDEN_CROSS)
            else:
                ma5_prev = TechnicalIndicators.ma(closes[:-1], 5)
                if ma5_prev and ma5_prev >= ma10:
                    patterns.append(PatternType.DEAD_CROSS)

        return patterns


# 全局单例
_trading_engine: Optional[TradingStrategyEngine] = None


def get_trading_engine() -> TradingStrategyEngine:
    """获取交易策略引擎单例"""
    global _trading_engine
    if _trading_engine is None:
        _trading_engine = TradingStrategyEngine()
    return _trading_engine


def reset_trading_engine():
    """重置交易策略引擎"""
    global _trading_engine
    _trading_engine = None
