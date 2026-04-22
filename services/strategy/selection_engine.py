"""
选股策略引擎 (Stock Selection Engine)

基于多维度因子评分系统，筛选出爆发力强、成交活跃、市场热度高的标的股票

因子体系：
1. 技术因子 - 均线形态、MACD、RSI、KDJ 等
2. 成交量因子 - 放量、缩量、量比等
3. 动量因子 - 涨幅、换手率、资金流向等
4. 形态因子 - 突破、金叉、涨停等
"""

import asyncio
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from enum import Enum
import json

from services.common.database import get_db_manager
from services.common.indicators import TechnicalIndicators
from services.data.local_data_service import get_local_data_service


# 中国时区
CHINA_TZ = timezone(timedelta(hours=8))

def get_china_time():
    """获取中国时区时间"""
    return datetime.now(CHINA_TZ).replace(tzinfo=None)


class FactorType(Enum):
    """因子类型"""
    TECHNICAL = "technical"  # 技术因子
    VOLUME = "volume"  # 成交量因子
    MOMENTUM = "momentum"  # 动量因子
    PATTERN = "pattern"  # 形态因子


class StockProfile(Enum):
    """股票画像标签"""
    HIGH_EXPLOSIVENESS = "high_explosiveness"  # 爆发力强
    ACTIVE_TRADING = "active_trading"  # 成交活跃
    HIGH_HEAT = "high_heat"  # 市场热度高
    STRONG_UPTREND = "strong_uptrend"  # 强势上涨
    CONSOLIDATION = "consolidation"  # 盘整蓄势
    WEAK = "weak"  # 弱势


@dataclass
class FactorConfig:
    """因子配置"""
    factor_id: str
    factor_name: str
    factor_type: FactorType
    weight: float  # 权重 (0-1)
    threshold: float  # 阈值
    direction: str  # "higher_better" 或 "lower_better"
    formula: str  # 计算公式/条件表达式
    description: str = ""


@dataclass
class StockScore:
    """股票评分结果"""
    stock_code: str
    stock_name: str
    total_score: float  # 总分 (0-100)
    factor_scores: Dict[str, float]  # 各因子得分
    factor_details: Dict[str, Any]  # 各因子详细数据
    profile_tags: List[str]  # 股票画像标签
    match_reasons: List[str]  # 匹配原因
    current_price: float
    change_pct: float  # 涨跌幅
    volume_ratio: float  # 量比
    turnover_rate: float  # 换手率
    scanned_at: str = field(default_factory=lambda: get_china_time().isoformat())


class StockSelectionEngine:
    """选股策略引擎"""

    def __init__(self):
        self.db = None
        self.local_service = None
        self._factor_configs: Dict[str, FactorConfig] = {}
        self._stock_profile_weights: Dict[str, float] = {
            # 爆发力强相关因子
            "5day_gain": 0.15,
            "10day_gain": 0.10,
            "breakthrough": 0.15,
            # 成交活跃相关因子
            "volume_ratio": 0.15,
            "turnover_rate": 0.15,
            # 市场热度相关因子
            "heat_score": 0.15,
            "limit_up_count": 0.10,
            # 其他因子
            "ma_uptrend": 0.05,
        }
        self._init_default_factors()

    def _init_default_factors(self):
        """初始化默认因子配置"""
        default_factors = [
            # ========== 技术因子 ==========
            FactorConfig(
                factor_id="ma_uptrend",
                factor_name="均线多头排列",
                factor_type=FactorType.TECHNICAL,
                weight=0.10,
                threshold=0.5,
                direction="higher_better",
                formula="MA5>MA10>MA20",
                description="短期均线在长期均线之上，呈多头排列"
            ),
            FactorConfig(
                factor_id="macd_golden_cross",
                factor_name="MACD 金叉",
                factor_type=FactorType.TECHNICAL,
                weight=0.15,
                threshold=0.5,
                direction="higher_better",
                formula="MACD>0 AND MACD_HIST>0",
                description="MACD 快线上穿慢线，形成金叉"
            ),
            FactorConfig(
                factor_id="rsi_strength",
                factor_name="RSI 强势",
                factor_type=FactorType.TECHNICAL,
                weight=0.10,
                threshold=50,
                direction="higher_better",
                formula="RSI>50",
                description="RSI 在 50 以上，显示多头强势"
            ),
            FactorConfig(
                factor_id="kdj_golden_cross",
                factor_name="KDJ 金叉",
                factor_type=FactorType.TECHNICAL,
                weight=0.10,
                threshold=0.5,
                direction="higher_better",
                formula="K>D AND J>50",
                description="K 线上穿 D 线，J 值在 50 以上"
            ),

            # ========== 成交量因子 ==========
            FactorConfig(
                factor_id="volume_ratio",
                factor_name="量比",
                factor_type=FactorType.VOLUME,
                weight=0.15,
                threshold=1.5,
                direction="higher_better",
                formula="VOL/MA(VOL,5)>=1.5",
                description="当日成交量是 5 日均量的 1.5 倍以上"
            ),
            FactorConfig(
                factor_id="volume_trend",
                factor_name="成交量趋势",
                factor_type=FactorType.VOLUME,
                weight=0.10,
                threshold=0.5,
                direction="higher_better",
                formula="VOL>MA(VOL,5)",
                description="成交量站在 5 日均量之上"
            ),

            # ========== 动量因子 ==========
            FactorConfig(
                factor_id="5day_gain",
                factor_name="5 日涨幅",
                factor_type=FactorType.MOMENTUM,
                weight=0.15,
                threshold=0.05,
                direction="higher_better",
                formula="(CLOSE-REF(CLOSE,5))/REF(CLOSE,5)>=0.05",
                description="近 5 日涨幅超过 5%"
            ),
            FactorConfig(
                factor_id="breakthrough",
                factor_name="突破新高",
                factor_type=FactorType.MOMENTUM,
                weight=0.15,
                threshold=0.5,
                direction="higher_better",
                formula="CLOSE>=HHV(HIGH,20)",
                description="收盘价突破近 20 日最高点"
            ),

            # ========== 形态因子 ==========
            FactorConfig(
                factor_id="bullish_engulfing",
                factor_name="阳包阴",
                factor_type=FactorType.PATTERN,
                weight=0.10,
                threshold=0.5,
                direction="higher_better",
                formula="CLOSE>OPEN AND CLOSE>REF(CLOSE,1) AND REF(CLOSE,1)<REF(OPEN,1)",
                description="今日阳线完全包裹昨日阴线"
            ),
        ]

        for factor in default_factors:
            self._factor_configs[factor.factor_id] = factor

    async def initialize(self):
        """初始化服务"""
        self.db = get_db_manager()
        await self.db.connect()
        self.local_service = get_local_data_service()

    async def close(self):
        """关闭服务"""
        if self.db:
            await self.db.close()

    def get_factor_configs(self) -> List[Dict]:
        """获取所有因子配置"""
        return [
            {
                "factor_id": f.factor_id,
                "factor_name": f.factor_name,
                "factor_type": f.factor_type.value,
                "weight": f.weight,
                "threshold": f.threshold,
                "direction": f.direction,
                "formula": f.formula,
                "description": f.description
            }
            for f in self._factor_configs.values()
        ]

    def update_factor_weights(self, weights: Dict[str, float]):
        """
        更新因子权重

        Args:
            weights: {factor_id: new_weight}
        """
        for factor_id, new_weight in weights.items():
            if factor_id in self._factor_configs:
                self._factor_configs[factor_id].weight = new_weight

    async def scan_stocks(
        self,
        stock_codes: Optional[List[str]] = None,
        min_score: float = 60.0,
        top_n: Optional[int] = None,
        use_local: bool = True
    ) -> List[StockScore]:
        """
        扫描股票并评分

        Args:
            stock_codes: 要扫描的股票列表，None 表示扫描全部
            min_score: 最低评分阈值
            top_n: 返回前 N 只股票，None 表示返回全部
            use_local: 是否使用本地数据源

        Returns:
            股票评分列表，按总分降序排列
        """
        if use_local:
            return await self._scan_from_local(stock_codes, min_score, top_n)
        else:
            return await self._scan_from_sdk(stock_codes, min_score, top_n)

    async def _scan_from_local(
        self,
        stock_codes: Optional[List[str]],
        min_score: float,
        top_n: Optional[int]
    ) -> List[StockScore]:
        """从本地数据库扫描"""
        if stock_codes is None:
            stock_codes = self.local_service.get_all_stocks()

        results = []
        processed = 0

        for stock_code in stock_codes:
            try:
                # 获取 K 线数据（60 日）
                kline_data = self.local_service.get_kline_data(stock_code, limit=60)
                if not kline_data or len(kline_data) < 30:
                    continue

                # 计算评分
                score = await self._evaluate_stock(stock_code, kline_data)
                if score and score.total_score >= min_score:
                    results.append(score)

                processed += 1
                if processed % 500 == 0:
                    print(f"[SelectionEngine] 已扫描 {processed}/{len(stock_codes)} 只股票")

            except Exception as e:
                print(f"[SelectionEngine] 扫描 {stock_code} 失败：{e}")
                continue

        # 按总分排序
        results.sort(key=lambda x: x.total_score, reverse=True)

        # 返回前 N 只
        if top_n:
            results = results[:top_n]

        print(f"[SelectionEngine] 扫描完成：共 {len(results)} 只股票达到最低评分 {min_score}")
        return results

    async def _scan_from_sdk(
        self,
        stock_codes: Optional[List[str]],
        min_score: float,
        top_n: Optional[int]
    ) -> List[StockScore]:
        """从 SDK 实时数据扫描（备用实现）"""
        from services.trading.gateway import get_gateway

        gateway = await get_gateway()
        if not gateway or not gateway.connected:
            raise Exception("交易网关未连接")

        # 获取股票列表
        if stock_codes is None:
            stock_list = await gateway.get_stock_list()
            stock_codes = [f"{s['code']}.{s['market']}" for s in stock_list]

        results = []

        for stock_code in stock_codes:
            try:
                # 获取实时 K 线数据
                kline_data = await gateway.get_kline_data(stock_code, period="day", limit=60)
                if not kline_data or len(kline_data) < 30:
                    continue

                score = await self._evaluate_stock(stock_code, kline_data)
                if score and score.total_score >= min_score:
                    results.append(score)

            except Exception as e:
                continue

        results.sort(key=lambda x: x.total_score, reverse=True)
        if top_n:
            results = results[:top_n]

        return results

    async def _evaluate_stock(
        self,
        stock_code: str,
        kline_data: List[Dict]
    ) -> Optional[StockScore]:
        """
        评估单只股票

        Args:
            stock_code: 股票代码
            kline_data: K 线数据列表

        Returns:
            股票评分结果
        """
        # 提取价格、成交量等数据
        closes = [k['close'] for k in kline_data]
        highs = [k.get('high', k['close']) for k in kline_data]
        lows = [k.get('low', k['close']) for k in kline_data]
        opens = [k.get('open', k['close']) for k in kline_data]
        volumes = [k.get('volume', 0) for k in kline_data]

        if len(closes) < 30:
            return None

        current_price = closes[-1]

        # 计算基础指标
        ma5 = TechnicalIndicators.ma(closes, 5)
        ma10 = TechnicalIndicators.ma(closes, 10)
        ma20 = TechnicalIndicators.ma(closes, 20)
        rsi = TechnicalIndicators.rsi(closes, 14)
        macd_data = TechnicalIndicators.macd(closes)
        kdj_data = TechnicalIndicators.kdj(highs, lows, closes)

        # 计算成交量均线
        ma_vol5 = TechnicalIndicators.ma(volumes, 5)
        ma_vol10 = TechnicalIndicators.ma(volumes, 10)
        current_volume = volumes[-1]

        # 计算涨跌幅
        change_pct = (closes[-1] - closes[-2]) / closes[-2] * 100 if len(closes) >= 2 else 0
        gain_5day = (closes[-1] - closes[-6]) / closes[-6] * 100 if len(closes) >= 6 else 0

        # 计算量比
        volume_ratio = current_volume / ma_vol5 if ma_vol5 and ma_vol5 > 0 else 1.0

        # 计算换手率（简化估算）
        turnover_rate = volume_ratio * 0.05  # 简化估算

        # 构建指标字典
        indicators = {
            "ma5": ma5, "ma10": ma10, "ma20": ma20,
            "rsi": rsi,
            "macd": macd_data.get("macd", 0) if macd_data else 0,
            "macd_hist": macd_data.get("histogram", 0) if macd_data else 0,
            "kdj_k": kdj_data.get("k", 50) if kdj_data else 50,
            "kdj_d": kdj_data.get("d", 50) if kdj_data else 50,
            "kdj_j": kdj_data.get("j", 50) if kdj_data else 50,
            "price": current_price,
            "volume": current_volume,
            "ma_vol5": ma_vol5,
            "ma_vol10": ma_vol10,
            "close": closes[-1],
            "open": opens[-1],
            "prev_close": closes[-2] if len(closes) >= 2 else closes[-1],
            "prev_open": opens[-2] if len(opens) >= 2 else opens[-1],
            "gain_5day": gain_5day,
            "volume_ratio": volume_ratio,
        }

        # 评估各因子
        factor_scores = {}
        factor_details = {}
        total_score = 0.0
        profile_tags = []
        match_reasons = []

        for factor_id, config in self._factor_configs.items():
            score, details = self._evaluate_factor(factor_id, indicators, closes, volumes)
            factor_scores[factor_id] = score
            factor_details[factor_id] = details

            # 计算加权得分
            weighted_score = score * config.weight * 100
            total_score += weighted_score

            # 记录匹配原因
            if score >= config.threshold:
                match_reasons.append(f"{config.factor_name}: {details.get('reason', '条件满足')}")

        # 确定股票画像标签
        profile_tags = self._determine_profile(indicators, factor_scores)

        # 获取股票名称（从股票代码提取）
        stock_name = stock_code

        return StockScore(
            stock_code=stock_code,
            stock_name=stock_name,
            total_score=round(total_score, 2),
            factor_scores=factor_scores,
            factor_details=factor_details,
            profile_tags=profile_tags,
            match_reasons=match_reasons,
            current_price=current_price,
            change_pct=round(change_pct, 2),
            volume_ratio=round(volume_ratio, 2),
            turnover_rate=round(turnover_rate, 2)
        )

    def _evaluate_factor(
        self,
        factor_id: str,
        indicators: Dict,
        closes: List[float],
        volumes: List[float]
    ) -> Tuple[float, Dict]:
        """
        评估单个因子

        Returns:
            (得分 0-1, 详细信息)
        """
        config = self._factor_configs.get(factor_id)
        if not config:
            return 0.5, {"reason": "未知因子"}

        score = 0.0
        details = {}

        try:
            if factor_id == "ma_uptrend":
                # 均线多头排列：MA5>MA10>MA20
                ma5, ma10, ma20 = indicators.get('ma5'), indicators.get('ma10'), indicators.get('ma20')
                if ma5 and ma10 and ma20:
                    if ma5 > ma10 > ma20:
                        score = 1.0
                        details["reason"] = f"MA5({ma5:.2f})>MA10({ma10:.2f})>MA20({ma20:.2f})"
                    elif ma5 > ma10:
                        score = 0.6
                        details["reason"] = f"MA5>MA10"
                    else:
                        score = 0.2
                details.update({"ma5": ma5, "ma10": ma10, "ma20": ma20})

            elif factor_id == "macd_golden_cross":
                # MACD 金叉
                macd = indicators.get('macd', 0)
                macd_hist = indicators.get('macd_hist', 0)
                if macd > 0 and macd_hist > 0:
                    score = 1.0
                    details["reason"] = "MACD 金叉"
                elif macd > 0:
                    score = 0.5
                    details["reason"] = "MACD>0"
                details.update({"macd": macd, "macd_hist": macd_hist})

            elif factor_id == "rsi_strength":
                # RSI 强势
                rsi = indicators.get('rsi', 50)
                if rsi and rsi >= 50:
                    score = min(1.0, (rsi - 50) / 30 + 0.5)  # 50-80 映射到 0.5-1.0
                    details["reason"] = f"RSI={rsi:.1f} 强势区"
                elif rsi:
                    score = rsi / 50 * 0.4  # 0-50 映射到 0-0.4
                    details["reason"] = f"RSI={rsi:.1f}"
                details.update({"rsi": rsi})

            elif factor_id == "kdj_golden_cross":
                # KDJ 金叉
                k = indicators.get('kdj_k', 50)
                d = indicators.get('kdj_d', 50)
                j = indicators.get('kdj_j', 50)
                if k > d and j > 50:
                    score = 1.0
                    details["reason"] = f"KDJ 金叉 (K={k:.1f},D={d:.1f},J={j:.1f})"
                elif k > d:
                    score = 0.6
                    details["reason"] = "K>D"
                details.update({"k": k, "d": d, "j": j})

            elif factor_id == "volume_ratio":
                # 量比
                vr = indicators.get('volume_ratio', 1.0)
                if vr >= 2.0:
                    score = 1.0
                    details["reason"] = f"量比={vr:.1f} 显著放量"
                elif vr >= 1.5:
                    score = 0.8
                    details["reason"] = f"量比={vr:.1f} 放量"
                elif vr >= 1.0:
                    score = 0.5
                    details["reason"] = f"量比={vr:.1f} 正常"
                else:
                    score = 0.3
                    details["reason"] = f"量比={vr:.1f} 缩量"
                details.update({"volume_ratio": vr})

            elif factor_id == "volume_trend":
                # 成交量趋势
                vol = indicators.get('volume', 0)
                ma_vol = indicators.get('ma_vol5', 0)
                if ma_vol and vol > ma_vol:
                    score = min(1.0, vol / ma_vol * 0.5)
                    details["reason"] = f"VOL>MA_VOL5"
                else:
                    score = 0.3
                    details["reason"] = f"VOL<=MA_VOL5"

            elif factor_id == "5day_gain":
                # 5 日涨幅
                gain = indicators.get('gain_5day', 0)
                if gain >= 10:
                    score = 1.0
                    details["reason"] = f"5 日涨幅={gain:.1f}% 强势"
                elif gain >= 5:
                    score = 0.8
                    details["reason"] = f"5 日涨幅={gain:.1f}%"
                elif gain >= 0:
                    score = 0.5 + gain / 10 * 0.3
                    details["reason"] = f"5 日涨幅={gain:.1f}%"
                else:
                    score = max(0, 0.5 + gain / 10)
                details.update({"5day_gain": gain})

            elif factor_id == "breakthrough":
                # 突破新高
                current = closes[-1]
                high_20 = max(closes[-20:]) if len(closes) >= 20 else closes[-1]
                if current >= high_20 * 0.98:  # 接近或突破 20 日新高
                    score = min(1.0, current / high_20)
                    details["reason"] = f"突破 20 日新高 {high_20:.2f}"
                else:
                    score = 0.3
                    details["reason"] = f"距 20 日新高差{(1-current/high_20)*100:.1f}%"

            elif factor_id == "bullish_engulfing":
                # 阳包阴
                curr_open = indicators.get('open', closes[-1])
                curr_close = indicators.get('close', closes[-1])
                prev_open = indicators.get('prev_open', closes[-2])
                prev_close = indicators.get('prev_close', closes[-2])

                is_bullish = (curr_close > curr_open and
                             curr_close > prev_close and
                             prev_close < prev_open)
                if is_bullish:
                    score = 1.0
                    details["reason"] = "阳包阴形态"
                else:
                    score = 0.3
                    details["reason"] = "非阳包阴"

        except Exception as e:
            details["error"] = str(e)
            score = 0.3

        return score, details

    def _determine_profile(
        self,
        indicators: Dict,
        factor_scores: Dict
    ) -> List[str]:
        """根据因子得分确定股票画像"""
        profile_tags = []

        # 爆发力强：5 日涨幅高或突破新高
        if (factor_scores.get('5day_gain', 0) >= 0.8 or
            factor_scores.get('breakthrough', 0) >= 0.8):
            profile_tags.append(StockProfile.HIGH_EXPLOSIVENESS.value)

        # 成交活跃：量比高
        if factor_scores.get('volume_ratio', 0) >= 0.8:
            profile_tags.append(StockProfile.ACTIVE_TRADING.value)

        # 市场热度高：RSI 强势且有成交量配合
        if (factor_scores.get('rsi_strength', 0) >= 0.7 and
            factor_scores.get('volume_trend', 0) >= 0.7):
            profile_tags.append(StockProfile.HIGH_HEAT.value)

        # 强势上涨：均线多头排列
        if factor_scores.get('ma_uptrend', 0) >= 0.8:
            profile_tags.append(StockProfile.STRONG_UPTREND.value)

        # 如果没有明显标签，标记为盘整或弱势
        if not profile_tags:
            avg_score = sum(factor_scores.values()) / max(len(factor_scores), 1)
            if avg_score >= 0.5:
                profile_tags.append(StockProfile.CONSOLIDATION.value)
            else:
                profile_tags.append(StockProfile.WEAK.value)

        return profile_tags


# 全局单例
_selection_engine: Optional[StockSelectionEngine] = None


def get_selection_engine() -> StockSelectionEngine:
    """获取选股引擎单例"""
    global _selection_engine
    if _selection_engine is None:
        _selection_engine = StockSelectionEngine()
    return _selection_engine


def reset_selection_engine():
    """重置选股引擎"""
    global _selection_engine
    _selection_engine = None
