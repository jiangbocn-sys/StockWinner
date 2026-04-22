"""
策略管理 API v2 - 三维度策略系统

提供选股策略、持仓策略、交易策略的统一管理接口
"""

from fastapi import APIRouter, HTTPException, Path, Body, Query
from typing import List, Optional, Dict, Any
from datetime import datetime, timezone, timedelta
from services.common.database import get_db_manager
from services.strategy.selection_engine import get_selection_engine, StockScore
from services.strategy.position_engine import get_position_engine, PositionRecommendation, MarketIndicator
from services.strategy.trading_engine import get_trading_engine, PositionData, TradingSignal
import json

# 中国时区
CHINA_TZ = timezone(timedelta(hours=8))

def get_china_time():
    """获取中国时区时间"""
    return datetime.now(CHINA_TZ).replace(tzinfo=None)

router = APIRouter()


# ============== 选股策略 API ==============

@router.get("/api/v2/strategies/{account_id}/selection/factors")
async def get_factor_configs(account_id: str = Path(...)):
    """获取选股因子配置列表"""
    db = get_db_manager()

    # 验证账户
    account = await db.fetchone(
        "SELECT 1 FROM accounts WHERE account_id = ? AND is_active = 1",
        (account_id,)
    )
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在：{account_id}")

    engine = get_selection_engine()
    factors = engine.get_factor_configs()

    return {
        "success": True,
        "factors": factors,
        "count": len(factors)
    }


@router.post("/api/v2/strategies/{account_id}/selection/scan")
async def scan_stocks(
    account_id: str = Path(...),
    min_score: float = Body(60.0, description="最低评分阈值"),
    top_n: Optional[int] = Body(50, description="返回前 N 只股票"),
    use_local: bool = Body(True, description="是否使用本地数据源")
):
    """扫描股票并评分"""
    db = get_db_manager()

    account = await db.fetchone(
        "SELECT 1 FROM accounts WHERE account_id = ? AND is_active = 1",
        (account_id,)
    )
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在：{account_id}")

    engine = get_selection_engine()
    await engine.initialize()

    try:
        results = await engine.scan_stocks(
            min_score=min_score,
            top_n=top_n,
            use_local=use_local
        )

        return {
            "success": True,
            "stocks": [
                {
                    "stock_code": r.stock_code,
                    "stock_name": r.stock_name,
                    "total_score": r.total_score,
                    "profile_tags": r.profile_tags,
                    "current_price": r.current_price,
                    "change_pct": r.change_pct,
                    "volume_ratio": r.volume_ratio,
                    "match_reasons": r.match_reasons,
                    "factor_scores": r.factor_scores
                }
                for r in results
            ],
            "count": len(results)
        }
    finally:
        await engine.close()


@router.get("/api/v2/strategies/{account_id}/selection/stock/{stock_code}")
async def get_stock_evaluation(
    account_id: str = Path(...),
    stock_code: str = Path(..., description="股票代码")
):
    """获取单只股票的详细评估"""
    db = get_db_manager()

    account = await db.fetchone(
        "SELECT 1 FROM accounts WHERE account_id = ? AND is_active = 1",
        (account_id,)
    )
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在：{account_id}")

    engine = get_selection_engine()
    await engine.initialize()

    try:
        from services.data.local_data_service import get_local_data_service
        local_service = get_local_data_service()

        kline_data = local_service.get_kline_data(stock_code, limit=60)
        if not kline_data:
            raise HTTPException(status_code=404, detail="股票数据不存在")

        score = await engine._evaluate_stock(stock_code, kline_data)

        if not score:
            raise HTTPException(status_code=404, detail="无法评估该股票")

        return {
            "success": True,
            "evaluation": {
                "stock_code": score.stock_code,
                "stock_name": score.stock_name,
                "total_score": score.total_score,
                "profile_tags": score.profile_tags,
                "factor_scores": score.factor_scores,
                "factor_details": score.factor_details,
                "match_reasons": score.match_reasons,
                "current_price": score.current_price,
                "change_pct": score.change_pct,
                "volume_ratio": score.volume_ratio,
                "turnover_rate": score.turnover_rate
            }
        }
    finally:
        await engine.close()


# ============== 持仓策略 API ==============

@router.get("/api/v2/strategies/{account_id}/position/config")
async def get_position_config(account_id: str = Path(...)):
    """获取持仓策略配置"""
    db = get_db_manager()

    account = await db.fetchone(
        "SELECT 1 FROM accounts WHERE account_id = ? AND is_active = 1",
        (account_id,)
    )
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在：{account_id}")

    # 从策略表读取配置
    strategy = await db.fetchone(
        "SELECT * FROM strategies WHERE account_id = ? AND status = 'active' LIMIT 1",
        (account_id,)
    )

    engine = get_position_engine()
    config = engine.get_position_config(strategy.get('id') if strategy else 0)

    return {
        "success": True,
        "config": {
            "strategy_id": config.strategy_id,
            "strategy_name": config.strategy_name,
            "base_position_pct": config.base_position_pct,
            "max_position_pct": config.max_position_pct,
            "min_position_pct": config.min_position_pct,
            "max_single_stock_pct": config.max_single_stock_pct,
            "max_holding_count": config.max_holding_count,
            "bull_position_pct": config.bull_position_pct,
            "bear_position_pct": config.bear_position_pct,
            "stop_loss_pct": config.stop_loss_pct,
            "take_profit_pct": config.take_profit_pct,
            "trailing_stop_pct": config.trailing_stop_pct,
            "default_period": config.default_period.value
        }
    }


@router.post("/api/v2/strategies/{account_id}/position/config")
async def save_position_config(
    account_id: str = Path(...),
    config: Dict[str, Any] = Body(..., description="持仓配置")
):
    """保存持仓策略配置"""
    db = get_db_manager()

    account = await db.fetchone(
        "SELECT 1 FROM accounts WHERE account_id = ? AND is_active = 1",
        (account_id,)
    )
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在：{account_id}")

    from services.strategy.position_engine import PositionConfig, HoldingPeriod

    position_config = PositionConfig(
        strategy_id=config.get('strategy_id', 0),
        strategy_name=config.get('strategy_name', '自定义策略'),
        base_position_pct=config.get('base_position_pct', 0.6),
        max_position_pct=config.get('max_position_pct', 0.8),
        min_position_pct=config.get('min_position_pct', 0.2),
        max_single_stock_pct=config.get('max_single_stock_pct', 0.2),
        max_holding_count=config.get('max_holding_count', 10),
        bull_position_pct=config.get('bull_position_pct', 0.9),
        bear_position_pct=config.get('bear_position_pct', 0.1),
        stop_loss_pct=config.get('stop_loss_pct', 0.05),
        take_profit_pct=config.get('take_profit_pct', 0.15),
        trailing_stop_pct=config.get('trailing_stop_pct', 0.08),
        default_period=HoldingPeriod[config.get('default_period', 'SHORT').upper()]
    )

    engine = get_position_engine()
    engine.save_position_config(position_config)

    return {
        "success": True,
        "message": "持仓配置已保存",
        "config": {
            "strategy_id": position_config.strategy_id,
            "strategy_name": position_config.strategy_name,
            "base_position_pct": position_config.base_position_pct,
            "max_single_stock_pct": position_config.max_single_stock_pct,
            "stop_loss_pct": position_config.stop_loss_pct,
            "take_profit_pct": position_config.take_profit_pct
        }
    }


@router.post("/api/v2/strategies/{account_id}/position/analyze")
async def analyze_market(
    account_id: str = Path(...),
    index_data: Dict[str, Any] = Body(..., description="指数数据")
):
    """分析市场环境并生成仓位建议"""
    db = get_db_manager()

    account = await db.fetchone(
        "SELECT 1 FROM accounts WHERE account_id = ? AND is_active = 1",
        (account_id,)
    )
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在：{account_id}")

    from services.strategy.position_engine import MarketIndicator

    # 构建市场指标对象
    market = MarketIndicator(
        index_code=index_data.get('index_code', '000001'),
        index_name=index_data.get('index_name', '上证指数'),
        current_price=index_data.get('current_price', 0),
        change_pct=index_data.get('change_pct', 0),
        volume=index_data.get('volume', 0),
        ma5=index_data.get('ma5'),
        ma10=index_data.get('ma10'),
        ma20=index_data.get('ma20'),
        ma60=index_data.get('ma60'),
        advance_count=index_data.get('advance_count', 0),
        decline_count=index_data.get('decline_count', 0),
        limit_up_count=index_data.get('limit_up_count', 0),
        limit_down_count=index_data.get('limit_down_count', 0)
    )

    engine = get_position_engine()

    # 分析市场
    analysis = await engine.analyze_market(market)

    # 生成仓位建议
    config = engine.get_position_config(0)
    recommendation = engine.generate_position_recommendation(analysis, config)

    return {
        "success": True,
        "market_analysis": {
            "market_condition": analysis.market_condition.value,
            "confidence": analysis.confidence,
            "trend_score": analysis.trend_score,
            "sentiment_score": analysis.sentiment_score,
            "risk_level": analysis.risk_level,
            "analysis_details": analysis.analysis_details
        },
        "position_recommendation": {
            "position_level": recommendation.position_level.value,
            "position_pct": recommendation.position_pct,
            "max_single_stock_pct": recommendation.max_single_stock_pct,
            "max_holding_count": recommendation.max_holding_count,
            "recommended_period": recommendation.recommended_period.value,
            "stop_loss_pct": recommendation.stop_loss_pct,
            "take_profit_pct": recommendation.take_profit_pct,
            "reasoning": recommendation.reasoning,
            "risk_warnings": recommendation.risk_warnings
        }
    }


@router.post("/api/v2/strategies/{account_id}/position/calculate")
async def calculate_position_size(
    account_id: str = Path(...),
    available_cash: float = Body(..., description="可用资金"),
    current_price: float = Body(..., description="当前股价"),
    position_pct: float = Body(0.2, description="建议仓位比例"),
    max_single_pct: float = Body(0.2, description="单只股票最大仓位")
):
    """计算单只股票的买入数量"""
    db = get_db_manager()

    account = await db.fetchone(
        "SELECT 1 FROM accounts WHERE account_id = ? AND is_active = 1",
        (account_id,)
    )
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在：{account_id}")

    engine = get_position_engine()

    quantity = engine.calculate_position_size(
        available_cash=available_cash,
        current_price=current_price,
        position_pct=position_pct,
        max_single_pct=max_single_pct
    )

    return {
        "success": True,
        "suggested_quantity": quantity,
        "estimated_amount": quantity * current_price,
        "current_price": current_price
    }


# ============== 交易策略 API ==============

@router.get("/api/v2/strategies/{account_id}/trading/config")
async def get_trading_config(account_id: str = Path(...)):
    """获取交易策略配置"""
    db = get_db_manager()

    account = await db.fetchone(
        "SELECT 1 FROM accounts WHERE account_id = ? AND is_active = 1",
        (account_id,)
    )
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在：{account_id}")

    engine = get_trading_engine()
    config = engine.get_config(0)

    return {
        "success": True,
        "config": {
            "strategy_id": config.strategy_id,
            "strategy_name": config.strategy_name,
            "buy_on_breakout": config.buy_on_breakout,
            "buy_on_golden_cross": config.buy_on_golden_cross,
            "buy_on_pattern": config.buy_on_pattern,
            "max_position_per_stock": config.max_position_per_stock,
            "max_total_positions": config.max_total_positions,
            "stop_loss": {
                "fixed_stop_loss_pct": config.stop_loss.fixed_stop_loss_pct,
                "trailing_stop_pct": config.stop_loss.trailing_stop_pct,
                "ma20_stop_loss": config.stop_loss.ma20_stop_loss,
                "time_stop_days": config.stop_loss.time_stop_days,
                "max_loss_per_stock_pct": config.stop_loss.max_loss_per_stock_pct
            },
            "take_profit": {
                "fixed_take_profit_pct": config.stop_loss.fixed_take_profit_pct if hasattr(config.stop_loss, 'fixed_take_profit_pct') else 0.15,
                "staged_take_profit": config.take_profit.staged_take_profit,
                "trend_tracking": config.take_profit.trend_tracking,
                "rsi_overbought": config.take_profit.rsi_overbought
            }
        }
    }


@router.post("/api/v2/strategies/{account_id}/trading/config")
async def save_trading_config(
    account_id: str = Path(...),
    config: Dict[str, Any] = Body(..., description="交易配置")
):
    """保存交易策略配置"""
    db = get_db_manager()

    account = await db.fetchone(
        "SELECT 1 FROM accounts WHERE account_id = ? AND is_active = 1",
        (account_id,)
    )
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在：{account_id}")

    from services.strategy.trading_engine import (
        TradingConfig, StopLossConfig, TakeProfitConfig
    )

    stop_loss = StopLossConfig(
        fixed_stop_loss_pct=config.get('stop_loss', {}).get('fixed_stop_loss_pct', 0.05),
        trailing_stop_pct=config.get('stop_loss', {}).get('trailing_stop_pct', 0.08),
        ma20_stop_loss=config.get('stop_loss', {}).get('ma20_stop_loss', True),
        time_stop_days=config.get('stop_loss', {}).get('time_stop_days', 10),
        max_loss_per_stock_pct=config.get('stop_loss', {}).get('max_loss_per_stock_pct', 0.10)
    )

    take_profit = TakeProfitConfig(
        fixed_take_profit_pct=config.get('take_profit', {}).get('fixed_take_profit_pct', 0.15),
        staged_take_profit=config.get('take_profit', {}).get('staged_take_profit', True),
        trend_tracking=config.get('take_profit', {}).get('trend_tracking', True),
        rsi_overbought=config.get('take_profit', {}).get('rsi_overbought', 80)
    )

    trading_config = TradingConfig(
        strategy_id=0,
        strategy_name=config.get('strategy_name', '自定义交易策略'),
        buy_on_breakout=config.get('buy_on_breakout', True),
        buy_on_golden_cross=config.get('buy_on_golden_cross', True),
        buy_on_pattern=config.get('buy_on_pattern', True),
        max_position_per_stock=config.get('max_position_per_stock', 0.2),
        max_total_positions=config.get('max_total_positions', 10),
        stop_loss=stop_loss,
        take_profit=take_profit
    )

    engine = get_trading_engine()
    engine.save_config(trading_config)

    return {
        "success": True,
        "message": "交易配置已保存",
        "config": {
            "stop_loss_pct": trading_config.stop_loss.fixed_stop_loss_pct,
            "take_profit_pct": trading_config.take_profit.fixed_take_profit_pct,
            "trailing_stop_pct": trading_config.stop_loss.trailing_stop_pct
        }
    }


@router.post("/api/v2/strategies/{account_id}/trading/evaluate")
async def evaluate_position(
    account_id: str = Path(...),
    position: Dict[str, Any] = Body(..., description="持仓数据")
):
    """评估持仓，生成交易信号"""
    db = get_db_manager()

    account = await db.fetchone(
        "SELECT 1 FROM accounts WHERE account_id = ? AND is_active = 1",
        (account_id,)
    )
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在：{account_id}")

    from services.strategy.trading_engine import PositionData

    # 构建持仓对象
    pos = PositionData(
        stock_code=position.get('stock_code', ''),
        stock_name=position.get('stock_name', ''),
        quantity=position.get('quantity', 0),
        avg_cost=position.get('avg_cost', 0),
        current_price=position.get('current_price', 0),
        market_value=position.get('market_value', 0),
        profit_loss=position.get('profit_loss', 0),
        profit_loss_pct=position.get('profit_loss_pct', 0),
        highest_price_since_buy=position.get('highest_price', 0),
        lowest_price_since_buy=position.get('lowest_price', 0),
        buy_date=position.get('buy_date', ''),
        holding_days=position.get('holding_days', 0)
    )

    # 获取 K 线数据
    from services.data.local_data_service import get_local_data_service
    local_service = get_local_data_service()
    kline_data = local_service.get_kline_data(pos.stock_code, limit=60)

    engine = get_trading_engine()
    signal = await engine.evaluate_position(pos, kline_data or [])

    if not signal:
        return {
            "success": True,
            "signal": None,
            "message": "继续持有，无交易信号"
        }

    return {
        "success": True,
        "signal": {
            "signal_type": signal.signal_type.value,
            "strength": signal.strength.value,
            "stock_code": signal.stock_code,
            "current_price": signal.current_price,
            "suggested_price": signal.suggested_price,
            "suggested_quantity": signal.suggested_quantity,
            "reasons": signal.reasons,
            "stop_loss_price": signal.stop_loss_price,
            "take_profit_price": signal.take_profit_price,
            "confidence": signal.confidence
        }
    }


@router.post("/api/v2/strategies/{account_id}/trading/scan")
async def scan_buy_opportunities(
    account_id: str = Path(...),
    stock_codes: Optional[List[str]] = Body(None, description="要扫描的股票列表"),
    limit: int = Body(20, description="返回数量限制")
):
    """扫描买入机会"""
    db = get_db_manager()

    account = await db.fetchone(
        "SELECT 1 FROM accounts WHERE account_id = ? AND is_active = 1",
        (account_id,)
    )
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在：{account_id}")

    from services.data.local_data_service import get_local_data_service
    local_service = get_local_data_service()

    # 获取股票列表
    if stock_codes is None:
        stock_codes = local_service.get_all_stocks()[:100]  # 限制扫描数量

    # 准备数据
    stock_data = {}
    for code in stock_codes[:limit]:
        kline = local_service.get_kline_data(code, limit=60)
        if kline and len(kline) >= 30:
            stock_data[code] = {
                "kline_data": kline,
                "current_price": kline[-1]['close'] if kline else 0
            }

    engine = get_trading_engine()
    signals = await engine.scan_buy_opportunities(stock_data)

    return {
        "success": True,
        "opportunities": [
            {
                "stock_code": s.stock_code,
                "stock_name": s.stock_name,
                "current_price": s.current_price,
                "suggested_price": s.suggested_price,
                "signal_strength": s.strength.value,
                "confidence": s.confidence,
                "reasons": s.reasons,
                "pattern": s.pattern.value if s.pattern else None,
                "stop_loss_price": s.stop_loss_price,
                "take_profit_price": s.take_profit_price
            }
            for s in signals[:20]  # 最多返回 20 个
        ],
        "count": len(signals)
    }


@router.post("/api/v2/strategies/{account_id}/trading/patterns")
async def detect_patterns(
    account_id: str = Path(...),
    stock_code: str = Body(..., description="股票代码")
):
    """检测股票 K 线形态"""
    db = get_db_manager()

    account = await db.fetchone(
        "SELECT 1 FROM accounts WHERE account_id = ? AND is_active = 1",
        (account_id,)
    )
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在：{account_id}")

    from services.data.local_data_service import get_local_data_service
    local_service = get_local_data_service()

    kline_data = local_service.get_kline_data(stock_code, limit=60)
    if not kline_data:
        raise HTTPException(status_code=404, detail="股票数据不存在")

    engine = get_trading_engine()
    patterns = engine.detect_patterns(kline_data)

    pattern_names = {
        "bullish_engulfing": "阳包阴",
        "bearish_engulfing": "阴包阳",
        "hammer": "锤头线",
        "shooting_star": "射击之星",
        "golden_cross": "金叉",
        "dead_cross": "死叉",
        "breakout": "突破",
        "breakdown": "破位"
    }

    return {
        "success": True,
        "stock_code": stock_code,
        "patterns": [
            {"type": p.value, "name": pattern_names.get(p.value, p.value)}
            for p in patterns
        ],
        "count": len(patterns)
    }
