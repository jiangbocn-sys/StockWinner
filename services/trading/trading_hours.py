"""
交易时间工具

A 股交易时段规则：
- 09:15-09:20  集合竞价（可挂单、可撤单）
- 09:20-09:25  集合竞价（可挂单、不可撤单）
- 09:25-09:30  集合竞价撮合等待（不可挂单、不可撤单，系统处理中）
- 09:30-11:30  连续竞价（上午）
- 11:30-13:00  午间休市
- 13:00-15:00  连续竞价（下午）
- 15:00        收盘

注：
- 09:25 第一次价格匹配撮合，冻结买卖挂单
- 09:30 按照集合竞价结果撮合成交
- 收盘后需撤销全部未成交委托
"""

from datetime import datetime, time as dtime
from services.common.timezone import get_china_time
from enum import Enum


class TradingPhase(Enum):
    """交易阶段枚举"""
    PRE_OPEN = "pre_open"                    # 09:15 前，盘前
    AUCTION_ORDER = "auction_order"          # 09:15-09:20 集合竞价（可挂可撤）
    AUCTION_LOCK = "auction_lock"            # 09:20-09:25 集合竞价（可挂不可撤）
    AUCTION_MATCH = "auction_match"          # 09:25-09:30 撮合等待（不可挂不可撤）
    MORNING_SESSION = "morning_session"      # 09:30-11:30 连续竞价上午
    LUNCH_BREAK = "lunch_break"              # 11:30-13:00 午间休市
    AFTERNOON_SESSION = "afternoon_session"  # 13:00-15:00 连续竞价下午
    POST_CLOSE = "post_close"                # 15:00 后，盘后


# 交易时段时间边界
PHASE_TIMES = [
    (dtime(9, 15), TradingPhase.AUCTION_ORDER),
    (dtime(9, 20), TradingPhase.AUCTION_LOCK),
    (dtime(9, 25), TradingPhase.AUCTION_MATCH),
    (dtime(9, 30), TradingPhase.MORNING_SESSION),
    (dtime(11, 30), TradingPhase.LUNCH_BREAK),
    (dtime(13, 0), TradingPhase.AFTERNOON_SESSION),
    (dtime(15, 0), TradingPhase.POST_CLOSE),
]


def get_trading_phase(dt: datetime = None) -> TradingPhase:
    """获取当前交易阶段"""
    if dt is None:
        dt = get_china_time()
    t = dt.time()

    for boundary, phase in PHASE_TIMES:
        if t < boundary:
            return phase

    if t >= dtime(15, 0):
        return TradingPhase.POST_CLOSE
    return TradingPhase.PRE_OPEN


def can_place_order(dt: datetime = None) -> bool:
    """是否允许挂单

    可挂单时段：
    - 09:15-09:25（集合竞价，含不可撤单阶段）
    - 09:30-11:30（上午连续竞价）
    - 13:00-15:00（下午连续竞价）
    """
    if dt is None:
        dt = get_china_time()
    t = dt.time()

    return (
        (dtime(9, 15) <= t < dtime(9, 25)) or
        (dtime(9, 30) <= t < dtime(11, 30)) or
        (dtime(13, 0) <= t < dtime(15, 0))
    )


def can_cancel_order(dt: datetime = None) -> bool:
    """是否允许撤单

    可撤单时段：
    - 09:15-09:20（集合竞价可撤阶段）
    - 09:30-11:30（上午连续竞价）
    - 13:00-15:00（下午连续竞价）

    不可撤单：
    - 09:20-09:25（集合竞价锁定）
    - 09:25-09:30（撮合等待）
    """
    if dt is None:
        dt = get_china_time()
    t = dt.time()

    return (
        (dtime(9, 15) <= t < dtime(9, 20)) or
        (dtime(9, 30) <= t < dtime(11, 30)) or
        (dtime(13, 0) <= t < dtime(15, 0))
    )


def is_trading_time(dt: datetime = None) -> bool:
    """是否在交易时间内（包含所有可挂单时段 + 撮合等待）

    交易时间：09:15 ~ 15:00
    （排除 11:30-13:00 午间休市）
    """
    if dt is None:
        dt = get_china_time()
    t = dt.time()

    return (
        (dtime(9, 15) <= t < dtime(11, 30)) or
        (dtime(13, 0) <= t < dtime(15, 0))
    )


def should_cancel_all_orders(dt: datetime = None) -> bool:
    """是否应该撤销全部未成交委托

    收盘后（15:00）触发
    """
    if dt is None:
        dt = get_china_time()
    return dt.time() >= dtime(15, 0)


def get_phase_description(phase: TradingPhase) -> str:
    """获取阶段描述"""
    descriptions = {
        TradingPhase.PRE_OPEN: "盘前，不可交易",
        TradingPhase.AUCTION_ORDER: "集合竞价（可挂可撤）",
        TradingPhase.AUCTION_LOCK: "集合竞价（可挂不可撤）",
        TradingPhase.AUCTION_MATCH: "撮合等待（不可挂不可撤）",
        TradingPhase.MORNING_SESSION: "连续竞价（上午）",
        TradingPhase.LUNCH_BREAK: "午间休市",
        TradingPhase.AFTERNOON_SESSION: "连续竞价（下午）",
        TradingPhase.POST_CLOSE: "收盘后，应撤销全部未成交委托",
    }
    return descriptions.get(phase, "未知时段")


def is_today_trading_day(dt: datetime = None) -> bool:
    """判断今天是否为交易日（使用 SDK 交易日历）"""
    if dt is None:
        dt = get_china_time()
    today = int(dt.strftime('%Y%m%d'))

    try:
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        calendar = sdk_mgr.get_calendar()  # int 列表
        return today in calendar
    except Exception as e:
        print(f"[TradingHours] 获取交易日历失败，降级为工作日判断: {e}")
        return dt.weekday() < 5  # 降级：周一到周五


def can_trade(dt: datetime = None) -> bool:
    """综合判断：是否是交易日 且 在交易时间内"""
    if dt is None:
        dt = get_china_time()

    if not is_today_trading_day(dt):
        return False

    return is_trading_time(dt)


def get_trading_status(dt: datetime = None) -> dict:
    """获取完整交易状态

    Returns:
        {
            "phase": TradingPhase,
            "phase_desc": str,
            "is_trading_day": bool,
            "is_trading_time": bool,
            "can_place_order": bool,
            "can_cancel_order": bool,
            "should_cancel_all": bool,
        }
    """
    if dt is None:
        dt = get_china_time()

    phase = get_trading_phase(dt)
    trading_day = is_today_trading_day(dt)

    return {
        "phase": phase.value,
        "phase_desc": get_phase_description(phase),
        "is_trading_day": trading_day,
        "is_trading_time": is_trading_time(dt),
        "can_place_order": can_place_order(dt) and trading_day,
        "can_cancel_order": can_cancel_order(dt) and trading_day,
        "should_cancel_all": should_cancel_all_orders(dt) and trading_day,
    }
