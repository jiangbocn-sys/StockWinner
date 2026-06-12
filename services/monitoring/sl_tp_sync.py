"""
止盈止损同步服务 — 统一计算和同步 watchlist 止盈止损价格。

核心职责：
1. calculate_effective_sl_tp(): 计算当前生效的止损止盈价格
2. sync_to_watchlist(): 将计算结果写入 watchlist 表
3. 多个触发时机：买入成功、配置变更、highest_price 更新、手动请求

设计原则：与 signal_evaluator._evaluate_sell_decision 保持一致的计算优先级。
"""
from typing import Dict, List, Optional, Any
import json

from services.common.database import get_db_manager
from services.common.db_write_queue import get_db_write_queue
from services.common.timezone import get_china_time, format_china_time
from services.common.structured_logger import get_logger


async def calculate_effective_sl_tp(
    account_id: str,
    stock_code: str,
    avg_cost: Optional[float] = None,
    highest_price: Optional[float] = None,
) -> Dict[str, Any]:
    """
    计算当前真正生效的止损止盈价格

    计算优先级（与 signal_evaluator._evaluate_sell_decision 保持一致）:
    1. trading_strategies 固定价格 > 0 → 直接返回
    2. 根据 strategy_type 计算:
       - trailing_stop: sl = highest_price × (1 - stop_loss_pct)
       - fixed: sl = avg_cost × (1 - stop_loss_pct)
    3. 无配置 → 返回 0

    Args:
        account_id: 账户ID
        stock_code: 股票代码
        avg_cost: 持仓成本（可选，未传则自动查询）
        highest_price: 买入后最高价（可选，未传则自动查询）

    Returns:
        {
            "stop_loss_price": float,
            "take_profit_price": float,
            "source_sl": "ts_fixed" | "ts_trailing" | "ts_pct" | "none",
            "source_tp": "ts_fixed" | "ts_pct" | "none",
            "config_exists": bool,
            "avg_cost_used": float,
            "highest_price_used": float,
        }
    """
    db = get_db_manager()

    # 1. 获取 trading_strategies 配置
    ts = await db.fetchone(
        "SELECT * FROM trading_strategies WHERE account_id = ? AND stock_code = ?",
        (account_id, stock_code)
    )

    if not ts:
        return {
            "stop_loss_price": 0,
            "take_profit_price": 0,
            "source_sl": "none",
            "source_tp": "none",
            "config_exists": False,
            "avg_cost_used": avg_cost or 0,
            "highest_price_used": highest_price or 0,
        }

    # 2. 解析配置
    ts_sl_price = ts.get("stop_loss_price", 0) or 0
    ts_tp_price = ts.get("take_profit_price", 0) or 0
    ts_sl_pct = ts.get("stop_loss_pct", 0) or 0
    ts_tp_pct = ts.get("take_profit_pct", 0) or 0
    strategy_type = ts.get("strategy_type", "fixed") or "fixed"

    # 3. 获取必要的持仓数据（如果未传入）
    if avg_cost is None or highest_price is None:
        pos = await db.fetchone(
            "SELECT avg_cost, highest_price FROM stock_positions "
            "WHERE account_id = ? AND stock_code = ? AND quantity > 0",
            (account_id, stock_code)
        )
        if pos:
            avg_cost = avg_cost or pos.get("avg_cost", 0) or 0
            highest_price = highest_price or pos.get("highest_price", 0) or 0

    avg_cost = avg_cost or 0
    highest_price = highest_price or 0

    # 4. 计算止损价
    sl = 0
    sl_source = "none"

    if ts_sl_price > 0:
        # 优先级1: 固定止损价
        sl = ts_sl_price
        sl_source = "ts_fixed"
    elif strategy_type == "trailing_stop" and highest_price > 0 and ts_sl_pct > 0:
        # 移动止损: 最高价 × (1 - 止损比例)
        # 注意：trailing_stop 时 stop_loss_pct 作为回撤比例
        sl = round(highest_price * (1 - ts_sl_pct), 2)
        sl_source = "ts_trailing"
    elif ts_sl_pct > 0 and avg_cost > 0:
        # 固定比例止损: 成本价 × (1 - 止损比例)
        sl = round(avg_cost * (1 - ts_sl_pct), 2)
        sl_source = "ts_pct"

    # 5. 计算止盈价
    tp = 0
    tp_source = "none"

    if ts_tp_price > 0:
        tp = ts_tp_price
        tp_source = "ts_fixed"
    elif ts_tp_pct > 0 and avg_cost > 0:
        tp = round(avg_cost * (1 + ts_tp_pct), 2)
        tp_source = "ts_pct"

    return {
        "stop_loss_price": sl,
        "take_profit_price": tp,
        "source_sl": sl_source,
        "source_tp": tp_source,
        "config_exists": True,
        "avg_cost_used": avg_cost,
        "highest_price_used": highest_price,
    }


async def sync_to_watchlist(
    account_id: str,
    stock_code: str,
    stop_loss_price: float,
    take_profit_price: float,
    log_reason: str = "",
) -> bool:
    """
    将计算结果同步到 watchlist 表

    Args:
        account_id: 账户ID
        stock_code: 股票代码
        stop_loss_price: 止损价
        take_profit_price: 止盈价
        log_reason: 同步原因（用于日志）

    Returns:
        True 如果更新成功，False 如果无匹配记录
    """
    db = get_db_manager()

    # 检查 watchlist 是否存在活跃记录
    existing = await db.fetchone(
        "SELECT id, stop_loss_price, take_profit_price FROM watchlist "
        "WHERE account_id = ? AND stock_code = ? AND status IN ('pending', 'watching', 'bought')",
        (account_id, stock_code)
    )

    if not existing:
        return False

    old_sl = existing.get("stop_loss_price", 0) or 0
    old_tp = existing.get("take_profit_price", 0) or 0

    # 只有值发生变化才更新（异步写入）
    if abs(old_sl - stop_loss_price) < 0.01 and abs(old_tp - take_profit_price) < 0.01:
        return True  # 无变化，跳过

    write_queue = get_db_write_queue()
    write_queue.execute_async(
        "UPDATE watchlist SET stop_loss_price = ?, take_profit_price = ?, updated_at = ? "
        "WHERE id = ?",
        (stop_loss_price, take_profit_price, get_china_time(), existing["id"])
    )

    logger = get_logger("monitor")
    logger.log_event("sl_tp_sync",
        f"同步止盈止损: {stock_code} SL {old_sl:.2f}→{stop_loss_price:.2f}, "
        f"TP {old_tp:.2f}→{take_profit_price:.2f} [{log_reason}]",
        account_id=account_id, stock_code=stock_code,
        old_sl=old_sl, new_sl=stop_loss_price,
        old_tp=old_tp, new_tp=take_profit_price,
        reason=log_reason)

    return True


async def sync_on_buy_success(
    account_id: str,
    stock_code: str,
    buy_price: float,
    quantity: int,
) -> None:
    """
    买入成功后触发同步

    特殊处理:
    1. 如果 trading_strategies 不存在，自动创建（基于选股策略默认配置）
    2. 用实际成交价作为 avg_cost 计算止损止盈
    3. 初始化 highest_price = buy_price

    Args:
        account_id: 账户ID
        stock_code: 股票代码
        buy_price: 实际买入成交价
        quantity: 买入数量
    """
    db = get_db_manager()

    # 1. 检查/创建 trading_strategies
    ts = await db.fetchone(
        "SELECT id FROM trading_strategies WHERE account_id = ? AND stock_code = ?",
        (account_id, stock_code)
    )

    if not ts:
        # 默认止损止盈比例
        default_sl_pct = 0.08   # 默认止损 8%
        default_tp_pct = 0.15   # 默认止盈 15%

        # 尝试从关联的选股策略获取配置
        wl = await db.fetchone(
            "SELECT strategy_id FROM watchlist "
            "WHERE account_id = ? AND stock_code = ?",
            (account_id, stock_code)
        )
        if wl and wl.get("strategy_id"):
            strategy = await db.fetchone(
                "SELECT config FROM strategies WHERE id = ?",
                (wl["strategy_id"],)
            )
            if strategy and strategy.get("config"):
                try:
                    config = json.loads(strategy["config"]) if isinstance(strategy["config"], str) else strategy["config"]
                    default_sl_pct = float(config.get("stop_loss_pct", default_sl_pct))
                    default_tp_pct = float(config.get("take_profit_pct", default_tp_pct))
                except (json.JSONDecodeError, ValueError, TypeError):
                    pass

        # 创建 trading_strategies（异步写入）
        write_queue = get_db_write_queue()
        write_queue.insert_async(
            "trading_strategies",
            {
                "account_id": account_id,
                "stock_code": stock_code,
                "strategy_type": "fixed",
                "stop_loss_pct": default_sl_pct,
                "take_profit_pct": default_tp_pct,
                "entry_price": buy_price,
                "updated_at": format_china_time()
            }
        )

        get_logger("monitor").log_event("sl_tp_auto_create",
            f"自动创建风控配置: {stock_code} SL={default_sl_pct*100:.0f}% TP={default_tp_pct*100:.0f}%",
            account_id=account_id, stock_code=stock_code,
            sl_pct=default_sl_pct, tp_pct=default_tp_pct, entry_price=buy_price)

    # 2. 更新 stock_positions 的 highest_price（初始化为买入价，异步写入）
    write_queue = get_db_write_queue()
    write_queue.execute_async(
        "UPDATE stock_positions SET highest_price = ? "
        "WHERE account_id = ? AND stock_code = ? AND (highest_price = 0 OR highest_price IS NULL OR highest_price < ?)",
        (buy_price, account_id, stock_code, buy_price)
    )

    # 3. 计算并同步止盈止损
    result = await calculate_effective_sl_tp(
        account_id, stock_code,
        avg_cost=buy_price,
        highest_price=buy_price
    )

    await sync_to_watchlist(
        account_id, stock_code,
        result["stop_loss_price"],
        result["take_profit_price"],
        log_reason=f"buy_success@{buy_price:.2f}"
    )


async def sync_on_strategy_change(
    account_id: str,
    stock_code: str,
) -> None:
    """
    trading_strategies 配置变更后触发同步

    自动获取持仓的 avg_cost 和 highest_price 进行计算

    Args:
        account_id: 账户ID
        stock_code: 股票代码
    """
    result = await calculate_effective_sl_tp(account_id, stock_code)

    if result["config_exists"]:
        await sync_to_watchlist(
            account_id, stock_code,
            result["stop_loss_price"],
            result["take_profit_price"],
            log_reason="strategy_change"
        )


async def sync_on_highest_price_update(
    account_id: str,
    stock_code: str,
    new_highest_price: float,
) -> None:
    """
    highest_price 更新后触发同步（仅对 trailing_stop 策略有效）

    调用时机:
    1. 监控服务盘中检测到新高时
    2. morning_seller 更新后

    Args:
        account_id: 账户ID
        stock_code: 股票代码
        new_highest_price: 新的最高价
    """
    db = get_db_manager()

    # 检查是否为 trailing_stop 策略
    ts = await db.fetchone(
        "SELECT strategy_type, stop_loss_pct FROM trading_strategies "
        "WHERE account_id = ? AND stock_code = ?",
        (account_id, stock_code)
    )

    if not ts or ts.get("strategy_type") != "trailing_stop":
        return  # 非 trailing_stop 不需要更新止损价

    # 获取 avg_cost
    pos = await db.fetchone(
        "SELECT avg_cost FROM stock_positions "
        "WHERE account_id = ? AND stock_code = ? AND quantity > 0",
        (account_id, stock_code)
    )

    if not pos:
        return

    avg_cost = pos.get("avg_cost", 0) or 0

    # 计算（trailing_stop 时止损价基于 highest_price）
    result = await calculate_effective_sl_tp(
        account_id, stock_code,
        avg_cost=avg_cost,
        highest_price=new_highest_price
    )

    await sync_to_watchlist(
        account_id, stock_code,
        result["stop_loss_price"],
        result["take_profit_price"],
        log_reason=f"highest_price_update:{new_highest_price:.2f}"
    )


async def batch_sync_positions(
    account_id: str,
    stock_codes: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    批量同步持仓的止盈止损

    Args:
        account_id: 账户ID
        stock_codes: 指定股票列表，None 表示同步所有持仓

    Returns:
        {"synced": int, "skipped": int, "errors": list}
    """
    db = get_db_manager()

    # 获取需要同步的股票列表
    if stock_codes:
        positions = [{"stock_code": c} for c in stock_codes]
    else:
        positions = await db.fetchall(
            "SELECT stock_code FROM stock_positions "
            "WHERE account_id = ? AND quantity > 0",
            (account_id,)
        )

    synced = 0
    skipped = 0
    errors = []

    for pos in positions:
        stock_code = pos["stock_code"]
        try:
            result = await calculate_effective_sl_tp(account_id, stock_code)

            if not result["config_exists"]:
                skipped += 1
                continue

            updated = await sync_to_watchlist(
                account_id, stock_code,
                result["stop_loss_price"],
                result["take_profit_price"],
                log_reason="batch_sync"
            )

            if updated:
                synced += 1
            else:
                skipped += 1

        except Exception as e:
            errors.append({"stock_code": stock_code, "error": str(e)})

    return {"synced": synced, "skipped": skipped, "errors": errors}