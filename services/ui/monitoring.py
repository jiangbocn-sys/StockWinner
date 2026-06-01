"""
交易监控 API — 只读状态查询（系统服务，前端不可启停）
"""

from fastapi import APIRouter, HTTPException, Path, Query, Body
from typing import List, Optional
from datetime import datetime
import sqlite3
from services.common.database import get_db_manager, get_sync_connection
from services.monitoring.service import get_trading_monitor
from services.common.timezone import get_china_time, format_china_time

router = APIRouter()


# ============== 只读状态查询 ==============

@router.get("/api/v1/ui/{account_id}/monitoring/status")
async def get_monitoring_status(account_id: str = Path(..., description="账户 ID（仅验证账户存在，返回全局监控状态）")):
    """获取交易监控服务状态（只读，系统服务由调度器自动管理）"""
    db = get_db_manager()

    # 验证账户存在即可
    await validate_account_active(account_id)

    monitor = get_trading_monitor()
    status = monitor.get_status()

    # 不在接口中同步等待 SDK 连接（SDK login 需 15 秒）
    # SDK 健康状态由监控服务后台维护，get_status() 返回的是内存状态
    return {
        "account_id": account_id,
        "monitoring": status
    }


# ============== 交易信号管理 ==============

@router.get("/api/v1/ui/{account_id}/signals")
async def get_signals(
    account_id: str = Path(..., description="账户 ID"),
    status: Optional[str] = Query(None, description="状态筛选：pending/executed/cancelled"),
    signal_type: Optional[str] = Query(None, description="信号类型：buy/sell_stop_loss/sell_take_profit")
):
    """获取交易信号列表"""
    db = get_db_manager()

    # 从数据库验证账户
    await validate_account_active(account_id)

    conditions = ["account_id = ?"]
    params = [account_id]

    if status:
        conditions.append("status = ?")
        params.append(status)
    if signal_type:
        conditions.append("signal_type = ?")
        params.append(signal_type)

    where = " AND ".join(conditions)
    signals = await db.fetchall(
        f"SELECT * FROM trading_signals WHERE {where} ORDER BY created_at DESC LIMIT 100",
        params
    )

    # 批量查询现价（从 kline.db）
    stock_codes = list(set(s['stock_code'] for s in signals))
    price_map = {}
    if stock_codes:
        try:
            conn = get_sync_connection("kline")
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            placeholders = ','.join(['?' for _ in stock_codes])
            cursor.execute(
                f"SELECT stock_code, close FROM kline_data WHERE stock_code IN ({placeholders}) ORDER BY trade_date DESC",
                stock_codes
            )
            for row in cursor.fetchall():
                if row['stock_code'] not in price_map:
                    price_map[row['stock_code']] = row['close']
        except Exception:
            pass

    # 附加现价字段
    for s in signals:
        s['current_price'] = price_map.get(s['stock_code'])

    return {
        "account_id": account_id,
        "signals": signals,
        "count": len(signals)
    }


@router.get("/api/v1/ui/{account_id}/signals/{signal_id}")
async def get_signal(
    account_id: str = Path(..., description="账户 ID"),
    signal_id: int = Path(..., description="信号 ID")
):
    """获取单个交易信号详情"""
    db = get_db_manager()

    # 从数据库验证账户
    await validate_account_active(account_id)

    signal = await db.fetchone(
        "SELECT * FROM trading_signals WHERE id = ? AND account_id = ?",
        (signal_id, account_id)
    )

    if not signal:
        raise HTTPException(status_code=404, detail="信号不存在")

    return {"signal": signal}


@router.post("/api/v1/ui/{account_id}/signals/{signal_id}/execute")
async def execute_signal(
    account_id: str = Path(..., description="账户 ID"),
    signal_id: int = Path(..., description="信号 ID"),
    force: bool = Query(False, description="强制在非交易时间执行（仅测试用）")
):
    """执行交易信号

    交易时间检查：
    - 默认只在 A 股交易时段允许执行
    - 传 force=true 可跳过检查（测试用）
    """
    db = get_db_manager()

    # 交易时间检查
    if not force:
        from services.trading.trading_hours import can_trade, get_trading_phase, get_phase_description
        if not can_trade():
            phase = get_trading_phase()
            phase_desc = get_phase_description(phase)
            return {
                "success": False,
                "message": f"非交易时段({phase_desc})，无法执行。如需测试请加 ?force=true",
            }

    # 从数据库验证账户
    await validate_account_active(account_id)

    # 检查信号是否存在
    signal = await db.fetchone(
        "SELECT * FROM trading_signals WHERE id = ? AND account_id = ?",
        (signal_id, account_id)
    )

    if not signal:
        raise HTTPException(status_code=404, detail="信号不存在")

    # 使用交易执行服务
    from services.trading.execution_service import get_trade_execution_service

    execution = get_trade_execution_service(account_id)
    stock_code = signal.get('stock_code')
    signal_type = signal.get('signal_type')

    if signal_type == 'buy':
        # 从 watchlist 查找对应的 signal_id
        wl = await db.fetchone(
            "SELECT id FROM watchlist WHERE account_id = ? AND stock_code = ? AND status IN ('pending','watching') ORDER BY created_at DESC LIMIT 1",
            (account_id, stock_code)
        )
        result = await execution.execute_buy(
            stock_code=stock_code,
            stock_name=signal.get('stock_name', ''),
            price=signal.get('price', 0),
            target_quantity=signal.get('quantity') or 0,
            strategy_id=signal.get('strategy_id'),
            signal_id=wl['id'] if wl else None,
        )
    elif signal_type in ('sell_stop_loss', 'sell_take_profit'):
        result = await execution.execute_sell(
            stock_code=stock_code,
            stock_name=signal.get('stock_name', ''),
            price=signal.get('price', 0),
            target_quantity=signal.get('quantity') or 0,
        )
    else:
        return {"success": False, "message": "未知的信号类型"}

    if result["success"]:
        # 更新信号状态
        await db.update(
            "trading_signals",
            {"status": "executed", "executed_at": format_china_time(), "result": str(result)},
            "id = ?",
            (signal_id,)
        )

        # 更新 watchlist 状态
        if signal_type == 'buy':
            await db.update(
                "watchlist",
                {"status": "watching", "updated_at": format_china_time()},
                "account_id = ? AND stock_code = ?",
                (account_id, stock_code)
            )
        else:
            # 卖出时检查是否有剩余持仓
            remaining = await db.fetchone(
                "SELECT quantity FROM stock_positions WHERE account_id = ? AND stock_code = ?",
                (account_id, stock_code)
            )
            new_status = 'sold' if not remaining or remaining.get("quantity", 0) == 0 else 'bought'
            await db.update(
                "watchlist",
                {"status": new_status, "updated_at": format_china_time()},
                "account_id = ? AND stock_code = ?",
                (account_id, stock_code)
            )

    return {
        "success": result["success"],
        "message": result.get("message", ""),
        "details": result
    }


@router.post("/api/v1/ui/{account_id}/signals/{signal_id}/cancel")
async def cancel_signal(
    account_id: str = Path(..., description="账户 ID"),
    signal_id: int = Path(..., description="信号 ID")
):
    """取消交易信号

    流程：
    1. 查询信号记录
    2. 查询关联的 orders 记录
    3. 接入实盘后：调用 gateway.cancel_order 撤销券商委托
    4. 更新信号状态为 cancelled
    5. 更新订单状态为 cancelled
    """
    db = get_db_manager()

    # 从数据库验证账户
    await validate_account_active(account_id)

    signal = await db.fetchone(
        "SELECT * FROM trading_signals WHERE id = ? AND account_id = ?",
        (signal_id, account_id)
    )

    if not signal:
        raise HTTPException(status_code=404, detail="信号不存在")

    if signal.get("status") != "pending":
        return {"success": False, "message": f"信号状态为 {signal.get('status')}，仅 pending 状态可取消"}

    # TODO: 接入券商实盘后，需要撤销券商委托单
    # 流程：
    # 1. 通过 signal 信息查询近期创建的 orders 记录
    #    signal_created_at = signal.get("created_at")
    #    order = await db.fetchone(
    #        "SELECT * FROM orders WHERE account_id = ? AND stock_code = ? AND trade_type = ? AND status IN ('pending', 'submitted') AND created_at >= ? ORDER BY created_at DESC LIMIT 1",
    #        (account_id, signal["stock_code"], signal["signal_type"], signal_created_at),
    #    )
    # 2. 如果存在券商委托号，调用 gateway.cancel_order
    #    if order and order.get("broker_order_id"):
    #        from services.trading.gateway import get_gateway
    #        gateway = await get_gateway()
    #        cancel_result = await gateway.cancel_order(
    #            order_no=order["broker_order_id"],
    #            account_id=account_id,
    #        )
    #        if not cancel_result.get("success"):
    #            return {"success": False, "message": f"券商撤单失败: {cancel_result.get('message')}"}
    # 3. 更新订单状态
    #    await db.update("orders", {"status": "cancelled", "updated_at": format_china_time()}, "id = ?", (order["id"],))

    # 更新信号状态
    await db.update(
        "trading_signals",
        {"status": "cancelled", "executed_at": format_china_time()},
        "id = ?",
        (signal_id,)
    )

    return {
        "success": True,
        "message": "交易信号已取消"
    }


@router.post("/api/v1/ui/{account_id}/signals/clear")
async def clear_signals(
    account_id: str = Path(..., description="账户 ID"),
    status: Optional[str] = Body(None, description="可选，只清除指定状态的信号")
):
    """清空交易信号"""
    db = get_db_manager()

    # 从数据库验证账户
    await validate_account_active(account_id)

    if status:
        await db.delete("trading_signals", "account_id = ? AND status = ?", (account_id, status))
    else:
        await db.delete("trading_signals", "account_id = ?", (account_id,))

    return {
        "success": True,
        "message": "交易信号已清空"
    }


# ============== 手动下单 ==============

@router.post("/api/v1/ui/{account_id}/manual-order/quote")
async def get_manual_order_quote(
    account_id: str = Path(..., description="账户 ID"),
    stock_code: str = Body(..., embed=True, description="股票代码（纯数字或带后缀）"),
):
    """从缓存/SDK 获取实时行情（用于下单前的价格参考和名称填充）"""
    db = get_db_manager()
    await validate_account_active(account_id)

    from services.common.stock_code import normalize_stock_code
    normalized_code = normalize_stock_code(stock_code)

    # 先从 PriceCache 读取
    from services.common.price_cache import get_price_cache
    cache = get_price_cache()
    ohlcv = cache.get_ohlcv_with_ttl(normalized_code)

    is_fresh = ohlcv is not None and ohlcv['is_fresh']

    # 如果缓存不新鲜，触发后台刷新
    if not is_fresh:
        async def _bg_refresh_quote():
            try:
                from services.trading.gateway import get_gateway
                gw = await get_gateway()
                gw.subscribe(f"moq:{normalized_code}", {normalized_code}, refresh_interval=0, priority=2)
                await gw.refresh_now(f"moq:{normalized_code}")
                gw.unsubscribe(f"moq:{normalized_code}")
            except Exception:
                pass
        import asyncio
        asyncio.create_task(_bg_refresh_quote())

        # 等待 1-2 秒后重新从缓存读取
        async def _bg_wait_and_retry():
            try:
                import asyncio
                await asyncio.sleep(2)
                # 刷新后缓存会被更新，无需额外操作
            except Exception:
                pass
        asyncio.create_task(_bg_wait_and_retry())

    # 尝试从 SDK 获取完整行情（含 bid/ask 盘口数据）
    try:
        from services.trading.gateway import get_gateway
        gateway = await get_gateway()
        market_data = await gateway.get_market_data(normalized_code)
        is_fresh = True
        price_source = "sdk_realtime"
        result = {
            "success": True,
            "stock_code": normalized_code,
            "stock_name": market_data.stock_name,
            "current_price": market_data.current_price,
            "bid1": market_data.bid[0] if market_data.bid else None,
            "ask1": market_data.ask[0] if market_data.ask else None,
            "bid_levels": market_data.bid[:5] if market_data.bid else [],
            "ask_levels": market_data.ask[:5] if market_data.ask else [],
            "bid_volumes": [int(v) for v in market_data.bid_volume[:5]] if market_data.bid_volume else [],
            "ask_volumes": [int(v) for v in market_data.ask_volume[:5]] if market_data.ask_volume else [],
            "change_percent": market_data.change_percent,
            "high": market_data.high,
            "low": market_data.low,
            "open_price": market_data.open_price,
            "prev_close": market_data.prev_close,
            "volume": market_data.volume,
            "amount": market_data.amount,
            "price_fresh": True,
            "price_source": price_source,
        }
    except Exception:
        # SDK 不可用，使用缓存数据
        if ohlcv:
            d = ohlcv['data']
            result = {
                "success": True,
                "stock_code": normalized_code,
                "stock_name": "",
                "current_price": d.get('close'),
                "bid1": None,
                "ask1": None,
                "bid_levels": [],
                "ask_levels": [],
                "bid_volumes": [],
                "ask_volumes": [],
                "change_percent": d.get('change_pct'),
                "high": d.get('high'),
                "low": d.get('low'),
                "open_price": d.get('open'),
                "prev_close": None,
                "volume": d.get('volume'),
                "amount": d.get('amount'),
                "price_fresh": is_fresh,
                "price_source": "cache" if is_fresh else "cache_stale",
            }
        else:
            return {"success": False, "message": "无法获取行情数据，请稍后重试"}

    # 同时返回最大可买/可卖数量参考（基于可用资金和持仓）
    from services.trading.execution_service import get_trade_execution_service
    execution = get_trade_execution_service(account_id)

    price = result.get("current_price")
    if price and price > 0:
        try:
            account_info = await execution.get_account_info()
            available_cash = account_info.get("available_cash", 0.0) if account_info else 0.0
            fees_cfg = await execution._get_fee_config()
            max_single_pct = account_info.get("max_single_position_pct", 0.15) if account_info else 0.15
            fee_rate = fees_cfg["commission_rate"] + fees_cfg["transfer_fee"]

            positions = await db.fetchall(
                "SELECT SUM(market_value) as total_mv FROM stock_positions WHERE account_id = ?",
                (account_id,)
            )
            current_mv = positions[0]["total_mv"] if positions and positions[0]["total_mv"] else 0
            total_assets = current_mv + available_cash

            fund_limit = int((available_cash - fees_cfg["min_commission"]) / (price * (1 + fee_rate)))
            fund_limit = (fund_limit // 100) * 100

            risk_limit = int(total_assets * max_single_pct / price) if price > 0 and max_single_pct > 0 else 0
            risk_limit = (risk_limit // 100) * 100

            result["max_buy_quantity"] = risk_limit
            result["fund_limit_quantity"] = fund_limit
        except Exception:
            pass

    position = await execution.get_position(normalized_code)
    if position:
        result["position_quantity"] = position.get("quantity", 0)
        result["available_quantity"] = position.get("available_quantity", 0)
    else:
        result["position_quantity"] = 0
        result["available_quantity"] = 0

    return result


@router.post("/api/v1/ui/{account_id}/manual-order/calculate")
async def calculate_manual_order(
    account_id: str = Path(..., description="账户 ID"),
    stock_code: str = Body(..., description="股票代码（纯数字或带后缀）"),
    stock_name: Optional[str] = Body("", description="股票名称"),
    trade_type: str = Body(..., description="buy 或 sell"),
    price: float = Body(..., description="委托价格"),
    quantity: int = Body(0, description="委托数量，0 表示自动计算最大可买/可卖"),
):
    """预计算手动订单：可买/可卖数量、总价、手续费"""
    db = get_db_manager()
    await validate_account_active(account_id)

    if trade_type not in ("buy", "sell"):
        return {"success": False, "message": "trade_type 必须为 buy 或 sell"}

    if price <= 0:
        return {"success": False, "message": "委托价格必须大于 0"}

    from services.common.stock_code import normalize_stock_code
    normalized_code = normalize_stock_code(stock_code)

    # SDK 连接检查
    from services.common.sdk_manager import get_sdk_manager
    if not get_sdk_manager().is_connected():
        return {"success": False, "message": "券商服务器连接失败，请检查网络后重试"}

    from services.trading.execution_service import get_trade_execution_service
    execution = get_trade_execution_service(account_id)

    position_qty = 0
    available_qty = 0

    if trade_type == "buy":
        qty, total_amount, fees = await execution.calculate_buy_quantity(
            stock_code=normalized_code, price=price,
            target_quantity=quantity if quantity > 0 else None
        )
        max_qty = qty
        available_cash = await execution.get_available_cash()
    else:
        position = await execution.get_position(normalized_code)
        position_qty = position.get("quantity", 0) if position else 0
        available_qty = position.get("available_quantity", 0) if position else 0

        qty, net_amount, fees = await execution.calculate_sell_quantity(
            stock_code=normalized_code, price=price,
            target_quantity=quantity if quantity > 0 else None
        )
        max_qty = available_qty

    result: dict = {
        "success": True,
        "stock_code": normalized_code,
        "stock_name": stock_name,
        "trade_type": trade_type,
        "quantity": qty,
        "max_quantity": max_qty,
        "total_amount": round(
            (price * qty + fees["total_fee"]) if trade_type == "buy"
            else (price * qty - fees["total_fee"]), 2
        ),
        "fees": fees,
        "position_quantity": position_qty,
        "available_quantity": available_qty,
    }
    if trade_type == "buy":
        result["available_cash"] = round(available_cash, 2)

    return result


@router.post("/api/v1/ui/{account_id}/manual-order/submit")
async def submit_manual_order(
    account_id: str = Path(..., description="账户 ID"),
    stock_code: str = Body(..., description="股票代码"),
    stock_name: str = Body("", description="股票名称"),
    trade_type: str = Body(..., description="buy 或 sell"),
    price: float = Body(..., description="委托价格"),
    quantity: int = Body(..., description="委托数量"),
    order_type: str = Body("day", description="day=当日有效，gtc=长期有效"),
    group_id: Optional[int] = Body(None, description="watchlist 分组 ID（买入单且不在 watchlist 中时需要）"),
):
    """提交手动委托订单 → 创建 trading_signals 记录，由监控程序扫描执行"""
    db = get_db_manager()

    await validate_account_active(account_id)

    if trade_type not in ("buy", "sell"):
        return {"success": False, "message": "trade_type 必须为 buy 或 sell"}

    if price <= 0:
        return {"success": False, "message": "委托价格必须大于 0"}

    if quantity <= 0:
        return {"success": False, "message": "委托数量必须大于 0"}

    if quantity % 100 != 0:
        return {"success": False, "message": "委托数量必须是 100 的整数倍"}

    # 买入资金预检（防止下单数量超过可用资金，产生无效信号）
    if trade_type == "buy":
        total_amount = price * quantity
        account_available_cash = account.get("available_cash", 0)

        # 计算所有策略现金之和（手动买入不能挪用策略资金）
        strategies = await db.fetchall(
            "SELECT id, name, strategy_cash FROM strategies WHERE account_id = ? AND is_active = 1",
            (account_id,)
        )
        total_strategy_cash = sum(s.get("strategy_cash", 0) or 0 for s in strategies)

        # 手动买入可用现金 = 账户可用资金 - 策略现金之和
        manual_available_cash = account_available_cash - total_strategy_cash

        if manual_available_cash < total_amount:
            if manual_available_cash <= 0:
                return {
                    "success": False,
                    "message": f"账户可用资金已被策略分配占用，无可用资金进行手动买入。请从策略中转出资金后再试。",
                    "total_strategy_cash": total_strategy_cash,
                    "strategies": [{"id": s["id"], "name": s["name"], "cash": s.get("strategy_cash", 0)} for s in strategies if s.get("strategy_cash", 0) > 0],
                }
            else:
                return {
                    "success": False,
                    "message": f"手动买入可用资金 {manual_available_cash:.2f} 元不足（账户可用 {account_available_cash:.2f} 元，策略占用 {total_strategy_cash:.2f} 元），本次需 {total_amount:.2f} 元。请从策略中转出资金或减少买入数量。",
                    "manual_available_cash": round(manual_available_cash, 2),
                    "total_strategy_cash": round(total_strategy_cash, 2),
                }

        # 单只仓位限制：不超过总资产的 max_single_position_pct（默认 15%）
        # 获取持仓市值计算总资产
        positions = await db.fetchall(
            "SELECT SUM(market_value) as total_mv FROM stock_positions WHERE account_id = ?",
            (account_id,)
        )
        current_mv = positions[0]["total_mv"] if positions and positions[0]["total_mv"] else 0
        total_assets = current_mv + account_available_cash
        max_single_pct = account.get("max_single_position_pct", 0.15)
        if total_assets > 0 and total_amount > total_assets * max_single_pct:
            return {
                "success": False,
                "message": f"单笔仓位限制：该金额（{total_amount:.2f} 元）超过总资产的 {int(max_single_pct*100)}%（{total_assets * max_single_pct:.2f} 元），请减少数量",
            }

    from services.common.stock_code import normalize_stock_code
    normalized_code = normalize_stock_code(stock_code)

    from services.common.timezone import format_china_time

    # 卖出持仓预检（防止无持仓的股票被卖出委托）
    if trade_type == "sell":
        position = await db.fetchone(
            "SELECT available_quantity FROM stock_positions WHERE account_id = ? AND stock_code = ? AND quantity > 0",
            (account_id, normalized_code)
        )
        if not position:
            return {
                "success": False,
                "message": f"该账户未持有 {normalized_code}，无法提交卖出委托",
            }
        if position["available_quantity"] < quantity:
            return {
                "success": False,
                "message": f"可卖数量不足（持仓可卖 {position['available_quantity']} 股，委托 {quantity} 股）",
            }

    # 创建/更新 watchlist 记录，状态为 pending，由监控程序扫描执行
    watchlist_action = "existing"  # existing / added / skipped

    # 只检查"手动下单"分组中是否已有该股票，不跨分组更新
    existing_group = await db.fetchone(
        "SELECT id FROM candidate_groups WHERE account_id = ? AND name = '手动下单' AND group_type = 'manual'",
        (account_id,)
    )
    if existing_group:
        manual_group_id = existing_group['id']
    else:
        manual_group_id = await db.insert("candidate_groups", {
            "account_id": account_id,
            "name": "手动下单",
            "group_type": "manual",
            "screening_strategy_id": None,
        })

    # 检查"手动下单"分组中是否已有该股票
    existing = await db.fetchone(
        "SELECT id FROM watchlist WHERE account_id = ? AND stock_code = ? AND group_id = ? AND status IN ('pending', 'watching', 'bought')",
        (account_id, normalized_code, manual_group_id)
    )
    if existing:
        # 已在"手动下单"分组中，更新价格、数量和信号类型
        await db.update(
            "watchlist",
            {"trigger_price": price, "target_quantity": quantity, "signal_type": trade_type, "source_type": "manual", "status": "pending", "updated_at": format_china_time()},
            "id = ?",
            (existing['id'],)
        )
    else:
        # 不在"手动下单"分组中，创建新记录
        await db.insert("watchlist", {
            "account_id": account_id,
            "stock_code": normalized_code,
            "stock_name": stock_name or normalized_code,
            "trigger_price": price,
            "target_quantity": quantity,
            "signal_type": trade_type,
            "status": "pending",
            "source_type": "manual",
            "group_id": manual_group_id,
            "created_at": format_china_time(),
            "updated_at": format_china_time(),
        })
        watchlist_action = "added"

    result = {
        "success": True,
        "message": "已添加到 watchlist，等待监控程序执行",
        "stock_code": normalized_code,
        "price": price,
        "quantity": quantity,
        "trade_type": trade_type,
        "watchlist_action": watchlist_action,
    }

    return result


@router.post("/api/v1/ui/{account_id}/positions/{stock_code}/immediate-sell")
async def immediate_sell_position(
    account_id: str = Path(..., description="账户 ID"),
    stock_code: str = Path(..., description="股票代码"),
):
    """交易时间内一键清仓：获取买盘价格，按持仓数量，立即以买一价卖出

    逻辑：
    1. 检查是否在交易时间
    2. 获取买一价
    3. 直接调用 gateway.sell() 以买一价卖出全部可卖持仓
    4. 非交易时间才创建 watchlist pending 等待开盘执行
    """
    from services.common.stock_code import normalize_stock_code
    normalized_code = normalize_stock_code(stock_code)

    db = get_db_manager()
    await validate_account_active(account_id)

    # 检查持仓
    position = await db.fetchone(
        "SELECT quantity, available_quantity FROM stock_positions WHERE account_id = ? AND stock_code = ? AND quantity > 0",
        (account_id, normalized_code)
    )
    if not position:
        return {"success": False, "message": f"该账户未持有 {normalized_code}，无法清仓"}

    sell_qty = position["available_quantity"]
    if sell_qty <= 0:
        return {"success": False, "message": "可卖数量为 0（T+1 冻结），无法清仓"}

    # 检查交易时间
    from services.trading.trading_hours import can_trade, get_trading_phase, get_trading_status
    trading_status = get_trading_status()

    # 多数据源行情查询：PriceCache → SDK/AKShare 并发竞争 → kline.db 兜底
    from services.trading.market_data_service import MarketDataService
    from services.common.sdk_manager import get_sdk_manager

    connected = get_sdk_manager().is_connected()
    mds = MarketDataService()

    try:
        market_data = await mds.get_market_data(normalized_code, connected)
    except Exception as e:
        # 所有数据源均不可用
        if not can_trade():
            # 非交易时间：尝试从 PriceCache 获取最后缓存价格作为触发价
            from services.common.price_cache import get_price_cache
            cache = get_price_cache()
            entry = cache.get_ohlcv_with_ttl(normalized_code)
            if entry and entry.get('data', {}).get('close', 0) > 0:
                cached_price = entry['data']['close']
                # 用缓存收盘价作为触发价，创建 watchlist pending
                return await _create_pending_sell(
                    db, account_id, normalized_code, round(cached_price, 2), sell_qty,
                    "缓存收盘价（数据源不可用）"
                )
        return {"success": False, "message": f"无法获取行情数据：{e}"}

    bid_levels = market_data.bid[:5] if market_data.bid else []
    current_price = market_data.current_price

    if can_trade():
        # 交易时间内：直接以买一价卖出
        if not bid_levels or all(p == 0 for p in bid_levels):
            sell_price = round(current_price * 0.999, 2) if current_price > 0 else 0
            price_source = "现价-0.1%"
        else:
            sell_price = bid_levels[0]
            price_source = "买一价"

        sell_price = round(sell_price, 2)
        if sell_price <= 0:
            return {"success": False, "message": "无法获取有效卖出价格"}

        # 直接执行卖出
        from services.trading.execution_service import get_trade_execution_service
        execution = get_trade_execution_service(account_id)
        result = await execution.execute_sell(
            stock_code=normalized_code,
            stock_name=market_data.stock_name or normalized_code,
            price=sell_price,
            target_quantity=sell_qty,
            trigger_source="manual_clear",
        )

        if result["success"]:
            return {
                "success": True,
                "trading_time": True,
                "stock_code": normalized_code,
                "stock_name": market_data.stock_name,
                "sell_price": sell_price,
                "sell_quantity": result.get("quantity", sell_qty),
                "price_source": price_source,
                "bid_levels": bid_levels,
                "message": f"清仓卖出成功：{normalized_code} {result['quantity']}股 @ ¥{result['price']}（{price_source}）",
            }
        else:
            return {"success": False, "message": f"清仓卖出失败：{result.get('message', '未知错误')}"}

    else:
        # 非交易时间：创建 watchlist pending 等待开盘
        phase = get_trading_phase()
        trigger_price = bid_levels[0] if bid_levels and any(p > 0 for p in bid_levels) else current_price
        trigger_price = round(trigger_price, 2) if trigger_price else 0

        existing_group = await db.fetchone(
            "SELECT id FROM candidate_groups WHERE account_id = ? AND name = '手动下单' AND group_type = 'manual'",
            (account_id,)
        )
        manual_group_id = existing_group['id'] if existing_group else await db.insert("candidate_groups", {
            "account_id": account_id,
            "name": "手动下单",
            "group_type": "manual",
        })

        existing = await db.fetchone(
            "SELECT id FROM watchlist WHERE account_id = ? AND stock_code = ? AND group_id = ? AND status IN ('pending', 'watching', 'bought')",
            (account_id, normalized_code, manual_group_id)
        )
        if existing:
            await db.update(
                "watchlist",
                {"trigger_price": trigger_price, "target_quantity": sell_qty, "signal_type": "sell", "source_type": "manual", "status": "pending", "updated_at": format_china_time()},
                "id = ?",
                (existing['id'],)
            )
        else:
            await db.insert("watchlist", {
                "account_id": account_id,
                "stock_code": normalized_code,
                "stock_name": market_data.stock_name or normalized_code,
                "trigger_price": trigger_price,
                "target_quantity": sell_qty,
                "signal_type": "sell",
                "status": "pending",
                "source_type": "manual",
                "group_id": manual_group_id,
                "created_at": format_china_time(),
                "updated_at": format_china_time(),
            })

        return {
            "success": False,
            "trading_time": False,
            "phase": phase.value,
            "phase_desc": trading_status["phase_desc"],
            "message": f"当前非交易时间（{trading_status['phase_desc']}），已创建清仓委托（触发价 ¥{trigger_price}），将在开盘后执行",
        }


async def _create_pending_sell(db, account_id: str, stock_code: str, trigger_price: float, quantity: int, price_note: str):
    """非交易时间创建 pending 卖单，返回与 immediate_sell_position 非交易分支一致的响应"""
    from services.common.timezone import format_china_time
    from services.trading.trading_hours import get_trading_phase, get_trading_status

    trading_status = get_trading_status()
    phase = get_trading_phase()

    existing_group = await db.fetchone(
        "SELECT id FROM candidate_groups WHERE account_id = ? AND name = '手动下单' AND group_type = 'manual'",
        (account_id,)
    )
    manual_group_id = existing_group['id'] if existing_group else await db.insert("candidate_groups", {
        "account_id": account_id,
        "name": "手动下单",
        "group_type": "manual",
    })

    existing = await db.fetchone(
        "SELECT id FROM watchlist WHERE account_id = ? AND stock_code = ? AND group_id = ? AND status IN ('pending', 'watching', 'bought')",
        (account_id, stock_code, manual_group_id)
    )
    if existing:
        await db.update(
            "watchlist",
            {"trigger_price": trigger_price, "target_quantity": quantity, "signal_type": "sell", "source_type": "manual", "status": "pending", "updated_at": format_china_time()},
            "id = ?",
            (existing['id'],)
        )
    else:
        await db.insert("watchlist", {
            "account_id": account_id,
            "stock_code": stock_code,
            "stock_name": stock_code,
            "trigger_price": trigger_price,
            "target_quantity": quantity,
            "signal_type": "sell",
            "status": "pending",
            "source_type": "manual",
            "group_id": manual_group_id,
            "created_at": format_china_time(),
            "updated_at": format_china_time(),
        })

    return {
        "success": False,
        "trading_time": False,
        "phase": phase.value,
        "phase_desc": trading_status["phase_desc"],
        "message": f"当前非交易时间（{trading_status['phase_desc']}），已创建清仓委托（触发价 ¥{trigger_price}，{price_note}），将在开盘后执行",
    }
