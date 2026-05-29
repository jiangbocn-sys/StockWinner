"""
信号执行器 — 买入/卖出执行、pending 信号扫描、信号/watchlist CRUD。
"""
from typing import Dict, List, Optional, Any

from services.common.database import get_db_manager
from services.common.timezone import get_china_time, format_china_time
from services.common.structured_logger import get_logger


class SignalExecutor:
    """信号执行：买卖委托 + pending 信号扫描 + watchlist/信号 CRUD"""

    async def execute_buy_signal(
        self,
        account_id: str,
        stock: Dict,
        current_price: float,
        target_quantity: int,
        trigger_source: Optional[str] = None,
    ):
        """执行买入交易

        买入数量解析（按优先级）：
        1. 个股策略 max_trade_quantity > 0 → 按该数量
        2. 选股策略 config.quantity → 按固定数量
        3. 选股策略 config.position_pct → 按可用资金比例计算
        4. 账户 max_single_position_pct → 按单股最大仓位计算
        5. 回退到 watchlist target_quantity
        """
        stock_code = stock.get('stock_code')
        stock_name = stock.get('stock_name', '')
        db = get_db_manager()

        # 优先级 1：个股策略 max_trade_quantity
        ts = await db.fetchone(
            "SELECT max_trade_quantity FROM trading_strategies WHERE account_id = ? AND stock_code = ?",
            (account_id, stock_code),
        )
        if ts and ts.get("max_trade_quantity", 0) > 0:
            target_quantity = ts["max_trade_quantity"]
        else:
            # 优先级 2-3：从选股策略配置读取 quantity 或 position_pct
            strategy_id = stock.get('strategy_id')
            if strategy_id:
                strategy = await db.fetchone(
                    "SELECT config FROM strategies WHERE id = ? AND account_id = ?",
                    (strategy_id, account_id),
                )
                if strategy and strategy.get("config"):
                    try:
                        import json
                        config = json.loads(strategy["config"]) if isinstance(strategy["config"], str) else strategy["config"]
                        if config.get("quantity"):
                            target_quantity = int(config["quantity"])
                        elif config.get("position_pct"):
                            position_pct = float(config["position_pct"]) / 100.0
                            account = await db.fetchone(
                                "SELECT available_cash FROM accounts WHERE account_id = ?",
                                (account_id,)
                            )
                            if account:
                                available_cash = account.get('available_cash', 0)
                                buy_amount = available_cash * position_pct
                                if current_price > 0:
                                    qty = int((buy_amount / current_price) // 100) * 100
                                    target_quantity = max(qty, 0)
                    except (json.JSONDecodeError, ValueError, TypeError):
                        pass

            # 优先级 4：按账户单股最大仓位计算
            if not target_quantity:
                account = await db.fetchone(
                    "SELECT available_cash, max_single_position_pct FROM accounts WHERE account_id = ?",
                    (account_id,)
                )
                if account and current_price > 0:
                    available_cash = account.get('available_cash', 0)
                    max_single_pct = account.get('max_single_position_pct', 0.15)
                    positions = await db.fetchall(
                        "SELECT SUM(market_value) as total_mv FROM stock_positions WHERE account_id = ?",
                        (account_id,)
                    )
                    current_mv = positions[0]["total_mv"] if positions and positions[0]["total_mv"] else 0
                    total_assets = current_mv + available_cash
                    risk_limit = int(total_assets * max_single_pct / current_price)
                    if risk_limit >= 100:
                        target_quantity = (risk_limit // 100) * 100

        # 检查信号分配配置中的单股最小金额
        strategy_id = stock.get('strategy_id')
        if strategy_id and target_quantity > 0 and current_price > 0:
            strategy = await db.fetchone(
                "SELECT config FROM strategies WHERE id = ?",
                (strategy_id,)
            )
            if strategy and strategy.get("config"):
                try:
                    import json
                    config = json.loads(strategy["config"]) if isinstance(strategy["config"], str) else strategy["config"]
                    signal_alloc = config.get("signal_allocation", {})
                    min_amount = signal_alloc.get("min_amount_per_stock", 0)
                    if min_amount > 0:
                        actual_amount = target_quantity * current_price
                        if actual_amount < min_amount:
                            logger = get_logger("monitor")
                            logger.log_event("buy_skip_min_amount",
                                f"跳过买入 {stock_code}：金额 {actual_amount:.0f} 小于配置最小金额 {min_amount}",
                                stock_code=stock_code, actual_amount=actual_amount, min_amount=min_amount,
                                strategy_id=strategy_id)
                            return  # 不执行买入
                except (json.JSONDecodeError, ValueError, TypeError):
                    pass

        # 执行买入
        from services.trading.execution_service import get_trade_execution_service
        execution = get_trade_execution_service(account_id)
        result = await execution.execute_buy(
            stock_code=stock_code,
            stock_name=stock_name,
            price=current_price,
            target_quantity=target_quantity,
            trigger_source=trigger_source,
            strategy_id=stock.get('strategy_id') or 0,  # 手动买入时 strategy_id=0
        )

        if result["success"]:
            log = get_logger("monitor")
            log.log_event("buy_success", f"买入成功：{stock_code}",
                          stock_code=stock_code, quantity=result['quantity'],
                          price=result['price'], total_amount=result['total_amount'],
                          fees=result['fees']['total_fee'])

            await self._update_watchlist_status(account_id, stock_code, 'bought')
            await self._update_signal_status(account_id, stock_code, 'executed', result['quantity'])

            from services.notifications import get_notification_service
            notification = get_notification_service()
            await notification.emit(
                event_type="trade_executed",
                account_id=account_id,
                payload={
                    "trade_type": "buy",
                    "stock_code": stock_code,
                    "stock_name": stock_name,
                    "price": f"{current_price:.2f}",
                    "quantity": result['quantity'],
                    "amount": f"{result['total_amount']:.2f}",
                    "fees": f"{result['fees']['total_fee']:.2f}",
                    "trigger_source": trigger_source or "监控",
                },
            )
        else:
            get_logger("monitor").warn("monitor", f"买入失败: {result['message']}",
                                        stock_code=stock_code, reason=result['message'])
            await self._update_signal_status(account_id, stock_code, 'cancelled', target_quantity)

            from services.notifications import get_notification_service
            notification = get_notification_service()
            await notification.emit(
                event_type="order_rejected",
                account_id=account_id,
                payload={
                    "trade_type": "buy",
                    "stock_code": stock_code,
                    "stock_name": stock_name,
                    "price": f"{current_price:.2f}",
                    "quantity": target_quantity,
                    "reason": result['message'],
                },
            )

    async def execute_sell_signal(
        self,
        account_id: str,
        stock: Dict,
        current_price: float,
        signal_type: str,
        target_quantity: int,
        trigger_source: Optional[str] = None,
    ):
        """执行卖出交易"""
        stock_code = stock.get('stock_code')
        stock_name = stock.get('stock_name', '')

        from services.trading.execution_service import get_trade_execution_service
        execution = get_trade_execution_service(account_id)
        result = await execution.execute_sell(
            stock_code=stock_code,
            stock_name=stock_name,
            price=current_price,
            target_quantity=target_quantity,
            trigger_source=trigger_source,
        )

        if result["success"]:
            log = get_logger("monitor")
            log.log_event("sell_success", f"卖出成功：{stock_code}",
                          stock_code=stock_code, quantity=result['quantity'],
                          price=result['price'], net_amount=result['net_amount'],
                          fees=result['fees']['total_fee'], profit_loss=result['profit_loss'])

            await self._update_watchlist_status(account_id, stock_code, 'sold')
            await self._update_signal_status(account_id, stock_code, 'executed', result['quantity'])

            from services.notifications import get_notification_service
            notification = get_notification_service()
            await notification.emit(
                event_type="trade_executed",
                account_id=account_id,
                payload={
                    "trade_type": "sell",
                    "stock_code": stock_code,
                    "stock_name": stock_name,
                    "price": f"{current_price:.2f}",
                    "quantity": result['quantity'],
                    "amount": f"{result['net_amount']:.2f}",
                    "fees": f"{result['fees']['total_fee']:.2f}",
                    "profit_loss": f"{result['profit_loss']:.2f}",
                    "trigger_source": trigger_source or signal_type,
                },
            )
        else:
            get_logger("monitor").warn("monitor", f"卖出失败: {result['message']}",
                                        stock_code=stock_code, reason=result['message'])
            await self._update_signal_status(account_id, stock_code, 'cancelled', 0)

            from services.notifications import get_notification_service
            notification = get_notification_service()
            await notification.emit(
                event_type="order_rejected",
                account_id=account_id,
                payload={
                    "trade_type": "sell",
                    "stock_code": stock_code,
                    "stock_name": stock_name,
                    "price": f"{current_price:.2f}",
                    "quantity": target_quantity,
                    "reason": result['message'],
                },
            )

    async def scan_pending_signals(self, account_id: str, market_data_cache: Optional[Dict[str, Any]] = None):
        """扫描手动创建的 pending 信号并执行

        只处理无 strategy_id 的手动信号，有 strategy_id 的由即时执行路径处理。
        """
        db = get_db_manager()
        signals = await db.fetchall(
            "SELECT * FROM trading_signals WHERE account_id = ? AND status = 'pending' AND strategy_id IS NULL ORDER BY created_at ASC",
            (account_id,)
        )
        if not signals:
            return

        stock_codes = [s['stock_code'] for s in signals]

        # 使用预取行情或 price_cache
        if market_data_cache:
            market_data = market_data_cache
        else:
            from services.common.price_cache import get_price_cache
            from services.trading.gateway import MarketData
            cache = get_price_cache()
            batch_ohlcv = cache.get_all_for_codes(set(stock_codes))
            if batch_ohlcv:
                market_data = {}
                for code, ohlcv in batch_ohlcv.items():
                    market_data[code] = MarketData(
                        stock_code=code, stock_name="",
                        current_price=ohlcv.get('close', 0),
                        change_percent=ohlcv.get('change_pct', 0),
                        high=ohlcv.get('high', 0), low=ohlcv.get('low', 0),
                        open_price=ohlcv.get('open', 0), prev_close=ohlcv.get('close', 0),
                        volume=int(ohlcv.get('volume', 0)), amount=ohlcv.get('amount', 0),
                        source=ohlcv.get('source', ''),
                    )
            else:
                get_logger("monitor").warn("monitor", f"_execute_pending_signals 无缓存数据，跳过账户 {account_id}")
                return

        for signal in signals:
            stock_code = signal['stock_code']
            signal_type = signal['signal_type']
            target_price = signal.get('price', 0)
            quantity = signal.get('target_quantity', 0)

            md = market_data.get(stock_code)
            if not md or not md.current_price:
                get_logger("monitor").warn("monitor", f"无法获取 {stock_code} 行情，跳过信号 {signal['id']}",
                                            stock_code=stock_code, signal_id=signal['id'])
                continue

            current_price = md.current_price

            if signal_type == 'buy':
                if target_price > 0 and current_price > target_price:
                    get_logger("monitor").log_event("pending_signal_skip",
                        f"跳过手动 pending 信号: {stock_code} 买入，现价 {current_price:.2f} 高于目标价 {target_price:.2f}",
                        stock_code=stock_code, signal_type=signal_type,
                        target_price=target_price, current_price=current_price, quantity=quantity)
                    continue

                get_logger("monitor").log_event("pending_signal_execute",
                    f"执行手动 pending 信号: {stock_code} {signal_type}",
                    stock_code=stock_code, signal_type=signal_type,
                    target_price=target_price, current_price=current_price, quantity=quantity)

                await self.execute_buy_signal(
                    account_id,
                    {"stock_code": stock_code, "stock_name": signal.get('stock_name', stock_code)},
                    current_price, quantity, trigger_source="manual",
                )
                # 标记信号为 cancelled（即使被风控拒绝也不再重复）
                await db.update(
                    "trading_signals",
                    {"status": "cancelled", "executed_at": format_china_time(),
                     "result": '{"message": "执行失败，见订单记录"}'},
                    "id = ?", (signal['id'],)
                )
            elif signal_type in ('sell_stop_loss', 'sell_take_profit', 'sell'):
                await self.execute_sell_signal(
                    account_id,
                    {"stock_code": stock_code, "stock_name": signal.get('stock_name', stock_code)},
                    current_price, signal_type, quantity, trigger_source="manual",
                )
            else:
                await db.update(
                    "trading_signals",
                    {"status": "cancelled", "executed_at": format_china_time()},
                    "id = ?", (signal['id'],)
                )
                get_logger("monitor").warn("monitor", f"未知信号类型 {signal_type}，取消信号",
                                            signal_type=signal_type, signal_id=signal['id'])

    async def create_pending_signal(
        self, account_id: str, stock: Dict, signal_type: str, price: float, target_quantity: int,
    ):
        """创建 pending 状态的交易信号"""
        db = get_db_manager()
        now = get_china_time()
        signal_data = {
            "account_id": account_id,
            "strategy_id": stock.get('strategy_id'),
            "stock_code": stock.get('stock_code'),
            "stock_name": stock.get('stock_name'),
            "signal_type": signal_type,
            "price": price,
            "target_quantity": target_quantity,
            "status": "pending",
            "created_at": now.isoformat(),
            "executed_at": None,
        }
        signal_id = await db.insert("trading_signals", signal_data)
        get_logger("monitor").log_event("signal_created",
            f"创建交易信号(pending)：{stock.get('stock_code')} - {signal_type}",
            stock_code=stock.get('stock_code'), signal_type=signal_type, signal_id=signal_id)
        return signal_id

    async def _update_signal_status(self, account_id: str, stock_code: str, status: str, quantity: int):
        """更新信号状态：pending → executed / cancelled"""
        db = get_db_manager()
        now = get_china_time().isoformat()
        if status == "executed":
            await db.execute(
                "UPDATE trading_signals SET status = ?, target_quantity = ?, executed_at = ? "
                "WHERE rowid = (SELECT rowid FROM trading_signals "
                "WHERE account_id = ? AND stock_code = ? AND status = 'pending' "
                "ORDER BY created_at DESC LIMIT 1)",
                (status, quantity, now, account_id, stock_code),
            )
        else:
            await db.execute(
                "UPDATE trading_signals SET status = ?, executed_at = ? "
                "WHERE rowid = (SELECT rowid FROM trading_signals "
                "WHERE account_id = ? AND stock_code = ? AND status = 'pending' "
                "ORDER BY created_at DESC LIMIT 1)",
                (status, now, account_id, stock_code),
            )

    async def _update_watchlist_status(self, account_id: str, stock_code: str, status: str):
        """更新 watchlist 状态

        特殊处理：status='sold' 时检查是否有剩余持仓
        - 无持仓：设为 'sold'
        - 有持仓（部分卖出）：保持 'bought'
        """
        db = get_db_manager()

        # 卖出时检查剩余持仓
        if status == 'sold':
            remaining = await db.fetchone(
                "SELECT quantity FROM stock_positions WHERE account_id = ? AND stock_code = ?",
                (account_id, stock_code)
            )
            if remaining and remaining.get("quantity", 0) > 0:
                status = 'bought'  # 部分卖出，保持持仓状态

        old = await db.fetchone(
            "SELECT status FROM watchlist WHERE account_id = ? AND stock_code = ?",
            (account_id, stock_code)
        )
        old_status = old["status"] if old else "unknown"
        await db.update(
            "watchlist",
            {"status": status, "updated_at": get_china_time()},
            "account_id = ? AND stock_code = ?",
            (account_id, stock_code)
        )
        get_logger("monitor").log_event("watchlist_status_change",
            f"watchlist 状态变更: {stock_code} {old_status} → {status}",
            account_id=account_id, stock_code=stock_code,
            old_status=old_status, new_status=status)
