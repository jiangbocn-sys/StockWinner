"""
信号执行器 — 买入/卖出执行、pending 信号扫描、信号/watchlist CRUD。
"""
import asyncio
from typing import Dict, List, Optional, Any

from services.common.database import get_db_manager
from services.common.db_write_queue import get_db_write_queue
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
        strategy_id = stock.get('strategy_id')

        # 前置检查：查询策略配置和剩余现金
        strategy_config = None
        strategy_cash = 0
        signal_alloc = {}
        min_amount_per_stock = 0
        max_position_pct = 0

        if strategy_id:
            strategy = await db.fetchone(
                "SELECT config, strategy_cash FROM strategies WHERE id = ? AND account_id = ?",
                (strategy_id, account_id),
            )
            if strategy and strategy.get("config"):
                try:
                    import json
                    strategy_config = json.loads(strategy["config"]) if isinstance(strategy["config"], str) else strategy["config"]
                    strategy_cash = strategy.get("strategy_cash", 0) or 0
                    signal_alloc = strategy_config.get("signal_allocation", {})
                    min_amount_per_stock = signal_alloc.get("min_amount_per_stock", 0)
                    max_position_pct = signal_alloc.get("max_position_pct", 0)
                except (json.JSONDecodeError, ValueError, TypeError):
                    pass

        logger = get_logger("monitor")
        logger.log_event("buy_pre_check",
            f"买入前置检查: {stock_code}, strategy_id={strategy_id}, strategy_cash={strategy_cash}, min_amount={min_amount_per_stock}, max_position_pct={max_position_pct}",
            stock_code=stock_code, strategy_id=strategy_id, strategy_cash=strategy_cash,
            min_amount=min_amount_per_stock, max_position_pct=max_position_pct)

        # 前置筛选：策略剩余现金必须 >= min_amount_per_stock
        if min_amount_per_stock > 0 and strategy_cash < min_amount_per_stock:
            logger.log_event("buy_skip_cash_insufficient",
                f"跳过买入 {stock_code}：策略剩余现金 {strategy_cash:.0f} 不足最小买入金额 {min_amount_per_stock}",
                stock_code=stock_code, strategy_cash=strategy_cash, min_amount=min_amount_per_stock,
                strategy_id=strategy_id)
            return  # 不执行买入

        # 优先级 1：个股策略 max_trade_quantity
        ts = await db.fetchone(
            "SELECT max_trade_quantity FROM trading_strategies WHERE account_id = ? AND stock_code = ?",
            (account_id, stock_code),
        )
        if ts and ts.get("max_trade_quantity", 0) > 0:
            target_quantity = ts["max_trade_quantity"]

        # 优先级 2：策略配置 quantity 固定数量
        elif strategy_config and strategy_config.get("quantity"):
            target_quantity = int(strategy_config.get("quantity"))

        # 优先级 3：策略配置 position_pct 按可用资金比例
        elif strategy_config and strategy_config.get("position_pct"):
            position_pct = float(strategy_config.get("position_pct", 0.1))
            account = await db.fetchone(
                "SELECT available_cash FROM accounts WHERE account_id = ?",
                (account_id,)
            )
            if account and current_price > 0:
                available_cash = account.get('available_cash', 0)
                buy_amount = available_cash * position_pct
                qty = int((buy_amount / current_price) // 100) * 100
                target_quantity = max(qty, 0)

        # 优先级 4：signal_allocation.max_position_pct 按策略总市值比例
        elif max_position_pct > 0 and strategy_id and current_price > 0:
            # 查询策略持仓市值（策略关联的持仓）
            strategy_positions = await db.fetchall(
                "SELECT SUM(market_value) as total_mv FROM stock_positions sp "
                "JOIN watchlist w ON sp.stock_code = w.stock_code AND sp.account_id = w.account_id "
                "WHERE sp.account_id = ? AND w.strategy_id = ? AND sp.quantity > 0",
                (account_id, strategy_id)
            )
            position_mv = strategy_positions[0]["total_mv"] if strategy_positions and strategy_positions[0]["total_mv"] else 0
            strategy_total_value = position_mv + strategy_cash  # 策略总市值 = 持仓 + 现金

            # 允许买入上限 = 策略总市值 * max_position_pct%
            max_buy_amount = strategy_total_value * (max_position_pct / 100.0)
            # 实际买入金额 = min(剩余现金, 允许上限)
            actual_buy_amount = min(strategy_cash, max_buy_amount)

            logger.log_event("buy_calc_position_pct",
                f"按 max_position_pct 计算买入: {stock_code}, 策略总市值={strategy_total_value:.0f}, "
                f"持仓={position_mv:.0f}, 现金={strategy_cash:.0f}, 上限={max_buy_amount:.0f}, 实际={actual_buy_amount:.0f}",
                stock_code=stock_code, strategy_total_value=strategy_total_value, position_mv=position_mv,
                strategy_cash=strategy_cash, max_buy_amount=max_buy_amount, actual_buy_amount=actual_buy_amount)

            # 后置检查：实际买入金额必须 >= min_amount_per_stock
            if min_amount_per_stock > 0 and actual_buy_amount < min_amount_per_stock:
                logger.log_event("buy_skip_below_min",
                    f"跳过买入 {stock_code}：实际买入金额 {actual_buy_amount:.0f} 小于最低要求 {min_amount_per_stock}",
                    stock_code=stock_code, actual_buy_amount=actual_buy_amount, min_amount=min_amount_per_stock,
                    strategy_id=strategy_id)
                return  # 不执行买入

            if actual_buy_amount > 0:
                qty = int((actual_buy_amount / current_price) // 100) * 100
                target_quantity = max(qty, 0)

        # 优先级 5：账户单股最大仓位
        elif not target_quantity:
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

        # 后置检查：实际买入金额必须 >= min_amount_per_stock
        if min_amount_per_stock > 0 and target_quantity > 0:
            actual_amount = target_quantity * current_price
            if actual_amount < min_amount_per_stock:
                logger.log_event("buy_skip_amount_insufficient",
                    f"跳过买入 {stock_code}：计算买入金额 {actual_amount:.0f} 小于最小买入金额 {min_amount_per_stock}",
                    stock_code=stock_code, actual_amount=actual_amount, min_amount=min_amount_per_stock,
                    target_quantity=target_quantity, strategy_id=strategy_id)
                return  # 不执行买入

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

            # 买入成功后同步止盈止损（基于实际成交价重新计算）
            from services.monitoring.sl_tp_sync import sync_on_buy_success
            await sync_on_buy_success(
                account_id, stock_code,
                buy_price=result['price'],
                quantity=result['quantity']
            )

            from services.notifications import get_notification_manager
            notification = get_notification_manager()
            await notification.trigger(
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

            # 资金不足等失败时，更新 watchlist 状态避免反复触发
            if "资金不足" in result['message'] or "现金不足" in result['message'] or "可用资金不足" in result['message']:
                # 回退到 watching 状态，等待资金恢复后可再次触发
                await self._update_watchlist_status(account_id, stock_code, 'watching')
                get_logger("monitor").log_event("buy_failed_fund",
                    f"资金不足回退: {stock_code} pending → watching",
                    stock_code=stock_code, reason=result['message'])
            elif "风控拦截" in result['message'] or "仓位超限" in result['message']:
                # 风控拦截，标记为 failed，不再触发（异步写入）
                write_queue = get_db_write_queue()
                write_queue.update_async(
                    "watchlist",
                    {"status": "failed", "updated_at": get_china_time()},
                    "account_id = ? AND stock_code = ?",
                    (account_id, stock_code),
                    callback=lambda _, err: get_logger("monitor").log_event(
                        "watchlist_update_callback",
                        f"watchlist 状态更新完成: {stock_code} → failed" if not err else f"更新失败: {err}"
                    )
                )
                get_logger("monitor").log_event("buy_failed_risk",
                    f"风控拦截终止: {stock_code} pending → failed",
                    stock_code=stock_code, reason=result['message'])

            from services.notifications import get_notification_manager
            notification = get_notification_manager()
            await notification.trigger(
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

            from services.notifications import get_notification_manager
            notification = get_notification_manager()
            await notification.trigger(
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

            from services.notifications import get_notification_manager
            notification = get_notification_manager()
            await notification.trigger(
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
            from services.trading.models import MarketData
            cache = get_price_cache()
            batch_ohlcv = cache.get_all_for_codes(set(stock_codes))
            if batch_ohlcv:
                market_data = {}
                for code, ohlcv in batch_ohlcv.items():
                    market_data[code] = MarketData.from_ohlcv(ohlcv, code, stock_name="")
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
                # 标记信号为 cancelled（即使被风控拒绝也不再重复，异步写入）
                write_queue = get_db_write_queue()
                write_queue.update_async(
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
                write_queue = get_db_write_queue()
                write_queue.update_async(
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
        strategy_id = stock.get('strategy_id')
        # 验证 strategy_id 是否存在（避免 FOREIGN KEY 错误）
        if strategy_id is not None:
            try:
                valid = await db.fetchone(
                    "SELECT 1 FROM strategies WHERE id = ?",
                    (strategy_id,)
                )
                if not valid:
                    strategy_id = None  # 无效，清除
            except Exception:
                strategy_id = None
        signal_data = {
            "account_id": account_id,
            "strategy_id": strategy_id,
            "stock_code": stock.get('stock_code'),
            "stock_name": stock.get('stock_name'),
            "signal_type": signal_type,
            "price": price,
            "target_quantity": target_quantity,
            "status": "pending",
            "created_at": now.isoformat(),
            "executed_at": None,
        }
        # 同步写入，需要返回 signal_id
        write_queue = get_db_write_queue()
        signal_id = await asyncio.to_thread(write_queue.insert, "trading_signals", signal_data)
        get_logger("monitor").log_event("signal_created",
            f"创建交易信号(pending)：{stock.get('stock_code')} - {signal_type}",
            stock_code=stock.get('stock_code'), signal_type=signal_type, signal_id=signal_id)
        return signal_id

    async def _update_signal_status(self, account_id: str, stock_code: str, status: str, quantity: int):
        """更新信号状态：pending → executed / cancelled（异步写入）"""
        now = get_china_time().isoformat()
        write_queue = get_db_write_queue()
        if status == "executed":
            write_queue.execute_async(
                "UPDATE trading_signals SET status = ?, target_quantity = ?, executed_at = ? "
                "WHERE rowid = (SELECT rowid FROM trading_signals "
                "WHERE account_id = ? AND stock_code = ? AND status = 'pending' "
                "ORDER BY created_at DESC LIMIT 1)",
                (status, quantity, now, account_id, stock_code),
            )
        else:
            write_queue.execute_async(
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
        # 异步写入，不阻塞
        write_queue = get_db_write_queue()
        write_queue.update_async(
            "watchlist",
            {"status": status, "updated_at": get_china_time()},
            "account_id = ? AND stock_code = ?",
            (account_id, stock_code)
        )
        get_logger("monitor").log_event("watchlist_status_change",
            f"watchlist 状态变更: {stock_code} {old_status} → {status}",
            account_id=account_id, stock_code=stock_code,
            old_status=old_status, new_status=status)
