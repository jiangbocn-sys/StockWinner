"""
交易执行服务 (Trade Execution)
- 管理账户资金
- 计算可买数量
- 执行交易并更新资金
- 计算交易手续费
"""

import asyncio
from typing import Dict, List, Optional, Any, Tuple
from services.common.database import get_db_manager
from services.common.timezone import get_china_time, format_china_time

# 手续费率默认配置（A 股标准，账户未设置时使用）
DEFAULT_FEE_CONFIG = {
    "stamp_tax": 0.0005,      # 印花税 0.05%（只收卖出）
    "commission": 0.0003,     # 佣金 0.03%（买卖双向收取）
    "transfer_fee": 0.00002,  # 过户费 0.002%（买卖双向收取，仅沪市）
    "min_commission": 5.0     # 最低佣金 5 元
}


class TradeExecutionService:
    """交易执行服务"""

    def __init__(self, account_id: str):
        self.account_id = account_id
        self.db = get_db_manager()
        self._fee_cache: Optional[dict] = None

    async def _get_fee_config(self) -> dict:
        """从账户配置读取费率参数，未设置的使用默认值"""
        if self._fee_cache is not None:
            return self._fee_cache
        account = await self.get_account_info()
        self._fee_cache = {
            "commission_rate": account.get("commission_rate") or DEFAULT_FEE_CONFIG["commission"],
            "stamp_tax": account.get("stamp_tax") or DEFAULT_FEE_CONFIG["stamp_tax"],
            "transfer_fee": account.get("transfer_fee") or DEFAULT_FEE_CONFIG["transfer_fee"],
            "min_commission": account.get("min_commission") or DEFAULT_FEE_CONFIG["min_commission"],
        }
        return self._fee_cache

    async def get_account_info(self) -> Optional[Dict]:
        """获取账户信息"""
        account = await self.db.fetchone(
            "SELECT * FROM accounts WHERE account_id = ?",
            (self.account_id,)
        )
        return account

    async def get_available_cash(self) -> float:
        """获取可用资金"""
        account = await self.get_account_info()
        if not account:
            return 0.0
        return account.get("available_cash", 0.0)

    async def get_position(self, stock_code: str) -> Optional[Dict]:
        """获取持仓"""
        position = await self.db.fetchone(
            "SELECT * FROM stock_positions WHERE account_id = ? AND stock_code = ?",
            (self.account_id, stock_code)
        )
        return position

    async def calculate_buy_quantity(
        self,
        stock_code: str,
        price: float,
        target_quantity: Optional[int] = None
    ) -> Tuple[int, float, Dict]:
        """
        计算买入数量

        风控计算：
        1. 按账户 cash_reserve_pct 保留现金 → usable_cash = available * (1 - reserve_pct)
        2. 按 max_single_position_pct 计算单只上限 → risk_limit = usable * max_pct / price
        3. 按可用资金计算资金上限 → fund_limit = available / (price * (1 + fee_rate))
        4. 如果 target_quantity > 0 → quantity = min(target, risk_limit, fund_limit)
        5. 如果 target_quantity = 0 → quantity = min(risk_limit, fund_limit)

        Returns:
            (可买数量，总金额，费用明细)
        """
        account = await self.get_account_info()
        available_cash = account.get("available_cash", 0.0) if account else 0.0
        fees_cfg = await self._get_fee_config()
        commission_rate = fees_cfg["commission_rate"]

        # 风控参数
        cash_reserve_pct = account.get("cash_reserve_pct", 0.20) if account else 0.20
        max_single_pct = account.get("max_single_position_pct", 0.15) if account else 0.15

        # 总资产 = 持仓市值 + 可用资金
        positions = await self.db.fetchall(
            "SELECT SUM(market_value) as total_mv FROM stock_positions WHERE account_id = ?",
            (self.account_id,)
        )
        current_mv = positions[0]["total_mv"] if positions and positions[0]["total_mv"] else 0
        total_assets = current_mv + available_cash

        # 可用资金上限（扣除保留现金）
        usable_cash = available_cash * (1 - cash_reserve_pct)

        fee_rate = commission_rate + fees_cfg["transfer_fee"]

        # 风控限制：单只最大仓位（基于总资产）
        if total_assets > 0 and max_single_pct > 0:
            risk_limit = int(total_assets * max_single_pct / price)
        else:
            risk_limit = 0

        # 资金上限（考虑手续费）
        if price > 0:
            max_quantity = int((available_cash - fees_cfg["min_commission"]) / (price * (1 + fee_rate)))
        else:
            max_quantity = 0

        # 确定实际买入数量
        if target_quantity and target_quantity > 0:
            # 有目标数量，取三者最小值
            quantity = min(target_quantity, risk_limit, max_quantity) if risk_limit > 0 else min(target_quantity, max_quantity)
        else:
            # 无目标数量，取风控和资金上限的较小值
            quantity = min(risk_limit, max_quantity) if risk_limit > 0 else max_quantity

        # 必须是 100 的整数倍（A 股一手=100 股）
        quantity = (quantity // 100) * 100

        # 计算费用
        if quantity > 0:
            fees = self._calculate_fees(stock_code, price, quantity, "buy", commission_rate)
        else:
            fees = {"commission": 0, "transfer_fee": 0, "stamp_tax": 0, "total_fee": 0}

        total_amount = price * quantity + fees["total_fee"]

        return quantity, total_amount, fees

    async def calculate_sell_quantity(
        self,
        stock_code: str,
        price: float,
        target_quantity: Optional[int] = None
    ) -> Tuple[int, float, Dict]:
        """
        计算卖出数量

        Returns:
            (可卖数量，净得金额，费用明细)
        """
        position = await self.get_position(stock_code)

        if not position:
            return 0, 0.0, {"commission": 0, "transfer_fee": 0, "stamp_tax": 0, "total_fee": 0}

        available_quantity = position.get("available_quantity", 0)

        # 确定实际卖出数量
        if target_quantity:
            quantity = min(target_quantity, available_quantity)
        else:
            quantity = available_quantity

        # 计算费用
        if quantity > 0:
            fees_cfg = await self._get_fee_config()
            fees = self._calculate_fees(stock_code, price, quantity, "sell", fees_cfg["commission_rate"])
        else:
            fees = {"commission": 0, "transfer_fee": 0, "stamp_tax": 0, "total_fee": 0}

        # 卖出净得 = 价格 × 数量 - 费用
        net_amount = price * quantity - fees["total_fee"]

        return quantity, net_amount, fees

    def _calculate_fees(
        self,
        stock_code: str,
        price: float,
        quantity: int,
        trade_type: str,
        commission_rate: Optional[float] = None,
    ) -> Dict[str, float]:
        """
        计算交易费用（从账户配置读取费率）

        Args:
            stock_code: 股票代码
            price: 价格
            quantity: 数量
            trade_type: buy 或 sell
            commission_rate: 佣金费率（可选，不传则使用 fee config 缓存）
        """
        if commission_rate is not None:
            fees_cfg = self._fee_cache or DEFAULT_FEE_CONFIG
        else:
            fees_cfg = self._fee_cache or DEFAULT_FEE_CONFIG
        commission_rate = commission_rate if commission_rate is not None else fees_cfg["commission_rate"]
        stamp_tax = fees_cfg["stamp_tax"]
        transfer_fee_rate = fees_cfg["transfer_fee"]
        min_commission = fees_cfg["min_commission"]

        total_amount = price * quantity

        # 佣金（买卖双向收取，最低 5 元）
        commission = max(total_amount * commission_rate, min_commission)

        # 过户费（仅沪市，买卖双向收取）
        transfer_fee = 0
        if stock_code.startswith("6") or stock_code.startswith("000"):
            transfer_fee = total_amount * transfer_fee_rate

        # 印花税（只收卖出）
        stamp = 0
        if trade_type == "sell":
            stamp = total_amount * stamp_tax

        total_fee = commission + transfer_fee + stamp

        return {
            "commission": round(commission, 2),
            "transfer_fee": round(transfer_fee, 4),
            "stamp_tax": round(stamp, 2),
            "total_fee": round(total_fee, 2)
        }

    async def execute_buy(
        self,
        stock_code: str,
        stock_name: str,
        price: float,
        target_quantity: Optional[int] = None,
        order_id: Optional[str] = None,
        trigger_source: Optional[str] = None,
        strategy_id: Optional[int] = None,
        signal_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        执行买入交易（订单状态机 + 原子事务 + 风控检查）

        订单流转：pending → submitted → filled
        若任何环节失败，订单标记为 rejected

        Returns:
            交易结果
        """
        # 价格校验
        if price <= 0:
            return {
                "success": False,
                "message": f"无效价格: {price}，拒绝买入",
                "quantity": 0,
                "fees": {"commission": 0, "transfer_fee": 0, "stamp_tax": 0, "total_fee": 0}
            }

        # 计算可买数量和费用
        quantity, total_amount, fees = await self.calculate_buy_quantity(
            stock_code, price, target_quantity
        )

        if quantity <= 0:
            return {
                "success": False,
                "message": "可用资金不足",
                "quantity": 0,
                "fees": {"commission": 0, "transfer_fee": 0, "stamp_tax": 0, "total_fee": 0}
            }

        # 创建订单（pending 状态）
        from services.trading.order_service import get_order_service
        order_svc = get_order_service(self.account_id)
        db_order_id = await order_svc.create_order(
            stock_code=stock_code,
            stock_name=stock_name,
            trade_type="buy",
            price=price,
            quantity=quantity,
            trigger_source=trigger_source,
            stop_loss_price=None,
            take_profit_price=None,
        )

        # 风控检查
        try:
            from services.trading.risk_service import get_risk_service
            risk = get_risk_service(self.account_id)
            passed, reason = await risk.check_buy(stock_code, price, quantity)
            if not passed:
                await order_svc.update_status(db_order_id, "rejected", reject_reason=reason)
                return {
                    "success": False,
                    "message": f"风控拦截: {reason}",
                    "quantity": 0,
                    "fees": {"commission": 0, "transfer_fee": 0, "stamp_tax": 0, "total_fee": 0}
                }

            # 策略级仓位上限检查
            if strategy_id:
                passed, reason = await risk.check_strategy_position(stock_code, price, quantity, strategy_id)
                if not passed:
                    await order_svc.update_status(db_order_id, "rejected", reject_reason=reason)
                    return {
                        "success": False,
                        "message": f"策略仓位超限: {reason}",
                        "quantity": 0,
                        "fees": {"commission": 0, "transfer_fee": 0, "stamp_tax": 0, "total_fee": 0}
                    }
        except Exception as e:
            print(f"[RiskService] 风控检查异常: {e}，放行")

        # 更新订单为 submitted
        await order_svc.update_status(db_order_id, "submitted")

        # 通过网关下单（mock 模式直接返回成功，实盘调用券商 SDK）
        try:
            from services.trading.gateway import get_gateway
            gateway = await get_gateway()
            order_result = await gateway.buy(
                stock_code=stock_code,
                price=price,
                quantity=quantity,
                account_id=self.account_id,
            )
            if not order_result.success:
                await order_svc.update_status(db_order_id, "rejected", reject_reason=order_result.message)
                return {
                    "success": False,
                    "message": f"网关下单失败: {order_result.message}",
                    "quantity": 0,
                    "fees": fees
                }
        except Exception as e:
            await order_svc.update_status(db_order_id, "rejected", reject_reason=str(e))
            return {
                "success": False,
                "message": f"网关调用异常: {e}",
                "quantity": 0,
                "fees": fees
            }

        # 获取账户信息
        account = await self.get_account_info()
        current_cash = account.get("available_cash", 0)
        user_id = account.get("id")

        # 原子事务：资金扣减 + 持仓更新 + 交易记录 + 订单完成
        try:
            async with self.db.transaction() as conn:
                # 1. 更新账户资金
                await conn.execute(
                    """UPDATE accounts
                       SET available_cash = ?, updated_at = ?
                       WHERE account_id = ?""",
                    (
                        current_cash - total_amount,
                        format_china_time(),
                        self.account_id
                    )
                )

                # 2. 插入或更新持仓
                existing_position = await self.get_position(stock_code)
                if existing_position:
                    old_qty = existing_position.get("quantity", 0)
                    old_cost = existing_position.get("avg_cost", 0)
                    old_available = existing_position.get("available_quantity", 0)
                    new_qty = old_qty + quantity
                    # 加权成本 = (旧持仓总成本 + 本次含费总成本) / 新持仓数量
                    new_cost = (old_qty * old_cost + total_amount) / new_qty if new_qty > 0 else price
                    new_available = old_available

                    await conn.execute(
                        """UPDATE stock_positions
                           SET quantity = ?, avg_cost = ?, available_quantity = ?, market_value = ?,
                               updated_at = ?, strategy_id = COALESCE(?, strategy_id)
                           WHERE account_id = ? AND stock_code = ?""",
                        (new_qty, new_cost, new_available, new_qty * price,
                         format_china_time(), strategy_id, self.account_id, stock_code)
                    )
                else:
                    await conn.execute(
                        """INSERT INTO stock_positions
                           (account_id, user_id, stock_code, stock_name, quantity, available_quantity,
                            avg_cost, market_value, strategy_id, created_at, updated_at)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (self.account_id, user_id, stock_code, stock_name, quantity, 0, price,
                         quantity * price, strategy_id, format_china_time(), format_china_time())
                    )

                # 3. 记录交易
                trade_data = {
                    "account_id": self.account_id,
                    "user_id": user_id,
                    "order_id": order_id or f"BUY_{get_china_time().strftime('%H%M%S')}",
                    "stock_code": stock_code,
                    "stock_name": stock_name,
                    "trade_type": "buy",
                    "quantity": quantity,
                    "price": price,
                    "amount": total_amount - fees["total_fee"],
                    "commission": fees["commission"],
                    "trade_time": format_china_time(),
                    "status": "completed",
                    "trigger_source": trigger_source,
                    "strategy_id": strategy_id,
                    "signal_id": signal_id,
                    "created_at": format_china_time()
                }
                columns = ', '.join(trade_data.keys())
                placeholders = ', '.join(['?' for _ in trade_data])
                cursor = await conn.execute(
                    f"INSERT INTO trade_records ({columns}) VALUES ({placeholders})",
                    tuple(trade_data.values())
                )
                trade_record_id = cursor.lastrowid

                # 4. 更新 watchlist 标记已买入
                if signal_id:
                    await conn.execute(
                        "UPDATE watchlist SET status = 'bought', bought = 1, buy_trade_id = ? WHERE id = ?",
                        (trade_record_id, signal_id)
                    )
                else:
                    await conn.execute(
                        "UPDATE watchlist SET status = 'bought', bought = 1 WHERE account_id = ? AND stock_code = ?",
                        (self.account_id, stock_code)
                    )

                # 5. 更新订单状态为 filled + 关联券商委托号
                broker_order_id = order_result.order_id if hasattr(order_result, 'order_id') else None
                await conn.execute(
                    "UPDATE orders SET status = 'filled', filled_quantity = ?, filled_amount = ?, broker_order_id = ?, updated_at = ? WHERE id = ?",
                    (quantity, total_amount, broker_order_id, format_china_time(), db_order_id)
                )

            return {
                "success": True,
                "message": "买入成功",
                "quantity": quantity,
                "price": price,
                "total_amount": total_amount,
                "fees": fees,
                "trade_record_id": trade_record_id,
                "order_id": db_order_id,
            }

        except Exception as e:
            # 事务已回滚，标记订单为 rejected
            try:
                await order_svc.update_status(db_order_id, "rejected", reject_reason=str(e))
            except Exception:
                pass
            return {
                "success": False,
                "message": f"买入执行失败: {e}",
                "quantity": 0,
                "fees": fees
            }

    async def execute_sell(
        self,
        stock_code: str,
        stock_name: str,
        price: float,
        target_quantity: Optional[int] = None,
        order_id: Optional[str] = None,
        trigger_source: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        执行卖出交易（订单状态机 + 原子事务）

        Returns:
            交易结果
        """
        # 价格校验（防止 price=0 导致止损误卖）
        if price <= 0:
            return {
                "success": False,
                "message": f"无效价格: {price}，拒绝卖出",
            }

        # 创建订单（pending 状态）
        from services.trading.order_service import get_order_service
        order_svc = get_order_service(self.account_id)
        db_order_id = await order_svc.create_order(
            stock_code=stock_code,
            stock_name=stock_name,
            trade_type="sell",
            price=price,
            quantity=target_quantity or 0,
            trigger_source=trigger_source,
        )

        # 计算可卖数量和费用
        quantity, net_amount, fees = await self.calculate_sell_quantity(
            stock_code, price, target_quantity
        )

        if quantity <= 0:
            await order_svc.update_status(db_order_id, "rejected", reject_reason="持仓不足")
            return {
                "success": False,
                "message": "持仓不足",
                "quantity": 0,
                "fees": fees
            }

        # 更新订单为 submitted
        await order_svc.update_status(db_order_id, "submitted")

        # 通过网关下单（mock 模式直接返回成功，实盘调用券商 SDK）
        try:
            from services.trading.gateway import get_gateway
            gateway = await get_gateway()
            order_result = await gateway.sell(
                stock_code=stock_code,
                price=price,
                quantity=quantity,
                account_id=self.account_id,
            )
            if not order_result.success:
                await order_svc.update_status(db_order_id, "rejected", reject_reason=order_result.message)
                return {
                    "success": False,
                    "message": f"网关下单失败: {order_result.message}",
                    "quantity": 0,
                    "fees": fees
                }
        except Exception as e:
            await order_svc.update_status(db_order_id, "rejected", reject_reason=str(e))
            return {
                "success": False,
                "message": f"网关调用异常: {e}",
                "quantity": 0,
                "fees": fees
            }

        # 获取持仓信息
        position = await self.get_position(stock_code)
        avg_cost = position.get("avg_cost", price)
        profit_loss = (price - avg_cost) * quantity

        old_qty = position.get("quantity", 0)
        old_available = position.get("available_quantity", 0)
        new_qty = old_qty - quantity
        new_available = old_available - quantity

        # 获取账户信息
        account = await self.get_account_info()
        current_cash = account.get("available_cash", 0)
        user_id = account.get("id")

        # 原子事务：持仓更新 + 资金增加 + 交易记录 + 订单完成
        try:
            async with self.db.transaction() as conn:
                # 1. 更新持仓
                if new_qty <= 0:
                    await conn.execute(
                        "DELETE FROM stock_positions WHERE account_id = ? AND stock_code = ?",
                        (self.account_id, stock_code)
                    )
                else:
                    # 盈亏摊薄成本法：卖出盈亏摊入剩余持仓
                    # 新 avg_cost = (原总成本 - 卖出净收入) / 剩余数量
                    old_total_cost = old_qty * avg_cost
                    new_total_cost = old_total_cost - net_amount
                    new_cost = new_total_cost / new_qty if new_qty > 0 else avg_cost

                    await conn.execute(
                        """UPDATE stock_positions
                           SET quantity = ?, available_quantity = ?, avg_cost = ?,
                               market_value = ?, updated_at = ?
                           WHERE account_id = ? AND stock_code = ?""",
                        (new_qty, new_available, new_cost, new_qty * price, format_china_time(),
                         self.account_id, stock_code)
                    )

                # 2. 更新账户资金
                await conn.execute(
                    """UPDATE accounts
                       SET available_cash = ?, updated_at = ?
                       WHERE account_id = ?""",
                    (
                        current_cash + net_amount,
                        format_china_time(),
                        self.account_id
                    )
                )

                # 3. 记录交易
                buy_record = await self.db.fetchone(
                    "SELECT strategy_id, signal_id FROM trade_records WHERE account_id = ? AND stock_code = ? AND trade_type = 'buy' ORDER BY trade_time DESC LIMIT 1",
                    (self.account_id, stock_code)
                )
                inherited_strategy_id = buy_record["strategy_id"] if buy_record and buy_record.get("strategy_id") else None
                inherited_signal_id = buy_record["signal_id"] if buy_record and buy_record.get("signal_id") else None

                # 在事务内执行插入
                trade_data = {
                    "account_id": self.account_id,
                    "user_id": user_id,
                    "order_id": order_id or f"SELL_{get_china_time().strftime('%H%M%S')}",
                    "stock_code": stock_code,
                    "stock_name": stock_name,
                    "trade_type": "sell",
                    "quantity": quantity,
                    "price": price,
                    "amount": net_amount + fees["total_fee"],
                    "commission": fees["commission"],
                    "profit_loss": profit_loss,
                    "trade_time": format_china_time(),
                    "status": "completed",
                    "trigger_source": trigger_source,
                    "strategy_id": inherited_strategy_id,
                    "signal_id": inherited_signal_id,
                    "created_at": format_china_time()
                }
                columns = ', '.join(trade_data.keys())
                placeholders = ', '.join(['?' for _ in trade_data])
                cursor = await conn.execute(
                    f"INSERT INTO trade_records ({columns}) VALUES ({placeholders})",
                    tuple(trade_data.values())
                )
                trade_record_id = cursor.lastrowid

                # 4. 更新订单状态为 filled + 关联券商委托号
                broker_order_id = order_result.order_id if hasattr(order_result, 'order_id') else None
                await conn.execute(
                    "UPDATE orders SET status = 'filled', filled_quantity = ?, filled_amount = ?, broker_order_id = ?, updated_at = ? WHERE id = ?",
                    (quantity, net_amount + fees["total_fee"], broker_order_id, format_china_time(), db_order_id)
                )

            return {
                "success": True,
                "message": "卖出成功",
                "quantity": quantity,
                "price": price,
                "net_amount": net_amount,
                "fees": fees,
                "profit_loss": profit_loss,
                "trade_record_id": trade_record_id,
                "order_id": db_order_id,
            }

        except Exception as e:
            # 事务已回滚，标记订单为 rejected
            try:
                await order_svc.update_status(db_order_id, "rejected", reject_reason=str(e))
            except Exception:
                pass
            return {
                "success": False,
                "message": f"卖出执行失败: {e}",
                "quantity": 0,
                "fees": fees
            }

    async def unfreeze_positions(self):
        """
        解冻昨日买入的持仓（T+1 规则）
        在每个交易日开盘前调用，将 available_quantity 设置为等于 quantity
        """
        await self.db.execute(
            """UPDATE stock_positions
               SET available_quantity = quantity, updated_at = ?
               WHERE account_id = ? AND available_quantity < quantity""",
            (format_china_time(), self.account_id)
        )
        print(f"[T+1] 已解冻账户 {self.account_id} 的所有持仓")


# 工厂函数
def get_trade_execution_service(account_id: str) -> TradeExecutionService:
    """获取交易执行服务实例"""
    return TradeExecutionService(account_id)
