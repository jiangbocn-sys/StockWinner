"""
交易执行服务 (Trade Execution)
- 管理账户资金
- 计算可买数量
- 执行交易并更新资金
- 计算交易手续费
"""

import asyncio
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Tuple
from services.common.database import get_db_manager

# 中国时区
CHINA_TZ = timezone(timedelta(hours=8))

def get_china_time():
    """获取中国时区时间"""
    return datetime.now(CHINA_TZ).replace(tzinfo=None)

# 手续费率配置（A 股标准）
FEE_CONFIG = {
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

        Returns:
            (可买数量，总金额，费用明细)
        """
        available_cash = await self.get_available_cash()

        # 计算费用
        fees = self._calculate_fees(stock_code, price, target_quantity or 100, "buy")

        # 计算最大可买数量（考虑费用）
        # 总金额 = 价格 × 数量 + 费用
        # 费用 ≈ 价格 × 数量 × (commission + transfer_fee) + min_commission
        fee_rate = FEE_CONFIG["commission"] + FEE_CONFIG["transfer_fee"]

        if price > 0:
            # 估算最大数量：available_cash = price * qty * (1 + fee_rate) + min_commission
            max_quantity = int((available_cash - FEE_CONFIG["min_commission"]) / (price * (1 + fee_rate)))
            # 必须是 100 的整数倍（A 股一手=100 股）
            max_quantity = (max_quantity // 100) * 100
        else:
            max_quantity = 0

        # 确定实际买入数量
        if target_quantity:
            quantity = min(target_quantity, max_quantity)
        else:
            quantity = max_quantity

        # 重新计算实际费用
        if quantity > 0:
            fees = self._calculate_fees(stock_code, price, quantity, "buy")
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
            fees = self._calculate_fees(stock_code, price, quantity, "sell")
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
        trade_type: str
    ) -> Dict[str, float]:
        """
        计算交易费用

        Args:
            stock_code: 股票代码
            price: 价格
            quantity: 数量
            trade_type: buy 或 sell

        Returns:
            费用明细
        """
        total_amount = price * quantity

        # 佣金（买卖双向收取，最低 5 元）
        commission = max(total_amount * FEE_CONFIG["commission"], FEE_CONFIG["min_commission"])

        # 过户费（仅沪市，买卖双向收取）
        transfer_fee = 0
        if stock_code.startswith("6") or stock_code.startswith("000"):
            transfer_fee = total_amount * FEE_CONFIG["transfer_fee"]

        # 印花税（只收卖出）
        stamp_tax = 0
        if trade_type == "sell":
            stamp_tax = total_amount * FEE_CONFIG["stamp_tax"]

        total_fee = commission + transfer_fee + stamp_tax

        return {
            "commission": round(commission, 2),
            "transfer_fee": round(transfer_fee, 4),
            "stamp_tax": round(stamp_tax, 2),
            "total_fee": round(total_fee, 2)
        }

    async def execute_buy(
        self,
        stock_code: str,
        stock_name: str,
        price: float,
        target_quantity: Optional[int] = None,
        order_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        执行买入交易

        Returns:
            交易结果
        """
        # 计算可买数量和费用
        quantity, total_amount, fees = await self.calculate_buy_quantity(
            stock_code, price, target_quantity
        )

        if quantity <= 0:
            return {
                "success": False,
                "message": "可用资金不足",
                "quantity": 0,
                "fees": fees
            }

        # 获取账户信息
        account = await self.get_account_info()
        current_cash = account.get("available_cash", 0)
        user_id = account.get("id")  # 获取内部用户 ID

        # 更新账户资金（只更新 available_cash）
        await self.db.execute(
            """UPDATE accounts
               SET available_cash = ?, updated_at = ?
               WHERE account_id = ?""",
            (
                current_cash - total_amount,
                get_china_time().isoformat(),
                self.account_id
            )
        )

        # 插入或更新持仓
        existing_position = await self.get_position(stock_code)
        if existing_position:
            # 更新现有持仓 - 加权平均计算成本
            old_qty = existing_position.get("quantity", 0)
            old_cost = existing_position.get("avg_cost", 0)
            old_available = existing_position.get("available_quantity", 0)
            new_qty = old_qty + quantity
            # 计算新的持仓成本（加权平均）
            new_cost = (old_qty * old_cost + quantity * price) / new_qty if new_qty > 0 else price
            # T+1 规则：当日买入的数量不可卖出，available_quantity 保持不变
            new_available = old_available

            await self.db.execute(
                """UPDATE stock_positions
                   SET quantity = ?, avg_cost = ?, available_quantity = ?, market_value = ?,
                       updated_at = ?
                   WHERE account_id = ? AND stock_code = ?""",
                (new_qty, new_cost, new_available, new_qty * price, get_china_time().isoformat(),
                 self.account_id, stock_code)
            )
        else:
            # 插入新持仓 - T+1 规则：当日买入，available_quantity 为 0，次日解冻
            await self.db.execute(
                """INSERT INTO stock_positions
                   (account_id, user_id, stock_code, stock_name, quantity, available_quantity, avg_cost, market_value, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (self.account_id, user_id, stock_code, stock_name, quantity, 0, price,
                 quantity * price, get_china_time().isoformat(), get_china_time().isoformat())
            )

        # 记录交易记录
        trade_record_id = await self._insert_trade_record(
            order_id=order_id or f"BUY_{get_china_time().strftime('%H%M%S')}",
            stock_code=stock_code,
            stock_name=stock_name,
            trade_type="buy",
            quantity=quantity,
            price=price,
            amount=total_amount - fees["total_fee"],  # 净买入金额（不含费用）
            commission=fees["commission"],
            status="completed"
        )

        return {
            "success": True,
            "message": "买入成功",
            "quantity": quantity,
            "price": price,
            "total_amount": total_amount,
            "fees": fees,
            "trade_record_id": trade_record_id
        }

    async def execute_sell(
        self,
        stock_code: str,
        stock_name: str,
        price: float,
        target_quantity: Optional[int] = None,
        order_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        执行卖出交易

        Returns:
            交易结果
        """
        # 计算可卖数量和费用
        quantity, net_amount, fees = await self.calculate_sell_quantity(
            stock_code, price, target_quantity
        )

        if quantity <= 0:
            return {
                "success": False,
                "message": "持仓不足",
                "quantity": 0,
                "fees": fees
            }

        # 获取持仓信息
        position = await self.get_position(stock_code)
        avg_cost = position.get("avg_cost", price)
        profit_loss = (price - avg_cost) * quantity  # 盈亏

        # 更新持仓
        old_qty = position.get("quantity", 0)
        old_available = position.get("available_quantity", 0)
        new_qty = old_qty - quantity
        new_available = old_available - quantity

        if new_qty <= 0:
            # 清空持仓
            await self.db.execute(
                "DELETE FROM stock_positions WHERE account_id = ? AND stock_code = ?",
                (self.account_id, stock_code)
            )
        else:
            # 更新持仓数量和可用数量
            await self.db.execute(
                """UPDATE stock_positions
                   SET quantity = ?, available_quantity = ?, market_value = ?, updated_at = ?
                   WHERE account_id = ? AND stock_code = ?""",
                (new_qty, new_available, new_qty * price, get_china_time().isoformat(),
                 self.account_id, stock_code)
            )

        # 更新账户资金（卖出后资金增加，只更新 available_cash）
        account = await self.get_account_info()
        current_cash = account.get("available_cash", 0)

        await self.db.execute(
            """UPDATE accounts
               SET available_cash = ?, updated_at = ?
               WHERE account_id = ?""",
            (
                current_cash + net_amount,
                get_china_time().isoformat(),
                self.account_id
            )
        )

        # 记录交易记录
        trade_record_id = await self._insert_trade_record(
            order_id=order_id or f"SELL_{get_china_time().strftime('%H%M%S')}",
            stock_code=stock_code,
            stock_name=stock_name,
            trade_type="sell",
            quantity=quantity,
            price=price,
            amount=net_amount + fees["total_fee"],  # 卖出总额（含费用）
            commission=fees["commission"],
            profit_loss=profit_loss,
            status="completed"
        )

        return {
            "success": True,
            "message": "卖出成功",
            "quantity": quantity,
            "price": price,
            "net_amount": net_amount,
            "fees": fees,
            "profit_loss": profit_loss,
            "trade_record_id": trade_record_id
        }

    async def _insert_trade_record(
        self,
        order_id: str,
        stock_code: str,
        stock_name: str,
        trade_type: str,
        quantity: int,
        price: float,
        amount: float,
        commission: float,
        profit_loss: Optional[float] = None,
        status: str = "completed"
    ) -> int:
        """插入交易记录"""
        # 获取 user_id
        account = await self.get_account_info()
        user_id = account.get("id")

        trade_data = {
            "account_id": self.account_id,
            "user_id": user_id,
            "order_id": order_id,
            "stock_code": stock_code,
            "stock_name": stock_name,
            "trade_type": trade_type,
            "quantity": quantity,
            "price": price,
            "amount": amount,
            "commission": commission,
            "profit_loss": profit_loss,
            "trade_time": get_china_time().isoformat(),
            "status": status,
            "created_at": get_china_time().isoformat()
        }

        # 移除 None 值
        trade_data = {k: v for k, v in trade_data.items() if v is not None}

        return await self.db.insert("trade_records", trade_data)

    async def unfreeze_positions(self):
        """
        解冻昨日买入的持仓（T+1 规则）
        在每个交易日开盘前调用，将 available_quantity 设置为等于 quantity
        """
        await self.db.execute(
            """UPDATE stock_positions
               SET available_quantity = quantity, updated_at = ?
               WHERE account_id = ? AND available_quantity < quantity""",
            (get_china_time().isoformat(), self.account_id)
        )
        print(f"[T+1] 已解冻账户 {self.account_id} 的所有持仓")


# 工厂函数
def get_trade_execution_service(account_id: str) -> TradeExecutionService:
    """获取交易执行服务实例"""
    return TradeExecutionService(account_id)
