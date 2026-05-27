"""
策略现金服务 (Strategy Cash Service)

虚拟账户模型：
- 策略资产 = strategy_cash（现金余额） + 持仓市值
- 策略总资产上限 = allocated_capital
- 买入扣减 strategy_cash，卖出增加 strategy_cash

特殊场景：手动买入借用策略现金时，记录 strategy_cash_borrows
"""

from typing import Dict, List, Optional, Tuple
from services.common.database import get_db_manager
from services.common.timezone import get_china_time, format_china_time
from services.common.structured_logger import get_logger


logger = get_logger("strategy_cash")


class StrategyCashService:
    """策略现金服务"""

    def __init__(self, account_id: str):
        self.account_id = account_id
        self.db = get_db_manager()

    async def get_strategy_total_asset(self, strategy_id: int) -> Dict:
        """
        获取策略总资产

        Returns:
            {
                strategy_id: int,
                allocated_capital: float,  # 分配上限
                strategy_cash: float,      # 现金余额
                positions_mv: float,       # 持仓市值
                total_asset: float,        # 总资产 = cash + positions_mv
                available: float,          # 可用额度 = allocated - total_asset
            }
        """
        strategy = await self.db.fetchone(
            "SELECT id, name, allocated_capital, strategy_cash FROM strategies WHERE id = ? AND account_id = ?",
            (strategy_id, self.account_id)
        )
        if not strategy:
            return None

        allocated = strategy.get("allocated_capital") or 0
        cash = strategy.get("strategy_cash") or 0

        # 计算持仓市值
        positions = await self.db.fetchall(
            "SELECT SUM(market_value) as total_mv FROM stock_positions WHERE account_id = ? AND strategy_id = ? AND quantity > 0",
            (self.account_id, strategy_id)
        )
        positions_mv = positions[0]["total_mv"] if positions and positions[0]["total_mv"] else 0

        total_asset = cash + positions_mv
        available = allocated - total_asset

        return {
            "strategy_id": strategy_id,
            "name": strategy.get("name"),
            "allocated_capital": allocated,
            "strategy_cash": cash,
            "positions_mv": positions_mv,
            "total_asset": total_asset,
            "available": available,
        }

    async def get_all_strategies_summary(self) -> List[Dict]:
        """
        获取所有策略的资金汇总

        Returns:
            [
                {strategy_id, name, allocated_capital, strategy_cash, positions_mv, total_asset, available},
                ...
            ]
        """
        strategies = await self.db.fetchall(
            "SELECT id, name, allocated_capital, strategy_cash FROM strategies WHERE account_id = ? AND is_active = 1",
            (self.account_id,)
        )
        summaries = []
        for s in strategies:
            summary = await self.get_strategy_total_asset(s["id"])
            if summary:
                summaries.append(summary)
        return summaries

    async def check_strategy_buy(self, strategy_id: int, amount: float) -> Tuple[bool, str]:
        """
        策略买入前检查

        Args:
            strategy_id: 策略ID
            amount: 买入金额（含费用）

        Returns:
            (passed, reason)
        """
        strategy = await self.db.fetchone(
            "SELECT strategy_cash FROM strategies WHERE id = ? AND account_id = ?",
            (strategy_id, self.account_id)
        )
        if not strategy:
            return False, "策略不存在"

        cash = strategy.get("strategy_cash") or 0

        # 策略买入只需检查现金够不够（allocated_capital 是总资产上限，买入不单独检查）
        if cash < amount:
            return False, f"策略现金不足：需 ¥{amount:.2f}，现有 ¥{cash:.2f}"

        return True, "检查通过"

    async def execute_buy_cash_change(self, strategy_id: int, amount: float,
                                        stock_code: str, trade_record_id: int) -> bool:
        """
        策略买入后扣减现金

        Args:
            strategy_id: 策略ID
            amount: 扣减金额（买入金额 + 费用）
            stock_code: 股票代码
            trade_record_id: 交易记录ID

        Returns:
            success
        """
        # 获取当前现金
        strategy = await self.db.fetchone(
            "SELECT strategy_cash FROM strategies WHERE id = ? AND account_id = ?",
            (strategy_id, self.account_id)
        )
        if not strategy:
            logger.error("strategy_cash_buy", f"策略不存在: {strategy_id}")
            return False

        balance_before = strategy.get("strategy_cash") or 0
        balance_after = balance_before - amount

        # 更新策略现金
        await self.db.execute(
            "UPDATE strategies SET strategy_cash = ?, updated_at = ? WHERE id = ? AND account_id = ?",
            (balance_after, format_china_time(), strategy_id, self.account_id)
        )

        # 记录交易
        await self.db.insert("strategy_cash_transactions", {
            "account_id": self.account_id,
            "strategy_id": strategy_id,
            "transaction_type": "buy_deduct",
            "amount": -amount,
            "stock_code": stock_code,
            "trade_record_id": trade_record_id,
            "balance_before": balance_before,
            "balance_after": balance_after,
            "reason": f"买入 {stock_code}",
            "created_at": format_china_time(),
        })

        logger.log_event("strategy_cash_buy", f"策略 #{strategy_id} 买入扣减 ¥{amount:.2f}",
                         strategy_id=strategy_id, stock_code=stock_code,
                         balance_before=balance_before, balance_after=balance_after)

        return True

    async def execute_sell_cash_change(self, strategy_id: int, amount: float,
                                         stock_code: str, trade_record_id: int) -> bool:
        """
        策略卖出后增加现金

        Args:
            strategy_id: 策略ID
            amount: 增加金额（卖出净金额，扣除费用）
            stock_code: 股票代码
            trade_record_id: 交易记录ID

        Returns:
            success
        """
        strategy = await self.db.fetchone(
            "SELECT strategy_cash FROM strategies WHERE id = ? AND account_id = ?",
            (strategy_id, self.account_id)
        )
        if not strategy:
            logger.error("strategy_cash_sell", f"策略不存在: {strategy_id}")
            return False

        balance_before = strategy.get("strategy_cash") or 0
        balance_after = balance_before + amount

        # 更新策略现金
        await self.db.execute(
            "UPDATE strategies SET strategy_cash = ?, updated_at = ? WHERE id = ? AND account_id = ?",
            (balance_after, format_china_time(), strategy_id, self.account_id)
        )

        # 记录交易
        await self.db.insert("strategy_cash_transactions", {
            "account_id": self.account_id,
            "strategy_id": strategy_id,
            "transaction_type": "sell_add",
            "amount": amount,
            "stock_code": stock_code,
            "trade_record_id": trade_record_id,
            "balance_before": balance_before,
            "balance_after": balance_after,
            "reason": f"卖出 {stock_code}",
            "created_at": format_china_time(),
        })

        logger.log_event("strategy_cash_sell", f"策略 #{strategy_id} 卖出增加 ¥{amount:.2f}",
                         strategy_id=strategy_id, stock_code=stock_code,
                         balance_before=balance_before, balance_after=balance_after)

        return True

    async def borrow_cash(self, strategy_id: int, amount: float,
                          trade_record_id: int) -> Tuple[bool, str]:
        """
        手动买入借用策略现金

        Args:
            strategy_id: 被借用的策略ID
            amount: 借用金额
            trade_record_id: 手动交易记录ID

        Returns:
            (success, reason)
        """
        summary = await self.get_strategy_total_asset(strategy_id)
        if not summary:
            return False, "策略不存在"

        cash = summary["strategy_cash"]

        # 检查策略现金是否够借
        if cash < amount:
            return False, f"策略现金不足：需 ¥{amount:.2f}，现有 ¥{cash:.2f}"

        balance_before = cash
        balance_after = balance_before - amount

        # 更新策略现金
        await self.db.execute(
            "UPDATE strategies SET strategy_cash = ?, updated_at = ? WHERE id = ? AND account_id = ?",
            (balance_after, format_china_time(), strategy_id, self.account_id)
        )

        # 记录借用
        await self.db.insert("strategy_cash_borrows", {
            "account_id": self.account_id,
            "strategy_id": strategy_id,
            "borrow_amount": amount,
            "trade_record_id": trade_record_id,
            "status": "borrowed",
            "returned_amount": 0,
            "created_at": format_china_time(),
        })

        # 记录交易
        await self.db.insert("strategy_cash_transactions", {
            "account_id": self.account_id,
            "strategy_id": strategy_id,
            "transaction_type": "borrow",
            "amount": -amount,
            "stock_code": None,
            "trade_record_id": trade_record_id,
            "balance_before": balance_before,
            "balance_after": balance_after,
            "reason": "手动买入借用",
            "created_at": format_china_time(),
        })

        logger.log_event("strategy_cash_borrow", f"策略 #{strategy_id} 借出 ¥{amount:.2f}",
                         strategy_id=strategy_id, trade_record_id=trade_record_id,
                         balance_before=balance_before, balance_after=balance_after)

        return True, "借用成功"

    async def get_borrow_records(self, strategy_id: Optional[int] = None,
                                  status: Optional[str] = None) -> List[Dict]:
        """
        获取借用记录列表
        """
        conditions = ["account_id = ?"]
        params = [self.account_id]

        if strategy_id:
            conditions.append("strategy_id = ?")
            params.append(strategy_id)

        if status:
            conditions.append("status = ?")
            params.append(status)

        where_clause = " AND ".join(conditions)
        rows = await self.db.fetchall(
            f"SELECT * FROM strategy_cash_borrows WHERE {where_clause} ORDER BY created_at DESC LIMIT 50",
            tuple(params)
        )
        return [dict(r) for r in rows]

    async def return_cash(self, borrow_id: int, amount: float) -> Tuple[bool, str]:
        """
        归还借用的现金

        Args:
            borrow_id: 借用记录ID
            amount: 归还金额

        Returns:
            (success, reason)
        """
        borrow = await self.db.fetchone(
            "SELECT * FROM strategy_cash_borrows WHERE id = ? AND account_id = ?",
            (borrow_id, self.account_id)
        )
        if not borrow:
            return False, "借用记录不存在"

        if borrow["status"] != "borrowed":
            return False, "该记录已归还"

        strategy_id = borrow["strategy_id"]
        remaining = borrow["borrow_amount"] - borrow["returned_amount"]
        if amount > remaining:
            return False, f"归还金额超过剩余：剩余 ¥{remaining:.2f}"

        # 更新借用记录
        new_returned = borrow["returned_amount"] + amount
        new_status = "returned" if new_returned >= borrow["borrow_amount"] else "borrowed"

        await self.db.execute(
            "UPDATE strategy_cash_borrows SET returned_amount = ?, status = ?, returned_at = ? WHERE id = ?",
            (new_returned, new_status, format_china_time() if new_status == "returned" else None, borrow_id)
        )

        # 更新策略现金
        strategy = await self.db.fetchone(
            "SELECT strategy_cash FROM strategies WHERE id = ? AND account_id = ?",
            (strategy_id, self.account_id)
        )
        balance_before = strategy.get("strategy_cash") or 0
        balance_after = balance_before + amount

        await self.db.execute(
            "UPDATE strategies SET strategy_cash = ?, updated_at = ? WHERE id = ? AND account_id = ?",
            (balance_after, format_china_time(), strategy_id, self.account_id)
        )

        # 记录交易
        await self.db.insert("strategy_cash_transactions", {
            "account_id": self.account_id,
            "strategy_id": strategy_id,
            "transaction_type": "return",
            "amount": amount,
            "stock_code": None,
            "trade_record_id": None,
            "balance_before": balance_before,
            "balance_after": balance_after,
            "reason": f"归还借用 #{borrow_id}",
            "created_at": format_china_time(),
        })

        logger.log_event("strategy_cash_return", f"策略 #{strategy_id} 归还 ¥{amount:.2f}",
                         strategy_id=strategy_id, borrow_id=borrow_id,
                         balance_before=balance_before, balance_after=balance_after)

        return True, "归还成功"

    async def adjust_allocation(self, strategy_id: int, new_allocated: float) -> Tuple[bool, str]:
        """
        调整策略分配上限

        Args:
            strategy_id: 策略ID
            new_allocated: 新分配上限

        Returns:
            (success, reason)
        """
        summary = await self.get_strategy_total_asset(strategy_id)
        if not summary:
            return False, "策略不存在"

        old_allocated = summary["allocated_capital"]

        # 减少：检查是否超限
        if new_allocated < old_allocated:
            if summary["total_asset"] > new_allocated:
                # 超限：可以选择接受（可用额度变负）或拒绝
                # 当前实现：接受，可用额度变负
                logger.log_event("strategy_alloc_reduce",
                                 f"策略 #{strategy_id} 分配上限减少后超限：总资产 ¥{summary['total_asset']:.2f} > 新上限 ¥{new_allocated:.2f}",
                                 strategy_id=strategy_id)

        # 更新分配上限
        await self.db.execute(
            "UPDATE strategies SET allocated_capital = ?, updated_at = ? WHERE id = ? AND account_id = ?",
            (new_allocated, format_china_time(), strategy_id, self.account_id)
        )

        # 记录交易
        await self.db.insert("strategy_cash_transactions", {
            "account_id": self.account_id,
            "strategy_id": strategy_id,
            "transaction_type": "allocation_adjust",
            "amount": new_allocated - old_allocated,
            "stock_code": None,
            "trade_record_id": None,
            "balance_before": old_allocated,
            "balance_after": new_allocated,
            "reason": "分配上限调整",
            "created_at": format_china_time(),
        })

        logger.log_event("strategy_alloc_adjust",
                         f"策略 #{strategy_id} 分配上限调整：¥{old_allocated:.2f} → ¥{new_allocated:.2f}",
                         strategy_id=strategy_id)

        return True, "调整成功"

    async def recalculate_strategy_cash(self, strategy_id: int) -> float:
        """
        从交易记录重新计算策略现金余额（数据修复）

        Returns:
            计算后的现金余额
        """
        # 获取策略所有买入记录的总金额
        buys = await self.db.fetchall(
            "SELECT SUM(amount + commission) as total_buy FROM trade_records WHERE account_id = ? AND strategy_id = ? AND trade_type = 'buy'",
            (self.account_id, strategy_id)
        )
        total_buy = buys[0]["total_buy"] if buys and buys[0]["total_buy"] else 0

        # 获取策略所有卖出记录的净金额
        sells = await self.db.fetchall(
            "SELECT SUM(amount - commission) as total_sell FROM trade_records WHERE account_id = ? AND strategy_id = ? AND trade_type = 'sell'",
            (self.account_id, strategy_id)
        )
        total_sell = sells[0]["total_sell"] if sells and sells[0]["total_sell"] else 0

        # 计算现金余额（假设初始分配 = allocated_capital）
        strategy = await self.db.fetchone(
            "SELECT allocated_capital FROM strategies WHERE id = ? AND account_id = ?",
            (strategy_id, self.account_id)
        )
        allocated = strategy.get("allocated_capital") or 0

        # 现金余额 = 分配上限 - 买入总金额 + 卖出总金额
        calculated_cash = allocated - total_buy + total_sell

        # 更新数据库
        await self.db.execute(
            "UPDATE strategies SET strategy_cash = ?, updated_at = ? WHERE id = ? AND account_id = ?",
            (calculated_cash, format_china_time(), strategy_id, self.account_id)
        )

        logger.log_event("strategy_cash_recalc",
                         f"策略 #{strategy_id} 现金重算：¥{calculated_cash:.2f}",
                         strategy_id=strategy_id, allocated=allocated,
                         total_buy=total_buy, total_sell=total_sell)

        return calculated_cash


# 单例缓存
_services: Dict[str, StrategyCashService] = {}


def get_strategy_cash_service(account_id: str) -> StrategyCashService:
    """获取策略现金服务单例"""
    if account_id not in _services:
        _services[account_id] = StrategyCashService(account_id)
    return _services[account_id]