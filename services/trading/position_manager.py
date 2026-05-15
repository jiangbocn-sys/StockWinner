"""
持仓管理服务 (Position Manager)

负责：
1. 启动时持仓对账（DB 持仓 vs 券商持仓）
2. 实时盈亏刷新
3. T+1 自动解冻
"""

from typing import Dict, List, Optional
from services.common.database import get_db_manager
from services.common.timezone import get_china_time


class PositionManager:
    """持仓管理服务"""

    def __init__(self, account_id: str):
        self.account_id = account_id
        self.db = get_db_manager()

    async def reconcile_positions(
        self,
        broker_positions: List[Dict],
    ) -> Dict:
        """
        持仓对账：将 DB 持仓与券商持仓对比

        Args:
            broker_positions: 券商返回的持仓列表，每项包含
                {stock_code, stock_name, quantity, avg_cost, current_price}

        Returns:
            {matched, created, deleted, discrepancy}
        """
        result = {"matched": 0, "created": 0, "deleted": 0, "discrepancy": []}

        # 获取 DB 中所有持仓
        db_positions = await self.db.fetchall(
            "SELECT * FROM stock_positions WHERE account_id = ?",
            (self.account_id,),
        )
        db_map = {p["stock_code"]: p for p in db_positions}
        broker_map = {p["stock_code"]: p for p in broker_positions}

        # 比对券商有的股票
        for code, broker_pos in broker_map.items():
            if code in db_map:
                db_pos = db_map[code]
                db_qty = db_pos.get("quantity", 0)
                broker_qty = broker_pos.get("quantity", 0)

                if db_qty != broker_qty:
                    result["discrepancy"].append({
                        "stock_code": code,
                        "db_quantity": db_qty,
                        "broker_quantity": broker_qty,
                    })
                else:
                    result["matched"] += 1

                # 更新券商提供的最新价格
                broker_price = broker_pos.get("current_price", db_pos.get("current_price", 0))
                broker_cost = broker_pos.get("avg_cost", db_pos.get("avg_cost", 0))

                await self.db.execute(
                    """UPDATE stock_positions
                       SET current_price = ?, avg_cost = ?, market_value = ?,
                           profit_loss = (current_price - avg_cost) * quantity,
                           updated_at = ?
                       WHERE account_id = ? AND stock_code = ?""",
                    (
                        broker_price,
                        broker_cost,
                        broker_price * db_qty,
                        get_china_time().isoformat(),
                        self.account_id,
                        code,
                    )
                )
            else:
                # DB 没有但券商有 → 可能是系统崩溃后遗留的持仓，从券商同步创建
                await self.db.execute(
                    """INSERT INTO stock_positions
                       (account_id, stock_code, stock_name, quantity, available_quantity,
                        avg_cost, current_price, market_value, profit_loss, created_at, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?)""",
                    (
                        self.account_id,
                        code,
                        broker_pos.get("stock_name", code),
                        broker_pos.get("quantity", 0),
                        broker_pos.get("quantity", 0),  # 券商持仓全部可用
                        broker_pos.get("avg_cost", 0),
                        broker_pos.get("current_price", 0),
                        broker_pos.get("current_price", 0) * broker_pos.get("quantity", 0),
                        get_china_time().isoformat(),
                        get_china_time().isoformat(),
                    )
                )
                result["created"] += 1

        # DB 有但券商没有 → 可能是记录错误，标记
        for code, db_pos in db_map.items():
            if code not in broker_map:
                result["discrepancy"].append({
                    "stock_code": code,
                    "db_quantity": db_pos.get("quantity", 0),
                    "broker_quantity": 0,
                    "note": "DB有但券商无",
                })

        return result

    async def refresh_pnl(self, current_prices: Dict[str, float]) -> int:
        """
        刷新持仓盈亏

        Args:
            current_prices: {stock_code: current_price}

        Returns:
            刷新数量
        """
        count = 0
        for code, price in current_prices.items():
            await self.db.execute(
                """UPDATE stock_positions
                   SET current_price = ?,
                       market_value = quantity * ?,
                       profit_loss = (? - avg_cost) * quantity,
                       updated_at = ?
                   WHERE account_id = ? AND stock_code = ?""",
                (price, price, price, get_china_time().isoformat(), self.account_id, code)
            )
            count += 1
        return count

    async def unfreeze_positions(self) -> int:
        """
        解冻昨日买入的持仓（T+1 规则）

        只解冻今天之前买入的份额，今日买入的继续冻结。
        对于今天有买入的股票：available = quantity - 今日买入量
        对于今天无买入的股票：available = quantity（全部解冻）

        Returns:
            解冻数量
        """
        now = get_china_time()
        today = now.strftime("%Y-%m-%d")

        # 1. 今天无买入的持仓：全部解冻
        no_buy_today = await self.db.execute(
            """UPDATE stock_positions
               SET available_quantity = quantity, updated_at = ?
               WHERE account_id = ?
                 AND available_quantity < quantity
                 AND stock_code NOT IN (
                     SELECT stock_code FROM trade_records
                     WHERE account_id = ? AND trade_type = 'buy'
                       AND date(trade_time) = ?
                 )""",
            (now.isoformat(), self.account_id, self.account_id, today)
        )
        count = no_buy_today.rowcount if no_buy_today.rowcount else 0

        # 2. 今天有买入的持仓：available = quantity - 今日买入量
        #    仅对 available != quantity - today_buy 的记录执行更新
        partial_unfreeze = await self.db.execute(
            """UPDATE stock_positions
               SET available_quantity = quantity - COALESCE(
                       (SELECT SUM(t.quantity) FROM trade_records t
                        WHERE t.account_id = stock_positions.account_id
                          AND t.stock_code = stock_positions.stock_code
                          AND t.trade_type = 'buy'
                          AND date(t.trade_time) = ?), 0),
                   updated_at = ?
               WHERE account_id = ?
                 AND stock_code IN (
                     SELECT stock_code FROM trade_records
                     WHERE account_id = ? AND trade_type = 'buy'
                       AND date(trade_time) = ?
                 )
                 AND available_quantity != quantity - COALESCE(
                       (SELECT SUM(t.quantity) FROM trade_records t
                        WHERE t.account_id = stock_positions.account_id
                          AND t.stock_code = stock_positions.stock_code
                          AND t.trade_type = 'buy'
                          AND date(t.trade_time) = ?), 0)""",
            (today, now.isoformat(), self.account_id,
             self.account_id, today, today)
        )
        count += partial_unfreeze.rowcount if partial_unfreeze.rowcount else 0

        return count


# 全局缓存
_position_managers: Dict[str, PositionManager] = {}


def get_position_manager(account_id: str) -> PositionManager:
    """获取持仓管理服务实例（每账户一个）"""
    if account_id not in _position_managers:
        _position_managers[account_id] = PositionManager(account_id)
    return _position_managers[account_id]
