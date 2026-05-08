"""
订单服务 (Order Service)

订单状态机：
pending → submitted → partial → filled
                     → cancelled
                     → rejected

负责：
1. 订单创建和状态流转
2. 订单与券商委托编号的映射
3. 订单完成后触发资金/持仓更新
"""

from datetime import datetime
from typing import Dict, List, Optional
from services.common.database import get_db_manager
from services.common.timezone import get_china_time, format_china_time


# 订单状态枚举
ORDER_STATUS = {
    "pending": "待提交",
    "submitted": "已提交",
    "partial": "部分成交",
    "filled": "全部成交",
    "cancelled": "已撤销",
    "rejected": "已拒绝",
}

VALID_TRANSITIONS = {
    "pending": ["submitted", "cancelled", "rejected"],
    "submitted": ["partial", "filled", "cancelled", "rejected"],
    "partial": ["filled", "cancelled"],
    "filled": [],   # 终态
    "cancelled": [],  # 终态
    "rejected": [],   # 终态
}


class OrderService:
    """订单服务"""

    def __init__(self, account_id: str):
        self.account_id = account_id
        self.db = get_db_manager()

    async def create_order(
        self,
        stock_code: str,
        stock_name: str,
        trade_type: str,
        price: float,
        quantity: int,
        trigger_source: Optional[str] = None,
        stop_loss_price: Optional[float] = None,
        take_profit_price: Optional[float] = None,
    ) -> Optional[int]:
        """
        创建订单（初始状态 pending）

        Returns:
            订单 ID，失败返回 None
        """
        order_id = f"ORD_{get_china_time().strftime('%Y%m%d_%H%M%S')}_{stock_code}"

        data = {
            "account_id": self.account_id,
            "stock_code": stock_code,
            "stock_name": stock_name,
            "trade_type": trade_type,
            "order_price": price,
            "order_quantity": quantity,
            "filled_quantity": 0,
            "filled_amount": 0,
            "status": "pending",
            "trigger_source": trigger_source,
            "stop_loss_price": stop_loss_price,
            "take_profit_price": take_profit_price,
            "order_no": order_id,
            "created_at": format_china_time(),
            "updated_at": format_china_time(),
        }

        # 移除 None 值
        data = {k: v for k, v in data.items() if v is not None}

        return await self.db.insert("orders", data)

    async def update_status(
        self,
        order_id: int,
        new_status: str,
        broker_order_id: Optional[str] = None,
        filled_quantity: Optional[int] = None,
        filled_amount: Optional[float] = None,
        reject_reason: Optional[str] = None,
    ) -> bool:
        """
        更新订单状态

        Returns:
            是否成功（状态转换合法）
        """
        order = await self.db.fetchone(
            "SELECT status FROM orders WHERE id = ? AND account_id = ?",
            (order_id, self.account_id),
        )
        if not order:
            return False

        current_status = order["status"]

        # 检查状态转换是否合法
        allowed = VALID_TRANSITIONS.get(current_status, [])
        if new_status not in allowed:
            return False

        # 更新
        updates = {"status": new_status, "updated_at": format_china_time()}
        if broker_order_id:
            updates["broker_order_id"] = broker_order_id
        if filled_quantity is not None:
            updates["filled_quantity"] = filled_quantity
        if filled_amount is not None:
            updates["filled_amount"] = filled_amount
        if reject_reason:
            updates["reject_reason"] = reject_reason

        await self.db.update("orders", updates, "id = ? AND account_id = ?", (order_id, self.account_id))
        return True

    async def get_pending_orders(self) -> List[Dict]:
        """获取所有未完成订单"""
        return await self.db.fetchall(
            "SELECT * FROM orders WHERE account_id = ? AND status IN ('pending', 'submitted', 'partial') ORDER BY created_at DESC",
            (self.account_id,),
        )

    async def get_today_orders(self) -> List[Dict]:
        """获取今日所有订单"""
        today = get_china_time().strftime("%Y-%m-%d")
        return await self.db.fetchall(
            "SELECT * FROM orders WHERE account_id = ? AND DATE(created_at) = ? ORDER BY created_at DESC",
            (self.account_id, today),
        )


# 全局缓存
_order_services: Dict[str, OrderService] = {}


def get_order_service(account_id: str) -> OrderService:
    """获取订单服务实例（每账户一个）"""
    if account_id not in _order_services:
        _order_services[account_id] = OrderService(account_id)
    return _order_services[account_id]
