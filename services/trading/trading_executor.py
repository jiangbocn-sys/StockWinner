"""
交易执行服务 — 通过 BrokerAdapter 执行交易

架构：
TradingExecutor → BrokerRegistry → BrokerAdapter → SDK/mock

支持多种交易模式：
- mock: 模拟交易，不执行实际下单
- qmt: miniQmt/QMT 实盘交易（需 Windows + xtquant）
"""
import logging
from typing import Optional, Dict, Any, List

from services.trading.models import OrderResult

logger = logging.getLogger(__name__)


class TradingExecutorService:
    """交易执行服务 — 通过 BrokerAdapter 执行交易"""

    def __init__(self):
        pass

    async def _get_broker_config(self, account_id: str) -> Optional[Dict[str, Any]]:
        """从数据库读取账户的券商配置"""
        if not account_id:
            return None

        from services.common.database import get_db_manager
        db = get_db_manager()

        try:
            acct = await db.fetchone(
                """SELECT trade_mode, broker_account, broker_password,
                          broker_qmt_userdata_path, broker_qmt_session
                   FROM accounts WHERE account_id = ?""",
                (account_id,)
            )
            if not acct:
                logger.warning(f"账户不存在: {account_id}")
                return None

            return {
                "broker_type": acct.get("trade_mode", "mock"),
                "account_id": acct.get("broker_account", ""),
                "password": acct.get("broker_password", ""),
                "userdata_path": acct.get("broker_qmt_userdata_path", ""),
                "session_id": acct.get("broker_qmt_session", ""),
            }
        except Exception as e:
            logger.warning(f"获取账户券商配置失败: {e}")
            return None

    async def _get_adapter(self, account_id: str) -> Optional[Any]:
        """获取账户对应的 BrokerAdapter"""
        from services.trading.brokers.registry import get_adapter_for_account
        from services.trading.brokers.base import BrokerConfig

        config_data = await self._get_broker_config(account_id)
        if not config_data:
            # 无配置时返回 None（后续会使用 mock）
            return None

        config = BrokerConfig(
            broker_type=config_data["broker_type"],
            account_id=config_data["account_id"],
            password=config_data["password"],
            userdata_path=config_data["userdata_path"],
            session_id=config_data["session_id"],
        )

        adapter = await get_adapter_for_account(account_id, config)
        return adapter

    async def buy(
        self,
        stock_code: str,
        price: float,
        quantity: int,
        account_id: str = None,
        connected: bool = True
    ) -> OrderResult:
        """买入

        Args:
            stock_code: 股票代码（格式：600000.SH）
            price: 价格
            quantity: 数量（股）
            account_id: 账户 ID（用于获取券商配置）
            connected: 网关连接状态（预留）

        Returns:
            OrderResult
        """
        if not connected:
            return OrderResult(False, message="网关未连接")

        # 无账户时使用 mock
        if not account_id:
            logger.info(f"[Mock] 买入 {stock_code} {quantity}股 @ {price:.2f}")
            return OrderResult(True, order_id=f"MOCK_{stock_code}", message=f"[Mock] 买入成功")

        # 获取适配器
        adapter = await self._get_adapter(account_id)
        if not adapter:
            logger.warning(f"无法获取交易适配器，使用 mock: account={account_id}")
            return OrderResult(True, order_id=f"MOCK_{stock_code}", message=f"[Mock] 买入成功（无适配器）")

        # 执行买入
        result = await adapter.buy(stock_code, price, quantity)

        if result.success:
            logger.info(f"[{adapter.broker_type}] 买入成功: {stock_code} {quantity}股 @ {price:.2f}, order_id={result.order_id}")
        else:
            logger.error(f"[{adapter.broker_type}] 买入失败: {stock_code}, {result.message}")

        return result

    async def sell(
        self,
        stock_code: str,
        price: float,
        quantity: int,
        account_id: str = None,
        connected: bool = True
    ) -> OrderResult:
        """卖出"""
        if not connected:
            return OrderResult(False, message="网关未连接")

        if not account_id:
            logger.info(f"[Mock] 卖出 {stock_code} {quantity}股 @ {price:.2f}")
            return OrderResult(True, order_id=f"MOCK_{stock_code}", message=f"[Mock] 卖出成功")

        adapter = await self._get_adapter(account_id)
        if not adapter:
            logger.warning(f"无法获取交易适配器，使用 mock: account={account_id}")
            return OrderResult(True, order_id=f"MOCK_{stock_code}", message=f"[Mock] 卖出成功（无适配器）")

        result = await adapter.sell(stock_code, price, quantity)

        if result.success:
            logger.info(f"[{adapter.broker_type}] 卖出成功: {stock_code} {quantity}股 @ {price:.2f}, order_id={result.order_id}")
        else:
            logger.error(f"[{adapter.broker_type}] 卖出失败: {stock_code}, {result.message}")

        return result

    async def cancel_order(self, order_id: str, account_id: str = None) -> dict:
        """撤单"""
        if not account_id:
            return {"success": False, "message": "mock 模式不支持撤单"}

        adapter = await self._get_adapter(account_id)
        if not adapter:
            return {"success": False, "message": "无法获取交易适配器"}

        success = await adapter.cancel_order(order_id)
        return {
            "success": success,
            "message": "撤单成功" if success else "撤单失败"
        }

    async def query_order_status(self, order_id: str, account_id: str = None) -> dict:
        """查询委托状态"""
        if not account_id:
            return {"status": "mock", "message": "mock 模式"}

        adapter = await self._get_adapter(account_id)
        if not adapter:
            return {"status": "error", "message": "无法获取交易适配器"}

        orders = await adapter.query_orders()
        for order in orders:
            if str(order.get("order_id")) == order_id:
                return order

        return {"status": "not_found", "message": "委托不存在"}

    async def cancel_all_pending_orders(self, account_id: str = None) -> dict:
        """撤销全部未成交委托"""
        if not account_id:
            return {"total": 0, "cancelled": 0, "failed": 0, "messages": ["mock 模式"]}

        adapter = await self._get_adapter(account_id)
        if not adapter:
            return {"total": 0, "cancelled": 0, "failed": 1, "messages": ["无法获取交易适配器"]}

        orders = await adapter.query_orders(cancelable_only=True)

        cancelled = 0
        failed = 0
        messages = []

        for order in orders:
            success = await adapter.cancel_order(str(order["order_id"]))
            if success:
                cancelled += 1
                messages.append(f"撤单成功: {order['stock_code']}")
            else:
                failed += 1
                messages.append(f"撤单失败: {order['stock_code']}")

        return {
            "total": len(orders),
            "cancelled": cancelled,
            "failed": failed,
            "messages": messages
        }

    async def query_positions(self, account_id: str = None) -> list:
        """查询持仓（从券商 SDK）"""
        if not account_id:
            return []

        adapter = await self._get_adapter(account_id)
        if not adapter:
            return []

        positions = await adapter.query_positions()
        return [
            {
                "stock_code": p.stock_code,
                "stock_name": p.stock_name,
                "quantity": p.quantity,
                "available_quantity": p.available_quantity,
                "avg_cost": p.avg_cost,
                "market_value": p.market_value,
                "profit_rate": p.profit_rate,
            }
            for p in positions
        ]

    async def query_asset(self, account_id: str = None) -> dict:
        """查询资金（从券商 SDK）"""
        if not account_id:
            return {"total_asset": 0, "available_cash": 0, "market_value": 0}

        adapter = await self._get_adapter(account_id)
        if not adapter:
            return {"total_asset": 0, "available_cash": 0, "market_value": 0}

        asset = await adapter.query_asset()
        return {
            "total_asset": asset.total_asset,
            "available_cash": asset.available_cash,
            "frozen_cash": asset.frozen_cash,
            "market_value": asset.market_value,
        }

    async def health_check(self, account_id: str = None) -> dict:
        """健康检查"""
        if not account_id:
            return {"ok": True, "broker_type": "mock", "message": "mock 模式"}

        adapter = await self._get_adapter(account_id)
        if not adapter:
            return {"ok": False, "message": "无法获取交易适配器"}

        return await adapter.health_check()