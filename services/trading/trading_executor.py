"""
交易执行服务 — 买入/卖出/撤单/委托查询（mock + 实盘预留）。
"""
import logging
from typing import Optional

from services.trading.models import OrderResult

logger = logging.getLogger(__name__)


class TradingExecutorService:
    """交易执行服务：mock 买入/卖出，实盘交易接口预留"""

    def __init__(self):
        pass

    async def buy(self, stock_code: str, price: float, quantity: int, account_id: str = None, connected: bool = True) -> OrderResult:
        """买入（mock 模式，实盘接口预留）"""
        if not connected:
            return OrderResult(False, message="网关未连接")

        is_mock = True
        if account_id:
            try:
                from services.common.database import get_db_manager
                db = get_db_manager()
                acct = await db.fetchone(
                    "SELECT trade_mode FROM accounts WHERE account_id = ?",
                    (account_id,)
                )
                if acct:
                    is_mock = acct.get("trade_mode", "mock") == "mock"
            except Exception as e:
                logger.warning(f"获取账户交易模式失败，默认 mock: {e}")

        if is_mock:
            logger.info(f"[Mock] 买入 {stock_code} {quantity}股 @ {price:.2f}")
            return OrderResult(True, message=f"[Mock] 买入成功 {stock_code} {quantity}股 @ {price:.2f}")
        else:
            logger.warning(f"实盘交易接口尚未接入，当前为 mock 模式")
            return OrderResult(False, message="SDK 实盘交易接口待实现")

    async def sell(self, stock_code: str, price: float, quantity: int, account_id: str = None, connected: bool = True) -> OrderResult:
        """卖出（mock 模式，实盘接口预留）"""
        if not connected:
            return OrderResult(False, message="网关未连接")

        is_mock = True
        if account_id:
            try:
                from services.common.database import get_db_manager
                db = get_db_manager()
                acct = await db.fetchone(
                    "SELECT trade_mode FROM accounts WHERE account_id = ?",
                    (account_id,)
                )
                if acct:
                    is_mock = acct.get("trade_mode", "mock") == "mock"
            except Exception as e:
                logger.warning(f"获取账户交易模式失败，默认 mock: {e}")

        if is_mock:
            logger.info(f"[Mock] 卖出 {stock_code} {quantity}股 @ {price:.2f}")
            return OrderResult(True, message=f"[Mock] 卖出成功 {stock_code} {quantity}股 @ {price:.2f}")
        else:
            logger.warning(f"实盘交易接口尚未接入，当前为 mock 模式")
            return OrderResult(False, message="SDK 实盘交易接口待实现")

    async def cancel_order(self, order_no: str, account_id: str = None) -> dict:
        """撤销委托单（实盘预留）"""
        return {"success": False, "message": "实盘交易接口待实现"}

    async def query_order_status(self, order_no: str, account_id: str = None) -> dict:
        """查询委托成交情况（实盘预留）"""
        return {"status": "unknown", "message": "实盘交易接口待实现"}

    async def cancel_all_pending_orders(self, account_id: str = None) -> dict:
        """收盘后撤销全部未成交委托（实盘预留）"""
        return {"total": 0, "cancelled": 0, "failed": 0, "messages": ["实盘交易接口待实现"]}
