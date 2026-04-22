"""
交易监控模块 (Trading Monitor)
监控 watchlist 中的股票，根据策略条件执行交易

功能：
1. 按 watchlist 监控候选股票行情，到达预设买卖价位进行交易
2. 读取持仓策略，确定买入份额
3. 交易前读取账户可用资金，确定可买数量
4. 交易后更新可用资金
5. 计算并记录交易手续费
"""

import asyncio
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any
from services.common.database import get_db_manager
from services.trading.gateway import get_gateway, MarketData
from services.trading.execution_service import get_trade_execution_service

# 中国时区
CHINA_TZ = timezone(timedelta(hours=8))

def get_china_time():
    """获取中国时区时间"""
    return datetime.now(CHINA_TZ).replace(tzinfo=None)


class TradingMonitor:
    """交易监控服务"""

    def __init__(self):
        self._running = False
        self._task = None
        self._account_id = None

    async def start_monitoring(
        self,
        account_id: str,
        interval: int = 30
    ):
        """
        启动交易监控服务

        Args:
            account_id: 账户 ID
            interval: 监控间隔（秒）
        """
        if self._running:
            return {"success": False, "message": "交易监控服务已在运行"}

        self._running = True
        self._account_id = account_id
        self._task = asyncio.create_task(
            self._run_monitoring_loop(account_id, interval)
        )
        return {"success": True, "message": "交易监控服务已启动"}

    async def stop_monitoring(self):
        """停止交易监控服务"""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        return {"success": True, "message": "交易监控服务已停止"}

    async def _run_monitoring_loop(
        self,
        account_id: str,
        interval: int
    ):
        """交易监控循环"""
        print(f"[Monitor] 启动交易监控服务 - 账户：{account_id}, 间隔：{interval}s")

        while self._running:
            try:
                await self._run_monitoring(account_id)
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"[Monitor] 错误：{e}")
                await asyncio.sleep(interval)

        print(f"[Monitor] 交易监控服务已停止")

    async def _run_monitoring(self, account_id: str):
        """执行一次交易监控"""
        db = get_db_manager()

        # 获取 watchlist 中的股票
        watchlist = await db.fetchall(
            "SELECT * FROM watchlist WHERE account_id = ? AND status IN ('pending', 'watching')",
            (account_id,)
        )

        if watchlist:
            print(f"[Monitor] 监控 {len(watchlist)} 只股票")

        for stock in watchlist:
            await self._check_stock_signals(account_id, stock)

    async def _check_stock_signals(self, account_id: str, stock: Dict):
        """
        检查单只股票的交易信号
        使用真实行情数据
        """
        stock_code = stock.get('stock_code')
        buy_price = stock.get('buy_price', 0)
        stop_loss = stock.get('stop_loss_price', 0)
        take_profit = stock.get('take_profit_price', 0)
        target_quantity = stock.get('target_quantity', 100)
        status = stock.get('status')

        # 获取交易网关
        gateway = None
        current_price = None

        try:
            gateway = await get_gateway()
            # 获取实时行情 - 直接传入股票代码，网关会自动处理格式
            market_data: Optional[MarketData] = await gateway.get_market_data(stock_code)

            if market_data:
                current_price = market_data.current_price
                print(f"[Monitor] {stock_code} 实时价格：{current_price:.2f}")
            else:
                print(f"[Monitor] {stock_code} 无法获取行情数据")
                return
        except Exception as e:
            print(f"[Monitor] 获取 {stock_code} 行情数据失败：{e}")
            return

        if current_price is None:
            return

        # 检查买入条件
        if status == 'pending':
            # 价格达到买入价附近 2% 范围内
            if buy_price > 0 and abs(current_price - buy_price) / buy_price <= 0.02:
                print(f"[Monitor] 触发买入信号：{stock_code}, 目标价：{buy_price:.2f}, 当前价：{current_price:.2f}")
                await self._execute_buy_signal(account_id, stock, current_price, target_quantity)

        # 检查止损/止盈条件
        elif status == 'watching':
            # 检查止损条件
            if stop_loss > 0 and current_price <= stop_loss:
                print(f"[Monitor] 触发止损：{stock_code}, 止损价：{stop_loss:.2f}, 当前价：{current_price:.2f}")
                await self._execute_sell_signal(
                    account_id, stock, current_price,
                    'sell_stop_loss', target_quantity
                )

            # 检查止盈条件
            elif take_profit > 0 and current_price >= take_profit:
                print(f"[Monitor] 触发止盈：{stock_code}, 止盈价：{take_profit:.2f}, 当前价：{current_price:.2f}")
                await self._execute_sell_signal(
                    account_id, stock, current_price,
                    'sell_take_profit', target_quantity
                )

    async def _execute_buy_signal(
        self,
        account_id: str,
        stock: Dict,
        current_price: float,
        target_quantity: int
    ):
        """执行买入交易"""
        stock_code = stock.get('stock_code')
        stock_name = stock.get('stock_name', '')

        # 获取交易执行服务
        execution = get_trade_execution_service(account_id)

        # 执行买入（自动计算可用资金和手续费）
        result = await execution.execute_buy(
            stock_code=stock_code,
            stock_name=stock_name,
            price=current_price,
            target_quantity=target_quantity
        )

        if result["success"]:
            print(f"[Monitor] 买入成功：{stock_code}")
            print(f"  数量：{result['quantity']} 股")
            print(f"  价格：{result['price']:.2f} 元")
            print(f"  总金额：{result['total_amount']:.2f} 元")
            print(f"  手续费：{result['fees']['total_fee']:.2f} 元")

            # 更新 watchlist 状态
            await self._update_watchlist_status(
                account_id, stock_code, 'watching'
            )

            # 创建交易信号记录
            await self._create_signal(
                account_id, stock, 'buy_executed', current_price,
                result['quantity'], result['total_amount']
            )
        else:
            print(f"[Monitor] 买入失败：{result['message']}")
            # 创建失败信号记录
            await self._create_signal(
                account_id, stock, 'buy_failed', current_price,
                target_quantity, 0, result['message']
            )

    async def _execute_sell_signal(
        self,
        account_id: str,
        stock: Dict,
        current_price: float,
        signal_type: str,
        target_quantity: int
    ):
        """执行卖出交易"""
        stock_code = stock.get('stock_code')
        stock_name = stock.get('stock_name', '')

        # 获取交易执行服务
        execution = get_trade_execution_service(account_id)

        # 执行卖出（自动计算持仓数量和手续费）
        result = await execution.execute_sell(
            stock_code=stock_code,
            stock_name=stock_name,
            price=current_price,
            target_quantity=target_quantity
        )

        if result["success"]:
            print(f"[Monitor] 卖出成功：{stock_code}")
            print(f"  数量：{result['quantity']} 股")
            print(f"  价格：{result['price']:.2f} 元")
            print(f"  净得：{result['net_amount']:.2f} 元")
            print(f"  手续费：{result['fees']['total_fee']:.2f} 元")
            print(f"  盈亏：{result['profit_loss']:.2f} 元")

            # 如果清空持仓，更新 watchlist 状态
            await self._update_watchlist_status(
                account_id, stock_code, 'sold'
            )

            # 创建交易信号记录
            await self._create_signal(
                account_id, stock, signal_type, current_price,
                result['quantity'], result['net_amount'],
                profit_loss=result['profit_loss']
            )
        else:
            print(f"[Monitor] 卖出失败：{result['message']}")

    async def _create_signal(
        self,
        account_id: str,
        stock: Dict,
        signal_type: str,
        price: float,
        quantity: int = 0,
        amount: float = 0,
        profit_loss: Optional[float] = None,
        message: Optional[str] = None
    ):
        """创建交易信号记录"""
        db = get_db_manager()

        signal_data = {
            "account_id": account_id,
            "strategy_id": stock.get('strategy_id'),
            "stock_code": stock.get('stock_code'),
            "stock_name": stock.get('stock_name'),
            "signal_type": signal_type,
            "price": price,
            "quantity": quantity,
            "amount": amount,
            "profit_loss": profit_loss,
            "message": message,
            "status": "completed" if "failed" not in signal_type else "failed",
            "created_at": get_china_time().isoformat()
        }

        # 移除 None 值
        signal_data = {k: v for k, v in signal_data.items() if v is not None}

        await db.insert("trading_signals", signal_data)
        print(f"[Monitor] 创建交易信号：{stock.get('stock_code')} - {signal_type}")

    async def _update_watchlist_status(
        self,
        account_id: str,
        stock_code: str,
        status: str
    ):
        """更新 watchlist 状态"""
        db = get_db_manager()
        await db.update(
            "watchlist",
            {"status": status, "updated_at": get_china_time()},
            "account_id = ? AND stock_code = ?",
            (account_id, stock_code)
        )

    def get_status(self) -> Dict:
        """获取服务状态"""
        return {
            "running": self._running,
            "account_id": self._account_id,
            "task": "active" if self._task else None
        }


# 全局单例
_trading_monitor: Optional[TradingMonitor] = None


def get_trading_monitor() -> TradingMonitor:
    """获取交易监控服务单例"""
    global _trading_monitor
    if _trading_monitor is None:
        _trading_monitor = TradingMonitor()
    return _trading_monitor


def reset_trading_monitor():
    """重置交易监控服务（用于测试）"""
    global _trading_monitor
    _trading_monitor = None
