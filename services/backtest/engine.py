"""
回测引擎 - 主编排器

职责：
1. 接收回测参数，创建/更新 backtest_runs 记录
2. 执行数据完整性检查
3. 分发到对应模式引擎（撮合模拟盘 / 收益率累积）
4. 保存回测结果到数据库
"""

import json
import logging
from typing import Dict, List, Optional
from datetime import datetime

from services.common.database import get_db_manager
from services.common.structured_logger import get_logger
from services.common.timezone import get_china_time
from services.backtest.data_validator import DataCompletenessChecker, DataGapReport
from services.backtest.execution import FeeConfig, PositionLimits
from services.backtest.metrics import PerformanceMetrics

logger = get_logger("backtest")


class BacktestEngine:
    """回测引擎"""

    def __init__(self, account_id: str = ""):
        self.account_id = account_id
        self.db = get_db_manager()
        from services.factors.kline_manager import get_kline_manager
        self.km = get_kline_manager()

    async def create_run(
        self,
        name: str,
        strategy_id: Optional[int],
        mode: str,
        start_date: str,
        end_date: str,
        initial_capital: float = 1000000,
        stock_pool: Optional[List[str]] = None,
        markets: Optional[List[str]] = None,
        config: Optional[Dict] = None,
    ) -> int:
        """创建回测任务，返回 run_id"""
        run_id = await self.db.insert("backtest_runs", {
            "account_id": self.account_id,
            "strategy_id": strategy_id,
            "name": name,
            "mode": mode,
            "start_date": start_date,
            "end_date": end_date,
            "initial_capital": initial_capital,
            "config": json.dumps(config or {}),
            "status": "pending",
        })
        return run_id

    def _run_backtest_sync(
        self,
        run_id: int,
        strategy_config: Dict,
        mode: str = "simulated",
        start_date: str = "",
        end_date: str = "",
        initial_capital: float = 1000000,
        stock_pool: Optional[List[str]] = None,
        fee_config: Optional[FeeConfig] = None,
        position_limits: Optional[PositionLimits] = None,
        slippage_pct: float = 0.0,
        stop_loss_pct: Optional[float] = None,
        take_profit_pct: Optional[float] = None,
        trailing_stop_pct: Optional[float] = None,
        initial_positions: Optional[List[Dict]] = None,
        initial_cash: Optional[float] = None,
        liquidate_at_end: bool = True,
        trading_strategies: Optional[List[Dict]] = None,  # 预加载的策略代码
        is_subprocess: bool = False,
    ) -> Dict:
        """同步版本的 run_backtest，用于在线程池中执行。

        Args:
            trading_strategies: 预加载的交易策略代码 [{id, name, code, function_name}, ...]
            is_subprocess: 子进程模式下跳过数据库写入，避免 database is locked 错误
        """
        import asyncio

        # 在线程中创建新事件循环运行异步逻辑
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(
                self.run_backtest(
                    run_id=run_id,
                    strategy_config=strategy_config,
                    mode=mode,
                    start_date=start_date,
                    end_date=end_date,
                    initial_capital=initial_capital,
                    stock_pool=stock_pool,
                    fee_config=fee_config,
                    position_limits=position_limits,
                    slippage_pct=slippage_pct,
                    stop_loss_pct=stop_loss_pct,
                    take_profit_pct=take_profit_pct,
                    trailing_stop_pct=trailing_stop_pct,
                    initial_positions=initial_positions,
                    initial_cash=initial_cash,
                    liquidate_at_end=liquidate_at_end,
                    trading_strategies=trading_strategies,
                    is_subprocess=is_subprocess,
                )
            )
        finally:
            loop.close()

    def _run_segmented_backtest_sync(
        self,
        run_id: int,
        strategy_config: Dict,
        mode: str,
        start_date: str,
        end_date: str,
        initial_capital: float,
        pool_schedule: List[Dict],
        fee_config: Optional[FeeConfig] = None,
        position_limits: Optional[PositionLimits] = None,
        slippage_pct: float = 0.0,
        stop_loss_pct: Optional[float] = None,
        take_profit_pct: Optional[float] = None,
        trailing_stop_pct: Optional[float] = None,
        trading_strategies: Optional[List[Dict]] = None,  # 预加载的策略代码
        is_subprocess: bool = False,
    ) -> Dict:
        """同步版本的分段回测，用于在线程池中执行。

        Args:
            trading_strategies: 预加载的交易策略代码 [{id, name, code, function_name}, ...]
            is_subprocess: 子进程模式下跳过数据库写入，避免 database is locked 错误
        """
        import asyncio

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            # 先获取基准数据（子进程模式下跳过，主进程会处理）
            benchmark_data = [] if is_subprocess else loop.run_until_complete(
                self._fetch_benchmark_data(start_date, end_date)
            )

            result = loop.run_until_complete(
                self.run_segmented_backtest(
                    run_id=run_id,
                    strategy_config=strategy_config,
                    mode=mode,
                    start_date=start_date,
                    end_date=end_date,
                    initial_capital=initial_capital,
                    pool_schedule=pool_schedule,
                    fee_config=fee_config,
                    position_limits=position_limits,
                    slippage_pct=slippage_pct,
                    stop_loss_pct=stop_loss_pct,
                    take_profit_pct=take_profit_pct,
                    trailing_stop_pct=trailing_stop_pct,
                    trading_strategies=trading_strategies,
                    is_subprocess=is_subprocess,
                )
            )

            if "error" not in result and not is_subprocess:
                # 子进程模式下不保存结果（由主进程处理）
                try:
                    loop.run_until_complete(
                        self._save_results(run_id, result, benchmark_data)
                    )
                except Exception as e:
                    logger.warn("backtest", f"保存回测结果失败: {e}")
                    result["error"] = f"保存结果失败: {e}"

            return result
        finally:
            loop.close()

    async def run_backtest(
        self,
        run_id: int,
        strategy_config: Dict,
        mode: str = "simulated",
        start_date: str = "",
        end_date: str = "",
        initial_capital: float = 1000000,
        stock_pool: Optional[List[str]] = None,
        fee_config: Optional[FeeConfig] = None,
        position_limits: Optional[PositionLimits] = None,
        slippage_pct: float = 0.0,
        stop_loss_pct: Optional[float] = None,
        take_profit_pct: Optional[float] = None,
        trailing_stop_pct: Optional[float] = None,
        initial_positions: Optional[List[Dict]] = None,
        initial_cash: Optional[float] = None,
        liquidate_at_end: bool = True,
        trading_strategies: Optional[List[Dict]] = None,  # 预加载的策略代码
        is_subprocess: bool = False,
    ) -> Dict:
        """
        执行回测。

        Args:
            trading_strategies: 预加载的交易策略代码（子进程模式下传入）
            is_subprocess: 子进程模式下跳过数据库写入（由主进程处理）

        Returns:
            回测结果字典
        """
        # 1. 更新状态为 running（子进程模式下跳过）
        if not is_subprocess:
            await self.db.execute(
            "UPDATE backtest_runs SET status = 'running', started_at = ? WHERE id = ?",
            (get_china_time().isoformat(), run_id)
        )

        try:
            # 2. 获取策略配置
            if not start_date:
                start_date = strategy_config.get("start_date", "2024-01-01")
            if not end_date:
                end_date = strategy_config.get("end_date", "2024-12-31")

            # 3. 确定股票池
            if not stock_pool:
                stock_pool = self._resolve_stock_pool(strategy_config)

            # 4. 数据完整性检查（子进程模式下跳过，避免数据库访问）
            if not is_subprocess:
                max_check_stocks = 1000
                if len(stock_pool) > max_check_stocks:
                    import random
                    check_pool = random.sample(stock_pool, max_check_stocks)
                else:
                    check_pool = stock_pool

                gap_report = await self._check_data_completeness(
                    check_pool, start_date, end_date
                )
                if not gap_report.can_proceed:
                    await self._mark_failed(run_id, "数据完整性检查未通过", gap_report)
                    return {"error": "数据完整性检查未通过", "gap_report": gap_report.to_dict()}

                # 5. 保存数据完整性报告
                await self.db.execute(
                    "UPDATE backtest_runs SET data_gap_report = ? WHERE id = ?",
                    (json.dumps(gap_report.to_dict(), ensure_ascii=False), run_id)
                )

            # 6. 执行回测
            if mode == "simulated":
                result = await self._run_simulated(
                    run_id, strategy_config, start_date, end_date,
                    initial_capital, stock_pool, fee_config, position_limits,
                    slippage_pct, stop_loss_pct, take_profit_pct, trailing_stop_pct,
                    initial_positions, initial_cash, liquidate_at_end,
                    trading_strategies, is_subprocess=is_subprocess,
                )
            elif mode == "return_accumulation":
                result = await self._run_return_accumulation(
                    run_id, strategy_config, start_date, end_date,
                    initial_capital, stock_pool,
                    trading_strategies=trading_strategies,
                    is_subprocess=is_subprocess,
                )
            else:
                result = {"error": f"不支持的回测模式: {mode}"}

            return result

        except Exception as e:
            logger.error("backtest", f"回测执行失败 (run_id={run_id}): {e}")
            if not is_subprocess:
                await self._mark_failed(run_id, str(e), None)
            return {"error": str(e)}

    async def _run_simulated(
        self, run_id, strategy_config, start_date, end_date,
        initial_capital, stock_pool, fee_config, position_limits,
        slippage_pct, stop_loss_pct, take_profit_pct, trailing_stop_pct,
        initial_positions=None, initial_cash=None, liquidate_at_end=True,
        trading_strategies=None, is_subprocess=False,
    ) -> Dict:
        """执行撮合模拟盘模式

        Args:
            trading_strategies: 预加载的交易策略代码
            is_subprocess: 子进程模式下跳过数据库写入
        """
        from services.backtest.modes.simulated_trading import SimulatedTradingEngine

        # 从 strategy_config 读取费率配置
        fee = fee_config or FeeConfig(
            commission_rate=strategy_config.get("commission_rate", 0.0001),
            min_commission=strategy_config.get("min_commission", 5.0),
            stamp_tax=strategy_config.get("stamp_tax", 0.0005),
            transfer_fee=strategy_config.get("transfer_fee", 0.00002),
        )

        limits = position_limits or PositionLimits(
            max_total_position_pct=strategy_config.get("max_total_position_pct", 0.80),
            max_single_position_pct=strategy_config.get("max_single_position_pct", 0.15),
            cash_reserve_pct=strategy_config.get("cash_reserve_pct", 0.10),
        )

        # 从 strategy_config 读取止盈止损
        if stop_loss_pct is None:
            stop_loss_pct = strategy_config.get("stop_loss_pct")
        if take_profit_pct is None:
            take_profit_pct = strategy_config.get("take_profit_pct")
        if trailing_stop_pct is None:
            trailing_stop_pct = strategy_config.get("trailing_stop_pct")

        engine = SimulatedTradingEngine(
            strategy_config=strategy_config,
            initial_capital=initial_capital,
            start_date=start_date,
            end_date=end_date,
            stock_pool=stock_pool,
            fee_config=fee,
            position_limits=limits,
            slippage_pct=slippage_pct,
            stop_loss_pct=stop_loss_pct,
            take_profit_pct=take_profit_pct,
            trailing_stop_pct=trailing_stop_pct,
            stop_execution_price=strategy_config.get("stop_execution_price", "close"),
            initial_positions=initial_positions,
            initial_cash=initial_cash,
            liquidate_at_end=liquidate_at_end,
            trading_strategies=trading_strategies,  # 使用预加载的策略
        )

        # 进度回调
        last_saved = [0]
        def progress(current, total, trade_date=""):
            pct = round(current / total * 100, 1) if total > 0 else 0
            # 子进程也用 sync 连接写进度到 DB，保持前端可见
            # is_subprocess 只控制最终 skip _save_results / _mark_failed
            try:
                import sqlite3
                from services.common.database import get_sync_connection
                conn = get_sync_connection("stockwinner")
                conn.execute(
                    "UPDATE backtest_runs SET progress = ?, current_trade_date = ? WHERE id = ?",
                    (pct, trade_date, run_id)
                )

                # 每 2% 计算一次实时指标
                if pct - last_saved[0] >= 2.0:
                    partial_result = PerformanceMetrics.compute(
                        nav_series=engine.nav_series,
                        trades=engine.trades,
                        initial_capital=initial_capital,
                        start_date=start_date,
                        end_date=trade_date,
                    )
                    conn.execute(
                        "UPDATE backtest_runs SET result_summary = ? WHERE id = ?",
                        (json.dumps(partial_result.to_dict(), ensure_ascii=False), run_id)
                    )
                    last_saved[0] = pct

                conn.commit()
            except Exception as e:
                logger.warn("backtest", f"保存回测进度失败: {e}")

        result = engine.run(progress_callback=progress)

        # 子进程模式下不保存结果（由主进程通过队列处理）
        if is_subprocess:
            return result

        if "error" in result:
            await self._mark_failed(run_id, result["error"], None)
            return result

        # 获取基准数据（沪深300）
        benchmark_data = await self._fetch_benchmark_data(start_date, end_date)

        # 保存结果（传入基准数据）
        await self._save_results(run_id, result, benchmark_data)
        return result

    async def _fetch_benchmark_data(self, start_date: str, end_date: str) -> List[Dict]:
        """获取沪深300指数（000300.SH）基准数据"""
        from services.factors.kline_manager import get_kline_manager
        km = get_kline_manager()
        kline = km.get_kline_data("000300.SH", start_date=start_date, end_date=end_date)
        if kline is None or len(kline) == 0:
            return []
        # 转换为 [{'trade_date': ..., 'close': ...}] 格式
        if hasattr(kline, 'to_dict'):
            return [
                {"trade_date": row["trade_date"], "close": float(row["close"])}
                for row in kline.to_dict("records")
            ]
        elif isinstance(kline, list):
            return [
                {"trade_date": row.get("trade_date", ""), "close": float(row.get("close", 0))}
                for row in kline
            ]
        return []

    async def _run_return_accumulation(
        self, run_id, strategy_config, start_date, end_date,
        initial_capital, stock_pool,
        trading_strategies: Optional[List[Dict]] = None,
        is_subprocess: bool = False,
    ) -> Dict:
        """执行收益率累积模式

        Args:
            trading_strategies: 预加载的交易策略代码
            is_subprocess: 子进程模式下跳过数据库写入（由主进程处理）
        """
        from services.backtest.modes.return_accumulation import ReturnAccumulationEngine

        stop_loss_pct = strategy_config.get("stop_loss_pct")
        take_profit_pct = strategy_config.get("take_profit_pct")
        holding_period = strategy_config.get("holding_period")  # 不设默认值

        engine = ReturnAccumulationEngine(
            strategy_config=strategy_config,
            initial_capital=initial_capital,
            start_date=start_date,
            end_date=end_date,
            stock_pool=stock_pool,
            stop_loss_pct=stop_loss_pct,
            take_profit_pct=take_profit_pct,
            holding_period=holding_period,  # 传入 None 时不启用固定持有天数
            trading_strategies=trading_strategies,  # 传入交易策略
        )

        # 进度回调（收益率累积模式）
        # 子进程也用 sync 连接写进度到 DB，保持前端可见
        def progress(current, total, trade_date=""):
            pct = round(current / total * 100, 1) if total > 0 else 0
            try:
                import sqlite3
                from services.common.database import get_sync_connection
                conn = get_sync_connection("stockwinner")
                conn.execute(
                    "UPDATE backtest_runs SET progress = ?, current_trade_date = ? WHERE id = ?",
                    (pct, trade_date, run_id)
                )
                conn.commit()
            except Exception as e:
                logger.warn("backtest", f"保存回测进度失败: {e}")

        result = engine.run(progress_callback=progress)

        # 子进程模式下不保存结果（由主进程通过队列处理）
        if is_subprocess:
            return result

        if "error" in result:
            await self._mark_failed(run_id, result["error"], None)
            return result

        benchmark_data = await self._fetch_benchmark_data(start_date, end_date)
        await self._save_results(run_id, result, benchmark_data)
        return result

    async def _check_data_completeness(
        self, stock_pool: List[str], start_date: str, end_date: str
    ) -> DataGapReport:
        """数据完整性检查"""
        checker = DataCompletenessChecker()
        report = await checker.check(stock_pool, start_date, end_date)
        return report

    async def _save_results(
        self, run_id: int, result: Dict, benchmark_data: Optional[List[Dict]] = None
    ):
        """保存回测结果到数据库"""
        backtest_trades = result.get("trades", [])
        nav_series = result.get("nav_series", [])
        daily_positions = result.get("daily_positions", [])
        summary = result.get("result", {})

        # 如果有基准数据，重新计算含对比的指标
        if benchmark_data and nav_series and backtest_trades:
            initial_capital = summary.get("initial_capital", 1000000)
            # 从 nav_series 推断日期范围
            first_date = nav_series[0]["trade_date"] if nav_series else ""
            last_date = nav_series[-1]["trade_date"] if nav_series else ""
            recomputed = PerformanceMetrics.compute(
                nav_series=nav_series,
                trades=backtest_trades,
                initial_capital=initial_capital,
                start_date=first_date,
                end_date=last_date,
                benchmark_series=benchmark_data,
            )
            summary = recomputed.to_dict()

        # 1. 保存交易记录
        for trade in backtest_trades:
            if trade.get("trade_type") == "buy":
                # 检查是否已有未卖出的记录（同一股票）
                stock_code = trade["stock_code"]
                existing = await self.db.fetchone(
                    "SELECT id, buy_quantity, buy_price, buy_commission, remaining_quantity FROM backtest_trades "
                    "WHERE backtest_run_id = ? AND stock_code = ? AND sell_date IS NULL",
                    (run_id, stock_code)
                )
                if existing:
                    # 加仓：更新数量和成本（加权平均）
                    old_qty = existing["buy_quantity"] or 0
                    old_price = existing["buy_price"] or 0
                    new_qty = trade["quantity"]
                    new_price = trade["price"]
                    total_qty = old_qty + new_qty
                    avg_price = (old_price * old_qty + new_price * new_qty) / total_qty if total_qty > 0 else new_price
                    await self.db.execute(
                        "UPDATE backtest_trades SET buy_quantity = ?, buy_price = ?, buy_commission = ?, remaining_quantity = ? WHERE id = ?",
                        (total_qty, avg_price, existing["buy_commission"] + trade.get("commission", 0), total_qty, existing["id"])
                    )
                else:
                    # 新买入：插入记录
                    await self.db.insert("backtest_trades", {
                        "backtest_run_id": run_id,
                        "stock_code": stock_code,
                        "stock_name": trade.get("stock_name", ""),
                        "buy_date": trade["date"],
                        "buy_price": trade["price"],
                        "buy_quantity": trade["quantity"],
                        "remaining_quantity": trade["quantity"],  # 初始剩余 = 买入数量
                        "buy_commission": trade.get("commission", 0),
                    })
            elif trade.get("trade_type") == "sell":
                stock_code = trade.get("stock_code", "")
                sell_qty = trade.get("sell_quantity", trade.get("quantity", 0))
                remaining_qty = trade.get("remaining_quantity", 0)

                # 插入卖出记录
                await self.db.insert("backtest_trades", {
                    "backtest_run_id": run_id,
                    "stock_code": stock_code,
                    "stock_name": trade.get("stock_name", ""),
                    "buy_date": trade.get("buy_date", ""),
                    "buy_price": trade.get("buy_price", 0),
                    "sell_date": trade["date"],
                    "sell_price": trade["price"],
                    "sell_quantity": sell_qty,
                    "remaining_quantity": remaining_qty,
                    "sell_commission": trade.get("commission", 0),
                    "sell_reason": trade.get("reason", ""),
                    "pnl": trade.get("pnl", 0),
                    "pnl_pct": trade.get("pnl_pct", 0),
                    "holding_days": trade.get("holding_days", 0),
                })

                # 如果全部卖出（remaining_quantity = 0），更新买入记录标记已清仓
                if remaining_qty == 0:
                    await self.db.execute(
                        "UPDATE backtest_trades SET sell_date = ?, sell_reason = '已清仓' "
                        "WHERE backtest_run_id = ? AND stock_code = ? AND sell_date IS NULL",
                        (trade["date"], run_id, stock_code)
                    )

        # 2. 保存每日净值
        for nav in nav_series:
            await self.db.execute(
                """INSERT OR REPLACE INTO backtest_daily_nav
                   (backtest_run_id, trade_date, nav, total_value, cash,
                    positions_value, position_count, drawdown, max_drawdown, daily_return)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    run_id, nav["trade_date"], nav["nav"], nav["total_value"],
                    nav["cash"], nav["positions_value"], nav.get("position_count", 0),
                    nav.get("drawdown", 0), nav.get("max_drawdown", 0),
                    nav.get("daily_return", 0),
                )
            )

        # 3. 保存每日持仓快照（先删除旧数据防止重复）
        await self.db.execute("DELETE FROM backtest_daily_positions WHERE backtest_run_id = ?", (run_id,))
        for pos in daily_positions:
            await self.db.insert("backtest_daily_positions", {
                "backtest_run_id": run_id,
                "trade_date": pos["trade_date"],
                "stock_code": pos["stock_code"],
                "stock_name": pos.get("stock_name", ""),
                "quantity": pos["quantity"],
                "avg_cost": pos["avg_cost"],
                "close_price": pos["close_price"],
                "market_value": pos["market_value"],
                "unrealized_pnl": pos.get("unrealized_pnl", 0),
            })

        # 4. 更新回测任务状态
        await self.db.execute(
            "UPDATE backtest_runs SET status = 'completed', progress = 100, "
            "result_summary = ?, completed_at = ? WHERE id = ?",
            (json.dumps(summary, ensure_ascii=False), get_china_time().isoformat(), run_id)
        )

    async def _update_progress(self, run_id: int, progress_pct: float):
        """更新回测进度"""
        await self.db.execute(
            "UPDATE backtest_runs SET progress = ? WHERE id = ?",
            (progress_pct, run_id)
        )

    async def _mark_failed(
        self, run_id: int, error: str, gap_report: Optional[DataGapReport]
    ):
        """标记回测失败"""
        gap_json = json.dumps(gap_report.to_dict(), ensure_ascii=False) if gap_report else None
        await self.db.execute(
            "UPDATE backtest_runs SET status = 'failed', error_message = ?, "
            "data_gap_report = ?, completed_at = ? WHERE id = ?",
            (error, gap_json, get_china_time().isoformat(), run_id)
        )

    def _resolve_stock_pool(self, strategy_config: Dict) -> List[str]:
        """从策略配置中解析股票池"""
        # 1. 直接指定 stock_pool
        stock_pool = strategy_config.get("stock_pool")
        if stock_pool:
            return stock_pool

        # 2. 按市场筛选
        markets = strategy_config.get("markets")
        if markets:
            from services.factors.kline_manager import get_kline_manager
            km = get_kline_manager()
            all_stocks = km.get_all_stocks()
            return [c for c in all_stocks if c.split(".")[-1] in markets]

        # 3. 默认：数据库中所有股票（排除 BJ 北交所和 SI 指数）
        from services.factors.kline_manager import get_kline_manager
        km = get_kline_manager()
        all_stocks = km.get_all_stocks()
        return [c for c in all_stocks if c.split(".")[-1] in ("SH", "SZ")]

    async def run_segmented_backtest(
        self,
        run_id: int,
        strategy_config: Dict,
        mode: str,
        start_date: str,
        end_date: str,
        initial_capital: float,
        pool_schedule: List[Dict],
        fee_config: Optional[FeeConfig] = None,
        position_limits: Optional[PositionLimits] = None,
        slippage_pct: float = 0.0,
        stop_loss_pct: Optional[float] = None,
        take_profit_pct: Optional[float] = None,
        trailing_stop_pct: Optional[float] = None,
        trading_strategies: Optional[List[Dict]] = None,  # 预加载的策略代码
        progress_callback=None,
        is_subprocess: bool = False,
    ) -> Dict:
        """
        分段回测：每段独立运行，持仓和现金在段间传递。
        段只需要 start_date，结束日期自动取下一段的 start_date 或整体 end_date。

        Args:
            pool_schedule: [
                {"start_date": "2024-01-01", "stock_pool": ["..."], "strategy_config": {...}},
                ...
            ]
            trading_strategies: 预加载的交易策略代码
            is_subprocess: 子进程模式下跳过数据库写入
        """
        from services.backtest.modes.simulated_trading import SimulatedTradingEngine
        from datetime import datetime, timedelta

        if not pool_schedule:
            return {"error": "pool_schedule 不能为空"}

        # 校验并补全 end_date：段之间必须连续
        resolved_schedule = []
        for i, seg in enumerate(pool_schedule):
            seg_start = seg.get("start_date")
            if not seg_start:
                return {"error": f"分段 {i+1} 缺少 start_date"}
            # 结束日期取下一段开始日期，或整体 end_date
            if i + 1 < len(pool_schedule):
                seg_end = pool_schedule[i + 1]["start_date"]
            else:
                seg_end = end_date

            resolved_schedule.append({
                "start_date": seg_start,
                "end_date": seg_end,
                "stock_pool": seg.get("stock_pool", []),
                "strategy_config": seg.get("strategy_config") or strategy_config,
            })

        # 校验连续性：第一段 start_date 必须 <= 整体 start_date，段间不能有重叠
        if resolved_schedule[0]["start_date"] > start_date:
            return {"error": f"第一段 start_date ({resolved_schedule[0]['start_date']}) 不能晚于整体 start_date ({start_date})"}

        # 计算总交易日数（用于进度条）
        total_trade_days = 0
        for seg in resolved_schedule:
            days = self.km.get_all_trade_dates(seg["start_date"], seg["end_date"]) if self.km else []
            total_trade_days += len(days)

        if total_trade_days == 0:
            return {"error": "分段日期范围内无交易日"}

        # 累积结果
        all_trades: List[Dict] = []
        all_nav: List[Dict] = []
        all_daily_positions: List[Dict] = []
        carried_positions: List[Dict] = []  # 段间传递的持仓
        carried_cash: Optional[float] = None

        # 逐段执行
        for seg_idx, seg in enumerate(resolved_schedule):
            is_last = seg_idx == len(pool_schedule) - 1
            seg_start = seg["start_date"]
            seg_end = seg["end_date"]
            seg_pool = seg.get("stock_pool", [])
            seg_strategy = seg.get("strategy_config") or strategy_config

            logger.warn("backtest", f"分段 {seg_idx+1}/{len(pool_schedule)}: {seg_start} ~ {seg_end}, "
                           f"股票池 {len(seg_pool)} 只, 初始现金={carried_cash or initial_capital:.0f}, "
                           f"持仓 {len(carried_positions)} 只")

            # 构造本段的进度回调（子进程模式下跳过数据库写入）
            def seg_progress(current, total, trade_date="", idx=seg_idx):
                acc_days = sum(
                    len(self.km.get_all_trade_dates(s["start_date"], s["end_date"]))
                    for s in resolved_schedule[:idx]
                ) if self.km else 0
                acc = acc_days + current
                pct = round(acc / total_trade_days * 100, 1) if total_trade_days > 0 else 0
                if is_subprocess:
                    # 子进程模式下不写数据库，只调用外部回调传递进度
                    if progress_callback:
                        progress_callback(acc, total_trade_days, trade_date)
                    return
                try:
                    import sqlite3
                    from services.common.database import get_sync_connection
                    conn = get_sync_connection("stockwinner")
                    # 分段进度标注：[段号/总数] 日期 (累计天数/总天数)
                    label = f"[{idx+1}/{len(resolved_schedule)}] {trade_date} ({acc}/{total_trade_days})"
                    conn.execute(
                        "UPDATE backtest_runs SET progress = ?, current_trade_date = ? WHERE id = ?",
                        (pct, label, run_id)
                    )
                    conn.commit()
                    conn.close()
                except Exception as e:
                    logger.warn("backtest", f"分段进度保存失败: {e}")
                # 调用外部回调
                if progress_callback:
                    progress_callback(acc, total_trade_days, trade_date)

            # 创建本段引擎
            fee = fee_config or FeeConfig(
                commission_rate=seg_strategy.get("commission_rate", 0.0001),
                min_commission=seg_strategy.get("min_commission", 5.0),
                stamp_tax=seg_strategy.get("stamp_tax", 0.0005),
                transfer_fee=seg_strategy.get("transfer_fee", 0.00002),
            )
            limits = position_limits or PositionLimits(
                max_total_position_pct=seg_strategy.get("max_total_position_pct", 0.80),
                max_single_position_pct=seg_strategy.get("max_single_position_pct", 0.15),
                cash_reserve_pct=seg_strategy.get("cash_reserve_pct", 0.10),
            )

            engine = SimulatedTradingEngine(
                strategy_config=seg_strategy,
                initial_capital=initial_capital,
                start_date=seg_start,
                end_date=seg_end,
                stock_pool=seg_pool,
                fee_config=fee,
                position_limits=limits,
                slippage_pct=slippage_pct,
                stop_loss_pct=stop_loss_pct or seg_strategy.get("stop_loss_pct"),
                take_profit_pct=take_profit_pct or seg_strategy.get("take_profit_pct"),
                trailing_stop_pct=trailing_stop_pct or seg_strategy.get("trailing_stop_pct"),
                stop_execution_price=seg_strategy.get("stop_execution_price", "close"),
                initial_positions=carried_positions if carried_positions else None,
                initial_cash=carried_cash,
                liquidate_at_end=is_last,
                trading_strategies=trading_strategies,  # 使用预加载的策略
            )

            # 执行本段
            seg_result = engine.run(progress_callback=seg_progress)

            if "error" in seg_result:
                return {"error": f"分段 {seg_idx+1} 执行失败: {seg_result['error']}"}

            # 合并结果
            all_trades.extend(seg_result.get("trades", []))
            all_nav.extend(seg_result.get("nav_series", []))
            all_daily_positions.extend(seg_result.get("daily_positions", []))

            # 提取本段结束时的状态，传递给下一段
            if not is_last:
                carried_cash = engine.execution.cash
                carried_positions = []
                for code, pos in engine.execution.positions.items():
                    carried_positions.append({
                        "stock_code": code,
                        "stock_name": pos.stock_name,
                        "quantity": pos.quantity,
                        "avg_cost": pos.avg_cost,
                        "buy_date": pos.buy_date,
                        "total_cost": pos.total_cost,
                    })

        # 所有段执行完毕，合并后的汇总指标
        if all_nav:
            first_date = all_nav[0]["trade_date"]
            last_date = all_nav[-1]["trade_date"]
            summary = PerformanceMetrics.compute(
                nav_series=all_nav,
                trades=all_trades,
                initial_capital=initial_capital,
                start_date=first_date,
                end_date=last_date,
            ).to_dict()
        else:
            summary = {"total_return": 0, "annualized_return": 0}

        return {
            "result": summary,
            "trades": all_trades,
            "nav_series": all_nav,
            "daily_positions": all_daily_positions,
            "segment_count": len(pool_schedule),
        }
