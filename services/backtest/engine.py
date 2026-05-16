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

logger = get_logger("backtest")


class BacktestEngine:
    """回测引擎"""

    def __init__(self, account_id: str = ""):
        self.account_id = account_id
        self.db = get_db_manager()

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
    ) -> Dict:
        """同步版本的 run_backtest，用于在线程池中执行。"""
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
                )
            )
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
    ) -> Dict:
        """
        执行回测。

        Returns:
            回测结果字典
        """
        # 1. 更新状态为 running
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

            # 4. 数据完整性检查（股票池超过 1000 只时做采样检查，避免阻塞）
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
                )
            elif mode == "return_accumulation":
                result = await self._run_return_accumulation(
                    run_id, strategy_config, start_date, end_date,
                    initial_capital, stock_pool,
                )
            else:
                result = {"error": f"不支持的回测模式: {mode}"}

            return result

        except Exception as e:
            logger.error(f"回测执行失败 (run_id={run_id}): {e}", exc_info=True)
            await self._mark_failed(run_id, str(e), None)
            return {"error": str(e)}

    async def _run_simulated(
        self, run_id, strategy_config, start_date, end_date,
        initial_capital, stock_pool, fee_config, position_limits,
        slippage_pct, stop_loss_pct, take_profit_pct, trailing_stop_pct,
    ) -> Dict:
        """执行撮合模拟盘模式"""
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
        )

        # 进度回调（同步版本，直接执行 DB 更新）
        last_saved = [0]  # 用列表实现 mutable nonlocal
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

                # 每 2% 计算一次实时指标
                if pct - last_saved[0] >= 2.0:
                    from services.backtest.metrics import PerformanceMetrics
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
                conn.close()
            except Exception:
                pass

        result = engine.run(progress_callback=progress)

        if "error" in result:
            await self._mark_failed(run_id, result["error"], None)
            return result

        # 保存结果
        await self._save_results(run_id, result)
        return result

    async def _run_return_accumulation(
        self, run_id, strategy_config, start_date, end_date,
        initial_capital, stock_pool,
    ) -> Dict:
        """执行收益率累积模式"""
        from services.backtest.modes.return_accumulation import ReturnAccumulationEngine

        stop_loss_pct = strategy_config.get("stop_loss_pct")
        take_profit_pct = strategy_config.get("take_profit_pct")

        engine = ReturnAccumulationEngine(
            strategy_config=strategy_config,
            initial_capital=initial_capital,
            start_date=start_date,
            end_date=end_date,
            stock_pool=stock_pool,
            stop_loss_pct=stop_loss_pct,
            take_profit_pct=take_profit_pct,
            holding_period=strategy_config.get("holding_period", 5),
        )

        result = engine.run()

        if "error" in result:
            await self._mark_failed(run_id, result["error"], None)
            return result

        await self._save_results(run_id, result)
        return result

    async def _check_data_completeness(
        self, stock_pool: List[str], start_date: str, end_date: str
    ) -> DataGapReport:
        """数据完整性检查"""
        checker = DataCompletenessChecker()
        report = await checker.check(stock_pool, start_date, end_date)
        return report

    async def _save_results(self, run_id: int, result: Dict):
        """保存回测结果到数据库"""
        backtest_trades = result.get("trades", [])
        nav_series = result.get("nav_series", [])
        daily_positions = result.get("daily_positions", [])
        summary = result.get("result", {})

        # 1. 保存交易记录
        for trade in backtest_trades:
            if trade.get("trade_type") == "buy":
                await self.db.insert("backtest_trades", {
                    "backtest_run_id": run_id,
                    "stock_code": trade["stock_code"],
                    "stock_name": trade.get("stock_name", ""),
                    "buy_date": trade["date"],
                    "buy_price": trade["price"],
                    "buy_quantity": trade["quantity"],
                    "buy_commission": trade.get("commission", 0),
                })
            elif trade.get("trade_type") == "sell":
                # 查找对应的买入记录并更新
                buy_date = trade.get("buy_date", "")
                stock_code = trade.get("stock_code", "")
                existing = await self.db.fetchone(
                    "SELECT id FROM backtest_trades WHERE backtest_run_id = ? AND stock_code = ? AND buy_date = ?",
                    (run_id, stock_code, buy_date)
                )
                if existing:
                    await self.db.execute(
                        "UPDATE backtest_trades SET sell_date = ?, sell_price = ?, sell_commission = ?, "
                        "sell_reason = ?, pnl = ?, pnl_pct = ?, holding_days = ? WHERE id = ?",
                        (
                            trade["date"], trade["price"], trade.get("commission", 0),
                            trade.get("reason", ""), trade.get("pnl", 0), trade.get("pnl_pct", 0),
                            trade.get("holding_days", 0), existing["id"]
                        )
                    )
                else:
                    # 找不到买入记录（可能是回测开始前就持仓的），直接插入
                    await self.db.insert("backtest_trades", {
                        "backtest_run_id": run_id,
                        "stock_code": stock_code,
                        "stock_name": trade.get("stock_name", ""),
                        "buy_date": trade.get("buy_date", ""),
                        "buy_price": trade.get("buy_price", 0),
                        "buy_quantity": trade.get("quantity", 0),
                        "buy_commission": trade.get("buy_commission", 0),
                        "sell_date": trade["date"],
                        "sell_price": trade["price"],
                        "sell_commission": trade.get("commission", 0),
                        "sell_reason": trade.get("reason", ""),
                        "pnl": trade.get("pnl", 0),
                        "pnl_pct": trade.get("pnl_pct", 0),
                        "holding_days": trade.get("holding_days", 0),
                    })

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

        # 3. 保存每日持仓快照
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
