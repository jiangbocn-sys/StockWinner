"""
回测子进程工作器

独立进程执行回测：
1. 从主进程接收回测配置（通过 IPC）
2. 执行回测逻辑，所有结果在内存累积
3. 定期发送进度更新（通过 IPC）
4. 回测结束后发送完整结果

数据库访问：
- kline.db：只读（并发安全）
- stockwinner.db：不访问（由主进程写入）

用法：
    from services.backtest.subprocess_worker import spawn_backtest_process
    process, queue = spawn_backtest_process(run_id, config)
    # 监听 queue 获取进度/结果
"""

import os
import sys
import json
import time
import multiprocessing as mp
from typing import Dict, Optional, Any
from pathlib import Path

# 加载 .env
from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parents[2] / ".env")

from services.common.structured_logger import get_logger


def _reinit_logging_for_subprocess():
    """子进程 fork 后重新初始化日志，修复 AsyncLogHandler 线程死亡问题。"""
    import logging
    from services.common.structured_logger import LOG_DIR

    # 清除父进程继承的所有 handler（后台线程已死）
    root = logging.getLogger("StockWinner")
    for h in list(root.handlers):
        root.removeHandler(h)

    # 只在子进程中用同步 StreamHandler 写日志（不经过队列）
    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(fmt)
    handler.setLevel(logging.INFO)
    root.addHandler(handler)

    # 同时写一份到日志文件
    from logging.handlers import TimedRotatingFileHandler
    file_handler = TimedRotatingFileHandler(
        LOG_DIR / "stockwinner.log",
        when="midnight", backupCount=10, encoding="utf-8",
    )
    file_handler.setFormatter(fmt)
    file_handler.setLevel(logging.INFO)
    root.addHandler(file_handler)


def backtest_worker_process(
    run_id: int,
    strategy_config: Dict,
    mode: str,
    start_date: str,
    end_date: str,
    initial_capital: float,
    stock_pool: Optional[list],
    pool_schedule: Optional[list],
    fee_config: Optional[Dict],
    position_limits: Optional[Dict],
    slippage_pct: float,
    stop_loss_pct: Optional[float],
    take_profit_pct: Optional[float],
    trailing_stop_pct: Optional[float],
    trading_strategies: Optional[list],  # 预加载的策略代码
    progress_queue: mp.Queue,
    result_queue: mp.Queue,
):
    """子进程中的回测执行函数。参数通过主进程传入，结果通过队列返回。"""
    _reinit_logging_for_subprocess()
    logger = get_logger("backtest_worker")
    logger.log_event("backtest_worker_started", f"子进程启动: run_id={run_id}, PID={os.getpid()}")

    try:
        # 1. 导入并创建回测引擎（在子进程中）
        from services.backtest.engine import BacktestEngine
        from services.backtest.execution import FeeConfig, PositionLimits

        # 2. 转换配置对象
        fee = FeeConfig(**fee_config) if fee_config else FeeConfig()
        limits = PositionLimits(**position_limits) if position_limits else PositionLimits()

        # 3. 创建引擎（不绑定数据库，纯内存模式）
        engine = BacktestEngine(account_id="subprocess")

        # 4. 执行回测（使用自定义进度回调）
        def progress_callback(current: int, total: int, trade_date: str = ""):
            pct = round(current / total * 100, 1) if total > 0 else 0
            try:
                progress_queue.put({
                    "type": "progress",
                    "run_id": run_id,
                    "current": current,
                    "total": total,
                    "progress": pct,
                    "trade_date": trade_date,
                }, block=False)
            except Exception:
                pass  # 队列满则忽略

        # 5. 执行回测（同步方法，不依赖主进程事件循环）
        # 注意：is_subprocess=True 确保子进程不写数据库，避免 database locked
        if pool_schedule:
            # 分段回测
            result = engine._run_segmented_backtest_sync(
                run_id=run_id,
                strategy_config=strategy_config,
                mode=mode,
                start_date=start_date,
                end_date=end_date,
                initial_capital=initial_capital,
                pool_schedule=pool_schedule,
                fee_config=fee,
                position_limits=limits,
                slippage_pct=slippage_pct,
                stop_loss_pct=stop_loss_pct,
                take_profit_pct=take_profit_pct,
                trailing_stop_pct=trailing_stop_pct,
                trading_strategies=trading_strategies,  # 预加载的策略
                is_subprocess=True,  # 子进程模式：不写数据库
            )
        else:
            # 普通回测
            result = engine._run_backtest_sync(
                run_id=run_id,
                strategy_config=strategy_config,
                mode=mode,
                start_date=start_date,
                end_date=end_date,
                initial_capital=initial_capital,
                stock_pool=stock_pool,
                fee_config=fee,
                position_limits=limits,
                slippage_pct=slippage_pct,
                stop_loss_pct=stop_loss_pct,
                take_profit_pct=take_profit_pct,
                trailing_stop_pct=trailing_stop_pct,
                trading_strategies=trading_strategies,  # 预加载的策略
                is_subprocess=True,  # 子进程模式：不写数据库
            )

        # 6. 发送最终结果（包含 trades, nav_series, daily_positions）
        result_queue.put({
            "type": "result",
            "run_id": run_id,
            "success": "error" not in result,
            "result": result,
        })

        logger.log_event("backtest_worker_completed", f"子进程完成: run_id={run_id}")

    except Exception as e:
        import traceback
        tb_str = traceback.format_exc()
        logger.error("backtest_worker_error", f"子进程异常: {e}\n{tb_str}")
        result_queue.put({
            "type": "error",
            "run_id": run_id,
            "error": str(e),
            "traceback": tb_str,
        })


def spawn_backtest_process(
    run_id: int,
    strategy_config: Dict,
    mode: str = "simulated",
    start_date: str = "",
    end_date: str = "",
    initial_capital: float = 1000000,
    stock_pool: Optional[list] = None,
    pool_schedule: Optional[list] = None,
    fee_config: Optional[Dict] = None,
    position_limits: Optional[Dict] = None,
    slippage_pct: float = 0.0,
    stop_loss_pct: Optional[float] = None,
    take_profit_pct: Optional[float] = None,
    trailing_stop_pct: Optional[float] = None,
    trading_strategy_ids: Optional[list] = None,
) -> tuple:
    """
    启动回测子进程。

    Returns:
        (process, progress_queue, result_queue)
        - process: multiprocessing.Process 对象
        - progress_queue: 进度更新队列
        - result_queue: 最终结果队列
    """
    # 主进程预加载交易策略代码（避免子进程访问 stockwinner.db）
    trading_strategies = []
    if trading_strategy_ids:
        from services.common.database import get_sync_connection
        conn = get_sync_connection("stockwinner")
        cursor = conn.cursor()
        # 从 strategies 表加载当前有效的交易策略
        placeholders = ",".join(["?"] * len(trading_strategy_ids))
        cursor.execute(
            f"SELECT id, name, code, function_name FROM strategies WHERE id IN ({placeholders}) AND code_scope = 'trading'",
            trading_strategy_ids
        )
        for row in cursor.fetchall():
            trading_strategies.append({
                "id": row[0],
                "name": row[1],
                "code": row[2],
                "function_name": row[3] or "run",
            })
        conn.close()

    # 创建队列
    progress_queue = mp.Queue(maxsize=100)  # 进度更新，避免阻塞
    result_queue = mp.Queue(maxsize=1)       # 最终结果，只存一条

    # 创建进程
    process = mp.Process(
        target=backtest_worker_process,
        args=(
            run_id,
            strategy_config,
            mode,
            start_date,
            end_date,
            initial_capital,
            stock_pool,
            pool_schedule,
            fee_config,
            position_limits,
            slippage_pct,
            stop_loss_pct,
            take_profit_pct,
            trailing_stop_pct,
            trading_strategies,  # 预加载的策略代码
            progress_queue,
            result_queue,
        ),
        name=f"backtest-{run_id}",
    )

    process.start()
    return process, progress_queue, result_queue


def _save_results_to_db_sync(run_id: int, result: Dict, benchmark_data: list):
    """
    保存回测结果到数据库（同步连接，批量写入，减少锁持有时间）

    在主进程中执行，通过 asyncio.to_thread 调用。
    """
    from services.common.database import get_sync_connection
    from services.common.timezone import get_china_time
    from services.backtest.metrics import PerformanceMetrics
    import json

    trades = result.get("trades", [])
    nav_series = result.get("nav_series", [])
    daily_positions = result.get("daily_positions", [])
    summary = result.get("result", {})

    # 如果有基准数据，重新计算含对比的指标
    if benchmark_data and nav_series and trades:
        initial_capital = summary.get("initial_capital", 1000000)
        first_date = nav_series[0]["trade_date"] if nav_series else ""
        last_date = nav_series[-1]["trade_date"] if nav_series else ""
        recomputed = PerformanceMetrics.compute(
            nav_series=nav_series,
            trades=trades,
            initial_capital=initial_capital,
            start_date=first_date,
            end_date=last_date,
            benchmark_series=benchmark_data,
        )
        summary = recomputed.to_dict()

    conn = get_sync_connection("stockwinner")
    cursor = conn.cursor()

    try:
        # 批量保存交易记录
        for trade in trades:
            if trade.get("trade_type") == "buy":
                cursor.execute(
                    """INSERT INTO backtest_trades
                       (backtest_run_id, stock_code, stock_name, buy_date, buy_price,
                        buy_quantity, remaining_quantity, buy_commission)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (run_id, trade["stock_code"], trade.get("stock_name", ""),
                     trade["date"], trade["price"], trade["quantity"],
                     trade["quantity"], trade.get("commission", 0))
                )
            elif trade.get("trade_type") == "sell":
                sell_qty = trade.get("sell_quantity", trade.get("quantity", 0))
                remaining_qty = trade.get("remaining_quantity", 0)
                cursor.execute(
                    """INSERT INTO backtest_trades
                       (backtest_run_id, stock_code, stock_name, buy_date, buy_price,
                        buy_quantity, buy_commission,
                        sell_date, sell_price, sell_quantity, remaining_quantity,
                        sell_commission, sell_reason, pnl, pnl_pct, holding_days)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (run_id, trade.get("stock_code", ""), trade.get("stock_name", ""),
                     trade.get("buy_date", ""), trade.get("buy_price", 0),
                     trade.get("quantity", 0), trade.get("buy_commission", 0),
                     trade["date"], trade["price"], sell_qty, remaining_qty,
                     trade.get("commission", 0), trade.get("reason", ""),
                     trade.get("pnl", 0), trade.get("pnl_pct", 0), trade.get("holding_days", 0))
                )

        # 批量保存每日净值
        for nav in nav_series:
            cursor.execute(
                """INSERT OR REPLACE INTO backtest_daily_nav
                   (backtest_run_id, trade_date, nav, total_value, cash,
                    positions_value, position_count, drawdown, max_drawdown, daily_return)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (run_id, nav["trade_date"], nav.get("nav", 1.0), nav.get("total_value", 0),
                 nav.get("cash", 0), nav.get("positions_value", 0), nav.get("position_count", 0),
                 nav.get("drawdown", 0), nav.get("max_drawdown", 0), nav.get("daily_return", 0))
            )

        # 批量保存每日持仓
        cursor.execute("DELETE FROM backtest_daily_positions WHERE backtest_run_id = ?", (run_id,))
        for pos in daily_positions:
            cursor.execute(
                """INSERT INTO backtest_daily_positions
                   (backtest_run_id, trade_date, stock_code, stock_name,
                    quantity, avg_cost, close_price, market_value, unrealized_pnl)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (run_id, pos["trade_date"], pos["stock_code"], pos.get("stock_name", ""),
                 pos.get("quantity", 0), pos.get("avg_cost", 0), pos.get("close_price", 0),
                 pos.get("market_value", 0), pos.get("unrealized_pnl", 0))
            )

        # 更新状态为完成
        cursor.execute(
            "UPDATE backtest_runs SET status = 'completed', completed_at = ?, result_summary = ? WHERE id = ?",
            (get_china_time().isoformat(), json.dumps(summary, ensure_ascii=False), run_id)
        )

        conn.commit()
    finally:
        conn.close()


def _mark_backtest_failed_sync(run_id: int, error_msg: str):
    """同步函数：标记回测失败（通过 asyncio.to_thread 调用）"""
    from services.common.database import get_sync_connection
    from services.common.timezone import get_china_time
    conn = get_sync_connection("stockwinner")
    conn.execute(
        "UPDATE backtest_runs SET status = 'failed', error_message = ?, completed_at = ? WHERE id = ?",
        (error_msg, get_china_time().isoformat(), run_id)
    )
    conn.commit()
    conn.close()


class BacktestProcessManager:
    """
    回测子进程管理器。

    负责：
    1. 启动子进程
    2. 监听进度更新并写入数据库
    3. 接收最终结果并保存到数据库
    4. 处理超时、异常、进程崩溃
    """

    def __init__(self, timeout_seconds: int = 3600):
        self.timeout = timeout_seconds
        self._active_processes: Dict[int, tuple] = {}  # {run_id: (process, queues)}

    async def execute_backtest(
        self,
        run_id: int,
        strategy_config: Dict,
        mode: str = "simulated",
        start_date: str = "",
        end_date: str = "",
        initial_capital: float = 1000000,
        stock_pool: Optional[list] = None,
        pool_schedule: Optional[list] = None,
        fee_config: Optional[Dict] = None,
        position_limits: Optional[Dict] = None,
        slippage_pct: float = 0.0,
        stop_loss_pct: Optional[float] = None,
        take_profit_pct: Optional[float] = None,
        trailing_stop_pct: Optional[float] = None,
        trading_strategy_ids: Optional[list] = None,
    ) -> Dict:
        """
        执行回测并返回结果。

        在主进程事件循环中异步等待，但回测逻辑在子进程中同步执行。
        """
        import asyncio
        from services.common.timezone import get_china_time

        logger = get_logger("backtest_manager")

        # 1. 更新状态为 running（通过线程隔离，避免阻塞事件循环）
        from services.common.database import get_sync_connection
        now = get_china_time().isoformat()
        def _set_running():
            conn = get_sync_connection("stockwinner")
            conn.execute(
                "UPDATE backtest_runs SET status = 'running', started_at = ? WHERE id = ?",
                (now, run_id)
            )
            conn.commit()
            conn.close()
        await asyncio.to_thread(_set_running)

        # 2. 启动子进程
        process, progress_queue, result_queue = spawn_backtest_process(
            run_id=run_id,
            strategy_config=strategy_config,
            mode=mode,
            start_date=start_date,
            end_date=end_date,
            initial_capital=initial_capital,
            stock_pool=stock_pool,
            pool_schedule=pool_schedule,
            fee_config=fee_config,
            position_limits=position_limits,
            slippage_pct=slippage_pct,
            stop_loss_pct=stop_loss_pct,
            take_profit_pct=take_profit_pct,
            trailing_stop_pct=trailing_stop_pct,
            trading_strategy_ids=trading_strategy_ids,
        )

        self._active_processes[run_id] = (process, progress_queue, result_queue)
        logger.log_event("backtest_process_spawned", f"启动子进程: run_id={run_id}, PID={process.pid}")

        # 3. 异步监听进度和结果
        final_result = None
        start_time = time.monotonic()

        async def _poll_progress():
            """轮询进度队列（DB写入通过线程隔离，不阻塞事件循环）"""
            while process.is_alive() and final_result is None:
                try:
                    if not progress_queue.empty():
                        msg = progress_queue.get(block=False)
                        if msg.get("type") == "progress":
                            # 通过线程池执行同步写入，避免 blocked event loop
                            def _write_progress(p, td, rid):
                                conn = get_sync_connection("stockwinner")
                                conn.execute(
                                    "UPDATE backtest_runs SET progress = ?, current_trade_date = ? WHERE id = ?",
                                    (p, td, rid)
                                )
                                conn.commit()
                                conn.close()
                            await asyncio.to_thread(
                                _write_progress, msg["progress"], msg["trade_date"], run_id
                            )
                except Exception as e:
                    logger.warn("backtest_progress", f"进度更新失败: {e}")

                await asyncio.sleep(0.5)

        async def _poll_result():
            """等待最终结果"""
            while process.is_alive():
                try:
                    if not result_queue.empty():
                        final_result = result_queue.get(block=False)
                        return final_result
                except Exception:
                    pass
                await asyncio.sleep(0.5)

            # 进程已结束，检查是否有结果
            if not result_queue.empty():
                return result_queue.get(block=False)
            return None

        # 3. 并发监听进度和结果
        progress_task = asyncio.create_task(_poll_progress())

        try:
            # 等待结果（带超时）
            while final_result is None:
                if time.monotonic() - start_time > self.timeout:
                    # 超时：终止进程
                    logger.warn("backtest_timeout", f"回测超时 ({self.timeout}s)，终止进程: run_id={run_id}")
                    process.terminate()
                    process.join(timeout=5)
                    if process.is_alive():
                        process.kill()
                    final_result = {"type": "error", "run_id": run_id, "error": "回测超时"}
                    break

                # 检查结果
                if not result_queue.empty():
                    final_result = result_queue.get(block=False)
                    break

                # 检查进程是否意外死亡
                if not process.is_alive() and result_queue.empty():
                    # 进程死亡但无结果 → 崩溃
                    logger.error("backtest_crash", f"进程崩溃: run_id={run_id}")
                    final_result = {"type": "error", "run_id": run_id, "error": "进程崩溃"}
                    break

                await asyncio.sleep(0.5)

        finally:
            progress_task.cancel()
            try:
                await progress_task
            except asyncio.CancelledError:
                pass

        # 4. 清理进程
        if process.is_alive():
            process.join(timeout=10)
            if process.is_alive():
                process.terminate()

        self._active_processes.pop(run_id, None)

        # 5. 保存结果到数据库
        if final_result:
            if final_result.get("type") == "result" and final_result.get("success"):
                result_data = final_result.get("result", {})
                # 获取基准数据
                benchmark_data = await self._fetch_benchmark_data(start_date, end_date)
                # 保存（调用 BacktestEngine._save_results）
                await self._save_results_to_db(run_id, result_data, benchmark_data)
                return result_data
            else:
                # 标记失败（使用同步连接，通过线程隔离）
                error_msg = final_result.get("error", "未知错误")
                await asyncio.to_thread(
                    _mark_backtest_failed_sync, run_id, error_msg
                )
                return {"error": error_msg}

        return {"error": "无结果"}

    async def _fetch_benchmark_data(self, start_date: str, end_date: str) -> list:
        """获取基准数据（沪深300）"""
        from services.factors.kline_manager import get_kline_manager
        km = get_kline_manager()
        kline = km.get_kline_data("000300.SH", start_date=start_date, end_date=end_date)
        if kline is None or len(kline) == 0:
            return []
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

    async def _save_results_to_db(self, run_id: int, result: Dict, benchmark_data: list):
        """保存回测结果到数据库（使用同步连接，批量写入）"""
        import asyncio
        await asyncio.to_thread(_save_results_to_db_sync, run_id, result, benchmark_data)

    def get_active_count(self) -> int:
        """获取活跃回测进程数量"""
        return len([p for p, _, _ in self._active_processes.values() if p.is_alive()])

    def terminate_all(self):
        """终止所有活跃进程"""
        for run_id, (process, _, _) in list(self._active_processes.items()):
            if process.is_alive():
                process.terminate()
                process.join(timeout=5)
                if process.is_alive():
                    process.kill()
        self._active_processes.clear()


# 全局管理器实例
_manager: Optional[BacktestProcessManager] = None


def get_backtest_process_manager() -> BacktestProcessManager:
    """获取全局回测进程管理器"""
    global _manager
    if _manager is None:
        _manager = BacktestProcessManager()
    return _manager