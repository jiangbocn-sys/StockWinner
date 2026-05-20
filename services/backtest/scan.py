"""
参数扫描器

对给定参数范围进行网格搜索，批量运行回测并对比结果。
"""

import asyncio
import itertools
import logging
from typing import Dict, List, Optional, Any
from concurrent.futures import ThreadPoolExecutor

from services.common.database import get_db_manager
from services.common.structured_logger import get_logger
from services.common.timezone import get_china_time
from services.backtest.execution import FeeConfig, PositionLimits
from services.backtest.engine import BacktestEngine

logger = get_logger("backtest")


class ParameterScanner:
    """参数扫描器"""

    SCAN_MAX_COMBINATIONS = 20

    # 可扫描的参数及其默认值
    SCANNABLE_PARAMS = {
        "stop_loss_pct",
        "take_profit_pct",
        "trailing_stop_pct",
        "slippage_pct",
        "commission_rate",
        "stamp_tax",
        "transfer_fee",
        "min_commission",
        "max_total_position_pct",
        "max_single_position_pct",
        "cash_reserve_pct",
    }

    def __init__(self, account_id: str):
        self.account_id = account_id
        self.db = get_db_manager()

    @staticmethod
    def generate_combinations(
        base_config: Dict,
        param_grid: Dict[str, List],
    ) -> List[Dict]:
        """生成参数网格的笛卡尔积，每个组合与 base_config 合并"""
        if not param_grid:
            raise ValueError("param_grid 不能为空")

        keys = sorted(param_grid.keys())
        for k in keys:
            if not isinstance(param_grid[k], list) or len(param_grid[k]) == 0:
                raise ValueError(f"参数 {k} 的值必须是非空列表")

        values = [param_grid[k] for k in keys]
        combinations = []
        for combo in itertools.product(*values):
            params = {**base_config}
            for k, v in zip(keys, combo):
                params[k] = v
            combinations.append(params)
        return combinations

    async def run_scan(
        self,
        base_config: Dict,
        param_grid: Dict[str, List],
        sort_by: str = "annualized_return",
        sort_desc: bool = True,
    ) -> Dict:
        """执行参数扫描，返回对比表"""
        combinations = self.generate_combinations(base_config, param_grid)
        if len(combinations) > self.SCAN_MAX_COMBINATIONS:
            raise ValueError(
                f"组合数 {len(combinations)} 超过上限 {self.SCAN_MAX_COMBINATIONS}，请减少参数范围"
            )

        scan_name = base_config.get("name", "参数扫描")

        # 为每个组合创建回测任务
        run_entries = []
        for i, combo in enumerate(combinations, 1):
            run_id = await self._create_scan_run(combo, scan_name, i)
            run_entries.append({"run_id": run_id, "params": combo, "status": "pending"})

        # 在线程池中执行所有回测
        results = await self._execute_all(run_entries, base_config)

        # 构建对比表
        comparison = self._build_comparison(run_entries, results, sort_by, sort_desc)

        return {
            "success": True,
            "scan_id": run_entries[0]["run_id"],
            "total_combinations": len(combinations),
            "completed": sum(1 for r in results if r.get("status") == "completed"),
            "failed": sum(1 for r in results if r.get("status") == "failed"),
            "comparison": comparison,
        }

    async def _create_scan_run(self, combo: Dict, scan_name: str, index: int) -> int:
        """为扫描组合创建回测任务"""
        engine = BacktestEngine(account_id=self.account_id)
        run_id = await engine.create_run(
            name=f"[scan] {scan_name} #{index}",
            strategy_id=combo.get("strategy_id"),
            mode=combo.get("mode", "simulated"),
            start_date=combo.get("start_date", ""),
            end_date=combo.get("end_date", ""),
            initial_capital=float(combo.get("initial_capital", 1000000)),
            stock_pool=combo.get("stock_pool"),
            markets=combo.get("markets"),
            config=combo.get("config", {}),
        )

        # 保存扫描参数到对应列
        for key in self.SCANNABLE_PARAMS:
            if key in combo and combo[key] is not None:
                await self.db.execute(
                    f"UPDATE backtest_runs SET {key} = ? WHERE id = ?",
                    (combo[key], run_id)
                )

        return run_id

    async def _execute_all(
        self, run_entries: List[Dict], base_config: Dict
    ) -> List[Dict]:
        """在线程池中执行所有回测"""
        loop = asyncio.get_event_loop()
        max_workers = min(4, len(run_entries))

        def run_one(entry):
            combo = entry["params"]
            engine = BacktestEngine(account_id=self.account_id)

            fee_config = FeeConfig(
                commission_rate=combo.get("commission_rate", 0.0001),
                min_commission=combo.get("min_commission", 5.0),
                stamp_tax=combo.get("stamp_tax", 0.0005),
                transfer_fee=combo.get("transfer_fee", 0.00002),
            )
            position_limits = PositionLimits(
                max_total_position_pct=combo.get("max_total_position_pct", 0.80),
                max_single_position_pct=combo.get("max_single_position_pct", 0.15),
                cash_reserve_pct=combo.get("cash_reserve_pct", 0.10),
            )

            strategy_config = {**combo.get("config", {})}
            for k, v in combo.items():
                if v is not None:
                    strategy_config[k] = v

            try:
                result = engine._run_backtest_sync(
                    run_id=entry["run_id"],
                    strategy_config=strategy_config,
                    mode=combo.get("mode", "simulated"),
                    start_date=combo.get("start_date", ""),
                    end_date=combo.get("end_date", ""),
                    initial_capital=float(combo.get("initial_capital", 1000000)),
                    stock_pool=combo.get("stock_pool"),
                    fee_config=fee_config,
                    position_limits=position_limits,
                    slippage_pct=float(combo.get("slippage_pct", 0.0)),
                    stop_loss_pct=combo.get("stop_loss_pct"),
                    take_profit_pct=combo.get("take_profit_pct"),
                    trailing_stop_pct=combo.get("trailing_stop_pct"),
                )
                return {"run_id": entry["run_id"], "status": "completed", "result": result}
            except Exception as e:
                logger.error("backtest", f"扫描回测 {entry['run_id']} 失败: {e}")
                try:
                    import asyncio as aio
                    loop2 = aio.new_event_loop()
                    aio.set_event_loop(loop2)
                    loop2.run_until_complete(
                        engine._mark_failed(entry["run_id"], str(e), None)
                    )
                    loop2.close()
                except Exception:
                    pass
                return {"run_id": entry["run_id"], "status": "failed", "error": str(e)}

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [loop.run_in_executor(executor, run_one, entry) for entry in run_entries]
            results = await asyncio.gather(*futures)

        return results

    @staticmethod
    def _build_comparison(
        run_entries: List[Dict],
        results: List[Dict],
        sort_by: str,
        sort_desc: bool,
    ) -> List[Dict]:
        """构建对比表"""
        comparison = []
        for i, entry in enumerate(run_entries):
            result = results[i]
            row = {
                "run_id": entry["run_id"],
                "rank": 0,
                "status": result.get("status", "unknown"),
                "parameters": {},
                "metrics": {},
            }

            # 提取扫描参数（只在 param_grid 中出现的）
            for k, v in entry["params"].items():
                if k in ParameterScanner.SCANNABLE_PARAMS:
                    row["parameters"][k] = v

            if result.get("status") == "completed":
                r = result.get("result", {})
                summary = r.get("result", {}) if isinstance(r.get("result"), dict) else r

                row["metrics"] = {
                    "total_return": summary.get("total_return"),
                    "annualized_return": summary.get("annualized_return"),
                    "max_drawdown": summary.get("max_drawdown"),
                    "sharpe_ratio": summary.get("sharpe_ratio"),
                    "calmar_ratio": summary.get("calmar_ratio"),
                    "win_rate": summary.get("win_rate"),
                    "profit_factor": summary.get("profit_factor"),
                    "total_trades": summary.get("total_trades"),
                    "avg_holding_days": summary.get("avg_holding_days"),
                    "best_trade": summary.get("best_trade"),
                    "worst_trade": summary.get("worst_trade"),
                    "final_nav": summary.get("final_nav"),
                    "benchmark_return": summary.get("benchmark_return"),
                    "alpha": summary.get("alpha"),
                    "beta": summary.get("beta"),
                }
            else:
                row["error"] = result.get("error", "未知错误")

            comparison.append(row)

        # 排序：failed 排最后，completed 按指标排序
        valid = [r for r in comparison if r["status"] == "completed" and r["metrics"].get(sort_by) is not None]
        invalid = [r for r in comparison if r["status"] != "completed" or r["metrics"].get(sort_by) is None]

        valid.sort(key=lambda x: x["metrics"].get(sort_by) or 0, reverse=sort_desc)

        for i, r in enumerate(valid):
            r["rank"] = i + 1

        return valid + invalid
