"""
选股模块 (Stock Screening) — 协调器
根据策略条件扫描股票市场，筛选符合条件的股票

优化版本 (v2):
- 优先使用 stock_daily_factors 表中的预计算因子
- 只对缺失因子进行动态计算
- 支持可扩展的因子注册表
"""

import asyncio
from typing import Dict, List, Optional, Any

from services.common.database import get_db_manager
from services.common.account_manager import get_account_manager
from services.common.timezone import get_china_time, format_china_time
from services.common.structured_logger import get_logger

from .factor_registry import get_factor_registry
from .condition_parser import get_condition_parser, normalize_conditions
from .condition_evaluator import ConditionEvaluator
from .candidate_manager import CandidateManager


class ScreeningService:
    """选股服务 — 协调器，委托给子模块执行"""

    def __init__(self):
        self._running = False
        self._task = None
        self._progress = {
            "total_stocks": 0, "processed": 0, "matched": 0,
            "current_phase": "idle",
            "current_stock": None, "start_time": None,
            "estimated_remaining": None
        }
        self._current_account_id: Optional[str] = None
        self._current_strategy_id: Optional[int] = None

        # 进度回调函数
        def progress_callback(prog: dict):
            if self._current_account_id and self._current_strategy_id:
                from services.ui.dashboard import update_screening_progress
                update_screening_progress(self._current_account_id, self._current_strategy_id, {
                    "percent": prog.get("processed", 0) / max(prog.get("total_stocks", 1), 1) * 100,
                    "message": f"已处理 {prog.get('processed', 0)}/{prog.get('total_stocks', 0)}",
                    "processed": prog.get("processed", 0),
                    "total_stocks": prog.get("total_stocks", 0),
                    "matched": prog.get("matched", 0),
                    "current_stock": prog.get("current_stock", ""),
                    "start_time": prog.get("start_time"),
                })

        self._evaluator = ConditionEvaluator(self._progress, progress_callback)
        self._candidate_mgr = CandidateManager()

    async def start_screening(self, account_id: str, strategy_id: Optional[int] = None, interval: int = 60):
        """启动选股服务"""
        if self._running:
            return {"success": False, "message": "选股服务已在运行"}

        self._running = True
        self._task = asyncio.create_task(self._run_screening_loop(account_id, strategy_id, interval))
        return {"success": True, "message": "选股服务已启动"}

    async def stop_screening(self):
        """停止选股服务"""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        return {"success": True, "message": "选股服务已停止"}

    async def _run_screening_loop(self, account_id: str, strategy_id: Optional[int], interval: int):
        """选股扫描循环"""
        log = get_logger("screening")
        log.log_event("screening_start", f"启动选股服务",
                      account_id=account_id, interval=interval, strategy_id=strategy_id)

        while self._running:
            try:
                await self._run_screening(account_id, strategy_id)
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error("screening", f"选股扫描错误: {e}")
                await asyncio.sleep(interval)

        log.log_event("screening_stop", "选股服务已停止")

    async def _run_screening(
        self, account_id: str, strategy_id: Optional[int],
        use_local: bool = True, pending_to_temp: bool = False,
        require_active: bool = True, override_stock_scope: Optional[str] = None
    ):
        """执行一次选股扫描"""
        db = get_db_manager()
        account_manager = get_account_manager()
        log = get_logger("screening")

        if not await account_manager.validate_account(account_id):
            return

        # 清除临时表
        if pending_to_temp:
            await db.execute("DELETE FROM temp_candidates WHERE account_id = ?", (account_id,))

        # 重置进度
        self.reset_progress()
        self._progress["start_time"] = get_china_time().isoformat()
        self._progress["current_phase"] = "fetching_list"
        self._current_account_id = account_id  # 设置当前账户ID供回调使用

        # 导入进度更新函数
        from services.ui.dashboard import update_screening_progress, clear_screening_progress

        # 获取策略列表
        if strategy_id:
            if require_active:
                strategies = [await db.fetchone(
                    "SELECT * FROM strategies WHERE id = ? AND account_id = ? AND status = 'active'",
                    (strategy_id, account_id)
                )]
            else:
                strategies = [await db.fetchone(
                    "SELECT * FROM strategies WHERE id = ? AND account_id = ?",
                    (strategy_id, account_id)
                )]
        else:
            strategies = await db.fetchall(
                "SELECT * FROM strategies WHERE account_id = ? AND status = 'active'",
                (account_id,)
            )

        total_strategies = len([s for s in strategies if s])

        for strategy_idx, strategy in enumerate(strategies):
            if not strategy:
                continue

            current_strategy_id = strategy.get('id')
            self._current_strategy_id = current_strategy_id  # 设置当前策略ID供回调使用

            log.info("screening", f"执行策略 {strategy_idx + 1}/{total_strategies}: {strategy.get('name')}")

            # 标记筛选开始
            self._progress["current_phase"] = "scanning"
            update_screening_progress(account_id, current_strategy_id, {
                "percent": 0,
                "message": f"开始筛选: {strategy.get('name')}",
                "start_time": self._progress["start_time"],
            })

            strategy_type = strategy.get('strategy_type', 'screening')
            if strategy_type == 'python':
                stock_scope = override_stock_scope or 'market'
                await self._evaluator.execute_python_strategy(
                    account_id, strategy, stock_scope, pending_to_temp,
                    candidate_manager=self._candidate_mgr
                )
                # 更新进度
                update_screening_progress(account_id, current_strategy_id, {
                    "percent": self._progress.get("processed", 0) / max(self._progress.get("total_stocks", 1), 1) * 100,
                    "message": f"已处理 {self._progress.get('processed', 0)}/{self._progress.get('total_stocks', 0)}",
                    "processed": self._progress.get("processed", 0),
                    "total_stocks": self._progress.get("total_stocks", 0),
                    "matched": self._progress.get("matched", 0),
                    "current_stock": self._progress.get("current_stock", ""),
                    "start_time": self._progress.get("start_time"),
                })
                continue

            config = self._parse_config(strategy.get('config'))
            if not config:
                continue

            group_id = await self._candidate_mgr.ensure_candidate_group(
                account_id, strategy.get('id'), strategy.get('name')
            )

            match_score_threshold = strategy.get('match_score_threshold', 0.5)
            if match_score_threshold is None:
                match_score_threshold = config.get('match_score_threshold', 0.5)

            try:
                candidates = await self._evaluator.evaluate_optimized(config, match_score_threshold)
            except Exception as e:
                log.warn("screening", f"优化模式失败，回退到传统模式：{e}")
                candidates = await self._evaluator.evaluate_local(config, match_score_threshold)

            for candidate in candidates:
                if pending_to_temp:
                    await self._candidate_mgr.add_to_temp_candidates(
                        account_id, strategy.get('id'), candidate, config, group_id=group_id
                    )
                else:
                    await self._candidate_mgr.add_to_watchlist(
                        account_id, strategy.get('id'), candidate, config, group_id=group_id
                    )

            # 筛选完成，清除进度
            clear_screening_progress(account_id, current_strategy_id)
            log.info("screening", f"策略 {strategy.get('name')} 筛选完成，入选 {len(candidates)} 只")

        self._progress["current_phase"] = "done"

    @staticmethod
    def _parse_config(config) -> Dict:
        import json
        if not config:
            return {}
        if isinstance(config, str):
            try:
                return json.loads(config)
            except Exception:
                return {}
        return config

    async def confirm_candidates(
        self, account_id: str, stock_codes: Optional[List[str]] = None, confirm: bool = True
    ) -> Dict:
        """确认或拒绝临时候选股票"""
        return await self._candidate_mgr.confirm_candidates(account_id, stock_codes, confirm)

    def get_status(self) -> Dict:
        return {"running": self._running, "task": "active" if self._task else None}

    def get_progress(self) -> Dict:
        return self._progress.copy()

    def reset_progress(self):
        self._progress = {
            "total_stocks": 0, "processed": 0, "matched": 0,
            "current_phase": "idle",
            "current_stock": None, "start_time": None,
            "estimated_remaining": None
        }


# 全局单例
_screening_service: Optional[ScreeningService] = None


def get_screening_service() -> ScreeningService:
    global _screening_service
    if _screening_service is None:
        _screening_service = ScreeningService()
    return _screening_service


def reset_screening_service():
    global _screening_service
    _screening_service = None
