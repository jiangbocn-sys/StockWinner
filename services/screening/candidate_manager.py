"""
候选管理器 — watchlist 添加、临时候选管理、候选确认/拒绝、候选组管理。
"""
from typing import Dict, List, Optional

from services.common.database import get_db_manager
from services.common.timezone import format_china_time
from services.common.structured_logger import get_logger


class CandidateManager:
    """候选股票管理：watchlist / temp_candidates / candidate_groups"""

    async def ensure_candidate_group(self, account_id: str, strategy_id: int, strategy_name: str) -> int:
        """确保策略有对应的候选组，无则自动创建"""
        db = get_db_manager()
        group = await db.fetchone(
            "SELECT id FROM candidate_groups WHERE account_id = ? AND group_type = 'screening' AND screening_strategy_id = ?",
            (account_id, strategy_id)
        )
        if group:
            return group['id']

        group_id = await db.insert("candidate_groups", {
            "account_id": account_id,
            "name": f"策略: {strategy_name or strategy_id}",
            "group_type": "screening",
            "screening_strategy_id": strategy_id,
        })
        print(f"[Screening] 自动创建候选组: 策略: {strategy_name} (id={group_id})")
        return group_id

    async def add_to_watchlist(
        self,
        account_id: str, strategy_id: int, candidate: Dict,
        strategy_config: Optional[Dict] = None,
        group_id: Optional[int] = None,
        status: str = "watching",
    ):
        """将候选股票添加到 watchlist"""
        db = get_db_manager()

        existing = await db.fetchone(
            "SELECT id FROM watchlist WHERE account_id = ? AND stock_code = ? AND group_id = ?",
            (account_id, candidate['stock_code'], group_id)
        )
        if existing:
            return

        current_price = candidate.get('current_price', 0)
        stop_loss_pct = strategy_config.get('stop_loss_pct', 0.05) if strategy_config else 0.05
        take_profit_pct = strategy_config.get('take_profit_pct', 0.15) if strategy_config else 0.15
        stop_loss = current_price * (1 - stop_loss_pct)
        take_profit = current_price * (1 + take_profit_pct)

        target_quantity = await self._calc_quantity(strategy_config, account_id, current_price)

        watchlist_data = {
            "account_id": account_id,
            "strategy_id": strategy_id,
            "group_id": group_id,
            "source_type": "screening",
            "stock_code": candidate['stock_code'],
            "stock_name": candidate.get('stock_name', ''),
            "reason": candidate.get('reason', ''),
            "trigger_price": current_price,
            "stop_loss_price": stop_loss,
            "take_profit_price": take_profit,
            "target_quantity": target_quantity,
            "status": status,
            "created_at": format_china_time(),
            "updated_at": format_china_time()
        }

        await db.insert("watchlist", watchlist_data)
        get_logger("screening").log_event("screening_add_watchlist",
            f"选股加入 watchlist: {candidate['stock_code']}",
            account_id=account_id, stock_code=candidate['stock_code'],
            stock_name=candidate.get('stock_name', ''),
            strategy_id=strategy_id,
            trigger_price=current_price, stop_loss=stop_loss,
            take_profit=take_profit, target_quantity=target_quantity)

    async def add_to_temp_candidates(
        self,
        account_id: str, strategy_id: int, candidate: Dict,
        strategy_config: Optional[Dict] = None,
        group_id: Optional[int] = None,
    ):
        """将候选股票暂存到临时表（待用户确认）"""
        db = get_db_manager()

        existing = await db.fetchone(
            "SELECT id FROM temp_candidates WHERE account_id = ? AND stock_code = ?",
            (account_id, candidate['stock_code'])
        )
        if existing:
            return

        current_price = candidate.get('current_price', 0)
        stop_loss_pct = strategy_config.get('stop_loss_pct', 0.05) if strategy_config else 0.05
        take_profit_pct = strategy_config.get('take_profit_pct', 0.15) if strategy_config else 0.15
        stop_loss = current_price * (1 - stop_loss_pct)
        take_profit = current_price * (1 + take_profit_pct)

        target_quantity = await self._calc_quantity(strategy_config, account_id, current_price)

        temp_data = {
            "account_id": account_id,
            "strategy_id": strategy_id,
            "group_id": group_id,
            "stock_code": candidate['stock_code'],
            "stock_name": candidate.get('stock_name', ''),
            "reason": candidate.get('reason', ''),
            "trigger_price": current_price,
            "stop_loss_price": stop_loss,
            "take_profit_price": take_profit,
            "target_quantity": target_quantity,
            "match_score": candidate.get('match_score', 0),
            "created_at": format_china_time()
        }

        await db.insert("temp_candidates", temp_data)
        print(f"[Screening] 暂存候选：{candidate['stock_code']} - {candidate.get('stock_name')} (匹配度：{candidate.get('match_score', 0)*100:.0f}%)")

    async def confirm_candidates(
        self, account_id: str, stock_codes: Optional[List[str]] = None, confirm: bool = True
    ) -> Dict:
        """确认或拒绝临时候选股票"""
        db = get_db_manager()
        result = {"success": True, "confirmed": 0, "rejected": 0}

        if stock_codes:
            for stock_code in stock_codes:
                candidate = await db.fetchone(
                    "SELECT * FROM temp_candidates WHERE account_id = ? AND stock_code = ?",
                    (account_id, stock_code)
                )
                if not candidate:
                    continue

                if confirm:
                    await db.insert("watchlist", {
                        "account_id": account_id,
                        "strategy_id": candidate['strategy_id'],
                        "group_id": candidate.get('group_id'),
                        "source_type": "screening",
                        "stock_code": stock_code,
                        "stock_name": candidate['stock_name'],
                        "reason": candidate['reason'],
                        "trigger_price": candidate['trigger_price'],
                        "stop_loss_price": candidate['stop_loss_price'],
                        "take_profit_price": candidate['take_profit_price'],
                        "target_quantity": candidate['target_quantity'],
                        "status": "pending",
                        "created_at": format_china_time(),
                        "updated_at": format_china_time()
                    })
                    result["confirmed"] += 1
                    get_logger("screening").log_event("screening_confirm_pending",
                        f"确认选股加入 watchlist: {stock_code}",
                        account_id=account_id, stock_code=stock_code,
                        stock_name=candidate['stock_name'],
                        strategy_id=candidate['strategy_id'],
                        trigger_price=candidate['trigger_price'])
                else:
                    result["rejected"] += 1

                await db.delete("temp_candidates", "account_id = ? AND stock_code = ?", (account_id, stock_code))
        else:
            candidates = await db.fetchall(
                "SELECT * FROM temp_candidates WHERE account_id = ?", (account_id,)
            )
            for candidate in candidates:
                if confirm:
                    await db.insert("watchlist", {
                        "account_id": account_id,
                        "strategy_id": candidate['strategy_id'],
                        "group_id": candidate.get('group_id'),
                        "source_type": "screening",
                        "stock_code": candidate['stock_code'],
                        "stock_name": candidate['stock_name'],
                        "reason": candidate['reason'],
                        "trigger_price": candidate['trigger_price'],
                        "stop_loss_price": candidate['stop_loss_price'],
                        "take_profit_price": candidate['take_profit_price'],
                        "target_quantity": candidate['target_quantity'],
                        "status": "pending",
                        "created_at": format_china_time(),
                        "updated_at": format_china_time()
                    })
                    result["confirmed"] += 1
                else:
                    result["rejected"] += 1

            await db.delete("temp_candidates", "account_id = ?", (account_id,))

        return result

    async def _calc_quantity(self, strategy_config, account_id, current_price) -> Optional[int]:
        """根据策略配置计算目标买入数量"""
        if not strategy_config:
            return None

        if strategy_config.get('quantity'):
            return int(strategy_config.get('quantity'))

        if strategy_config.get('position_pct'):
            position_pct = float(strategy_config.get('position_pct', 0.1))
            db = get_db_manager()
            account = await db.fetchone(
                "SELECT available_cash FROM accounts WHERE account_id = ?",
                (account_id,)
            )
            if account:
                available_cash = account.get('available_cash', 0)
                buy_amount = available_cash * position_pct
                if current_price > 0:
                    return int((buy_amount / current_price) // 100) * 100

        return None
