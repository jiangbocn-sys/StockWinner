"""
信号分配配置 API
"""

from fastapi import APIRouter, HTTPException, Path, Body
from typing import Dict, Any
from services.common.database import get_db_manager
from services.common.structured_logger import get_logger
import json

router = APIRouter()
logger = get_logger("signal_allocator_api")

# 默认配置
DEFAULT_SIGNAL_ALLOCATION = {
    "max_stocks": 5,
    "allocation_mode": "equal",
    "min_amount_per_stock": 1000,
    "max_position_pct": 20,
    "score_field": "score",
}


@router.get("/api/v1/ui/{account_id}/strategies/{strategy_id}/signal-allocation")
async def get_signal_allocation_config(
    account_id: str = Path(...),
    strategy_id: int = Path(...),
):
    """获取策略的信号分配配置"""
    db = get_db_manager()

    # 验证账户
    account = await db.fetchone(
        "SELECT 1 FROM accounts WHERE account_id = ? AND is_active = 1",
        (account_id,)
    )
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在：{account_id}")

    # 获取策略
    strategy = await db.fetchone(
        "SELECT id, name, config FROM strategies WHERE id = ? AND account_id = ?",
        (strategy_id, account_id)
    )
    if not strategy:
        raise HTTPException(status_code=404, detail=f"策略不存在：{strategy_id}")

    # 解析配置
    config = DEFAULT_SIGNAL_ALLOCATION.copy()
    if strategy.get("config"):
        try:
            strategy_config = json.loads(strategy["config"]) if isinstance(strategy["config"], str) else strategy["config"]
            if "signal_allocation" in strategy_config:
                config.update(strategy_config["signal_allocation"])
        except (json.JSONDecodeError, TypeError):
            pass

    return {
        "success": True,
        "strategy_id": strategy_id,
        "strategy_name": strategy.get("name", ""),
        "config": config,
        "defaults": DEFAULT_SIGNAL_ALLOCATION,
    }


@router.put("/api/v1/ui/{account_id}/strategies/{strategy_id}/signal-allocation")
async def update_signal_allocation_config(
    account_id: str = Path(...),
    strategy_id: int = Path(...),
    updates: Dict[str, Any] = Body(..., description="分配配置更新"),
):
    """更新策略的信号分配配置

    可配置参数：
    - max_stocks: 单次最多买入股票数 (1-20)
    - allocation_mode: 分配模式 (equal/weighted/top_n)
    - min_amount_per_stock: 单股最小金额 (100-100000)
    - max_position_pct: 单股最大仓位百分比 (1-100)
    - score_field: 评分字段名
    """
    db = get_db_manager()

    # 验证账户
    account = await db.fetchone(
        "SELECT 1 FROM accounts WHERE account_id = ? AND is_active = 1",
        (account_id,)
    )
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在：{account_id}")

    # 获取策略
    strategy = await db.fetchone(
        "SELECT id, config FROM strategies WHERE id = ? AND account_id = ?",
        (strategy_id, account_id)
    )
    if not strategy:
        raise HTTPException(status_code=404, detail=f"策略不存在：{strategy_id}")

    # 参数验证
    if "max_stocks" in updates:
        val = updates["max_stocks"]
        if not isinstance(val, int) or val < 1 or val > 20:
            raise HTTPException(status_code=400, detail="max_stocks 必须是 1-20")

    if "allocation_mode" in updates:
        val = updates["allocation_mode"]
        if val not in ("equal", "weighted", "top_n"):
            raise HTTPException(status_code=400, detail="allocation_mode 必须是 equal/weighted/top_n")

    if "min_amount_per_stock" in updates:
        val = updates["min_amount_per_stock"]
        if not isinstance(val, (int, float)) or val < 100 or val > 100000:
            raise HTTPException(status_code=400, detail="min_amount_per_stock 必须是 100-100000")

    if "max_position_pct" in updates:
        val = updates["max_position_pct"]
        if not isinstance(val, (int, float)) or val < 1 or val > 100:
            raise HTTPException(status_code=400, detail="max_position_pct 必须是 1-100")

    # 合并配置
    existing_config = {}
    if strategy.get("config"):
        try:
            existing_config = json.loads(strategy["config"]) if isinstance(strategy["config"], str) else strategy["config"]
        except (json.JSONDecodeError, TypeError):
            existing_config = {}

    signal_allocation = existing_config.get("signal_allocation", DEFAULT_SIGNAL_ALLOCATION.copy())
    signal_allocation.update(updates)
    existing_config["signal_allocation"] = signal_allocation

    # 保存
    await db.update(
        "strategies",
        {"config": json.dumps(existing_config, ensure_ascii=False)},
        "id = ? AND account_id = ?",
        (strategy_id, account_id)
    )

    # 清除分配器缓存
    from services.monitoring.signal_allocator import get_signal_allocator
    allocator = get_signal_allocator()
    allocator.clear_cache()

    logger.log_event("allocation_config_updated", f"策略 {strategy_id} 信号分配配置已更新",
                     strategy_id=strategy_id, config=signal_allocation)

    return {
        "success": True,
        "message": "配置已更新",
        "config": signal_allocation,
    }