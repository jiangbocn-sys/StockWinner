"""
持仓调整规则 API
根据市场条件动态调整持仓策略参数
"""

from fastapi import APIRouter, HTTPException, Path, Body
from typing import Optional, List
from datetime import datetime, timezone, timedelta
from services.common.database import get_db_manager

# 中国时区
CHINA_TZ = timezone(timedelta(hours=8))

def get_china_time():
    """获取中国时区时间"""
    return datetime.now(CHINA_TZ).replace(tzinfo=None)

router = APIRouter()


# 支持的触发条件列表
SUPPORTED_TRIGGER_CONDITIONS = {
    # 指数 MACD 信号
    "INDEX_000001_SH_MACD_CROSS_UP_DEA": "上证指数MACD金叉",
    "INDEX_000001_SH_MACD_CROSS_DOWN_DEA": "上证指数MACD死叉",
    "INDEX_399001_SZ_MACD_CROSS_UP_DEA": "深证成指MACD金叉",
    "INDEX_399001_SZ_MACD_CROSS_DOWN_DEA": "深证成指MACD死叉",
    # 指数 MA 信号
    "INDEX_000001_SH_MA5_CROSS_UP_MA10": "上证指数MA5金叉MA10",
    "INDEX_000001_SH_MA5_CROSS_DOWN_MA10": "上证指数MA5死叉MA10",
    # 指数 RSI 信号
    "INDEX_000001_SH_RSI_14_LT_30": "上证指数RSI跌破30（超卖）",
    "INDEX_000001_SH_RSI_14_GT_70": "上证指数RSI突破70（超买）",
}


@router.get("/api/v1/ui/{account_id}/position-rules")
async def list_position_rules(account_id: str = Path(..., description="账户 ID")):
    """获取账户的持仓调整规则列表"""
    db = get_db_manager()

    # 验证账户
    account = await db.fetchone(
        "SELECT 1 FROM accounts WHERE account_id = ? AND is_active = 1",
        (account_id,)
    )
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    rules = await db.fetchall(
        "SELECT * FROM position_adjust_rules WHERE account_id = ? ORDER BY priority DESC, id ASC",
        (account_id,)
    )

    return {
        "success": True,
        "rules": [
            {
                "id": r["id"],
                "account_id": r["account_id"],
                "trigger_condition": r["trigger_condition"],
                "trigger_description": SUPPORTED_TRIGGER_CONDITIONS.get(r["trigger_condition"], "未知条件"),
                "target_max_total_pct": float(r.get("target_max_total_pct") or 0),
                "target_max_single_pct": float(r.get("target_max_single_pct") or 0),
                "description": r.get("description") or "",
                "priority": int(r.get("priority") or 0),
                "is_active": bool(r.get("is_active"))
            }
            for r in rules
        ],
        "supported_conditions": SUPPORTED_TRIGGER_CONDITIONS
    }


@router.get("/api/v1/ui/{account_id}/position-rules/supported-conditions")
async def get_supported_conditions(account_id: str = Path(..., description="账户 ID")):
    """获取支持的触发条件列表"""
    return {
        "success": True,
        "conditions": SUPPORTED_TRIGGER_CONDITIONS
    }


@router.post("/api/v1/ui/{account_id}/position-rules")
async def create_position_rule(
    account_id: str = Path(..., description="账户 ID"),
    trigger_condition: str = Body(..., description="触发条件"),
    target_max_total_pct: Optional[float] = Body(None, description="目标总仓位比例"),
    target_max_single_pct: Optional[float] = Body(None, description="目标单股仓位比例"),
    description: Optional[str] = Body(None, description="规则描述"),
    priority: Optional[int] = Body(0, description="优先级"),
    is_active: Optional[int] = Body(1, description="是否启用")
):
    """创建持仓调整规则"""
    db = get_db_manager()

    # 验证账户
    account = await db.fetchone(
        "SELECT 1 FROM accounts WHERE account_id = ? AND is_active = 1",
        (account_id,)
    )
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    # 验证触发条件
    if trigger_condition not in SUPPORTED_TRIGGER_CONDITIONS:
        raise HTTPException(
            status_code=400,
            detail=f"不支持的触发条件：{trigger_condition}。支持的条件：{list(SUPPORTED_TRIGGER_CONDITIONS.keys())}"
        )

    # 验证参数范围
    if target_max_total_pct is not None and not (0.0 <= target_max_total_pct <= 1.0):
        raise HTTPException(status_code=400, detail="总仓位比例必须在0-1之间")
    if target_max_single_pct is not None and not (0.0 <= target_max_single_pct <= 1.0):
        raise HTTPException(status_code=400, detail="单股仓位比例必须在0-1之间")

    # 检查是否已存在相同触发条件
    existing = await db.fetchone(
        "SELECT id FROM position_adjust_rules WHERE account_id = ? AND trigger_condition = ?",
        (account_id, trigger_condition)
    )

    if existing:
        raise HTTPException(status_code=400, detail=f"该触发条件已存在规则，请更新或删除后重新创建")

    # 创建规则
    rule_id = await db.insert(
        "position_adjust_rules",
        {
            "account_id": account_id,
            "trigger_condition": trigger_condition,
            "target_max_total_pct": target_max_total_pct or 0,
            "target_max_single_pct": target_max_single_pct or 0,
            "description": description or SUPPORTED_TRIGGER_CONDITIONS.get(trigger_condition, ""),
            "priority": priority or 0,
            "is_active": is_active or 1
        }
    )

    return {
        "success": True,
        "message": "持仓调整规则创建成功",
        "rule_id": rule_id
    }


@router.put("/api/v1/ui/{account_id}/position-rules/{rule_id}")
async def update_position_rule(
    account_id: str = Path(..., description="账户 ID"),
    rule_id: int = Path(..., description="规则 ID"),
    target_max_total_pct: Optional[float] = Body(None, description="目标总仓位比例"),
    target_max_single_pct: Optional[float] = Body(None, description="目标单股仓位比例"),
    description: Optional[str] = Body(None, description="规则描述"),
    priority: Optional[int] = Body(None, description="优先级"),
    is_active: Optional[int] = Body(None, description="是否启用")
):
    """更新持仓调整规则"""
    db = get_db_manager()

    # 检查规则是否存在
    existing = await db.fetchone(
        "SELECT id FROM position_adjust_rules WHERE account_id = ? AND id = ?",
        (account_id, rule_id)
    )

    if not existing:
        raise HTTPException(status_code=404, detail="规则不存在")

    # 验证参数范围
    if target_max_total_pct is not None and not (0.0 <= target_max_total_pct <= 1.0):
        raise HTTPException(status_code=400, detail="总仓位比例必须在0-1之间")
    if target_max_single_pct is not None and not (0.0 <= target_max_single_pct <= 1.0):
        raise HTTPException(status_code=400, detail="单股仓位比例必须在0-1之间")

    # 更新
    update_fields = []
    params = []

    if target_max_total_pct is not None:
        update_fields.append("target_max_total_pct = ?")
        params.append(target_max_total_pct)
    if target_max_single_pct is not None:
        update_fields.append("target_max_single_pct = ?")
        params.append(target_max_single_pct)
    if description is not None:
        update_fields.append("description = ?")
        params.append(description)
    if priority is not None:
        update_fields.append("priority = ?")
        params.append(priority)
    if is_active is not None:
        update_fields.append("is_active = ?")
        params.append(is_active)

    if update_fields:
        params.append(rule_id)
        await db.execute(
            f"""UPDATE position_adjust_rules SET {", ".join(update_fields)} WHERE id = ?""",
            params
        )

    return {
        "success": True,
        "message": "持仓调整规则更新成功"
    }


@router.delete("/api/v1/ui/{account_id}/position-rules/{rule_id}")
async def delete_position_rule(
    account_id: str = Path(..., description="账户 ID"),
    rule_id: int = Path(..., description="规则 ID")
):
    """删除持仓调整规则"""
    db = get_db_manager()

    # 检查规则是否存在
    existing = await db.fetchone(
        "SELECT id FROM position_adjust_rules WHERE account_id = ? AND id = ?",
        (account_id, rule_id)
    )

    if not existing:
        raise HTTPException(status_code=404, detail="规则不存在")

    await db.execute(
        "DELETE FROM position_adjust_rules WHERE account_id = ? AND id = ?",
        (account_id, rule_id)
    )

    return {
        "success": True,
        "message": "持仓调整规则已删除"
    }


@router.post("/api/v1/ui/{account_id}/position-rules/{rule_id}/toggle")
async def toggle_position_rule(
    account_id: str = Path(..., description="账户 ID"),
    rule_id: int = Path(..., description="规则 ID")
):
    """启用/停用持仓调整规则"""
    db = get_db_manager()

    # 检查规则是否存在
    rule = await db.fetchone(
        "SELECT id, is_active FROM position_adjust_rules WHERE account_id = ? AND id = ?",
        (account_id, rule_id)
    )

    if not rule:
        raise HTTPException(status_code=404, detail="规则不存在")

    new_status = 0 if rule["is_active"] else 1
    await db.execute(
        "UPDATE position_adjust_rules SET is_active = ? WHERE id = ?",
        (new_status, rule_id)
    )

    return {
        "success": True,
        "message": f"规则已{'停用' if new_status == 0 else '启用'}",
        "is_active": new_status
    }