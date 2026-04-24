"""
策略管理 API
"""

from fastapi import APIRouter, HTTPException, Path, Body
from typing import List, Optional, Dict, Any
from datetime import datetime, timezone, timedelta
from services.common.database import get_db_manager
import json

# 中国时区
CHINA_TZ = timezone(timedelta(hours=8))

def get_china_time():
    """获取中国时区时间"""
    return datetime.now(CHINA_TZ).replace(tzinfo=None)

router = APIRouter()


@router.get("/api/v1/ui/{account_id}/strategies")
async def get_strategies(
    account_id: str = Path(..., description="账户 ID"),
    status: Optional[str] = None
):
    """获取策略列表"""
    db = get_db_manager()

    # 从数据库验证账户
    account = await db.fetchone(
        "SELECT 1 FROM accounts WHERE account_id = ? AND is_active = 1",
        (account_id,)
    )
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    if status:
        strategies = await db.fetchall(
            "SELECT * FROM strategies WHERE account_id = ? AND status = ? ORDER BY created_at DESC",
            (account_id, status)
        )
    else:
        strategies = await db.fetchall(
            "SELECT * FROM strategies WHERE account_id = ? ORDER BY created_at DESC",
            (account_id,)
        )

    return {
        "account_id": account_id,
        "strategies": strategies
    }


@router.get("/api/v1/ui/{account_id}/strategies/{strategy_id}")
async def get_strategy(
    account_id: str = Path(..., description="账户 ID"),
    strategy_id: int = Path(..., description="策略 ID")
):
    """获取策略详情"""
    db = get_db_manager()

    # 从数据库验证账户
    account = await db.fetchone(
        "SELECT 1 FROM accounts WHERE account_id = ? AND is_active = 1",
        (account_id,)
    )
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在：{account_id}")

    strategy = await db.fetchone(
        "SELECT * FROM strategies WHERE id = ? AND account_id = ?",
        (strategy_id, account_id)
    )

    if not strategy:
        raise HTTPException(status_code=404, detail="策略不存在")

    # 解析配置
    if strategy.get('config'):
        try:
            strategy['config'] = json.loads(strategy['config'])
        except:
            pass

    return {"strategy": strategy}


@router.post("/api/v1/ui/{account_id}/strategies")
async def create_strategy(
    account_id: str = Path(..., description="账户 ID"),
    name: str = Body(..., description="策略名称"),
    description: Optional[str] = Body(None, description="策略描述"),
    strategy_type: str = Body("manual", description="策略类型：manual/llm"),
    config: Optional[Dict[str, Any]] = Body(None, description="策略配置"),
    match_score_threshold: Optional[float] = Body(0.5, description="匹配度阈值")
):
    """创建新策略"""
    db = get_db_manager()

    # 从数据库验证账户
    account = await db.fetchone(
        "SELECT 1 FROM accounts WHERE account_id = ? AND is_active = 1",
        (account_id,)
    )
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在：{account_id}")

    strategy_data = {
        "account_id": account_id,
        "name": name,
        "description": description,
        "strategy_type": strategy_type,
        "config": json.dumps(config) if config else None,
        "status": "draft",
        "match_score_threshold": match_score_threshold,
        "created_at": get_china_time().strftime("%Y-%m-%d %H:%M:%S"),
        "updated_at": get_china_time().strftime("%Y-%m-%d %H:%M:%S")
    }

    strategy_id = await db.insert("strategies", strategy_data)

    strategy = await db.fetchone(
        "SELECT * FROM strategies WHERE id = ?",
        (strategy_id,)
    )

    return {
        "success": True,
        "message": "策略创建成功",
        "strategy": strategy
    }


@router.put("/api/v1/ui/{account_id}/strategies/{strategy_id}")
async def update_strategy(
    account_id: str = Path(..., description="账户 ID"),
    strategy_id: int = Path(..., description="策略 ID"),
    name: Optional[str] = Body(None, description="策略名称"),
    description: Optional[str] = Body(None, description="策略描述"),
    config: Optional[Dict[str, Any]] = Body(None, description="策略配置"),
    status: Optional[str] = Body(None, description="策略状态")
):
    """更新策略"""
    db = get_db_manager()

    # 从数据库验证账户
    account = await db.fetchone(
        "SELECT 1 FROM accounts WHERE account_id = ? AND is_active = 1",
        (account_id,)
    )
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在：{account_id}")

    # 检查策略是否存在
    strategy = await db.fetchone(
        "SELECT * FROM strategies WHERE id = ? AND account_id = ?",
        (strategy_id, account_id)
    )

    if not strategy:
        raise HTTPException(status_code=404, detail="策略不存在")

    update_data = {}
    if name is not None:
        update_data["name"] = name
    if description is not None:
        update_data["description"] = description
    if config is not None:
        update_data["config"] = json.dumps(config)
    if status is not None:
        update_data["status"] = status

    if update_data:
        update_data["updated_at"] = get_china_time().strftime("%Y-%m-%d %H:%M:%S")
        await db.update("strategies", update_data, "id = ?", (strategy_id,))

    strategy = await db.fetchone(
        "SELECT * FROM strategies WHERE id = ?",
        (strategy_id,)
    )

    return {
        "success": True,
        "message": "策略更新成功",
        "strategy": strategy
    }


@router.delete("/api/v1/ui/{account_id}/strategies/{strategy_id}")
async def delete_strategy(
    account_id: str = Path(..., description="账户 ID"),
    strategy_id: int = Path(..., description="策略 ID")
):
    """删除策略"""
    db = get_db_manager()

    # 从数据库验证账户
    account = await db.fetchone(
        "SELECT 1 FROM accounts WHERE account_id = ? AND is_active = 1",
        (account_id,)
    )
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在：{account_id}")

    # 检查策略是否存在
    strategy = await db.fetchone(
        "SELECT * FROM strategies WHERE id = ? AND account_id = ?",
        (strategy_id, account_id)
    )

    if not strategy:
        raise HTTPException(status_code=404, detail="策略不存在")

    # 先删除关联的临时候选数据（避免外键约束失败）
    await db.execute("DELETE FROM temp_candidates WHERE strategy_id = ?", (strategy_id,))

    # 删除策略
    await db.delete("strategies", "id = ?", (strategy_id,))

    return {
        "success": True,
        "message": "策略删除成功"
    }


@router.post("/api/v1/ui/{account_id}/strategies/{strategy_id}/activate")
async def activate_strategy(
    account_id: str = Path(..., description="账户 ID"),
    strategy_id: int = Path(..., description="策略 ID")
):
    """激活策略"""
    db = get_db_manager()

    # 从数据库验证账户
    account = await db.fetchone(
        "SELECT 1 FROM accounts WHERE account_id = ? AND is_active = 1",
        (account_id,)
    )
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在：{account_id}")

    strategy = await db.fetchone(
        "SELECT * FROM strategies WHERE id = ? AND account_id = ?",
        (strategy_id, account_id)
    )

    if not strategy:
        raise HTTPException(status_code=404, detail="策略不存在")

    await db.update("strategies", {"status": "active"}, "id = ?", (strategy_id,))

    return {
        "success": True,
        "message": "策略已激活"
    }


@router.post("/api/v1/ui/{account_id}/strategies/{strategy_id}/deactivate")
async def deactivate_strategy(
    account_id: str = Path(..., description="账户 ID"),
    strategy_id: int = Path(..., description="策略 ID")
):
    """停用策略"""
    db = get_db_manager()

    # 从数据库验证账户
    account = await db.fetchone(
        "SELECT 1 FROM accounts WHERE account_id = ? AND is_active = 1",
        (account_id,)
    )
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在：{account_id}")

    strategy = await db.fetchone(
        "SELECT * FROM strategies WHERE id = ? AND account_id = ?",
        (strategy_id, account_id)
    )

    if not strategy:
        raise HTTPException(status_code=404, detail="策略不存在")

    await db.update("strategies", {"status": "inactive"}, "id = ?", (strategy_id,))

    return {
        "success": True,
        "message": "策略已停用"
    }


@router.get("/api/v1/ui/{account_id}/strategies/{strategy_id}/backtest")
async def get_backtest_result(
    account_id: str = Path(..., description="账户 ID"),
    strategy_id: int = Path(..., description="策略 ID")
):
    """获取回测结果（模拟）"""
    db = get_db_manager()

    # 从数据库验证账户
    account = await db.fetchone(
        "SELECT 1 FROM accounts WHERE account_id = ? AND is_active = 1",
        (account_id,)
    )
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在：{account_id}")

    strategy = await db.fetchone(
        "SELECT * FROM strategies WHERE id = ? AND account_id = ?",
        (strategy_id, account_id)
    )

    if not strategy:
        raise HTTPException(status_code=404, detail="策略不存在")

    # TODO: 实现真实的回测逻辑
    # 这里返回模拟数据
    return {
        "strategy_id": strategy_id,
        "strategy_name": strategy.get("name"),
        "backtest_result": {
            "status": "simulated",
            "message": "回测功能开发中",
            "data": {
                "total_return": 0.15,
                "annual_return": 0.22,
                "sharpe_ratio": 1.5,
                "max_drawdown": 0.08,
                "win_rate": 0.65,
                "total_trades": 0
            }
        }
    }


@router.post("/api/v1/ui/{account_id}/strategies/generate")
async def generate_strategy_by_llm(
    account_id: str = Path(..., description="账户 ID"),
    description: str = Body(..., embed=True, description="策略描述")
):
    """使用 LLM 生成选股策略"""
    db = get_db_manager()

    # 从数据库验证账户
    account = await db.fetchone(
        "SELECT 1 FROM accounts WHERE account_id = ? AND is_active = 1",
        (account_id,)
    )
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在：{account_id}")

    try:
        # 使用 LLM 生成策略
        from services.llm.strategy_generator import get_strategy_generator
        generator = get_strategy_generator()
        result = generator.generate(description)

        # 获取生成的配置
        config = result["config"]

        # 验证生成的条件是否可识别
        from services.common.indicators import validate_condition
        validated_conditions = []
        validation_warnings = []

        buy_conditions = config.get("buy_conditions", [])
        for cond in buy_conditions:
            validation = validate_condition(cond)
            validated_conditions.append({
                "condition": cond,
                "valid": validation["valid"],
                "reason": validation["reason"],
                "normalized": validation["normalized"]
            })
            if not validation["valid"]:
                validation_warnings.append(f"买入条件 '{cond}' 无法识别: {validation['reason']}")

        return {
            "success": True,
            "message": "LLM 选股策略生成成功",
            "strategy": {
                "name": f"LLM选股策略-{get_china_time().strftime('%Y%m%d%H%M')}",
                "description": description,
                "strategy_type": "screening",
                "config": config,
                "validated_conditions": validated_conditions,
                "validation_warnings": validation_warnings,
                "source": "llm"
            }
        }
    except Exception as e:
        # 返回错误原因
        return {
            "success": False,
            "message": f"LLM 策略生成失败：{str(e)}",
            "error": str(e)
        }
