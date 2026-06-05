"""
策略管理 API
"""

from fastapi import APIRouter, HTTPException, Path, Body
from typing import List, Optional, Dict, Any
from datetime import datetime
from services.common.database import get_db_manager
from services.common.timezone import get_china_time, format_china_time
from services.auth.account_validator import validate_account_active, validate_account_exists
import json

router = APIRouter()


@router.get("/api/v1/ui/{account_id}/strategies")
async def get_strategies(
    account_id: str = Path(..., description="账户 ID"),
    status: Optional[str] = None
):
    """获取策略列表"""
    db = get_db_manager()

    # 从数据库验证账户
    await validate_account_active(account_id)

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
    
    await validate_account_active(account_id)

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
    strategy_type: str = Body(..., description="策略类型：screening/python"),
    config: Optional[Dict[str, Any]] = Body(None, description="策略配置"),
    match_score_threshold: Optional[float] = Body(None, description="匹配度阈值"),
    code: Optional[str] = Body(None, description="策略代码"),
    code_type: Optional[str] = Body(None, description="代码类型"),
    code_scope: Optional[str] = Body(None, description="代码范围：screening/trading"),
    target_scope: Optional[str] = Body(None, description="作用域"),
    function_name: Optional[str] = Body(None, description="入口函数名"),
    status: Optional[str] = Body(None, description="策略状态"),
    buy_strategy_id: Optional[int] = Body(None, description="关联买入策略 ID"),
    sell_strategy_id: Optional[int] = Body(None, description="关联卖出策略 ID"),
):
    """创建新策略"""
    db = get_db_manager()

    
    await validate_account_active(account_id)

    # Validate strategy_type
    if strategy_type not in ('screening', 'python'):
        raise HTTPException(status_code=400, detail=f"不支持的 strategy_type: {strategy_type}")

    # 如果是代码型策略，进行自动验证
    validation_warnings = []
    if strategy_type == "python" and code is not None and code.strip():
        from services.strategy.engine import get_strategy_engine
        engine = get_strategy_engine()
        validation = engine.validate_code(code)
        if not validation["valid"]:
            raise HTTPException(status_code=400, detail=f"代码验证失败: {validation['error']}")
        validation_warnings = validation.get("warnings", [])
        # 检查是否有严重警告（DataFrame列名错误）
        critical_warnings = [w for w in validation_warnings if "⚠️" in w and ("列名" in w or "groupby" in w)]
        if critical_warnings:
            # 严重警告：返回错误，阻止保存
            raise HTTPException(
                status_code=400,
                detail=f"代码存在严重问题：\n{chr(10).join(critical_warnings[:3])}\n请修正后再保存"
            )

    strategy_data = {
        "account_id": account_id,
        "name": name,
        "description": description,
        "strategy_type": strategy_type,
        "status": status or "draft",
        "created_at": get_china_time().strftime("%Y-%m-%d %H:%M:%S"),
        "updated_at": get_china_time().strftime("%Y-%m-%d %H:%M:%S")
    }
    if config is not None: strategy_data["config"] = json.dumps(config)
    if match_score_threshold is not None: strategy_data["match_score_threshold"] = match_score_threshold
    if code is not None: strategy_data["code"] = code
    if code_type is not None: strategy_data["code_type"] = code_type
    if code_scope is not None: strategy_data["code_scope"] = code_scope
    if target_scope is not None: strategy_data["target_scope"] = target_scope
    if function_name is not None: strategy_data["function_name"] = function_name
    if buy_strategy_id is not None: strategy_data["buy_strategy_id"] = buy_strategy_id
    if sell_strategy_id is not None: strategy_data["sell_strategy_id"] = sell_strategy_id

    strategy_id = await db.insert("strategies", strategy_data)

    # 版本存档：创建
    try:
        from services.strategy.version_archive import archive_strategy_version
        await archive_strategy_version(account_id, strategy_id, "created", strategy_data)
    except Exception as e:
        print(f"[StrategyVersion] 创建存档失败: {e}")

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
    strategy_type: Optional[str] = Body(None, description="策略类型"),
    config: Optional[Dict[str, Any]] = Body(None, description="策略配置"),
    status: Optional[str] = Body(None, description="策略状态"),
    code: Optional[str] = Body(None, description="策略代码"),
    code_type: Optional[str] = Body(None, description="代码类型"),
    code_scope: Optional[str] = Body(None, description="代码范围"),
    target_scope: Optional[str] = Body(None, description="作用域"),
    function_name: Optional[str] = Body(None, description="入口函数名"),
    buy_strategy_id: Optional[int] = Body(None, description="关联买入策略 ID"),
    sell_strategy_id: Optional[int] = Body(None, description="关联卖出策略 ID"),
):
    """更新策略"""
    db = get_db_manager()

    await validate_account_active(account_id)

    strategy = await db.fetchone(
        "SELECT id FROM strategies WHERE id = ? AND account_id = ?",
        (strategy_id, account_id)
    )
    if not strategy:
        raise HTTPException(status_code=404, detail="策略不存在")

    # 读取旧数据用于 diff
    old_strategy = await db.fetchone(
        "SELECT * FROM strategies WHERE id = ?",
        (strategy_id,)
    )

    update_data = {"updated_at": format_china_time()}
    if name is not None: update_data["name"] = name
    if description is not None: update_data["description"] = description
    if strategy_type is not None: update_data["strategy_type"] = strategy_type
    if config is not None: update_data["config"] = json.dumps(config, ensure_ascii=False)
    if status is not None: update_data["status"] = status
    if code is not None: update_data["code"] = code
    if code_type is not None: update_data["code_type"] = code_type
    if code_scope is not None: update_data["code_scope"] = code_scope
    if target_scope is not None: update_data["target_scope"] = target_scope
    if function_name is not None: update_data["function_name"] = function_name
    if buy_strategy_id is not None:
        if buy_strategy_id == strategy_id:
            raise HTTPException(status_code=400, detail="买入策略不能引用自身")
        ref = await db.fetchone(
            "SELECT strategy_type FROM strategies WHERE id = ? AND account_id = ?",
            (buy_strategy_id, account_id),
        )
        if not ref:
            raise HTTPException(status_code=400, detail="引用的买入策略不存在")
        if ref["strategy_type"] != "python":
            raise HTTPException(status_code=400, detail="买入策略必须是代码型策略 (python)")
        update_data["buy_strategy_id"] = buy_strategy_id
    if sell_strategy_id is not None:
        if sell_strategy_id == strategy_id:
            raise HTTPException(status_code=400, detail="卖出策略不能引用自身")
        ref = await db.fetchone(
            "SELECT strategy_type FROM strategies WHERE id = ? AND account_id = ?",
            (sell_strategy_id, account_id),
        )
        if not ref:
            raise HTTPException(status_code=400, detail="引用的卖出策略不存在")
        if ref["strategy_type"] != "python":
            raise HTTPException(status_code=400, detail="卖出策略必须是代码型策略 (python)")
        update_data["sell_strategy_id"] = sell_strategy_id

    # 如果更新了代码，进行自动验证
    validation_warnings = []
    if code is not None and code.strip():
        from services.strategy.engine import get_strategy_engine
        engine = get_strategy_engine()
        validation = engine.validate_code(code)
        if not validation["valid"]:
            raise HTTPException(status_code=400, detail=f"代码验证失败: {validation['error']}")
        validation_warnings = validation.get("warnings", [])
        # 检查是否有严重警告（DataFrame列名错误）
        critical_warnings = [w for w in validation_warnings if "⚠️" in w and ("列名" in w or "groupby" in w)]
        if critical_warnings:
            # 严重警告：返回错误，阻止保存
            raise HTTPException(
                status_code=400,
                detail=f"代码存在严重问题：\n{chr(10).join(critical_warnings[:3])}\n请修正后再保存"
            )

    if len(update_data) > 1:
        await db.update("strategies", update_data, "id = ?", (strategy_id,))

    # 版本存档：更新
    try:
        from services.strategy.version_archive import archive_strategy_version, compute_diff_summary
        new_strategy = await db.fetchone("SELECT * FROM strategies WHERE id = ?", (strategy_id,))
        diff = compute_diff_summary(dict(old_strategy) if old_strategy else {}, dict(new_strategy) if new_strategy else {})
        await archive_strategy_version(account_id, strategy_id, "updated", dict(new_strategy) if new_strategy else {}, diff_summary=diff)
    except Exception as e:
        print(f"[StrategyVersion] 更新存档失败: {e}")

    return {"success": True, "message": "策略已更新"}


@router.delete("/api/v1/ui/{account_id}/strategies/{strategy_id}")
async def delete_strategy(
    account_id: str = Path(..., description="账户 ID"),
    strategy_id: int = Path(..., description="策略 ID")
):
    """删除策略

    删除前校验：
    - 检查是否有运行中的策略任务
    - 检查是否有策略持仓
    """
    db = get_db_manager()

    # 从数据库验证账户
    
    await validate_account_active(account_id)

    # 检查策略是否存在
    strategy = await db.fetchone(
        "SELECT id, name FROM strategies WHERE id = ? AND account_id = ?",
        (strategy_id, account_id)
    )

    if not strategy:
        raise HTTPException(status_code=404, detail="策略不存在")

    # 1. 检查是否有运行中的策略任务
    running_task = await db.fetchone(
        "SELECT id, name FROM strategy_tasks WHERE strategy_id = ? AND last_status = 'running'",
        (strategy_id,)
    )
    if running_task:
        raise HTTPException(
            status_code=400,
            detail=f"策略「{strategy['name']}」有运行中的任务「{running_task['name']}」，请先停止任务再删除策略"
        )

    # 2. 检查是否有策略持仓（quantity > 0）
    position = await db.fetchone(
        "SELECT stock_code, stock_name, quantity FROM stock_positions WHERE account_id = ? AND strategy_id = ? AND quantity > 0 LIMIT 1",
        (account_id, strategy_id)
    )
    if position:
        raise HTTPException(
            status_code=400,
            detail=f"策略「{strategy['name']}」有持仓「{position['stock_name']} ({position['stock_code']})」{position['quantity']}股，请先清仓再删除策略"
        )

    # 删除前存档
    try:
        from services.strategy.version_archive import archive_before_delete
        await archive_before_delete(account_id, strategy_id)
    except Exception as e:
        print(f"[StrategyVersion] 删除前存档失败: {e}")

    # 先删除关联数据（避免外键约束失败）
    await db.execute("DELETE FROM temp_candidates WHERE strategy_id = ?", (strategy_id,))
    await db.execute("DELETE FROM candidate_groups WHERE screening_strategy_id = ?", (strategy_id,))
    await db.execute("DELETE FROM trading_signals WHERE strategy_id = ? AND account_id = ?", (strategy_id, account_id))

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
    
    await validate_account_active(account_id)

    strategy = await db.fetchone(
        "SELECT * FROM strategies WHERE id = ? AND account_id = ?",
        (strategy_id, account_id)
    )

    if not strategy:
        raise HTTPException(status_code=404, detail="策略不存在")

    await db.update("strategies", {"status": "active"}, "id = ?", (strategy_id,))

    # 版本存档：状态变化
    try:
        from services.strategy.version_archive import archive_strategy_version
        new_strategy = await db.fetchone("SELECT * FROM strategies WHERE id = ?", (strategy_id,))
        await archive_strategy_version(account_id, strategy_id, "updated", dict(new_strategy) if new_strategy else {}, diff_summary="status: inactive→active")
    except Exception as e:
        print(f"[StrategyVersion] 激活存档失败: {e}")

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
    
    await validate_account_active(account_id)

    strategy = await db.fetchone(
        "SELECT * FROM strategies WHERE id = ? AND account_id = ?",
        (strategy_id, account_id)
    )

    if not strategy:
        raise HTTPException(status_code=404, detail="策略不存在")

    await db.update("strategies", {"status": "inactive"}, "id = ?", (strategy_id,))

    # 版本存档：状态变化
    try:
        from services.strategy.version_archive import archive_strategy_version
        new_strategy = await db.fetchone("SELECT * FROM strategies WHERE id = ?", (strategy_id,))
        await archive_strategy_version(account_id, strategy_id, "updated", dict(new_strategy) if new_strategy else {}, diff_summary="status: active→inactive")
    except Exception as e:
        print(f"[StrategyVersion] 停用存档失败: {e}")

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
    
    await validate_account_active(account_id)

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
    
    await validate_account_active(account_id)

    try:
        # 使用 LLM 生成策略
        from services.llm.strategy_generator import get_strategy_generator
        generator = get_strategy_generator(account_id)
        result = generator.generate(description)

        # 获取生成的配置
        config = result["config"]

        # 验证生成的条件是否可识别
        from services.common.indicators import validate_condition
        from services.screening.condition_parser import get_condition_parser, normalize_conditions

        validated_conditions = []
        validation_warnings = []

        # 使用ConditionParser提取基本条件
        parser = get_condition_parser()
        buy_conditions_config = config.get("buy_conditions", {})
        buy_conditions_normalized = normalize_conditions(buy_conditions_config)

        # 提取所有基本条件（用于验证）
        all_conditions = parser.get_all_conditions(buy_conditions_normalized)

        for cond in all_conditions:
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


# ============== 代码型策略 ==============

@router.get("/api/v1/ui/{account_id}/code-strategies")
async def get_code_strategies(
    account_id: str = Path(..., description="账户 ID"),
    code_scope: Optional[str] = None,
):
    """获取代码型策略列表（strategy_type='python'）"""
    db = get_db_manager()

    await validate_account_active(account_id)

    if code_scope:
        strategies = await db.fetchall(
            "SELECT * FROM strategies WHERE account_id = ? AND strategy_type = 'python' AND code_scope = ? ORDER BY created_at DESC",
            (account_id, code_scope)
        )
    else:
        strategies = await db.fetchall(
            "SELECT * FROM strategies WHERE account_id = ? AND strategy_type = 'python' ORDER BY created_at DESC",
            (account_id,)
        )

    return {
        "account_id": account_id,
        "strategies": strategies
    }


@router.put("/api/v1/ui/{account_id}/code-strategies/{strategy_id}")
async def update_code_strategy(
    account_id: str = Path(..., description="账户 ID"),
    strategy_id: int = Path(..., description="策略 ID"),
    name: Optional[str] = Body(None, description="策略名称"),
    description: Optional[str] = Body(None, description="策略描述"),
    code: Optional[str] = Body(None, description="策略代码"),
    code_scope: Optional[str] = Body(None, description="代码范围"),
    function_name: Optional[str] = Body(None, description="入口函数名"),
    status: Optional[str] = Body(None, description="策略状态"),
    config: Optional[str] = Body(None, description="策略配置JSON"),
):
    """更新代码型策略"""
    db = get_db_manager()

    await validate_account_active(account_id)

    strategy = await db.fetchone(
        "SELECT id, strategy_type FROM strategies WHERE id = ? AND account_id = ?",
        (strategy_id, account_id)
    )
    if not strategy:
        raise HTTPException(status_code=404, detail="策略不存在")
    if strategy["strategy_type"] != "python":
        raise HTTPException(status_code=400, detail="该接口只能更新代码型策略")

    # 读取旧数据用于 diff
    old_strategy = await db.fetchone(
        "SELECT * FROM strategies WHERE id = ?",
        (strategy_id,)
    )

    update_data = {"updated_at": format_china_time()}
    if name is not None: update_data["name"] = name
    if description is not None: update_data["description"] = description
    if code is not None: update_data["code"] = code
    if code_scope is not None: update_data["code_scope"] = code_scope
    if function_name is not None: update_data["function_name"] = function_name
    if status is not None: update_data["status"] = status
    if config is not None: update_data["config"] = config

    if len(update_data) > 1:
        await db.update("strategies", update_data, "id = ?", (strategy_id,))

    # 版本存档：更新
    try:
        from services.strategy.version_archive import archive_strategy_version, compute_diff_summary
        new_strategy = await db.fetchone("SELECT * FROM strategies WHERE id = ?", (strategy_id,))
        diff = compute_diff_summary(dict(old_strategy) if old_strategy else {}, dict(new_strategy) if new_strategy else {})
        await archive_strategy_version(account_id, strategy_id, "updated", dict(new_strategy) if new_strategy else {}, diff_summary=diff)
    except Exception as e:
        print(f"[StrategyVersion] 代码策略存档失败: {e}")

    strategy = await db.fetchone(
        "SELECT * FROM strategies WHERE id = ?",
        (strategy_id,)
    )
    return {"success": True, "message": "策略已更新", "strategy": strategy}


# ============== 策略版本存档 ==============


@router.get("/api/v1/ui/{account_id}/strategies/{strategy_id}/versions")
async def get_strategy_versions_list(
    account_id: str = Path(..., description="账户 ID"),
    strategy_id: int = Path(..., description="策略 ID"),
):
    """查询策略版本历史（系统存档，不面向用户展示）"""
    db = get_db_manager()

    
    await validate_account_active(account_id)

    strategy = await db.fetchone(
        "SELECT id FROM strategies WHERE id = ? AND account_id = ?",
        (strategy_id, account_id)
    )
    if not strategy:
        raise HTTPException(status_code=404, detail="策略不存在")

    from services.strategy.version_archive import get_strategy_versions
    versions = await get_strategy_versions(account_id, strategy_id)

    return {
        "strategy_id": strategy_id,
        "account_id": account_id,
        "version_count": len(versions),
        "versions": versions,
    }


@router.post("/api/v1/ui/{account_id}/strategies/{strategy_id}/restore")
async def restore_strategy_version(
    account_id: str = Path(..., description="账户 ID"),
    strategy_id: int = Path(..., description="策略 ID"),
    version: int = Body(..., embed=True, description="要恢复的版本号"),
):
    """从指定版本恢复策略数据"""
    db = get_db_manager()

    
    await validate_account_active(account_id)

    strategy = await db.fetchone(
        "SELECT id FROM strategies WHERE id = ? AND account_id = ?",
        (strategy_id, account_id)
    )
    if not strategy:
        raise HTTPException(status_code=404, detail="策略不存在")

    from services.strategy.version_archive import restore_strategy_from_version
    restored = await restore_strategy_from_version(account_id, strategy_id, version)
    if not restored:
        raise HTTPException(status_code=404, detail=f"版本 #{version} 不存在")

    return {
        "success": True,
        "message": f"策略已恢复到版本 #{version}",
        "restored_version": restored,
    }


# ============== 关联策略查询 ==============

@router.get("/api/v1/ui/{account_id}/sell-strategies")
async def get_sell_strategy_options(account_id: str = Path(..., description="账户 ID")):
    """获取可用作卖出策略的代码型策略列表"""
    db = get_db_manager()
    await validate_account_active(account_id)
    strategies = await db.fetchall(
        "SELECT id, name, description, status FROM strategies "
        "WHERE account_id = ? AND strategy_type = 'python' AND code_scope = 'trading' "
        "ORDER BY name",
        (account_id,)
    )
    return {"success": True, "strategies": strategies}


@router.get("/api/v1/ui/{account_id}/buy-strategies")
async def get_buy_strategy_options(account_id: str = Path(..., description="账户 ID")):
    """获取可用作买入策略的代码型策略列表"""
    db = get_db_manager()
    await validate_account_active(account_id)
    strategies = await db.fetchall(
        "SELECT id, name, description, status FROM strategies "
        "WHERE account_id = ? AND strategy_type = 'python' "
        "ORDER BY name",
        (account_id,)
    )
    return {"success": True, "strategies": strategies}


@router.post("/api/v1/ui/{account_id}/strategies/validate-code")
async def validate_strategy_code(
    account_id: str = Path(..., description="账户 ID"),
    code: str = Body(..., description="策略代码"),
):
    """验证策略代码语法"""
    from services.strategy.engine import get_strategy_engine
    db = get_db_manager()

    await validate_account_active(account_id)

    engine = get_strategy_engine()
    return engine.validate_code(code)


@router.post("/api/v1/ui/{account_id}/strategies/{strategy_id}/compare-code")
async def compare_strategy_code(
    account_id: str = Path(..., description="账户 ID"),
    strategy_id: int = Path(..., description="策略 ID"),
    new_code: str = Body(..., description="新代码"),
):
    """
    对比策略代码变更

    Returns:
        {
            "added_lines": int,
            "removed_lines": int,
            "changes": [{"line": int, "type": "add/remove/change", "content": "..."}],
            "critical_changes": [{"type": "...", "old": "...", "new": "..."}],
            "warnings": [...]
        }
    """
    import difflib

    db = get_db_manager()
    await validate_account_active(account_id)

    # 获取旧代码
    strategy = await db.fetchone(
        "SELECT code FROM strategies WHERE id = ? AND account_id = ?",
        (strategy_id, account_id)
    )
    if not strategy:
        raise HTTPException(status_code=404, detail="策略不存在")

    old_code = strategy.get("code", "") or ""

    # 行级对比
    old_lines = old_code.splitlines()
    new_lines = new_code.splitlines()

    diff = difflib.unified_diff(old_lines, new_lines, lineterm='')

    added = 0
    removed = 0
    changes = []

    for line in list(diff)[2:]:  # Skip header lines
        if line.startswith('+') and not line.startswith('+++'):
            added += 1
            changes.append({"type": "add", "content": line[1:]})
        elif line.startswith('-') and not line.startswith('---'):
            removed += 1
            changes.append({"type": "remove", "content": line[1:]})

    # 检测关键变更（如列名修改）
    critical_changes = []
    warnings = []

    # 检查是否有 trade_date → date 的错误变更
    if "groupby('trade_date')" in old_code and "groupby('date')" in new_code:
        critical_changes.append({
            "type": "wrong_column",
            "message": "⚠️ 检测到错误的列名变更: trade_date → date",
            "severity": "critical"
        })
        warnings.append("groupby('trade_date') 被错误改为 groupby('date')，这会导致 KeyError")

    if "['trade_date']" in old_code and "['date']" in new_code:
        critical_changes.append({
            "type": "wrong_column",
            "message": "⚠️ 检测到错误的列名变更: ['trade_date'] → ['date']",
            "severity": "critical"
        })
        warnings.append("['trade_date'] 被错误改为 ['date']，这会导致 KeyError")

    # 检查是否有新增的错误代码
    if "groupby('date')" in new_code and "groupby('date')" not in old_code:
        critical_changes.append({
            "type": "new_bug",
            "message": "⚠️ 新增了错误的 groupby('date')，应为 groupby('trade_date')",
            "severity": "critical"
        })

    return {
        "added_lines": added,
        "removed_lines": removed,
        "changes": changes[:50],  # 限制返回数量
        "critical_changes": critical_changes,
        "warnings": warnings,
        "has_critical_issue": len(critical_changes) > 0
    }


@router.post("/api/v1/ui/{account_id}/strategies/test-run")
async def test_run_strategy(
    account_id: str = Path(..., description="账户 ID"),
    code: str = Body(..., embed=True, description="策略代码"),
    function_name: Optional[str] = Body(None, description="入口函数名，默认 run"),
    group_id: Optional[int] = Body(None, description="候选组 ID，指定后使用该组全部股票"),
    test_stocks: Optional[List[str]] = Body(None, description="测试用股票代码列表，不提供则从候选组取"),
):
    """
    试运行策略代码：用真实数据执行一次，不写入 watchlist

    Returns:
        {
            "success": bool,
            "signals": [...],  # 生成的信号列表
            "output": "...",   # print 输出捕获
            "error": "...",    # 错误信息
            "duration_ms": 123
        }
    """
    db = get_db_manager()

    await validate_account_active(account_id)

    from services.strategy.engine import get_strategy_engine
    from services.common import technical_indicators
    from services.data.local_data_service import get_local_data_service, is_trading_hours
    import io
    import time

    engine = get_strategy_engine()

    # 1. 先做语法/调用检查
    validation = engine.validate_code(code)
    if not validation["valid"]:
        return {
            "success": False,
            "error": validation["error"],
            "validation_warnings": validation.get("warnings", []),
            "signals": [],
            "output": "",
        }

    # 2. 准备测试数据
    if test_stocks:
        stocks = [{"stock_code": c, "stock_name": c} for c in test_stocks]
    elif group_id:
        # 从指定候选组取全部股票
        watchlist_rows = await db.fetchall(
            "SELECT DISTINCT stock_code, stock_name FROM watchlist WHERE account_id = ? AND group_id = ?",
            (account_id, group_id),
        )
        stocks = [dict(r) for r in watchlist_rows] if watchlist_rows else []
    else:
        # 从该账户的 watchlist 取全部股票作为测试数据
        watchlist_rows = await db.fetchall(
            "SELECT DISTINCT stock_code, stock_name FROM watchlist WHERE account_id = ?",
            (account_id,),
        )
        stocks = [dict(r) for r in watchlist_rows] if watchlist_rows else [
            {"stock_code": "600000.SH", "stock_name": "浦发银行"},
            {"stock_code": "000001.SZ", "stock_name": "平安银行"},
        ]

    stock_codes = [s["stock_code"] for s in stocks]

    # 3. 构建数据获取函数
    lds = get_local_data_service()

    def _get_kline_local(stock_code: str, limit: int = 100, start_date: str = None):
        return lds.get_kline_data(stock_code, start_date=start_date, limit=limit)

    def _get_batch_kline(codes: list, limit: int = 100):
        return lds.get_batch_kline(codes, limit=limit)

    def _get_factors(sc: str, date: str = None):
        target_date = date or get_china_time().strftime("%Y-%m-%d")
        return lds.get_daily_factors(sc, target_date)

    def _get_factors_batch(codes: list, date: str = None):
        target_date = date or get_china_time().strftime("%Y-%m-%d")
        return lds.get_daily_factors_batch(codes, target_date)

    def _get_kline_spliced(codes: list, lookback: int = 100):
        return lds.get_kline_spliced(codes, lookback=lookback)

    def _get_kline_smart(codes: list, lookback: int = 100):
        return lds.get_kline_with_realtime(codes, lookback=lookback)

    async def _get_realtime_quote(stock_code: str):
        from services.trading.gateway import get_gateway
        gateway = await get_gateway()
        return await gateway.get_market_data(stock_code)

    async def _get_kline_async(stock_code: str, period: str = "day", start_date: str = None):
        from services.trading.gateway import get_gateway
        gateway = await get_gateway()
        return await gateway.get_kline_data(stock_code, period=period, start_date=start_date)

    async def _get_market_data_async(stock_code: str):
        from services.trading.gateway import get_gateway
        gateway = await get_gateway()
        return await gateway.get_market_data(stock_code)

    # 导入公共数据库查询函数（用于 context 传递）
    from services.common.database import query_kline_db as _query_kline_db
    from services.common.database import query_db as _query_db

    context = {
        "stocks": stocks,
        "account_id": account_id,
        "today": get_china_time().strftime("%Y-%m-%d"),
        "indicators": {
            "calculate_ma": technical_indicators.calculate_ma,
            "calculate_rsi": technical_indicators.calculate_rsi,
            "calculate_macd": technical_indicators.calculate_macd,
            "calculate_kdj": technical_indicators.calculate_kdj,
            "calculate_bollinger_bands": technical_indicators.calculate_bollinger_bands,
            "calculate_adx": technical_indicators.calculate_adx,
            "calculate_atr": technical_indicators.calculate_atr,
            "calculate_ema": technical_indicators.calculate_ema,
            "calculate_obv": technical_indicators.calculate_obv,
            "calculate_historical_volatility": technical_indicators.calculate_historical_volatility,
        },
        "get_kline": _get_kline_async,
        "get_market_data": _get_market_data_async,
        "get_kline_local": _get_kline_local,
        "get_batch_kline": _get_batch_kline,
        "get_factors": _get_factors,
        "get_factors_batch": _get_factors_batch,
        "get_kline_spliced": _get_kline_spliced,
        "get_kline_smart": _get_kline_smart,
        "get_realtime_quote": _get_realtime_quote,
        "query_kline_db": _query_kline_db,
        "query_db": _query_db,
    }

    # 4. 捕获 print 输出并执行
    old_stdout = __import__("sys").stdout
    captured_output = io.StringIO()
    __import__("sys").stdout = captured_output

    start_time = time.time()
    try:
        from services.common.kronos_service import get_kronos_service
        kronos_svc = get_kronos_service()

        strategy = {"code": code, "function_name": function_name or "run", "name": "test_run"}
        signals = engine.execute_strategy(strategy, context)
        duration_ms = int((time.time() - start_time) * 1000)
        output = captured_output.getvalue()

        return {
            "success": True,
            "signals": signals,
            "signal_count": len(signals),
            "output": output,
            "error": None,
            "duration_ms": duration_ms,
            "validation_warnings": validation.get("warnings", []),
            "kronos_unavailable": not kronos_svc.is_available,
            "kronos_error": kronos_svc.error if not kronos_svc.is_available else None,
        }
    except Exception as e:
        from services.common.kronos_service import get_kronos_service
        kronos_svc = get_kronos_service()
        duration_ms = int((time.time() - start_time) * 1000)
        output = captured_output.getvalue()
        __import__("sys").stdout = old_stdout

        return {
            "success": False,
            "signals": [],
            "output": output,
            "error": f"{type(e).__name__}: {str(e)}",
            "duration_ms": duration_ms,
            "validation_warnings": validation.get("warnings", []),
            "kronos_unavailable": not kronos_svc.is_available,
            "kronos_error": kronos_svc.error if not kronos_svc.is_available else None,
        }
    finally:
        __import__("sys").stdout = old_stdout
