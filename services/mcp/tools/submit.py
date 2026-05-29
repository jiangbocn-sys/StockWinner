# -*- coding: utf-8 -*-
"""
策略操作工具

通过 Agent API 的 /submit/* 端点进行策略创建/更新/删除。
需要 strategist+ 权限。
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from fastmcp import FastMCP
from typing import Optional, Dict, Any
from services.mcp.utils import get_api_client
from services.mcp.server import mcp

api = get_api_client()


# ================================================================
# 策略创建
# ================================================================

@mcp.tool()
async def mcp_create_screening_strategy(
    name: str,
    config: Dict[str, Any],
    account_id: str,
    target_scope: str = "all",
    match_score_threshold: float = 0.8
) -> dict:
    """
    创建配置型选股策略

    Args:
        name: 策略名称
        config: 策略配置，包含 buy_conditions 等
        account_id: 账户 ID
        target_scope: 目标范围，可选 all/sh/sz/bj
        match_score_threshold: 匹配分数阈值

    Returns:
        创建结果，包含策略 ID

    Note:
        需要 strategist+ 权限
    """
    return await api.post("/submit/screening", {
        "name": name,
        "config": config,
        "target_scope": target_scope,
        "match_score_threshold": match_score_threshold
    }, params={"account_id": account_id})


@mcp.tool()
async def mcp_create_code_strategy(
    name: str,
    code: str,
    function_name: str,
    account_id: str,
    code_scope: str = "screening"
) -> dict:
    """
    创建代码型策略

    Args:
        name: 策略名称
        code: Python 代码（会被 AST 校验）
        function_name: 执行函数名
        account_id: 账户 ID
        code_scope: 代码作用域，可选 screening/trading

    Returns:
        创建结果，包含策略 ID 和验证结果

    Note:
        需要 strategist+ 权限
        代码会经过沙箱校验
    """
    return await api.post("/submit/strategy-code", {
        "name": name,
        "code": code,
        "function_name": function_name,
        "code_scope": code_scope
    }, params={"account_id": account_id})


# ================================================================
# 策略更新/删除
# ================================================================

@mcp.tool()
async def mcp_update_strategy(
    strategy_id: int,
    account_id: str,
    name: str = None,
    config: Dict[str, Any] = None,
    status: str = None
) -> dict:
    """
    更新策略

    Args:
        strategy_id: 策略 ID
        account_id: 账户 ID
        name: 新名称（可选）
        config: 新配置（可选）
        status: 新状态（可选），如 active/inactive

    Returns:
        更新结果

    Note:
        需要 strategist+ 权限
    """
    body = {}
    if name:
        body["name"] = name
    if config:
        body["config"] = config
    if status:
        body["status"] = status

    return await api.put(f"/strategy/{strategy_id}", body, params={"account_id": account_id})


@mcp.tool()
async def mcp_delete_strategy(strategy_id: int, account_id: str) -> dict:
    """
    删除策略

    Args:
        strategy_id: 策略 ID
        account_id: 账户 ID

    Returns:
        删除结果

    Note:
        需要 operator+ 权限
        如有持仓或运行任务会阻止删除
    """
    return await api.delete(f"/strategy/{strategy_id}", {"account_id": account_id})


@mcp.tool()
async def mcp_restore_strategy_version(strategy_id: int, version_id: int) -> dict:
    """
    恢复策略历史版本

    Args:
        strategy_id: 策略 ID
        version_id: 版本 ID

    Returns:
        恢复结果

    Note:
        需要 strategist+ 权限
    """
    return await api.post(f"/submit/strategy/{strategy_id}/restore", {"version_id": version_id})


# ================================================================
# 止盈止损策略
# ================================================================

@mcp.tool()
async def mcp_set_trading_strategy(
    account_id: str,
    stock_code: str,
    stock_name: str = None,
    strategy_type: str = "percentage",
    stop_loss_pct: float = None,
    take_profit_pct: float = None,
    trailing_stop_pct: float = None,
    stop_loss_price: float = None,
    take_profit_price: float = None
) -> dict:
    """
    设置个股止盈止损

    Args:
        account_id: 账户 ID
        stock_code: 股票代码
        stock_name: 股票名称（可选）
        strategy_type: 策略类型，可选 percentage/fixed
        stop_loss_pct: 止损百分比（如 0.05 表示 5%）
        take_profit_pct: 止盈百分比
        trailing_stop_pct: 移动止损百分比
        stop_loss_price: 固定止损价（strategy_type=fixed 时使用）
        take_profit_price: 固定止盈价

    Returns:
        设置结果

    Note:
        需要 operator+ 权限
    """
    body = {
        "stock_code": stock_code,
        "strategy_type": strategy_type
    }
    if stock_name:
        body["stock_name"] = stock_name
    if stop_loss_pct:
        body["stop_loss_pct"] = stop_loss_pct
    if take_profit_pct:
        body["take_profit_pct"] = take_profit_pct
    if trailing_stop_pct:
        body["trailing_stop_pct"] = trailing_stop_pct
    if stop_loss_price:
        body["stop_loss_price"] = stop_loss_price
    if take_profit_price:
        body["take_profit_price"] = take_profit_price

    return await api.post("/submit/trading-strategy/stock", body, params={"account_id": account_id})


@mcp.tool()
async def mcp_delete_trading_strategy(account_id: str, stock_code: str) -> dict:
    """
    删除个股止盈止损配置

    Args:
        account_id: 账户 ID
        stock_code: 股票代码

    Returns:
        删除结果

    Note:
        需要 operator+ 权限
    """
    return await api.delete(f"/submit/trading-strategy/stock/{stock_code}", {"account_id": account_id})