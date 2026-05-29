# -*- coding: utf-8 -*-
"""
业务数据查询工具

通过 Agent API 的 /query/* 端点获取业务数据。
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from fastmcp import FastMCP
from services.mcp.utils import get_api_client
from services.mcp.server import mcp

api = get_api_client()


# ================================================================
# 核心业务数据
# ================================================================

@mcp.tool()
async def mcp_dashboard(account_id: str) -> dict:
    """
    查询账户仪表盘数据

    Args:
        account_id: 账户 ID

    Returns:
        仪表盘数据，包含持仓概览、今日交易、系统状态等
    """
    return await api.get("/query/dashboard", {"account_id": account_id})


@mcp.tool()
async def mcp_positions(account_id: str) -> dict:
    """
    查询当前持仓

    Args:
        account_id: 账户 ID

    Returns:
        持仓列表，包含股票代码、数量、成本、盈亏、策略等
    """
    return await api.get("/query/positions", {"account_id": account_id})


@mcp.tool()
async def mcp_trades(account_id: str, start_date: str = None, end_date: str = None, limit: int = 100) -> dict:
    """
    查询交易记录

    Args:
        account_id: 账户 ID
        start_date: 开始日期（YYYY-MM-DD）
        end_date: 结束日期（YYYY-MM-DD）
        limit: 返回记录数

    Returns:
        交易记录列表
    """
    params = {"account_id": account_id, "limit": limit}
    if start_date:
        params["start_date"] = start_date
    if end_date:
        params["end_date"] = end_date
    return await api.get("/query/trades", params)


@mcp.tool()
async def mcp_signals(account_id: str, limit: int = 50) -> dict:
    """
    查询交易信号

    Args:
        account_id: 账户 ID
        limit: 返回记录数

    Returns:
        信号列表，包含买入/卖出信号、触发原因、状态等
    """
    return await api.get("/query/signals", {"account_id": account_id, "limit": limit})


@mcp.tool()
async def mcp_watchlist(account_id: str, group_id: int = None) -> dict:
    """
    查询候选股列表（Watchlist）

    Args:
        account_id: 账户 ID
        group_id: 候选组 ID（可选）

    Returns:
        候选股列表，包含止盈止损价、状态、关联策略等
    """
    params = {"account_id": account_id}
    if group_id:
        params["group_id"] = group_id
    return await api.get("/query/watchlist", params)


@mcp.tool()
async def mcp_candidates(account_id: str, group_id: int = None) -> dict:
    """
    查询候选股详情

    Args:
        account_id: 账户 ID
        group_id: 候选组 ID（可选）

    Returns:
        候选股详情列表
    """
    params = {"account_id": account_id}
    if group_id:
        params["group_id"] = group_id
    return await api.get("/query/candidates", params)


# ================================================================
# 策略数据
# ================================================================

@mcp.tool()
async def mcp_strategies(account_id: str) -> dict:
    """
    查询策略列表

    Args:
        account_id: 账户 ID

    Returns:
        策略列表，包含选股策略、代码策略、交易策略等
    """
    return await api.get("/query/strategies", {"account_id": account_id})


@mcp.tool()
async def mcp_strategy_detail(strategy_id: int, account_id: str = None) -> dict:
    """
    查询策略详情

    Args:
        strategy_id: 策略 ID
        account_id: 账户 ID（可选）

    Returns:
        策略详情，包含配置、代码、状态等
    """
    params = {}
    if account_id:
        params["account_id"] = account_id
    return await api.get(f"/query/strategy/{strategy_id}", params)


@mcp.tool()
async def mcp_strategy_versions(strategy_id: int) -> dict:
    """
    查询策略版本历史

    Args:
        strategy_id: 策略 ID

    Returns:
        版本历史列表
    """
    return await api.get(f"/query/strategy/{strategy_id}/versions")


# ================================================================
# 止盈止损策略
# ================================================================

@mcp.tool()
async def mcp_trading_strategy_stock(account_id: str, stock_code: str) -> dict:
    """
    查询个股止盈止损配置

    Args:
        account_id: 账户 ID
        stock_code: 股票代码

    Returns:
        止盈止损配置，包含止损价、止盈价、移动止损等
    """
    return await api.get(f"/query/trading-strategy/stock/{stock_code}", {"account_id": account_id})


@mcp.tool()
async def mcp_trading_strategy_list(account_id: str) -> dict:
    """
    查询止盈止损策略列表

    Args:
        account_id: 账户 ID

    Returns:
        止盈止损策略列表
    """
    return await api.get("/query/trading-strategy/stock-list", {"account_id": account_id})


# ================================================================
# 绩效数据
# ================================================================

@mcp.tool()
async def mcp_performance_summary(account_id: str) -> dict:
    """
    查询策略绩效汇总

    Args:
        account_id: 账户 ID

    Returns:
        各策略绩效汇总，包含收益率、胜率、盈亏比等
    """
    return await api.get("/query/performance/summary", {"account_id": account_id})


@mcp.tool()
async def mcp_performance_selections(strategy_id: int, account_id: str, limit: int = 50) -> dict:
    """
    查询策略选股记录

    Args:
        strategy_id: 策略 ID
        account_id: 账户 ID
        limit: 返回记录数

    Returns:
        选股记录列表
    """
    return await api.get(f"/query/performance/{strategy_id}/selections", {
        "account_id": account_id,
        "limit": limit
    })


@mcp.tool()
async def mcp_performance_trades(strategy_id: int, account_id: str, limit: int = 50) -> dict:
    """
    查询策略交易明细

    Args:
        strategy_id: 策略 ID
        account_id: 账户 ID
        limit: 返回记录数

    Returns:
        交易明细列表
    """
    return await api.get(f"/query/performance/{strategy_id}/trades", {
        "account_id": account_id,
        "limit": limit
    })


# ================================================================
# 资金数据
# ================================================================

@mcp.tool()
async def mcp_capital_overview(account_id: str) -> dict:
    """
    查询资金概况

    Args:
        account_id: 账户 ID

    Returns:
        资金概况，包含可用现金、持仓市值、总盈亏、策略资金等
    """
    return await api.get("/query/capital/overview", {"account_id": account_id})


@mcp.tool()
async def mcp_capital_strategy(strategy_id: int, account_id: str) -> dict:
    """
    查询策略资金详情

    Args:
        strategy_id: 策略 ID
        account_id: 账户 ID

    Returns:
        策略资金详情，包含借入/归还记录、持仓盈亏等
    """
    return await api.get(f"/query/capital/strategies/{strategy_id}", {"account_id": account_id})


# ================================================================
# 系统状态
# ================================================================

@mcp.tool()
async def mcp_health(account_id: str) -> dict:
    """
    查询系统健康状态

    Args:
        account_id: 账户 ID

    Returns:
        健康状态，包含 SDK 连接、持仓数、待执行信号数等
    """
    return await api.get("/query/health", {"account_id": account_id})


@mcp.tool()
async def mcp_notifications(account_id: str, limit: int = 20) -> dict:
    """
    查询通知历史

    Args:
        account_id: 账户 ID
        limit: 返回记录数

    Returns:
        通知历史列表
    """
    return await api.get("/query/notifications", {"account_id": account_id, "limit": limit})