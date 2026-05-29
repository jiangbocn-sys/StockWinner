# -*- coding: utf-8 -*-
"""
系统管理工具

通过 Agent API 的 /manage/* 端点进行系统管理操作。
需要 operator+ 权限。
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from fastmcp import FastMCP
from services.mcp.utils import get_api_client
from services.mcp import mcp

api = get_api_client()


# ================================================================
# 调度任务管理
# ================================================================

@mcp.tool()
async def mcp_scheduler_run_now(task_id: int) -> dict:
    """
    立即执行调度任务

    Args:
        task_id: 任务 ID

    Returns:
        执行结果

    Note:
        需要 operator+ 权限
    """
    return await api.post("/manage/scheduler/run-now", {"task_id": task_id})


@mcp.tool()
async def mcp_scheduler_toggle(task_id: int, enabled: int) -> dict:
    """
    启用/禁用调度任务

    Args:
        task_id: 任务 ID
        enabled: 启用状态，1=启用，0=禁用

    Returns:
        操作结果

    Note:
        需要 operator+ 权限
    """
    return await api.post("/manage/scheduler/toggle", {
        "task_id": task_id,
        "enabled": enabled
    })


# ================================================================
# 监控服务管理
# ================================================================

@mcp.tool()
async def mcp_monitoring_start(interval: int = 60) -> dict:
    """
    启动交易监控服务

    Args:
        interval: 监控间隔（秒），默认 60

    Returns:
        启动结果

    Note:
        需要 operator+ 权限
    """
    return await api.post("/manage/monitoring/start", {"interval": interval})


@mcp.tool()
async def mcp_monitoring_stop() -> dict:
    """
    停止交易监控服务

    Returns:
        停止结果

    Note:
        需要 operator+ 权限
    """
    return await api.post("/manage/monitoring/stop")


# ================================================================
# 选股服务管理
# ================================================================

@mcp.tool()
async def mcp_screening_start(account_id: str, strategy_id: int, interval: int = 300) -> dict:
    """
    启动选股服务

    Args:
        account_id: 账户 ID
        strategy_id: 策略 ID
        interval: 执行间隔（秒），默认 300

    Returns:
        启动结果

    Note:
        需要 operator+ 权限
    """
    return await api.post("/manage/screening/start", {
        "account_id": account_id,
        "strategy_id": strategy_id,
        "interval": interval
    })


@mcp.tool()
async def mcp_screening_stop() -> dict:
    """
    停止选股服务

    Returns:
        停止结果

    Note:
        需要 operator+ 权限
    """
    return await api.post("/manage/screening/stop")


# ================================================================
# 策略执行
# ================================================================

@mcp.tool()
async def mcp_strategy_execute(strategy_id: int, account_id: str) -> dict:
    """
    执行策略（生成信号）

    Args:
        strategy_id: 策略 ID
        account_id: 账户 ID

    Returns:
        执行结果，包含生成的信号列表

    Note:
        需要 operator+ 权限
    """
    return await api.post("/manage/strategy/execute", {
        "strategy_id": strategy_id,
        "account_id": account_id
    })