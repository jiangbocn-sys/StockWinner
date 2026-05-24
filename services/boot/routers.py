"""
路由注册 — 所有 API 路由集中管理。
"""

from fastapi import FastAPI
from services.ui import (
    dashboard, accounts, positions, trades, strategies,
    screening, monitoring, market_data, data_explorer,
    position_rules, factors, scheduler, notifications,
    trading_strategies, strategy_performance, data_service, data_sources,
)
from services.backtest.api import router as backtest_router
from services.strategy.api import router as strategy_v2_router
from services.account_management.api import router as account_management_router
from services.auth.api import router as auth_router
from services.llm.api import router as llm_router
from services.agent.api import register_agent_routers


def register_routers(app: FastAPI):
    """将所有路由注册到 FastAPI 应用"""
    # UI 端点
    app.include_router(dashboard.router)
    app.include_router(accounts.router)
    app.include_router(positions.router)
    app.include_router(trades.router)
    app.include_router(strategies.router)
    app.include_router(screening.router)
    app.include_router(monitoring.router)
    app.include_router(market_data.router)
    app.include_router(data_explorer.router)
    app.include_router(position_rules.router)
    app.include_router(factors.router)
    app.include_router(scheduler.router)
    app.include_router(trading_strategies.router)
    app.include_router(strategy_v2_router)
    app.include_router(account_management_router)
    app.include_router(auth_router)
    app.include_router(llm_router)
    app.include_router(notifications.router)
    app.include_router(strategy_performance.router)
    app.include_router(data_service.router)
    app.include_router(backtest_router)
    app.include_router(data_sources.router)

    # Agent API（独立路径，不影响 UI）
    register_agent_routers(app)
