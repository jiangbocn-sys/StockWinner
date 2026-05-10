"""
Agent 路由注册

将所有 handler 注册到 /api/v1/agent/ 路径下。
"""

from fastapi import APIRouter
from services.agent.handlers import router as agent_router

# agent_router 已经是带前缀的 APIRouter
# 实际注册在 handlers.py 中完成


def register_agent_routers(app):
    """注册 Agent 路由到 FastAPI app"""
    # handlers.py 中的 router 已经定义了所有端点
    # 通过 prefix 统一加 /api/v1/agent
    app.include_router(agent_router, prefix="/api/v1/agent", tags=["agent"])
