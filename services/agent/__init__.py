"""
Agent API 模块

提供 AI Agent 通过 API Key 认证后按角色权限调用系统功能的接口。
独立于现有 UI 端点（/api/v1/ui/），使用 /api/v1/agent/ 路径。
"""

from services.agent.api import register_agent_routers

__all__ = ["register_agent_routers"]
