"""StockWinner MCP - 全局 mcp 实例"""
from fastmcp import FastMCP
from fastmcp.server.middleware import Middleware, MiddlewareContext
from fastmcp.server.dependencies import get_http_request
from services.mcp.utils.api_client import set_agent_key
import logging
from urllib.parse import urlparse, parse_qs

logger = logging.getLogger(__name__)

mcp = FastMCP("StockWinner")


class AgentKeyMiddleware(Middleware):
    """提取认证信息，支持两种方式：
    1. HTTP Header: X-Agent-Key
    2. URL Query 参数: agent_key 或 api_key
    """

    async def on_call_tool(self, context, call_next):
        return await self._extract_key_and_call(context, call_next)

    async def on_initialize(self, context, call_next):
        return await self._extract_key_and_call(context, call_next)

    async def on_list_tools(self, context, call_next):
        return await self._extract_key_and_call(context, call_next)

    async def _extract_key_and_call(self, context, call_next):
        agent_key = None

        try:
            request = get_http_request()
            if request:
                # 方式1: Header
                agent_key = request.headers.get("X-Agent-Key")

                # 方式2: URL Query 参数
                if not agent_key:
                    parsed = urlparse(str(request.url))
                    query_params = parse_qs(parsed.query)
                    agent_key = query_params.get("agent_key", [None])[0] or query_params.get("api_key", [None])[0]

                if agent_key:
                    logger.debug(f"MCP 认证成功: key={agent_key[:15] if len(agent_key) > 15 else agent_key}...")
                else:
                    logger.warning("MCP 请求缺少认证信息，请在 URL 或 Header 中传递 agent_key")
        except Exception as e:
            logger.debug(f"MCP 提取认证失败（可能是 stdio 模式）: {e}")

        set_agent_key(agent_key)
        return await call_next(context)


mcp.add_middleware(AgentKeyMiddleware())
