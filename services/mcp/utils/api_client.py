# -*- coding: utf-8 -*-
"""
StockWinner MCP 服务 - Agent API 客户端

通过 HTTP 调用 Agent API，复用现有认证和权限体系。
**关键设计：透传外部 Agent 的 X-Agent-Key，不使用固定的环境变量 key**。
这样 8080 后端能正确识别调用者身份。
"""

import os
import httpx
import logging
import contextvars
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)

# 存储当前请求的 agent key（由 middleware 注入，每个请求独立）
_current_agent_key: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(
    "current_agent_key", default=None
)


def set_agent_key(key: Optional[str]):
    """在 middleware 中设置当前请求的 agent key"""
    _current_agent_key.set(key)


def get_agent_key() -> Optional[str]:
    """获取当前请求的 agent key"""
    return _current_agent_key.get()


class AgentAPIClient:
    """Agent API HTTP 客户端

    所有 MCP 工具调用通过此客户端转发到 Agent API，
    复用现有的权限校验、速率限制、审计日志。

    认证逻辑（优先级从高到低）：
    1. middleware 注入的动态 key（来自外部 Agent 的 X-Agent-Key）
    2. 构造函数传入的 api_key
    3. 环境变量 AGENT_API_KEY（fallback，stdio 模式使用）
    """

    def __init__(
        self,
        base_url: str = None,
        timeout: float = 30.0
    ):
        self.base_url = base_url or os.getenv(
            "AGENT_API_BASE_URL",
            "http://localhost:8080/api/v1/agent"
        )
        self.timeout = timeout

    async def _request(
        self,
        method: str,
        path: str,
        body: Dict[str, Any] = None,
        params: Dict[str, Any] = None,
    ) -> Dict[str, Any]:
        """发起 HTTP 请求（每次创建新 client，确保 key 透传正确）"""
        agent_key = get_agent_key()

        # 白名单 IP 特殊处理：使用内部信任 key
        if agent_key and agent_key.startswith("whitelist:"):
            # 白名单请求，传递来源 IP 信息给 backend
            source_ip = agent_key.replace("whitelist:", "")
            headers = {
                "Content-Type": "application/json",
                "X-MCP-Whitelist-IP": source_ip,
            }
            agent_key = None  # 不需要 Agent key
        elif not agent_key:
            return {"success": False, "error_type": "missing_auth", "message": "缺少 Agent API Key，请传递 X-Agent-Key header 或 URL 参数 agent_key"}
        else:
            headers = {
                "Content-Type": "application/json",
                "X-Agent-Key": agent_key,
            }

        url = f"{self.base_url}{path}"

        logger.debug(f"MCP -> Agent API {method}: {url} params={params} key={set if agent_key else NONE}")

        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                if method == "GET":
                    response = await client.get(url, params=params or {}, headers=headers)
                elif method == "POST":
                    response = await client.post(url, json=body or {}, params=params or {}, headers=headers)
                elif method == "PUT":
                    response = await client.put(url, json=body or {}, params=params or {}, headers=headers)
                elif method == "DELETE":
                    response = await client.delete(url, params=params or {}, headers=headers)
                else:
                    return {"success": False, "error_type": "unknown_method", "message": method}

                return self._handle_response(response, path)
        except httpx.TimeoutException:
            return {
                "success": False,
                "error_type": "timeout",
                "message": f"请求超时 ({self.timeout}s)",
                "path": path,
            }
        except Exception as e:
            return {
                "success": False,
                "error_type": "connection_error",
                "message": str(e),
                "path": path,
            }

    async def get(self, path: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        return await self._request("GET", path, params=params)

    async def post(self, path: str, body: Dict[str, Any] = None, params: Dict[str, Any] = None) -> Dict[str, Any]:
        return await self._request("POST", path, body=body, params=params)

    async def put(self, path: str, body: Dict[str, Any] = None, params: Dict[str, Any] = None) -> Dict[str, Any]:
        return await self._request("PUT", path, body=body, params=params)

    async def delete(self, path: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        return await self._request("DELETE", path, params=params)

    def _handle_response(self, response: httpx.Response, path: str) -> Dict[str, Any]:
        try:
            data = response.json()
            if response.status_code >= 400:
                data.setdefault("success", False)
                data.setdefault("error_type", "api_error")
                data.setdefault("http_status", response.status_code)
            return data
        except Exception as e:
            return {
                "success": False,
                "error_type": "parse_error",
                "message": f"响应解析失败: {str(e)}",
                "http_status": response.status_code,
                "path": path,
            }

    async def close(self):
        """无需关闭（每次请求用完即弃）"""
        pass


# 全局客户端实例（单例）
_client: Optional[AgentAPIClient] = None


def get_api_client() -> AgentAPIClient:
    """获取 API 客户端单例"""
    global _client
    if _client is None:
        _client = AgentAPIClient()
    return _client
