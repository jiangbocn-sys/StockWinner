# -*- coding: utf-8 -*-
"""
StockWinner MCP 服务 - Agent API 客户端

通过 HTTP 调用 Agent API，复用现有认证和权限体系。
"""

import os
import httpx
import logging
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)


class AgentAPIClient:
    """Agent API HTTP 客户端

    所有 MCP 工具调用通过此客户端转发到 Agent API，
    复用现有的权限校验、速率限制、审计日志。
    """

    def __init__(
        self,
        base_url: str = None,
        api_key: str = None,
        timeout: float = 30.0
    ):
        self.base_url = base_url or os.getenv(
            "AGENT_API_BASE_URL",
            "http://localhost:8080/api/v1/agent"
        )
        self.api_key = api_key or os.getenv("AGENT_API_KEY", "sk-mcp-proxy")
        self.timeout = timeout

        # 创建异步 HTTP 客户端
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        """获取或创建 HTTP 客户端（延迟初始化）"""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=self.timeout,
                headers={
                    "X-Agent-Key": self.api_key,
                    "Content-Type": "application/json"
                }
            )
        return self._client

    async def close(self):
        """关闭 HTTP 客户端"""
        if self._client and not self._client.is_closed:
            await self._client.aclose()
            self._client = None

    async def get(self, path: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        """GET 请求

        Args:
            path: API 路径（如 /query/positions）
            params: 查询参数

        Returns:
            API 响应 JSON
        """
        client = await self._get_client()
        url = f"{self.base_url}{path}"

        logger.debug(f"MCP → Agent API GET: {url} params={params}")

        try:
            response = await client.get(url, params=params or {})
            return self._handle_response(response, path)
        except httpx.TimeoutException:
            return {
                "success": False,
                "error_type": "timeout",
                "message": f"请求超时 ({self.timeout}s)",
                "path": path
            }
        except Exception as e:
            return {
                "success": False,
                "error_type": "connection_error",
                "message": str(e),
                "path": path
            }

    async def post(self, path: str, body: Dict[str, Any] = None, params: Dict[str, Any] = None) -> Dict[str, Any]:
        """POST 请求

        Args:
            path: API 路径（如 /submit/screening）
            body: 请求体
            params: 查询参数

        Returns:
            API 响应 JSON
        """
        client = await self._get_client()
        url = f"{self.base_url}{path}"

        logger.debug(f"MCP → Agent API POST: {url} params={params} body={body}")

        try:
            response = await client.post(url, json=body or {}, params=params or {})
            return self._handle_response(response, path)
        except httpx.TimeoutException:
            return {
                "success": False,
                "error_type": "timeout",
                "message": f"请求超时 ({self.timeout}s)",
                "path": path
            }
        except Exception as e:
            return {
                "success": False,
                "error_type": "connection_error",
                "message": str(e),
                "path": path
            }

    async def put(self, path: str, body: Dict[str, Any] = None, params: Dict[str, Any] = None) -> Dict[str, Any]:
        """PUT 请求

        Args:
            path: API 路径
            body: 请求体
            params: 查询参数
        """
        client = await self._get_client()
        url = f"{self.base_url}{path}"

        logger.debug(f"MCP → Agent API PUT: {url} params={params} body={body}")

        try:
            response = await client.put(url, json=body or {}, params=params or {})
            return self._handle_response(response, path)
        except httpx.TimeoutException:
            return {
                "success": False,
                "error_type": "timeout",
                "message": f"请求超时 ({self.timeout}s)",
                "path": path
            }
        except Exception as e:
            return {
                "success": False,
                "error_type": "connection_error",
                "message": str(e),
                "path": path
            }

    async def delete(self, path: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        """DELETE 请求"""
        client = await self._get_client()
        url = f"{self.base_url}{path}"

        logger.debug(f"MCP → Agent API DELETE: {url} params={params}")

        try:
            response = await client.delete(url, params=params or {})
            return self._handle_response(response, path)
        except httpx.TimeoutException:
            return {
                "success": False,
                "error_type": "timeout",
                "message": f"请求超时 ({self.timeout}s)",
                "path": path
            }
        except Exception as e:
            return {
                "success": False,
                "error_type": "connection_error",
                "message": str(e),
                "path": path
            }

    def _handle_response(self, response: httpx.Response, path: str) -> Dict[str, Any]:
        """处理 API 响应"""
        try:
            data = response.json()

            # 添加 HTTP 状态码信息
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
                "path": path
            }


# 全局客户端实例（单例）
_client: Optional[AgentAPIClient] = None


def get_api_client() -> AgentAPIClient:
    """获取 API 客户端单例"""
    global _client
    if _client is None:
        _client = AgentAPIClient()
    return _client