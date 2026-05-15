"""
HTTP 请求计时中间件

记录每次 API 调用的耗时和基本信息，写入 performance.jsonl。
写操作（POST/PUT/DELETE/PATCH）额外记录审计级别日志。

注册顺序：应在 CORS 之后、其他业务中间件之前，确保捕获所有请求。
"""

import time
from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware

from services.common.structured_logger import get_logger


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        start = time.monotonic()
        logger = get_logger("http")

        # 识别请求来源
        source = "unknown"
        agent_id = None
        account_id = None

        if request.headers.get("X-Agent-Key"):
            source = "agent"
        elif request.headers.get("Authorization", "").startswith("Bearer sk-agent-"):
            source = "agent"
        elif request.url.path.startswith("/api/v1/ui/"):
            source = "ui"

        # 执行请求
        try:
            response = await call_next(request)
            status_code = response.status_code
        except Exception:
            status_code = 500
            response = None
            raise
        finally:
            duration_ms = (time.monotonic() - start) * 1000
            path = request.url.path
            method = request.method

            # 所有请求：性能指标
            logger.log_duration(
                operation="http_request",
                duration_ms=duration_ms,
                method=method,
                path=path,
                status_code=status_code,
                source=source,
                agent_id=agent_id,
                account_id=account_id,
            )

            # 写操作：额外审计
            if method in ("POST", "PUT", "DELETE", "PATCH"):
                logger.log_request(
                    method=method,
                    path=path,
                    status_code=status_code,
                    duration_ms=duration_ms,
                    source=source,
                    agent_id=agent_id,
                    account_id=account_id,
                )

        return response
