"""
中间件注册 — CORS、请求日志、认证、Agent 安全。
"""

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from services.common.request_logging_middleware import RequestLoggingMiddleware

_UI_AUTH_WHITELIST = {"/api/v1/health", "/api/auth/login", "/", "/docs", "/openapi.json", "/redoc"}


def register_middleware(app: FastAPI):
    """将所有中间件注册到 FastAPI 应用"""
    # CORS
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # 请求计时中间件
    app.add_middleware(RequestLoggingMiddleware)

    # UI 端点认证中间件
    app.middleware("http")(ui_token_middleware)

    # Agent 安全约束中间件
    app.middleware("http")(agent_security_middleware)


async def ui_token_middleware(request: Request, call_next):
    path = request.url.path

    if path in _UI_AUTH_WHITELIST or path.startswith("/ui/") or path.startswith("/static/"):
        return await call_next(request)

    if path.startswith("/api/v1/ui/") and "/monitoring/status" in path:
        return await call_next(request)

    if path.startswith("/api/v1/ui/"):
        if request.headers.get("X-Agent-Key"):
            return await call_next(request)
        auth_check = request.headers.get("Authorization", "")
        if auth_check.startswith("Bearer ") and auth_check.startswith("Bearer sk-agent-"):
            return await call_next(request)

        token = request.headers.get("X-Auth-Token")
        if not token:
            auth_header = request.headers.get("Authorization", "")
            if auth_header.startswith("Bearer "):
                token = auth_header[7:]

        if not token:
            return JSONResponse(status_code=401, content={"success": False, "message": "缺少认证 token"})

        from services.auth.service import get_auth_service
        auth_service = get_auth_service()
        account = auth_service.validate_token(token)

        if not account:
            return JSONResponse(status_code=401, content={"success": False, "message": "认证失败或会话已过期"})

        request.state.auth_token = token
        request.state.account_id = account.get("account_id", "")
        request.state.account_name = account.get("name", "")

    return await call_next(request)


# Agent 路径 → 权限映射（仅写操作）
_AGENT_ACTION_PERMS: dict = {
    ("POST", "manual-order/submit"): "trading:execute",
    ("POST", "signals/*/execute"): "trading:execute",
    ("POST", "signals/*/cancel"): "trading:execute",
    ("POST", "signals/clear"): "trading:execute",
    ("POST", "monitoring/start"): "monitoring:start",
    ("POST", "monitoring/stop"): "monitoring:stop",
    ("POST", "scheduler/*"): "scheduler:start",
    ("PUT", "scheduler/*"): "scheduler:start",
    ("DELETE", "scheduler/*"): "scheduler:stop",
    ("POST", "strategies"): "strategy:create",
    ("PUT", "strategies/*"): "strategy:update",
    ("DELETE", "strategies/*"): "strategy:delete",
    ("POST", "strategies/*/execute"): "strategy:execute",
    ("POST", "screening/*"): "screening:create",
    ("POST", "watchlist"): "watchlist:manage",
    ("POST", "watchlist/batch-add"): "watchlist:manage",
    ("DELETE", "watchlist/*"): "watchlist:manage",
    ("PUT", "watchlist/*/status"): "watchlist:manage",
    ("POST", "watchlist/batch-status"): "watchlist:manage",
    ("POST", "candidate-groups"): "watchlist:manage",
    ("PUT", "candidate-groups/*"): "watchlist:manage",
    ("DELETE", "candidate-groups/*"): "watchlist:manage",
    ("PUT", "accounts/*"): "account:write",
    ("POST", "accounts/*"): "account:write",
    ("POST", "position-rules/*"): "account:write",
    ("PUT", "position-rules/*"): "account:write",
    ("POST", "notifications/webhook"): "system:config",
    ("DELETE", "notifications/webhook"): "system:config",
    ("POST", "llm/*"): "system:config",
    ("PUT", "llm/*"): "system:config",
    ("DELETE", "llm/*"): "system:config",
    ("POST", "auth/agent/*"): "agent:manage",
    ("DELETE", "auth/agent/*"): "agent:manage",
    ("PUT", "auth/agent/*"): "agent:manage",
}


def _match_agent_action(method: str, path: str):
    for (m, p), perm in _AGENT_ACTION_PERMS.items():
        if m != method:
            continue
        if p.endswith("*"):
            if path.startswith(p[:-1]):
                return perm
        elif p == path:
            return perm
    return None


async def agent_security_middleware(request: Request, call_next):
    agent_key = request.headers.get("X-Agent-Key")
    if not agent_key:
        auth_header = request.headers.get("Authorization", "")
        if auth_header.startswith("Bearer sk-agent-"):
            agent_key = auth_header[7:]
    if not agent_key:
        return await call_next(request)

    from services.agent.models import hash_api_key, get_effective_permissions, ROLE_RATE_LIMITS, has_permission
    from services.common.database import get_db_manager
    from services.common.timezone import get_china_time
    import json as _json

    key_hash = hash_api_key(agent_key)
    db = get_db_manager()
    agent = await db.fetchone(
        "SELECT * FROM agent_accounts WHERE api_key_hash = ? AND enabled = 1",
        (key_hash,)
    )

    if not agent:
        return JSONResponse(status_code=401, content={"success": False, "message": "无效的 Agent API Key"})

    now = get_china_time().strftime("%Y-%m-%dT%H:%M:%S")
    try:
        await db.execute("UPDATE agent_accounts SET last_used_at = ? WHERE agent_id = ?", (now, agent["agent_id"]))
    except Exception:
        pass

    allowed_perms = None
    denied_perms = None
    try:
        if agent.get("allowed_permissions"):
            allowed_perms = _json.loads(agent["allowed_permissions"])
        if agent.get("denied_permissions"):
            denied_perms = _json.loads(agent["denied_permissions"])
    except Exception:
        pass

    effective_perms = get_effective_permissions(agent["role"], allowed_perms, denied_perms)
    try:
        allowed_accounts = _json.loads(agent.get("allowed_account_ids", '["*"]'))
    except Exception:
        allowed_accounts = ["*"]

    request.state.agent_id = agent["agent_id"]
    request.state.user_id = agent.get("user_id", "")
    request.state.agent_name = agent["name"]
    request.state.agent_type = agent.get("agent_type", "generic")
    request.state.agent_role = agent["role"]
    request.state.agent_permissions = effective_perms
    request.state.agent_allowed_accounts = allowed_accounts
    request.state.agent_rate_limit = agent.get("rate_limit_per_min") or ROLE_RATE_LIMITS.get(agent["role"], 60)
    request.state.is_agent_request = True

    method = request.method
    if method in ("POST", "PUT", "DELETE", "PATCH"):
        path = request.url.path.lstrip("/api/v1/")
        required_perm = _match_agent_action(method, path)
        if required_perm and not has_permission(effective_perms, required_perm):
            return JSONResponse(status_code=403, content={"success": False, "message": f"权限不足：需要 '{required_perm}' 权限"})

    from services.agent.middleware import check_rate_limit
    try:
        check_rate_limit(agent["agent_id"], request.state.agent_rate_limit)
    except Exception as e:
        if "429" in str(e):
            return JSONResponse(status_code=429, content={"success": False, "message": "请求过于频繁，请稍后重试"})

    response = await call_next(request)

    method = request.method
    path = request.url.path
    if method in ("POST", "PUT", "DELETE", "PATCH"):
        try:
            from services.agent.audit import log_action
            await log_action(
                agent_id=agent["agent_id"],
                user_id=agent.get("user_id", ""),
                action=f"middleware.{method.lower()}.{path.rstrip('/').split('/')[-1]}",
                risk_level="medium",
                account_id=agent.get("user_id", ""),
                request_payload={"method": method, "path": path},
                ip_address=request.client.host if request.client else None,
            )
        except Exception:
            pass

    return response
