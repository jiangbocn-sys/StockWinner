"""
Agent 认证中间件 + 权限检查 + 内存限速

FastAPI 依赖注入模式：
- verify_agent_key: 验证 X-Agent-Key 头，加载 agent 档案到 request.state
- require_permission: 检查 agent 是否有指定权限
- require_role: 检查 agent 角色等级
- check_rate_limit: 内存 token bucket 限速
"""

import time
import threading
from typing import Dict, Optional, List
from collections import defaultdict
from fastapi import Header, Request, HTTPException

from services.common.database import get_db_manager
from services.agent.models import (
    AgentRole, has_role_level, get_effective_permissions, has_permission,
    ROLE_RATE_LIMITS,
)


# ============================================================
# 内存 Token Bucket 限速器
# ============================================================

class TokenBucket:
    """单 agent 的 token bucket"""
    __slots__ = ("capacity", "tokens", "refill_rate", "last_refill")

    def __init__(self, capacity: int):
        self.capacity = capacity
        self.tokens = float(capacity)
        self.refill_rate = capacity / 60.0  # tokens per second
        self.last_refill = time.monotonic()

    def consume(self) -> bool:
        now = time.monotonic()
        elapsed = now - self.last_refill
        self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_rate)
        self.last_refill = now
        if self.tokens >= 1:
            self.tokens -= 1
            return True
        return False


class RateLimiter:
    """全局限速管理器"""

    def __init__(self):
        self._lock = threading.Lock()
        self._buckets: Dict[str, TokenBucket] = {}

    def get_bucket(self, agent_id: str, rate_per_min: int) -> TokenBucket:
        with self._lock:
            if agent_id not in self._buckets:
                self._buckets[agent_id] = TokenBucket(rate_per_min)
            return self._buckets[agent_id]

    def check(self, agent_id: str, rate_per_min: int) -> bool:
        bucket = self.get_bucket(agent_id, rate_per_min)
        return bucket.consume()


# 全局实例
_rate_limiter = RateLimiter()


def check_rate_limit(agent_id: str, rate_per_min: int):
    """检查限速，超限时抛 429"""
    if not _rate_limiter.check(agent_id, rate_per_min):
        raise HTTPException(status_code=429, detail="请求过于频繁，请稍后重试")


# ============================================================
# Agent 认证
# ============================================================

async def verify_agent_key(
    request: Request,
    x_agent_key: Optional[str] = Header(None, alias="X-Agent-Key"),
    authorization: Optional[str] = Header(None, alias="Authorization"),
) -> dict:
    """FastAPI 依赖：验证 Agent API Key，加载档案到 request.state

    兼容两种认证方式：
    - X-Agent-Key: sk-agent-xxxx（推荐）
    - Authorization: Bearer sk-agent-xxxx（LiteLLM 等标准客户端）

    用法：
        @router.get("/agent/me")
        async def get_me(agent: dict = Depends(verify_agent_key)):
            ...
    """
    # 优先使用 X-Agent-Key，其次从 Authorization: Bearer 提取
    api_key = x_agent_key
    if not api_key and authorization and authorization.startswith("Bearer "):
        api_key = authorization[7:]

    if not api_key:
        raise HTTPException(status_code=401, detail="缺少 X-Agent-Key 请求头或 Authorization: Bearer token")

    from services.agent.models import hash_api_key
    key_hash = hash_api_key(x_agent_key)

    db = get_db_manager()
    agent = await db.fetchone(
        "SELECT * FROM agent_accounts WHERE api_key_hash = ? AND enabled = 1",
        (key_hash,)
    )

    if not agent:
        raise HTTPException(status_code=401, detail="无效的 Agent API Key")

    # 更新 last_used_at
    from services.common.timezone import get_china_time
    now = get_china_time().strftime("%Y-%m-%dT%H:%M:%S")
    try:
        await db.execute(
            "UPDATE agent_accounts SET last_used_at = ? WHERE agent_id = ?",
            (now, agent["agent_id"])
        )
    except Exception:
        pass

    # 计算有效权限
    allowed_perms = None
    denied_perms = None
    try:
        import json
        if agent.get("allowed_permissions"):
            allowed_perms = json.loads(agent["allowed_permissions"])
        if agent.get("denied_permissions"):
            denied_perms = json.loads(agent["denied_permissions"])
    except Exception:
        pass

    effective_perms = get_effective_permissions(
        agent["role"], allowed_perms, denied_perms
    )

    # 解析 allowed_account_ids
    try:
        allowed_accounts = json.loads(agent.get("allowed_account_ids", '["*"]'))
    except Exception:
        allowed_accounts = ["*"]

    # 写入 request.state
    request.state.agent_id = agent["agent_id"]
    request.state.user_id = agent.get("user_id", "")
    request.state.agent_name = agent["name"]
    request.state.agent_type = agent.get("agent_type", "generic")
    request.state.agent_role = agent["role"]
    request.state.agent_permissions = effective_perms
    request.state.agent_allowed_accounts = allowed_accounts
    request.state.agent_rate_limit = agent.get("rate_limit_per_min") or ROLE_RATE_LIMITS.get(agent["role"], 60)

    # 限速检查
    check_rate_limit(agent["agent_id"], request.state.agent_rate_limit)

    return {
        "agent_id": agent["agent_id"],
        "user_id": agent.get("user_id", ""),
        "name": agent["name"],
        "agent_type": agent.get("agent_type", "generic"),
        "role": agent["role"],
        "permissions": effective_perms,
        "allowed_accounts": allowed_accounts,
    }


# ============================================================
# 权限检查依赖
# ============================================================

def require_permission(permission: str):
    """FastAPI 依赖工厂：检查指定权限

    用法：
        @router.post("/agent/submit/strategy")
        async def create_strategy(
            agent: dict = Depends(verify_agent_key),
            _: None = Depends(require_permission("strategy:create"))
        ):
            ...
    """
    def _check(request: Request):
        if not has_permission(request.state.agent_permissions, permission):
            raise HTTPException(status_code=403, detail=f"权限不足：需要 {permission}")
    return _check


def require_role(min_role: AgentRole):
    """FastAPI 依赖工厂：检查角色等级

    用法：
        @router.get("/agent/admin/audit")
        async def get_audit(
            agent: dict = Depends(verify_agent_key),
            _: None = Depends(require_role(AgentRole.ADMIN))
        ):
            ...
    """
    def _check(request: Request):
        if not has_role_level(request.state.agent_role, min_role):
            raise HTTPException(
                status_code=403,
                detail=f"角色等级不足：需要 {min_role.value}，当前为 {request.state.agent_role}"
            )
    return _check


# ============================================================
# 账户范围检查
# ============================================================

def validate_account_scope(request: Request, account_id: str):
    """检查 account_id 是否在 agent 允许范围内"""
    allowed = request.state.agent_allowed_accounts
    if "*" not in allowed and account_id not in allowed:
        raise HTTPException(
            status_code=403,
            detail=f"账户 {account_id} 不在 Agent 访问范围内"
        )
