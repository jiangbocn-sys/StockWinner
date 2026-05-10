"""
Agent 数据模型、角色常量、权限矩阵、Pydantic 请求/响应模型
"""

from enum import Enum
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field
import hashlib


# ============================================================
# 角色常量
# ============================================================

class AgentRole(str, Enum):
    VIEWER = "viewer"
    STRATEGIST = "strategist"
    OPERATOR = "operator"
    ADMIN = "admin"


ROLE_HIERARCHY = {
    AgentRole.VIEWER: 0,
    AgentRole.STRATEGIST: 1,
    AgentRole.OPERATOR: 2,
    AgentRole.ADMIN: 3,
}


# ============================================================
# 权限定义
# ============================================================

# 所有权限列表
PERMISSIONS = [
    "query:*",           # 所有只读查询
    "strategy:create",
    "strategy:update",
    "strategy:delete",
    "strategy:execute",
    "screening:create",
    "scheduler:start",
    "scheduler:stop",
    "monitoring:start",
    "monitoring:stop",
    "trading:execute",
    "data:export",
    "account:read",
    "account:write",
    "system:config",
    "agent:manage",
]

# 角色-权限矩阵（基础权限，可被 allowed_account_ids / denied_permissions 覆盖）
ROLE_PERMISSIONS: Dict[str, List[str]] = {
    AgentRole.VIEWER: [
        "query:*",
    ],
    AgentRole.STRATEGIST: [
        "query:*",
        "strategy:create",
        "strategy:update",
        "screening:create",
        "strategy:execute",
    ],
    AgentRole.OPERATOR: [
        "query:*",
        "strategy:create",
        "strategy:update",
        "strategy:delete",
        "strategy:execute",
        "screening:create",
        "scheduler:start",
        "scheduler:stop",
        "monitoring:start",
        "monitoring:stop",
        "data:export",
    ],
    AgentRole.ADMIN: [
        "query:*",
        "strategy:create",
        "strategy:update",
        "strategy:delete",
        "strategy:execute",
        "screening:create",
        "scheduler:start",
        "scheduler:stop",
        "monitoring:start",
        "monitoring:stop",
        "trading:execute",
        "data:export",
        "account:read",
        "account:write",
        "system:config",
        "agent:manage",
    ],
}

# 默认角色限速（每分钟请求数）
ROLE_RATE_LIMITS = {
    AgentRole.VIEWER: 120,
    AgentRole.STRATEGIST: 60,
    AgentRole.OPERATOR: 30,
    AgentRole.ADMIN: 120,
}


# ============================================================
# 风险等级
# ============================================================

class RiskLevel(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


# 操作到风险等级的映射
ACTION_RISK_MAP: Dict[str, str] = {
    # low
    "query.dashboard": RiskLevel.LOW,
    "query.positions": RiskLevel.LOW,
    "query.trades": RiskLevel.LOW,
    "query.signals": RiskLevel.LOW,
    "query.watchlist": RiskLevel.LOW,
    "query.candidates": RiskLevel.LOW,
    "query.market": RiskLevel.LOW,
    "query.kline": RiskLevel.LOW,
    "query.factors": RiskLevel.LOW,
    "query.strategies": RiskLevel.LOW,
    "query.strategy_detail": RiskLevel.LOW,
    "query.notifications": RiskLevel.LOW,
    # medium
    "strategy.create": RiskLevel.MEDIUM,
    "strategy.update": RiskLevel.MEDIUM,
    "screening.create": RiskLevel.MEDIUM,
    "trading_strategy.create": RiskLevel.MEDIUM,
    # high
    "strategy.delete": RiskLevel.HIGH,
    "scheduler.run_now": RiskLevel.HIGH,
    "scheduler.toggle": RiskLevel.HIGH,
    "monitoring.start": RiskLevel.HIGH,
    "monitoring.stop": RiskLevel.HIGH,
    "screening.start": RiskLevel.HIGH,
    "screening.stop": RiskLevel.HIGH,
    "strategy.execute": RiskLevel.HIGH,
    # critical
    "trading.execute": RiskLevel.CRITICAL,
    "account.manage": RiskLevel.CRITICAL,
    "system.config": RiskLevel.CRITICAL,
}


# ============================================================
# 工具函数
# ============================================================

def hash_api_key(api_key: str) -> str:
    """对 API Key 进行 SHA256 哈希"""
    return hashlib.sha256(api_key.encode("utf-8")).hexdigest()


def generate_api_key() -> str:
    """生成随机 API Key（明文，仅返回给创建者一次）"""
    import secrets
    return f"sk-agent-{secrets.token_hex(24)}"


def has_role_level(agent_role: str, required_role: str) -> bool:
    """检查 agent 角色等级是否 >= 所需角色等级"""
    return ROLE_HIERARCHY.get(agent_role, -1) >= ROLE_HIERARCHY.get(required_role, 999)


def get_effective_permissions(role: str, allowed_permissions: Optional[List[str]] = None,
                              denied_permissions: Optional[List[str]] = None) -> List[str]:
    """获取 agent 的有效权限列表（基础 + 覆盖 - 拒绝）"""
    perms = set(ROLE_PERMISSIONS.get(role, []))
    if allowed_permissions:
        perms.update(allowed_permissions)
    if denied_permissions:
        perms -= set(denied_permissions)
    return list(perms)


def has_permission(effective_permissions: List[str], required: str) -> bool:
    """检查是否拥有指定权限（支持 query:* 通配）"""
    if required in effective_permissions:
        return True
    if "query:*" in effective_permissions and required.startswith("query."):
        return True
    return False


# ============================================================
# Pydantic 模型
# ============================================================

class AgentInfo(BaseModel):
    """Agent 自身信息"""
    agent_id: str
    name: str
    role: str
    rate_limit_per_min: int
    enabled: bool
    created_at: str
    last_used_at: Optional[str] = None


class AgentCreateRequest(BaseModel):
    """创建 Agent 请求（admin only）"""
    name: str = Field(..., min_length=1, max_length=100)
    user_id: Optional[str] = Field(None, description="绑定的用户 ID")
    agent_type: Optional[str] = Field(None, description="Agent 类型：openclaw/hermes/claude_code")
    role: AgentRole = AgentRole.VIEWER
    allowed_account_ids: Optional[List[str]] = None  # None = ["*"]
    rate_limit_per_min: Optional[int] = None


class AgentUpdateRequest(BaseModel):
    """更新 Agent 请求（admin only）"""
    name: Optional[str] = None
    role: Optional[AgentRole] = None
    allowed_account_ids: Optional[List[str]] = None
    rate_limit_per_min: Optional[int] = None
    enabled: Optional[bool] = None


class AuditLogEntry(BaseModel):
    """审计日志条目"""
    id: int
    agent_id: str
    action: str
    resource_type: Optional[str] = None
    resource_id: Optional[str] = None
    account_id: Optional[str] = None
    status: str
    risk_level: str
    created_at: str


class ConfirmationEntry(BaseModel):
    """人工确认条目"""
    confirmation_id: str
    agent_id: str
    action: str
    account_id: Optional[str] = None
    risk_level: str
    status: str
    created_at: str
    expires_at: str
