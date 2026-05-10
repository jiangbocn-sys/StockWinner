"""
审计日志服务 + 装饰器

每个 Agent API 调用自动记录到 agent_audit_log 表。
支持 fire-and-forget 异步写入，不阻塞主请求流程。
"""

import json
import functools
from typing import Optional, Any, Dict

from services.common.database import get_db_manager
from services.common.timezone import get_china_time
from services.agent.models import RiskLevel, ACTION_RISK_MAP


async def log_action(
    agent_id: str,
    action: str,
    risk_level: str,
    account_id: Optional[str] = None,
    user_id: Optional[str] = None,
    request_payload: Optional[Dict[str, Any]] = None,
    response_summary: Optional[Dict[str, Any]] = None,
    status: str = "success",
    resource_type: Optional[str] = None,
    resource_id: Optional[str] = None,
    ip_address: Optional[str] = None,
    confirmation_id: Optional[str] = None,
):
    """异步写入审计日志（fire-and-forget）"""
    try:
        db = get_db_manager()

        # 脱敏：移除可能的密码、key 字段
        safe_payload = None
        if request_payload:
            safe_payload = {k: v for k, v in request_payload.items()
                          if k not in ("api_key", "password", "secret", "token")}

        await db.execute("""
            INSERT INTO agent_audit_log
            (agent_id, user_id, action, resource_type, resource_id, account_id, status,
             request_payload, response_summary, risk_level, ip_address, confirmation_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            agent_id,
            user_id or "",
            action,
            resource_type,
            resource_id,
            account_id,
            status,
            json.dumps(safe_payload, ensure_ascii=False) if safe_payload else None,
            json.dumps(response_summary, ensure_ascii=False) if response_summary else None,
            risk_level,
            ip_address,
            confirmation_id,
        ))
    except Exception as e:
        print(f"[AgentAudit] 写入失败: {e}")


def audit_action(action: str, resource_type: Optional[str] = None):
    """装饰器：自动记录审计日志

    用法：
        @audit_action(action="strategy.create", resource_type="strategy")
        async def create_strategy(request, ...):
            ...
    """
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            from fastapi import Request
            request = None
            for arg in args:
                if isinstance(arg, Request):
                    request = arg
                    break
            if request is None:
                request = kwargs.get("request")

            agent_id = getattr(request.state, "agent_id", "unknown") if request else "unknown"
            user_id = getattr(request.state, "user_id", "") if request else ""
            account_id = kwargs.get("account_id") or request.query_params.get("account_id") if request else None

            risk_level = ACTION_RISK_MAP.get(action, RiskLevel.LOW)
            ip_address = request.client.host if request and request.client else None

            try:
                result = await func(*args, **kwargs)
                # 成功响应
                await log_action(
                    agent_id=agent_id,
                    user_id=user_id,
                    action=action,
                    risk_level=risk_level,
                    account_id=account_id,
                    status="success",
                    resource_type=resource_type,
                    ip_address=ip_address,
                )
                return result
            except Exception as e:
                # 失败响应
                await log_action(
                    agent_id=agent_id,
                    user_id=user_id,
                    action=action,
                    risk_level=risk_level,
                    account_id=account_id,
                    status="error",
                    resource_type=resource_type,
                    ip_address=ip_address,
                    response_summary={"error": str(e)},
                )
                raise
        return wrapper
    return decorator
