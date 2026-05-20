"""
审计日志服务 + 装饰器

每个 Agent API 调用自动记录到 agent_audit_log 表。
采用后台异步队列模式：log_action 仅将记录入队（非阻塞），由后台协程批量刷盘。
避免与业务请求竞争数据库锁。
"""

import json
import asyncio
import functools
from typing import Optional, Any, Dict

from services.common.database import get_db_manager
from services.common.timezone import get_china_time
from services.agent.models import RiskLevel, ACTION_RISK_MAP

# ============================================================
# 后台审计日志队列
# ============================================================

_audit_queue: Optional[asyncio.Queue] = None
_audit_consumer_task: Optional[asyncio.Task] = None
_AUDIT_BATCH_SIZE = 20        # 每批写入的最大记录数
_AUDIT_FLUSH_INTERVAL = 3.0   # 最大等待时间（秒），超时自动刷盘


async def _audit_consumer():
    """后台消费者：从队列中取出审计日志记录，批量写入数据库"""
    buffer: list = []
    while True:
        try:
            # 等待第一条记录（带超时，避免无限等待）
            record = await asyncio.wait_for(_audit_queue.get(), timeout=_AUDIT_FLUSH_INTERVAL)
            buffer.append(record)
            _audit_queue.task_done()

            # 继续消费当前积压
            while len(buffer) < _AUDIT_BATCH_SIZE:
                try:
                    record = _audit_queue.get_nowait()
                    buffer.append(record)
                    _audit_queue.task_done()
                except asyncio.QueueEmpty:
                    break

            # 批量写入
            await _flush_buffer(buffer)
            buffer.clear()

        except asyncio.TimeoutError:
            # 超时，刷新剩余缓存
            if buffer:
                await _flush_buffer(buffer)
                buffer.clear()
        except asyncio.CancelledError:
            # 消费者被取消，刷新剩余缓存后退出
            if buffer:
                await _flush_buffer(buffer)
                buffer.clear()
            break
        except Exception:
            # 写入失败不阻塞队列，清空 buffer 继续
            buffer.clear()


async def _flush_buffer(buffer: list):
    """将缓存中的审计记录批量写入数据库"""
    if not buffer:
        return
    try:
        db = get_db_manager()
        for rec in buffer:
            await db.execute("""
                INSERT INTO agent_audit_log
                (agent_id, user_id, action, resource_type, resource_id, account_id, status,
                 request_payload, response_summary, risk_level, ip_address, confirmation_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                rec.get("agent_id", ""),
                rec.get("user_id", ""),
                rec.get("action", ""),
                rec.get("resource_type"),
                rec.get("resource_id"),
                rec.get("account_id"),
                rec.get("status", "success"),
                rec.get("request_payload"),
                rec.get("response_summary"),
                rec.get("risk_level", "low"),
                rec.get("ip_address"),
                rec.get("confirmation_id"),
            ))
    except Exception as e:
        print(f"[AgentAudit] 批量写入失败 ({len(buffer)}条): {e}")


def start_audit_consumer():
    """启动后台审计日志消费者（在 lifespan startup 中调用）"""
    global _audit_queue, _audit_consumer_task
    if _audit_consumer_task and not _audit_consumer_task.done():
        return  # 已在运行
    _audit_queue = asyncio.Queue(maxsize=1000)
    _audit_consumer_task = asyncio.create_task(_audit_consumer())


async def stop_audit_consumer():
    """关闭消费者并清空队列（在 lifespan shutdown 中调用）"""
    global _audit_consumer_task
    if _audit_consumer_task and not _audit_consumer_task.done():
        _audit_consumer_task.cancel()
        try:
            await _audit_consumer_task
        except asyncio.CancelledError:
            pass
        _audit_consumer_task = None


def log_action_nowait(
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
    """将审计日志记录放入后台队列（非阻塞，fire-and-forget）"""
    global _audit_queue

    # 脱敏：移除可能的密码、key 字段
    safe_payload = None
    if request_payload:
        safe_payload = {k: v for k, v in request_payload.items()
                        if k not in ("api_key", "password", "secret", "token")}

    record = {
        "agent_id": agent_id,
        "user_id": user_id or "",
        "action": action,
        "resource_type": resource_type,
        "resource_id": resource_id,
        "account_id": account_id,
        "status": status,
        "request_payload": json.dumps(safe_payload, ensure_ascii=False) if safe_payload else None,
        "response_summary": json.dumps(response_summary, ensure_ascii=False) if response_summary else None,
        "risk_level": risk_level,
        "ip_address": ip_address,
        "confirmation_id": confirmation_id,
    }

    if _audit_queue is None:
        # 队列未初始化（如测试环境），降级为同步写入
        import warnings
        warnings.warn("Audit queue not initialized, falling back to sync write")
        # 创建临时事件循环用于同步写入
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(_flush_buffer([record]))
        except RuntimeError:
            pass
        return

    if _audit_queue.full():
        # 队列满，丢弃最旧的记录（保留最新的审计信息）
        try:
            _audit_queue.get_nowait()
            _audit_queue.task_done()
        except asyncio.QueueEmpty:
            pass

    try:
        _audit_queue.put_nowait(record)
    except Exception as e:
        print(f"[AgentAudit] 入队失败: {e}")


# 保持向后兼容：旧的 log_action 是 async，现在改为非阻塞包装
async def log_action(*args, **kwargs):
    """异步包装，内部使用非阻塞入队"""
    log_action_nowait(*args, **kwargs)


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
