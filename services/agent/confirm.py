"""
确认流程服务

处理 high/critical 风险操作的人工确认流程。
包括创建确认记录、批准、拒绝、过期检查。
"""

import json
import uuid
from typing import Optional, Dict, Any
from services.common.database import get_db_manager
from services.common.timezone import get_china_time
from datetime import timedelta


async def create_confirmation(
    agent_id: str,
    action: str,
    risk_level: str,
    account_id: Optional[str] = None,
    request_payload: Optional[Dict[str, Any]] = None,
) -> str:
    """创建人工确认记录，返回 confirmation_id"""
    confirmation_id = str(uuid.uuid4())
    now = get_china_time()
    expires_at = now + timedelta(minutes=5)

    db = get_db_manager()
    await db.execute("""
        INSERT INTO agent_confirmations
        (confirmation_id, agent_id, action, account_id, request_payload,
         risk_level, status, created_at, expires_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        confirmation_id,
        agent_id,
        action,
        account_id,
        json.dumps(request_payload, ensure_ascii=False) if request_payload else None,
        risk_level,
        "pending",
        now.strftime("%Y-%m-%dT%H:%M:%S"),
        expires_at.strftime("%Y-%m-%dT%H:%M:%S"),
    ))

    return confirmation_id


async def get_confirmation(confirmation_id: str) -> Optional[dict]:
    """获取确认记录"""
    db = get_db_manager()
    return await db.fetchone(
        "SELECT * FROM agent_confirmations WHERE confirmation_id = ?",
        (confirmation_id,)
    )


async def list_pending_confirmations() -> list:
    """列出所有待处理的确认"""
    db = get_db_manager()
    now = get_china_time().strftime("%Y-%m-%dT%H:%M:%S")
    # 先标记过期的
    await db.execute(
        "UPDATE agent_confirmations SET status = 'expired' WHERE status = 'pending' AND expires_at < ?",
        (now,)
    )
    return await db.fetchall(
        "SELECT * FROM agent_confirmations WHERE status = 'pending' ORDER BY created_at DESC"
    )


async def approve_confirmation(
    confirmation_id: str,
    reviewed_by: str,
    review_notes: Optional[str] = None,
) -> bool:
    """批准确认"""
    db = get_db_manager()
    now = get_china_time().strftime("%Y-%m-%dT%H:%M:%S")
    result = await db.execute(
        "UPDATE agent_confirmations SET status = 'approved', reviewed_by = ?, review_notes = ?, updated_at = ? WHERE confirmation_id = ? AND status = 'pending'",
        (reviewed_by, review_notes, now, confirmation_id)
    )
    return getattr(result, "rowcount", 0) > 0


async def reject_confirmation(
    confirmation_id: str,
    reviewed_by: str,
    review_notes: Optional[str] = None,
) -> bool:
    """拒绝确认"""
    db = get_db_manager()
    now = get_china_time().strftime("%Y-%m-%dT%H:%M:%S")
    result = await db.execute(
        "UPDATE agent_confirmations SET status = 'rejected', reviewed_by = ?, review_notes = ?, updated_at = ? WHERE confirmation_id = ? AND status = 'pending'",
        (reviewed_by, review_notes, now, confirmation_id)
    )
    return getattr(result, "rowcount", 0) > 0
