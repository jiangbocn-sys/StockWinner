"""
历史记录器
记录通知发送历史
"""

import json
from typing import Dict, Any, List, Optional
from datetime import datetime

from services.common.database import get_db_manager
from services.common.timezone import get_china_time


class HistoryRecorder:
    """历史记录器

    将通知发送结果写入 notification_history 表
    """

    async def record(
        self,
        account_id: str,
        event_type: str,
        channels: List[Dict],
        title: str,
        content: str,
        results: List[Dict],
        payload: Dict[str, Any],
        context: Optional[Dict] = None,
        rule_id: Optional[int] = None,
    ) -> int:
        """记录通知发送历史

        Args:
            account_id: 账户ID
            event_type: 事件类型
            channels: 渠道配置列表
            title: 标题
            content: 内容
            results: 发送结果列表
            payload: 原始事件数据
            context: 上下文信息
            rule_id: 匹配的规则ID

        Returns:
            记录ID
        """
        db = get_db_manager()

        # 计算整体状态
        success_count = sum(1 for r in results if r.get("success"))
        total_count = len(results)

        if success_count == total_count:
            status = "sent"
        elif success_count > 0:
            status = "partial"
        else:
            status = "failed"

        # 合并响应信息
        response_text = json.dumps(results, ensure_ascii=False)

        now = get_china_time().isoformat()

        try:
            record_id = await db.insert("notification_history", {
                "account_id": account_id,
                "channel": json.dumps([c.channel_type for c in channels], ensure_ascii=False) if channels else "[]",
                "event_type": event_type,
                "title": title,
                "content": content[:2000] if content else "",  # 限制长度
                "status": status,
                "response": response_text[:5000],  # 限制长度
                "created_at": now,
                # 扩展字段
                "channels": json.dumps([{"type": c.channel_type, "url": c.webhook_url} for c in channels], ensure_ascii=False) if channels else "[]",
                "context": json.dumps(context or {}, ensure_ascii=False),
                "payload": json.dumps(payload, ensure_ascii=False)[:5000],
                "rule_id": rule_id,
            })
            return record_id
        except Exception:
            # 表可能没有扩展字段，使用基础字段
            try:
                record_id = await db.insert("notification_history", {
                    "account_id": account_id,
                    "channel": "feishu",
                    "event_type": event_type,
                    "title": title,
                    "content": content[:2000] if content else "",
                    "status": status,
                    "response": response_text[:5000],
                    "created_at": now,
                })
                return record_id
            except Exception:
                return 0


# 全局单例
_recorder: Optional[HistoryRecorder] = None


def get_history_recorder() -> HistoryRecorder:
    """获取历史记录器单例"""
    global _recorder
    if _recorder is None:
        _recorder = HistoryRecorder()
    return _recorder