"""
飞书 Webhook 通知渠道
"""

import json
import httpx
from typing import Optional

from .base import NotificationChannel

# 颜色映射：飞书 header template 支持的颜色
COLOR_MAP = {
    "blue": "blue",       # 信息类（信号触发）
    "green": "green",     # 成功类（买入成交）
    "red": "red",         # 错误/警告类（卖出成交、失败）
    "orange": "orange",   # 警告类
    "purple": "purple",   # 其他
}


class FeishuWebhookChannel(NotificationChannel):
    """飞书自定义机器人 Webhook 渠道"""

    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url

    async def send(
        self,
        account_id: str,
        title: str,
        content: str,
        color: str = "blue",
        event_type: Optional[str] = None,
    ) -> dict:
        """
        发送飞书交互式卡片消息

        卡片格式：
        - header: 标题 + 颜色
        - elements: 内容段落
        """
        template = COLOR_MAP.get(color, "blue")

        payload = {
            "msg_type": "interactive",
            "card": {
                "config": {"wide_screen_mode": True},
                "header": {
                    "title": {"tag": "plain_text", "content": title},
                    "template": template,
                },
                "elements": [
                    {
                        "tag": "div",
                        "text": {"tag": "lark_md", "content": content},
                    },
                    {
                        "tag": "hr",
                    },
                    {
                        "tag": "note",
                        "elements": [
                            {
                                "tag": "plain_text",
                                "content": f"账户: {account_id} | StockWinner",
                            }
                        ],
                    },
                ],
            },
        }

        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.post(
                    self.webhook_url,
                    json=payload,
                    headers={"Content-Type": "application/json"},
                )
                resp.raise_for_status()
                result = resp.json()

                # 飞书成功返回: {"StatusCode": 0, "StatusMessage": "success", "code": 0}
                success = result.get("StatusCode") == 0 or result.get("code") == 0
                return {
                    "success": success,
                    "response": json.dumps(result, ensure_ascii=False),
                    "status": "sent" if success else "failed",
                }
        except Exception as e:
            return {
                "success": False,
                "response": str(e),
                "status": "error",
            }
