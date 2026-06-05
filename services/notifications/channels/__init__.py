"""
通知渠道
"""

from .base import NotificationChannel
from .dispatcher import ChannelDispatcher, ChannelConfigSimple, get_channel_dispatcher

__all__ = ["NotificationChannel", "ChannelDispatcher", "ChannelConfigSimple", "get_channel_dispatcher"]
