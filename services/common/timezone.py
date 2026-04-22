"""
时区工具模块

提供统一的中国时区定义和转换函数
"""

from datetime import datetime, timezone, timedelta
from typing import Optional

# 中国时区 (UTC+8)
CHINA_TZ = timezone(timedelta(hours=8))


def get_china_time() -> datetime:
    """
    获取当前中国时区时间（无时区信息）

    Returns:
        当前中国时区的 datetime 对象（tzinfo=None）
    """
    return datetime.now(CHINA_TZ).replace(tzinfo=None)


def to_china_time(dt: datetime) -> datetime:
    """
    将任意时区的 datetime 转换为中国时区（无时区信息）

    Args:
        dt: 输入 datetime（可带时区或不带时区）

    Returns:
        中国时区的 datetime（tzinfo=None）
    """
    if dt.tzinfo is None:
        # 假设输入已经是 UTC
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(CHINA_TZ).replace(tzinfo=None)


def format_china_time(dt: Optional[datetime] = None, fmt: str = '%Y-%m-%d %H:%M:%S') -> str:
    """
    格式化中国时区时间

    Args:
        dt: 要格式化的时间，None 则使用当前时间
        fmt: 格式化字符串

    Returns:
        格式化后的时间字符串
    """
    if dt is None:
        dt = get_china_time()
    else:
        dt = to_china_time(dt)
    return dt.strftime(fmt)
