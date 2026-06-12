"""
版本号和服务器启动时间
独立模块，避免 main.py 和 dashboard.py 之间的循环导入
"""

from services.common.timezone import get_china_time

VERSION = "8.0.8"
_server_start_time = None


def set_start_time():
    """记录服务启动时间"""
    global _server_start_time
    _server_start_time = get_china_time()


def get_start_time():
    """获取服务启动时间"""
    return _server_start_time
