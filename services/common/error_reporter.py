"""
统一错误上报

兼容旧接口，转发到 structured_logger。
所有模块可直接用 error/warn/info/debug 快捷函数。
"""

from typing import Optional, Dict, Any

from services.common.structured_logger import get_logger

_reporter: Optional["ErrorReporter"] = None


class ErrorReporter:
    """兼容旧接口的错误上报器。内部转发到 StructuredLogger。"""

    def __init__(self, logger_name: str = "StockWinner"):
        self._logger = get_logger(logger_name.replace(".", "_"))

    def error(self, component: str, message: str, context: Optional[Dict[str, Any]] = None):
        self._logger.error(component, message, context)

    def warn(self, component: str, message: str, context: Optional[Dict[str, Any]] = None):
        self._logger.warn(component, message, context)

    def info(self, component: str, message: str, context: Optional[Dict[str, Any]] = None):
        self._logger.info(component, message, context)

    def debug(self, component: str, message: str, context: Optional[Dict[str, Any]] = None):
        self._logger.debug(component, message, context)


def get_reporter() -> ErrorReporter:
    global _reporter
    if _reporter is None:
        _reporter = ErrorReporter()
    return _reporter


# 快捷函数
def error(component: str, message: str, context: Optional[Dict[str, Any]] = None):
    get_reporter().error(component, message, context)


def warn(component: str, message: str, context: Optional[Dict[str, Any]] = None):
    get_reporter().warn(component, message, context)


def info(component: str, message: str, context: Optional[Dict[str, Any]] = None):
    get_reporter().info(component, message, context)


def debug(component: str, message: str, context: Optional[Dict[str, Any]] = None):
    get_reporter().debug(component, message, context)
