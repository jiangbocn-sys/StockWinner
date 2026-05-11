"""
统一错误上报

替代系统中 print() / logger.warning() / logger.error() 混用的问题。
所有模块统一使用本模块上报错误和警告。

格式: [组件名] 级别 | 消息 | 上下文 (可选)

使用示例:
    from services.common.error_reporter import reporter

    # 错误
    reporter.error("kline_download", f"股票 {code} 下载失败", {"error": str(e), "retry": 3})

    # 警告
    reporter.warn("factor_calc", f"股票 {code} 因子计算跳过", {"reason": "数据不足"})

    # 信息
    reporter.info("scheduler", "任务完成", {"duration": "5m"})
"""

import logging
from typing import Optional, Dict, Any


class ErrorReporter:
    """统一错误上报器"""

    def __init__(self, logger_name: str = "StockWinner"):
        self._logger = logging.getLogger(logger_name)

    def error(self, component: str, message: str, context: Optional[Dict[str, Any]] = None):
        """上报错误"""
        extra = ""
        if context:
            extra = " | " + ", ".join(f"{k}={v}" for k, v in context.items())
        self._logger.error(f"[{component}] ERROR | {message}{extra}")

    def warn(self, component: str, message: str, context: Optional[Dict[str, Any]] = None):
        """上报警告"""
        extra = ""
        if context:
            extra = " | " + ", ".join(f"{k}={v}" for k, v in context.items())
        self._logger.warning(f"[{component}] WARN | {message}{extra}")

    def info(self, component: str, message: str, context: Optional[Dict[str, Any]] = None):
        """上报信息"""
        extra = ""
        if context:
            extra = " | " + ", ".join(f"{k}={v}" for k, v in context.items())
        self._logger.info(f"[{component}] INFO | {message}{extra}")

    def debug(self, component: str, message: str, context: Optional[Dict[str, Any]] = None):
        """上报调试信息"""
        extra = ""
        if context:
            extra = " | " + ", ".join(f"{k}={v}" for k, v in context.items())
        self._logger.debug(f"[{component}] DEBUG | {message}{extra}")


# 全局单例
_reporter: Optional[ErrorReporter] = None


def get_reporter() -> ErrorReporter:
    """获取全局错误上报器"""
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
