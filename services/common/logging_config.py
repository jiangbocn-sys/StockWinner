"""
日志配置模块

提供统一的日志记录和错误处理
"""

import logging
import sys
from pathlib import Path
from typing import Optional


# 日志配置
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
LOG_LEVEL = logging.INFO

# 日志目录
LOG_DIR = Path(__file__).parent.parent.parent / "logs"


def setup_logger(
    name: str,
    level: int = LOG_LEVEL,
    log_to_file: bool = True,
    log_to_console: bool = True
) -> logging.Logger:
    """
    设置并返回 logger

    注意：structured_logger 已接管日志输出，此函数仅用于兼容旧代码。
    新代码请使用 from services.common.structured_logger import get_logger

    Args:
        name: logger 名称（通常使用 __name__）
        level: 日志级别
        log_to_file: 是否记录到文件（已废弃）
        log_to_console: 是否输出到控制台（已废弃）

    Returns:
        配置好的 Logger 对象
    """
    logger = logging.getLogger(name)
    # 不再添加 handler，由 structured_logger 统一处理
    if not logger.handlers:
        logger.setLevel(level)
        logger.propagate = True  # 传播到 StockWinner 根 logger
    return logger


def get_logger(name: str) -> logging.Logger:
    """获取 logger（便捷函数）"""
    return setup_logger(name)


# 预定义的 logger
core_logger: Optional[logging.Logger] = None
trading_logger: Optional[logging.Logger] = None
screening_logger: Optional[logging.Logger] = None
factor_logger: Optional[logging.Logger] = None


def init_loggers():
    """初始化所有预定义 logger"""
    global core_logger, trading_logger, screening_logger, factor_logger

    core_logger = setup_logger("services.core")
    trading_logger = setup_logger("services.trading")
    screening_logger = setup_logger("services.screening")
    factor_logger = setup_logger("services.factors")


def get_core_logger() -> logging.Logger:
    """获取核心服务 logger"""
    global core_logger
    if core_logger is None:
        init_loggers()
    return core_logger


def get_trading_logger() -> logging.Logger:
    """获取交易服务 logger"""
    global trading_logger
    if trading_logger is None:
        init_loggers()
    return trading_logger


def get_screening_logger() -> logging.Logger:
    """获取选股服务 logger"""
    global screening_logger
    if screening_logger is None:
        init_loggers()
    return screening_logger


def get_factor_logger() -> logging.Logger:
    """获取因子计算 logger"""
    global factor_logger
    if factor_logger is None:
        init_loggers()
    return factor_logger


# 错误处理辅助函数

class ServiceError(Exception):
    """服务层错误基类"""
    pass


class DataError(Exception):
    """数据层错误"""
    pass


class SDKError(Exception):
    """SDK 相关错误"""
    pass


def handle_error(
    logger: logging.Logger,
    error: Exception,
    message: str = "操作失败",
    raise_new: Optional[Exception] = None
):
    """
    统一错误处理

    Args:
        logger: 使用的 logger
        error: 捕获的异常
        message: 错误消息前缀
        raise_new: 如果提供，则抛出新的异常；否则重新抛出原异常
    """
    logger.error(f"{message}: {error}", exc_info=True)
    if raise_new:
        raise raise_new from error
