"""
StockWinner 公共模块

提供统一的工具和服务：
- timezone: 时区处理
- technical_indicators: 技术指标计算
- sdk_manager: SDK 登录管理
- stock_code: 股票代码格式化
- config: 统一配置
- logging_config: 日志配置
- progress: 进度追踪
- database: 数据库管理
- account_manager: 账户管理
- indicators: 技术指标（向后兼容）
"""

from .timezone import CHINA_TZ, get_china_time, to_china_time, format_china_time
from .technical_indicators import (
    calculate_ma, calculate_ema, calculate_rsi, calculate_macd,
    calculate_kdj, calculate_bollinger_bands, calculate_atr,
    calculate_indicators_for_screening,
    add_price_performance_to_df, add_kdj_to_df, add_macd_to_df,
    add_all_technical_indicators_to_df,
)
from .stock_code import (
    normalize_stock_code, parse_stock_code, get_market, strip_market,
    is_sh_stock, is_sz_stock, format_stock_code_list,
)
from .config import (
    BATCH_SIZE, DATE_FORMAT, DATE_FORMAT_INT, DATETIME_FORMAT,
    MARKET_CLOSE_HOUR, INCREMENTAL_MONTHS, FULL_DOWNLOAD_MONTHS,
    DEFAULT_MATCH_SCORE_THRESHOLD, DEFAULT_STOP_LOSS_PCT, DEFAULT_TAKE_PROFIT_PCT,
    KDJ_N, KDJ_M1, KDJ_M2, MACD_FAST, MACD_SLOW, MACD_SIGNAL, RSI_PERIOD,
    MA_WINDOWS, SQLITE_WAL_MODE, DB_TIMEOUT,
    SDK_USERNAME, SDK_PASSWORD, SDK_HOST, SDK_PORT,
)
from .progress import ProgressTracker, ProgressManager, get_progress_manager
from .logging_config import (
    get_logger, setup_logger, get_core_logger, get_trading_logger,
    get_screening_logger, get_factor_logger, init_loggers,
    ServiceError, DataError, SDKError, handle_error,
)

# 数据库和账户管理（已有模块）
from .database import get_db_manager, DatabaseManager
from .account_manager import get_account_manager, AccountManager

# 技术指标兼容层
from .indicators import TechnicalIndicators, StockScreener

__all__ = [
    # 时区
    'CHINA_TZ', 'get_china_time', 'to_china_time', 'format_china_time',
    # 技术指标
    'calculate_ma', 'calculate_ema', 'calculate_rsi', 'calculate_macd',
    'calculate_kdj', 'calculate_bollinger_bands', 'calculate_atr',
    'calculate_indicators_for_screening',
    'add_price_performance_to_df', 'add_kdj_to_df', 'add_macd_to_df',
    'add_all_technical_indicators_to_df',
    'TechnicalIndicators', 'StockScreener',
    # 股票代码
    'normalize_stock_code', 'parse_stock_code', 'get_market', 'strip_market',
    'is_sh_stock', 'is_sz_stock', 'format_stock_code_list',
    # 配置
    'BATCH_SIZE', 'DATE_FORMAT', 'DATE_FORMAT_INT', 'DATETIME_FORMAT',
    'MARKET_CLOSE_HOUR', 'INCREMENTAL_MONTHS', 'FULL_DOWNLOAD_MONTHS',
    'DEFAULT_MATCH_SCORE_THRESHOLD', 'DEFAULT_STOP_LOSS_PCT', 'DEFAULT_TAKE_PROFIT_PCT',
    'KDJ_N', 'KDJ_M1', 'KDJ_M2', 'MACD_FAST', 'MACD_SLOW', 'MACD_SIGNAL', 'RSI_PERIOD',
    'MA_WINDOWS', 'SQLITE_WAL_MODE', 'DB_TIMEOUT',
    'SDK_USERNAME', 'SDK_PASSWORD', 'SDK_HOST', 'SDK_PORT',
    # 进度追踪
    'ProgressTracker', 'ProgressManager', 'get_progress_manager',
    # 日志
    'get_logger', 'setup_logger', 'get_core_logger', 'get_trading_logger',
    'get_screening_logger', 'get_factor_logger', 'init_loggers',
    'ServiceError', 'DataError', 'SDKError', 'handle_error',
    # 数据库和账户
    'get_db_manager', 'DatabaseManager',
    'get_account_manager', 'AccountManager',
]
