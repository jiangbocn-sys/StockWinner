"""
统一配置模块

集中管理所有配置常量
"""

# ==================== 批次大小配置 ====================

BATCH_SIZE = {
    'download': 50,      # SDK 批量下载股票数量（SDK 限制）
    'factor_calc': 50,   # 因子计算批次大小
    'market_cap': 500,   # 市值更新批次大小
    'sdk_query': 100,    # SDK 批量查询参数上限
}

# ==================== 日期格式配置 ====================

DATE_FORMAT = '%Y-%m-%d'
DATE_FORMAT_INT = '%Y%m%d'  # SDK 使用的格式
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'

# ==================== 交易时间配置 ====================

# A 股收盘后数据处理时间（16:00）
MARKET_CLOSE_HOUR = 16

# ==================== 数据下载配置 ====================

# 增量下载默认月数
INCREMENTAL_MONTHS = 6

# 全量下载默认月数
FULL_DOWNLOAD_MONTHS = 24

# 数据完整性检查容差（天）
DATA_INTEGRITY_TOLERANCE_DAYS = 1

# ==================== 选股配置 ====================

# 默认匹配度阈值
DEFAULT_MATCH_SCORE_THRESHOLD = 0.5

# 默认止损比例
DEFAULT_STOP_LOSS_PCT = 0.05

# 默认止盈比例
DEFAULT_TAKE_PROFIT_PCT = 0.15

# 默认买入数量
DEFAULT_BUY_QUANTITY = 100

# 默认仓位比例
DEFAULT_POSITION_PCT = 0.1

# ==================== 技术指标配置 ====================

# KDJ 默认参数
KDJ_N = 9
KDJ_M1 = 3
KDJ_M2 = 3

# MACD 默认参数
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9

# RSI 默认周期
RSI_PERIOD = 14

# 布林带默认参数
BOLLINGER_PERIOD = 20
BOLLINGER_STD_DEV = 2.0

# 默认 MA 窗口
MA_WINDOWS = [5, 10, 20]

# ==================== 数据库配置 ====================

# SQLite WAL 模式（推荐开启）
SQLITE_WAL_MODE = True

# 数据库连接超时（秒）
DB_TIMEOUT = 30

# 数据库连接池大小（如果使用 aiosqlite）
DB_POOL_SIZE = 10

# ==================== 日志配置 ====================

# 日志级别
LOG_LEVEL = 'INFO'

# 日志格式
LOG_FORMAT = '%(asctime)s [%(levelname)s] %(name)s: %(message)s'

# ==================== SDK 配置 ====================

# SDK 登录信息
SDK_USERNAME = "REDACTED_SDK_USERNAME"
SDK_PASSWORD = "REDACTED_SDK_PASSWORD"
SDK_HOST = "101.230.159.234"
SDK_PORT = 8600

# SDK 超时（秒）
SDK_TIMEOUT = 60

# ==================== 缓存配置 ====================

# SDK 登录缓存时间（秒）
SDK_LOGIN_CACHE_TTL = 3600

# 行情数据缓存时间（秒）
MARKET_DATA_CACHE_TTL = 60

# ==================== 重试配置 ====================

# 最大重试次数
MAX_RETRIES = 3

# 重试间隔（秒）
RETRY_DELAY = 1

# 指数退诊基数（秒）
RETRY_BACKOFF_BASE = 2
