"""
系统配置服务

管理全局系统参数（PriceCache TTL、监控间隔等）
配置存储在 config/system_config.json，管理员可通过 UI 调整。
"""

import json
import os
from pathlib import Path
from typing import Dict, Any, Optional
from services.common.structured_logger import get_logger

logger = get_logger("system_config")

# 配置文件路径
CONFIG_PATH = Path(__file__).parent.parent.parent / "config" / "system_config.json"

# 默认配置
DEFAULT_CONFIG = {
    "price_cache_ttl_trading": 300,      # 交易时段 TTL（秒）
    "price_cache_ttl_non_trading": 43200, # 非交易时段 TTL（秒）= 12小时
    "price_cache_max_size": 10000,       # 缓存容量上限
    "price_cache_flush_interval": 900,   # 刷盘间隔（秒）= 15分钟
    "monitor_interval": 60,              # 监控循环间隔（秒）
    "monitor_watch_refresh_interval": 120, # watchlist刷新间隔（秒）
    "auto_start_monitor": True,          # 是否自动启动监控
    "circuit_breaker_recovery_timeout": 300, # SDK熔断恢复时间（秒）
    "circuit_breaker_failure_threshold": 5,  # SDK连续失败阈值
}


class SystemConfigService:
    """系统配置管理服务"""

    def __init__(self):
        self._config: Dict[str, Any] = {}
        self._load_config()

    def _load_config(self):
        """加载配置文件"""
        try:
            if CONFIG_PATH.exists():
                with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
                    self._config = json.load(f)
                logger.log_event("config_loaded", f"系统配置已加载: {self._config}")
            else:
                self._config = DEFAULT_CONFIG.copy()
                self._save_config()
                logger.log_event("config_created", f"创建默认系统配置文件")
        except Exception as e:
            logger.error("config_load_error", f"加载系统配置失败: {e}")
            self._config = DEFAULT_CONFIG.copy()

    def _save_config(self):
        """保存配置文件"""
        try:
            with open(CONFIG_PATH, 'w', encoding='utf-8') as f:
                json.dump(self._config, f, indent=2, ensure_ascii=False)
            logger.log_event("config_saved", f"系统配置已保存: {self._config}")
            return True
        except Exception as e:
            logger.error("config_save_error", f"保存系统配置失败: {e}")
            return False

    def get_all(self) -> Dict[str, Any]:
        """获取全部配置"""
        return self._config.copy()

    def get(self, key: str, default: Any = None) -> Any:
        """获取单个配置项"""
        return self._config.get(key, default)

    def update(self, updates: Dict[str, Any]) -> bool:
        """更新配置（部分更新）"""
        # 验证配置项
        valid_keys = set(DEFAULT_CONFIG.keys())
        for key in updates:
            if key not in valid_keys:
                logger.warning("config_update", f"未知配置项: {key}")
                return False

        # 类型验证
        int_ranges = {
            "price_cache_ttl_trading": (60, 3600),
            "price_cache_ttl_non_trading": (3600, 86400),
            "price_cache_max_size": (1000, 50000),
            "price_cache_flush_interval": (300, 3600),
            "monitor_interval": (30, 300),
            "monitor_watch_refresh_interval": (60, 300),
            "circuit_breaker_recovery_timeout": (60, 600),
            "circuit_breaker_failure_threshold": (1, 20),
        }

        for key, (min_val, max_val) in int_ranges.items():
            if key in updates:
                val = updates[key]
                if not isinstance(val, int) or val < min_val or val > max_val:
                    logger.warning("config_update", f"{key} 必须是 {min_val}-{max_val}")
                    return False

        if "auto_start_monitor" in updates:
            val = updates["auto_start_monitor"]
            if not isinstance(val, bool):
                logger.warning("config_update", f"auto_start_monitor 必须是布尔值")
                return False

        self._config.update(updates)
        return self._save_config()

    def get_price_cache_ttl(self, is_trading: bool) -> int:
        """获取当前时段的 TTL"""
        key = "price_cache_ttl_trading" if is_trading else "price_cache_ttl_non_trading"
        return self._config.get(key, DEFAULT_CONFIG[key])


# 单例
_service: Optional[SystemConfigService] = None


def get_system_config() -> SystemConfigService:
    """获取系统配置服务单例"""
    global _service
    if _service is None:
        _service = SystemConfigService()
    return _service