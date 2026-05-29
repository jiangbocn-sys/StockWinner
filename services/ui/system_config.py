"""
系统配置 API（管理员专用）
"""

from fastapi import APIRouter, HTTPException, Body
from typing import Dict, Any
from services.common.system_config import get_system_config
from services.common.structured_logger import get_logger

router = APIRouter()
logger = get_logger("system_config_api")


@router.get("/api/v1/ui/system-config")
async def get_system_config_api():
    """获取系统配置（管理员）"""
    config = get_system_config()
    return {
        "success": True,
        "config": config.get_all(),
        "defaults": config._config.__class__.__bases__[0].__dict__.get('DEFAULT_CONFIG', {}) if hasattr(config, '_config') else {}
    }


@router.put("/api/v1/ui/system-config")
async def update_system_config_api(
    updates: Dict[str, Any] = Body(..., description="配置更新")
):
    """更新系统配置（管理员）

    可更新参数：
    - price_cache_ttl_trading: 60-3600 秒
    - price_cache_ttl_non_trading: 3600-86400 秒
    - price_cache_max_size: 1000-50000 条
    - price_cache_flush_interval: 300-3600 秒
    - monitor_interval: 30-300 秒
    - monitor_watch_refresh_interval: 60-300 秒
    - auto_start_monitor: true/false
    - circuit_breaker_recovery_timeout: 60-600 秒
    - circuit_breaker_failure_threshold: 1-20 次
    """
    config = get_system_config()

    # 应用配置
    if config.update(updates):
        # 立即生效：更新 PriceCache TTL
        try:
            from services.common.price_cache import get_price_cache
            from services.data.local_data_service import is_trading_hours
            cache = get_price_cache()
            ttl = config.get_price_cache_ttl(is_trading_hours())
            cache.set_ttl(ttl)
            logger.log_event("config_applied", f"PriceCache TTL 已更新为 {ttl} 秒")
        except Exception as e:
            logger.error("config_apply_error", f"应用 TTL 配置失败: {e}")

        # 立即生效：更新熔断器参数
        try:
            from services.common.circuit_breaker import circuit_breaker
            if "circuit_breaker_recovery_timeout" in updates:
                circuit_breaker.recovery_timeout = updates["circuit_breaker_recovery_timeout"]
            if "circuit_breaker_failure_threshold" in updates:
                circuit_breaker.failure_threshold = updates["circuit_breaker_failure_threshold"]
            logger.log_event("config_applied", f"熔断器参数已更新")
        except Exception as e:
            logger.error("config_apply_error", f"应用熔断器配置失败: {e}")

        return {
            "success": True,
            "message": "配置已更新并生效",
            "config": config.get_all()
        }
    else:
        raise HTTPException(status_code=400, detail="配置验证失败，请检查参数范围")


@router.get("/api/v1/ui/system-config/defaults")
async def get_system_config_defaults():
    """获取默认配置（管理员）"""
    from services.common.system_config import DEFAULT_CONFIG
    return {
        "success": True,
        "defaults": DEFAULT_CONFIG
    }