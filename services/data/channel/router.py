"""
通道路由器

根据通道类型（TRADING / MARKET_DATA / DATA_DOWNLOAD）选择数据源，
支持自动降级（主通道失败时按顺序重试备用通道）。
"""

import asyncio
import enum
import logging
import time
from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional

from services.common.structured_logger import get_logger
from services.data.providers.base import DataProvider, DataProviderError

logger = get_logger("channel_router")


class ChannelType(enum.Enum):
    """通道类型"""
    TRADING = "trading"          # 实时行情，用于交易决策
    MARKET_DATA = "market_data"   # K线/参考数据，用于UI展示/分析
    DATA_DOWNLOAD = "download"    # 批量数据下载


@dataclass
class ChannelConfig:
    """单个通道的配置"""
    channel_type: ChannelType
    provider_order: List[str] = field(default_factory=list)  # provider_id 优先级顺序
    max_retries_per_provider: int = 1
    timeout_seconds: float = 15.0
    fallback_to_local_cache: bool = True


class ChannelRouter:
    """通道路由器 — 管理数据源选择和自动降级"""

    def __init__(self):
        self._providers: Dict[str, DataProvider] = {}  # provider_id -> instance
        self._channel_configs: Dict[ChannelType, ChannelConfig] = {}
        self._failure_counts: Dict[str, int] = {}  # provider_id -> consecutive failures
        self._initialized = False

    # ============================================================
    # 注册和配置
    # ============================================================

    def register_provider(self, provider: DataProvider):
        """注册一个数据源"""
        self._providers[provider.provider_id] = provider
        logger.info(f"register_provider", f"注册数据源: {provider.info.display_name}")

    def set_channel_config(self, channel_type: ChannelType, config: ChannelConfig):
        """设置通道配置"""
        self._channel_configs[channel_type] = config

    def get_providers(self) -> Dict[str, DataProvider]:
        """获取所有已注册的 Provider"""
        return dict(self._providers)

    def get_channel_config(self, channel_type: ChannelType) -> Optional[ChannelConfig]:
        """获取通道配置"""
        return self._channel_configs.get(channel_type)

    # ============================================================
    # 执行（自动降级）
    # ============================================================

    async def execute(self, channel_type: ChannelType, method_name: str, **kwargs) -> Any:
        """
        在指定通道上执行方法调用。
        按优先级逐一尝试 Provider，失败自动降级到下一个。

        Args:
            channel_type: 通道类型
            method_name: Provider 方法名
            **kwargs: 方法参数

        Returns:
            方法执行结果

        Raises:
            DataProviderError: 所有 Provider 均失败
        """
        config = self._channel_configs.get(channel_type)
        if not config or not config.provider_order:
            raise DataProviderError(
                "router",
                f"通道 {channel_type.value} 未配置 provider 顺序"
            )

        last_error = None
        tried = []

        for provider_id in config.provider_order:
            provider = self._providers.get(provider_id)
            if not provider:
                continue

            tried.append(provider_id)
            try:
                method = getattr(provider, method_name, None)
                if not method:
                    continue

                result = await asyncio.wait_for(
                    method(**kwargs),
                    timeout=config.timeout_seconds,
                )
                # 成功：重置失败计数
                self._failure_counts[provider_id] = 0
                logger.debug(
                    "channel_execute",
                    f"通道 {channel_type.value}.{method_name} 成功: {provider_id}"
                )
                return result

            except asyncio.TimeoutError:
                self._failure_counts[provider_id] = self._failure_counts.get(provider_id, 0) + 1
                last_error = DataProviderError(
                    provider_id,
                    f"超时 ({config.timeout_seconds}s)",
                )
                logger.warning(
                    "channel_timeout",
                    f"Provider {provider_id} 超时: {method_name}({kwargs})"
                )

            except DataProviderError as e:
                self._failure_counts[provider_id] = self._failure_counts.get(provider_id, 0) + 1
                last_error = e
                logger.warning(
                    "channel_error",
                    f"Provider {provider_id} 失败: {e}"
                )

            except Exception as e:
                self._failure_counts[provider_id] = self._failure_counts.get(provider_id, 0) + 1
                last_error = DataProviderError(
                    provider_id,
                    str(e),
                    e,
                )
                logger.warning(
                    "channel_error",
                    f"Provider {provider_id} 异常: {method_name}({kwargs}): {e}"
                )

        # 所有 Provider 均失败
        failed_list = ", ".join(tried)
        raise DataProviderError(
            "router",
            f"所有 Provider 失败 [{failed_list}] for {channel_type.value}.{method_name}",
            last_error,
        )

    # ============================================================
    # 健康检查
    # ============================================================

    async def health_check_all(self) -> Dict[str, Dict[str, Any]]:
        """检查所有 Provider 的健康状态"""
        results = {}
        for provider_id, provider in self._providers.items():
            try:
                result = await asyncio.wait_for(
                    provider.health_check(),
                    timeout=10.0,
                )
                results[provider_id] = result
            except Exception as e:
                results[provider_id] = {
                    "ok": False,
                    "message": str(e),
                    "latency_ms": -1,
                }
        return results

    # ============================================================
    # 统计
    # ============================================================

    def get_failure_count(self, provider_id: str) -> int:
        """获取 Provider 的连续失败次数"""
        return self._failure_counts.get(provider_id, 0)


# ============================================================
# 全局单例
# ============================================================

_router: Optional[ChannelRouter] = None


def get_channel_router() -> ChannelRouter:
    """获取 ChannelRouter 单例"""
    global _router
    if _router is None:
        _router = ChannelRouter()
    return _router


def reset_channel_router():
    """重置 ChannelRouter（用于测试）"""
    global _router
    _router = None
