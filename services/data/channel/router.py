"""
通道路由器

根据通道类型（TRADING / MARKET_DATA / DATA_DOWNLOAD）选择数据源，
支持自动降级（主通道失败时按顺序重试备用通道）。
新增：数据源使用统计（内存累积 + 定时批量写入）
"""

import asyncio
import enum
import logging
import random
import time
from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional

from services.common.database import get_db_manager
from services.common.structured_logger import get_logger
from services.common.timezone import get_china_time
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
    """通道路由器 — 管理数据源选择和自动降级

    失败通道冷却机制：通道失败后 10 分钟内不再重试，避免每次请求都浪费资源。
    使用统计：内存累积，每 10-15 分钟随机时间批量写入数据库。
    """

    FAILED_PROVIDER_COOLDOWN = 600  # 失败通道冷却 10 分钟

    def __init__(self):
        self._providers: Dict[str, DataProvider] = {}  # provider_id -> instance
        self._channel_configs: Dict[ChannelType, ChannelConfig] = {}
        self._failure_counts: Dict[str, int] = {}  # provider_id -> consecutive failures
        self._initialized = False
        self._call_counts: Dict[str, int] = {}  # provider_id -> total calls (内存缓存)
        self._success_counts: Dict[str, int] = {}  # provider_id -> total successes
        self._provider_cooldowns: Dict[str, float] = {}  # provider_id -> cooldown_until

        # 使用统计内存累积
        # key: (provider_id, date), value: {call_count, success_count, failure_count, total_latency_ms, last_error}
        self._usage_buffer: Dict[tuple, Dict] = {}
        self._flush_task: Optional[asyncio.Task] = None
        self._flush_running = False

    def start_flush_loop(self):
        """启动定时刷盘任务（在 lifespan 中调用）"""
        if self._flush_running:
            return
        self._flush_running = True
        try:
            loop = asyncio.get_running_loop()
            self._flush_task = loop.create_task(self._flush_loop())
            logger.log_event("usage_flush_started", "使用统计刷盘任务已启动")
        except RuntimeError:
            pass  # 无事件循环

    async def _flush_loop(self):
        """定时刷盘循环：每 10-15 分钟随机时间写入数据库"""
        while self._flush_running:
            # 随机等待 10-15 分钟
            wait_seconds = random.randint(600, 900)  # 10-15 分钟
            await asyncio.sleep(wait_seconds)

            # 刷盘
            try:
                await self._flush_usage_stats()
            except Exception as e:
                logger.warning("usage_flush", f"刷盘失败: {e}")

    async def _flush_usage_stats(self):
        """批量写入使用统计到数据库"""
        if not self._usage_buffer:
            return

        db = get_db_manager()
        today = get_china_time().strftime("%Y-%m-%d")

        # 复制并清空缓冲区（避免刷盘期间新数据覆盖）
        buffer_copy = dict(self._usage_buffer)
        self._usage_buffer.clear()

        flushed_count = 0
        for (provider_id, date), stats in buffer_copy.items():
            try:
                # 检查数据库中是否已有记录
                existing = await db.fetchone(
                    """SELECT id, call_count, success_count, failure_count, avg_latency_ms
                       FROM data_source_usage_stats
                       WHERE provider_id = ? AND date = ?""",
                    (provider_id, date)
                )

                if existing:
                    # 更新增量
                    new_calls = existing["call_count"] + stats["call_count"]
                    new_success = existing["success_count"] + stats["success_count"]
                    new_failure = existing["failure_count"] + stats["failure_count"]
                    # 加权平均延迟
                    old_avg = existing["avg_latency_ms"] or 0
                    old_count = existing["call_count"] or 0
                    if new_calls > 0:
                        new_avg = (old_avg * old_count + stats["total_latency_ms"]) / new_calls
                    else:
                        new_avg = 0

                    await db.execute(
                        """UPDATE data_source_usage_stats
                           SET call_count = ?, success_count = ?, failure_count = ?, avg_latency_ms = ?, last_error = ?
                           WHERE id = ?""",
                        (new_calls, new_success, new_failure, new_avg, stats.get("last_error"), existing["id"])
                    )
                else:
                    # 新增记录
                    avg_latency = stats["total_latency_ms"] / stats["call_count"] if stats["call_count"] > 0 else 0
                    await db.execute(
                        """INSERT INTO data_source_usage_stats
                           (provider_id, date, call_count, success_count, failure_count, avg_latency_ms, last_error)
                           VALUES (?, ?, ?, ?, ?, ?, ?)""",
                        (provider_id, date, stats["call_count"], stats["success_count"], stats["failure_count"], avg_latency, stats.get("last_error"))
                    )
                flushed_count += 1
            except Exception as e:
                logger.warning("usage_flush_item", f"写入 {provider_id} 失败: {e}")

        if flushed_count > 0:
            logger.log_event("usage_flush_done", f"已刷盘 {flushed_count} 条使用统计")

    def stop_flush_loop(self):
        """停止刷盘任务（在 lifespan shutdown 中调用）"""
        self._flush_running = False
        if self._flush_task:
            self._flush_task.cancel()
            try:
                asyncio.get_running_loop().run_until_complete(self._flush_task)
            except asyncio.CancelledError:
                pass
        # 最后一次刷盘
        try:
            loop = asyncio.get_running_loop()
            loop.run_until_complete(self._flush_usage_stats())
        except Exception:
            pass

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
        now_ts = time.time()

        for provider_id in config.provider_order:
            # 检查冷却：失败通道 30 分钟内跳过
            cooldown_until = self._provider_cooldowns.get(provider_id, 0)
            if now_ts < cooldown_until:
                remaining = int(cooldown_until - now_ts)
                if remaining > 60:
                    logger.debug("channel_cooldown_skip",
                        f"跳过冷却中的通道 {provider_id}（剩余 {remaining}s）")
                continue

            provider = self._providers.get(provider_id)
            if not provider:
                continue

            tried.append(provider_id)
            start_time = time.monotonic()

            try:
                method = getattr(provider, method_name, None)
                if not method:
                    continue

                result = await asyncio.wait_for(
                    method(**kwargs),
                    timeout=config.timeout_seconds,
                )

                # 验证结果：batch market data 如果全为 None 则视为失败，降级到下一个 provider
                if method_name == "get_batch_market_data" and isinstance(result, dict):
                    valid = sum(1 for v in result.values()
                               if v is not None and v.get("current_price", 0) > 0)
                    if valid == 0:
                        raise DataProviderError(provider_id, f"返回 {len(result)} 只股票但全部无效，降级到下一数据源")

                # 成功：清除冷却，记录统计
                self._provider_cooldowns.pop(provider_id, None)
                elapsed_ms = (time.monotonic() - start_time) * 1000
                self._failure_counts[provider_id] = 0
                self._call_counts[provider_id] = self._call_counts.get(provider_id, 0) + 1
                self._success_counts[provider_id] = self._success_counts.get(provider_id, 0) + 1

                # 异步写入数据库统计（不阻塞）
                self._record_usage(provider_id, channel_type.value, method_name, True, elapsed_ms)

                logger.info(
                    "channel_execute",
                    f"通道 {channel_type.value}.{method_name} 成功: {provider_id} ({elapsed_ms:.0f}ms)"
                )
                from services.common.events import emit_provider_status
                emit_provider_status(provider_id, True)
                return result

            except asyncio.TimeoutError:
                self._provider_cooldowns[provider_id] = time.time() + self.FAILED_PROVIDER_COOLDOWN
                elapsed_ms = (time.monotonic() - start_time) * 1000
                self._failure_counts[provider_id] = self._failure_counts.get(provider_id, 0) + 1
                self._call_counts[provider_id] = self._call_counts.get(provider_id, 0) + 1
                self._record_usage(provider_id, channel_type.value, method_name, False, elapsed_ms, "timeout")
                last_error = DataProviderError(provider_id, f"超时 ({config.timeout_seconds}s)")
                logger.warning("channel_timeout", f"Provider {provider_id} 超时: {method_name} ({elapsed_ms:.0f}ms)，冷却 {self.FAILED_PROVIDER_COOLDOWN}s")
                from services.common.events import emit_provider_status
                emit_provider_status(provider_id, False, f"超时 ({config.timeout_seconds}s)")

            except DataProviderError as e:
                self._provider_cooldowns[provider_id] = time.time() + self.FAILED_PROVIDER_COOLDOWN
                elapsed_ms = (time.monotonic() - start_time) * 1000
                self._failure_counts[provider_id] = self._failure_counts.get(provider_id, 0) + 1
                self._call_counts[provider_id] = self._call_counts.get(provider_id, 0) + 1
                self._record_usage(provider_id, channel_type.value, method_name, False, elapsed_ms, str(e))
                last_error = e
                logger.warning("channel_error", f"Provider {provider_id} 失败: {e}，冷却 {self.FAILED_PROVIDER_COOLDOWN}s")
                from services.common.events import emit_provider_status
                emit_provider_status(provider_id, False, str(e))

            except Exception as e:
                self._provider_cooldowns[provider_id] = time.time() + self.FAILED_PROVIDER_COOLDOWN
                elapsed_ms = (time.monotonic() - start_time) * 1000
                self._failure_counts[provider_id] = self._failure_counts.get(provider_id, 0) + 1
                self._call_counts[provider_id] = self._call_counts.get(provider_id, 0) + 1
                self._record_usage(provider_id, channel_type.value, method_name, False, elapsed_ms, str(e))
                last_error = DataProviderError(provider_id, str(e), e)
                logger.warning("channel_error", f"Provider {provider_id} 异常: {method_name}: {e}，冷却 {self.FAILED_PROVIDER_COOLDOWN}s")
                from services.common.events import emit_provider_status
                emit_provider_status(provider_id, False, str(e))

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

    def get_call_stats(self) -> Dict[str, Dict[str, int]]:
        """获取所有 Provider 的调用统计（内存）"""
        return {
            pid: {
                "calls": self._call_counts.get(pid, 0),
                "successes": self._success_counts.get(pid, 0),
                "failures": self._failure_counts.get(pid, 0),
            }
            for pid in self._providers.keys()
        }

    def _record_usage(self, provider_id: str, channel_type: str, method: str,
                      success: bool, elapsed_ms: float, error: str = None):
        """记录数据源使用统计到内存缓冲区（每 10-15 分钟批量写入数据库）"""
        try:
            today = get_china_time().strftime("%Y-%m-%d")
            key = (provider_id, today)

            # 累积到内存缓冲区
            if key not in self._usage_buffer:
                self._usage_buffer[key] = {
                    "call_count": 0,
                    "success_count": 0,
                    "failure_count": 0,
                    "total_latency_ms": 0.0,
                    "last_error": None,
                }

            buf = self._usage_buffer[key]
            buf["call_count"] += 1
            if success:
                buf["success_count"] += 1
            else:
                buf["failure_count"] += 1
                buf["last_error"] = error
            buf["total_latency_ms"] += elapsed_ms

        except Exception:
            pass  # 统计失败不影响主流程

    async def get_usage_stats(self, provider_id: str = None, days: int = 7) -> List[Dict]:
        """查询数据源使用统计"""
        db = get_db_manager()
        if provider_id:
            rows = await db.fetchall(
                """SELECT * FROM data_source_usage_stats
                   WHERE provider_id = ? AND date >= date('now', ?)
                   ORDER BY date DESC""",
                (provider_id, f'-{days} days')
            )
        else:
            rows = await db.fetchall(
                """SELECT * FROM data_source_usage_stats
                   WHERE date >= date('now', ?)
                   ORDER BY date DESC, provider_id""",
                (f'-{days} days',)
            )
        return [dict(r) for r in rows]


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
