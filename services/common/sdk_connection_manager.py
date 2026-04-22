"""
SDK 连接队列管理器

统一管理 TGW SDK 连接的并发访问，避免连接数超限。

策略：
1. 所有 SDK 调用必须通过此管理器获取"连接令牌"
2. 长时间操作（下载/选股）：在批次之间释放令牌，允许其他请求穿插
3. 短时间操作（行情查询）：获取令牌 → 执行 → 立即释放
4. 超时等待：如果等待超过指定时间，返回错误让前端决定是否重试

优先级：
- query（行情查询）：最高优先级，快速响应
- download/screening：普通优先级，分批次释放
"""

import asyncio
import threading
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List
from enum import Enum
from dataclasses import dataclass
import uuid

# 中国时区
CHINA_TZ = timezone(timedelta(hours=8))


def get_china_time() -> datetime:
    """获取中国时区时间"""
    return datetime.now(CHINA_TZ).replace(tzinfo=None)


class TaskType(Enum):
    """任务类型（决定优先级和超时时间）"""
    QUERY = "query"          # 短查询，最高优先级，默认超时10秒
    DOWNLOAD = "download"    # 下载数据，批次间释放
    SCREENING = "screening"  # 选股服务，批次间释放


class ConnectionToken:
    """连接令牌 - 持有者需要在使用完毕后释放"""

    def __init__(self, task_id: str, task_type: TaskType, manager: 'SDKConnectionManager'):
        self.task_id = task_id
        self.task_type = task_type
        self.manager = manager
        self.acquired_at = get_china_time()
        self._released = False

    def release(self):
        """释放连接令牌"""
        if not self._released:
            self._released = True
            self.manager._release_token(self)

    async def release_async(self):
        """异步释放"""
        self.release()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()


class SDKConnectionManager:
    """
    SDK 连接队列管理器（单例）

    使用 asyncio.Lock 实现排队机制：
    - 同一时刻只有一个任务持有连接
    - 其他任务在队列中等待
    - 持有者释放后，下一个等待者自动获取
    """

    _instance: Optional['SDKConnectionManager'] = None
    _lock = threading.Lock()  # 用于单例初始化

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        # 避免重复初始化
        if hasattr(self, '_initialized') and self._initialized:
            return

        # asyncio.Lock 延迟初始化，避免绑定到错误的事件循环
        # 在第一次使用时根据当前事件循环创建
        self._async_lock: Optional[asyncio.Lock] = None

        # 当前持有者信息
        self._current_holder: Optional[Dict[str, Any]] = None

        # 等待队列信息（用于状态反馈）
        self._waiting_count = 0
        self._waiting_tasks: List[Dict[str, Any]] = []

        # 统计
        self._stats = {
            "total_acquires": 0,
            "total_releases": 0,
            "total_timeout_fails": 0,
            "max_wait_seconds": 0
        }

        self._initialized = True

    def _get_lock(self) -> asyncio.Lock:
        """
        获取当前事件循环的Lock（延迟初始化）

        asyncio.Lock 必须在正确的事件循环中创建和使用。
        如果在初始化时创建，后续在其他事件循环中使用会报错：
        "Lock is bound to a different event loop"
        """
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # 没有运行中的事件循环，返回None（同步调用场景）
            return None

        # 如果Lock不存在或绑定到不同的事件循环，创建新的
        if self._async_lock is None:
            self._async_lock = asyncio.Lock()
        elif hasattr(self._async_lock, '_loop'):
            # 检查是否绑定到同一事件循环
            if self._async_lock._loop != loop:
                self._async_lock = asyncio.Lock()

        return self._async_lock

    @classmethod
    def get_instance(cls) -> 'SDKConnectionManager':
        """获取管理器单例"""
        if cls._instance is None:
            cls._instance = SDKConnectionManager()
        return cls._instance

    async def acquire(
        self,
        task_type: TaskType = TaskType.QUERY,
        task_id: Optional[str] = None,
        timeout: float = None
    ) -> ConnectionToken:
        """
        获取 SDK 连接令牌

        Args:
            task_type: 任务类型，决定默认超时时间
            task_id: 任务标识，不提供则自动生成
            timeout: 等待超时时间（秒），不提供则使用默认值

        Returns:
            ConnectionToken: 连接令牌，使用完毕必须调用 release()

        Raises:
            asyncio.TimeoutError: 等待超时
        """
        if task_id is None:
            task_id = f"{task_type.value}_{uuid.uuid4().hex[:8]}"

        # 根据任务类型设置默认超时
        if timeout is None:
            if task_type == TaskType.QUERY:
                timeout = 15.0   # 短查询，快速超时让前端知道忙
            elif task_type == TaskType.DOWNLOAD:
                timeout = 300.0  # 下载数据，可以等较长
            else:
                timeout = 60.0   # 选股服务

        # 记录等待开始
        wait_start = get_china_time()
        wait_info = {
            "task_id": task_id,
            "task_type": task_type.value,
            "wait_start": wait_start
        }
        self._waiting_count += 1
        self._waiting_tasks.append(wait_info)

        try:
            # 等待获取锁
            lock = self._get_lock()
            if lock is None:
                raise RuntimeError("无法获取asyncio.Lock：没有运行中的事件循环")

            async with asyncio.timeout(timeout):
                await lock.acquire()

            # 成功获取
            self._waiting_count -= 1
            self._waiting_tasks.remove(wait_info)

            # 记录持有者
            self._current_holder = {
                "task_id": task_id,
                "task_type": task_type.value,
                "acquired_at": get_china_time()
            }
            self._stats["total_acquires"] += 1

            # 更新最大等待时间
            wait_seconds = (get_china_time() - wait_start).total_seconds()
            if wait_seconds > self._stats["max_wait_seconds"]:
                self._stats["max_wait_seconds"] = wait_seconds

            return ConnectionToken(task_id, task_type, self)

        except asyncio.TimeoutError:
            # 超时，清理等待记录
            self._waiting_count -= 1
            if wait_info in self._waiting_tasks:
                self._waiting_tasks.remove(wait_info)
            self._stats["total_timeout_fails"] += 1
            raise

    def _release_token(self, token: ConnectionToken):
        """释放连接令牌（内部方法）"""
        if self._current_holder and self._current_holder["task_id"] == token.task_id:
            self._current_holder = None

        self._stats["total_releases"] += 1

        # 释放锁，下一个等待者会自动获取
        try:
            lock = self._get_lock()
            if lock:
                lock.release()
        except RuntimeError:
            # 锁可能已释放
            pass

    def get_status(self) -> Dict[str, Any]:
        """
        获取连接状态

        Returns:
            {
                status: "idle" | "busy" | "queued",
                current_holder: {task_id, task_type, elapsed_seconds} | None,
                waiting_count: int,
                waiting_tasks: [{task_id, task_type, wait_seconds}] | [],
                stats: {total_acquires, total_releases, total_timeout_fails, max_wait_seconds}
            }
        """
        if self._current_holder is None and self._waiting_count == 0:
            return {
                "status": "idle",
                "current_holder": None,
                "waiting_count": 0,
                "waiting_tasks": [],
                "stats": self._stats
            }

        # 计算当前持有者已用时
        holder_info = None
        if self._current_holder:
            elapsed = (get_china_time() - self._current_holder["acquired_at"]).total_seconds()
            holder_info = {
                "task_id": self._current_holder["task_id"],
                "task_type": self._current_holder["task_type"],
                "elapsed_seconds": round(elapsed)
            }

        # 计算等待者已等待时间
        waiting_list = []
        for w in self._waiting_tasks:
            wait_seconds = (get_china_time() - w["wait_start"]).total_seconds()
            waiting_list.append({
                "task_id": w["task_id"],
                "task_type": w["task_type"],
                "wait_seconds": round(wait_seconds)
            })

        return {
            "status": "busy" if self._current_holder else "queued",
            "current_holder": holder_info,
            "waiting_count": self._waiting_count,
            "waiting_tasks": waiting_list[:5],  # 只返回前5个
            "stats": self._stats
        }

    def is_busy(self) -> bool:
        """检查连接是否被占用"""
        lock = self._get_lock()
        return lock.locked() if lock else False


# 全局便捷函数
_manager: Optional[SDKConnectionManager] = None


def get_connection_manager() -> SDKConnectionManager:
    """获取连接管理器"""
    global _manager
    if _manager is None:
        _manager = SDKConnectionManager()
    return _manager


def reset_connection_manager():
    """重置连接管理器（用于测试）"""
    global _manager
    _manager = None