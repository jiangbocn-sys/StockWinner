"""
SDK 连接生命周期管理器

统一管理 AmazingData SDK TGW 连接的完整生命周期：
1. 连接状态检测 — 实时感知 TGW 是否可用
2. 自动重连 — 连接断开时自动发起登录
3. 并发排队 — 同一时刻只有一个任务持有连接
4. 超时控制 — 等待超时有明确反馈

调用方（中间层）透明调用，无需关心底层连接状态。

优先级：
- query（行情查询）：最高优先级，快速响应
- download/screening：普通优先级，分批次释放
"""

import asyncio
import threading
from datetime import datetime
from typing import Optional, Dict, Any, List
from enum import Enum
from dataclasses import dataclass
import uuid
import time

from services.common.timezone import get_china_time

# SDK 登录参数
import os
_SDK_USERNAME = os.environ.get("SDK_USERNAME", "")
_SDK_PASSWORD = os.environ.get("SDK_PASSWORD", "")
_SDK_HOST = os.environ.get("SDK_HOST", "")
_SDK_PORT = int(os.environ.get("SDK_PORT", "8600"))


class TaskType(Enum):
    """任务类型（决定优先级和超时时间）"""
    QUERY = "query"          # 短查询，最高优先级，默认超时15秒
    DOWNLOAD = "download"    # 下载数据，批次间释放
    SCREENING = "screening"  # 选股服务，批次间释放


class ConnectionState(Enum):
    """连接状态"""
    DISCONNECTED = "disconnected"   # 未连接 / 连接已断开
    CONNECTING = "connecting"       # 正在连接中
    CONNECTED = "connected"         # 已连接，可用
    FAILED = "failed"               # 连接失败（网络问题/凭证错误等）


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
    SDK 连接生命周期管理器（单例）

    职责：
    1. TGW 连接状态管理（检测/重连/状态报告）
    2. 并发排队（同一时刻只允许一个任务使用连接）
    3. 超时控制（等待超过阈值返回明确错误）

    使用：
    1. 调用方调用 ensure_connected() 确保连接可用
    2. 调用方调用 acquire() 获取连接令牌
    3. 使用完毕后调用 token.release() 释放
    """

    _instance: Optional['SDKConnectionManager'] = None
    _lock = threading.Lock()  # 用于单例初始化

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if hasattr(self, '_initialized') and self._initialized:
            return

        # asyncio.Lock 延迟初始化
        self._async_lock: Optional[asyncio.Lock] = None

        # 连接状态
        self._state: ConnectionState = ConnectionState.DISCONNECTED
        self._state_lock = threading.Lock()  # 保护连接状态读写

        # 错误信息
        self._error_message: str = ""
        self._last_error_time: Optional[str] = None
        self._consecutive_failures: int = 0

        # 当前持有者信息
        self._current_holder: Optional[Dict[str, Any]] = None

        # 等待队列
        self._waiting_count = 0
        self._waiting_tasks: List[Dict[str, Any]] = []

        # 统计
        self._stats = {
            "total_acquires": 0,
            "total_releases": 0,
            "total_timeout_fails": 0,
            "total_reconnects": 0,
            "max_wait_seconds": 0
        }

        self._initialized = True

    def _get_lock(self) -> asyncio.Lock:
        """获取当前事件循环的Lock（延迟初始化）"""
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return None

        if self._async_lock is None:
            self._async_lock = asyncio.Lock()
        elif hasattr(self._async_lock, '_loop'):
            if self._async_lock._loop != loop:
                self._async_lock = asyncio.Lock()

        return self._async_lock

    # ================================================================
    # 连接状态管理
    # ================================================================

    def get_state(self) -> ConnectionState:
        """获取当前连接状态"""
        with self._state_lock:
            return self._state

    def get_error_info(self) -> Dict[str, str]:
        """获取最近错误信息"""
        with self._state_lock:
            return {
                "message": self._error_message,
                "time": self._last_error_time,
                "consecutive_failures": self._consecutive_failures,
            }

    def _set_state(self, state: ConnectionState, error_msg: str = ""):
        """内部：设置连接状态"""
        with self._state_lock:
            self._state = state
            if error_msg:
                self._error_message = error_msg
                self._last_error_time = get_china_time().strftime("%Y-%m-%d %H:%M:%S")

    def _sdk_login(self) -> bool:
        """发起 SDK 登录（同步阻塞）"""
        try:
            from AmazingData import login
            result = login(_SDK_USERNAME, _SDK_PASSWORD, _SDK_HOST, _SDK_PORT)
            if result:
                print("[SDK] 登录成功")
                return True
            else:
                print("[SDK] 登录失败")
                return False
        except Exception as e:
            print(f"[SDK] 登录异常：{e}")
            raise

    def _test_connection(self) -> bool:
        """测试当前 SDK 连接是否有效"""
        try:
            from AmazingData import InfoData
            info = InfoData()
            # 轻量查询：获取一个股票的证券信息，超时快速返回
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(info.get_income, ["600000.SH"], is_local=False)
                result = future.result(timeout=5.0)
            # 能返回结果（即使是空）说明连接有效
            return result is not None
        except Exception:
            return False

    def ensure_connected(self, timeout: float = 30.0) -> bool:
        """
        确保 SDK 连接可用（同步调用，供中间层使用）

        流程：
        1. 检查当前状态
        2. 如已连接：测试连接有效性，断开则重连
        3. 如未连接：发起登录
        4. 如连接失败：返回 False，附带错误信息

        Returns:
            True: 连接可用
            False: 连接不可用，调用 get_error_info() 获取详情
        """
        current_state = self.get_state()

        if current_state == ConnectionState.CONNECTED:
            # 已连接，但需验证有效性（TGW可能已静默断开）
            if self._test_connection():
                return True
            # 连接已失效，需要重连
            print("[SDK] TGW 连接已断开，尝试重连...")

        # 需要连接/重连
        with self._state_lock:
            # 防止并发重连
            if self._state == ConnectionState.CONNECTING:
                return False  # 正在连接中，避免阻塞
            self._state = ConnectionState.CONNECTING

        try:
            success = self._sdk_login()
            if success:
                with self._state_lock:
                    self._state = ConnectionState.CONNECTED
                    self._consecutive_failures = 0
                self._stats["total_reconnects"] += 1
                return True
            else:
                with self._state_lock:
                    self._state = ConnectionState.FAILED
                    self._consecutive_failures += 1
                return False
        except Exception as e:
            with self._state_lock:
                self._state = ConnectionState.FAILED
                self._error_message = str(e)
                self._last_error_time = get_china_time().strftime("%Y-%m-%d %H:%M:%S")
                self._consecutive_failures += 1
            return False

    def disconnect(self):
        """主动断开连接（系统关闭时使用）"""
        with self._state_lock:
            self._state = ConnectionState.DISCONNECTED
            self._current_holder = None

    # ================================================================
    # 连接令牌管理
    # ================================================================

    async def acquire(
        self,
        task_type: TaskType = TaskType.QUERY,
        task_id: Optional[str] = None,
        timeout: float = None
    ) -> ConnectionToken:
        """
        获取 SDK 连接令牌

        注意：调用前应先调用 ensure_connected() 确保连接可用。
        此方法只负责并发排队。

        Args:
            task_type: 任务类型
            task_id: 任务标识
            timeout: 等待超时时间（秒）

        Returns:
            ConnectionToken: 连接令牌

        Raises:
            asyncio.TimeoutError: 等待超时
            RuntimeError: 连接不可用
        """
        # 检查连接状态
        if self.get_state() != ConnectionState.CONNECTED:
            raise RuntimeError(f"SDK 连接不可用: {self.get_state().value}")

        if task_id is None:
            task_id = f"{task_type.value}_{uuid.uuid4().hex[:8]}"

        if timeout is None:
            if task_type == TaskType.QUERY:
                timeout = 15.0
            elif task_type == TaskType.DOWNLOAD:
                timeout = 300.0
            else:
                timeout = 60.0

        wait_start = get_china_time()
        wait_info = {
            "task_id": task_id,
            "task_type": task_type.value,
            "wait_start": wait_start
        }
        self._waiting_count += 1
        self._waiting_tasks.append(wait_info)

        try:
            lock = self._get_lock()
            if lock is None:
                raise RuntimeError("无法获取asyncio.Lock：没有运行中的事件循环")

            async with asyncio.timeout(timeout):
                await lock.acquire()

            self._waiting_count -= 1
            self._waiting_tasks.remove(wait_info)

            self._current_holder = {
                "task_id": task_id,
                "task_type": task_type.value,
                "acquired_at": get_china_time()
            }
            self._stats["total_acquires"] += 1

            wait_seconds = (get_china_time() - wait_start).total_seconds()
            if wait_seconds > self._stats["max_wait_seconds"]:
                self._stats["max_wait_seconds"] = wait_seconds

            return ConnectionToken(task_id, task_type, self)

        except asyncio.TimeoutError:
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

        try:
            lock = self._get_lock()
            if lock:
                lock.release()
        except RuntimeError:
            pass

    def get_status(self) -> Dict[str, Any]:
        """
        获取连接状态

        Returns:
            {
                connection_state: "connected" | "disconnected" | "connecting" | "failed",
                connection_error: {message, time, consecutive_failures},
                status: "idle" | "busy" | "queued",
                current_holder: {...} | None,
                waiting_count: int,
                waiting_tasks: [...],
                stats: {...}
            }
        """
        conn_state = self.get_state()
        error_info = self.get_error_info()

        if self._current_holder is None and self._waiting_count == 0:
            holder_status = "idle"
        elif self._current_holder:
            holder_status = "busy"
        else:
            holder_status = "queued"

        holder_info = None
        if self._current_holder:
            elapsed = (get_china_time() - self._current_holder["acquired_at"]).total_seconds()
            holder_info = {
                "task_id": self._current_holder["task_id"],
                "task_type": self._current_holder["task_type"],
                "elapsed_seconds": round(elapsed)
            }

        waiting_list = []
        for w in self._waiting_tasks:
            wait_seconds = (get_china_time() - w["wait_start"]).total_seconds()
            waiting_list.append({
                "task_id": w["task_id"],
                "task_type": w["task_type"],
                "wait_seconds": round(wait_seconds)
            })

        return {
            "connection_state": conn_state.value,
            "connection_error": error_info,
            "status": holder_status,
            "current_holder": holder_info,
            "waiting_count": self._waiting_count,
            "waiting_tasks": waiting_list[:5],
            "stats": self._stats
        }

    def is_busy(self) -> bool:
        """检查连接是否被占用"""
        lock = self._get_lock()
        return lock.locked() if lock else False

    @classmethod
    def get_instance(cls) -> 'SDKConnectionManager':
        """获取管理器单例"""
        if cls._instance is None:
            cls._instance = SDKConnectionManager()
        return cls._instance


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
