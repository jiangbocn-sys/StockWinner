"""
SDK 连接生命周期管理器

统一管理 AmazingData SDK TGW 连接的完整生命周期：
1. 单一连接管理 — TGW 只允许一个 TCP 连接，由 login() 创建
2. 连接状态检测 — 不创建新实例，通过已有实例验证
3. 自动重连 — 连接断开时自动发起登录
4. 并发排队 — threading.Lock 跨线程串行化所有 SDK 调用
5. 超时控制 — 等待超时有明确反馈

架构设计：
- TGW 服务端限制每个用户只允许一个活跃 TCP 连接
- SDK 的 AmazingData.login() 创建这个连接
- InfoData/BaseData/MarketData 共享此连接
- 所有 SDK 数据调用必须串行化（不能并发）
- SDK 调用是同步阻塞的，可能在任何线程中执行
- 因此使用 threading.Lock 做串行化（跨线程工作）

调用方（中间层）透明调用，无需关心底层连接状态。

优先级：
- query（行情查询）：最高优先级，快速响应
- download/screening：普通优先级，分批次释放
"""

import threading
from datetime import datetime
from typing import Optional, Dict, Any, List
from enum import Enum
from dataclasses import dataclass
import uuid
import time
import concurrent.futures

from services.common.timezone import get_china_time

# SDK 登录参数
import os
_SDK_USERNAME = os.environ.get("SDK_USERNAME", "")
_SDK_PASSWORD = os.environ.get("SDK_PASSWORD", "")
_SDK_HOST = os.environ.get("SDK_HOST", "")
_SDK_PORT = int(os.environ.get("SDK_PORT", "8600"))


class TaskType(Enum):
    """任务类型（决定超时时间）"""
    QUERY = "query"          # 短查询，最高优先级，默认超时15秒
    DOWNLOAD = "download"    # 下载数据，默认超时300秒
    SCREENING = "screening"  # 选股服务，默认超时60秒


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

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()


class SDKConnectionManager:
    """
    SDK 连接生命周期管理器（单例）

    职责：
    1. TGW 连接状态管理（检测/重连/状态报告）
    2. 并发串行化（threading.Lock，所有 SDK 调用排队）
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

        # 串行化锁（threading，跨线程工作）
        self._serial_lock = threading.Lock()

        # 连接状态
        self._state: ConnectionState = ConnectionState.DISCONNECTED
        self._state_lock = threading.Lock()  # 保护连接状态读写

        # 错误信息
        self._error_message: str = ""
        self._last_error_time: Optional[str] = None
        self._consecutive_failures: int = 0

        # 当前持有者信息
        self._current_holder: Optional[Dict[str, Any]] = None

        # 等待队列（受 _state_lock 保护）
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
        """发起 SDK 登录（同步阻塞，带超时保护）"""
        try:
            from AmazingData import login

            result_container: List[Optional[bool]] = [None]
            error_container: List[Optional[Exception]] = [None]

            def _do_login():
                try:
                    result_container[0] = login(_SDK_USERNAME, _SDK_PASSWORD, _SDK_HOST, _SDK_PORT)
                except Exception as e:
                    error_container[0] = e

            t = threading.Thread(target=_do_login, daemon=True)
            t.start()
            t.join(timeout=30.0)

            if t.is_alive():
                print("[SDK] 登录超时（>30s）")
                self._set_state(ConnectionState.FAILED, "登录超时")
                return False

            if error_container[0]:
                print(f"[SDK] 登录异常：{error_container[0]}")
                self._set_state(ConnectionState.FAILED, str(error_container[0]))
                return False

            if result_container[0]:
                print("[SDK] 登录成功")
                return True
            else:
                print("[SDK] 登录失败")
                return False
        except Exception as e:
            print(f"[SDK] 登录异常：{e}")
            raise

    def _test_connection(self) -> bool:
        """
        测试当前 SDK 连接是否有效

        不创建新实例，使用 SDKManager 缓存的实例。
        避免额外的 InfoData() 创建导致连接数增加。
        """
        try:
            from services.common.sdk_manager import get_sdk_manager
            sdk_mgr = get_sdk_manager()

            # 状态标记为已连接，先检查缓存实例是否存在
            if sdk_mgr._info_instance is None:
                # 实例尚未创建，认为需要重新初始化
                return False

            info = sdk_mgr._info_instance
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(info.get_income, ["600000.SH"], is_local=False)
                result = future.result(timeout=5.0)
            return result is not None
        except Exception:
            return False

    def ensure_connected(self, timeout: float = 30.0) -> bool:
        """
        确保 SDK 连接可用（同步调用，供中间层使用）

        流程：
        1. 检查当前状态
        2. 如已连接：直接返回（不做主动探测，避免额外 SDK 调用干扰正常任务）
        3. 如未连接：发起登录
        4. 如连接失败：返回 False，附带错误信息

        连接有效性检测延迟到实际 SDK 调用失败时再进行（在 acquire 超时后）。

        Returns:
            True: 连接可用
            False: 连接不可用，调用 get_error_info() 获取详情
        """
        current_state = self.get_state()

        if current_state == ConnectionState.CONNECTED:
            # 已连接：直接信任，不做主动探测
            return True

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
    # 连接令牌管理（threading.Lock，跨线程串行化）
    # ================================================================

    def acquire(
        self,
        task_type: TaskType = TaskType.QUERY,
        task_id: Optional[str] = None,
        timeout: float = None
    ) -> Optional[ConnectionToken]:
        """
        获取 SDK 连接令牌（同步，跨线程安全）

        调用前应先调用 ensure_connected() 确保连接可用。
        此方法只负责并发排队串行化。

        Args:
            task_type: 任务类型
            task_id: 任务标识
            timeout: 等待超时时间（秒）

        Returns:
            ConnectionToken: 连接令牌，超时或不可用返回 None
        """
        # 检查连接状态
        if self.get_state() != ConnectionState.CONNECTED:
            return None

        if task_id is None:
            task_id = f"{task_type.value}_{uuid.uuid4().hex[:8]}"

        if timeout is None:
            if task_type == TaskType.QUERY:
                timeout = 15.0
            elif task_type == TaskType.DOWNLOAD:
                timeout = 300.0
            else:
                timeout = 60.0

        wait_start = time.monotonic()
        wait_info = {
            "task_id": task_id,
            "task_type": task_type.value,
            "wait_start": get_china_time()
        }
        with self._state_lock:
            self._waiting_count += 1
            self._waiting_tasks.append(wait_info)

        try:
            # threading.Lock.acquire 支持 timeout，跨线程生效
            acquired = self._serial_lock.acquire(timeout=timeout)

            elapsed = time.monotonic() - wait_start
            if elapsed > self._stats["max_wait_seconds"]:
                self._stats["max_wait_seconds"] = elapsed

            if not acquired:
                with self._state_lock:
                    self._waiting_count -= 1
                    if wait_info in self._waiting_tasks:
                        self._waiting_tasks.remove(wait_info)
                self._stats["total_timeout_fails"] += 1
                return None

            with self._state_lock:
                self._waiting_count -= 1
                if wait_info in self._waiting_tasks:
                    self._waiting_tasks.remove(wait_info)

            self._current_holder = {
                "task_id": task_id,
                "task_type": task_type.value,
                "acquired_at": get_china_time()
            }
            self._stats["total_acquires"] += 1

            return ConnectionToken(task_id, task_type, self)

        except Exception:
            with self._state_lock:
                self._waiting_count -= 1
                if wait_info in self._waiting_tasks:
                    self._waiting_tasks.remove(wait_info)
            self._stats["total_timeout_fails"] += 1
            return None

    def _release_token(self, token: ConnectionToken):
        """释放连接令牌（内部方法）"""
        with self._state_lock:
            if self._current_holder and self._current_holder["task_id"] == token.task_id:
                self._current_holder = None

        self._stats["total_releases"] += 1

        try:
            self._serial_lock.release()
        except RuntimeError:
            pass  # lock already released

    def get_status(self) -> Dict[str, Any]:
        """获取连接状态"""
        conn_state = self.get_state()
        error_info = self.get_error_info()

        with self._state_lock:
            if self._current_holder is None and self._waiting_count == 0:
                holder_status = "idle"
            elif self._current_holder:
                holder_status = "busy"
            else:
                holder_status = "queued"

            waiting_count = self._waiting_count
            waiting_tasks = list(self._waiting_tasks)

        holder_info = None
        if self._current_holder:
            elapsed = (get_china_time() - self._current_holder["acquired_at"]).total_seconds()
            holder_info = {
                "task_id": self._current_holder["task_id"],
                "task_type": self._current_holder["task_type"],
                "elapsed_seconds": round(elapsed)
            }

        waiting_list = []
        for w in waiting_tasks[:5]:
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
            "waiting_count": waiting_count,
            "waiting_tasks": waiting_list,
            "stats": self._stats
        }

    def is_busy(self) -> bool:
        """检查连接是否被占用"""
        return self._current_holder is not None

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
