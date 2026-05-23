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
import time
from datetime import datetime
from typing import Optional, Dict, Any, List
from enum import Enum
from dataclasses import dataclass
import uuid
import concurrent.futures

from services.common.timezone import get_china_time
from services.common.structured_logger import get_logger

# SDK 登录参数
import os
_SDK_USERNAME = os.environ.get("SDK_USERNAME", "")
_SDK_PASSWORD = os.environ.get("SDK_PASSWORD", "")
_SDK_HOST = os.environ.get("SDK_HOST", "")
_SDK_HOST_BACKUP = os.environ.get("SDK_HOST_BACKUP", "")
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

        # 连接健康检查
        self._last_success_time: float = 0.0  # 最后一次成功查询的时间戳
        self._health_check_interval: float = 300.0  # 健康检查间隔（秒），5 分钟

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
        """发起 SDK 登录（同步阻塞，带超时保护）

        先尝试主 IP，失败后自动尝试备 IP
        """
        logger = get_logger("sdk_connection")

        # 构建候选主机列表
        hosts = [_SDK_HOST] if _SDK_HOST else []
        if _SDK_HOST_BACKUP and _SDK_HOST_BACKUP not in hosts:
            hosts.append(_SDK_HOST_BACKUP)

        if not hosts:
            logger.error("sdk_login", "无可用 SDK 主机地址")
            self._set_state(ConnectionState.FAILED, "未配置 SDK 主机地址")
            return False

        for host in hosts:
            start = time.monotonic()
            try:
                from AmazingData import login

                result_container: List[Optional[bool]] = [None]
                error_container: List[Optional[Exception]] = [None]

                def _do_login():
                    try:
                        result_container[0] = login(_SDK_USERNAME, _SDK_PASSWORD, host, _SDK_PORT)
                    except Exception as e:
                        error_container[0] = e

                t = threading.Thread(target=_do_login, daemon=True)
                t.start()
                t.join(timeout=30.0)

                elapsed_ms = (time.monotonic() - start) * 1000

                if t.is_alive():
                    logger.warn("sdk_login", f"{host}:{_SDK_PORT} 登录超时")
                    logger.log_sdk_call("login", elapsed_ms, "connect", "timeout",
                                        host=host, error="登录超时")
                    continue

                if error_container[0]:
                    err_msg = str(error_container[0])
                    logger.warn("sdk_login", f"{host}:{_SDK_PORT} 登录异常: {err_msg}")
                    logger.log_sdk_call("login", elapsed_ms, "connect", "error",
                                        host=host, error=err_msg)
                    continue

                if result_container[0]:
                    logger.log_sdk_call("login", elapsed_ms, "connect", "success", host=host)
                    logger.log_event("sdk_login_success", f"SDK 登录成功 @ {host}:{_SDK_PORT}")
                    self._set_state(ConnectionState.CONNECTED)
                    return True
                else:
                    logger.warn("sdk_login", f"{host}:{_SDK_PORT} 登录失败（返回 False）")
                    logger.log_sdk_call("login", elapsed_ms, "connect", "failure", host=host)
                    continue
            except Exception as e:
                elapsed_ms = (time.monotonic() - start) * 1000
                logger.error("sdk_login", f"登录异常: {e}")
                logger.log_sdk_call("login", elapsed_ms, "connect", "error", host=host, error=str(e))
                raise

        # 所有主机都失败
        all_hosts = ", ".join(hosts)
        logger.error("sdk_login", f"所有主机均登录失败: {all_hosts}")
        self._set_state(ConnectionState.FAILED, f"无法连接 SDK 主机: {all_hosts}")
        return False

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

        如果连接已 CONNECTED 但长时间未成功查询（超过健康检查间隔），
        先测试连接是否实际存活，死连接则触发重连。
        """
        logger = get_logger("sdk_connection")
        current_state = self.get_state()

        if current_state == ConnectionState.CONNECTED:
            # 连接标记为 CONNECTED，但可能已过期（TGW token 超时）
            # 检查距离上次成功查询是否超过健康检查间隔
            import time
            if self._last_success_time > 0 and (time.time() - self._last_success_time) > self._health_check_interval:
                # 需要健康检查
                if not self._test_connection():
                    logger.log_event("sdk_stale_detected", "检测到 SDK 连接已失效，准备重连")
                    with self._state_lock:
                        self._state = ConnectionState.FAILED
                        self._consecutive_failures += 1
                    # 下方继续重连逻辑
                    current_state = ConnectionState.FAILED
                else:
                    # 连接仍然有效，更新时间戳
                    self._last_success_time = time.time()
                    return True
            else:
                return True

        # 需要连接/重连
        is_reconnect = current_state == ConnectionState.FAILED
        with self._state_lock:
            if self._state == ConnectionState.CONNECTING:
                return False
            self._state = ConnectionState.CONNECTING

        try:
            success = self._sdk_login()
            if success:
                import time
                with self._state_lock:
                    self._state = ConnectionState.CONNECTED
                    self._consecutive_failures = 0
                    self._last_success_time = time.time()
                self._stats["total_reconnects"] += 1
                if is_reconnect:
                    logger.log_event("sdk_reconnect", "SDK 连接重连成功")
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
        logger = get_logger("sdk_connection")

        # 调用 SDK logout 关闭 C 层 TGW socket
        try:
            from AmazingData import logout
            logout(_SDK_USERNAME)
            logger.log_event("sdk_logout", f"SDK logout 调用成功: {_SDK_USERNAME}")
        except Exception as e:
            logger.warn("sdk_logout", f"logout 异常: {e}")

        with self._state_lock:
            self._state = ConnectionState.DISCONNECTED
            self._current_holder = None

    def record_query_success(self):
        """记录一次成功的 SDK 查询（更新健康检查时间戳）"""
        import time
        self._last_success_time = time.time()

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
        """
        logger = get_logger("sdk_connection")

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
            acquired = self._serial_lock.acquire(timeout=timeout)

            elapsed = time.monotonic() - wait_start
            elapsed_ms = elapsed * 1000
            if elapsed_ms > self._stats["max_wait_seconds"]:
                self._stats["max_wait_seconds"] = elapsed_ms

            if not acquired:
                with self._state_lock:
                    self._waiting_count -= 1
                    if wait_info in self._waiting_tasks:
                        self._waiting_tasks.remove(wait_info)
                self._stats["total_timeout_fails"] += 1
                logger.log_duration("sdk_acquire_wait", elapsed_ms,
                                    task_id=task_id, task_type=task_type.value,
                                    status="timeout", timeout=timeout)
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

            logger.log_duration("sdk_acquire_wait", elapsed_ms,
                                task_id=task_id, task_type=task_type.value,
                                status="success")

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
        logger = get_logger("sdk_connection")

        acquired_at = None
        with self._state_lock:
            if self._current_holder and self._current_holder["task_id"] == token.task_id:
                acquired_at = self._current_holder.get("acquired_at")
                self._current_holder = None

        self._stats["total_releases"] += 1

        if acquired_at:
            hold_ms = (get_china_time() - acquired_at).total_seconds() * 1000
            logger.log_duration("sdk_token_hold", hold_ms,
                                task_id=token.task_id, task_type=token.task_type.value)

        try:
            self._serial_lock.release()
        except RuntimeError:
            pass

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
