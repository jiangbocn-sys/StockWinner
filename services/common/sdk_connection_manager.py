"""
SDK 连接生命周期管理器

架构：按需连接 + grace period 自动释放 + 主动 logout

状态机：
  DISCONNECTED → (任务到来) → login → CONNECTED → 执行查询 → 释放锁
                                                          ↓
                                                    等待 grace period (5s)
                                                     ↓ 有新任务       ↓ 无新任务
                                                保持连接        logout() → DISCONNECTED
                                                              + 清理 SDK 实例
                                              下次 login() 时创建新实例

根因修复：
  - 移除 _test_connection()（在 stale socket 上调用 SDK 方法导致 segfault）
  - 移除基于时间的健康检查
  - logout() 只在连接健康且无并发查询时调用（grace period 到期，_serial_lock 空闲）
  - logout() 后立即清理 _info_instance/_market_data_instance/_base_data_instance/_calendar
  - login() 前不再调用 logout()（login 前的 logout 在 stale 连接上会 segfault）
  - 新 login() 会自动替换 TGW 旧连接（TGW 服务端处理）

优先级：
- query（行情查询）：最高优先级，快速响应
- download/screening：普通优先级，分批次释放
"""

import threading
import time
from datetime import datetime
from typing import Optional, Dict, Any, List
from enum import Enum
import uuid

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
    DISCONNECTED = "disconnected"   # 未连接 / 已主动释放
    CONNECTING = "connecting"       # 正在连接中
    CONNECTED = "connected"         # 已连接，可用
    FAILED = "failed"               # 连接失败


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
    1. TGW 连接状态管理（按需连接，用完主动 logout 释放）
    2. 并发串行化（threading.Lock，所有 SDK 调用排队）
    3. 超时控制（等待超过阈值返回明确错误）
    4. 自动释放（最后一个查询完成后 5 秒无新任务 → logout → DISCONNECTED）
    5. 实例管理（logout 后清理所有 SDK 实例缓存，下次 login 重建）
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
        self._state_lock = threading.Lock()

        # 错误信息
        self._error_message: str = ""
        self._last_error_time: Optional[str] = None
        self._consecutive_failures: int = 0
        self._last_failed_time: Optional[float] = None  # time.time() of last FAILED state

        # 连接 TTL 跟踪（主动过期机制）
        self._connection_born_time: Optional[float] = None  # login 成功时的 time.time()
        self._observed_ttls: List[float] = []  # 观察到的连接寿命列表（超时/失效时的 elapsed）
        self._active_ttl: Optional[float] = None  # 当前生效的主动过期 TTL（秒），None=未观察到超时

        # 等待队列（受 _state_lock 保护）
        self._waiting_count = 0
        self._waiting_tasks: List[Dict[str, Any]] = []

        # Grace period 自动释放机制
        self._grace_period: float = 5.0  # 秒，最后一个查询完成后等待时间
        self._grace_lock = threading.Lock()  # 保护 _release_timer
        self._release_timer: Optional[threading.Timer] = None

        # 当前持有者信息
        self._current_holder: Optional[Dict[str, Any]] = None

        # 统计
        self._stats = {
            "total_acquires": 0,
            "total_releases": 0,
            "total_timeout_fails": 0,
            "total_reconnects": 0,
            "total_logins": 0,
            "total_logouts": 0,
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

    def _cancel_grace_timer(self):
        """取消 grace period 定时器（必须在 _grace_lock 下调用）"""
        if self._release_timer is not None:
            self._release_timer.cancel()
            self._release_timer = None

    def clear_sdk_instances(self):
        """清理所有 SDK 实例缓存（超时/重连后必须调用，防止使用失效 socket）"""
        try:
            from services.common.sdk_manager import SDKManager
            SDKManager._info_instance = None
            SDKManager._market_data_instance = None
            SDKManager._base_data_instance = None
            SDKManager._calendar = None
        except Exception:
            pass

    def invalidate_connection(self, reason: str = "SDK 超时"):
        """标记连接失效（SDK 超时/异常后调用）

        立即设置 FAILED 状态 + 清理 SDK 实例缓存 + 记录连接寿命。
        下次 ensure_connected() 会自动触发 login 重建连接。
        """
        logger = get_logger("sdk_connection")

        # 记录观察到的连接寿命
        self._record_connection_ttl()

        with self._state_lock:
            self._state = ConnectionState.FAILED
            self._consecutive_failures += 1
            self._current_holder = None
        self.clear_sdk_instances()
        logger.log_event("sdk_connection_invalid", f"{reason}，连接标记为 FAILED，下次查询自动重连")

    def _record_connection_ttl(self):
        """记录一次观察到的连接寿命（连接失效时调用）"""
        if self._connection_born_time is not None:
            elapsed = time.time() - self._connection_born_time
            self._observed_ttls.append(elapsed)
            # 保留最近 10 次观察
            if len(self._observed_ttls) > 10:
                self._observed_ttls = self._observed_ttls[-10:]
            # 取中位数 * 0.8 作为主动过期 TTL
            sorted_ttls = sorted(self._observed_ttls)
            median = sorted_ttls[len(sorted_ttls) // 2]
            self._active_ttl = median * 0.8
            self._connection_born_time = None  # 重置

    def _is_connection_expired(self) -> bool:
        """检查连接是否超过主动过期 TTL"""
        if self._connection_born_time is None:
            return False  # 未记录连接开始时间，不检查
        if self._active_ttl is None:
            return False  # 尚未观察到超时，不主动过期
        return (time.time() - self._connection_born_time) > self._active_ttl

    def _grace_period_expired(self):
        """Grace period 到期回调：如果没有新任务，直接断开连接

        不调用 logout() — logout 会阻塞。直接标记 DISCONNECTED + 清理实例，
        TGW 会靠 TCP keepalive 自动清理 orphan 连接。
        """
        with self._grace_lock:
            self._release_timer = None

        # 安全检查：确保无并发访问
        with self._state_lock:
            if self._waiting_count > 0 or self._current_holder is not None:
                return
            if self._state != ConnectionState.CONNECTED:
                return

        logger = get_logger("sdk_connection")

        # 清理 SDK 实例缓存
        self.clear_sdk_instances()

        self._set_state(ConnectionState.DISCONNECTED)
        logger.log_event("sdk_grace_release",
                         f"连接空闲超时，已断开（跳过 logout，TGW 自动清理）")
        self.clear_sdk_instances()

        self._set_state(ConnectionState.DISCONNECTED)
        logger.log_event("sdk_grace_release",
                         f"连接空闲超时，已 logout 并释放 TGW 连接")

    def _safe_logout(self, timeout: float = 3.0):
        """安全调用 logout（仅在连接健康、无并发时调用）

        带超时保护：如果 logout 阻塞（死 socket 无法发送），超时后放弃。
        """
        logger = get_logger("sdk_connection")
        try:
            from AmazingData import logout

            result_container: List[Optional[Exception]] = [None]

            def _run():
                try:
                    logout(_SDK_USERNAME)
                except Exception as e:
                    result_container[0] = e

            t = threading.Thread(target=_run, daemon=True)
            t.start()
            t.join(timeout=timeout)

            if t.is_alive():
                logger.log_event("sdk_logout_timeout",
                    f"logout 超时（{timeout}s），socket 可能已死，跳过")
                return

            if result_container[0]:
                logger.warn("sdk_logout_fail", f"logout 异常: {result_container[0]}")
            else:
                logger.log_event("sdk_logout_ok", f"logout 成功: {_SDK_USERNAME}")

            self._stats["total_logouts"] += 1
        except Exception as e:
            logger.warn("sdk_logout_fail", f"logout 异常（按已断开处理）: {e}")
            self._stats["total_logouts"] += 1

    def _sdk_login(self) -> bool:
        """发起 SDK 登录（同步阻塞，带超时保护）

        不在 login 前调用 logout() — 如果旧连接已失效（如 TGW token 超时），
        logout() 会 segfault。新 login() 会自动替换 TGW 旧连接。
        只有在已知连接健康时（grace period 到期），才由 _safe_logout() 调用 logout。

        重试策略：如果 login 失败（连接超限），等待 3 秒后重试一次。
        给 TGW 服务端时间清理 orphan 连接。
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

        # 连接超限重试策略：逐步退避（TGW 清理 orphan 连接需要时间，TCP keepalive 通常 2-5 分钟）
        # 最多重试 5 次：间隔 5s → 15s → 30s → 60s → 60s
        retry_delays = [5.0, 15.0, 30.0, 60.0, 60.0]
        for attempt in range(len(retry_delays) + 1):
            if attempt > 0:
                delay = retry_delays[attempt - 1]
                logger.log_event("sdk_login_retry",
                    f"第 {attempt} 次重试 login，等待 {delay}s（TGW 正在清理 orphan 连接）")
                time.sleep(delay)

            for host in hosts:
                start = time.monotonic()
                try:
                    from AmazingData import login

                    # 先尝试 logout 清除可能残留的旧连接（此时无活跃 socket，不会 segfault）
                    if attempt > 0:
                        try:
                            from AmazingData import logout
                            logout(_SDK_USERNAME)
                            logger.log_event("sdk_login_force_logout",
                                f"重试前调用 logout({_SDK_USERNAME}) 尝试清除 orphan 连接")
                        except Exception as logout_err:
                            logger.log_event("sdk_login_logout_ignore",
                                f"logout 失败（忽略）: {logout_err}")

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
                        self._consecutive_failures = 0
                        self._stats["total_logins"] += 1
                        # 记录连接建立时间，用于主动过期检测
                        self._connection_born_time = time.time()
                        if self._active_ttl is not None:
                            logger.log_event("sdk_ttl_info",
                                f"连接 TTL 跟踪：active_ttl={self._active_ttl:.0f}s, "
                                f"observations={len(self._observed_ttls)}")
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

        # 所有主机 + 所有重试都失败
        all_hosts = ", ".join(hosts)
        logger.error("sdk_login", f"所有主机均登录失败: {all_hosts}")
        self._set_state(ConnectionState.FAILED, f"无法连接 SDK 主机: {all_hosts}")
        return False

    def ensure_connected(self, timeout: float = 30.0) -> bool:
        """
        确保 SDK 连接可用（同步调用，供中间层使用）

        新逻辑：
        - DISCONNECTED → 尝试 login → CONNECTED 或 FAILED
        - CONNECTING → 返回 False（已在连接中）
        - CONNECTED → 直接返回 True（不做健康检查，避免 stale socket segfault）
        - FAILED → 尝试 login
        """
        logger = get_logger("sdk_connection")
        current_state = self.get_state()

        if current_state == ConnectionState.CONNECTED:
            # 检查连接是否超过主动过期 TTL（基于历史观察到的超时时间）
            if self._is_connection_expired():
                logger = get_logger("sdk_connection")
                born = self._connection_born_time or 0
                age = time.time() - born
                logger.log_event("sdk_connection_expired",
                    f"连接超过主动过期 TTL（age={age:.0f}s, active_ttl={self._active_ttl:.0f}s），主动重连")
                self.invalidate_connection(reason=f"连接主动过期 age={age:.0f}s")
                # 继续走下面的 login 流程
            else:
                return True

        if current_state == ConnectionState.CONNECTING:
            return False

        # DISCONNECTED 或 FAILED，需要 login
        is_reconnect = current_state == ConnectionState.FAILED

        # FAILED 状态加冷却期，避免每次请求都触发完整 login 周期（TGW orphan 连接清理需要时间）
        if is_reconnect and self._last_failed_time is not None:
            cooldown = 30.0  # 30 秒冷却
            if time.time() - self._last_failed_time < cooldown:
                return False

        with self._state_lock:
            if self._state == ConnectionState.CONNECTING:
                return False
            self._state = ConnectionState.CONNECTING

        try:
            success = self._sdk_login()
            if success:
                with self._state_lock:
                    self._state = ConnectionState.CONNECTED

                # 清理旧实例缓存，确保下次使用创建绑定到新 TCP 连接的新实例
                self.clear_sdk_instances()
                logger.log_event("sdk_instances_reset", "SDK 缓存实例已重置")

                self._stats["total_reconnects"] += 1
                if is_reconnect:
                    logger.log_event("sdk_reconnect", "SDK 连接重连成功")
                return True
            else:
                with self._state_lock:
                    self._state = ConnectionState.FAILED
                    self._consecutive_failures += 1
                    self._last_failed_time = time.time()
                return False
        except Exception as e:
            with self._state_lock:
                self._state = ConnectionState.FAILED
                self._error_message = str(e)
                self._last_error_time = get_china_time().strftime("%Y-%m-%d %H:%M:%S")
                self._consecutive_failures += 1
                self._last_failed_time = time.time()
            return False

    def disconnect(self):
        """主动断开连接（系统关闭时使用）

        不调用 logout() — logout 在活跃/死连接上都会无限阻塞。
        直接标记断开 + 清理实例，TGW 会靠 TCP keepalive 自动清理 orphan 连接。
        """
        logger = get_logger("sdk_connection")

        with self._grace_lock:
            self._cancel_grace_timer()

        with self._state_lock:
            self._state = ConnectionState.DISCONNECTED
            self._current_holder = None

        # 清理实例（不调用 logout）
        self.clear_sdk_instances()
        logger.log_event("sdk_disconnect", f"SDK 连接已断开（跳过 logout，TGW 自动清理）")

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

        调用方应确保在 acquire 前已调用 ensure_connected()。
        SDKManager._acquire_sync() 已自动处理。
        """
        logger = get_logger("sdk_connection")

        # 取消 grace timer（有新任务到来）
        with self._grace_lock:
            self._cancel_grace_timer()

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

        # 启动 grace period 定时器：如果 5 秒内无新任务，标记 DISCONNECTED
        with self._state_lock:
            has_waiting = self._waiting_count > 0

        if not has_waiting:
            with self._grace_lock:
                self._cancel_grace_timer()
                self._release_timer = threading.Timer(self._grace_period, self._grace_period_expired)
                self._release_timer.daemon = True
                self._release_timer.start()

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
            "stats": self._stats,
            "grace_period": self._grace_period,
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
