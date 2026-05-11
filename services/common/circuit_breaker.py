"""
熔断器 — SDK 失败保护

当 SDK 连续失败 N 次后自动进入熔断状态，避免无限重试消耗 TGW 连接资源。
熔断期间所有调用立即返回错误，不发起实际请求。

使用示例:
    from services.common.circuit_breaker import circuit_breaker

    try:
        token = circuit_breaker.execute(lambda: sdk.query_kline(...))
    except CircuitOpenError:
        print("SDK 已熔断，请稍后重试")
"""

import time
import threading
from typing import Callable, Any, Optional


class CircuitOpenError(Exception):
    """熔断器处于打开状态时抛出"""
    def __init__(self, remaining_seconds: float):
        self.remaining_seconds = remaining_seconds
        super().__init__(f"熔断器已打开，{remaining_seconds:.0f}s 后可重试")


class CircuitBreaker:
    """熔断器实现

    状态流转:
        CLOSED (正常) → OPEN (熔断) → HALF_OPEN (半开) → CLOSED (恢复)

    参数:
        failure_threshold: 连续失败多少次后熔断
        recovery_timeout: 熔断后多少秒进入半开状态
        half_open_max_calls: 半开状态允许的最大调用次数（成功后恢复 CLOSED）
    """

    def __init__(self, failure_threshold: int = 5, recovery_timeout: int = 300,
                 half_open_max_calls: int = 1):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_max_calls = half_open_max_calls

        self._state = "CLOSED"
        self._failure_count = 0
        self._opened_at: Optional[float] = None
        self._half_open_calls = 0
        self._lock = threading.Lock()

    @property
    def state(self) -> str:
        return self._state

    @property
    def failure_count(self) -> int:
        return self._failure_count

    def execute(self, fn: Callable[..., Any], *args, **kwargs) -> Any:
        """
        执行函数，自动管理熔断状态。

        - CLOSED: 正常执行，失败则计数，达到阈值则熔断
        - OPEN: 检查是否超时，超时则转为 HALF_OPEN，否则立即抛异常
        - HALF_OPEN: 允许有限次调用，成功则恢复 CLOSED，失败则重新 OPEN
        """
        with self._lock:
            self._check_state()
            if self._state == "OPEN":
                raise CircuitOpenError(self._remaining_time())

        try:
            result = fn(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            raise

    def _check_state(self):
        """检查并可能转换状态"""
        if self._state == "OPEN" and self._should_attempt_reset():
            self._state = "HALF_OPEN"
            self._half_open_calls = 0

    def _on_success(self):
        with self._lock:
            if self._state == "HALF_OPEN":
                self._half_open_calls += 1
                if self._half_open_calls >= self.half_open_max_calls:
                    self._reset()
            elif self._state == "CLOSED":
                self._failure_count = 0

    def _on_failure(self):
        with self._lock:
            if self._state == "HALF_OPEN":
                self._open()
            elif self._state == "CLOSED":
                self._failure_count += 1
                if self._failure_count >= self.failure_threshold:
                    self._open()

    def _open(self):
        self._state = "OPEN"
        self._opened_at = time.time()

    def _reset(self):
        self._state = "CLOSED"
        self._failure_count = 0
        self._opened_at = None
        self._half_open_calls = 0

    def _should_attempt_reset(self) -> bool:
        if self._opened_at is None:
            return False
        return (time.time() - self._opened_at) >= self.recovery_timeout

    def _remaining_time(self) -> float:
        if self._opened_at is None:
            return 0
        elapsed = time.time() - self._opened_at
        return max(0, self.recovery_timeout - elapsed)

    def reset(self):
        """手动重置熔断器"""
        with self._lock:
            self._reset()

    def get_status(self) -> dict:
        """获取熔断器状态"""
        with self._lock:
            remaining = self._remaining_time() if self._state == "OPEN" else 0
            return {
                "state": self._state,
                "failure_count": self._failure_count,
                "remaining_seconds": round(remaining, 1),
            }


# 全局单例
circuit_breaker = CircuitBreaker(failure_threshold=5, recovery_timeout=300)
