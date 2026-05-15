"""
结构化日志系统

统一日志基础设施，替代 print() / error_reporter / logging_config 混用。

输出通道：
1. Operational — logs/stockwinner.log（文本，人类可读）
2. Performance  — logs/performance.jsonl（JSON 单行，可解析）
3. Console     — stdout（同 operational 格式，便于 systemd/journal 集成）

所有日志写入通过 AsyncLogHandler 异步队列执行，不阻塞业务逻辑。
"""

import json
import logging
import queue
import atexit
import sys
import threading
import time
from pathlib import Path
from typing import Optional, Dict, Any

from services.common.timezone import get_china_time, CHINA_TZ

# ============================================================
# 日志目录
# ============================================================

LOG_DIR = Path(__file__).parent.parent.parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

# ============================================================
# 异步队列 Handler
# ============================================================


class AsyncLogHandler(logging.Handler):
    """异步日志 Handler：记录推入 queue.Queue，后台线程消费。

    使用 queue.Queue（线程安全）而非 asyncio.Queue，因为
    SDK 调用、调度任务等在纯线程环境中执行，无事件循环。
    """

    def __init__(self, wrapped: logging.Handler, queue_size: int = 10000):
        super().__init__()
        self.setLevel(wrapped.level)
        self.setFormatter(wrapped.formatter)
        self._queue: queue.Queue = queue.Queue(maxsize=queue_size)
        self._wrapped = wrapped
        self._shutdown = threading.Event()
        self._worker = threading.Thread(
            target=self._process_queue, daemon=True, name="async-log-writer"
        )
        self._worker.start()

    def emit(self, record):
        try:
            self._queue.put_nowait(record)
        except queue.Full:
            # 队列满：降级直写（不丢日志，但阻塞）
            try:
                self._wrapped.emit(record)
            except Exception:
                pass

    def _process_queue(self):
        while not self._shutdown.is_set():
            try:
                record = self._queue.get(timeout=1.0)
                try:
                    self._wrapped.emit(record)
                except Exception:
                    pass  # 不崩溃 writer 线程
                self._queue.task_done()
            except queue.Empty:
                continue

    def flush(self):
        self._queue.join()

    def close(self):
        self._shutdown.set()
        # 排空剩余队列
        while not self._queue.empty():
            try:
                record = self._queue.get_nowait()
                self._wrapped.emit(record)
            except queue.Empty:
                break
        self._wrapped.close()
        super().close()


# ============================================================
# JSON Formatter（Performance 日志用）
# ============================================================


class JSONFormatter(logging.Formatter):
    def format(self, record):
        log_obj = {
            "ts": self.formatTime(record),
            "level": record.levelname,
            "module": record.name,
            "msg": record.getMessage(),
        }
        if hasattr(record, "structured"):
            log_obj.update(record.structured)
        return json.dumps(log_obj, ensure_ascii=False, default=str)


# ============================================================
# StructuredLogger
# ============================================================

# 缓存已初始化的 root logger
_root_logger: Optional[logging.Logger] = None
_logger_cache: Dict[str, "StructuredLogger"] = {}
_cache_lock = threading.Lock()


def _get_root_logger() -> logging.Logger:
    """初始化根 logger（仅一次），返回共享的 logging.Logger。"""
    global _root_logger
    if _root_logger is not None:
        return _root_logger

    root = logging.getLogger("StockWinner")
    root.setLevel(logging.INFO)
    root.propagate = False  # 避免重复输出到 basicConfig 的根 StreamHandler

    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    json_fmt = JSONFormatter()

    # Operational: 文本，每日轮转，10 备份
    from logging.handlers import TimedRotatingFileHandler

    op_handler = TimedRotatingFileHandler(
        LOG_DIR / "stockwinner.log",
        when="midnight", backupCount=10, encoding="utf-8",
    )
    op_handler.setFormatter(fmt)
    op_handler.setLevel(logging.INFO)

    # Performance: JSON，每日轮转，5 备份
    perf_handler = TimedRotatingFileHandler(
        LOG_DIR / "performance.jsonl",
        when="midnight", backupCount=5, encoding="utf-8",
    )
    perf_handler.setFormatter(json_fmt)
    perf_handler.setLevel(logging.INFO)

    # Console: 文本
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(fmt)
    console_handler.setLevel(logging.INFO)

    # 异步包装
    root.addHandler(AsyncLogHandler(op_handler))
    root.addHandler(AsyncLogHandler(perf_handler))
    root.addHandler(AsyncLogHandler(console_handler))

    _root_logger = root

    # 注册关机 flush
    atexit.register(_shutdown_loggers)

    return root


def _shutdown_loggers():
    """关机时排空异步队列。"""
    root = _root_logger
    if root is None:
        return
    for h in root.handlers[:]:
        if isinstance(h, AsyncLogHandler):
            h.flush()
            h.close()
    time.sleep(0.2)  # 给后台线程时间排空


class StructuredLogger:
    """结构化日志封装。

    所有方法不阻塞调用方（日志写入走异步队列）。
    """

    def __init__(self, name: str):
        self._name = name
        self._logger = _get_root_logger().getChild(name)

    # ---------------------------------------------------------------
    # 通用事件日志 → operational（文本）
    # ---------------------------------------------------------------

    def log_event(self, event_type: str, message: str = "", **context):
        """记录结构化事件。"""
        ctx = {"event": event_type, **context}
        msg = message or event_type
        self._logger.info(f"{msg} | {json.dumps(ctx, ensure_ascii=False, default=str)}")

    # ---------------------------------------------------------------
    # 耗时指标 → performance（JSON）
    # ---------------------------------------------------------------

    def log_duration(self, operation: str, duration_ms: float, **context):
        """记录操作耗时（写入 performance.jsonl）。"""
        record = logging.LogRecord(
            name=self._logger.name,
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg=operation,
            args=(),
            exc_info=None,
        )
        record.structured = {
            "event_type": "duration",
            "operation": operation,
            "duration_ms": round(duration_ms, 1),
            **context,
        }
        self._logger.handle(record)

    def log_sdk_call(
        self,
        method: str,
        duration_ms: float,
        task_type: str,
        status: str,
        error: Optional[str] = None,
        **context,
    ):
        """记录 SDK 调用性能。"""
        record = logging.LogRecord(
            name=self._logger.name,
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg=f"SDK {method} {status}",
            args=(),
            exc_info=None,
        )
        structured: Dict[str, Any] = {
            "event_type": "sdk_call",
            "method": method,
            "duration_ms": round(duration_ms, 1),
            "task_type": task_type,
            "status": status,
        }
        if error:
            structured["error"] = error
        structured.update(context)
        record.structured = structured
        self._logger.handle(record)

    def log_request(
        self,
        method: str,
        path: str,
        status_code: int,
        duration_ms: float,
        source: str,
        agent_id: Optional[str] = None,
        account_id: Optional[str] = None,
        **context,
    ):
        """记录 HTTP 请求审计。"""
        record = logging.LogRecord(
            name=self._logger.name,
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg=f"{method} {path} {status_code}",
            args=(),
            exc_info=None,
        )
        structured: Dict[str, Any] = {
            "event_type": "http_request",
            "method": method,
            "path": path,
            "status_code": status_code,
            "duration_ms": round(duration_ms, 1),
            "source": source,
        }
        if agent_id:
            structured["agent_id"] = agent_id
        if account_id:
            structured["account_id"] = account_id
        structured.update(context)
        record.structured = structured
        self._logger.handle(record)

    # ---------------------------------------------------------------
    # 兼容 ErrorReporter 接口
    # ---------------------------------------------------------------

    def error(self, component: str, message: str, context: Optional[Dict[str, Any]] = None):
        extra = f" | {json.dumps(context, ensure_ascii=False, default=str)}" if context else ""
        self._logger.error(f"[{component}] ERROR | {message}{extra}")

    def warn(self, component: str, message: str, context: Optional[Dict[str, Any]] = None):
        extra = f" | {json.dumps(context, ensure_ascii=False, default=str)}" if context else ""
        self._logger.warning(f"[{component}] WARN | {message}{extra}")

    def info(self, component: str, message: str, context: Optional[Dict[str, Any]] = None):
        extra = f" | {json.dumps(context, ensure_ascii=False, default=str)}" if context else ""
        self._logger.info(f"[{component}] INFO | {message}{extra}")

    def debug(self, component: str, message: str, context: Optional[Dict[str, Any]] = None):
        extra = f" | {json.dumps(context, ensure_ascii=False, default=str)}" if context else ""
        self._logger.debug(f"[{component}] DEBUG | {message}{extra}")


def get_logger(name: str) -> StructuredLogger:
    """获取 StructuredLogger 实例（带缓存）。"""
    with _cache_lock:
        if name not in _logger_cache:
            _logger_cache[name] = StructuredLogger(name)
        return _logger_cache[name]
