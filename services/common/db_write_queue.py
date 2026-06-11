"""
数据库写入队列 — 混合方案

同步写入：业务关键操作（信号、交易、持仓），阻塞等待结果
异步写入：统计/日志/后台刷新，立即返回，后台执行

解决 SQLite 并发写入锁竞争问题，同时保证关键数据一致性。
"""

import asyncio
import threading
import queue
import time
from typing import Any, Dict, Optional, Callable
from dataclasses import dataclass
from enum import Enum

from services.common.structured_logger import get_logger


class WriteType(Enum):
    """写入类型"""
    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"
    EXECUTE = "execute"  # 原始 SQL 执行


class WriteMode(Enum):
    """写入模式"""
    SYNC = "sync"   # 同步：阻塞等待结果
    ASYNC = "async" # 异步：立即返回，后台执行


@dataclass
class WriteRequest:
    """写入请求"""
    request_id: int
    write_type: WriteType
    write_mode: WriteMode
    table: Optional[str]
    data: Optional[Dict]
    where: Optional[str]
    where_args: Optional[tuple]
    sql: Optional[str]
    sql_args: Optional[tuple]
    callback: Optional[Callable]  # 异步写入完成回调
    result_event: Optional[threading.Event]  # 同步写入用
    result: Any = None
    error: Optional[str] = None
    retry_count: int = 0
    created_at: float = 0.0


class DatabaseWriteQueue:
    """数据库写入队列 — 混合同步/异步模式"""

    _instance: Optional['DatabaseWriteQueue'] = None
    _lock = threading.Lock()

    # 配置
    MAX_RETRY = 3           # 失败重试次数
    RETRY_DELAY_MS = 50     # 重试间隔
    QUEUE_TIMEOUT_S = 30    # 同步写入等待超时

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self._initialized = True

        self._queue: queue.Queue[WriteRequest] = queue.Queue()
        self._worker_thread: Optional[threading.Thread] = None
        self._running = False
        self._request_counter = 0
        self._counter_lock = threading.Lock()
        self._logger = get_logger("db_write_queue")

        # 失败队列（异步写入失败后暂存，定期重试）
        self._failed_queue: queue.Queue[WriteRequest] = queue.Queue()
        self._retry_thread: Optional[threading.Thread] = None

        # 统计
        self._stats = {
            "total_writes": 0,
            "sync_writes": 0,
            "async_writes": 0,
            "failed_writes": 0,
            "retried_writes": 0,
            "total_wait_ms": 0,
            "queue_size": 0,
        }
        self._stats_lock = threading.Lock()

    def start(self):
        """启动写入工作线程和重试线程"""
        if self._running:
            return

        self._running = True

        # 主写入线程
        self._worker_thread = threading.Thread(
            target=self._worker_loop,
            name="db_write_worker",
            daemon=True
        )
        self._worker_thread.start()

        # 失败重试线程
        self._retry_thread = threading.Thread(
            target=self._retry_loop,
            name="db_write_retry",
            daemon=True
        )
        self._retry_thread.start()

        self._logger.log_event("db_write_queue_started", "数据库写入队列已启动（同步+异步）")

    def stop(self):
        """停止写入工作线程"""
        self._running = False
        self._queue.put(None)  # 停止信号
        self._failed_queue.put(None)

        if self._worker_thread:
            self._worker_thread.join(timeout=5)
        if self._retry_thread:
            self._retry_thread.join(timeout=2)

        self._logger.log_event("db_write_queue_stopped", "数据库写入队列已停止")

    def _worker_loop(self):
        """工作线程循环：从队列取出请求并执行"""
        from services.common.database import get_sync_connection

        while self._running:
            try:
                request = self._queue.get(timeout=1.0)
                if request is None:
                    continue

                start_time = time.monotonic()
                request.created_at = start_time

                # 执行写入（带重试）
                success = False
                for retry in range(self.MAX_RETRY + 1):
                    request.retry_count = retry
                    try:
                        conn = get_sync_connection()
                        conn.row_factory = None

                        if request.write_type == WriteType.INSERT:
                            columns = ", ".join(request.data.keys())
                            placeholders = ", ".join(["?" for _ in request.data])
                            sql = f"INSERT OR REPLACE INTO {request.table} ({columns}) VALUES ({placeholders})"
                            conn.execute(sql, tuple(request.data.values()))
                            conn.commit()
                            request.result = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

                        elif request.write_type == WriteType.UPDATE:
                            set_clause = ", ".join([f"{k} = ?" for k in request.data.keys()])
                            sql = f"UPDATE {request.table} SET {set_clause} WHERE {request.where}"
                            conn.execute(sql, tuple(request.data.values()) + (request.where_args or tuple()))
                            conn.commit()
                            request.result = True

                        elif request.write_type == WriteType.DELETE:
                            sql = f"DELETE FROM {request.table} WHERE {request.where}"
                            conn.execute(sql, request.where_args or tuple())
                            conn.commit()
                            request.result = conn.execute("SELECT changes()").fetchone()[0]

                        elif request.write_type == WriteType.EXECUTE:
                            conn.execute(request.sql, request.sql_args or tuple())
                            conn.commit()
                            request.result = True

                        request.error = None
                        success = True
                        break

                    except Exception as e:
                        request.error = str(e)
                        if retry < self.MAX_RETRY:
                            time.sleep(self.RETRY_DELAY_MS / 1000)
                        continue

                elapsed_ms = (time.monotonic() - start_time) * 1000

                # 更新统计
                with self._stats_lock:
                    self._stats["total_writes"] += 1
                    if request.write_mode == WriteMode.SYNC:
                        self._stats["sync_writes"] += 1
                    else:
                        self._stats["async_writes"] += 1
                    self._stats["total_wait_ms"] += elapsed_ms
                    if not success:
                        self._stats["failed_writes"] += 1
                    if request.retry_count > 0:
                        self._stats["retried_writes"] += 1

                # 处理结果
                if request.write_mode == WriteMode.SYNC:
                    # 同步写入：通知等待的调用方
                    if request.result_event:
                        request.result_event.set()
                else:
                    # 异步写入：执行回调或记录失败
                    if success:
                        if request.callback:
                            try:
                                request.callback(request.result, None)
                            except Exception as cb_err:
                                self._logger.error("db_write_callback", f"回调执行失败: {cb_err}")
                    else:
                        # 异步写入失败：放入失败队列等待重试
                        self._failed_queue.put(request)
                        self._logger.warn("db_write_async_failed",
                            f"异步写入失败（已加入重试队列）: {request.error}")

            except queue.Empty:
                continue
            except Exception as e:
                self._logger.error("db_write_worker_error", f"工作线程异常: {e}")

    def _retry_loop(self):
        """失败重试线程：定期重试失败的异步写入"""
        while self._running:
            try:
                request = self._failed_queue.get(timeout=5.0)
                if request is None:
                    continue

                # 等待一段时间后重试
                time.sleep(0.5)

                # 重新提交到主队列
                request.write_mode = WriteMode.ASYNC  # 保持异步
                request.retry_count += 1
                self._queue.put(request)

                with self._stats_lock:
                    self._stats["retried_writes"] += 1

            except queue.Empty:
                continue
            except Exception as e:
                self._logger.error("db_write_retry_error", f"重试线程异常: {e}")

    def _get_request_id(self) -> int:
        """获取唯一请求 ID"""
        with self._counter_lock:
            self._request_counter += 1
            return self._request_counter

    def _update_queue_size(self):
        """更新队列大小统计"""
        with self._stats_lock:
            self._stats["queue_size"] = self._queue.qsize()

    # ── 同步写入接口（阻塞等待结果）──

    def insert(self, table: str, data: Dict) -> int:
        """插入记录（同步），返回 rowid"""
        request = WriteRequest(
            request_id=self._get_request_id(),
            write_type=WriteType.INSERT,
            write_mode=WriteMode.SYNC,
            table=table,
            data=data,
            where=None,
            where_args=None,
            sql=None,
            sql_args=None,
            callback=None,
            result_event=threading.Event(),
        )
        self._queue.put(request)
        self._update_queue_size()

        if request.result_event.wait(timeout=self.QUEUE_TIMEOUT_S):
            if request.error:
                raise RuntimeError(f"写入失败: {request.error}")
            return request.result
        else:
            raise RuntimeError("写入请求超时")

    def update(self, table: str, data: Dict, where: str, where_args: tuple = None) -> bool:
        """更新记录（同步）"""
        request = WriteRequest(
            request_id=self._get_request_id(),
            write_type=WriteType.UPDATE,
            write_mode=WriteMode.SYNC,
            table=table,
            data=data,
            where=where,
            where_args=where_args,
            sql=None,
            sql_args=None,
            callback=None,
            result_event=threading.Event(),
        )
        self._queue.put(request)
        self._update_queue_size()

        if request.result_event.wait(timeout=self.QUEUE_TIMEOUT_S):
            if request.error:
                raise RuntimeError(f"更新失败: {request.error}")
            return request.result
        else:
            raise RuntimeError("更新请求超时")

    def delete(self, table: str, where: str, where_args: tuple = None) -> int:
        """删除记录（同步），返回删除行数"""
        request = WriteRequest(
            request_id=self._get_request_id(),
            write_type=WriteType.DELETE,
            write_mode=WriteMode.SYNC,
            table=table,
            data=None,
            where=where,
            where_args=where_args,
            sql=None,
            sql_args=None,
            callback=None,
            result_event=threading.Event(),
        )
        self._queue.put(request)
        self._update_queue_size()

        if request.result_event.wait(timeout=self.QUEUE_TIMEOUT_S):
            if request.error:
                raise RuntimeError(f"删除失败: {request.error}")
            return request.result
        else:
            raise RuntimeError("删除请求超时")

    def execute(self, sql: str, args: tuple = None) -> bool:
        """执行原始 SQL（同步）"""
        request = WriteRequest(
            request_id=self._get_request_id(),
            write_type=WriteType.EXECUTE,
            write_mode=WriteMode.SYNC,
            table=None,
            data=None,
            where=None,
            where_args=None,
            sql=sql,
            sql_args=args,
            callback=None,
            result_event=threading.Event(),
        )
        self._queue.put(request)
        self._update_queue_size()

        if request.result_event.wait(timeout=self.QUEUE_TIMEOUT_S):
            if request.error:
                raise RuntimeError(f"执行失败: {request.error}")
            return request.result
        else:
            raise RuntimeError("执行请求超时")

    # ── 异步写入接口（立即返回）──

    def insert_async(self, table: str, data: Dict, callback: Callable = None) -> int:
        """插入记录（异步），返回 request_id

        Args:
            callback: 可选回调函数 callback(result, error)
        """
        request = WriteRequest(
            request_id=self._get_request_id(),
            write_type=WriteType.INSERT,
            write_mode=WriteMode.ASYNC,
            table=table,
            data=data,
            where=None,
            where_args=None,
            sql=None,
            sql_args=None,
            callback=callback,
            result_event=None,
        )
        self._queue.put(request)
        self._update_queue_size()
        return request.request_id

    def update_async(self, table: str, data: Dict, where: str,
                     where_args: tuple = None, callback: Callable = None) -> int:
        """更新记录（异步）"""
        request = WriteRequest(
            request_id=self._get_request_id(),
            write_type=WriteType.UPDATE,
            write_mode=WriteMode.ASYNC,
            table=table,
            data=data,
            where=where,
            where_args=where_args,
            sql=None,
            sql_args=None,
            callback=callback,
            result_event=None,
        )
        self._queue.put(request)
        self._update_queue_size()
        return request.request_id

    def execute_async(self, sql: str, args: tuple = None, callback: Callable = None) -> int:
        """执行原始 SQL（异步）"""
        request = WriteRequest(
            request_id=self._get_request_id(),
            write_type=WriteType.EXECUTE,
            write_mode=WriteMode.ASYNC,
            table=None,
            data=None,
            where=None,
            where_args=None,
            sql=sql,
            sql_args=args,
            callback=callback,
            result_event=None,
        )
        self._queue.put(request)
        self._update_queue_size()
        return request.request_id

    # ── 统计信息 ──

    def get_stats(self) -> Dict:
        """获取统计信息"""
        with self._stats_lock:
            stats = dict(self._stats)
            stats["avg_wait_ms"] = (
                stats["total_wait_ms"] / stats["total_writes"]
                if stats["total_writes"] > 0 else 0
            )
            stats["failed_queue_size"] = self._failed_queue.qsize()
            return stats


# 全局单例
def get_db_write_queue() -> DatabaseWriteQueue:
    """获取数据库写入队列单例"""
    return DatabaseWriteQueue()