"""
数据库管理器
- 异步：aiosqlite + DatabaseManager（FastAPI 异步端点）
- 同步：sqlite3 连接池（后台任务、定时任务、工具脚本）

重要：
- 所有模块统一通过 get_sync_connection() / get_db_context() 获取连接
- 禁止在各模块中直接使用 sqlite3.connect()，避免 WAL mode / busy_timeout 不一致
- 系统维护两个数据库：stockwinner.db（业务数据）和 kline.db（行情数据）

优化 v1：
- 主事件循环固定：lifespan 启动时设置，避免每次 async 操作检测 loop_id 变化
- APScheduler 后台线程应使用 sync 连接（get_sync_connection），不访问 async pool
"""

import aiosqlite
import asyncio
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from contextlib import asynccontextmanager, contextmanager
import logging
import threading
import time

logger = logging.getLogger('Database')

DB_PATH = Path(__file__).parent.parent.parent / "data" / "stockwinner.db"
KLINE_DB_PATH = Path(__file__).parent.parent.parent / "data" / "kline.db"
PROJECT_ROOT = Path(__file__).parent.parent.parent
SPEC_DIR = PROJECT_ROOT / "spec"

# ── 主事件循环固定机制 ──
# 由 lifespan 在启动时设置，避免每次 async 操作都检测 loop_id 变化
_primary_loop: Optional[asyncio.AbstractEventLoop] = None
_primary_loop_lock = threading.Lock()


def set_primary_loop(loop: Optional[asyncio.AbstractEventLoop]):
    """设置主事件循环（由 lifespan 在启动时调用）

    设置后，DatabaseManager 将使用此循环创建连接池，
    不再每次操作都检测 loop_id 变化。
    """
    global _primary_loop
    with _primary_loop_lock:
        _primary_loop = loop
        if loop:
            logger.info(f"主事件循环已固定: loop_id={id(loop)}")


def get_primary_loop() -> Optional[asyncio.AbstractEventLoop]:
    """获取主事件循环"""
    return _primary_loop


def reset_primary_loop():
    """重置主事件循环（服务关闭时调用）"""
    global _primary_loop
    with _primary_loop_lock:
        _primary_loop = None


class DatabaseManager:
    """数据库管理器 — 使用连接池而非单连接，避免长时间运行后连接失效

    优化 v1：使用主事件循环固定机制，避免每次操作都检测 loop_id 变化
    """

    def __init__(self, db_path: Path = DB_PATH, pool_size: int = 5):
        self.db_path = db_path
        self._pool_size = pool_size
        self._pool: Optional[asyncio.Queue] = None  # 延迟初始化，避免跨事件循环问题
        self._initialized = False
        self._lock: Optional[asyncio.Lock] = None
        self._loop_id: Optional[int] = None  # 记录初始化时的事件循环 ID（仅用于诊断）

        # 吞吐量计数器（线程安全）
        self._counter_lock = threading.Lock()
        self._total_queries = 0
        self._total_reads = 0
        self._total_writes = 0
        self._total_rows_read = 0
        self._total_rows_written = 0

        # 上次快照（用于计算速率）
        self._last_snapshot_time = time.monotonic()
        self._snap_queries = 0
        self._snap_reads = 0
        self._snap_writes = 0
        self._snap_rows_read = 0
        self._snap_rows_written = 0

    def _ensure_loop_resources(self):
        """确保在主事件循环中初始化 pool 和 lock

        优化：不再每次操作都检测 loop_id 变化，而是使用 lifespan 设置的主事件循环。
        APScheduler 后台线程应使用 sync 连接（get_sync_connection），不访问 async pool。
        """
        # 使用主事件循环（由 lifespan 设置）
        primary_loop = get_primary_loop()
        if primary_loop is None:
            # 主循环未设置时，尝试获取当前运行循环（兼容旧启动流程）
            try:
                primary_loop = asyncio.get_running_loop()
            except RuntimeError:
                return  # 没有运行中的事件循环

        if self._lock is None:
            self._lock = asyncio.Lock()
            self._loop_id = id(primary_loop)
            logger.debug(f"DatabaseManager Lock 已创建，绑定到 loop_id={id(primary_loop)}")

        if self._pool is None:
            self._pool = asyncio.Queue()
            self._loop_id = id(primary_loop)
            logger.debug(f"DatabaseManager Pool 已创建，绑定到 loop_id={id(primary_loop)}")

    async def _create_connection(self) -> aiosqlite.Connection:
        """创建并配置新连接"""
        conn = await aiosqlite.connect(str(self.db_path))
        conn.row_factory = aiosqlite.Row
        await conn.execute("PRAGMA journal_mode = WAL")
        await conn.execute("PRAGMA foreign_keys = ON")
        await conn.execute("PRAGMA busy_timeout = 5000")
        return conn

    async def connect(self):
        """初始化连接池"""
        self.db_path.parent.mkdir(exist_ok=True)
        self._ensure_loop_resources()
        if self._lock is None or self._pool is None:
            return  # 事件循环未就绪
        async with self._lock:
            if self._initialized:
                return
            for _ in range(self._pool_size):
                conn = await self._create_connection()
                await self._pool.put(conn)
            self._initialized = True

    async def close(self):
        """关闭连接池中的所有连接"""
        self._ensure_loop_resources()
        if self._lock is None or self._pool is None:
            return
        async with self._lock:
            if not self._initialized:
                return
            while not self._pool.empty():
                try:
                    conn = self._pool.get_nowait()
                    await conn.close()
                except Exception:
                    pass
            self._initialized = False

    async def _acquire(self):
        """从池中获取一个连接（自动处理事件循环切换和初始化）"""
        self._ensure_loop_resources()
        if self._pool is None or not self._initialized:
            await self.connect()
        return await self._pool.get()

    async def _release(self, conn):
        """将连接释放回池中"""
        # 验证连接是否仍然有效
        try:
            await conn.execute("SELECT 1")
            await self._pool.put(conn)
        except Exception:
            # 连接已损坏，创建新连接替换
            try:
                await conn.close()
            except Exception:
                pass
            new_conn = await self._create_connection()
            await self._pool.put(new_conn)

    @asynccontextmanager
    async def transaction(self):
        """事务上下文 — 从池中获取连接，使用完毕后释放"""
        conn = await self._acquire()
        try:
            yield conn
            await conn.commit()
        except Exception as e:
            await conn.rollback()
            raise
        finally:
            await self._release(conn)

    async def _execute_with_conn(self, query: str, params: Tuple = (), commit: bool = True) -> aiosqlite.Cursor:
        """内部方法：从池中获取连接执行查询"""
        conn = await self._acquire()
        try:
            cursor = await conn.execute(query, params)
            if commit:
                await conn.commit()
            return cursor
        finally:
            await self._release(conn)

    async def execute(self, query: str, params: Tuple = ()) -> aiosqlite.Cursor:
        """执行 SQL"""
        self._increment_counters(writes=1, rows_written=1)
        return await self._execute_with_conn(query, params, commit=True)

    async def executemany(self, query: str, params_list: List[Tuple]) -> aiosqlite.Cursor:
        """批量执行 SQL"""
        self._increment_counters(writes=1, rows_written=len(params_list))
        conn = await self._acquire()
        try:
            cursor = await conn.executemany(query, params_list)
            await conn.commit()
            return cursor
        finally:
            await self._release(conn)

    async def fetchone(self, query: str, params: Tuple = ()) -> Optional[Dict]:
        """查询单条记录"""
        self._increment_counters(reads=1, rows_read=1)
        conn = await self._acquire()
        try:
            cursor = await conn.execute(query, params)
            row = await cursor.fetchone()
            return dict(row) if row else None
        finally:
            await self._release(conn)

    async def fetchall(self, query: str, params: Tuple = ()) -> List[Dict]:
        """查询多条记录"""
        self._increment_counters(reads=1, rows_read=0)  # 行数在返回后补充
        conn = await self._acquire()
        try:
            cursor = await conn.execute(query, params)
            rows = await cursor.fetchall()
            self._increment_counters(rows_read=len(rows))
            return [dict(row) for row in rows]
        finally:
            await self._release(conn)

    async def fetchval(self, query: str, params: Tuple = ()) -> Optional[Any]:
        """查询单个值"""
        self._increment_counters(reads=1, rows_read=1)
        conn = await self._acquire()
        try:
            cursor = await conn.execute(query, params)
            row = await cursor.fetchone()
            if row:
                return row[0]
            return None
        finally:
            await self._release(conn)

    async def insert(self, table: str, data: Dict) -> int:
        """插入记录"""
        columns = ', '.join(data.keys())
        placeholders = ', '.join(['?' for _ in data])
        query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
        conn = await self._acquire()
        try:
            self._increment_counters(writes=1, rows_written=1)
            cursor = await conn.execute(query, tuple(data.values()))
            await conn.commit()
            return cursor.lastrowid
        finally:
            await self._release(conn)

    async def update(self, table: str, data: Dict, where: str, params: Tuple = ()) -> int:
        """更新记录"""
        set_clause = ', '.join([f"{k} = ?" for k in data.keys()])
        query = f"UPDATE {table} SET {set_clause} WHERE {where}"
        conn = await self._acquire()
        try:
            cursor = await conn.execute(query, tuple(data.values()) + params)
            await conn.commit()
            self._increment_counters(writes=1, rows_written=cursor.rowcount or 1)
            return cursor.rowcount
        finally:
            await self._release(conn)

    async def delete(self, table: str, where: str, params: Tuple = ()) -> int:
        """删除记录"""
        query = f"DELETE FROM {table} WHERE {where}"
        conn = await self._acquire()
        try:
            cursor = await conn.execute(query, params)
            await conn.commit()
            self._increment_counters(writes=1, rows_written=cursor.rowcount or 1)
            return cursor.rowcount
        finally:
            await self._release(conn)

    async def commit(self):
        """提交事务 — 池化模式下不直接支持，建议使用 transaction()"""
        pass

    async def rollback(self):
        """回滚事务 — 池化模式下不直接支持，建议使用 transaction()"""
        pass

    def _increment_counters(self, reads: int = 0, writes: int = 0, rows_read: int = 0, rows_written: int = 0):
        """线程安全计数器"""
        with self._counter_lock:
            self._total_queries += reads + writes
            self._total_reads += reads
            self._total_writes += writes
            self._total_rows_read += rows_read
            self._total_rows_written += rows_written

    def get_throughput(self) -> Dict:
        """
        获取数据库吞吐量统计。
        返回累计值 + 上次调用以来的速率（queries/sec, reads/sec, writes/sec）。
        """
        now = time.monotonic()
        with self._counter_lock:
            elapsed = now - self._last_snapshot_time if now > self._last_snapshot_time else 1.0
            delta_q = self._total_queries - self._snap_queries
            delta_r = self._total_reads - self._snap_reads
            delta_w = self._total_writes - self._snap_writes
            delta_rr = self._total_rows_read - self._snap_rows_read
            delta_rw = self._total_rows_written - self._snap_rows_written

            qps = round(delta_q / elapsed, 1) if elapsed > 0 else 0
            rps = round(delta_r / elapsed, 1) if elapsed > 0 else 0
            wps = round(delta_w / elapsed, 1) if elapsed > 0 else 0
            rrs = round(delta_rr / elapsed, 1) if elapsed > 0 else 0
            rws = round(delta_rw / elapsed, 1) if elapsed > 0 else 0

            # 更新快照
            self._snap_queries = self._total_queries
            self._snap_reads = self._total_reads
            self._snap_writes = self._total_writes
            self._snap_rows_read = self._total_rows_read
            self._snap_rows_written = self._total_rows_written
            self._last_snapshot_time = now

        return {
            "total_queries": self._total_queries,
            "total_reads": self._total_reads,
            "total_writes": self._total_writes,
            "total_rows_read": self._total_rows_read,
            "total_rows_written": self._total_rows_written,
            "queries_per_sec": qps,
            "reads_per_sec": rps,
            "writes_per_sec": wps,
            "rows_read_per_sec": rrs,
            "rows_written_per_sec": rws,
        }


# 全局单例
_db_manager: Optional[DatabaseManager] = None


def get_db_manager() -> DatabaseManager:
    """获取数据库管理器单例"""
    global _db_manager
    if _db_manager is None:
        _db_manager = DatabaseManager()
    return _db_manager


def reset_db_manager():
    """重置数据库管理器"""
    global _db_manager
    _db_manager = None


# ================================================================
# 同步连接池 — 供后台任务、定时任务、工具脚本使用
# ================================================================

# 连接老化配置
_CONNECTION_MAX_AGE_SECONDS = 3600  # 1小时未使用则清理
_CONNECTION_MAX_IDLE_COUNT = 50     # 最多缓存50个连接

# 每个线程独立的连接缓存：thread_id → {cache_key: (connection, last_used_time)}
_thread_connections: Dict[int, Dict[str, Tuple[sqlite3.Connection, float]]] = {}
_connections_lock = threading.Lock()
_last_cleanup_time: float = 0.0  # 上次清理时间

# 已知数据库路径
DATABASE_PATHS = {
    "stockwinner": DB_PATH,
    "kline": KLINE_DB_PATH,
}


def _configure_connection(conn: sqlite3.Connection):
    """配置 SQLite 连接：WAL mode、busy timeout、foreign keys"""
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA busy_timeout = 30000")  # 30秒，避免多线程写入竞争
    conn.execute("PRAGMA foreign_keys = ON")


def configure_kline_connection(conn: sqlite3.Connection):
    """配置 kline.db 的 sqlite3 连接：WAL mode + busy timeout

    供所有直接 sqlite3.connect(kline.db) 的位置调用，确保 PRAGMA 一致。
    用法：
        conn = sqlite3.connect(kline_path, timeout=30)
        configure_kline_connection(conn)
    """
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA busy_timeout = 30000")
    conn.execute("PRAGMA foreign_keys = ON")


def get_sync_connection(db_name: str = "stockwinner",
                        path: Optional[Path] = None) -> sqlite3.Connection:
    """
    获取预配置的 sqlite3 连接（线程缓存，自动复用，老化清理）

    Args:
        db_name: 预定义数据库名称 ("stockwinner" | "kline")
        path: 自定义数据库路径（覆盖 db_name）

    Returns:
        已配置 WAL mode + busy_timeout 的 sqlite3 连接

    使用示例:
        conn = get_sync_connection("kline")
        cursor = conn.execute("SELECT ...")
        # 连接自动缓存，无需手动 close

    资源管理:
        - 连接缓存带 last_used_time 时间戳
        - 每次获取连接时检查老化，清理超过1小时未用的连接
        - 总缓存数超过50时，清理最旧的连接
    """
    global _last_cleanup_time

    db_path = path or DATABASE_PATHS.get(db_name)
    if db_path is None:
        raise ValueError(f"未知数据库: {db_name}，可选: {list(DATABASE_PATHS.keys())}")

    db_path.parent.mkdir(parents=True, exist_ok=True)

    thread_id = threading.current_thread().ident
    cache_key = f"{thread_id}:{db_path}"
    now = time.monotonic()

    with _connections_lock:
        # 定期清理老化连接（每次调用检查，但只每5分钟执行一次清理）
        if now - _last_cleanup_time > 300:
            _cleanup_stale_connections(now)
            _last_cleanup_time = now

        thread_conns = _thread_connections.get(thread_id)
        if thread_conns is None:
            thread_conns = {}
            _thread_connections[thread_id] = thread_conns

        entry = thread_conns.get(cache_key)
        if entry is None:
            conn = sqlite3.connect(str(db_path))
            conn.row_factory = sqlite3.Row
            _configure_connection(conn)
            thread_conns[cache_key] = (conn, now)
        else:
            conn, last_used = entry
            # 验证连接是否仍然有效
            try:
                conn.execute("SELECT 1")
                # 更新使用时间
                thread_conns[cache_key] = (conn, now)
            except Exception:
                try:
                    conn.close()
                except Exception:
                    pass
                conn = sqlite3.connect(str(db_path))
                conn.row_factory = sqlite3.Row
                _configure_connection(conn)
                thread_conns[cache_key] = (conn, now)

    return conn


def query_kline_db(sql: str, params: tuple = None, adjusted: bool = True):
    """同步查询 kline.db（只读）。返回 List[Dict] 或 int。

    用于策略沙盒环境，统一封装数据库查询操作。
    默认自动对 kline_data/weekly_kline_data 查询结果应用后复权。

    Args:
        sql: SQL 查询语句
        params: 参数元组
        adjusted: 是否自动复权（默认 True）
    """
    conn = get_sync_connection("kline")
    cursor = conn.cursor()
    try:
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        if sql.strip().upper().startswith("SELECT"):
            rows = [dict(r) for r in cursor.fetchall()]
            if adjusted:
                rows = _auto_adjust_kline_result(rows, sql, params)
            return rows
        return cursor.rowcount
    except Exception:
        conn.rollback()
        raise


def query_kline_db_raw(sql: str, params: tuple = None):
    """同步查询 kline.db（不复权）。返回 List[Dict] 或 int。

    用于K线图表等需要展示原始价格的场景。
    """
    return query_kline_db(sql, params, adjusted=False)


def _auto_adjust_kline_result(rows, sql, params):
    """自动对K线查询结果应用后复权。

    检测条件：SQL 查询 kline_data 或 weekly_kline_data 表，
    且 params[0] 是单个股票代码（非 IN 批量查询）。
    """
    if not rows or not params:
        return rows
    sql_lower = sql.lower()
    if "kline_data" not in sql_lower and "weekly_kline_data" not in sql_lower:
        return rows
    # 批量查询（IN 子句）跳过
    if " in " in sql_lower and "(" in sql_lower:
        return rows
    stock_code = params[0]
    if not isinstance(stock_code, str) or "." not in stock_code:
        return rows
    from services.common.price_adjuster import adjust_klines
    date_field = "week_start_date" if "weekly_kline_data" in sql_lower else "trade_date"
    return adjust_klines(rows, stock_code, date_field=date_field)


def query_db(sql: str, params: tuple = None):
    """同步查询 stockwinner.db。返回 List[Dict] 或 int。

    用于策略沙盒环境，统一封装数据库查询操作。
    """
    conn = get_sync_connection()
    cursor = conn.cursor()
    try:
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        if sql.strip().upper().startswith("SELECT"):
            rows = [dict(r) for r in cursor.fetchall()]
            return rows
        conn.commit()
        return cursor.rowcount
    except Exception:
        conn.rollback()
        raise


def _cleanup_stale_connections(now: float):
    """清理老化连接：超过1小时未用或总缓存超过限制"""
    total_count = 0
    stale_keys = []

    # 统计并收集老化连接
    for thread_id, thread_conns in list(_thread_connections.items()):
        for cache_key, entry in list(thread_conns.items()):
            conn, last_used = entry
            total_count += 1
            age = now - last_used
            if age > _CONNECTION_MAX_AGE_SECONDS:
                stale_keys.append((thread_id, cache_key, conn, age))

    # 清理老化连接
    for thread_id, cache_key, conn, age in stale_keys:
        try:
            conn.close()
            logger.debug(f"关闭老化连接: {cache_key} (age={age:.0f}s)")
        except Exception:
            pass
        if thread_id in _thread_connections:
            _thread_connections[thread_id].pop(cache_key, None)

    # 如果总缓存数超过限制，清理最旧的
    if total_count > _CONNECTION_MAX_IDLE_COUNT:
        # 按使用时间排序，清理最旧的
        all_entries = []
        for thread_id, thread_conns in _thread_connections.items():
            for cache_key, entry in thread_conns.items():
                conn, last_used = entry
                all_entries.append((thread_id, cache_key, conn, last_used))

        all_entries.sort(key=lambda x: x[3])  # 按使用时间升序
        excess_count = total_count - _CONNECTION_MAX_IDLE_COUNT

        for i in range(min(excess_count, len(all_entries))):
            thread_id, cache_key, conn, _ = all_entries[i]
            try:
                conn.close()
                logger.debug(f"关闭超限连接: {cache_key}")
            except Exception:
                pass
            if thread_id in _thread_connections:
                _thread_connections[thread_id].pop(cache_key, None)


@contextmanager
def get_db_context(db_name: str = "stockwinner",
                   path: Optional[Path] = None):
    """
    上下文管理器：自动提交/回滚，但不关闭连接（连接仍缓存）

    使用示例:
        with get_db_context("kline") as conn:
            conn.execute("INSERT INTO ...", (...))
            # 自动 commit，异常时自动 rollback
    """
    conn = get_sync_connection(db_name, path)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise


@contextmanager
def get_db_context_isolated(db_name: str = "stockwinner",
                            path: Optional[Path] = None):
    """
    上下文管理器：使用完毕后关闭连接（适用于独立脚本/一次性任务）

    使用示例:
        with get_db_context_isolated("kline") as conn:
            conn.execute("SELECT ...")
            # 自动 close
    """
    db_path = path or DATABASE_PATHS.get(db_name)
    if db_path is None:
        raise ValueError(f"未知数据库: {db_name}")

    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    _configure_connection(conn)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        try:
            conn.close()
        except Exception:
            pass


def close_all_sync_connections():
    """关闭当前线程的所有同步连接（用于清理）"""
    thread_id = threading.current_thread().ident
    with _connections_lock:
        thread_conns = _thread_connections.get(thread_id, {})
        for cache_key, entry in list(thread_conns.items()):
            conn, _ = entry
            try:
                conn.close()
            except Exception:
                pass
        if thread_id in _thread_connections:
            _thread_connections[thread_id].clear()


def close_all_sync_connections_all_threads():
    """关闭所有线程的所有同步连接（用于服务停止时清理）"""
    with _connections_lock:
        for thread_id, thread_conns in list(_thread_connections.items()):
            for cache_key, entry in list(thread_conns.items()):
                conn, _ = entry
                try:
                    conn.close()
                except Exception:
                    pass
            thread_conns.clear()
        _thread_connections.clear()
