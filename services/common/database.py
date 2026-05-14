"""
数据库管理器
- 异步：aiosqlite + DatabaseManager（FastAPI 异步端点）
- 同步：sqlite3 连接池（后台任务、定时任务、工具脚本）

重要：
- 所有模块统一通过 get_sync_connection() / get_db_context() 获取连接
- 禁止在各模块中直接使用 sqlite3.connect()，避免 WAL mode / busy_timeout 不一致
- 系统维护两个数据库：stockwinner.db（业务数据）和 kline.db（行情数据）
"""

import aiosqlite
import asyncio
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from contextlib import asynccontextmanager, contextmanager
import logging
import threading

logger = logging.getLogger('Database')

DB_PATH = Path(__file__).parent.parent.parent / "data" / "stockwinner.db"
KLINE_DB_PATH = Path(__file__).parent.parent.parent / "data" / "kline.db"


class DatabaseManager:
    """数据库管理器 — 使用连接池而非单连接，避免长时间运行后连接失效"""

    def __init__(self, db_path: Path = DB_PATH, pool_size: int = 5):
        self.db_path = db_path
        self._pool_size = pool_size
        self._pool: Optional[asyncio.Queue] = None  # 延迟初始化，避免跨事件循环问题
        self._initialized = False
        self._lock: Optional[asyncio.Lock] = None

    def _ensure_loop_resources(self):
        """确保在当前事件循环中初始化 pool 和 lock"""
        if self._pool is None or self._lock is None:
            try:
                asyncio.get_running_loop()
            except RuntimeError:
                return  # 没有运行中的事件循环
            if self._lock is None:
                self._lock = asyncio.Lock()
            if self._pool is None:
                self._pool = asyncio.Queue()

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

    def _acquire(self):
        """从池中获取一个连接（非阻塞，用于上下文）"""
        return self._pool.get()

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
        return await self._execute_with_conn(query, params, commit=True)

    async def executemany(self, query: str, params_list: List[Tuple]) -> aiosqlite.Cursor:
        """批量执行 SQL"""
        conn = await self._acquire()
        try:
            cursor = await conn.executemany(query, params_list)
            await conn.commit()
            return cursor
        finally:
            await self._release(conn)

    async def fetchone(self, query: str, params: Tuple = ()) -> Optional[Dict]:
        """查询单条记录"""
        conn = await self._acquire()
        try:
            cursor = await conn.execute(query, params)
            row = await cursor.fetchone()
            return dict(row) if row else None
        finally:
            await self._release(conn)

    async def fetchall(self, query: str, params: Tuple = ()) -> List[Dict]:
        """查询多条记录"""
        conn = await self._acquire()
        try:
            cursor = await conn.execute(query, params)
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]
        finally:
            await self._release(conn)

    async def fetchval(self, query: str, params: Tuple = ()) -> Optional[Any]:
        """查询单个值"""
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
            return cursor.rowcount
        finally:
            await self._release(conn)

    async def commit(self):
        """提交事务 — 池化模式下不直接支持，建议使用 transaction()"""
        pass

    async def rollback(self):
        """回滚事务 — 池化模式下不直接支持，建议使用 transaction()"""
        pass


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

# 每个线程独立的连接缓存：thread_id → (db_name → connection)
_thread_connections: Dict[int, Dict[str, sqlite3.Connection]] = {}
_connections_lock = threading.Lock()

# 已知数据库路径
DATABASE_PATHS = {
    "stockwinner": DB_PATH,
    "kline": KLINE_DB_PATH,
}


def _configure_connection(conn: sqlite3.Connection):
    """配置 SQLite 连接：WAL mode、busy timeout、foreign keys"""
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA busy_timeout = 10000")
    conn.execute("PRAGMA foreign_keys = ON")


def get_sync_connection(db_name: str = "stockwinner",
                        path: Optional[Path] = None) -> sqlite3.Connection:
    """
    获取预配置的 sqlite3 连接（线程缓存，自动复用）

    Args:
        db_name: 预定义数据库名称 ("stockwinner" | "kline")
        path: 自定义数据库路径（覆盖 db_name）

    Returns:
        已配置 WAL mode + busy_timeout 的 sqlite3 连接

    使用示例:
        conn = get_sync_connection("kline")
        cursor = conn.execute("SELECT ...")
        # 连接自动缓存，无需手动 close
    """
    db_path = path or DATABASE_PATHS.get(db_name)
    if db_path is None:
        raise ValueError(f"未知数据库: {db_name}，可选: {list(DATABASE_PATHS.keys())}")

    db_path.parent.mkdir(parents=True, exist_ok=True)

    thread_id = threading.current_thread().ident
    cache_key = f"{thread_id}:{db_path}"

    with _connections_lock:
        thread_conns = _thread_connections.get(thread_id)
        if thread_conns is None:
            thread_conns = {}
            _thread_connections[thread_id] = thread_conns

        conn = thread_conns.get(cache_key)
        if conn is None:
            conn = sqlite3.connect(str(db_path))
            conn.row_factory = sqlite3.Row
            _configure_connection(conn)
            thread_conns[cache_key] = conn
        else:
            # 验证连接是否仍然有效
            try:
                conn.execute("SELECT 1")
            except Exception:
                conn.close()
                conn = sqlite3.connect(str(db_path))
                conn.row_factory = sqlite3.Row
                _configure_connection(conn)
                thread_conns[cache_key] = conn

    return conn


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
        for conn in thread_conns.values():
            try:
                conn.close()
            except Exception:
                pass
        thread_conns.clear()


def close_all_sync_connections_all_threads():
    """关闭所有线程的所有同步连接（用于服务停止时清理）"""
    with _connections_lock:
        for thread_conns in _thread_connections.values():
            for conn in thread_conns.values():
                try:
                    conn.close()
                except Exception:
                    pass
        _thread_connections.clear()
