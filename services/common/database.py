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
    """数据库管理器"""

    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path
        self._connection: Optional[aiosqlite.Connection] = None
        self._lock = asyncio.Lock()

    async def connect(self):
        """连接数据库"""
        self.db_path.parent.mkdir(exist_ok=True)
        self._connection = await aiosqlite.connect(str(self.db_path))
        self._connection.row_factory = aiosqlite.Row
        await self._connection.execute("PRAGMA journal_mode = WAL")
        await self._connection.execute("PRAGMA foreign_keys = ON")
        await self._connection.execute("PRAGMA busy_timeout = 5000")

    async def close(self):
        """关闭数据库连接"""
        if self._connection:
            try:
                await self._connection.close()
            except Exception:
                pass
            self._connection = None

    async def _ensure_connection(self):
        """确保连接有效，如果损坏则重建"""
        if self._connection is not None:
            try:
                # 尝试一个轻量查询检测连接是否存活
                await self._connection.execute("SELECT 1")
                return
            except Exception:
                logger.warning("数据库连接已损坏，正在重建...")
                try:
                    await self._connection.close()
                except Exception:
                    pass
                self._connection = None

        await self.connect()

    @asynccontextmanager
    async def transaction(self):
        """事务上下文"""
        await self._ensure_connection()
        try:
            yield self._connection
            await self._connection.commit()
        except Exception as e:
            await self._connection.rollback()
            raise

    async def execute(self, query: str, params: Tuple = ()) -> aiosqlite.Cursor:
        """执行 SQL"""
        await self._ensure_connection()
        cursor = await self._connection.execute(query, params)
        await self._connection.commit()
        return cursor

    async def executemany(self, query: str, params_list: List[Tuple]) -> aiosqlite.Cursor:
        """批量执行 SQL"""
        await self._ensure_connection()
        cursor = await self._connection.executemany(query, params_list)
        await self._connection.commit()
        return cursor

    async def fetchone(self, query: str, params: Tuple = ()) -> Optional[Dict]:
        """查询单条记录"""
        await self._ensure_connection()
        cursor = await self._connection.execute(query, params)
        row = await cursor.fetchone()
        return dict(row) if row else None

    async def fetchall(self, query: str, params: Tuple = ()) -> List[Dict]:
        """查询多条记录"""
        await self._ensure_connection()
        cursor = await self._connection.execute(query, params)
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]

    async def fetchval(self, query: str, params: Tuple = ()) -> Optional[Any]:
        """查询单个值"""
        await self._ensure_connection()
        cursor = await self._connection.execute(query, params)
        row = await cursor.fetchone()
        if row:
            return row[0]
        return None

    async def insert(self, table: str, data: Dict) -> int:
        """插入记录"""
        columns = ', '.join(data.keys())
        placeholders = ', '.join(['?' for _ in data])
        query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
        cursor = await self.execute(query, tuple(data.values()))
        return cursor.lastrowid

    async def update(self, table: str, data: Dict, where: str, params: Tuple = ()) -> int:
        """更新记录"""
        set_clause = ', '.join([f"{k} = ?" for k in data.keys()])
        query = f"UPDATE {table} SET {set_clause} WHERE {where}"
        cursor = await self.execute(query, tuple(data.values()) + params)
        return cursor.rowcount

    async def delete(self, table: str, where: str, params: Tuple = ()) -> int:
        """删除记录"""
        query = f"DELETE FROM {table} WHERE {where}"
        cursor = await self.execute(query, params)
        return cursor.rowcount

    async def commit(self):
        """提交事务"""
        if self._connection:
            await self._connection.commit()

    async def rollback(self):
        """回滚事务"""
        if self._connection:
            await self._connection.rollback()


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
