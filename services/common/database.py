"""
数据库管理器
SQLite 异步数据库操作
"""

import aiosqlite
import asyncio
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from contextlib import asynccontextmanager
import logging

logger = logging.getLogger('Database')

DB_PATH = Path(__file__).parent.parent.parent / "data" / "stockwinner.db"


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
