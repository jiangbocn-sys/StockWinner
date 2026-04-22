"""
数据库管理器
SQLite 异步数据库操作
"""

import aiosqlite
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from contextlib import asynccontextmanager

DB_PATH = Path(__file__).parent.parent.parent / "data" / "stockwinner.db"


class DatabaseManager:
    """数据库管理器"""

    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path
        self._connection: Optional[aiosqlite.Connection] = None

    async def connect(self):
        """连接数据库"""
        self.db_path.parent.mkdir(exist_ok=True)
        self._connection = await aiosqlite.connect(str(self.db_path))
        self._connection.row_factory = aiosqlite.Row
        # 启用 WAL 模式提升并发性能
        await self._connection.execute("PRAGMA journal_mode = WAL")
        await self._connection.execute("PRAGMA foreign_keys = ON")

    async def close(self):
        """关闭数据库连接"""
        if self._connection:
            await self._connection.close()
            self._connection = None

    @asynccontextmanager
    async def transaction(self):
        """事务上下文"""
        if not self._connection:
            await self.connect()
        try:
            yield self._connection
            await self._connection.commit()
        except Exception as e:
            await self._connection.rollback()
            raise

    async def execute(self, query: str, params: Tuple = ()) -> aiosqlite.Cursor:
        """执行 SQL"""
        if not self._connection:
            await self.connect()
        cursor = await self._connection.execute(query, params)
        await self._connection.commit()
        return cursor

    async def fetchone(self, query: str, params: Tuple = ()) -> Optional[Dict]:
        """查询单条记录"""
        if not self._connection:
            await self.connect()
        cursor = await self._connection.execute(query, params)
        row = await cursor.fetchone()
        return dict(row) if row else None

    async def fetchall(self, query: str, params: Tuple = ()) -> List[Dict]:
        """查询多条记录"""
        if not self._connection:
            await self.connect()
        cursor = await self._connection.execute(query, params)
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]

    async def fetchval(self, query: str, params: Tuple = ()) -> Optional[Any]:
        """查询单个值"""
        if not self._connection:
            await self.connect()
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
    """重置数据库管理器（用于测试）"""
    global _db_manager
    _db_manager = None
