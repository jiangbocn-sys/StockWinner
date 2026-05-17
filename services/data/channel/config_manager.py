"""
数据源配置管理

管理 data_source_config 和 data_source_credentials 表的读写。
"""

import json
import logging
from typing import Dict, List, Optional, Any

from services.common.database import get_db_manager
from services.common.structured_logger import get_logger

logger = get_logger("data_source_config")


async def seed_provider_configs(registry_path: str = None):
    """
    从 registry.json 初始化 data_source_config 表。
    仅插入不存在的记录（幂等）。
    """
    import os
    if registry_path is None:
        registry_path = os.path.join(
            os.path.dirname(__file__),
            "..", "providers", "registry.json"
        )
        registry_path = os.path.normpath(registry_path)

    if not os.path.exists(registry_path):
        logger.warning("seed_configs", f"registry.json 不存在: {registry_path}")
        return

    with open(registry_path, "r") as f:
        registry = json.load(f)

    db = get_db_manager()

    for p in registry.get("providers", []):
        provider_id = p["provider_id"]

        # 检查是否已存在
        existing = await db.fetchone(
            "SELECT provider_id FROM data_source_config WHERE provider_id = ?",
            (provider_id,)
        )
        if existing:
            continue

        await db.execute(
            """INSERT INTO data_source_config
               (provider_id, display_name, is_enabled, channel_priority_json,
                capabilities_json, requires_config, system_config_json)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                provider_id,
                p["display_name"],
                1 if p.get("default_enabled", False) else 0,
                json.dumps(p.get("priority", {})),
                json.dumps(p.get("capabilities", {})),
                1 if p.get("requires_config", False) else 0,
                json.dumps({}),  # system_config 初始为空
            )
        )
        logger.info("seed_config", f"初始化数据源配置: {provider_id}")


async def get_all_provider_configs() -> List[Dict[str, Any]]:
    """获取所有数据源配置"""
    db = get_db_manager()
    rows = await db.fetchall(
        "SELECT * FROM data_source_config ORDER BY provider_id"
    )
    result = []
    for r in rows:
        d = dict(r)
        for json_key in ("channel_priority_json", "capabilities_json", "system_config_json"):
            if d.get(json_key):
                try:
                    d[json_key] = json.loads(d[json_key])
                except (json.JSONDecodeError, TypeError):
                    d[json_key] = {}
        result.append(d)
    return result


async def toggle_provider(provider_id: str, is_enabled: bool):
    """启用/禁用数据源"""
    db = get_db_manager()
    await db.execute(
        "UPDATE data_source_config SET is_enabled = ?, updated_at = datetime('now') WHERE provider_id = ?",
        (1 if is_enabled else 0, provider_id)
    )


async def update_provider_config(provider_id: str, system_config: Dict[str, Any]):
    """更新数据源系统配置"""
    db = get_db_manager()
    await db.execute(
        "UPDATE data_source_config SET system_config_json = ?, updated_at = datetime('now') WHERE provider_id = ?",
        (json.dumps(system_config), provider_id)
    )


async def update_channel_priority(channel_type: str, provider_order: List[str]):
    """更新某个通道的 Provider 优先级"""
    db = get_db_manager()
    # 将优先级写入所有 provider 的 channel_priority_json
    for idx, pid in enumerate(provider_order):
        row = await db.fetchone(
            "SELECT channel_priority_json FROM data_source_config WHERE provider_id = ?",
            (pid,)
        )
        if row:
            priority = json.loads(row.get("channel_priority_json", "{}")) if row.get("channel_priority_json") else {}
            priority[channel_type] = idx + 1
            await db.execute(
                "UPDATE data_source_config SET channel_priority_json = ? WHERE provider_id = ?",
                (json.dumps(priority), pid)
            )


async def load_channel_order(channel_type: str) -> List[str]:
    """从数据库加载某个通道的 Provider 顺序（按 priority 排序）"""
    db = get_db_manager()
    rows = await db.fetchall(
        "SELECT provider_id, channel_priority_json FROM data_source_config WHERE is_enabled = 1"
    )

    provider_priorities = []
    for r in rows:
        pid = r["provider_id"]
        priority = json.loads(r["channel_priority_json"]) if r.get("channel_priority_json") else {}
        rank = priority.get(channel_type, 999)
        provider_priorities.append((pid, rank))

    provider_priorities.sort(key=lambda x: x[1])
    return [pid for pid, _ in provider_priorities]


async def add_account_role_column():
    """确保 accounts 表有 role 列"""
    db = get_db_manager()
    try:
        columns = await db.fetchall("PRAGMA table_info(accounts)")
        has_role = any(c["name"] == "role" for c in columns)
        if not has_role:
            await db.execute("ALTER TABLE accounts ADD COLUMN role TEXT DEFAULT 'user'")
            logger.info("migration", "accounts 表新增 role 列")
    except Exception as e:
        logger.warning("migration", f"添加 role 列失败 (可能已存在): {e}")


async def create_data_source_tables():
    """创建数据源相关表（如果不存在）"""
    db = get_db_manager()

    # data_source_config
    await db.execute("""
        CREATE TABLE IF NOT EXISTS data_source_config (
            provider_id TEXT PRIMARY KEY,
            display_name TEXT NOT NULL,
            is_enabled INTEGER DEFAULT 0,
            channel_priority_json TEXT,
            system_config_json TEXT,
            capabilities_json TEXT,
            requires_config INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    logger.info("migration", "data_source_config 表已就绪")
