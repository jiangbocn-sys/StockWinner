"""
策略版本存档服务

每次策略创建、更新、删除时自动写入版本快照到 strategy_versions 表。
用于策略演化追踪、数据分析、以及误删恢复。
不面向用户展示，仅系统内部存档使用。
"""

from typing import Dict, Optional, List, Any
from services.common.database import get_db_manager
from services.common.timezone import format_china_time


def compute_diff_summary(old: Dict[str, Any], new: Dict[str, Any]) -> str:
    """计算两个策略快照之间的差异摘要"""
    if not old and not new:
        return ""
    if not old:
        return "新建策略"
    if not new:
        return "策略已删除"

    tracked_fields = [
        "name", "description", "strategy_type", "config", "code",
        "code_type", "code_scope", "function_name", "target_scope",
        "status", "match_score_threshold", "buy_strategy_id", "sell_strategy_id",
    ]
    changes = []
    for field in tracked_fields:
        old_val = old.get(field)
        new_val = new.get(field)
        if old_val != new_val:
            if field == "code":
                # 代码变化，只记录行数差异
                old_lines = len((old_val or "").splitlines())
                new_lines = len((new_val or "").splitlines())
                changes.append(f"code: {old_lines}行→{new_lines}行")
            elif field == "config":
                changes.append("config: 已修改")
            else:
                changes.append(f"{field}: {repr(old_val)}→{repr(new_val)}")
    return "; ".join(changes) if changes else "无变化"


async def archive_strategy_version(
    account_id: str,
    strategy_id: int,
    action: str,
    strategy_data: Dict[str, Any],
    diff_summary: str = "",
) -> int:
    """
    写入一条策略版本记录

    Args:
        account_id: 账户 ID
        strategy_id: 策略 ID
        action: 'created' / 'updated' / 'deleted'
        strategy_data: 策略各字段（来自 strategies 表）
        diff_summary: 变更摘要

    Returns:
        新创建的 version 记录 ID
    """
    db = get_db_manager()

    # 计算版本号
    row = await db.fetchone(
        "SELECT COALESCE(MAX(version), 0) as max_v FROM strategy_versions WHERE strategy_id = ?",
        (strategy_id,),
    )
    version = (row["max_v"] if row else 0) + 1

    # 提取所有需要存档的字段
    record = {
        "strategy_id": strategy_id,
        "account_id": account_id,
        "version": version,
        "action": action,
        "name": strategy_data.get("name"),
        "description": strategy_data.get("description"),
        "strategy_type": strategy_data.get("strategy_type"),
        "config": strategy_data.get("config"),
        "code": strategy_data.get("code"),
        "code_type": strategy_data.get("code_type"),
        "code_scope": strategy_data.get("code_scope"),
        "function_name": strategy_data.get("function_name"),
        "target_scope": strategy_data.get("target_scope"),
        "status": strategy_data.get("status"),
        "match_score_threshold": strategy_data.get("match_score_threshold"),
        "buy_strategy_id": strategy_data.get("buy_strategy_id"),
        "sell_strategy_id": strategy_data.get("sell_strategy_id"),
        "diff_summary": diff_summary,
        "created_at": format_china_time(),
    }

    version_id = await db.insert("strategy_versions", record)
    return version_id


async def archive_before_delete(
    account_id: str,
    strategy_id: int,
) -> Optional[int]:
    """删除策略前，自动存档当前版本"""
    db = get_db_manager()
    old = await db.fetchone(
        "SELECT * FROM strategies WHERE id = ? AND account_id = ?",
        (strategy_id, account_id),
    )
    if not old:
        return None
    return await archive_strategy_version(
        account_id, strategy_id, "deleted", dict(old),
        diff_summary="策略已删除"
    )


async def get_strategy_versions(
    account_id: str,
    strategy_id: int,
) -> List[Dict[str, Any]]:
    """查询某策略的版本历史（按版本号升序）"""
    db = get_db_manager()
    rows = await db.fetchall(
        """SELECT * FROM strategy_versions
           WHERE strategy_id = ? AND account_id = ?
           ORDER BY version ASC""",
        (strategy_id, account_id),
    )
    return [dict(r) for r in rows]


async def restore_strategy_from_version(
    account_id: str,
    strategy_id: int,
    version: int,
) -> Optional[Dict[str, Any]]:
    """
    从指定版本恢复策略数据到 strategies 表。
    返回恢复后的策略数据。
    """
    db = get_db_manager()

    # 验证策略属于该账户
    strategy = await db.fetchone(
        "SELECT id FROM strategies WHERE id = ? AND account_id = ?",
        (strategy_id, account_id),
    )
    if not strategy:
        return None

    # 读取版本快照
    ver = await db.fetchone(
        "SELECT * FROM strategy_versions WHERE strategy_id = ? AND account_id = ? AND version = ?",
        (strategy_id, account_id, version),
    )
    if not ver:
        return None

    # 恢复字段（不恢复 action/version/diff_summary/created_at/strategy_id/account_id）
    restore_fields = [
        "name", "description", "strategy_type", "config", "code",
        "code_type", "code_scope", "function_name", "target_scope",
        "status", "match_score_threshold", "buy_strategy_id", "sell_strategy_id",
    ]
    update_data = {"updated_at": format_china_time()}
    for field in restore_fields:
        if field in ver:
            update_data[field] = ver[field]

    await db.update("strategies", update_data, "id = ?", (strategy_id,))

    # 同时写入一条 'restored' 类型的版本记录
    restored_data = {k: ver.get(k) for k in restore_fields}
    await archive_strategy_version(
        account_id, strategy_id, "restored",
        restored_data,
        diff_summary=f"从版本 #{version} 恢复"
    )

    return dict(ver)
