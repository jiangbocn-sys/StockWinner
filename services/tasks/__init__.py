"""
调度任务插件注册系统

类似 agent skills 的管理方式：
- 代码文件放入指定目录 → 系统扫描 → 读取文件头元数据 → 注册为可选任务
- 内置任务: services/tasks/*.py
- 用户自定义: services/tasks/user_custom/*.py

文件头元数据格式（YAML-like）：
    # ---
    # name: K线增量检查
    # description: 检查kline_data表最新日期，如缺失则下载当日数据
    # category: 数据下载
    # ---
"""

import re
from pathlib import Path
from typing import Dict, List, Optional, Callable, Any

TASKS_DIR = Path(__file__).parent
USER_CUSTOM_DIR = TASKS_DIR / "user_custom"

# 内存缓存：{module_name: {name, description, category, handler, source}}
_registry: Dict[str, Dict] = {}


def parse_metadata(filepath: Path) -> Optional[Dict[str, str]]:
    """解析文件头部的元数据块"""
    content = filepath.read_text(encoding="utf-8")
    # 匹配 --- ... --- 块
    match = re.search(r"# ---\s*\n(.*?)# ---", content, re.DOTALL)
    if not match:
        return None

    metadata = {}
    for line in match.group(1).split("\n"):
        line = line.lstrip("#").strip()
        if not line:
            continue
        if ":" in line:
            key, _, value = line.partition(":")
            metadata[key.strip()] = value.strip()

    if "name" not in metadata:
        return None
    return metadata


def register_task(
    module_name: str,
    name: str,
    description: str,
    handler: Callable,
    category: str = "其他",
    source: str = "builtin",
):
    """注册一个任务到全局注册表"""
    _registry[module_name] = {
        "module": module_name,
        "name": name,
        "description": description,
        "category": category,
        "handler": handler,
        "source": source,
    }


def unregister_task(module_name: str):
    """从注册表移除"""
    _registry.pop(module_name, None)


def get_registry() -> Dict[str, Dict]:
    """获取当前注册表"""
    return _registry.copy()


def get_task(module_name: str) -> Optional[Dict]:
    """获取指定任务"""
    return _registry.get(module_name)


def scan_tasks() -> Dict[str, Dict]:
    """扫描内置和用户自定义任务目录，更新注册表"""
    _registry.clear()

    # 扫描内置任务
    _scan_dir(TASKS_DIR, source="builtin")

    # 扫描用户自定义任务
    if USER_CUSTOM_DIR.exists():
        _scan_dir(USER_CUSTOM_DIR, source="user")

    return _registry.copy()


def _scan_dir(directory: Path, source: str):
    """扫描目录下所有 .py 文件（非 __init__.py），解析元数据并注册"""
    for filepath in sorted(directory.glob("*.py")):
        if filepath.name.startswith("_"):
            continue

        metadata = parse_metadata(filepath)
        if not metadata:
            continue

        module_name = filepath.stem

        # 动态导入模块以获取 handler
        try:
            import importlib.util
            spec = importlib.util.spec_from_file_location(
                f"tasks.{source}.{module_name}",
                filepath
            )
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)

            # 查找 execute 函数
            handler = getattr(module, "execute", None)
            if handler is None:
                continue

            register_task(
                module_name=module_name,
                name=metadata["name"],
                description=metadata.get("description", ""),
                handler=handler,
                category=metadata.get("category", "其他"),
                source=source,
            )
        except Exception as e:
            # 导入失败，只注册元数据（无 handler）
            register_task(
                module_name=module_name,
                name=metadata["name"],
                description=metadata.get("description", ""),
                handler=None,
                category=metadata.get("category", "其他"),
                source=source,
            )
