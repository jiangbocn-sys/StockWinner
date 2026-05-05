"""
调度服务 API 接口

提供调度状态查询和手动触发功能
"""

from fastapi import APIRouter, Query, Path, Body, HTTPException
from typing import Dict, List, Optional, Any

router = APIRouter()


@router.get("/api/v1/ui/scheduler/status")
async def get_scheduler_status() -> Dict:
    """
    获取调度服务状态

    Returns:
        调度服务运行状态、任务列表、执行历史
    """
    from services.common.scheduler_service import get_scheduler

    scheduler = get_scheduler()
    return scheduler.get_status()


@router.post("/api/v1/ui/scheduler/start")
async def start_scheduler() -> Dict:
    """
    启动调度服务

    Returns:
        启动结果
    """
    from services.common.scheduler_service import start_scheduler

    scheduler = start_scheduler()
    return {
        'success': True,
        'message': '调度服务已启动',
        'status': scheduler.get_status()
    }


@router.post("/api/v1/ui/scheduler/stop")
async def stop_scheduler() -> Dict:
    """
    停止调度服务

    Returns:
        停止结果
    """
    from services.common.scheduler_service import stop_scheduler

    stop_scheduler()
    return {'success': True, 'message': '调度服务已停止'}


@router.post("/api/v1/ui/scheduler/kline/check")
async def manual_kline_check(full: bool = Query(False, description="是否全量下载")) -> Dict:
    """
    手动触发K线数据检查

    默认增量检查，full=true 时执行全量下载

    Returns:
        任务启动状态
    """
    from services.common.scheduler_service import get_scheduler

    scheduler = get_scheduler()
    return scheduler.run_manual_kline_check(full=full)


@router.post("/api/v1/ui/scheduler/weekly/kline")
async def manual_weekly_kline_download() -> Dict:
    """
    手动触发周K线数据下载

    Returns:
        任务启动状态
    """
    from services.common.scheduler_service import get_scheduler

    scheduler = get_scheduler()
    return scheduler.run_manual_weekly_kline_download()


@router.post("/api/v1/ui/scheduler/monthly/check")
async def manual_monthly_check() -> Dict:
    """
    手动触发月频因子更新

    检查月频因子是否需要更新，如需要则启动更新任务

    Returns:
        任务启动状态
    """
    from services.common.scheduler_service import get_scheduler

    scheduler = get_scheduler()
    return scheduler.run_manual_monthly_check()


@router.post("/api/v1/ui/scheduler/industry/download")
async def manual_industry_indices_download() -> Dict:
    """
    手动触发申万行业指数下载

    使用SDK的InfoData.get_industry_daily方法下载31个申万一级行业指数数据

    Returns:
        任务启动状态
    """
    from services.common.scheduler_service import get_scheduler

    scheduler = get_scheduler()
    return scheduler.run_manual_industry_indices_download()


@router.get("/api/v1/ui/data/status")
async def get_data_status() -> Dict:
    """
    获取各数据表最新日期

    Returns:
        kline_data、weekly_kline_data、stock_daily_factors、stock_monthly_factors 最新日期
    """
    import sqlite3
    from pathlib import Path

    db_path = Path(__file__).parent.parent.parent / "data" / "kline.db"

    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    status = {}

    # kline_data 最新日期
    cursor.execute("SELECT MAX(trade_date) FROM kline_data")
    status['kline_latest'] = cursor.fetchone()[0]

    # weekly_kline_data 最新日期
    cursor.execute("SELECT MAX(week_end_date) FROM weekly_kline_data")
    status['weekly_kline_latest'] = cursor.fetchone()[0]

    # stock_daily_factors 最新日期
    cursor.execute("SELECT MAX(trade_date) FROM stock_daily_factors")
    status['daily_factors_latest'] = cursor.fetchone()[0]

    # stock_monthly_factors 最新报告期
    cursor.execute("SELECT MAX(report_date) FROM stock_monthly_factors")
    status['monthly_factors_latest'] = cursor.fetchone()[0]

    conn.close()
    return status


# ============== 任务插件管理 ==============

@router.get("/api/v1/ui/scheduler/task-registry")
async def get_task_registry():
    """
    获取已注册的任务插件列表

    扫描 services/tasks/ 和 services/tasks/user_custom/ 目录，
    返回所有带元数据头的可调度任务。

    Returns:
        任务插件列表，按 category 分组
    """
    from services.tasks import scan_tasks
    registry = scan_tasks()
    tasks = []
    for info in registry.values():
        tasks.append({
            "module": info["module"],
            "name": info["name"],
            "description": info["description"],
            "category": info["category"],
            "source": info["source"],
            "available": info["handler"] is not None,
        })
    # 按 category 排序
    tasks.sort(key=lambda x: (x["category"], x["name"]))
    return {"success": True, "tasks": tasks}


@router.post("/api/v1/ui/scheduler/scan-tasks")
async def scan_task_registry():
    """
    手动触发任务插件扫描

    适用于在 user_custom/ 目录中新增文件后，
    不重启后端的情况下刷新注册表。

    Returns:
        最新注册的任务列表
    """
    from services.tasks import scan_tasks
    registry = scan_tasks()
    tasks = []
    for info in registry.values():
        tasks.append({
            "module": info["module"],
            "name": info["name"],
            "description": info["description"],
            "category": info["category"],
            "source": info["source"],
            "available": info["handler"] is not None,
        })
    tasks.sort(key=lambda x: (x["category"], x["name"]))
    return {"success": True, "message": f"已扫描到 {len(tasks)} 个任务", "tasks": tasks}


# ============== 策略任务管理 ==============

@router.get("/api/v1/ui/{account_id}/strategy-tasks")
async def list_strategy_tasks(account_id: str = Path(..., description="账户 ID")):
    """列出该账户下所有策略任务"""
    from services.common.database import get_db_manager
    db = get_db_manager()

    account = await db.fetchone("SELECT * FROM accounts WHERE account_id = ? AND is_active = 1", (account_id,))
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    rows = await db.fetchall("""
        SELECT t.*, s.name as strategy_name, g.name as group_name
        FROM strategy_tasks t
        LEFT JOIN strategies s ON t.strategy_id = s.id
        LEFT JOIN candidate_groups g ON t.group_id = g.id
        WHERE t.account_id = ?
        ORDER BY t.created_at DESC
    """, (account_id,))

    # 为 builtin 任务添加可读名称
    tasks = []
    for r in rows:
        task = dict(r)
        if task.get("task_type") == "builtin" and task.get("module"):
            from services.tasks import get_task
            info = get_task(task["module"])
            if info:
                task["task_name"] = info["name"]
        elif not task.get("task_name"):
            task["task_name"] = task.get("strategy_name") or "未知策略"
        tasks.append(task)

    return {"success": True, "tasks": tasks}


@router.post("/api/v1/ui/scheduler/translate-cron")
async def translate_cron(text: str = Body(..., embed=True, description="自然语言描述，如：每个交易日14:30执行")):
    """将自然语言翻译为 cron 表达式"""
    from pathlib import Path
    import json

    # 加载 LLM 配置
    config_path = Path(__file__).parent.parent.parent / "config" / "llm.json"
    if not config_path.exists():
        return {"success": False, "error": "LLM 未配置，请在设置中配置 API 密钥"}

    with open(config_path) as f:
        config = json.load(f)

    api_key = config.get("api_key", "")
    if not api_key:
        return {"success": False, "error": "LLM API 密钥未配置"}

    base_url = config.get("base_url", "")
    model = config.get("model", "")
    if not base_url or not model:
        return {"success": False, "error": "LLM 地址或模型未配置"}

    from services.llm.strategy_generator import LLM_PROVIDERS
    provider = config.get("provider", "custom")
    preset = LLM_PROVIDERS.get(provider, {})
    api_format = preset.get("format", "openai")

    # 使用 preset 的 base_url（含完整路径），仅覆盖 model 和 api_key
    base_url = preset.get("base_url", "")
    if config.get("base_url"):
        # 如果用户自定义了 base_url 但缺少路径，自动补全
        custom = config["base_url"]
        if "/chat/completions" not in custom and "/messages" not in custom:
            custom = custom.rstrip("/") + "/chat/completions"
        base_url = custom
    model = config.get("model", "")
    if not model:
        return {"success": False, "error": "LLM 模型未配置"}

    headers = {"Content-Type": "application/json"}
    auth_header = preset.get("auth_header", "Authorization")
    auth_prefix = preset.get("auth_prefix", "")
    if auth_header == "x-api-key":
        headers["x-api-key"] = api_key
    else:
        headers[auth_header] = f"{auth_prefix}{api_key}"
    if preset.get("api_version_header"):
        headers[preset["api_version_header"]] = preset.get("api_version", "")

    system_prompt = "你是一个 cron 表达式翻译助手。将用户的中文自然语言描述转换为标准 cron 表达式（5位格式: 分 时 日 月 周）。只返回 JSON，格式: {\"cron\": \"表达式\", \"description\": \"中文解释\"}。交易日用 1-5 表示周一至周五。"

    if api_format == "anthropic":
        data = {
            "model": model,
            "max_tokens": 256,
            "system": system_prompt,
            "messages": [{"role": "user", "content": f"翻译: {text}"}]
        }
    else:
        data = {
            "model": model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": f"翻译: {text}"}
            ],
            "temperature": 0.1,
            "max_tokens": 256
        }

    import urllib.request
    import urllib.error
    try:
        req = urllib.request.Request(
            base_url,
            data=json.dumps(data).encode("utf-8"),
            headers=headers,
            method="POST"
        )
        with urllib.request.urlopen(req, timeout=30) as response:
            result = json.loads(response.read().decode("utf-8"))
            if api_format == "anthropic":
                content = result["content"][0]["text"]
            else:
                content = result["choices"][0]["message"]["content"]
            content = content.strip()
            if content.startswith("```"):
                content = content.split("\n", 1)[-1].rsplit("```", 1)[0].strip()
            parsed = json.loads(content)
            return {"success": True, "cron": parsed.get("cron"), "description": parsed.get("description")}
    except Exception as e:
        return {"success": False, "error": f"LLM 调用失败: {str(e)}"}


@router.post("/api/v1/ui/scheduler/reload-tasks")
async def reload_strategy_tasks():
    """重新加载策略任务到 APScheduler，不重启后端"""
    from services.common.scheduler_service import get_scheduler
    svc = get_scheduler()
    result = svc.reload_strategy_tasks()
    return result


@router.get("/api/v1/ui/kronos/status")
async def get_kronos_status() -> Dict:
    """获取 Kronos 模型加载状态"""
    from services.common.kronos_service import get_kronos_service
    svc = get_kronos_service()
    return {
        "success": True,
        "available": svc.is_available,
        "error": svc.error,
        "device": getattr(svc, "_device", "unknown") if svc.is_available else None,
    }


@router.post("/api/v1/ui/{account_id}/strategy-tasks")
async def create_strategy_task(
    account_id: str = Path(..., description="账户 ID"),
    task_type: str = Body("strategy", description="任务类型: builtin / strategy"),
    module: Optional[str] = Body(None, description="内置任务模块名（builtin 类型必填）"),
    strategy_id: Optional[int] = Body(None, description="策略 ID（strategy 类型必填）"),
    group_id: Optional[int] = Body(None, description="候选组 ID"),
    cron_expression: str = Body(..., description="Cron 表达式"),
    enabled: int = Body(1, description="是否启用"),
):
    """创建调度任务"""
    from services.common.database import get_db_manager
    from services.tasks import get_task
    db = get_db_manager()

    account = await db.fetchone("SELECT * FROM accounts WHERE account_id = ? AND is_active = 1", (account_id,))
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    if task_type == "builtin":
        # 验证内置任务存在
        if not module:
            raise HTTPException(status_code=400, detail="builtin 类型任务需指定 module")
        task_info = get_task(module)
        if not task_info or task_info.get("handler") is None:
            raise HTTPException(status_code=404, detail=f"任务模块不存在或加载失败: {module}")
        task_name = task_info["name"]
    elif task_type == "strategy":
        # 验证策略存在
        if not strategy_id:
            raise HTTPException(status_code=400, detail="strategy 类型任务需指定 strategy_id")
        strategy = await db.fetchone("SELECT id, name FROM strategies WHERE id = ?", (strategy_id,))
        if not strategy:
            raise HTTPException(status_code=404, detail="策略不存在")
        task_name = strategy["name"]
        module = None
    else:
        raise HTTPException(status_code=400, detail=f"不支持的任务类型: {task_type}")

    if group_id:
        group = await db.fetchone("SELECT id FROM candidate_groups WHERE id = ? AND account_id = ?", (group_id, account_id))
        if not group:
            raise HTTPException(status_code=404, detail="候选组不存在")

    task_id = await db.insert("strategy_tasks", {
        "task_type": task_type,
        "module": module,
        "strategy_id": strategy_id if task_type == "strategy" else None,
        "group_id": group_id,
        "account_id": account_id,
        "cron_expression": cron_expression,
        "enabled": enabled,
    })

    from services.common.scheduler_service import get_scheduler
    get_scheduler().reload_strategy_tasks()
    return {"success": True, "message": f"任务已创建: {task_name}", "task_id": task_id}


@router.put("/api/v1/ui/{account_id}/strategy-tasks/{task_id}")
async def update_strategy_task(
    account_id: str = Path(..., description="账户 ID"),
    task_id: int = Path(..., description="任务 ID"),
    cron_expression: Optional[str] = Body(None, description="Cron 表达式"),
    enabled: Optional[int] = Body(None, description="是否启用"),
):
    """更新策略任务"""
    from services.common.database import get_db_manager
    db = get_db_manager()

    account = await db.fetchone("SELECT * FROM accounts WHERE account_id = ? AND is_active = 1", (account_id,))
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    task = await db.fetchone("SELECT * FROM strategy_tasks WHERE id = ? AND account_id = ?", (task_id, account_id))
    if not task:
        raise HTTPException(status_code=404, detail="任务不存在")

    update_data = {"updated_at": __import__("services.common.timezone", fromlist=["get_china_time"]).get_china_time().isoformat()}
    if cron_expression is not None:
        update_data["cron_expression"] = cron_expression
    if enabled is not None:
        update_data["enabled"] = enabled

    if len(update_data) > 1:
        await db.update("strategy_tasks", update_data, "id = ?", (task_id,))
        from services.common.scheduler_service import get_scheduler
        get_scheduler().reload_strategy_tasks()

    return {"success": True, "message": "任务已更新"}


@router.delete("/api/v1/ui/{account_id}/strategy-tasks/{task_id}")
async def delete_strategy_task(
    account_id: str = Path(..., description="账户 ID"),
    task_id: int = Path(..., description="任务 ID"),
):
    """删除策略任务"""
    from services.common.database import get_db_manager
    db = get_db_manager()

    account = await db.fetchone("SELECT * FROM accounts WHERE account_id = ? AND is_active = 1", (account_id,))
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    task = await db.fetchone("SELECT id FROM strategy_tasks WHERE id = ? AND account_id = ?", (task_id, account_id))
    if not task:
        raise HTTPException(status_code=404, detail="任务不存在")

    await db.delete("strategy_tasks", "id = ?", (task_id,))
    from services.common.scheduler_service import get_scheduler
    get_scheduler().reload_strategy_tasks()
    return {"success": True, "message": "任务已删除"}


@router.post("/api/v1/ui/{account_id}/strategy-tasks/{task_id}/run")
async def run_strategy_task_manual(
    account_id: str = Path(..., description="账户 ID"),
    task_id: int = Path(..., description="任务 ID"),
):
    """手动执行一次策略任务"""
    from services.common.scheduler_service import get_scheduler
    from services.common.database import get_db_manager
    db = get_db_manager()

    account = await db.fetchone("SELECT * FROM accounts WHERE account_id = ? AND is_active = 1", (account_id,))
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    task = await db.fetchone("SELECT id FROM strategy_tasks WHERE id = ? AND account_id = ?", (task_id, account_id))
    if not task:
        raise HTTPException(status_code=404, detail="任务不存在")

    scheduler = get_scheduler()
    return scheduler.run_manual_strategy_task(task_id)