"""
调度服务 API 接口

提供调度状态查询和手动触发功能
"""

from fastapi import APIRouter, Query, Path, Body, HTTPException, BackgroundTasks
from typing import Dict, List, Optional, Any
from services.common.timezone import format_china_time
from services.auth.account_validator import validate_account_active

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
    from services.common.database import get_sync_connection

    db_path = Path(__file__).parent.parent.parent / "data" / "kline.db"

    conn = get_sync_connection("kline")
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

    await validate_account_active(account_id)

    rows = await db.fetchall("""
        SELECT t.*, s.name as strategy_name, g.name as group_name
        FROM strategy_tasks t
        LEFT JOIN strategies s ON t.strategy_id = s.id
        LEFT JOIN candidate_groups g ON t.group_id = g.id
        WHERE t.account_id = ? OR t.account_id = 'SYSTEM'
        ORDER BY t.created_at DESC
    """, (account_id,))

    # 为 builtin 任务添加可读名称 + cron 可读描述 + 实时运行状态
    # builtin 任务模块名 → task_manager 任务类型映射
    MODULE_TO_TASK_TYPE = {
        "kline_check": "data_download",
        "weekly_kline": "weekly_kline_download",
        "monthly_factors": "monthly_factor_update",
    }

    # 获取 task_manager 实时状态
    try:
        from services.common.task_manager import get_task_manager
        tm = get_task_manager()
        tm_status = tm.get_all_status()
    except Exception:
        tm_status = {}

    tasks = []
    for r in rows:
        task = dict(r)
        if task.get("task_type") == "builtin" and task.get("module"):
            from services.tasks import get_task
            info = get_task(task["module"])
            if info:
                task["task_name"] = info["name"]
            # 注入实时运行状态
            task_type_key = MODULE_TO_TASK_TYPE.get(task["module"])
            if task_type_key and task_type_key in tm_status:
                real_status = tm_status[task_type_key]
                if real_status.get("status") == "running":
                    task["realtime_status"] = "running"
                    task["realtime_progress"] = real_status.get("progress", {})
        elif not task.get("task_name"):
            task["task_name"] = task.get("strategy_name") or "未知策略"
        # cron 可读描述
        task["cron_description"] = _describe_cron(task.get("cron_expression", ""))
        # 注入下次执行时间（从 APScheduler 获取）
        job_id = f'task_{task["id"]}'
        try:
            job = get_scheduler()._scheduler.get_job(job_id)
            if job and job.next_run_time:
                task["next_run_time"] = job.next_run_time.isoformat()
        except Exception:
            pass
        tasks.append(task)

    # 按下次执行时间排序（最近执行的排前面），无下次运行时间的排最后
    from services.common.timezone import CHINA_TZ
    from datetime import datetime, timezone
    far_future = datetime(9999, 12, 31, 23, 59, 59, tzinfo=timezone.utc)
    def _sort_key(t):
        nrt = t.get("next_run_time")
        if nrt:
            try:
                return datetime.fromisoformat(nrt)
            except Exception:
                pass
        return far_future
    tasks.sort(key=_sort_key)

    return {"success": True, "tasks": tasks}


@router.get("/api/v1/ui/{account_id}/system-tasks")
async def list_system_tasks(account_id: str = Path(..., description="账户 ID")):
    """列出所有内置系统任务（仅管理员可见）"""
    from services.auth.account_validator import require_admin_role
    from services.common.database import get_db_manager
    from services.tasks import get_task

    await require_admin_role(account_id)

    db = get_db_manager()
    rows = await db.fetchall("""
        SELECT * FROM strategy_tasks
        WHERE task_type = 'builtin' AND account_id = 'SYSTEM'
        ORDER BY id
    """, ())
    tasks = []
    for r in rows:
        task = dict(r)
        if task.get("module"):
            info = get_task(task["module"])
            if info:
                task["task_name"] = info["name"]
        task["cron_description"] = _describe_cron(task.get("cron_expression", ""))
        tasks.append(task)
    return {"success": True, "tasks": tasks}


@router.get("/api/v1/ui/{account_id}/strategy-tasks-only")
async def list_strategy_tasks_only(account_id: str = Path(..., description="账户 ID")):
    """列出该账户的策略任务（不含系统任务）"""
    from services.auth.account_validator import validate_account_active
    from services.common.database import get_db_manager

    await validate_account_active(account_id)

    db = get_db_manager()
    rows = await db.fetchall("""
        SELECT t.*, s.name as strategy_name, g.name as group_name
        FROM strategy_tasks t
        LEFT JOIN strategies s ON t.strategy_id = s.id
        LEFT JOIN candidate_groups g ON t.group_id = g.id
        WHERE t.task_type = 'strategy' AND t.account_id = ?
        ORDER BY t.created_at DESC
    """, (account_id,))

    tasks = []
    for r in rows:
        task = dict(r)
        task["task_name"] = task.get("strategy_name") or "未知策略"
        task["cron_description"] = _describe_cron(task.get("cron_expression", ""))
        tasks.append(task)

    return {"success": True, "tasks": tasks}


def _describe_cron(cron: str) -> str:
    """将标准 cron 表达式翻译为中文可读描述"""
    if not cron:
        return ""
    parts = cron.split()
    if len(parts) != 5:
        # 非标准格式（如 3 字段的错误 cron），直接返回原文
        return cron
    minute, hour, day, month, dow = parts

    # 常用 cron 表达式直接匹配
    common = {
        "0 1 * * *": "每天 01:00",
        "0 1 5 * *": "每月 5 日 01:00",
        "0 2 * * 6": "每周六 02:00",
        "0 3 * * 1-5": "每周一至周五 03:00",
        "0 3 * * mon-fri": "每周一至周五 03:00",
        "0 14 * * *": "每天 14:00",
        "0 14 * * 1-5": "每周一至周五 14:00",
        "0 14 * * mon-fri": "每周一至周五 14:00",
        "0 10 * * 1-5": "每周一至周五 10:00",
        "0 10 * * mon-fri": "每周一至周五 10:00",
        "0 * * * *": "每小时",
        "30 14 * * 1-5": "每周一至周五 14:30",
        "30 14 * * mon-fri": "每周一至周五 14:30",
        "0 0 * * *": "每天 00:00",
        "0 9 * * *": "每天 09:00",
        "0 15 * * *": "每天 15:00",
        "35 14 * * mon-fri": "每周一至周五 14:35",
    }
    if cron in common:
        return common[cron]

    # 通用解析
    desc_parts = []

    # 解析时间字段（支持多值如 "14,15"）
    def parse_time_field(field: str) -> str:
        """解析时间字段，支持逗号分隔的多值"""
        if "," in field:
            values = [int(v.strip()) for v in field.split(",")]
            return ",".join([f"{v:02d}" for v in values])
        elif field == "*":
            return "每小时"
        else:
            try:
                return f"{int(field):02d}"
            except ValueError:
                return field

    # 构建时间描述
    hour_str = parse_time_field(hour)
    if minute == "0":
        if hour == "*":
            time_str = "每小时整点"
        elif "," in hour:
            time_str = f"{hour_str}:00"
        else:
            time_str = f"{hour_str}:00"
    elif minute == "30":
        if "," in hour:
            time_str = f"{hour_str}:30"
        else:
            time_str = f"{hour_str}:30"
    elif minute == "*":
        time_str = f"{hour_str} 每分钟"
    else:
        try:
            min_str = f"{int(minute):02d}"
            if "," in hour:
                time_str = f"{hour_str}:{min_str}"
            else:
                time_str = f"{hour_str}:{min_str}"
        except ValueError:
            time_str = f"{hour}:{minute}"

    # 频率
    if day == "*" and month == "*" and dow == "*":
        desc_parts.append(f"每天 {time_str}")
    elif day == "*" and month == "*" and dow != "*":
        dow_days = {"1": "周一", "2": "周二", "3": "周三", "4": "周四", "5": "周五", "6": "周六", "0": "周日", "7": "周日",
                    "mon": "周一", "tue": "周二", "wed": "周三", "thu": "周四", "fri": "周五", "sat": "周六", "sun": "周日"}
        if "-" in dow:
            start, end = dow.split("-")
            start_name = dow_days.get(start, start)
            end_name = dow_days.get(end, end)
            desc_parts.append(f"每周{start_name}至{end_name} {time_str}")
        elif "," in dow:
            dow_str = "、".join([dow_days.get(d.strip(), d.strip()) for d in dow.split(",")])
            desc_parts.append(f"{dow_str} {time_str}")
        else:
            day_name = dow_days.get(dow, dow)
            desc_parts.append(f"每周{day_name} {time_str}")
    elif day != "*" and month == "*" and dow == "*":
        try:
            desc_parts.append(f"每月 {int(day)} 日 {time_str}")
        except ValueError:
            desc_parts.append(f"每月 {day} 日 {time_str}")
    else:
        desc_parts.append(f"{time_str} (cron: {cron})")

    return " ".join(desc_parts)


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

    system_prompt = "你是一个 cron 表达式翻译助手。将用户的中文自然语言描述转换为标准 cron 表达式（5位格式: 分 时 日 月 周）。只返回 JSON，格式: {\"cron\": \"表达式\", \"description\": \"中文解释\"}。注意：day_of_week 字段必须使用命名格式 mon-fri（周一至周五），禁止使用数字 1-5 或 0-4（APScheduler CronTrigger.from_crontab() 中 0=周一，数字格式会导致日期偏移）。"

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
    group_id: Optional[int] = Body(None, description="候选组 ID（筛选股票池）"),
    cron_expression: str = Body(..., description="Cron 表达式或自然语言描述"),
    enabled: int = Body(1, description="是否启用"),
    require_trading_day: int = Body(0, description="是否仅交易日执行"),
    full_market: int = Body(0, description="全市场模式（非交易时段全A股扫描）"),
    signal_action: str = Body("trade", description="信号处理方式: trade=直接交易, watch=继续观察"),
    target_group_id: Optional[int] = Body(None, description="信号输出目标分组（watch 模式可用，不填则写入源分组）"),
):
    """创建调度任务"""
    from services.common.database import get_db_manager
    from services.tasks import get_task
    import re
    db = get_db_manager()

    await validate_account_active(account_id)

    # 自动翻译中文 cron → 标准 cron
    cron_re = re.compile(r'^[0-9*,/\-]+$')
    if not cron_re.match(cron_expression):
        # 看起来不是标准 cron，尝试翻译
        try:
            config_path = Path(__file__).parent.parent.parent / "config" / "llm.json"
            if config_path.exists():
                with open(config_path) as f:
                    cfg = json.load(f)
                if cfg.get("api_key") and cfg.get("base_url") and cfg.get("model"):
                    from services.llm.strategy_generator import LLM_PROVIDERS
                    provider = cfg.get("provider", "custom")
                    preset = LLM_PROVIDERS.get(provider, {})
                    api_format = preset.get("format", "openai")
                    custom_url = cfg.get("base_url", "")
                    if custom_url and "/chat/completions" not in custom_url and "/messages" not in custom_url:
                        custom_url = custom_url.rstrip("/") + "/chat/completions"
                    auth_header = preset.get("auth_header", "Authorization")
                    headers = {"Content-Type": "application/json"}
                    if auth_header == "x-api-key":
                        headers["x-api-key"] = cfg["api_key"]
                    else:
                        headers[auth_header] = f"{preset.get('auth_prefix', '')}{cfg['api_key']}"
                    if preset.get("api_version_header"):
                        headers[preset["api_version_header"]] = preset.get("api_version", "")
                    system_prompt = "你是一个 cron 表达式翻译助手。将用户的中文自然语言描述转换为标准 cron 表达式（5位格式: 分 时 日 月 周）。只返回 JSON 格式: {\"cron\": \"表达式\", \"description\": \"中文解释\"}。注意：day_of_week 字段必须使用命名格式 mon-fri（周一至周五），禁止使用数字 1-5 或 0-4（APScheduler CronTrigger.from_crontab() 中 0=周一，数字格式会导致日期偏移）。"
                    data = {"model": cfg["model"], "messages": [{"role": "system", "content": system_prompt}, {"role": "user", "content": f"翻译: {cron_expression}"}], "temperature": 0.1, "max_tokens": 256} if api_format != "anthropic" else {"model": cfg["model"], "max_tokens": 256, "system": system_prompt, "messages": [{"role": "user", "content": f"翻译: {cron_expression}"}]}
                    import urllib.request
                    req = urllib.request.Request(custom_url, data=json.dumps(data).encode("utf-8"), headers=headers, method="POST")
                    with urllib.request.urlopen(req, timeout=15) as resp:
                        llm_result = json.loads(resp.read().decode("utf-8"))
                        if api_format == "anthropic":
                            content = llm_result["content"][0]["text"]
                        else:
                            content = llm_result["choices"][0]["message"]["content"]
                        content = content.strip()
                        if content.startswith("```"):
                            content = content.split("\n", 1)[-1].rsplit("```", 1)[0].strip()
                        parsed = json.loads(content)
                        cron_expression = parsed.get("cron", cron_expression)
        except Exception:
            pass  # 翻译失败，保留原文

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

    if target_group_id:
        tg = await db.fetchone("SELECT id FROM candidate_groups WHERE id = ? AND account_id = ?", (target_group_id, account_id))
        if not tg:
            raise HTTPException(status_code=404, detail="目标分组不存在")
        if target_group_id == group_id:
            raise HTTPException(status_code=400, detail="目标分组不能与源分组相同")

    task_id = await db.insert("strategy_tasks", {
        "task_type": task_type,
        "module": module,
        "strategy_id": strategy_id if task_type == "strategy" else None,
        "group_id": group_id,
        "account_id": account_id,
        "cron_expression": cron_expression,
        "enabled": enabled,
        "require_trading_day": require_trading_day,
        "full_market": full_market,
        "signal_action": signal_action,
        "target_group_id": target_group_id,
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
    require_trading_day: Optional[int] = Body(None, description="是否仅交易日执行"),
    full_market: Optional[int] = Body(None, description="全市场模式"),
    strategy_id: Optional[int] = Body(None, description="关联策略 ID"),
    group_id: Optional[int] = Body(None, description="候选分组 ID"),
    signal_action: Optional[str] = Body(None, description="信号处理方式: trade / watch"),
    target_group_id: Optional[int] = Body(None, description="信号输出目标分组 ID"),
):
    """更新策略任务"""
    from services.common.database import get_db_manager
    db = get_db_manager()

    await validate_account_active(account_id)

    task = await db.fetchone("SELECT * FROM strategy_tasks WHERE id = ? AND (account_id = ? OR account_id = 'SYSTEM')", (task_id, account_id))
    if not task:
        raise HTTPException(status_code=404, detail="任务不存在")

    update_data = {"updated_at": format_china_time()}
    if cron_expression is not None:
        update_data["cron_expression"] = cron_expression
    if enabled is not None:
        update_data["enabled"] = enabled
    if require_trading_day is not None:
        update_data["require_trading_day"] = require_trading_day
    if full_market is not None:
        update_data["full_market"] = full_market
    if strategy_id is not None:
        update_data["strategy_id"] = strategy_id
    if group_id is not None:
        update_data["group_id"] = group_id
    if signal_action is not None:
        update_data["signal_action"] = signal_action
    if target_group_id is not None:
        update_data["target_group_id"] = target_group_id

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

    await validate_account_active(account_id)

    task = await db.fetchone("SELECT id FROM strategy_tasks WHERE id = ? AND (account_id = ? OR account_id = 'SYSTEM')", (task_id, account_id))
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
    background_tasks: BackgroundTasks = None,
):
    """手动执行一次策略任务 — 后台执行，立即返回"""
    from services.common.scheduler_service import get_scheduler
    from services.common.database import get_db_manager
    db = get_db_manager()

    await validate_account_active(account_id)

    task = await db.fetchone("SELECT id FROM strategy_tasks WHERE id = ? AND (account_id = ? OR account_id = 'SYSTEM')", (task_id, account_id))
    if not task:
        raise HTTPException(status_code=404, detail="任务不存在")

    scheduler = get_scheduler()
    if background_tasks:
        background_tasks.add_task(scheduler.run_manual_strategy_task, task_id)
    else:
        scheduler.run_manual_strategy_task(task_id)
    return {'success': True, 'message': f'策略任务 {task_id} 已启动'}