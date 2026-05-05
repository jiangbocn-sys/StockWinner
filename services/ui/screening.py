"""
选股和 Watchlist 管理 API
"""

from fastapi import APIRouter, HTTPException, Path, Body, Query
from typing import List, Optional, Dict, Any
from fastapi.background import BackgroundTasks
import os
from services.common.database import get_db_manager
from services.screening.service import get_screening_service
from services.common.timezone import get_china_time

router = APIRouter()


# ============== 本地数据下载 ==============

@router.post("/api/v1/ui/{account_id}/data/download")
async def download_kline_data(
    account_id: str = Path(..., description="账户 ID"),
    years: Optional[float] = Body(None, description="下载的年数（默认 2 年）", ge=0.5, le=20),
    start_date: Optional[str] = Body(None, description="开始日期（YYYY-MM-DD）"),
    end_date: Optional[str] = Body(None, description="结束日期（YYYY-MM-DD）"),
    batch_size: int = Body(20, description="每批次下载的股票数量", ge=10, le=50),
    market_filter: Optional[List[str]] = Body(None, description="市场筛选：['SH'] 上证 A 股，['SZ'] 深证 A 股，['BJ'] 北交 A 股，默认全部下载"),
    background_tasks: BackgroundTasks = None
):
    """下载全市场 K 线数据到本地数据库"""
    db = get_db_manager()

    # 验证账户
    account = await db.fetchone("SELECT * FROM accounts WHERE account_id = ? AND is_active = 1", (account_id,))
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    # 获取账户的 broker credentials
    broker_account = account.get("broker_account", "")
    broker_password = account.get("broker_password", "")

    # 预检查 SDK 是否可用
    try:
        from services.trading.gateway import create_gateway
        # 使用账户的 broker credentials 创建网关
        gateway = create_gateway(galaxy_app_id=broker_account, galaxy_password=broker_password)
        if not gateway or not hasattr(gateway, 'sdk_available') or not gateway.sdk_available:
            return {
                "success": False,
                "message": "交易网关 SDK 不可用，无法下载数据。请检查账户的券商配置是否正确。",
                "gateway_status": "disconnected",
                "hint": "请在账户管理中配置有效的银河证券资金账号和密码"
            }
    except Exception as e:
        return {
            "success": False,
            "message": f"交易网关初始化失败：{str(e)}",
            "gateway_status": "error"
        }

    # 导入下载函数
    from services.data.local_data_service import download_all_kline_data, get_local_data_service, download_all_kline_data_sync

    # 检查本地数据服务
    local_service = get_local_data_service()
    stats = local_service.get_download_stats()

    # 解析日期范围参数
    if start_date and end_date:
        # 使用自定义日期范围
        try:
            from datetime import datetime
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
            if start_dt > end_dt:
                raise HTTPException(status_code=400, detail="开始日期不能晚于结束日期")
            # 计算月数（用于增量下载逻辑）
            months = int((end_dt - start_dt).days / 30)
            months = max(1, months)  # 至少 1 个月
        except ValueError as e:
            raise HTTPException(status_code=400, detail=f"日期格式错误：{e}")
    elif years is not None:
        # 使用年数（预设范围）
        months = int(years * 12)
    else:
        # 默认 2 年
        months = 24

    # 使用 BackgroundTasks 执行下载任务
    if background_tasks:
        background_tasks.add_task(
            download_all_kline_data_sync,
            batch_size,
            months,
            start_date,
            end_date,
            broker_account,
            broker_password,
            True,  # calculate_factors
            market_filter  # 市场筛选参数
        )

    return {
        "success": True,
        "message": "开始下载 K 线数据，后台处理中...",
        "current_stats": stats,
        "download_params": {
            "years": years,
            "start_date": start_date,
            "end_date": end_date,
            "batch_size": batch_size,
            "months": months,
            "market_filter": market_filter or ["SH", "SZ", "BJ"]  # 默认全市场
        }
    }


@router.get("/api/v1/ui/{account_id}/data/stats")
async def get_data_stats(account_id: str = Path(..., description="账户 ID")):
    """获取本地 K 线数据统计信息"""
    db = get_db_manager()

    # 验证账户
    account = await db.fetchone("SELECT * FROM accounts WHERE account_id = ? AND is_active = 1", (account_id,))
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    from services.data.local_data_service import get_local_data_service

    local_service = get_local_data_service()
    stats = local_service.get_download_stats()

    return {
        "success": True,
        "stats": stats
    }


@router.get("/api/v1/ui/{account_id}/data/factor-stats")
async def get_factor_stats(account_id: str = Path(..., description="账户 ID")):
    """获取因子数据统计信息"""
    db = get_db_manager()

    # 验证账户
    account = await db.fetchone("SELECT * FROM accounts WHERE account_id = ? AND is_active = 1", (account_id,))
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    import sqlite3
    from pathlib import Path

    db_path = Path(__file__).parent.parent.parent / "data" / "kline.db"
    if not db_path.exists():
        return {"success": True, "stats": {"latest_date": None, "pending_count": 0}}

    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    # 获取因子数据最新日期
    cursor.execute("SELECT MAX(trade_date) FROM stock_daily_factors")
    factor_latest = cursor.fetchone()[0]

    # 获取 kline_data 最新日期
    cursor.execute("SELECT MAX(trade_date) FROM kline_data")
    kline_latest = cursor.fetchone()[0]

    # 计算待更新日期数
    pending_dates = 0
    if kline_latest and factor_latest:
        from datetime import datetime
        kline_dt = datetime.strptime(kline_latest, '%Y-%m-%d')
        factor_dt = datetime.strptime(factor_latest, '%Y-%m-%d')
        pending_dates = (kline_dt - factor_dt).days

    # 获取待计算股票数（今日K线有但因子表缺失的股票）
    if kline_latest:
        cursor.execute("""
            SELECT COUNT(DISTINCT k.stock_code)
            FROM kline_data k
            LEFT JOIN stock_daily_factors f
                ON k.stock_code = f.stock_code AND k.trade_date = f.trade_date
            WHERE k.trade_date = ? AND f.stock_code IS NULL
        """, (kline_latest,))
        pending_stocks = cursor.fetchone()[0]
    else:
        pending_stocks = 0

    conn.close()

    return {
        "success": True,
        "stats": {
            "latest_date": factor_latest,
            "kline_latest": kline_latest,
            "pending_count": pending_stocks,
            "pending_dates": pending_dates
        }
    }


@router.get("/api/v1/ui/{account_id}/data/factor-calc/progress")
async def get_factor_calc_progress(account_id: str = Path(..., description="账户 ID")):
    """获取因子计算进度"""
    db = get_db_manager()

    # 验证账户
    account = await db.fetchone("SELECT * FROM accounts WHERE account_id = ? AND is_active = 1", (account_id,))
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    from services.common.factor_calc_progress import get_factor_calc_tracker

    tracker = get_factor_calc_tracker()
    progress = tracker.get_progress()

    return {
        "success": True,
        "progress": progress
    }


@router.post("/api/v1/ui/{account_id}/data/calculate-factors")
async def calculate_factors(
    account_id: str = Path(..., description="账户 ID"),
    background_tasks: BackgroundTasks = None,
    mode: str = Body("smart", description="计算模式：smart/full (incremental/fill_empty已合并到smart)"),
    start_date: Optional[str] = Body(None, description="开始日期"),
    end_date: Optional[str] = Body(None, description="结束日期")
):
    """计算并更新日频因子数据

    mode选项:
    - smart: 智能更新（合并原incremental和fill_empty），只处理缺失记录和空值字段
    - full: 全量重新计算所有日期（用于初始化/重建）
    - incremental: 已合并到smart（向后兼容）
    - fill_empty: 已合并到smart（向后兼容）
    """
    db = get_db_manager()

    # 验证账户
    account = await db.fetchone("SELECT * FROM accounts WHERE account_id = ? AND is_active = 1", (account_id,))
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    from services.common.factor_calc_progress import get_factor_calc_tracker
    from services.data.local_data_service import (
        calculate_and_save_factors_for_dates,
        smart_update_factors
    )
    from datetime import datetime
    import sqlite3
    from pathlib import Path

    tracker = get_factor_calc_tracker()

    # 检查是否正在计算
    current_progress = tracker.get_progress()
    if current_progress['status'] == 'calculating':
        return {
            "success": True,
            "status": "running",
            "message": "因子计算任务正在运行中",
            "progress": current_progress
        }

    # 向后兼容：将旧模式映射到新模式
    if mode in ('incremental', 'fill_empty'):
        print(f"[FactorAPI] 模式 '{mode}' 已合并到 'smart'，使用智能更新模式")
        mode = 'smart'

    # 确定日期范围
    if start_date and end_date:
        target_start = start_date
        target_end = end_date
    else:
        db_path = Path(__file__).parent.parent.parent / "data" / "kline.db"
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()

        # 获取 kline_data 最新日期
        cursor.execute("SELECT MAX(trade_date) FROM kline_data")
        kline_latest = cursor.fetchone()[0]

        if mode == 'smart':
            # 智能更新：只计算最新日期的缺失记录
            target_start = kline_latest if kline_latest else get_china_time().strftime('%Y-%m-%d')
            target_end = kline_latest if kline_latest else get_china_time().strftime('%Y-%m-%d')
        else:
            # 全量：全部 kline 日期范围
            cursor.execute("SELECT MIN(trade_date) FROM kline_data")
            kline_earliest = cursor.fetchone()[0]
            target_start = kline_earliest if kline_earliest else '1970-01-01'
            target_end = kline_latest if kline_latest else get_china_time().strftime('%Y-%m-%d')

        conn.close()

    # 后台执行因子计算
    def run_calculation():
        try:
            tracker.start(total_stocks=0, total_batches=0)  # 初始化

            if mode == 'smart':
                result = smart_update_factors(
                    start_date=target_start,
                    end_date=target_end,
                    show_progress=True,
                    tracker=tracker
                )
                tracker.complete(inserted=result['inserted'], updated=result['updated'])
            else:
                inserted_count = calculate_and_save_factors_for_dates(
                    start_date=target_start,
                    end_date=target_end,
                    stock_codes=None,
                    only_new_dates=False,
                    show_progress=True,
                    tracker=tracker
                )
                tracker.complete(inserted=inserted_count, updated=0)

        except Exception as e:
            tracker.complete(error=str(e))

    # 启动后台任务
    if background_tasks:
        background_tasks.add_task(run_calculation)
    else:
        import threading
        thread = threading.Thread(target=run_calculation)
        thread.daemon = True
        thread.start()

    return {
        "success": True,
        "status": "started",
        "message": "因子计算任务已启动",
        "date_range": {"start": target_start, "end": target_end}
    }


@router.get("/api/v1/ui/{account_id}/data/download/progress")
async def get_download_progress(account_id: str = Path(..., description="账户 ID")):
    """获取数据下载进度"""
    db = get_db_manager()

    # 验证账户
    account = await db.fetchone("SELECT * FROM accounts WHERE account_id = ? AND is_active = 1", (account_id,))
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    from services.common.download_progress import get_progress_tracker

    tracker = get_progress_tracker()
    progress = tracker.get_progress()

    return {
        "success": True,
        "progress": progress
    }


@router.post("/api/v1/ui/{account_id}/data/download/reset")
async def reset_download_progress(account_id: str = Path(..., description="账户 ID")):
    """强制重置下载进度（用于恢复卡住的下载任务）"""
    db = get_db_manager()

    # 验证账户
    account = await db.fetchone("SELECT * FROM accounts WHERE account_id = ? AND is_active = 1", (account_id,))
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    from services.common.download_progress import get_progress_tracker

    tracker = get_progress_tracker()
    await tracker.reset()

    return {
        "success": True,
        "message": "下载进度已重置"
    }


@router.get("/api/v1/ui/{account_id}/sdk/status")
async def get_sdk_status(account_id: str = Path(..., description="账户 ID")):
    """
    获取 SDK 连接占用状态

    返回当前是否有长时间任务（下载/选股）占用 SDK 连接
    短时间请求可以根据此状态判断是否需要等待

    Returns:
        {
            sdk_busy: bool,           # SDK 是否被占用
            download_status: str,     # 下载任务状态
            screening_running: bool,  # 选股服务是否运行
            can_use_sdk: bool,        # 是否可以立即使用 SDK
            message: str,             # 状态说明
            queue_info: {             # 排队信息
                waiting_count: int,   # 等待数量
                waiting_tasks: [...]  # 等待任务列表
            }
        }
    """
    from services.common.download_progress import get_progress_tracker, DownloadStatus
    from services.common.sdk_connection_manager import get_connection_manager

    tracker = get_progress_tracker()
    progress = tracker.get_progress()
    download_status = progress.get("status")

    # 检查选股服务状态
    screening_running = False
    try:
        from services.screening.service import get_screening_service
        screening_svc = get_screening_service()
        screening_running = screening_svc.running if screening_svc else False
    except Exception:
        pass

    # 获取连接管理器状态
    conn_mgr = get_connection_manager()
    conn_status = conn_mgr.get_status()

    # 判断 SDK 是否被长时间任务占用
    busy_statuses = [DownloadStatus.DOWNLOADING.value, DownloadStatus.PREPARING.value,
                     DownloadStatus.CALCULATING_FACTORS.value]
    download_busy = download_status in busy_statuses

    sdk_busy = download_busy or screening_running or conn_status.get("status") == "busy"

    # 生成状态说明
    if download_busy:
        elapsed = progress.get("elapsed_seconds", 0)
        message = f"下载任务进行中（已运行 {elapsed//60} 分钟），短请求需要排队等待"
    elif screening_running:
        message = "选股服务运行中，短请求需要排队等待"
    elif conn_status.get("waiting_count", 0) > 0:
        message = f"当前有 {conn_status.get('waiting_count')} 个请求在排队等待"
    else:
        message = "SDK 连接空闲，可以立即使用"

    return {
        "success": True,
        "sdk_busy": sdk_busy,
        "download_status": download_status,
        "download_progress": progress.get("percent", 0),
        "screening_running": screening_running,
        "can_use_sdk": not sdk_busy,
        "message": message,
        "queue_info": {
            "status": conn_status.get("status"),
            "current_holder": conn_status.get("current_holder"),
            "waiting_count": conn_status.get("waiting_count", 0),
            "waiting_tasks": conn_status.get("waiting_tasks", [])
        },
        "detail": {
            "download": progress,
            "screening": screening_running,
            "connection": conn_status
        }
    }


# ============== 选股服务控制 ==============

@router.post("/api/v1/ui/{account_id}/screening/start")
async def start_screening(
    account_id: str = Path(..., description="账户 ID"),
    strategy_id: Optional[int] = Body(None, description="策略 ID"),
    interval: int = Body(60, description="扫描间隔（秒）", ge=10, le=3600)
):
    """启动选股服务"""
    db = get_db_manager()

    # 从数据库验证账户
    account = await db.fetchone("SELECT * FROM accounts WHERE account_id = ? AND is_active = 1", (account_id,))
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    screening_service = get_screening_service()
    result = await screening_service.start_screening(account_id, strategy_id, interval)

    return result


@router.post("/api/v1/ui/{account_id}/screening/stop")
async def stop_screening(account_id: str = Path(..., description="账户 ID")):
    """停止选股服务"""
    db = get_db_manager()

    # 从数据库验证账户
    account = await db.fetchone("SELECT * FROM accounts WHERE account_id = ? AND is_active = 1", (account_id,))
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    screening_service = get_screening_service()
    result = await screening_service.stop_screening()

    return result


@router.get("/api/v1/ui/{account_id}/screening/status")
async def get_screening_status(account_id: str = Path(..., description="账户 ID")):
    """获取选股服务状态"""
    db = get_db_manager()

    # 从数据库验证账户
    account = await db.fetchone("SELECT * FROM accounts WHERE account_id = ? AND is_active = 1", (account_id,))
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    screening_service = get_screening_service()
    status = screening_service.get_status()
    progress = screening_service.get_progress()

    return {
        "account_id": account_id,
        "screening": status,
        "progress": progress
    }


@router.post("/api/v1/ui/{account_id}/screening/run")
async def run_screening_once(
    account_id: str = Path(..., description="账户 ID"),
    strategy_id: Optional[int] = Body(None, description="策略 ID（可选，不传则扫描所有激活策略）"),
    use_local: bool = Body(True, description="是否使用本地数据源（默认 True，速度快）"),
    pending_to_temp: bool = Body(False, description="是否暂存到临时表待确认（默认 False）"),
    allow_draft: bool = Body(False, description="是否允许执行 draft/inactive 状态的策略（默认 False）")
):
    """立即执行一次选股扫描"""
    db = get_db_manager()

    # 从数据库验证账户
    account = await db.fetchone("SELECT * FROM accounts WHERE account_id = ? AND is_active = 1", (account_id,))
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    screening_service = get_screening_service()

    # 执行一次选股
    try:
        await screening_service._run_screening(
            account_id,
            strategy_id,
            use_local=use_local,
            pending_to_temp=pending_to_temp,
            require_active=not allow_draft
        )
        return {
            "success": True,
            "message": "选股扫描已完成"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"选股扫描失败：{str(e)}")


@router.get("/api/v1/ui/{account_id}/candidates")
async def get_temp_candidates(
    account_id: str = Path(..., description="账户 ID"),
):
    """获取临时候选股票列表（待确认）"""
    db = get_db_manager()

    # 从数据库验证账户
    account = await db.fetchone("SELECT * FROM accounts WHERE account_id = ? AND is_active = 1", (account_id,))
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    db = get_db_manager()
    candidates = await db.fetchall(
        "SELECT * FROM temp_candidates WHERE account_id = ? ORDER BY match_score DESC, created_at DESC",
        (account_id,)
    )

    return {
        "account_id": account_id,
        "candidates": candidates,
        "count": len(candidates)
    }


@router.post("/api/v1/ui/{account_id}/candidates/confirm")
async def confirm_candidates(
    account_id: str = Path(..., description="账户 ID"),
    stock_codes: Optional[List[str]] = Body(None, description="要确认的股票代码列表，None 表示全部"),
    confirm: bool = Body(True, description="True=确认加入，False=拒绝")
):
    """确认或拒绝临时候选股票"""
    db = get_db_manager()

    # 从数据库验证账户
    account = await db.fetchone("SELECT * FROM accounts WHERE account_id = ? AND is_active = 1", (account_id,))
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    screening_service = get_screening_service()
    result = await screening_service.confirm_candidates(account_id, stock_codes, confirm)

    return {
        "success": True,
        "confirmed": result["confirmed"],
        "rejected": result["rejected"]
    }


# ============== Watchlist 管理 ==============

@router.get("/api/v1/ui/{account_id}/watchlist")
async def get_watchlist(
    account_id: str = Path(..., description="账户 ID"),
    status: Optional[str] = Query(None, description="状态筛选：pending/watching/bought/sold"),
    group_id: Optional[int] = Query(None, description="候选组筛选"),
    grouped: Optional[bool] = Query(False, description="是否返回分组结构"),
):
    """获取 watchlist 列表"""
    db = get_db_manager()

    # 从数据库验证账户
    account = await db.fetchone("SELECT * FROM accounts WHERE account_id = ? AND is_active = 1", (account_id,))
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    db = get_db_manager()

    # JOIN 候选组和策略表，获取组名和策略名
    base_sql = """
        SELECT w.*, g.name as group_name, g.group_type, g.screening_strategy_id,
               s.name as strategy_name
        FROM watchlist w
        LEFT JOIN candidate_groups g ON w.group_id = g.id
        LEFT JOIN strategies s ON g.screening_strategy_id = s.id
        WHERE w.account_id = ?
    """
    params: list = [account_id]

    if status:
        base_sql += " AND w.status = ?"
        params.append(status)
    if group_id:
        base_sql += " AND w.group_id = ?"
        params.append(group_id)

    base_sql += " ORDER BY w.group_id, w.created_at DESC"

    watchlist = await db.fetchall(base_sql, tuple(params))

    if grouped:
        # 返回分组结构
        groups = {}
        for item in watchlist:
            if item.get('source_type') == 'manual':
                key = f"manual-{item.get('group_id', 'none')}"
                label = item.get('group_name') or '自建候选'
                gtype = 'manual'
            else:
                key = f"strategy-{item.get('group_id', 'none')}"
                label = item.get('group_name') or item.get('strategy_name') or '未知策略'
                gtype = 'screening'

            if key not in groups:
                groups[key] = {
                    "type": gtype,
                    "label": label,
                    "group_id": item.get('group_id'),
                    "screening_strategy_id": item.get('screening_strategy_id'),
                    "group_type": item.get('group_type', gtype),
                    "stocks": []
                }
            groups[key]["stocks"].append(item)

        return {
            "account_id": account_id,
            "groups": list(groups.values()),
            "total": len(watchlist)
        }

    return {
        "account_id": account_id,
        "watchlist": watchlist,
        "count": len(watchlist)
    }


@router.get("/api/v1/ui/{account_id}/watchlist/{stock_code}")
async def get_watchlist_stock(
    account_id: str = Path(..., description="账户 ID"),
    stock_code: str = Path(..., description="股票代码")
):
    """获取 watchlist 中单只股票信息"""
    db = get_db_manager()

    # 从数据库验证账户
    account = await db.fetchone("SELECT * FROM accounts WHERE account_id = ? AND is_active = 1", (account_id,))
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    db = get_db_manager()
    stock = await db.fetchone(
        "SELECT * FROM watchlist WHERE account_id = ? AND stock_code = ?",
        (account_id, stock_code)
    )

    if not stock:
        raise HTTPException(status_code=404, detail="股票不在 watchlist 中")

    return {"stock": stock}


@router.delete("/api/v1/ui/{account_id}/watchlist/{stock_code}")
async def remove_from_watchlist(
    account_id: str = Path(..., description="账户 ID"),
    stock_code: str = Path(..., description="股票代码"),
    group_id: Optional[int] = Query(None, description="候选组 ID，指定时只删除组内记录"),
):
    """从 watchlist 移除股票。指定 group_id 时只删除组内记录"""
    db = get_db_manager()

    account = await db.fetchone("SELECT * FROM accounts WHERE account_id = ? AND is_active = 1", (account_id,))
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    if group_id is not None:
        # 组内删除：只删除指定组的记录
        existing = await db.fetchone(
            "SELECT id FROM watchlist WHERE account_id = ? AND stock_code = ? AND group_id = ?",
            (account_id, stock_code, group_id)
        )
        if not existing:
            raise HTTPException(status_code=404, detail="该股票不在当前候选组中")
        await db.delete("watchlist", "account_id = ? AND stock_code = ? AND group_id = ?", (account_id, stock_code, group_id))
    else:
        # 全局删除（向后兼容）
        existing = await db.fetchone(
            "SELECT id FROM watchlist WHERE account_id = ? AND stock_code = ?",
            (account_id, stock_code)
        )
        if not existing:
            raise HTTPException(status_code=404, detail="股票不在 watchlist 中")
        await db.delete("watchlist", "account_id = ? AND stock_code = ?", (account_id, stock_code))

    return {
        "success": True,
        "message": f"已从 watchlist 移除 {stock_code}"
    }


@router.put("/api/v1/ui/{account_id}/watchlist/{stock_code}/status")
async def update_watchlist_status(
    account_id: str = Path(..., description="账户 ID"),
    stock_code: str = Path(..., description="股票代码"),
    status: str = Body(..., description="新状态：pending/watching/bought/sold/ignored")
):
    """更新 watchlist 股票状态"""
    db = get_db_manager()

    # 从数据库验证账户
    account = await db.fetchone("SELECT * FROM accounts WHERE account_id = ? AND is_active = 1", (account_id,))
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    db = get_db_manager()

    # 检查是否存在
    existing = await db.fetchone(
        "SELECT id FROM watchlist WHERE account_id = ? AND stock_code = ?",
        (account_id, stock_code)
    )

    if not existing:
        raise HTTPException(status_code=404, detail="股票不在 watchlist 中")

    await db.update("watchlist", {"status": status, "updated_at": get_china_time()},
                    "account_id = ? AND stock_code = ?", (account_id, stock_code))

    return {
        "success": True,
        "message": f"已更新 {stock_code} 状态为 {status}"
    }


@router.put("/api/v1/ui/{account_id}/candidate-groups/{group_id}/batch-status")
async def batch_update_watchlist_status(
    account_id: str = Path(..., description="账户 ID"),
    group_id: int = Path(..., description="候选组 ID"),
    status: str = Body(..., description="新状态：pending/watching/bought/sold/ignored"),
    stock_codes: Optional[List[str]] = Body(None, description="要更新的股票代码列表，为空则更新组内全部"),
):
    """批量更新候选组内股票状态

    适用场景：
    - 批量解除监控（watching → ignored/sold）
    - 批量标记已处理（pending → watching/sold）
    - 一键全部忽略
    """
    VALID_STATUSES = {"pending", "watching", "bought", "sold", "ignored"}
    if status not in VALID_STATUSES:
        raise HTTPException(status_code=400, detail=f"无效状态：{status}，可选：{', '.join(sorted(VALID_STATUSES))}")

    db = get_db_manager()

    account = await db.fetchone("SELECT * FROM accounts WHERE account_id = ? AND is_active = 1", (account_id,))
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    group = await db.fetchone("SELECT id FROM candidate_groups WHERE id = ? AND account_id = ?", (group_id, account_id))
    if not group:
        raise HTTPException(status_code=404, detail="候选组不存在")

    # 构建 SQL
    if stock_codes:
        placeholders = ",".join(["?"] * len(stock_codes))
        await db.execute(
            f"UPDATE watchlist SET status = ?, updated_at = ? WHERE group_id = ? AND stock_code IN ({placeholders})",
            [status, get_china_time(), group_id] + stock_codes
        )
        affected = len(stock_codes)
    else:
        await db.execute(
            "UPDATE watchlist SET status = ?, updated_at = ? WHERE group_id = ?",
            [status, get_china_time(), group_id]
        )
        affected = await db.fetchone("SELECT changes() as cnt")
        affected = affected["cnt"] if affected else 0

    return {
        "success": True,
        "message": f"已更新 {affected} 只股票状态为 {status}",
        "affected": affected,
    }


@router.put("/api/v1/ui/{account_id}/watchlist/{stock_code}/prices")
async def update_watchlist_prices(
    account_id: str = Path(..., description="账户 ID"),
    stock_code: str = Path(..., description="股票代码"),
    buy_price: Optional[float] = Body(None, description="买入价格"),
    stop_loss_price: Optional[float] = Body(None, description="止损价格"),
    take_profit_price: Optional[float] = Body(None, description="止盈价格"),
    target_quantity: Optional[int] = Body(None, description="目标数量")
):
    """更新 watchlist 股票的价格参数"""
    db = get_db_manager()

    # 从数据库验证账户
    account = await db.fetchone("SELECT * FROM accounts WHERE account_id = ? AND is_active = 1", (account_id,))
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    db = get_db_manager()

    update_data = {}
    if buy_price is not None:
        update_data["buy_price"] = buy_price
    if stop_loss_price is not None:
        update_data["stop_loss_price"] = stop_loss_price
    if take_profit_price is not None:
        update_data["take_profit_price"] = take_profit_price
    if target_quantity is not None:
        update_data["target_quantity"] = target_quantity

    if not update_data:
        return {"success": False, "message": "未提供更新数据"}

    update_data["updated_at"] = get_china_time()

    await db.update("watchlist", update_data,
                    "account_id = ? AND stock_code = ?", (account_id, stock_code))

    return {
        "success": True,
        "message": f"已更新 {stock_code} 价格参数"
    }


@router.put("/api/v1/ui/{account_id}/watchlist/{stock_code}")
async def update_watchlist_stock(
    account_id: str = Path(..., description="账户 ID"),
    stock_code: str = Path(..., description="股票代码"),
    group_id: int = Body(..., description="候选组 ID"),
    stock_name: Optional[str] = Body(None, description="股票名称"),
    buy_price: Optional[float] = Body(None, description="买入价格"),
    stop_loss_price: Optional[float] = Body(None, description="止损价格"),
    take_profit_price: Optional[float] = Body(None, description="止盈价格"),
    target_quantity: Optional[int] = Body(None, description="目标数量"),
    status: Optional[str] = Body(None, description="状态"),
    reason: Optional[str] = Body(None, description="入选原因"),
):
    """更新候选组内指定股票的参数（组级别编辑）"""
    db = get_db_manager()

    account = await db.fetchone("SELECT * FROM accounts WHERE account_id = ? AND is_active = 1", (account_id,))
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    # 检查当前组内是否存在
    existing = await db.fetchone(
        "SELECT id FROM watchlist WHERE account_id = ? AND stock_code = ? AND group_id = ?",
        (account_id, stock_code, group_id)
    )
    if not existing:
        raise HTTPException(status_code=404, detail="该股票不在当前候选组中")

    update_data = {"updated_at": get_china_time()}
    if stock_name is not None: update_data["stock_name"] = stock_name
    if buy_price is not None: update_data["buy_price"] = buy_price
    if stop_loss_price is not None: update_data["stop_loss_price"] = stop_loss_price
    if take_profit_price is not None: update_data["take_profit_price"] = take_profit_price
    if target_quantity is not None: update_data["target_quantity"] = target_quantity
    if status is not None: update_data["status"] = status
    if reason is not None: update_data["reason"] = reason

    if len(update_data) <= 1:
        return {"success": False, "message": "未提供更新数据"}

    await db.update("watchlist", update_data,
                    "account_id = ? AND stock_code = ? AND group_id = ?",
                    (account_id, stock_code, group_id))

    return {"success": True, "message": f"已更新 {stock_code}"}


@router.post("/api/v1/ui/{account_id}/watchlist/clear")
async def clear_watchlist(
    account_id: str = Path(..., description="账户 ID"),
    status: Optional[str] = Body(None, description="可选，只清除指定状态的记录")
):
    """清空 watchlist"""
    db = get_db_manager()

    # 从数据库验证账户
    account = await db.fetchone("SELECT * FROM accounts WHERE account_id = ? AND is_active = 1", (account_id,))
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    db = get_db_manager()

    if status:
        await db.delete("watchlist", "account_id = ? AND status = ?", (account_id, status))
    else:
        await db.delete("watchlist", "account_id = ?", (account_id,))

    return {
        "success": True,
        "message": "watchlist 已清空"
    }


# ============== 候选组管理 ==============

@router.get("/api/v1/ui/{account_id}/candidate-groups")
async def get_candidate_groups(
    account_id: str = Path(..., description="账户 ID"),
):
    """获取该账户下所有候选组（含股票数）"""
    db = get_db_manager()

    account = await db.fetchone("SELECT * FROM accounts WHERE account_id = ? AND is_active = 1", (account_id,))
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    groups = await db.fetchall("""
        SELECT g.*,
               s.name as strategy_name,
               (SELECT COUNT(*) FROM watchlist w WHERE w.group_id = g.id) as stock_count
        FROM candidate_groups g
        LEFT JOIN strategies s ON g.screening_strategy_id = s.id
        WHERE g.account_id = ?
        ORDER BY g.group_type, g.created_at DESC
    """, (account_id,))

    return {
        "account_id": account_id,
        "groups": groups,
        "count": len(groups)
    }


@router.post("/api/v1/ui/{account_id}/candidate-groups")
async def create_candidate_group(
    account_id: str = Path(..., description="账户 ID"),
    name: str = Body(..., description="候选组名称"),
    screening_strategy_id: Optional[int] = Body(None, description="关联的选股策略 ID"),
):
    """创建手动候选组"""
    db = get_db_manager()

    account = await db.fetchone("SELECT * FROM accounts WHERE account_id = ? AND is_active = 1", (account_id,))
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    # 如果关联了策略，验证策略存在
    if screening_strategy_id:
        strategy = await db.fetchone(
            "SELECT id FROM strategies WHERE id = ? AND account_id = ?",
            (screening_strategy_id, account_id)
        )
        if not strategy:
            raise HTTPException(status_code=404, detail="关联的选股策略不存在")

    now = get_china_time().isoformat()
    group_id = await db.insert("candidate_groups", {
        "account_id": account_id,
        "name": name,
        "group_type": "manual",
        "screening_strategy_id": screening_strategy_id,
        "created_at": now,
        "updated_at": now,
    })

    return {
        "success": True,
        "message": f"候选组「{name}」已创建",
        "group_id": group_id
    }


@router.put("/api/v1/ui/{account_id}/candidate-groups/{group_id}")
async def update_candidate_group(
    account_id: str = Path(...),
    group_id: int = Path(...),
    name: Optional[str] = Body(None, description="新组名"),
    screening_strategy_id: Optional[int] = Body(None, description="关联的选股策略 ID，None 表示取消关联"),
):
    """更新候选组（重命名 / 关联策略）"""
    db = get_db_manager()

    account = await db.fetchone("SELECT * FROM accounts WHERE account_id = ? AND is_active = 1", (account_id,))
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    # 检查组是否属于该账户
    group = await db.fetchone(
        "SELECT id, group_type FROM candidate_groups WHERE id = ? AND account_id = ?",
        (group_id, account_id)
    )
    if not group:
        raise HTTPException(status_code=404, detail="候选组不存在")

    if group['group_type'] == 'screening':
        raise HTTPException(status_code=400, detail="策略自动创建的候选组不可手动修改")

    update_data = {"updated_at": get_china_time().isoformat()}
    if name:
        update_data["name"] = name
    if screening_strategy_id is not None:
        if screening_strategy_id:
            strategy = await db.fetchone(
                "SELECT id FROM strategies WHERE id = ? AND account_id = ?",
                (screening_strategy_id, account_id)
            )
            if not strategy:
                raise HTTPException(status_code=404, detail="关联的选股策略不存在")
        update_data["screening_strategy_id"] = screening_strategy_id

    await db.update("candidate_groups", update_data, "id = ? AND account_id = ?", (group_id, account_id))

    return {"success": True, "message": "候选组已更新"}


@router.delete("/api/v1/ui/{account_id}/candidate-groups/{group_id}")
async def delete_candidate_group(
    account_id: str = Path(...),
    group_id: int = Path(...),
    force: bool = Query(False, description="强制删除，忽略监控中警告"),
):
    """删除候选组（默认提示监控中股票，force=true 时直接删除）"""
    db = get_db_manager()

    account = await db.fetchone("SELECT * FROM accounts WHERE account_id = ? AND is_active = 1", (account_id,))
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    group = await db.fetchone(
        "SELECT id, name, group_type FROM candidate_groups WHERE id = ? AND account_id = ?",
        (group_id, account_id)
    )
    if not group:
        raise HTTPException(status_code=404, detail="候选组不存在")

    if group['group_type'] == 'screening':
        raise HTTPException(status_code=400, detail="策略自动创建的候选组不可删除")

    # 检查组内是否有 active 状态的股票（非 force 模式时提示）
    active_rows = await db.fetchall(
        "SELECT stock_code, stock_name, status FROM watchlist WHERE group_id = ? AND status IN ('pending', 'watching') LIMIT 10",
        (group_id,)
    )
    if active_rows and not force:
        details = ", ".join([f"{r['stock_code']}({r['stock_name']})-{r['status']}" for r in active_rows])
        return {
            "success": False,
            "warning": True,
            "message": f"组内存在 {len(active_rows)} 只监控中的股票，删除后将解除监控",
            "details": details,
        }

    # 删除组内所有股票
    await db.delete("watchlist", "group_id = ?", (group_id,))
    # 删除组
    await db.delete("candidate_groups", "id = ? AND account_id = ?", (group_id, account_id))

    return {"success": True, "message": "候选组已删除"}


@router.post("/api/v1/ui/{account_id}/watchlist")
async def add_to_watchlist_manual(
    account_id: str = Path(..., description="账户 ID"),
    stock_code: str = Body(..., description="股票代码"),
    group_id: int = Body(..., description="候选组 ID"),
    stock_name: Optional[str] = Body(None, description="股票名称"),
    buy_price: Optional[float] = Body(None, description="买入价格"),
    stop_loss_price: Optional[float] = Body(None, description="止损价格"),
    take_profit_price: Optional[float] = Body(None, description="止盈价格"),
    target_quantity: Optional[int] = Body(100, description="目标数量"),
    reason: Optional[str] = Body("手动添加", description="入选原因"),
):
    """手动添加候选股票到指定候选组"""
    db = get_db_manager()

    account = await db.fetchone("SELECT * FROM accounts WHERE account_id = ? AND is_active = 1", (account_id,))
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    # 验证候选组存在且属于该账户
    group = await db.fetchone(
        "SELECT id FROM candidate_groups WHERE id = ? AND account_id = ?",
        (group_id, account_id)
    )
    if not group:
        raise HTTPException(status_code=404, detail="候选组不存在")

    # 重复检查
    existing = await db.fetchone(
        "SELECT id FROM watchlist WHERE account_id = ? AND stock_code = ? AND status IN ('pending', 'watching')",
        (account_id, stock_code)
    )
    if existing:
        raise HTTPException(status_code=409, detail="该股票已在候选列表中")

    now = get_china_time().isoformat()
    await db.insert("watchlist", {
        "account_id": account_id,
        "strategy_id": None,
        "group_id": group_id,
        "source_type": "manual",
        "stock_code": stock_code,
        "stock_name": stock_name or stock_code,
        "reason": reason or "手动添加",
        "buy_price": buy_price,
        "stop_loss_price": stop_loss_price,
        "take_profit_price": take_profit_price,
        "target_quantity": target_quantity,
        "status": "pending",
        "created_at": now,
        "updated_at": now,
    })

    return {"success": True, "message": f"已添加 {stock_code} 到候选组"}


# ============== 文件导入 ==============

@router.post("/api/v1/ui/{account_id}/watchlist/import-preview")
async def preview_watchlist_import(
    account_id: str = Path(..., description="账户 ID"),
    group_id: int = Body(..., description="候选组 ID"),
    items: List[Dict[str, Any]] = Body(..., description="待导入项列表 [{code, name}]"),
):
    """预览文件导入结果：规范化代码、查名称、检测组内重复"""
    from services.common.stock_code import normalize_stock_code
    import sqlite3

    db = get_db_manager()

    account = await db.fetchone("SELECT * FROM accounts WHERE account_id = ? AND is_active = 1", (account_id,))
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    group = await db.fetchone(
        "SELECT id FROM candidate_groups WHERE id = ? AND account_id = ?",
        (group_id, account_id)
    )
    if not group:
        raise HTTPException(status_code=404, detail="候选组不存在")

    # 当前组内已有的代码（组内不重复）
    existing_in_group_rows = await db.fetchall(
        "SELECT stock_code FROM watchlist WHERE account_id = ? AND group_id = ? AND status IN ('pending', 'watching')",
        (account_id, group_id)
    )
    existing_in_group = {row['stock_code'] for row in existing_in_group_rows}

    # 其他组中存在的代码及所在组名（用于提示）
    other_group_rows = await db.fetchall("""
        SELECT w.stock_code, GROUP_CONCAT(g.name, ',') as group_names
        FROM watchlist w
        LEFT JOIN candidate_groups g ON w.group_id = g.id
        WHERE w.account_id = ? AND w.group_id != ? AND w.status IN ('pending', 'watching')
        GROUP BY w.stock_code
    """, (account_id, group_id))
    existing_in_other_groups = {row['stock_code']: row['group_names'] for row in other_group_rows}

    # 连接 kline.db 查 stock_base_info
    kline_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "data", "kline.db")
    name_map = {}
    try:
        conn = sqlite3.connect(kline_path)
        conn.row_factory = sqlite3.Row
        codes_to_lookup = []
        for item in items:
            raw_code = item.get("code", "").strip()
            if not raw_code:
                continue
            normalized = normalize_stock_code(raw_code)
            if normalized and "." in normalized:
                codes_to_lookup.append(normalized)

        if codes_to_lookup:
            placeholders = ",".join("?" for _ in codes_to_lookup)
            rows = conn.execute(
                f"SELECT stock_code, stock_name FROM stock_base_info WHERE stock_code IN ({placeholders})",
                codes_to_lookup
            ).fetchall()
            name_map = {row['stock_code']: row['stock_name'] for row in rows}
        conn.close()
    except Exception:
        pass

    results = []
    for item in items:
        raw_code = item.get("code", "").strip()
        raw_name = item.get("name", "").strip()

        if not raw_code:
            results.append({"raw_code": raw_code, "stock_code": None, "stock_name": None, "status": "invalid", "existing_groups": None})
            continue

        normalized = normalize_stock_code(raw_code)
        if not normalized or "." not in normalized:
            results.append({"raw_code": raw_code, "stock_code": None, "stock_name": None, "status": "invalid", "existing_groups": None})
            continue

        stock_name = raw_name or name_map.get(normalized, normalized)

        # 检查重复：组内 > 其他组 > 新
        if normalized in existing_in_group:
            status = "duplicate_in_group"
        elif normalized in existing_in_other_groups:
            status = "duplicate_other"
        else:
            status = "new"

        results.append({
            "raw_code": raw_code,
            "stock_code": normalized,
            "stock_name": stock_name,
            "status": status,
            "existing_groups": existing_in_other_groups.get(normalized),
        })

    summary = {
        "total": len(results),
        "new": sum(1 for r in results if r["status"] == "new"),
        "duplicate_in_group": sum(1 for r in results if r["status"] == "duplicate_in_group"),
        "duplicate_other": sum(1 for r in results if r["status"] == "duplicate_other"),
        "invalid": sum(1 for r in results if r["status"] == "invalid"),
    }

    return {"results": results, "summary": summary}


@router.post("/api/v1/ui/{account_id}/watchlist/batch-add")
async def batch_add_to_watchlist(
    account_id: str = Path(..., description="账户 ID"),
    group_id: int = Body(..., description="候选组 ID"),
    items: List[Dict[str, Any]] = Body(..., description="确认导入项 [{stock_code, stock_name}]"),
):
    """批量添加股票到候选组，只检查组内重复"""
    db = get_db_manager()

    account = await db.fetchone("SELECT * FROM accounts WHERE account_id = ? AND is_active = 1", (account_id,))
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    group = await db.fetchone(
        "SELECT id FROM candidate_groups WHERE id = ? AND account_id = ?",
        (group_id, account_id)
    )
    if not group:
        raise HTTPException(status_code=404, detail="候选组不存在")

    # 只检查当前组内的重复
    existing_rows = await db.fetchall(
        "SELECT stock_code FROM watchlist WHERE account_id = ? AND group_id = ? AND status IN ('pending', 'watching')",
        (account_id, group_id)
    )
    existing_codes = {row['stock_code'] for row in existing_rows}

    added = 0
    skipped = 0
    now = get_china_time().isoformat()

    for item in items:
        stock_code = item.get("stock_code", "").strip()
        stock_name = (item.get("stock_name") or stock_code).strip()

        if not stock_code or "." not in stock_code:
            skipped += 1
            continue

        if stock_code in existing_codes:
            skipped += 1
            continue

        await db.insert("watchlist", {
            "account_id": account_id,
            "strategy_id": None,
            "group_id": group_id,
            "source_type": "manual",
            "stock_code": stock_code,
            "stock_name": stock_name,
            "reason": "文件导入",
            "buy_price": None,
            "stop_loss_price": None,
            "take_profit_price": None,
            "target_quantity": 100,
            "status": "pending",
            "created_at": now,
            "updated_at": now,
        })
        existing_codes.add(stock_code)
        added += 1

    message = f"成功导入 {added} 只股票"
    if skipped > 0:
        message += f"，跳过 {skipped} 只（组内重复或无效）"

    return {"success": True, "added": added, "skipped": skipped, "message": message}
