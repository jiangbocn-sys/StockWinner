"""
选股和 Watchlist 管理 API
"""

from fastapi import APIRouter, HTTPException, Path, Body, Query
from typing import List, Optional, Dict, Any
from fastapi.background import BackgroundTasks
from datetime import datetime, timezone, timedelta
from services.common.database import get_db_manager
from services.screening.service import get_screening_service

# 中国时区
CHINA_TZ = timezone(timedelta(hours=8))

def get_china_time():
    """获取中国时区时间"""
    return datetime.now(CHINA_TZ).replace(tzinfo=None)

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

    # 获取待计算股票数（kline有数据但因子表缺失的日期）
    cursor.execute("""
        SELECT COUNT(DISTINCT k.stock_code)
        FROM kline_data k
        WHERE k.trade_date > (SELECT COALESCE(MAX(trade_date), '1970-01-01') FROM stock_daily_factors)
    """)
    pending_stocks = cursor.fetchone()[0]

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


@router.post("/api/v1/ui/{account_id}/data/calculate-factors")
async def calculate_factors(
    account_id: str = Path(..., description="账户 ID"),
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

    from services.data.local_data_service import (
        calculate_and_save_factors_for_dates,
        smart_update_factors
    )
    from datetime import datetime

    # 向后兼容：将旧模式映射到新模式
    if mode in ('incremental', 'fill_empty'):
        print(f"[FactorAPI] 模式 '{mode}' 已合并到 'smart'，使用智能更新模式")
        mode = 'smart'

    # 确定日期范围
    if start_date and end_date:
        target_start = start_date
        target_end = end_date
    else:
        import sqlite3
        from pathlib import Path
        db_path = Path(__file__).parent.parent.parent / "data" / "kline.db"
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()

        # 获取 kline_data 最新日期
        cursor.execute("SELECT MAX(trade_date) FROM kline_data")
        kline_latest = cursor.fetchone()[0]

        if mode == 'smart':
            # 智能更新：覆盖kline所有日期范围（自动检测缺失和空值）
            cursor.execute("SELECT MIN(trade_date) FROM kline_data")
            kline_earliest = cursor.fetchone()[0]
            target_start = kline_earliest if kline_earliest else '1970-01-01'
            target_end = kline_latest if kline_latest else datetime.now().strftime('%Y-%m-%d')
        else:
            # 全量：全部 kline 日期范围
            cursor.execute("SELECT MIN(trade_date) FROM kline_data")
            kline_earliest = cursor.fetchone()[0]
            target_start = kline_earliest if kline_earliest else '1970-01-01'
            target_end = kline_latest if kline_latest else datetime.now().strftime('%Y-%m-%d')

        conn.close()

    # 执行因子计算
    try:
        if mode == 'smart':
            # 智能更新：只处理缺失记录和空值字段
            result = smart_update_factors(
                start_date=target_start,
                end_date=target_end,
                show_progress=True
            )
            return {
                "success": True,
                "inserted_count": result['inserted'],
                "updated_count": result['updated'],
                "total_count": result['inserted'] + result['updated'],
                "mode": "smart",
                "date_range": {"start": target_start, "end": target_end}
            }
        else:
            # 全量重新计算
            inserted_count = calculate_and_save_factors_for_dates(
                start_date=target_start,
                end_date=target_end,
                stock_codes=None,  # 自动获取所有股票
                only_new_dates=False,  # 全量模式
                show_progress=True
            )
            return {
                "success": True,
                "inserted_count": inserted_count,
                "mode": "full",
                "date_range": {"start": target_start, "end": target_end}
            }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
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
    status: Optional[str] = Query(None, description="状态筛选：pending/watching/bought/sold")
):
    """获取 watchlist 列表"""
    db = get_db_manager()

    # 从数据库验证账户
    account = await db.fetchone("SELECT * FROM accounts WHERE account_id = ? AND is_active = 1", (account_id,))
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    db = get_db_manager()

    if status:
        watchlist = await db.fetchall(
            "SELECT * FROM watchlist WHERE account_id = ? AND status = ? ORDER BY created_at DESC",
            (account_id, status)
        )
    else:
        watchlist = await db.fetchall(
            "SELECT * FROM watchlist WHERE account_id = ? ORDER BY created_at DESC",
            (account_id,)
        )

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
    stock_code: str = Path(..., description="股票代码")
):
    """从 watchlist 移除股票"""
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
