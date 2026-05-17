"""
回测 API 端点
"""

from fastapi import APIRouter, HTTPException, Path, Body, Query
from typing import List, Optional, Dict, Any
import json

from services.common.database import get_db_manager
from services.common.timezone import get_china_time
from services.backtest.engine import BacktestEngine
from services.backtest.execution import FeeConfig, PositionLimits

router = APIRouter()


@router.post("/api/v1/ui/{account_id}/backtest/runs")
async def create_backtest_run(
    account_id: str = Path(..., description="账户 ID"),
    body: Dict[str, Any] = Body(...),
):
    """
    创建并执行回测任务。

    Request body:
    {
        "name": "回测名称",
        "strategy_id": 1,  // 可选
        "mode": "simulated",  // simulated | return_accumulation
        "start_date": "2024-01-01",
        "end_date": "2024-12-31",
        "initial_capital": 1000000,
        "stock_pool": ["600000.SH", ...],  // 可选，不传则从 markets 或全市场
        "markets": ["SH", "SZ"],  // 可选
        "group_ids": [1, 2],  // 可选，候选组ID列表（优先级高于 markets）
        "stop_loss_pct": 0.05,  // 可选
        "take_profit_pct": 0.15,  // 可选
        "trailing_stop_pct": 0.03,  // 可选
        "stop_execution_price": "close",  // close | trigger
        "commission_rate": 0.0001,
        "stamp_tax": 0.0005,
        "slippage_pct": 0.0,
        "max_total_position_pct": 0.80,
        "max_single_position_pct": 0.15,
        "config": {}  // 策略配置
    }
    """
    db = get_db_manager()
    account = await db.fetchone(
        "SELECT * FROM accounts WHERE account_id = ? AND is_active = 1", (account_id,)
    )
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    engine = BacktestEngine(account_id=account_id)

    # 提取参数
    name = body.get("name", "未命名回测")
    description = body.get("description", "")
    strategy_id = body.get("strategy_id")
    mode = body.get("mode", "simulated")
    start_date = body.get("start_date")
    end_date = body.get("end_date")
    initial_capital = float(body.get("initial_capital", 1000000))
    stock_pool = body.get("stock_pool")
    markets = body.get("markets")
    group_ids = body.get("group_ids")
    config = body.get("config", {})

    # 如果指定了候选组，解析为股票代码
    if group_ids:
        placeholders = ",".join(["?"] * len(group_ids))
        rows = await db.fetchall(
            f"SELECT DISTINCT stock_code FROM watchlist WHERE account_id = ? AND group_id IN ({placeholders})",
            [account_id] + group_ids
        )
        stock_pool = [r["stock_code"] for r in rows]
        if not stock_pool:
            raise HTTPException(status_code=400, detail="所选候选组中无股票数据")
        # 存入 config 以便 rerun 时恢复
        config["group_ids"] = group_ids

    # 如果指定了 strategy_id，从数据库加载策略配置
    strategy_config = config.copy()
    if strategy_id:
        strategy = await db.fetchone(
            "SELECT * FROM strategies WHERE id = ? AND account_id = ?",
            (strategy_id, account_id)
        )
        if not strategy:
            raise HTTPException(status_code=404, detail=f"策略不存在：{strategy_id}")
        # 代码型策略：注入 code 和 strategy_type
        strategy_config["strategy_type"] = strategy.get("strategy_type", "screening")
        if strategy.get("code"):
            strategy_config["code"] = strategy["code"]
        if strategy.get("function_name"):
            strategy_config["function_name"] = strategy["function_name"]
        if strategy.get("config"):
            if isinstance(strategy["config"], str):
                strategy_config.update(json.loads(strategy["config"]))
            elif isinstance(strategy["config"], dict):
                strategy_config.update(strategy["config"])
        # 合并止盈止损配置
        strategy_config.setdefault("stop_loss_pct", strategy.get("stop_loss_pct"))
        strategy_config.setdefault("take_profit_pct", strategy.get("take_profit_pct"))

    # 合并 body 中的止盈止损
    for key in ("stop_loss_pct", "take_profit_pct", "trailing_stop_pct",
                "stop_execution_price",
                "commission_rate", "min_commission", "stamp_tax", "transfer_fee",
                "max_total_position_pct", "max_single_position_pct", "cash_reserve_pct"):
        if key in body:
            strategy_config[key] = body[key]

    # 创建回测任务
    run_id = await engine.create_run(
        name=name,
        strategy_id=strategy_id,
        mode=mode,
        start_date=start_date,
        end_date=end_date,
        initial_capital=initial_capital,
        stock_pool=stock_pool,
        markets=markets,
        config=config,
    )

    # 保存回测参数（用于详情展示和重试）
    for key in ("stop_loss_pct", "take_profit_pct", "trailing_stop_pct",
                "stop_execution_price", "slippage_pct",
                "commission_rate", "min_commission", "stamp_tax", "transfer_fee",
                "max_total_position_pct", "max_single_position_pct", "cash_reserve_pct"):
        if key in body:
            await db.execute(
                f"UPDATE backtest_runs SET {key} = ? WHERE id = ?",
                (body[key], run_id)
            )
    if description:
        await db.execute("UPDATE backtest_runs SET description = ? WHERE id = ?", (description, run_id))
    # markets / group_ids / stock_pool 存为 JSON
    if markets is not None:
        await db.execute("UPDATE backtest_runs SET markets = ? WHERE id = ?",
                         (json.dumps(markets), run_id))
    if group_ids is not None:
        await db.execute("UPDATE backtest_runs SET group_ids = ? WHERE id = ?",
                         (json.dumps(group_ids), run_id))
    if stock_pool is not None:
        await db.execute("UPDATE backtest_runs SET stock_pool = ? WHERE id = ?",
                         (json.dumps(stock_pool), run_id))

    # 在线程池中异步执行回测（避免阻塞事件循环）
    import asyncio
    from concurrent.futures import ThreadPoolExecutor

    async def run_async():
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor(max_workers=1) as executor:
            try:
                fee_config = FeeConfig(
                    commission_rate=strategy_config.get("commission_rate", 0.0001),
                    min_commission=strategy_config.get("min_commission", 5.0),
                    stamp_tax=strategy_config.get("stamp_tax", 0.0005),
                    transfer_fee=strategy_config.get("transfer_fee", 0.00002),
                )
                position_limits = PositionLimits(
                    max_total_position_pct=strategy_config.get("max_total_position_pct", 0.80),
                    max_single_position_pct=strategy_config.get("max_single_position_pct", 0.15),
                    cash_reserve_pct=strategy_config.get("cash_reserve_pct", 0.10),
                )
                await loop.run_in_executor(
                    executor,
                    lambda: engine._run_backtest_sync(
                        run_id=run_id,
                        strategy_config=strategy_config,
                        mode=mode,
                        start_date=start_date,
                        end_date=end_date,
                        initial_capital=initial_capital,
                        stock_pool=stock_pool,
                        fee_config=fee_config,
                        position_limits=position_limits,
                        slippage_pct=float(body.get("slippage_pct", 0.0)),
                        stop_loss_pct=strategy_config.get("stop_loss_pct"),
                        take_profit_pct=strategy_config.get("take_profit_pct"),
                        trailing_stop_pct=strategy_config.get("trailing_stop_pct"),
                    )
                )
            except Exception as e:
                # 线程中异常，标记失败
                try:
                    await engine._mark_failed(run_id, str(e), None)
                except Exception:
                    pass

    asyncio.ensure_future(run_async())

    return {
        "success": True,
        "run_id": run_id,
        "message": "回测任务已启动，请稍后查询结果",
    }


@router.get("/api/v1/ui/{account_id}/backtest/runs")
async def list_backtest_runs(
    account_id: str = Path(..., description="账户 ID"),
    status: Optional[str] = Query(None, description="状态过滤"),
    limit: int = Query(50, ge=1, le=200),
):
    """获取回测任务列表"""
    db = get_db_manager()
    account = await db.fetchone(
        "SELECT * FROM accounts WHERE account_id = ? AND is_active = 1", (account_id,)
    )
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    query = "SELECT br.*, s.name as strategy_name FROM backtest_runs br LEFT JOIN strategies s ON br.strategy_id = s.id WHERE br.account_id = ?"
    params: list = [account_id]

    if status:
        query += " AND br.status = ?"
        params.append(status)

    query += " ORDER BY br.created_at DESC LIMIT ?"
    params.append(limit)

    runs = await db.fetchall(query, params)
    result = []
    for run in runs:
        r = dict(run)
        # 解析 JSON 字段
        for json_key in ("config", "data_gap_report", "result_summary", "markets", "group_ids", "stock_pool"):
            if r.get(json_key):
                try:
                    r[json_key] = json.loads(r[json_key])
                except (json.JSONDecodeError, TypeError):
                    r[json_key] = None
        result.append(r)

    return {"success": True, "runs": result}


@router.get("/api/v1/ui/{account_id}/backtest/runs/{run_id}")
async def get_backtest_run(
    account_id: str = Path(..., description="账户 ID"),
    run_id: int = Path(..., description="回测任务 ID"),
):
    """获取回测任务详情"""
    db = get_db_manager()
    run = await db.fetchone(
        "SELECT br.*, s.name as strategy_name FROM backtest_runs br LEFT JOIN strategies s ON br.strategy_id = s.id WHERE br.id = ? AND br.account_id = ?",
        (run_id, account_id)
    )
    if not run:
        raise HTTPException(status_code=404, detail="回测任务不存在")

    r = dict(run)
    for json_key in ("config", "data_gap_report", "result_summary", "markets", "group_ids", "stock_pool"):
        if r.get(json_key):
            try:
                r[json_key] = json.loads(r[json_key])
            except (json.JSONDecodeError, TypeError):
                r[json_key] = None

    return {"success": True, "run": r}


@router.get("/api/v1/ui/{account_id}/backtest/runs/{run_id}/trades")
async def get_backtest_trades(
    account_id: str = Path(..., description="账户 ID"),
    run_id: int = Path(..., description="回测任务 ID"),
    stock_code: Optional[str] = Query(None, description="按股票代码过滤"),
):
    """获取回测交易记录"""
    db = get_db_manager()
    run = await db.fetchone(
        "SELECT id FROM backtest_runs WHERE id = ? AND account_id = ?",
        (run_id, account_id)
    )
    if not run:
        raise HTTPException(status_code=404, detail="回测任务不存在")

    query = "SELECT * FROM backtest_trades WHERE backtest_run_id = ?"
    params: list = [run_id]

    if stock_code:
        query += " AND stock_code = ?"
        params.append(stock_code)

    query += " ORDER BY buy_date ASC"
    trades = await db.fetchall(query, params)

    return {"success": True, "trades": [dict(t) for t in trades]}


@router.get("/api/v1/ui/{account_id}/backtest/runs/{run_id}/nav")
async def get_backtest_nav(
    account_id: str = Path(..., description="账户 ID"),
    run_id: int = Path(..., description="回测任务 ID"),
):
    """获取回测每日净值序列"""
    db = get_db_manager()
    run = await db.fetchone(
        "SELECT id FROM backtest_runs WHERE id = ? AND account_id = ?",
        (run_id, account_id)
    )
    if not run:
        raise HTTPException(status_code=404, detail="回测任务不存在")

    nav_data = await db.fetchall(
        "SELECT * FROM backtest_daily_nav WHERE backtest_run_id = ? ORDER BY trade_date ASC",
        (run_id,)
    )

    return {"success": True, "nav": [dict(n) for n in nav_data]}


@router.get("/api/v1/ui/{account_id}/backtest/runs/{run_id}/positions")
async def get_backtest_positions(
    account_id: str = Path(..., description="账户 ID"),
    run_id: int = Path(..., description="回测任务 ID"),
    trade_date: Optional[str] = Query(None, description="按交易日期过滤"),
):
    """获取回测每日持仓快照"""
    db = get_db_manager()
    run = await db.fetchone(
        "SELECT id FROM backtest_runs WHERE id = ? AND account_id = ?",
        (run_id, account_id)
    )
    if not run:
        raise HTTPException(status_code=404, detail="回测任务不存在")

    query = "SELECT * FROM backtest_daily_positions WHERE backtest_run_id = ?"
    params: list = [run_id]

    if trade_date:
        query += " AND trade_date = ?"
        params.append(trade_date)

    query += " ORDER BY trade_date ASC, stock_code ASC"
    positions = await db.fetchall(query, params)

    return {"success": True, "positions": [dict(p) for p in positions]}


@router.delete("/api/v1/ui/{account_id}/backtest/runs/{run_id}")
async def delete_backtest_run(
    account_id: str = Path(..., description="账户 ID"),
    run_id: int = Path(..., description="回测任务 ID"),
):
    """删除回测任务及其所有关联数据"""
    db = get_db_manager()
    run = await db.fetchone(
        "SELECT id FROM backtest_runs WHERE id = ? AND account_id = ?",
        (run_id, account_id)
    )
    if not run:
        raise HTTPException(status_code=404, detail="回测任务不存在")

    # 删除关联数据
    await db.execute("DELETE FROM backtest_trades WHERE backtest_run_id = ?", (run_id,))
    await db.execute("DELETE FROM backtest_daily_nav WHERE backtest_run_id = ?", (run_id,))
    await db.execute("DELETE FROM backtest_daily_positions WHERE backtest_run_id = ?", (run_id,))
    await db.execute("DELETE FROM backtest_runs WHERE id = ?", (run_id,))

    return {"success": True, "message": "回测任务已删除"}


@router.post("/api/v1/ui/{account_id}/backtest/check-data")
async def check_data_completeness(
    account_id: str = Path(..., description="账户 ID"),
    body: Dict[str, Any] = Body(...),
):
    """
    检查回测数据完整性（不执行回测）。

    Request body:
    {
        "start_date": "2024-01-01",
        "end_date": "2024-12-31",
        "stock_pool": ["600000.SH", ...],  // 可选
        "markets": ["SH", "SZ"]  // 可选
    }
    """
    db = get_db_manager()
    account = await db.fetchone(
        "SELECT * FROM accounts WHERE account_id = ? AND is_active = 1", (account_id,)
    )
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    from services.backtest.data_validator import DataCompletenessChecker

    start_date = body.get("start_date")
    end_date = body.get("end_date")
    stock_pool = body.get("stock_pool")
    group_ids = body.get("group_ids")

    if not stock_pool and group_ids:
        placeholders = ",".join(["?"] * len(group_ids))
        rows = await db.fetchall(
            f"SELECT DISTINCT stock_code FROM watchlist WHERE account_id = ? AND group_id IN ({placeholders})",
            [account_id] + group_ids
        )
        stock_pool = [r["stock_code"] for r in rows]

    if not stock_pool:
        markets = body.get("markets")
        from services.factors.kline_manager import get_kline_manager
        km = get_kline_manager()
        all_stocks = km.get_all_stocks()
        if markets:
            stock_pool = [c for c in all_stocks if c.split(".")[-1] in markets]
        else:
            stock_pool = all_stocks[:500]  # 默认只检查前500只

    checker = DataCompletenessChecker()
    report = await checker.check(stock_pool, start_date, end_date)

    return {"success": True, "report": report.to_dict()}
