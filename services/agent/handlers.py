"""
Agent API Handlers

包含所有端点 handler：查询、提交、管理、确认、管理员。
Phase 1 交付查询端点，后续阶段补充其他端点。
"""

import json
from typing import Optional, List
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, Query, Path, Body, Request

from services.common.database import get_db_manager
from services.common.timezone import get_china_time
from services.agent.middleware import (
    verify_agent_key, require_permission, require_role, validate_account_scope,
)
from services.agent.models import (
    AgentInfo, AgentCreateRequest, AgentUpdateRequest, AgentRole,
    generate_api_key, hash_api_key, get_effective_permissions, ROLE_RATE_LIMITS,
)
from services.agent.audit import audit_action, log_action

# ============================================================
# 幂等请求缓存 — 防止多个 Agent 或同一 Agent 重复调用
# ============================================================

_idempotency_cache = {}  # in-memory: {hash_key: {"result": ..., "ts": ...}}
IDEMPOTENCY_TTL = 300  # 5 分钟


def _make_idempotency_key(method: str, path: str, params: dict) -> str:
    """根据请求特征生成幂等 key"""
    import hashlib
    raw = f"{method}:{path}:{json.dumps(params, sort_keys=True, ensure_ascii=False)}"
    return hashlib.md5(raw.encode()).hexdigest()


async def _check_idempotency(method: str, path: str, params: dict) -> Optional[dict]:
    """检查是否存在相同请求的缓存结果（幂等去重）"""
    key = _make_idempotency_key(method, path, params)
    if key in _idempotency_cache:
        import time
        entry = _idempotency_cache[key]
        if time.time() - entry["ts"] < IDEMPOTENCY_TTL:
            return entry["result"]
        else:
            del _idempotency_cache[key]
    return None


async def _store_idempotency(method: str, path: str, params: dict, result: dict):
    """缓存请求结果"""
    key = _make_idempotency_key(method, path, params)
    import time
    _idempotency_cache[key] = {"result": result, "ts": time.time()}

# ============================================================
# Router 定义 —— 所有路由通过 verify_agent_key 依赖注入认证
# ============================================================

router = APIRouter()


# ============== 自身信息 ==============

@router.get("/spec")
async def get_agent_spec():
    """系统能力描述 — Agent 首次连接时获取系统边界、可用资源、策略编写规范

    返回内容包括：
    - 系统概述：StockWinner 是什么、能做什么
    - 数据表结构：可用表、字段含义、查询方式
    - API 端点列表：所有可用端点及权限要求
    - 策略编写规范：代码型策略的编写约束、可用资源
    - 安全约束：幂等保护、限速、风险等级
    """
    return {
        "system": {
            "name": "StockWinner",
            "description": "多账户智能股票交易系统，支持选股、策略管理、交易执行、持仓监控",
            "version": "v6.2.4",
        },
        "capabilities": {
            "market_data": "实时行情、日/周/月K线、技术指标（MA/RSI/MACD/BOLL/KDJ/ATR/CCI/ADX）",
            "screening": "因子选股（日频因子、月频因子），支持自定义条件筛选",
            "strategy": "策略创建（配置型/代码型）、策略执行、策略效能评估",
            "trading": "模拟交易（买/卖）、持仓管理、交易记录查询",
            "monitoring": "交易信号监控、通知推送（飞书 Webhook）",
            "scheduling": "定时任务调度（K线检查、因子计算、选股任务）",
            "notifications": "通知历史查询、通知配置管理",
        },
        "data_tables": {
            "kline_data": {
                "description": "日K线历史数据（本地缓存）",
                "key_columns": ["stock_code", "trade_date", "open", "high", "low", "close", "volume", "amount"],
                "query_hint": "WHERE stock_code = ? ORDER BY trade_date DESC LIMIT ?",
            },
            "weekly_kline_data": {
                "description": "周K线历史数据（市场交易周日历对齐）",
                "key_columns": ["stock_code", "stock_name", "week_start_date", "week_end_date", "open", "high", "low", "close", "volume", "amount"],
            },
            "stock_daily_factors": {
                "description": "日频因子数据（~62个字段，技术指标为主）",
                "key_columns": ["stock_code", "trade_date", "ma5", "ma10", "ma20", "ma60", "rsi_7", "rsi_14", "macd_dif", "macd_dea", "macd_hist", "boll_upper", "boll_mid", "boll_lower", "kdj_k", "kdj_d", "kdj_j", "atr_14", "cci_14", "adx_14", "vol_ratio", "turnover_rate"],
            },
            "stock_monthly_factors": {
                "description": "月频因子数据（财务数据 + 盈利/成长因子）",
                "key_columns": ["stock_code", "report_date", "pe_ttm", "pb", "ps_ttm", "roe", "roa", "gross_margin", "net_margin", "revenue_growth", "net_profit_growth"],
            },
            "stock_positions": {
                "description": "持仓记录",
                "key_columns": ["account_id", "stock_code", "stock_name", "quantity", "available_quantity", "avg_cost", "current_price", "market_value", "profit_loss"],
            },
            "trade_records": {
                "description": "交易记录",
                "key_columns": ["account_id", "stock_code", "stock_name", "trade_type", "price", "quantity", "amount", "trade_time", "commission"],
            },
            "strategies": {
                "description": "选股策略配置",
                "key_columns": ["id", "account_id", "name", "type", "config", "code", "code_type", "status"],
            },
            "watchlist": {
                "description": "观察清单（含候选股组关联）",
                "key_columns": ["account_id", "stock_code", "stock_name", "group_id", "source_type", "status", "current_price"],
            },
            "candidate_stocks": {
                "description": "候选股明细",
                "key_columns": ["group_id", "stock_code", "stock_name", "reason", "created_at"],
            },
            "trading_signals": {
                "description": "交易信号记录",
                "key_columns": ["account_id", "stock_code", "signal_type", "price", "created_at"],
            },
            "notification_history": {
                "description": "通知发送历史",
                "key_columns": ["account_id", "channel", "event_type", "title", "status", "created_at"],
            },
        },
        "code_strategy_spec": {
            "description": "代码型策略编写规范",
            "language": "Python 3 (安全沙盒执行)",
            "entry_point": "函数名由 strategy.function_name 字段指定，默认 run()",
            "function_signature": "def run(context: dict) -> dict:",
            "context_input": {
                "account_id": "str — 当前账户 ID",
                "strategy_id": "int — 策略 ID",
                "group_id": "int — 关联候选股组 ID",
                "db_query": "async function — SELECT 查询，返回行列表",
                "db_fetchone": "async function — 单行查询",
                "get_today": "function — 返回今天日期字符串 YYYY-MM-DD",
            },
            "return_value": {
                "stocks": "list[dict] — 筛选出的股票列表，每项含 stock_code, stock_name, reason",
                "message": "str — 执行摘要（可选）",
            },
            "available_modules": "json, math, datetime, statistics, collections",
            "prohibited": [
                "禁止 import os/sys/subprocess/socket/http 等模块",
                "禁止使用 eval/exec/open/input/print（print 可改用）",
                "禁止访问网络、文件系统、系统命令",
                "最大 500 行代码，最多 10 个函数",
            ],
            "example": """def run(context):
    # 查询今日因子数据
    rows = await context['db_query'](
        "SELECT stock_code, rsi_14, vol_ratio FROM stock_daily_factors WHERE trade_date = ? AND rsi_14 < 30",
        (context['get_today'](),)
    )
    return {
        "stocks": [{"stock_code": r["stock_code"], "reason": f"RSI超卖 {r['rsi_14']:.1f}"} for r in rows],
        "message": f"筛选出 {len(rows)} 只RSI超卖股票"
    }""",
            "validation": "提交时执行 AST 语法校验 + 沙盒试运行（用历史数据跑一次，返回结果供调试）",
        },
        "security": {
            "rate_limit": "每 Agent 有独立 token bucket 限速，默认 30-120 请求/分钟",
            "idempotency": "相同操作的重复请求会被去重（如 K 线下载、监控启停），系统返回上次结果",
            "risk_levels": {
                "low": "只读查询 — 自动放行",
                "medium": "创建/修改策略 — 审计记录 + AST 校验",
                "high": "删除/启停服务 — 需人工确认",
                "critical": "交易/账户变更 — 必须人工确认",
            },
            "account_scope": "Agent 只能访问 allowed_account_ids 指定的账户",
        },
    }


@router.get("/me")
async def get_me(agent: dict = Depends(verify_agent_key)):
    """Agent 自身信息、角色、限速"""
    db = get_db_manager()
    acct = await db.fetchone(
        "SELECT created_at, last_used_at FROM agent_accounts WHERE agent_id = ?",
        (agent["agent_id"],)
    )
    return {
        "agent_id": agent["agent_id"],
        "name": agent["name"],
        "role": agent["role"],
        "rate_limit_per_min": agent.get("rate_limit_per_min", 60),
        "enabled": True,
        "allowed_accounts": agent["allowed_accounts"],
        "created_at": acct["created_at"] if acct else None,
        "last_used_at": acct["last_used_at"] if acct else None,
    }


@router.get("/me/permissions")
async def get_permissions(agent: dict = Depends(verify_agent_key)):
    """Agent 有效权限列表"""
    return {
        "agent_id": agent["agent_id"],
        "role": agent["role"],
        "permissions": agent["permissions"],
    }


# ============== 查询端点 (viewer+) ==============

@router.get("/query/dashboard")
async def query_dashboard(
    request: Request,
    account_id: str = Query(..., description="账户 ID"),
    agent: dict = Depends(verify_agent_key),
):
    """仪表盘数据"""
    validate_account_scope(request, account_id)

    db = get_db_manager()
    account = await db.fetchone(
        "SELECT available_cash, display_name FROM accounts WHERE account_id = ?",
        (account_id,)
    )
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在：{account_id}")

    today = get_china_time().strftime("%Y-%m-%d")

    positions = await db.fetchone("""
        SELECT COUNT(*) as position_count,
               SUM(market_value) as total_market_value,
               SUM(profit_loss) as total_pnl
        FROM stock_positions WHERE account_id = ? AND quantity > 0
    """, (account_id,))

    trade_stats = await db.fetchone("""
        SELECT COUNT(*) as total_count,
               COALESCE(SUM(CASE WHEN trade_type='buy' THEN 1 ELSE 0 END), 0) as buy_count,
               COALESCE(SUM(CASE WHEN trade_type='sell' THEN 1 ELSE 0 END), 0) as sell_count,
               COALESCE(SUM(amount), 0) as total_amount
        FROM trade_records WHERE account_id = ? AND DATE(trade_time) = ?
    """, (account_id, today))

    daily_pnl = await db.fetchone("""
        SELECT SUM(profit_loss) as daily_pnl
        FROM trade_records WHERE account_id = ? AND DATE(trade_time) = ? AND trade_type = 'sell'
    """, (account_id, today))

    await log_action(
        agent_id=agent["agent_id"], action="query.dashboard", risk_level="low",
        account_id=account_id, ip_address=request.client.host if request.client else None,
    )

    return {
        "account_id": account_id,
        "account_name": account["display_name"],
        "timestamp": get_china_time().isoformat(),
        "positions_summary": {
            "available_cash": float(account.get("available_cash", 0)),
            "position_count": positions.get("position_count", 0) if positions else 0,
            "total_market_value": float(positions.get("total_market_value", 0)) if positions and positions.get("total_market_value") else 0,
            "total_pnl": float(positions.get("total_pnl", 0)) if positions and positions.get("total_pnl") else 0,
            "daily_pnl": float(daily_pnl.get("daily_pnl", 0)) if daily_pnl and daily_pnl.get("daily_pnl") else 0,
        },
        "today_trading": {
            "trade_count": trade_stats["total_count"],
            "buy_count": trade_stats["buy_count"],
            "sell_count": trade_stats["sell_count"],
            "total_amount": float(trade_stats["total_amount"]),
        },
    }


@router.get("/query/positions")
async def query_positions(
    request: Request,
    account_id: str = Query(..., description="账户 ID"),
    agent: dict = Depends(verify_agent_key),
):
    """持仓列表"""
    validate_account_scope(request, account_id)

    db = get_db_manager()
    positions = await db.fetchall(
        "SELECT * FROM stock_positions WHERE account_id = ? AND quantity > 0 ORDER BY stock_code",
        (account_id,)
    )

    await log_action(
        agent_id=agent["agent_id"], action="query.positions", risk_level="low",
        account_id=account_id, ip_address=request.client.host if request.client else None,
    )

    return {"account_id": account_id, "positions": positions}


@router.get("/query/trades")
async def query_trades(
    request: Request,
    account_id: str = Query(..., description="账户 ID"),
    start_date: Optional[str] = Query(None, description="开始日期 YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="结束日期 YYYY-MM-DD"),
    limit: int = Query(50, description="返回数量限制"),
    agent: dict = Depends(verify_agent_key),
):
    """交易记录"""
    validate_account_scope(request, account_id)

    db = get_db_manager()
    conditions = ["account_id = ?"]
    params: list = [account_id]

    if start_date:
        conditions.append("DATE(trade_time) >= ?")
        params.append(start_date)
    if end_date:
        conditions.append("DATE(trade_time) <= ?")
        params.append(end_date)

    sql = f"SELECT * FROM trade_records WHERE {' AND '.join(conditions)} ORDER BY trade_time DESC LIMIT ?"
    params.append(limit)
    trades = await db.fetchall(sql, params)

    await log_action(
        agent_id=agent["agent_id"], action="query.trades", risk_level="low",
        account_id=account_id, ip_address=request.client.host if request.client else None,
    )

    return {"account_id": account_id, "trades": trades}


@router.get("/query/signals")
async def query_signals(
    request: Request,
    account_id: str = Query(..., description="账户 ID"),
    limit: int = Query(50, description="返回数量限制"),
    agent: dict = Depends(verify_agent_key),
):
    """交易信号"""
    validate_account_scope(request, account_id)

    db = get_db_manager()
    signals = await db.fetchall(
        "SELECT * FROM trading_signals WHERE account_id = ? ORDER BY created_at DESC LIMIT ?",
        (account_id, limit)
    )

    await log_action(
        agent_id=agent["agent_id"], action="query.signals", risk_level="low",
        account_id=account_id, ip_address=request.client.host if request.client else None,
    )

    return {"account_id": account_id, "signals": signals}


@router.get("/query/watchlist")
async def query_watchlist(
    request: Request,
    account_id: str = Query(..., description="账户 ID"),
    group_id: Optional[int] = Query(None, description="候选股组 ID"),
    agent: dict = Depends(verify_agent_key),
):
    """观察清单"""
    validate_account_scope(request, account_id)

    db = get_db_manager()
    if group_id:
        watchlist = await db.fetchall(
            "SELECT * FROM watchlist WHERE account_id = ? AND group_id = ? ORDER BY stock_code",
            (account_id, group_id)
        )
    else:
        watchlist = await db.fetchall(
            "SELECT * FROM watchlist WHERE account_id = ? ORDER BY stock_code",
            (account_id,)
        )

    await log_action(
        agent_id=agent["agent_id"], action="query.watchlist", risk_level="low",
        account_id=account_id, ip_address=request.client.host if request.client else None,
    )

    return {"account_id": account_id, "watchlist": watchlist}


@router.get("/query/candidates")
async def query_candidates(
    request: Request,
    account_id: str = Query(..., description="账户 ID"),
    group_id: Optional[int] = Query(None, description="候选股组 ID"),
    agent: dict = Depends(verify_agent_key),
):
    """候选股"""
    validate_account_scope(request, account_id)

    db = get_db_manager()
    if group_id:
        candidates = await db.fetchall(
            "SELECT cs.* FROM candidate_stocks cs JOIN candidate_groups cg ON cs.group_id = cg.id WHERE cg.account_id = ? AND cs.group_id = ?",
            (account_id, group_id)
        )
    else:
        candidates = await db.fetchall(
            "SELECT cs.* FROM candidate_stocks cs JOIN candidate_groups cg ON cs.group_id = cg.id WHERE cg.account_id = ? ORDER BY cs.stock_code",
            (account_id,)
        )

    await log_action(
        agent_id=agent["agent_id"], action="query.candidates", risk_level="low",
        account_id=account_id, ip_address=request.client.host if request.client else None,
    )

    return {"account_id": account_id, "candidates": candidates}


@router.get("/query/market")
async def query_market(
    request: Request,
    stock_code: str = Query(..., description="股票代码"),
    agent: dict = Depends(verify_agent_key),
):
    """实时行情（通过 SDK）"""
    await log_action(
        agent_id=agent["agent_id"], action="query.market", risk_level="low",
        ip_address=request.client.host if request.client else None,
    )

    try:
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        md = sdk_mgr.get_market_data()

        code = stock_code
        if '.' not in code:
            code = f"{code}.SH" if code.startswith('6') else f"{code}.SZ"

        import datetime
        from services.common.timezone import CHINA_TZ
        end_dt = datetime.datetime.now(CHINA_TZ)
        begin_dt = end_dt - datetime.timedelta(days=2)
        end_date = int((end_dt + datetime.timedelta(days=1)).strftime('%Y%m%d'))
        begin_date = int(begin_dt.strftime('%Y%m%d'))

        kline_data = md.query_kline(
            code_list=[code],
            begin_date=begin_date,
            end_date=end_date,
            period=10008  # day
        )

        if kline_data and code in kline_data:
            df = kline_data[code]
            if len(df) > 0:
                latest = df.iloc[-1]
                return {
                    "stock_code": code,
                    "close": float(latest.get('close', 0)),
                    "open": float(latest.get('open', 0)),
                    "high": float(latest.get('high', 0)),
                    "low": float(latest.get('low', 0)),
                    "volume": int(latest.get('volume', 0)),
                    "amount": float(latest.get('amount', 0)),
                    "date": latest.get('trade_date', ''),
                }

        return {"stock_code": code, "error": "无数据"}
    except Exception as e:
        return {"stock_code": stock_code, "error": str(e)}


@router.get("/query/kline")
async def query_kline(
    request: Request,
    stock_code: str = Query(..., description="股票代码"),
    period: str = Query("day", description="周期：day/week/month"),
    limit: int = Query(100, description="返回数量限制"),
    agent: dict = Depends(verify_agent_key),
):
    """K 线数据（优先从本地数据库读取）"""
    await log_action(
        agent_id=agent["agent_id"], action="query.kline", risk_level="low",
        ip_address=request.client.host if request.client else None,
    )

    db = get_db_manager()

    code = stock_code
    if '.' not in code:
        code = f"{code}.SH" if code.startswith('6') else f"{code}.SZ"

    if period == "week":
        rows = await db.fetchall(
            "SELECT * FROM weekly_kline_data WHERE stock_code = ? ORDER BY week_end_date DESC LIMIT ?",
            (code, limit)
        )
    else:
        rows = await db.fetchall(
            "SELECT * FROM kline_data WHERE stock_code = ? ORDER BY trade_date DESC LIMIT ?",
            (code, limit)
        )

    return {"stock_code": code, "period": period, "data": rows}


@router.get("/query/factors")
async def query_factors(
    request: Request,
    stock_code: str = Query(..., description="股票代码"),
    date: Optional[str] = Query(None, description="日期 YYYY-MM-DD"),
    agent: dict = Depends(verify_agent_key),
):
    """因子数据"""
    await log_action(
        agent_id=agent["agent_id"], action="query.factors", risk_level="low",
        ip_address=request.client.host if request.client else None,
    )

    db = get_db_manager()
    code = stock_code
    if '.' not in code:
        code = f"{code}.SH" if code.startswith('6') else f"{code}.SZ"

    if date:
        rows = await db.fetchall(
            "SELECT * FROM stock_daily_factors WHERE stock_code = ? AND trade_date = ? LIMIT 1",
            (code, date)
        )
    else:
        rows = await db.fetchall(
            "SELECT * FROM stock_daily_factors WHERE stock_code = ? ORDER BY trade_date DESC LIMIT 1",
            (code,)
        )

    return {"stock_code": code, "factors": rows[0] if rows else None}


@router.get("/query/strategies")
async def query_strategies(
    request: Request,
    account_id: str = Query(..., description="账户 ID"),
    agent: dict = Depends(verify_agent_key),
):
    """策略列表"""
    validate_account_scope(request, account_id)

    db = get_db_manager()
    strategies = await db.fetchall(
        "SELECT * FROM strategies WHERE account_id = ? ORDER BY created_at DESC",
        (account_id,)
    )

    # 解析 config JSON
    for s in strategies:
        if s.get("config"):
            try:
                s["config"] = json.loads(s["config"])
            except Exception:
                pass

    await log_action(
        agent_id=agent["agent_id"], action="query.strategies", risk_level="low",
        account_id=account_id, ip_address=request.client.host if request.client else None,
    )

    return {"account_id": account_id, "strategies": strategies}


@router.get("/query/strategy/{strategy_id}")
async def query_strategy_detail(
    request: Request,
    strategy_id: int = Path(..., description="策略 ID"),
    account_id: Optional[str] = Query(None, description="账户 ID"),
    agent: dict = Depends(verify_agent_key),
):
    """策略详情"""
    db = get_db_manager()

    if account_id:
        validate_account_scope(request, account_id)
        strategy = await db.fetchone(
            "SELECT * FROM strategies WHERE id = ? AND account_id = ?",
            (strategy_id, account_id)
        )
    else:
        strategy = await db.fetchone(
            "SELECT * FROM strategies WHERE id = ?",
            (strategy_id,)
        )

    if not strategy:
        raise HTTPException(status_code=404, detail="策略不存在")

    if strategy.get("config"):
        try:
            strategy["config"] = json.loads(strategy["config"])
        except Exception:
            pass

    await log_action(
        agent_id=agent["agent_id"], action="query.strategy_detail", risk_level="low",
        account_id=account_id, resource_type="strategy", resource_id=str(strategy_id),
        ip_address=request.client.host if request.client else None,
    )

    return {"strategy": strategy}


@router.get("/query/notifications")
async def query_notifications(
    request: Request,
    account_id: str = Query(..., description="账户 ID"),
    limit: int = Query(50, description="返回数量限制"),
    agent: dict = Depends(verify_agent_key),
):
    """通知历史"""
    validate_account_scope(request, account_id)

    db = get_db_manager()
    notifications = await db.fetchall(
        "SELECT * FROM notification_history WHERE account_id = ? ORDER BY created_at DESC LIMIT ?",
        (account_id, limit)
    )

    await log_action(
        agent_id=agent["agent_id"], action="query.notifications", risk_level="low",
        account_id=account_id, ip_address=request.client.host if request.client else None,
    )

    return {"account_id": account_id, "notifications": notifications}


# ============== 策略提交端点 (strategist+) ==============
# Phase 2 实现

@router.post("/submit/screening")
async def submit_screening(
    request: Request,
    account_id: str = Query(...),
    name: str = Body(...),
    conditions: dict = Body(...),
    agent: dict = Depends(verify_agent_key),
    _: None = Depends(require_permission("screening:create")),
):
    """创建选股策略（Phase 2）"""
    raise HTTPException(status_code=501, detail="此端点将在 Phase 2 实现")


@router.post("/submit/strategy-config")
async def submit_strategy_config(
    request: Request,
    account_id: str = Query(...),
    name: str = Body(...),
    config: dict = Body(...),
    agent: dict = Depends(verify_agent_key),
    _: None = Depends(require_permission("strategy:create")),
):
    """创建配置型策略（Phase 2）"""
    raise HTTPException(status_code=501, detail="此端点将在 Phase 2 实现")


@router.post("/submit/strategy-code")
async def submit_strategy_code(
    request: Request,
    account_id: str = Query(...),
    name: str = Body(...),
    code: str = Body(...),
    agent: dict = Depends(verify_agent_key),
    _: None = Depends(require_permission("strategy:create")),
):
    """创建代码型策略（Phase 2，含 AST 校验）"""
    raise HTTPException(status_code=501, detail="此端点将在 Phase 2 实现")


@router.post("/submit/trading-strategy")
async def submit_trading_strategy(
    request: Request,
    account_id: str = Query(...),
    name: str = Body(...),
    config: dict = Body(...),
    agent: dict = Depends(verify_agent_key),
    _: None = Depends(require_permission("strategy:create")),
):
    """创建交易策略配置（Phase 2）"""
    raise HTTPException(status_code=501, detail="此端点将在 Phase 2 实现")


@router.put("/strategy/{strategy_id}")
async def update_strategy(
    request: Request,
    strategy_id: int = Path(...),
    account_id: str = Query(...),
    updates: dict = Body(...),
    agent: dict = Depends(verify_agent_key),
    _: None = Depends(require_permission("strategy:update")),
):
    """修改策略（Phase 2）"""
    raise HTTPException(status_code=501, detail="此端点将在 Phase 2 实现")


@router.delete("/strategy/{strategy_id}")
async def delete_strategy(
    request: Request,
    strategy_id: int = Path(...),
    account_id: str = Query(...),
    agent: dict = Depends(verify_agent_key),
    _: None = Depends(require_permission("strategy:delete")),
):
    """删除策略（Phase 2，需人工确认）"""
    raise HTTPException(status_code=501, detail="此端点将在 Phase 2 实现")


# ============== 系统管理端点 (operator+) ==============
# Phase 3 实现

@router.post("/manage/scheduler/run-now")
async def scheduler_run_now(
    request: Request,
    task_id: int = Query(...),
    agent: dict = Depends(verify_agent_key),
    _: None = Depends(require_role(AgentRole.OPERATOR)),
):
    """立即执行调度任务（Phase 3）"""
    raise HTTPException(status_code=501, detail="此端点将在 Phase 3 实现")


@router.post("/manage/scheduler/toggle")
async def scheduler_toggle(
    request: Request,
    task_id: int = Query(...),
    enabled: int = Query(..., ge=0, le=1),
    agent: dict = Depends(verify_agent_key),
    _: None = Depends(require_role(AgentRole.OPERATOR)),
):
    """启停定时任务（Phase 3）"""
    raise HTTPException(status_code=501, detail="此端点将在 Phase 3 实现")


@router.post("/manage/monitoring/start")
async def monitoring_start(
    request: Request,
    account_id: str = Query(...),
    agent: dict = Depends(verify_agent_key),
    _: None = Depends(require_role(AgentRole.OPERATOR)),
):
    """启动监控（Phase 3）"""
    raise HTTPException(status_code=501, detail="此端点将在 Phase 3 实现")


@router.post("/manage/monitoring/stop")
async def monitoring_stop(
    request: Request,
    account_id: str = Query(...),
    agent: dict = Depends(verify_agent_key),
    _: None = Depends(require_role(AgentRole.OPERATOR)),
):
    """停止监控（Phase 3）"""
    raise HTTPException(status_code=501, detail="此端点将在 Phase 3 实现")


@router.post("/manage/screening/start")
async def screening_start(
    request: Request,
    account_id: str = Query(...),
    agent: dict = Depends(verify_agent_key),
    _: None = Depends(require_role(AgentRole.OPERATOR)),
):
    """启动选股（Phase 3）"""
    raise HTTPException(status_code=501, detail="此端点将在 Phase 3 实现")


@router.post("/manage/screening/stop")
async def screening_stop(
    request: Request,
    account_id: str = Query(...),
    agent: dict = Depends(verify_agent_key),
    _: None = Depends(require_role(AgentRole.OPERATOR)),
):
    """停止选股（Phase 3）"""
    raise HTTPException(status_code=501, detail="此端点将在 Phase 3 实现")


@router.post("/manage/strategy/execute")
async def strategy_execute(
    request: Request,
    strategy_id: int = Query(...),
    account_id: str = Query(...),
    agent: dict = Depends(verify_agent_key),
    _: None = Depends(require_role(AgentRole.OPERATOR)),
):
    """执行策略（Phase 3）"""
    raise HTTPException(status_code=501, detail="此端点将在 Phase 3 实现")


# ============== 确认流程端点 ==============
# Phase 3 实现

@router.get("/confirmations/pending")
async def list_pending_confirmations(
    request: Request,
    agent: dict = Depends(verify_agent_key),
):
    """列出待确认（Phase 3）"""
    raise HTTPException(status_code=501, detail="此端点将在 Phase 3 实现")


@router.post("/confirmations/{confirmation_id}/approve")
async def confirm_approve(
    request: Request,
    confirmation_id: str = Path(...),
    agent: dict = Depends(verify_agent_key),
):
    """批准确认（Phase 3）"""
    raise HTTPException(status_code=501, detail="此端点将在 Phase 3 实现")


@router.post("/confirmations/{confirmation_id}/reject")
async def confirm_reject(
    request: Request,
    confirmation_id: str = Path(...),
    agent: dict = Depends(verify_agent_key),
):
    """拒绝确认（Phase 3）"""
    raise HTTPException(status_code=501, detail="此端点将在 Phase 3 实现")


# ============== 管理员端点 (admin only) ==============
# Phase 4 实现

@router.get("/admin/audit")
async def admin_audit(
    request: Request,
    agent_id: Optional[str] = Query(None, description="Agent ID 过滤"),
    start_date: Optional[str] = Query(None, description="开始日期 YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="结束日期 YYYY-MM-DD"),
    risk_level: Optional[str] = Query(None, description="风险等级过滤"),
    limit: int = Query(50, description="返回数量限制"),
    offset: int = Query(0, description="偏移量"),
    agent: dict = Depends(verify_agent_key),
    _: None = Depends(require_role(AgentRole.ADMIN)),
):
    """审计日志查询（Phase 1 基础版）"""
    db = get_db_manager()
    conditions = []
    params: list = []

    if agent_id:
        conditions.append("agent_id = ?")
        params.append(agent_id)
    if start_date:
        conditions.append("DATE(created_at) >= ?")
        params.append(start_date)
    if end_date:
        conditions.append("DATE(created_at) <= ?")
        params.append(end_date)
    if risk_level:
        conditions.append("risk_level = ?")
        params.append(risk_level)

    where = "WHERE " + " AND ".join(conditions) if conditions else ""
    rows = await db.fetchall(
        f"SELECT * FROM agent_audit_log {where} ORDER BY created_at DESC LIMIT ? OFFSET ?",
        params + [limit, offset]
    )

    total = await db.fetchone(
        f"SELECT COUNT(*) as cnt FROM agent_audit_log {where}", params
    )

    return {
        "total": total.get("cnt", 0) if total else 0,
        "limit": limit,
        "offset": offset,
        "logs": rows,
    }


@router.get("/admin/accounts")
async def admin_accounts(
    request: Request,
    agent: dict = Depends(verify_agent_key),
    _: None = Depends(require_role(AgentRole.ADMIN)),
):
    """账户列表（Phase 4）"""
    raise HTTPException(status_code=501, detail="此端点将在 Phase 4 实现")


@router.post("/admin/trading/execute")
async def admin_trading_execute(
    request: Request,
    account_id: str = Body(...),
    stock_code: str = Body(...),
    trade_type: str = Body(...),
    price: float = Body(...),
    quantity: int = Body(...),
    agent: dict = Depends(verify_agent_key),
    _: None = Depends(require_role(AgentRole.ADMIN)),
):
    """模拟交易执行（Phase 4）"""
    raise HTTPException(status_code=501, detail="此端点将在 Phase 4 实现")


# ============== Agent 管理端点 (admin only) ==============

@router.post("/admin/agents")
async def admin_create_agent(
    request: Request,
    body: AgentCreateRequest,
    agent: dict = Depends(verify_agent_key),
    _: None = Depends(require_role(AgentRole.ADMIN)),
):
    """创建新 Agent（admin only）"""
    db = get_db_manager()
    import uuid

    agent_id = str(uuid.uuid4())
    api_key = generate_api_key()
    key_hash = hash_api_key(api_key)
    now = get_china_time().strftime("%Y-%m-%dT%H:%M:%S")
    allowed_ids = json.dumps(body.allowed_account_ids if body.allowed_account_ids else ["*"])
    rate_limit = body.rate_limit_per_min or ROLE_RATE_LIMITS.get(body.role, 60)
    user_id = body.user_id or ""
    agent_type = body.agent_type or "generic"

    await db.execute("""
        INSERT INTO agent_accounts (agent_id, user_id, name, agent_type, api_key_hash, role, allowed_account_ids, rate_limit_per_min, enabled, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
    """, (agent_id, user_id, body.name, agent_type, key_hash, body.role, allowed_ids, rate_limit, now))

    await log_action(
        agent_id=agent["agent_id"], action="agent.create", risk_level="critical",
        request_payload={"name": body.name, "role": body.role, "user_id": user_id, "agent_type": agent_type},
        ip_address=request.client.host if request.client else None,
    )

    return {
        "agent_id": agent_id,
        "user_id": user_id,
        "name": body.name,
        "agent_type": agent_type,
        "role": body.role,
        "api_key": api_key,
        "rate_limit_per_min": rate_limit,
        "warning": "请妥善保存 api_key，系统不会再次显示",
    }


@router.get("/admin/agents")
async def admin_list_agents(
    request: Request,
    agent: dict = Depends(verify_agent_key),
    _: None = Depends(require_role(AgentRole.ADMIN)),
):
    """列出所有 Agent"""
    db = get_db_manager()
    agents = await db.fetchall(
        "SELECT agent_id, user_id, name, agent_type, role, allowed_account_ids, rate_limit_per_min, enabled, created_at, last_used_at FROM agent_accounts ORDER BY created_at DESC"
    )
    for a in agents:
        if a.get("allowed_account_ids"):
            try:
                a["allowed_account_ids"] = json.loads(a["allowed_account_ids"])
            except Exception:
                pass
    return {"agents": agents}


@router.put("/admin/agents/{agent_id}")
async def admin_update_agent(
    request: Request,
    agent_id: str = Path(...),
    body: AgentUpdateRequest = Body(...),
    agent: dict = Depends(verify_agent_key),
    _: None = Depends(require_role(AgentRole.ADMIN)),
):
    """更新 Agent"""
    db = get_db_manager()
    existing = await db.fetchone("SELECT * FROM agent_accounts WHERE agent_id = ?", (agent_id,))
    if not existing:
        raise HTTPException(status_code=404, detail="Agent 不存在")

    updates = {}
    if body.name is not None:
        updates["name"] = body.name
    if body.role is not None:
        updates["role"] = body.role
    if body.allowed_account_ids is not None:
        updates["allowed_account_ids"] = json.dumps(body.allowed_account_ids)
    if body.rate_limit_per_min is not None:
        updates["rate_limit_per_min"] = body.rate_limit_per_min
    if body.enabled is not None:
        updates["enabled"] = 1 if body.enabled else 0

    if updates:
        set_clause = ", ".join(f"{k} = ?" for k in updates)
        values = list(updates.values()) + [agent_id]
        await db.execute(f"UPDATE agent_accounts SET {set_clause} WHERE agent_id = ?", values)

    await log_action(
        agent_id=agent["agent_id"], action="agent.update", risk_level="high",
        request_payload={"agent_id": agent_id, **{k: str(v) for k, v in updates.items()}},
        ip_address=request.client.host if request.client else None,
    )

    return {"success": True, "updated": list(updates.keys())}


@router.delete("/admin/agents/{agent_id}")
async def admin_delete_agent(
    request: Request,
    agent_id: str = Path(...),
    agent: dict = Depends(verify_agent_key),
    _: None = Depends(require_role(AgentRole.ADMIN)),
):
    """删除 Agent"""
    if agent_id == agent["agent_id"]:
        raise HTTPException(status_code=400, detail="不能删除自己")

    db = get_db_manager()
    await db.execute("DELETE FROM agent_accounts WHERE agent_id = ?", (agent_id,))

    await log_action(
        agent_id=agent["agent_id"], action="agent.delete", risk_level="critical",
        request_payload={"agent_id": agent_id},
        ip_address=request.client.host if request.client else None,
    )

    return {"success": True}


@router.post("/admin/agents/{agent_id}/rotate-key")
async def admin_rotate_key(
    request: Request,
    agent_id: str = Path(...),
    agent: dict = Depends(verify_agent_key),
    _: None = Depends(require_role(AgentRole.ADMIN)),
):
    """重置 Agent API Key"""
    db = get_db_manager()
    existing = await db.fetchone("SELECT * FROM agent_accounts WHERE agent_id = ?", (agent_id,))
    if not existing:
        raise HTTPException(status_code=404, detail="Agent 不存在")

    new_key = generate_api_key()
    new_hash = hash_api_key(new_key)
    await db.execute(
        "UPDATE agent_accounts SET api_key_hash = ? WHERE agent_id = ?",
        (new_hash, agent_id)
    )

    await log_action(
        agent_id=agent["agent_id"], action="agent.rotate_key", risk_level="high",
        request_payload={"agent_id": agent_id},
        ip_address=request.client.host if request.client else None,
    )

    return {
        "agent_id": agent_id,
        "api_key": new_key,
        "warning": "请妥善保存新 api_key，旧 key 立即失效",
    }
