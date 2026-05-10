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
    - 行为约束：禁止行为清单、安全规则
    """
    return {
        "system": {
            "name": "StockWinner",
            "description": "多账户智能股票交易系统。支持选股、策略管理、交易执行、持仓监控、信号推送。",
            "version": "v7.1.6",
            "architecture": "FastAPI 后端 + Vue 3 前端 + SQLite 双库（stockwinner.db 业务 / kline.db 行情）",
        },
        "capabilities": {
            "market_data": "实时行情（SDK 直连银河证券）、日/周/月 K 线、技术指标",
            "screening": "因子选股（日频因子 62 字段 + 月频因子 23 字段），支持自定义条件筛选和代码型策略",
            "strategy": "策略创建（配置型/代码型）、策略执行、策略效能评估",
            "trading": "模拟交易（买/卖/持仓管理/交易记录）",
            "monitoring": "交易信号监控、通知推送（飞书 Webhook）",
            "scheduling": "APScheduler 定时任务（K线下载、因子计算、选股任务、盘后分析）",
            "notifications": "通知历史查询、通知配置管理",
        },

        # ================================================================
        # 数据表结构
        # ================================================================
        "data_tables": {
            "accounts": {
                "description": "用户账户信息",
                "key_columns": ["account_id", "name", "display_name", "is_active", "available_cash", "commission_rate", "stamp_tax"],
                "query": "SELECT * FROM accounts WHERE account_id = ? AND is_active = 1",
            },
            "stock_positions": {
                "description": "当前持仓记录",
                "key_columns": ["account_id", "stock_code", "stock_name", "quantity", "available_quantity", "avg_cost", "current_price", "market_value", "profit_loss", "highest_price"],
                "query": "SELECT * FROM stock_positions WHERE account_id = ? AND quantity > 0",
            },
            "trade_records": {
                "description": "交易记录（含历史）",
                "key_columns": ["account_id", "stock_code", "stock_name", "trade_type", "price", "quantity", "amount", "commission", "profit_loss", "trade_time", "trigger_source", "strategy_id"],
                "query": "SELECT * FROM trade_records WHERE account_id = ? ORDER BY trade_time DESC LIMIT ?",
            },
            "strategies": {
                "description": "策略配置表（配置型 + 代码型）",
                "key_columns": ["id", "account_id", "name", "strategy_type", "config", "status", "code", "code_type", "code_scope", "target_scope", "function_name", "buy_strategy_id", "sell_strategy_id"],
                "note": "strategy_type: 'screening'=配置型选股, 'python'=代码型; code_scope: 'screening'=选股, 'trading'=交易",
                "query": "SELECT * FROM strategies WHERE account_id = ? ORDER BY created_at DESC",
            },
            "watchlist": {
                "description": "观察清单（候选股组 + 信号）",
                "key_columns": ["account_id", "stock_code", "stock_name", "group_id", "source_type", "status", "current_price", "buy_price", "stop_loss_price", "take_profit_price", "bought", "strategy_id"],
                "note": "status: pending/watching/bought/sold/ignored; source_type: manual/strategy; bought=1 表示已实际买入",
                "query": "SELECT * FROM watchlist WHERE account_id = ? AND status = ?",
            },
            "candidate_groups": {
                "description": "候选股分组",
                "key_columns": ["id", "account_id", "name", "group_type", "screening_strategy_id"],
                "note": "group_type: 'manual'=手动创建, 'screening'=策略自动创建",
            },
            "candidate_stocks": {
                "description": "候选股明细（组级别）",
                "key_columns": ["group_id", "stock_code", "stock_name", "reason"],
            },
            "temp_candidates": {
                "description": "临时候选股（待确认）",
                "key_columns": ["account_id", "stock_code", "stock_name", "match_score", "reason"],
            },
            "trading_signals": {
                "description": "交易信号记录",
                "key_columns": ["account_id", "stock_code", "stock_name", "signal_type", "price", "reason", "strategy_id", "created_at"],
                "note": "signal_type: buy/sell/watch",
            },
            "kline_data": {
                "description": "日 K 线历史（kline.db）",
                "key_columns": ["stock_code", "stock_name", "trade_date", "open", "high", "low", "close", "volume", "amount", "turnover_rate"],
                "query": "SELECT * FROM kline_data WHERE stock_code = ? ORDER BY trade_date DESC LIMIT ?",
            },
            "weekly_kline_data": {
                "description": "周 K 线历史（kline.db，市场交易周日历对齐）",
                "key_columns": ["stock_code", "stock_name", "week_start_date", "week_end_date", "open", "high", "low", "close", "volume", "amount"],
                "query": "SELECT * FROM weekly_kline_data WHERE stock_code = ? ORDER BY week_end_date DESC LIMIT ?",
            },
            "stock_daily_factors": {
                "description": "日频因子数据（kline.db，~62 字段，技术指标为主）",
                "key_columns": ["stock_code", "trade_date", "ma5", "ma10", "ma20", "ma60", "rsi_7", "rsi_14", "macd_dif", "macd_dea", "macd_hist", "boll_upper", "boll_mid", "boll_lower", "kdj_k", "kdj_d", "kdj_j", "atr_14", "cci_14", "adx_14", "vol_ratio", "turnover_rate"],
                "query": "SELECT * FROM stock_daily_factors WHERE stock_code = ? AND trade_date = ?",
            },
            "stock_monthly_factors": {
                "description": "月频因子数据（kline.db，财务 + 盈利/成长因子，23 字段）",
                "key_columns": ["stock_code", "report_date", "pe_ttm", "pb", "ps_ttm", "roe", "roa", "gross_margin", "net_margin", "revenue_growth", "net_profit_growth"],
                "query": "SELECT * FROM stock_monthly_factors WHERE stock_code = ? ORDER BY report_date DESC LIMIT 1",
            },
            "notification_history": {
                "description": "通知发送历史",
                "key_columns": ["account_id", "channel", "event_type", "title", "status", "created_at"],
                "note": "event_type: trade/signal_triggered/task_completed/screening_completed",
            },
            "strategy_tasks": {
                "description": "调度任务配置",
                "key_columns": ["id", "account_id", "task_type", "module", "strategy_id", "group_id", "cron_expression", "enabled", "last_status"],
                "note": "task_type: 'strategy'=用户策略任务, 'builtin'=系统内置任务",
            },
        },

        # ================================================================
        # Agent API 端点
        # ================================================================
        "agent_api": {
            "authentication": {
                "header": "X-Agent-Key: sk-agent-xxxx",
                "note": "所有 /api/v1/agent/ 端点必须携带此 header，否则 401",
            },
            "query_endpoints": {
                "description": "只读查询，viewer 及以上角色可用",
                "endpoints": [
                    {"method": "GET", "path": "/api/v1/agent/query/dashboard?account_id=xxx", "desc": "仪表盘数据（仓位、现金、今日交易）"},
                    {"method": "GET", "path": "/api/v1/agent/query/positions?account_id=xxx", "desc": "当前持仓列表"},
                    {"method": "GET", "path": "/api/v1/agent/query/trades?account_id=xxx&start_date=&end_date=&limit=50", "desc": "交易记录"},
                    {"method": "GET", "path": "/api/v1/agent/query/signals?account_id=xxx&limit=50", "desc": "交易信号"},
                    {"method": "GET", "path": "/api/v1/agent/query/watchlist?account_id=xxx&group_id=", "desc": "观察清单"},
                    {"method": "GET", "path": "/api/v1/agent/query/candidates?account_id=xxx&group_id=", "desc": "候选股明细"},
                    {"method": "GET", "path": "/api/v1/agent/query/strategies?account_id=xxx", "desc": "策略列表（含 config JSON）"},
                    {"method": "GET", "path": "/api/v1/agent/query/strategy/{id}", "desc": "策略详情"},
                    {"method": "GET", "path": "/api/v1/agent/query/market?stock_code=600000.SH", "desc": "实时行情（SDK）"},
                    {"method": "GET", "path": "/api/v1/agent/query/kline?stock_code=600000.SH&period=day&limit=100", "desc": "K 线数据（day/week/month）"},
                    {"method": "GET", "path": "/api/v1/agent/query/factors?stock_code=600000.SH&date=2026-05-10", "desc": "日频因子"},
                    {"method": "GET", "path": "/api/v1/agent/query/notifications?account_id=xxx&limit=50", "desc": "通知历史"},
                ],
            },
            "submit_endpoints": {
                "description": "策略创建，strategist 及以上角色可用",
                "endpoints": [
                    {"method": "POST", "path": "/api/v1/agent/submit/screening?account_id=xxx", "desc": "创建配置型选股策略", "body": '{"name":"策略名","config":{"buy_conditions":[{"condition":"RSI_14 < 30","operator":"and"}]},"target_scope":"group","match_score_threshold":0.5}'},
                    {"method": "POST", "path": "/api/v1/agent/submit/strategy-code?account_id=xxx", "desc": "创建代码型策略（AST 校验 + 试运行）", "body": '{"name":"策略名","code":"def run(context):\\n    ...","function_name":"run","code_scope":"screening"}'},
                    {"method": "POST", "path": "/api/v1/agent/submit/trading-strategy?account_id=xxx", "desc": "创建交易策略（code_scope=trading）", "body": '{"name":"策略名","code":"def run(context):\\n    ...","function_name":"run","buy_strategy_id":38,"sell_strategy_id":39}'},
                    {"method": "PUT", "path": "/api/v1/agent/strategy/{id}?account_id=xxx", "desc": "修改策略（代码变更时重新 AST 校验）", "body": '{"name":"新名称","code":"..."}'},
                    {"method": "DELETE", "path": "/api/v1/agent/strategy/{id}?account_id=xxx", "desc": "删除策略（需 strategist 角色，清理关联数据）"},
                ],
            },
            "admin_endpoints": {
                "description": "管理员功能，admin 角色",
                "endpoints": [
                    {"method": "GET", "path": "/api/v1/agent/admin/audit?limit=50&offset=0", "desc": "审计日志"},
                    {"method": "POST", "path": "/api/v1/agent/admin/agents", "desc": "创建新 Agent"},
                    {"method": "GET", "path": "/api/v1/agent/admin/agents", "desc": "列出所有 Agent"},
                    {"method": "PUT", "path": "/api/v1/agent/admin/agents/{id}", "desc": "更新 Agent"},
                    {"method": "DELETE", "path": "/api/v1/agent/admin/agents/{id}", "desc": "删除 Agent"},
                    {"method": "POST", "path": "/api/v1/agent/admin/agents/{id}/rotate-key", "desc": "重置 Agent API Key"},
                ],
            },
        },

        # ================================================================
        # 策略编写规范
        # ================================================================
        "code_strategy_spec": {
            "description": "代码型策略编写规范",
            "language": "Python 3（安全沙盒执行，AST 校验 + 受限 __builtins__）",
            "entry_point": "def run(context) 函数，名称由 strategy.function_name 指定（默认 run）",
            "function_signature": "def run(context: dict) -> list:",
            "return_value": {
                "format": "list[dict] — 每只股票一个信号字典",
                "required_fields": {"stock_code": "str，如 600000.SH"},
                "optional_fields": {"action": "'buy'/'sell'/'watch'，默认 buy", "stock_name": "str", "buy_price": "float", "target_quantity": "int", "stop_loss_pct": "float，默认 0.05", "take_profit_pct": "float，默认 0.15", "reason": "str"},
            },
            "context_available": {
                "stocks": "list[dict] — 当前候选股票列表 [{stock_code, stock_name}]",
                "account_id": "str — 当前账户 ID",
                "today": "str — 今天日期 YYYY-MM-DD",
                "indicators": "dict — {calculate_ma, calculate_rsi, calculate_macd, calculate_kdj, calculate_bollinger_bands, calculate_adx, calculate_atr, calculate_ema, calculate_obv, calculate_historical_volatility}",
                "get_kline_smart(codes, lookback)": "智能 K 线（盘中实时/盘后本地）",
                "get_batch_kline(codes, limit)": "批量获取 K 线",
                "get_factors(code, date)": "获取单只股票日频因子",
                "get_factors_batch(codes, date)": "批量获取日频因子",
                "get_kline_spliced(codes, lookback)": "拼接 K 线",
                "get_kline_local(code, limit, start_date)": "本地 K 线（同步）",
                "query_db(sql, params)": "同步只读 SQL 查询，返回 list[dict]",
                "kronos_predict(df_hist, pred_len)": "Kronos 时间序列预测",
            },
            "allowed_imports": ["pandas", "numpy", "datetime", "statistics", "json", "math", "re", "collections", "itertools", "functools", "dataclasses", "typing", "time", "calendar", "decimal", "copy", "string"],
            "prohibited_imports": ["os", "sys", "subprocess", "socket", "http", "requests", "urllib", "sqlite3", "torch", "safetensors"],
            "prohibited_calls": ["eval", "exec", "compile", "open", "input", "breakpoint", "getattr", "setattr", "delattr", "globals", "locals", "__import__"],
            "limits": "最大 500 行代码，最多 10 个函数，不能是 async def",
            "example_screening": """def run(context):
    today = context['today']
    stocks = context['stocks']
    get_factors = context['get_factors']

    results = []
    for s in stocks:
        factors = get_factors(s['stock_code'], today)
        if factors and factors.get('rsi_14', 100) < 30:
            results.append({
                'stock_code': s['stock_code'],
                'stock_name': s['stock_name'],
                'action': 'buy',
                'reason': f"RSI超卖 {factors['rsi_14']:.1f}",
            })
    return results""",
            "example_trading": """def run(context):
    # 交易型策略：检查持仓决定是否卖出
    query_db = context['query_db']
    today = context['today']

    positions = query_db(
        "SELECT stock_code, avg_cost, current_price FROM stock_positions WHERE account_id = ? AND quantity > 0",
        (context['account_id'],)
    )
    results = []
    for p in positions:
        if p['current_price'] and p['avg_cost']:
            loss_pct = (p['avg_cost'] - p['current_price']) / p['avg_cost']
            if loss_pct > 0.08:  # 亏损超 8%
                results.append({
                    'stock_code': p['stock_code'],
                    'action': 'sell',
                    'reason': f"止损 亏损{loss_pct*100:.1f}%",
                })
    return results""",
        },

        # ================================================================
        # 行为约束（禁止行为清单）
        # ================================================================
        "behavioral_constraints": {
            "description": "Agent 行为约束 — 违反将导致请求被拒绝或触发安全审计",
            "never": [
                "禁止绕过 /api/v1/agent/ 路径直接 curl /api/v1/ui/ 端点 — 必须携带 X-Agent-Key 通过 agent API 操作，否则行为无法审计",
                "禁止在非交易时段（9:15 之前、11:30-13:00、15:00 之后）执行买入/卖出操作",
                "禁止删除未引用的策略（策略被其他策略通过 buy_strategy_id/sell_strategy_id 引用时）",
                "禁止对同一股票在同一天内创建多个重复的买入信号",
                "禁止使用硬编码的 account_id — 应从 /me 端点或用户配置中获取",
                "禁止修改 system 级别的任务（task_type='builtin' 的 strategy_tasks 记录）",
                "禁止直接操作数据库文件（stockwinner.db / kline.db）— 必须通过 API",
                "禁止在策略代码中 import os/sys/subprocess/socket/http 等模块",
                "禁止使用 eval/exec/open 等危险函数",
                "禁止在策略中建立网络连接或读写文件系统",
            ],
            "must": [
                "创建代码型策略后必须先通过 test-run 验证再激活",
                "执行交易操作前必须确认账户可用资金充足",
                "修改或删除策略前必须先查询策略详情确认影响范围",
                "所有写操作（POST/PUT/DELETE）必须使用幂等请求或接受可能的重复执行后果",
                "使用 get_kline_smart 而非直接调用 SDK 获取行情数据",
                "策略代码必须返回 list 类型，不能返回 dict 或其他类型",
            ],
            "should": [
                "优先使用批量接口（get_factors_batch, get_batch_kline）减少 TGW 连接占用",
                "策略代码尽量简洁，优先使用已注入的 context 工具函数",
                "查询数据时使用 LIMIT 防止返回过多记录",
                "交易策略应设置合理的 stop_loss_pct（0.05-0.10）和 take_profit_pct（0.15-0.30）",
            ],
        },

        # ================================================================
        # 安全约束
        # ================================================================
        "security": {
            "rate_limit": "每 Agent 独立 token bucket，默认 30-120 请求/分钟（按角色），超限返回 429",
            "idempotency": "相同请求（method+path+body 的 MD5）5 分钟内去重，返回上次结果",
            "risk_levels": {
                "low": "只读查询 — 自动放行",
                "medium": "创建/修改策略 — 审计记录 + AST 校验",
                "high": "删除策略/启停服务 — 审计记录，operator+ 可执行",
                "critical": "交易/账户变更 — 必须人工确认",
            },
            "account_scope": "Agent 只能访问 allowed_account_ids 指定的账户，越权返回 403",
            "audit": "所有写操作（POST/PUT/DELETE）自动记录到 agent_audit_log 表",
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
    config: dict = Body(...),
    target_scope: Optional[str] = Body(None),
    match_score_threshold: Optional[float] = Body(None),
    agent: dict = Depends(verify_agent_key),
    _: None = Depends(require_permission("strategy:create")),
):
    """创建选股策略（配置型）"""
    validate_account_scope(request, account_id)
    db = get_db_manager()

    account = await db.fetchone(
        "SELECT 1 FROM accounts WHERE account_id = ? AND is_active = 1",
        (account_id,)
    )
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在：{account_id}")

    now = get_china_time().strftime("%Y-%m-%d %H:%M:%S")
    strategy_data = {
        "account_id": account_id,
        "name": name,
        "strategy_type": "screening",
        "status": "active",
        "config": json.dumps(config),
        "match_score_threshold": match_score_threshold or 0.5,
        "target_scope": target_scope or "group",
        "created_at": now,
        "updated_at": now,
    }
    strategy_id = await db.insert("strategies", strategy_data)
    strategy = await db.fetchone("SELECT * FROM strategies WHERE id = ?", (strategy_id,))

    await log_action(
        agent_id=agent["agent_id"], action="submit.screening", risk_level="medium",
        account_id=account_id, resource_type="strategy", resource_id=str(strategy_id),
        request_payload={"name": name, "config_keys": list(config.keys())},
        ip_address=request.client.host if request.client else None,
    )

    return {"success": True, "message": "选股策略已创建", "strategy": strategy}


@router.post("/submit/strategy-config")
async def submit_strategy_config(
    request: Request,
    account_id: str = Query(...),
    name: str = Body(...),
    config: dict = Body(...),
    target_scope: Optional[str] = Body(None),
    match_score_threshold: Optional[float] = Body(None),
    agent: dict = Depends(verify_agent_key),
    _: None = Depends(require_permission("strategy:create")),
):
    """创建配置型策略（screening 别名）"""
    return await submit_screening(
        request, account_id=account_id, name=name, config=config,
        target_scope=target_scope, match_score_threshold=match_score_threshold, agent=agent
    )


@router.post("/submit/strategy-code")
async def submit_strategy_code(
    request: Request,
    account_id: str = Query(...),
    name: str = Body(...),
    code: str = Body(...),
    function_name: Optional[str] = Body(None),
    code_scope: Optional[str] = Body("screening"),
    agent: dict = Depends(verify_agent_key),
    _: None = Depends(require_permission("strategy:create")),
):
    """创建代码型策略：AST 校验 + 试运行 + 入库"""
    validate_account_scope(request, account_id)
    db = get_db_manager()

    account = await db.fetchone(
        "SELECT 1 FROM accounts WHERE account_id = ? AND is_active = 1",
        (account_id,)
    )
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在：{account_id}")

    # 1. AST 校验
    from services.strategy.engine import get_strategy_engine
    engine = get_strategy_engine()
    validation = engine.validate_code(code)
    if not validation["valid"]:
        await log_action(
            agent_id=agent["agent_id"], action="submit.strategy-code", risk_level="medium",
            account_id=account_id, resource_type="strategy",
            request_payload={"name": name, "validation_error": validation["error"]},
            ip_address=request.client.host if request.client else None,
        )
        raise HTTPException(status_code=400, detail=f"AST 校验失败：{validation['error']}")

    # 2. 试运行：构建 context 并沙盒执行
    today = get_china_time().strftime("%Y-%m-%d")
    watchlist_rows = await db.fetchall(
        "SELECT DISTINCT stock_code, stock_name FROM watchlist WHERE account_id = ? LIMIT 20",
        (account_id,)
    )
    stocks = [dict(r) for r in watchlist_rows] if watchlist_rows else [
        {"stock_code": "600000.SH", "stock_name": "浦发银行"},
    ]
    stock_codes = [s["stock_code"] for s in stocks]

    from services.common import technical_indicators
    from services.data.local_data_service import get_local_data_service

    lds = get_local_data_service()

    def _get_kline_local(sc, limit=100, start_date=None):
        return lds.get_kline_data(sc, start_date=start_date, limit=limit)

    def _get_batch_kline(codes, limit=100):
        return lds.get_batch_kline(codes, limit=limit)

    def _get_factors(sc, date=None):
        return lds.get_daily_factors(sc, date or today)

    def _get_factors_batch(codes, date=None):
        return lds.get_daily_factors_batch(codes, date or today)

    def _get_kline_spliced(codes, lookback=100):
        return lds.get_kline_spliced(codes, lookback=lookback)

    def _get_kline_smart(codes, lookback=100):
        return lds.get_kline_with_realtime(codes, lookback=lookback)

    test_run_result = None
    test_run_error = None
    test_run_output = ""
    import io
    import time

    context = {
        "stocks": stocks,
        "account_id": account_id,
        "today": today,
        "indicators": {
            "calculate_ma": technical_indicators.calculate_ma,
            "calculate_rsi": technical_indicators.calculate_rsi,
            "calculate_macd": technical_indicators.calculate_macd,
            "calculate_kdj": technical_indicators.calculate_kdj,
            "calculate_bollinger_bands": technical_indicators.calculate_bollinger_bands,
            "calculate_adx": technical_indicators.calculate_adx,
            "calculate_atr": technical_indicators.calculate_atr,
            "calculate_ema": technical_indicators.calculate_ema,
            "calculate_obv": technical_indicators.calculate_obv,
            "calculate_historical_volatility": technical_indicators.calculate_historical_volatility,
        },
        "get_kline_local": _get_kline_local,
        "get_batch_kline": _get_batch_kline,
        "get_factors": _get_factors,
        "get_factors_batch": _get_factors_batch,
        "get_kline_spliced": _get_kline_spliced,
        "get_kline_smart": _get_kline_smart,
    }

    old_stdout = __import__("sys").stdout
    captured_output = io.StringIO()
    __import__("sys").stdout = captured_output
    start_time = time.time()

    try:
        strategy_dict = {"code": code, "function_name": function_name or "run", "name": name}
        signals = engine.execute_strategy(strategy_dict, context)
        duration_ms = int((time.time() - start_time) * 1000)
        test_run_output = captured_output.getvalue()
        test_run_result = {
            "signals": signals[:10],  # 只返回前 10 条
            "signal_count": len(signals),
            "output": test_run_output,
            "duration_ms": duration_ms,
        }
    except Exception as e:
        test_run_error = f"{type(e).__name__}: {str(e)}"
        test_run_output = captured_output.getvalue()
    finally:
        __import__("sys").stdout = old_stdout

    # 3. 试运行有错误 → 报错
    if test_run_error:
        await log_action(
            agent_id=agent["agent_id"], action="submit.strategy-code", risk_level="medium",
            account_id=account_id, resource_type="strategy",
            request_payload={"name": name, "test_run_error": test_run_error},
            ip_address=request.client.host if request.client else None,
        )
        raise HTTPException(status_code=400, detail=f"试运行失败：{test_run_error}")

    # 4. 入库
    now = get_china_time().strftime("%Y-%m-%d %H:%M:%S")
    strategy_data = {
        "account_id": account_id,
        "name": name,
        "strategy_type": "python",
        "status": "active",
        "code": code,
        "code_type": "python",
        "code_scope": code_scope or "screening",
        "function_name": function_name or "run",
        "target_scope": "group",
        "created_at": now,
        "updated_at": now,
    }
    strategy_id = await db.insert("strategies", strategy_data)
    strategy = await db.fetchone("SELECT * FROM strategies WHERE id = ?", (strategy_id,))

    await log_action(
        agent_id=agent["agent_id"], action="submit.strategy-code", risk_level="medium",
        account_id=account_id, resource_type="strategy", resource_id=str(strategy_id),
        request_payload={"name": name, "code_scope": code_scope, "signal_count": len(signals) if signals else 0},
        ip_address=request.client.host if request.client else None,
    )

    return {
        "success": True,
        "message": "代码型策略已创建",
        "strategy": strategy,
        "validation": validation,
        "test_run": test_run_result,
    }


@router.post("/submit/trading-strategy")
async def submit_trading_strategy(
    request: Request,
    account_id: str = Query(...),
    name: str = Body(...),
    code: str = Body(...),
    function_name: Optional[str] = Body(None),
    buy_strategy_id: Optional[int] = Body(None),
    sell_strategy_id: Optional[int] = Body(None),
    agent: dict = Depends(verify_agent_key),
    _: None = Depends(require_permission("strategy:create")),
):
    """创建交易策略（code_scope=trading）"""
    validate_account_scope(request, account_id)
    db = get_db_manager()

    account = await db.fetchone(
        "SELECT 1 FROM accounts WHERE account_id = ? AND is_active = 1",
        (account_id,)
    )
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在：{account_id}")

    # AST 校验
    from services.strategy.engine import get_strategy_engine
    engine = get_strategy_engine()
    validation = engine.validate_code(code)
    if not validation["valid"]:
        raise HTTPException(status_code=400, detail=f"AST 校验失败：{validation['error']}")

    # 校验引用的策略
    if buy_strategy_id:
        ref = await db.fetchone(
            "SELECT strategy_type FROM strategies WHERE id = ? AND account_id = ?",
            (buy_strategy_id, account_id)
        )
        if not ref:
            raise HTTPException(status_code=400, detail="引用的买入策略不存在")
        if ref["strategy_type"] != "python":
            raise HTTPException(status_code=400, detail="买入策略必须是代码型策略")

    if sell_strategy_id:
        ref = await db.fetchone(
            "SELECT strategy_type FROM strategies WHERE id = ? AND account_id = ?",
            (sell_strategy_id, account_id)
        )
        if not ref:
            raise HTTPException(status_code=400, detail="引用的卖出策略不存在")
        if ref["strategy_type"] != "python":
            raise HTTPException(status_code=400, detail="卖出策略必须是代码型策略")

    # 入库
    now = get_china_time().strftime("%Y-%m-%d %H:%M:%S")
    strategy_data = {
        "account_id": account_id,
        "name": name,
        "strategy_type": "python",
        "status": "active",
        "code": code,
        "code_type": "python",
        "code_scope": "trading",
        "function_name": function_name or "run",
        "target_scope": "group",
        "buy_strategy_id": buy_strategy_id,
        "sell_strategy_id": sell_strategy_id,
        "created_at": now,
        "updated_at": now,
    }
    strategy_id = await db.insert("strategies", strategy_data)
    strategy = await db.fetchone("SELECT * FROM strategies WHERE id = ?", (strategy_id,))

    await log_action(
        agent_id=agent["agent_id"], action="submit.trading-strategy", risk_level="medium",
        account_id=account_id, resource_type="strategy", resource_id=str(strategy_id),
        request_payload={"name": name},
        ip_address=request.client.host if request.client else None,
    )

    return {
        "success": True,
        "message": "交易策略已创建",
        "strategy": strategy,
        "validation": validation,
    }


@router.put("/strategy/{strategy_id}")
async def update_strategy(
    request: Request,
    strategy_id: int = Path(...),
    account_id: str = Query(...),
    updates: dict = Body(...),
    agent: dict = Depends(verify_agent_key),
    _: None = Depends(require_permission("strategy:update")),
):
    """修改策略（代码型策略更新 code 时重新 AST 校验）"""
    validate_account_scope(request, account_id)
    db = get_db_manager()

    strategy = await db.fetchone(
        "SELECT * FROM strategies WHERE id = ? AND account_id = ?",
        (strategy_id, account_id)
    )
    if not strategy:
        raise HTTPException(status_code=404, detail="策略不存在")

    # 如果更新 code → AST 校验
    new_code = updates.get("code")
    if new_code:
        from services.strategy.engine import get_strategy_engine
        validation = get_strategy_engine().validate_code(new_code)
        if not validation["valid"]:
            raise HTTPException(status_code=400, detail=f"AST 校验失败：{validation['error']}")

    # 构建更新数据
    update_data = {"updated_at": get_china_time().strftime("%Y-%m-%d %H:%M:%S")}
    allowed_fields = {"name", "description", "strategy_type", "config", "status",
                      "code", "code_type", "code_scope", "target_scope", "function_name",
                      "match_score_threshold", "buy_strategy_id", "sell_strategy_id"}
    for key, value in updates.items():
        if key in allowed_fields:
            if key == "config" and isinstance(value, dict):
                update_data[key] = json.dumps(value, ensure_ascii=False)
            else:
                update_data[key] = value

    if len(update_data) > 1:
        await db.update("strategies", update_data, "id = ?", (strategy_id,))

    updated = await db.fetchone("SELECT * FROM strategies WHERE id = ?", (strategy_id,))

    await log_action(
        agent_id=agent["agent_id"], action="strategy.update", risk_level="medium",
        account_id=account_id, resource_type="strategy", resource_id=str(strategy_id),
        request_payload={"updates": list(updates.keys())},
        ip_address=request.client.host if request.client else None,
    )

    return {"success": True, "message": "策略已更新", "strategy": updated}


@router.delete("/strategy/{strategy_id}")
async def delete_strategy(
    request: Request,
    strategy_id: int = Path(...),
    account_id: str = Query(...),
    agent: dict = Depends(verify_agent_key),
    _: None = Depends(require_permission("strategy:delete")),
):
    """删除策略"""
    validate_account_scope(request, account_id)
    db = get_db_manager()

    strategy = await db.fetchone(
        "SELECT * FROM strategies WHERE id = ? AND account_id = ?",
        (strategy_id, account_id)
    )
    if not strategy:
        raise HTTPException(status_code=404, detail="策略不存在")

    # 清理关联数据
    await db.execute("DELETE FROM temp_candidates WHERE strategy_id = ?", (strategy_id,))
    await db.execute("DELETE FROM candidate_groups WHERE screening_strategy_id = ?", (strategy_id,))
    await db.execute("DELETE FROM trading_signals WHERE strategy_id = ? AND account_id = ?", (strategy_id, account_id))
    await db.delete("strategies", "id = ?", (strategy_id,))

    await log_action(
        agent_id=agent["agent_id"], action="strategy.delete", risk_level="high",
        account_id=account_id, resource_type="strategy", resource_id=str(strategy_id),
        request_payload={"name": strategy.get("name")},
        ip_address=request.client.host if request.client else None,
    )

    return {"success": True, "message": f"策略「{strategy['name']}」已删除"}


# ============== 系统管理端点 (operator+) ==============
# Phase 3 实现

@router.post("/manage/scheduler/run-now")
async def scheduler_run_now(
    request: Request,
    task_id: int = Query(..., description="任务 ID（strategy_tasks 表）"),
    agent: dict = Depends(verify_agent_key),
    _: None = Depends(require_role(AgentRole.OPERATOR)),
):
    """立即执行调度任务"""
    from services.common.scheduler_service import get_scheduler

    scheduler = get_scheduler()
    if not scheduler._running or not scheduler._scheduler:
        return {"success": False, "message": "调度服务未启动"}

    result = scheduler.run_manual_strategy_task(task_id)
    agent_id = agent.get("id", "")
    log_action(
        agent_id=agent_id, action="scheduler:run_now",
        account_id="", resource_type="scheduler", resource_id=str(task_id),
        request_payload={"task_id": task_id},
        response_summary=str(result),
        ip_address=request.client.host if request.client else None,
    )
    return result


@router.post("/manage/scheduler/toggle")
async def scheduler_toggle(
    request: Request,
    task_id: int = Query(..., description="任务 ID（strategy_tasks 表）"),
    enabled: int = Query(..., ge=0, le=1, description="1=启用, 0=禁用"),
    agent: dict = Depends(verify_agent_key),
    _: None = Depends(require_role(AgentRole.OPERATOR)),
):
    """启停定时任务（更新 DB enabled 字段 + 重新加载 APScheduler）"""
    from services.common.scheduler_service import get_scheduler

    db = get_db_manager()
    task = await db.fetchone(
        "SELECT id, name, cron_expression, enabled FROM strategy_tasks WHERE id = ?",
        (task_id,)
    )
    if not task:
        return {"success": False, "message": f"任务 {task_id} 不存在"}

    await db.execute(
        "UPDATE strategy_tasks SET enabled = ? WHERE id = ?",
        (enabled, task_id)
    )

    scheduler = get_scheduler()
    if scheduler._running and scheduler._scheduler:
        reload_result = scheduler.reload_strategy_tasks()
    else:
        reload_result = {"success": False, "note": "调度服务未启动，重新加载已跳过"}

    agent_id = agent.get("id", "")
    log_action(
        agent_id=agent_id, action="scheduler:toggle",
        account_id="", resource_type="scheduler", resource_id=str(task_id),
        request_payload={"task_id": task_id, "enabled": enabled},
        response_summary=f"已{'启用' if enabled else '禁用'}: {task.get('name', 'unknown')}",
        ip_address=request.client.host if request.client else None,
    )
    return {
        "success": True,
        "message": f"任务「{task['name']}」已{'启用' if enabled else '禁用'}",
        "reload": reload_result,
    }


@router.post("/manage/monitoring/start")
async def monitoring_start(
    request: Request,
    account_id: str = Query(...),
    interval: int = Query(30, description="监控轮询间隔(秒)"),
    agent: dict = Depends(verify_agent_key),
    _: None = Depends(require_role(AgentRole.OPERATOR)),
):
    """启动交易监控"""
    from services.monitoring.service import get_trading_monitor

    # 验证账户
    db = get_db_manager()
    account = await db.fetchone(
        "SELECT account_id FROM accounts WHERE account_id = ? AND is_active = 1",
        (account_id,)
    )
    if not account:
        return {"success": False, "message": f"账户 {account_id} 不存在或未激活"}

    monitor = get_trading_monitor()
    result = await monitor.start_monitoring(account_id, interval)

    agent_id = agent.get("id", "")
    log_action(
        agent_id=agent_id, action="monitoring:start",
        account_id=account_id, resource_type="monitoring", resource_id=account_id,
        request_payload={"account_id": account_id, "interval": interval},
        response_summary=str(result),
        ip_address=request.client.host if request.client else None,
    )
    return result


@router.post("/manage/monitoring/stop")
async def monitoring_stop(
    request: Request,
    agent: dict = Depends(verify_agent_key),
    _: None = Depends(require_role(AgentRole.OPERATOR)),
):
    """停止交易监控"""
    from services.monitoring.service import get_trading_monitor

    monitor = get_trading_monitor()
    result = await monitor.stop_monitoring()

    agent_id = agent.get("id", "")
    log_action(
        agent_id=agent_id, action="monitoring:stop",
        account_id="", resource_type="monitoring", resource_id="",
        response_summary=str(result),
        ip_address=request.client.host if request.client else None,
    )
    return result


@router.post("/manage/screening/start")
async def screening_start(
    request: Request,
    account_id: str = Query(...),
    strategy_id: Optional[int] = Query(None, description="策略 ID，不传则扫描所有活跃策略"),
    interval: int = Query(60, description="选股轮询间隔(秒)"),
    agent: dict = Depends(verify_agent_key),
    _: None = Depends(require_role(AgentRole.OPERATOR)),
):
    """启动因子选股"""
    from services.screening.service import get_screening_service

    # 验证账户
    db = get_db_manager()
    account = await db.fetchone(
        "SELECT account_id FROM accounts WHERE account_id = ? AND is_active = 1",
        (account_id,)
    )
    if not account:
        return {"success": False, "message": f"账户 {account_id} 不存在或未激活"}

    # 如果指定了 strategy_id，验证策略存在
    if strategy_id:
        strategy = await db.fetchone(
            "SELECT id, name FROM strategies WHERE id = ? AND account_id = ?",
            (strategy_id, account_id)
        )
        if not strategy:
            return {"success": False, "message": f"策略 {strategy_id} 不存在或不属于该账户"}

    service = get_screening_service()
    result = await service.start_screening(account_id, strategy_id, interval)

    agent_id = agent.get("id", "")
    log_action(
        agent_id=agent_id, action="screening:start",
        account_id=account_id, resource_type="screening", resource_id=str(strategy_id or 0),
        request_payload={"account_id": account_id, "strategy_id": strategy_id, "interval": interval},
        response_summary=str(result),
        ip_address=request.client.host if request.client else None,
    )
    return result


@router.post("/manage/screening/stop")
async def screening_stop(
    request: Request,
    agent: dict = Depends(verify_agent_key),
    _: None = Depends(require_role(AgentRole.OPERATOR)),
):
    """停止因子选股"""
    from services.screening.service import get_screening_service

    service = get_screening_service()
    result = await service.stop_screening()

    agent_id = agent.get("id", "")
    log_action(
        agent_id=agent_id, action="screening:stop",
        account_id="", resource_type="screening", resource_id="",
        response_summary=str(result),
        ip_address=request.client.host if request.client else None,
    )
    return result


@router.post("/manage/strategy/execute")
async def strategy_execute(
    request: Request,
    strategy_id: int = Query(...),
    account_id: str = Query(...),
    agent: dict = Depends(verify_agent_key),
    _: None = Depends(require_role(AgentRole.OPERATOR)),
):
    """执行代码型策略（返回信号，不写入 watchlist）"""
    from services.strategy.engine import get_strategy_engine
    from services.common import technical_indicators
    from services.common.timezone import get_china_time
    from services.data.local_data_service import get_local_data_service, is_trading_hours

    db = get_db_manager()

    # 验证账户
    account = await db.fetchone(
        "SELECT account_id FROM accounts WHERE account_id = ? AND is_active = 1",
        (account_id,)
    )
    if not account:
        return {"success": False, "message": f"账户 {account_id} 不存在或未激活"}

    # 获取策略
    strategy = await db.fetchone(
        "SELECT * FROM strategies WHERE id = ? AND strategy_type = 'python'",
        (strategy_id,)
    )
    if not strategy:
        return {"success": False, "message": f"代码型策略 {strategy_id} 不存在"}

    # 获取候选股票
    code_scope = strategy.get("code_scope", "screening")
    if code_scope == "trading":
        stocks = await db.fetchall(
            "SELECT * FROM watchlist WHERE account_id = ? AND status = 'bought'",
            (account_id,)
        )
    else:
        group_id = strategy.get("target_scope") or 0
        stocks = await db.fetchall(
            "SELECT * FROM watchlist WHERE account_id = ? AND group_id = ? AND status IN ('pending', 'watching')",
            (account_id, group_id)
        )

    today = get_china_time().strftime("%Y-%m-%d")
    stock_codes = [s["stock_code"] for s in stocks]
    lds = get_local_data_service()

    # 构建数据获取函数
    def _get_kline(stock_code, limit=100, start_date=None):
        return lds.get_kline_data(stock_code, start_date=start_date, limit=limit)

    def _get_batch_kline(codes, limit=100):
        return lds.get_batch_kline(codes, limit=limit)

    def _get_factors(stock_code, date=None):
        return lds.get_daily_factors(stock_code, date or today)

    def _get_factors_batch(codes, date=None):
        return lds.get_daily_factors_batch(codes, date or today)

    def _get_market_data(stock_code):
        return None

    def _get_realtime_quote(stock_code):
        return None

    context = {
        "stocks": stocks,
        "account_id": account_id,
        "today": today,
        "indicators": technical_indicators,
        "get_kline": _get_kline,
        "get_batch_kline": _get_batch_kline,
        "get_factors": _get_factors,
        "get_factors_batch": _get_factors_batch,
        "get_kline_smart": lambda codes, lookback=100: {c: _get_kline(c, limit=lookback) for c in codes},
        "get_kline_spliced": lambda codes, lookback=100: {c: _get_kline(c, limit=lookback) for c in codes},
        "get_market_data": _get_market_data,
        "get_realtime_quote": _get_realtime_quote,
    }

    engine = get_strategy_engine()
    signals = engine.execute_strategy(strategy, context)

    agent_id = agent.get("id", "")
    log_action(
        agent_id=agent_id, action="strategy:execute",
        account_id=account_id, resource_type="strategy", resource_id=str(strategy_id),
        request_payload={"strategy_id": strategy_id, "account_id": account_id},
        response_summary=f"生成 {len(signals)} 个信号",
        ip_address=request.client.host if request.client else None,
    )
    return {"success": True, "strategy": strategy, "signals": signals, "count": len(signals)}


# ============== 确认流程端点 ==============

@router.get("/confirmations/pending")
async def list_pending_confirmations_endpoint(
    request: Request,
    agent: dict = Depends(verify_agent_key),
):
    """列出所有待处理的确认"""
    from services.agent.confirm import list_pending_confirmations as _list_pending

    results = await _list_pending()

    agent_id = agent.get("id", "")
    log_action(
        agent_id=agent_id, action="confirmation:list_pending",
        account_id="", resource_type="confirmation", resource_id="",
        response_summary=f"返回 {len(results)} 条待确认",
        ip_address=request.client.host if request.client else None,
    )
    return {"success": True, "count": len(results), "confirmations": results}


@router.post("/confirmations/{confirmation_id}/approve")
async def confirm_approve(
    request: Request,
    confirmation_id: str = Path(...),
    review_notes: Optional[str] = Body(None, embed=True),
    agent: dict = Depends(verify_agent_key),
):
    """批准一条确认"""
    from services.agent.confirm import approve_confirmation, get_confirmation

    # 验证记录存在
    record = await get_confirmation(confirmation_id)
    if not record:
        return {"success": False, "message": "确认记录不存在"}
    if record.get("status") != "pending":
        return {"success": False, "message": f"记录状态为 {record.get('status')}，无法批准"}

    reviewer = agent.get("id", "")
    ok = await approve_confirmation(confirmation_id, reviewer, review_notes)

    agent_id = agent.get("id", "")
    log_action(
        agent_id=agent_id, action="confirmation:approve",
        account_id=record.get("account_id", ""),
        resource_type="confirmation", resource_id=confirmation_id,
        response_summary="已批准" if ok else "批准失败（可能已被其他人处理）",
        ip_address=request.client.host if request.client else None,
    )
    return {"success": ok, "message": "已批准" if ok else "批准失败"}


@router.post("/confirmations/{confirmation_id}/reject")
async def confirm_reject(
    request: Request,
    confirmation_id: str = Path(...),
    review_notes: Optional[str] = Body(None, embed=True),
    agent: dict = Depends(verify_agent_key),
):
    """拒绝一条确认"""
    from services.agent.confirm import reject_confirmation, get_confirmation

    record = await get_confirmation(confirmation_id)
    if not record:
        return {"success": False, "message": "确认记录不存在"}
    if record.get("status") != "pending":
        return {"success": False, "message": f"记录状态为 {record.get('status')}，无法拒绝"}

    reviewer = agent.get("id", "")
    ok = await reject_confirmation(confirmation_id, reviewer, review_notes)

    agent_id = agent.get("id", "")
    log_action(
        agent_id=agent_id, action="confirmation:reject",
        account_id=record.get("account_id", ""),
        resource_type="confirmation", resource_id=confirmation_id,
        response_summary="已拒绝" if ok else "拒绝失败",
        ip_address=request.client.host if request.client else None,
    )
    return {"success": ok, "message": "已拒绝" if ok else "拒绝失败"}


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
