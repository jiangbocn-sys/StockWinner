"""
FastAPI 主应用
"""

# 在任何其他导入之前加载环境变量
from dotenv import load_dotenv
load_dotenv()  # 从 .env 文件加载环境变量

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from contextlib import asynccontextmanager
from typing import Optional
import asyncio
import os

from services.common.timezone import get_china_time
from services.common.database import get_db_manager, reset_db_manager
from services.common.account_manager import get_account_manager, reset_account_manager
from services.common.structured_logger import get_logger
from services.ui import dashboard, accounts, positions, trades, strategies, screening, monitoring, market_data, data_explorer, position_rules, factors, scheduler, notifications, trading_strategies, strategy_performance, data_service
from services.backtest.api import router as backtest_router
from services.strategy.api import router as strategy_v2_router
from services.account_management.api import router as account_management_router
from services.auth.api import router as auth_router
from services.llm.api import router as llm_router
from services.agent.api import register_agent_routers

from services._version import VERSION, set_start_time
_server_start_time = None

# 数据库迁移版本号 —— 每次新增迁移时递增
MIGRATION_VERSION = 6


@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期"""
    set_start_time()
    log = get_logger("core")

    # 启动时初始化
    log.log_event("startup", f"StockWinner v{VERSION} 启动中...")

    # 初始化数据库连接
    db_manager = get_db_manager()
    await db_manager.connect()
    log.log_event("db_connected", "数据库连接已建立")

    # 从数据库加载账户列表
    account_manager = get_account_manager()
    accounts = await account_manager.list_accounts()
    active_accounts = [a for a in accounts if a.get('is_active')]
    active_ids = ', '.join([a['account_id'] for a in active_accounts])
    log.log_event("accounts_loaded", f"已加载 {len(active_accounts)} 个激活账户：{active_ids}", count=len(active_accounts))

    # 启动调度服务
    from services.common.scheduler_service import start_scheduler, get_scheduler, _set_fastapi_loop
    start_scheduler()
    log.log_event("scheduler_started", "调度服务已启动")

    # 设置 FastAPI 事件循环引用
    loop = asyncio.get_running_loop()
    _set_fastapi_loop(loop)

    # 启动时检查周K线覆盖情况
    try:
        scheduler = get_scheduler()
        need, msg = scheduler._check_weekly_kline_coverage()
        if need:
            log.log_event("weekly_kline_check", f"周K线数据不完整: {msg}", incomplete=True)
        else:
            log.log_event("weekly_kline_check", f"周K线数据已覆盖: {msg}")
    except Exception as e:
        log.error("weekly_kline", f"启动时周K线检查失败: {e}")

    # 启动时自动启动交易监控
    try:
        from services.monitoring.service import get_trading_monitor
        from services.trading.trading_hours import can_trade, is_today_trading_day

        if is_today_trading_day() and can_trade():
            monitor = get_trading_monitor()
            result = await monitor.start_monitoring(interval=30)
            log.log_event("monitor_autostart", f"交易监控已启动: {result.get('message', '')}")
        else:
            log.log_event("monitor_autostart", "不在交易时段或非交易日，跳过自动启动监控")
    except Exception as e:
        log.error("monitor_autostart", f"启动时自动监控检测失败: {e}")

    # 数据库迁移框架：按版本号执行，避免每次启动重复执行
    try:
        await db_manager.execute("""
            CREATE TABLE IF NOT EXISTS migration_version (
                version INTEGER PRIMARY KEY,
                applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        current_version = await db_manager.fetchone("SELECT MAX(version) as v FROM migration_version")
        applied_v = current_version["v"] if current_version and current_version.get("v") else 0
    except Exception as e:
        log.error("db_migration", f"数据库迁移: 版本检查失败 ({e})，跳过迁移")
        applied_v = 9999  # 跳过所有迁移

    async def run_migration(v: int, desc: str, sql_blocks: list):
        """执行指定版本的迁移"""
        if v <= applied_v:
            return
        log.log_event("db_migration_start", desc, version=v)
        for sql in sql_blocks:
            try:
                await db_manager.execute(sql)
            except Exception:
                pass  # 列/表已存在
        try:
            await db_manager.execute("INSERT INTO migration_version (version) VALUES (?)", (v,))
            log.log_event("db_migration_complete", desc, version=v)
        except Exception as e:
            log.error("db_migration", f"版本记录写入失败 ({e})", version=v)

    # v1: 所有现有迁移（首次运行时执行，之后跳过）
    await run_migration(1, "初始迁移（表结构扩展 + 新表创建）", [
        # watchlist 扩展
        "ALTER TABLE watchlist ADD COLUMN source_type TEXT DEFAULT 'screening'",
        "ALTER TABLE watchlist ADD COLUMN group_id INTEGER",
        "ALTER TABLE watchlist ADD COLUMN current_price REAL DEFAULT 0",
        "ALTER TABLE watchlist ADD COLUMN bought INTEGER DEFAULT 0",
        "ALTER TABLE watchlist ADD COLUMN buy_trade_id INTEGER",
        # strategies 扩展
        "ALTER TABLE strategies ADD COLUMN code TEXT",
        "ALTER TABLE strategies ADD COLUMN code_type TEXT DEFAULT 'config'",
        "ALTER TABLE strategies ADD COLUMN target_scope TEXT DEFAULT 'group'",
        "ALTER TABLE strategies ADD COLUMN function_name TEXT DEFAULT 'run'",
        "ALTER TABLE strategies ADD COLUMN code_scope TEXT DEFAULT 'screening'",
        # trade_records 扩展
        "ALTER TABLE trade_records ADD COLUMN trigger_source TEXT",
        "ALTER TABLE trade_records ADD COLUMN notification_sent INTEGER DEFAULT 0",
        "ALTER TABLE trade_records ADD COLUMN strategy_id INTEGER",
        "ALTER TABLE trade_records ADD COLUMN signal_id INTEGER",
        # accounts 扩展
        "ALTER TABLE accounts ADD COLUMN commission_rate REAL DEFAULT 0.0003",
        "ALTER TABLE accounts ADD COLUMN stamp_tax REAL DEFAULT 0.0005",
        "ALTER TABLE accounts ADD COLUMN transfer_fee REAL DEFAULT 0.00002",
        "ALTER TABLE accounts ADD COLUMN min_commission REAL DEFAULT 5.0",
        "ALTER TABLE accounts ADD COLUMN is_mock INTEGER DEFAULT 1",
        "ALTER TABLE accounts ADD COLUMN trade_mode TEXT DEFAULT 'mock'",
        # trading_strategies 扩展
        "ALTER TABLE trading_strategies ADD COLUMN strategy_type TEXT DEFAULT 'fixed'",
        "ALTER TABLE trading_strategies ADD COLUMN config TEXT DEFAULT '{}'",
        # stock_positions 扩展
        "ALTER TABLE stock_positions ADD COLUMN highest_price REAL DEFAULT 0",
        # strategy_tasks 扩展
        "ALTER TABLE strategy_tasks ADD COLUMN task_type TEXT DEFAULT 'strategy'",
        "ALTER TABLE strategy_tasks ADD COLUMN module TEXT DEFAULT NULL",
        # 创建新表
        """CREATE TABLE IF NOT EXISTS candidate_groups (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_id TEXT NOT NULL,
            name TEXT NOT NULL,
            group_type TEXT NOT NULL DEFAULT 'manual',
            screening_strategy_id INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (screening_strategy_id) REFERENCES strategies(id)
        )""",
        """CREATE TABLE IF NOT EXISTS strategy_tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            strategy_id INTEGER NOT NULL,
            group_id INTEGER NOT NULL,
            account_id TEXT NOT NULL,
            cron_expression TEXT NOT NULL,
            enabled INTEGER DEFAULT 1,
            last_run_at TIMESTAMP,
            last_status TEXT,
            last_output TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (strategy_id) REFERENCES strategies(id),
            FOREIGN KEY (group_id) REFERENCES candidate_groups(id)
        )""",
        """CREATE TABLE IF NOT EXISTS notification_config (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_id TEXT NOT NULL,
            channel TEXT NOT NULL DEFAULT 'feishu',
            webhook_url TEXT NOT NULL,
            enabled INTEGER DEFAULT 1,
            notify_on_trade INTEGER DEFAULT 1,
            notify_on_signal INTEGER DEFAULT 1,
            notify_on_task INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""",
        """CREATE TABLE IF NOT EXISTS notification_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_id TEXT NOT NULL,
            channel TEXT NOT NULL,
            event_type TEXT NOT NULL,
            title TEXT,
            content TEXT,
            status TEXT DEFAULT 'pending',
            response TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""",
        """CREATE TABLE IF NOT EXISTS trading_strategy_config (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_id TEXT NOT NULL,
            name TEXT NOT NULL,
            strategy_type TEXT NOT NULL,
            conditions TEXT NOT NULL,
            action TEXT NOT NULL,
            target_stocks TEXT,
            enabled INTEGER DEFAULT 1,
            cooldown_seconds INTEGER DEFAULT 300,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""",
        """CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_id TEXT NOT NULL,
            stock_code TEXT NOT NULL,
            stock_name TEXT,
            trade_type TEXT NOT NULL,
            order_price REAL NOT NULL,
            order_quantity INTEGER NOT NULL,
            filled_quantity INTEGER DEFAULT 0,
            filled_amount REAL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'pending',
            order_no TEXT NOT NULL,
            broker_order_id TEXT,
            trigger_source TEXT,
            stop_loss_price REAL,
            take_profit_price REAL,
            reject_reason TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""",
        """CREATE TABLE IF NOT EXISTS llm_config (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_id TEXT NOT NULL,
            provider TEXT DEFAULT 'openai',
            base_url TEXT NOT NULL,
            api_key TEXT NOT NULL,
            model_name TEXT NOT NULL,
            enabled INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""",
    ])

    # 以下迁移逻辑需要数据操作（非纯 DDL），在 v1 之后单独执行一次
    # 创建 candidate_groups 索引
    try:
        await db_manager.execute("CREATE INDEX IF NOT EXISTS idx_candidate_groups_account ON candidate_groups(account_id)")
    except Exception:
        pass
    # 创建 strategy_tasks 索引
    try:
        await db_manager.execute("CREATE INDEX IF NOT EXISTS idx_strategy_tasks_account ON strategy_tasks(account_id)")
        await db_manager.execute("CREATE INDEX IF NOT EXISTS idx_strategy_tasks_enabled ON strategy_tasks(enabled)")
    except Exception:
        pass
    # 创建 notification_config 索引
    try:
        await db_manager.execute("CREATE INDEX IF NOT EXISTS idx_notification_config_account ON notification_config(account_id)")
    except Exception:
        pass
    # 创建 notification_history 索引
    try:
        await db_manager.execute("CREATE INDEX IF NOT EXISTS idx_notification_history_account ON notification_history(account_id)")
        await db_manager.execute("CREATE INDEX IF NOT EXISTS idx_notification_history_created ON notification_history(created_at)")
    except Exception:
        pass
    # 创建 trading_strategy_config 索引
    try:
        await db_manager.execute("CREATE INDEX IF NOT EXISTS idx_trading_strategy_account ON trading_strategy_config(account_id)")
    except Exception:
        pass
    # 创建 orders 索引
    try:
        await db_manager.execute("CREATE INDEX IF NOT EXISTS idx_orders_account ON orders(account_id)")
        await db_manager.execute("CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status)")
    except Exception:
        pass
    # 创建 llm_config 唯一索引
    try:
        await db_manager.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_llm_config_account ON llm_config(account_id)")
    except Exception:
        pass

    # 归集未分组 watchlist 记录（只需执行一次）
    if applied_v < 1:
        try:
            accounts_with_ungrouped = await db_manager.fetchall(
                "SELECT DISTINCT account_id FROM watchlist WHERE group_id IS NULL"
            )
            for row in accounts_with_ungrouped:
                aid = row['account_id']
                existing = await db_manager.fetchone(
                    "SELECT id FROM candidate_groups WHERE account_id = ? AND name = '未分组'",
                    (aid,)
                )
                if not existing:
                    group_id = await db_manager.insert("candidate_groups", {
                        "account_id": aid,
                        "name": "未分组",
                        "group_type": "manual",
                        "screening_strategy_id": None,
                    })
                else:
                    group_id = existing['id']
                await db_manager.execute(
                    "UPDATE watchlist SET group_id = ? WHERE group_id IS NULL AND account_id = ?",
                    (group_id, aid)
                )
            log.log_event("migration_ungrouped", "「未分组」默认组已创建，未分组记录已归集")
        except Exception as e:
            log.error("db_migration", f"未分组归集跳过 ({e})")

    # 迁移旧版 LLM 配置（只需执行一次）
    if applied_v < 1:
        try:
            from pathlib import Path as _Path
            import json as _json
            legacy_path = _Path(__file__).parent.parent / "config" / "llm.json"
            if legacy_path.exists():
                existing_count = await db_manager.fetchone("SELECT COUNT(*) as cnt FROM llm_config")
                if existing_count and existing_count.get("cnt", 0) == 0:
                    with open(legacy_path, 'r') as f:
                        legacy = _json.load(f)
                    if legacy.get("api_key"):
                        await db_manager.insert("llm_config", {
                            "account_id": "SYSTEM",
                            "provider": legacy.get("provider", "custom"),
                            "base_url": legacy.get("base_url", ""),
                            "api_key": legacy.get("api_key", ""),
                            "model_name": legacy.get("model", ""),
                            "enabled": 1,
                        })
                        log.log_event("migration_llm", "已从 config/llm.json 迁移系统级配置")
        except Exception as e:
            log.error("db_migration", f"LLM 旧配置迁移失败 ({e})")

    # 清理遗留 running 状态（每次启动执行，不纳入版本控制）
    try:
        result = await db_manager.execute(
            "UPDATE strategy_tasks SET last_status = 'error', last_output = '{\"error\": \"服务重启，任务中断\"}', updated_at = ? WHERE last_status = 'running'",
            (get_china_time().isoformat(),)
        )
        reset_count = getattr(result, 'rowcount', 0) if hasattr(result, 'rowcount') else 0
        if reset_count > 0:
            log.log_event("migration_running_cleanup", f"已清理 {reset_count} 个遗留 running 状态", count=reset_count)
    except Exception as e:
        log.error("db_migration", f"清理 running 状态跳过 ({e})")

    # 恢复遗留未完成订单（每次启动执行）
    try:
        result = await db_manager.execute(
            "UPDATE orders SET status = 'cancelled', reject_reason = '服务重启，订单已失效', updated_at = ? WHERE status IN ('pending', 'submitted')",
            (get_china_time().isoformat(),)
        )
        cancelled_count = getattr(result, 'rowcount', 0) if hasattr(result, 'rowcount') else 0
        if cancelled_count > 0:
            log.log_event("migration_order_cleanup", f"已取消 {cancelled_count} 个遗留未完成订单", count=cancelled_count)
    except Exception:
        pass

    # T+1 解冻（每次启动执行，只解冻今天之前买入的持仓）
    try:
        from services.trading.position_manager import get_position_manager
        accounts = await db_manager.fetchall("SELECT DISTINCT account_id FROM stock_positions")
        today = get_china_time().strftime("%Y-%m-%d")
        total_thawed = 0
        total_reset = 0
        for row in accounts:
            pm = get_position_manager(row["account_id"])
            count = await pm.unfreeze_positions()
            total_thawed += count
            # 重置 watchlist pending→watching，排除今天有买入的股票
            result = await db_manager.execute(
                """UPDATE watchlist SET status = 'watching'
                   WHERE account_id = ? AND status = 'pending'
                   AND stock_code NOT IN (
                       SELECT stock_code FROM trade_records
                       WHERE account_id = ? AND trade_type = 'buy'
                         AND date(trade_time) = ?
                   )""",
                (row["account_id"], row["account_id"], today)
            )
            total_reset += getattr(result, 'rowcount', 0) if hasattr(result, 'rowcount') else 0
        if total_thawed > 0 or total_reset > 0:
            log.log_event("migration_t1_thaw", f"已解冻 {total_thawed} 只持仓，{total_reset} 个 pending 已重置为 watching", count=total_thawed)
    except Exception as e:
        log.error("db_migration", f"T+1 解冻跳过 ({e})")

    # 扫描任务插件
    try:
        from services.tasks import scan_tasks
        scan_tasks()
        log.log_event("migration_task_scan", "任务插件扫描完成")
    except Exception as e:
        log.error("db_migration", f"任务插件扫描失败: {e}")

    # 创建内置任务（只需执行一次）
    if applied_v < 1:
        builtin_defaults = [
            {"task_type": "builtin", "module": "kline_check", "account_id": "SYSTEM", "cron_expression": "0 1 * * *", "enabled": 1},
            {"task_type": "builtin", "module": "monthly_factors", "account_id": "SYSTEM", "cron_expression": "0 1 5 * *", "enabled": 1},
            {"task_type": "builtin", "module": "weekly_kline", "account_id": "SYSTEM", "cron_expression": "0 2 * * 6", "enabled": 1},
            {"task_type": "builtin", "module": "industry_download", "account_id": "SYSTEM", "cron_expression": "0 3 * * 1-5", "enabled": 0},
            {"task_type": "builtin", "module": "post_market_analysis", "account_id": "SYSTEM", "cron_expression": "30 15 * * 1-5", "enabled": 1},
        ]
        for t in builtin_defaults:
            existing = await db_manager.fetchone(
                "SELECT id FROM strategy_tasks WHERE task_type = 'builtin' AND module = ?",
                (t["module"],)
            )
            if not existing:
                await db_manager.insert("strategy_tasks", {
                    "task_type": "builtin",
                    "module": t["module"],
                    "strategy_id": None,
                    "group_id": None,
                    "account_id": t["account_id"],
                    "cron_expression": t["cron_expression"],
                    "enabled": t["enabled"],
                })
                log.log_event("migration_builtin_task", f"内置任务 {t['module']} 已创建", module=t['module'])

    # v2: watchlist 复合索引 + 现价字段优化（2026-05-08）
    await run_migration(2, "watchlist 复合索引", [
        "CREATE INDEX IF NOT EXISTS idx_watchlist_code_status_updated ON watchlist(stock_code, status, updated_at)",
        "CREATE INDEX IF NOT EXISTS idx_watchlist_account_status ON watchlist(account_id, status)",
        "CREATE INDEX IF NOT EXISTS idx_watchlist_group_status ON watchlist(group_id, status)",
    ])
    try:
        from services.common.kronos_service import load_kronos_on_startup
        load_kronos_on_startup()
    except Exception as e:
        log.error("kronos", f"Kronos 预加载跳过: {e}")

    # v3: Agent 协作框架 — Agent 账户表 + 审计日志表 + 确认表（2026-05-10）
    await run_migration(3, "Agent 协作框架", [
        """CREATE TABLE IF NOT EXISTS agent_accounts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            agent_id TEXT UNIQUE NOT NULL,
            user_id TEXT NOT NULL DEFAULT '',
            name TEXT NOT NULL,
            agent_type TEXT DEFAULT 'generic',
            api_key_hash TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT 'viewer',
            allowed_account_ids TEXT DEFAULT '["*"]',
            allowed_permissions TEXT DEFAULT '[]',
            denied_permissions TEXT DEFAULT '[]',
            rate_limit_per_min INTEGER DEFAULT 60,
            enabled INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_used_at TIMESTAMP
        )""",
        "CREATE INDEX IF NOT EXISTS idx_agent_accounts_user ON agent_accounts(user_id)",
        """CREATE TABLE IF NOT EXISTS agent_audit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            agent_id TEXT NOT NULL,
            user_id TEXT NOT NULL DEFAULT '',
            action TEXT NOT NULL,
            resource_type TEXT,
            resource_id TEXT,
            account_id TEXT,
            status TEXT NOT NULL,
            request_payload TEXT,
            response_summary TEXT,
            risk_level TEXT NOT NULL,
            confirmation_id TEXT,
            ip_address TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""",
        "CREATE INDEX IF NOT EXISTS idx_agent_audit_agent ON agent_audit_log(agent_id)",
        "CREATE INDEX IF NOT EXISTS idx_agent_audit_created ON agent_audit_log(created_at)",
        "CREATE INDEX IF NOT EXISTS idx_agent_audit_user ON agent_audit_log(user_id)",
        """CREATE TABLE IF NOT EXISTS agent_confirmations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            confirmation_id TEXT UNIQUE NOT NULL,
            agent_id TEXT NOT NULL,
            action TEXT NOT NULL,
            account_id TEXT,
            request_payload TEXT,
            risk_level TEXT NOT NULL,
            status TEXT DEFAULT 'pending',
            reviewed_by TEXT,
            review_notes TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            expires_at TIMESTAMP
        )""",
    ])

    # v4: 禁止 account_id 变更（2026-05-13）
    await run_migration(4, "account_id 不可变触发器", [
        """CREATE TRIGGER IF NOT EXISTS prevent_account_id_update
        BEFORE UPDATE OF account_id ON accounts
        BEGIN
            SELECT RAISE(ABORT, 'account_id 创建后不可变更');
        END;"""
    ])

    # v5: 信号表新增 order_type 字段（2026-05-15）
    await run_migration(5, "trading_signals 新增 order_type", [
        """ALTER TABLE trading_signals ADD COLUMN order_type TEXT DEFAULT 'day'""",
        """CREATE INDEX IF NOT EXISTS idx_signals_order_type ON trading_signals(account_id, status, order_type)""",
    ])

    # v6: 回测系统（2026-05-16）
    await run_migration(6, "回测系统四张新表", [
        """CREATE TABLE IF NOT EXISTS backtest_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_id TEXT NOT NULL,
            strategy_id INTEGER,
            name TEXT NOT NULL,
            mode TEXT NOT NULL DEFAULT 'simulated',
            start_date TEXT NOT NULL,
            end_date TEXT NOT NULL,
            initial_capital REAL NOT NULL DEFAULT 1000000,
            commission_rate REAL NOT NULL DEFAULT 0.0003,
            stamp_tax REAL NOT NULL DEFAULT 0.0005,
            transfer_fee REAL NOT NULL DEFAULT 0.00002,
            min_commission REAL NOT NULL DEFAULT 5.0,
            slippage_pct REAL NOT NULL DEFAULT 0.0,
            max_total_position_pct REAL DEFAULT 0.80,
            max_single_position_pct REAL DEFAULT 0.15,
            cash_reserve_pct REAL DEFAULT 0.10,
            config TEXT DEFAULT '{}',
            status TEXT NOT NULL DEFAULT 'pending',
            progress REAL DEFAULT 0,
            error_message TEXT,
            data_gap_report TEXT,
            result_summary TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            started_at TIMESTAMP,
            completed_at TIMESTAMP
        )""",
        """CREATE INDEX IF NOT EXISTS idx_backtest_runs_account ON backtest_runs(account_id)""",
        """CREATE INDEX IF NOT EXISTS idx_backtest_runs_status ON backtest_runs(status)""",
        """CREATE TABLE IF NOT EXISTS backtest_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            backtest_run_id INTEGER NOT NULL,
            stock_code TEXT NOT NULL,
            stock_name TEXT,
            buy_date TEXT NOT NULL,
            buy_price REAL NOT NULL,
            buy_quantity INTEGER NOT NULL,
            buy_commission REAL NOT NULL,
            sell_date TEXT,
            sell_price REAL,
            sell_commission REAL,
            sell_reason TEXT,
            pnl REAL,
            pnl_pct REAL,
            holding_days INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""",
        """CREATE INDEX IF NOT EXISTS idx_backtest_trades_run ON backtest_trades(backtest_run_id)""",
        """CREATE INDEX IF NOT EXISTS idx_backtest_trades_stock ON backtest_trades(backtest_run_id, stock_code)""",
        """CREATE TABLE IF NOT EXISTS backtest_daily_nav (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            backtest_run_id INTEGER NOT NULL,
            trade_date TEXT NOT NULL,
            nav REAL NOT NULL,
            total_value REAL NOT NULL,
            cash REAL NOT NULL,
            positions_value REAL NOT NULL,
            position_count INTEGER DEFAULT 0,
            drawdown REAL DEFAULT 0,
            max_drawdown REAL DEFAULT 0,
            daily_return REAL DEFAULT 0
        )""",
        """CREATE UNIQUE INDEX IF NOT EXISTS idx_backtest_daily_nav_run_date ON backtest_daily_nav(backtest_run_id, trade_date)""",
        """CREATE TABLE IF NOT EXISTS backtest_daily_positions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            backtest_run_id INTEGER NOT NULL,
            trade_date TEXT NOT NULL,
            stock_code TEXT NOT NULL,
            stock_name TEXT,
            quantity INTEGER NOT NULL,
            avg_cost REAL NOT NULL,
            close_price REAL NOT NULL,
            market_value REAL NOT NULL,
            unrealized_pnl REAL DEFAULT 0
        )""",
        """CREATE INDEX IF NOT EXISTS idx_backtest_daily_positions_run ON backtest_daily_positions(backtest_run_id, trade_date)""",
    ])

    yield

    # 关闭时清理
    log.log_event("shutdown", "StockWinner 关闭中...")

    # 停止交易监控
    try:
        from services.monitoring.service import get_trading_monitor
        from services.common.scheduler_service import _set_fastapi_loop
        monitor = get_trading_monitor()
        if monitor._running:
            await monitor.stop_monitoring()
            log.log_event("monitor_stopped", "交易监控已停止")
    except Exception as e:
        log.error("shutdown", f"停止监控失败: {e}")

    # 清除事件循环引用
    _set_fastapi_loop(None)

    from services.common.scheduler_service import stop_scheduler
    stop_scheduler()

    # 断开 SDK 连接，清理 gateway 缓存
    try:
        from services.common.sdk_manager import get_sdk_manager
        get_sdk_manager().disconnect()
        log.log_event("sdk_disconnected", "SDK 连接已断开")
    except Exception as e:
        log.error("shutdown", f"断开 SDK 连接失败: {e}")

    try:
        from services.trading.gateway import clear_gateway_cache
        clear_gateway_cache()
    except Exception:
        pass

    await db_manager.close()
    reset_db_manager()
    reset_account_manager()


# 创建 FastAPI 应用
app = FastAPI(
    title="StockWinner",
    description=f"智能股票交易系统 v{VERSION} - 调度服务增强",
    version=VERSION,
    lifespan=lifespan
)

# 配置 CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 生产环境应限制具体域名
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 请求计时中间件（在 CORS 之后、业务中间件之前）
from services.common.request_logging_middleware import RequestLoggingMiddleware
app.add_middleware(RequestLoggingMiddleware)


# ============================================================
# UI 端点认证中间件
# 所有 /api/v1/ui/ 路径的请求必须携带有效的 X-Auth-Token 或 Authorization: Bearer
# 白名单：/api/v1/health、/、静态文件 不受限制
# Agent 请求（带 X-Agent-Key）不受此中间件影响
# ============================================================

_UI_AUTH_WHITELIST = {"/api/v1/health", "/", "/docs", "/openapi.json", "/redoc"}

@app.middleware("http")
async def ui_token_middleware(request: Request, call_next):
    path = request.url.path

    # 白名单路径、静态文件、/ui/ 前缀、监控状态端点（系统服务无需 token）
    if path in _UI_AUTH_WHITELIST or path.startswith("/ui/") or path.startswith("/static/"):
        return await call_next(request)

    if path.startswith("/api/v1/ui/") and "/monitoring/status" in path:
        return await call_next(request)

    # /api/v1/ui/ 路径需要 token 认证
    if path.startswith("/api/v1/ui/"):
        # 优先检查 agent 请求（X-Agent-Key 或 Authorization: Bearer sk-agent-），由 agent 中间件处理
        if request.headers.get("X-Agent-Key"):
            return await call_next(request)
        auth_check = request.headers.get("Authorization", "")
        if auth_check.startswith("Bearer ") and auth_check.startswith("Bearer sk-agent-"):
            return await call_next(request)

        # 获取 token：支持 X-Auth-Token 或 Authorization: Bearer
        token = request.headers.get("X-Auth-Token")
        if not token:
            auth_header = request.headers.get("Authorization", "")
            if auth_header.startswith("Bearer "):
                token = auth_header[7:]

        if not token:
            return JSONResponse(status_code=401, content={"success": False, "message": "缺少认证 token"})

        # 验证 token
        from services.auth.service import get_auth_service
        auth_service = get_auth_service()
        account = auth_service.validate_token(token)

        if not account:
            return JSONResponse(status_code=401, content={"success": False, "message": "认证失败或会话已过期"})

        # 将账户信息写入 request.state，供下游端点使用
        request.state.auth_token = token
        request.state.account_id = account.get("account_id", "")
        request.state.account_name = account.get("name", "")

    return await call_next(request)


# ============================================================
# Agent 安全约束中间件
# 任何携带 X-Agent-Key 的请求（无论走哪个路径）都必须通过 Agent 认证 + 权限校验 + 审计
# 不携带该 header 的请求（浏览器/UI）完全不受影响
# ============================================================

# Agent 路径 → 权限映射：将 URL 路径 + HTTP 方法映射到所需权限
# 仅对写操作（POST/PUT/DELETE/PATCH）进行权限检查，GET 由 query:* 覆盖
_AGENT_ACTION_PERMS: dict = {
    # 手动下单 / 信号执行
    ("POST", "manual-order/submit"): "trading:execute",
    ("POST", "signals/*/execute"): "trading:execute",
    ("POST", "signals/*/cancel"): "trading:execute",
    ("POST", "signals/clear"): "trading:execute",
    # 监控服务启停
    ("POST", "monitoring/start"): "monitoring:start",
    ("POST", "monitoring/stop"): "monitoring:stop",
    # 调度/任务管理
    ("POST", "scheduler/*"): "scheduler:start",
    ("PUT", "scheduler/*"): "scheduler:start",
    ("DELETE", "scheduler/*"): "scheduler:stop",
    # 策略管理（写操作）
    ("POST", "strategies"): "strategy:create",
    ("PUT", "strategies/*"): "strategy:update",
    ("DELETE", "strategies/*"): "strategy:delete",
    ("POST", "strategies/*/execute"): "strategy:execute",
    # 选股任务
    ("POST", "screening/*"): "screening:create",
    # 账户管理
    ("PUT", "accounts/*"): "account:write",
    ("POST", "accounts/*"): "account:write",
    # 持仓调整规则
    ("POST", "position-rules/*"): "account:write",
    ("PUT", "position-rules/*"): "account:write",
    # 通知配置
    ("POST", "notifications/webhook"): "system:config",
    ("DELETE", "notifications/webhook"): "system:config",
    # LLM 配置
    ("POST", "llm/*"): "system:config",
    ("PUT", "llm/*"): "system:config",
    ("DELETE", "llm/*"): "system:config",
    # Agent 管理
    ("POST", "auth/agent/*"): "agent:manage",
    ("DELETE", "auth/agent/*"): "agent:manage",
    ("PUT", "auth/agent/*"): "agent:manage",
}


def _match_agent_action(method: str, path: str) -> Optional[str]:
    """根据方法和路径匹配所需权限"""
    # 先尝试精确匹配
    key = (method, path)
    if key in _AGENT_ACTION_PERMS:
        return _AGENT_ACTION_PERMS[key]
    # 通配符匹配
    for (m, p), perm in _AGENT_ACTION_PERMS.items():
        if m != method:
            continue
        if p.endswith("*"):
            prefix = p[:-1]  # 去掉末尾 *
            if path.startswith(prefix):
                return perm
        elif p == path:
            return perm
    return None


@app.middleware("http")
async def agent_security_middleware(request: Request, call_next):
    # 兼容 X-Agent-Key 和 Authorization: Bearer sk-agent-
    agent_key = request.headers.get("X-Agent-Key")
    if not agent_key:
        auth_header = request.headers.get("Authorization", "")
        if auth_header.startswith("Bearer sk-agent-"):
            agent_key = auth_header[7:]
    if not agent_key:
        return await call_next(request)

    # 携带了 X-Agent-Key → 必须走 Agent 认证
    from services.agent.models import hash_api_key, get_effective_permissions, ROLE_RATE_LIMITS, has_permission
    import json as _json

    key_hash = hash_api_key(agent_key)
    db = get_db_manager()
    agent = await db.fetchone(
        "SELECT * FROM agent_accounts WHERE api_key_hash = ? AND enabled = 1",
        (key_hash,)
    )

    if not agent:
        return JSONResponse(status_code=401, content={"success": False, "message": "无效的 Agent API Key"})

    # 更新 last_used_at
    now = get_china_time().strftime("%Y-%m-%dT%H:%M:%S")
    try:
        await db.execute("UPDATE agent_accounts SET last_used_at = ? WHERE agent_id = ?", (now, agent["agent_id"]))
    except Exception:
        pass

    # 解析权限
    allowed_perms = None
    denied_perms = None
    try:
        if agent.get("allowed_permissions"):
            allowed_perms = _json.loads(agent["allowed_permissions"])
        if agent.get("denied_permissions"):
            denied_perms = _json.loads(agent["denied_permissions"])
    except Exception:
        pass

    effective_perms = get_effective_permissions(agent["role"], allowed_perms, denied_perms)
    try:
        allowed_accounts = _json.loads(agent.get("allowed_account_ids", '["*"]'))
    except Exception:
        allowed_accounts = ["*"]

    # 写入 request.state
    request.state.agent_id = agent["agent_id"]
    request.state.user_id = agent.get("user_id", "")
    request.state.agent_name = agent["name"]
    request.state.agent_type = agent.get("agent_type", "generic")
    request.state.agent_role = agent["role"]
    request.state.agent_permissions = effective_perms
    request.state.agent_allowed_accounts = allowed_accounts
    request.state.agent_rate_limit = agent.get("rate_limit_per_min") or ROLE_RATE_LIMITS.get(agent["role"], 60)
    request.state.is_agent_request = True

    # 权限检查（仅写操作）
    method = request.method
    if method in ("POST", "PUT", "DELETE", "PATCH"):
        path = request.url.path.lstrip("/api/v1/")
        required_perm = _match_agent_action(method, path)
        if required_perm and not has_permission(effective_perms, required_perm):
            return JSONResponse(status_code=403, content={"success": False, "message": f"权限不足：需要 '{required_perm}' 权限"})

    # 限速检查
    from services.agent.middleware import check_rate_limit
    try:
        check_rate_limit(agent["agent_id"], request.state.agent_rate_limit)
    except Exception as e:
        if "429" in str(e):
            return JSONResponse(status_code=429, content={"success": False, "message": "请求过于频繁，请稍后重试"})

    # 放行请求
    response = await call_next(request)

    # 写审计日志（仅写操作路径）
    method = request.method
    path = request.url.path
    if method in ("POST", "PUT", "DELETE", "PATCH"):
        try:
            from services.agent.audit import log_action
            await log_action(
                agent_id=agent["agent_id"],
                user_id=agent.get("user_id", ""),
                action=f"middleware.{method.lower()}.{path.rstrip('/').split('/')[-1]}",
                risk_level="medium",
                account_id=agent.get("user_id", ""),
                request_payload={"method": method, "path": path},
                ip_address=request.client.host if request.client else None,
            )
        except Exception:
            pass

    return response


# 全局异常处理
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """全局异常处理"""
    log = get_logger("core")
    log.error("exception", f"{request.method} {request.url}", context={"error": str(exc)})
    return JSONResponse(
        status_code=500,
        content={"success": False, "message": "服务器内部错误"}
    )


# 注册路由
app.include_router(dashboard.router)
app.include_router(accounts.router)
app.include_router(positions.router)
app.include_router(trades.router)
app.include_router(strategies.router)
app.include_router(screening.router)
app.include_router(monitoring.router)
app.include_router(market_data.router)  # 市场行情数据 API
app.include_router(data_explorer.router)  # 通用数据浏览 API
app.include_router(position_rules.router)  # 持仓调整规则 API
app.include_router(factors.router)  # 因子计算 API
app.include_router(scheduler.router)  # 调度服务 API
app.include_router(trading_strategies.router)  # 交易策略 API（个股+条件触发）
app.include_router(strategy_v2_router)  # 策略管理 API v2
# 添加账户管理API路由
app.include_router(account_management_router)
app.include_router(auth_router)
app.include_router(llm_router)  # LLM 配置路由
app.include_router(notifications.router)  # 通知 API 路由
app.include_router(strategy_performance.router)  # 策略效能评估 API 路由
app.include_router(data_service.router)  # 扩展数据服务 API（指数/行业/财报/龙虎榜/两融等）
app.include_router(backtest_router)  # 回测系统 API 路由

# Agent API 路由（独立路径，不影响 UI）
register_agent_routers(app)

# 挂载前端静态文件（支持 Vue Router history mode）
frontend_dist = os.environ.get("FRONTEND_DIST", "/home/bobo/StockWinner/frontend/dist")
if os.path.exists(frontend_dist):
    app.mount("/ui/assets", StaticFiles(directory=os.path.join(frontend_dist, "assets")), name="ui-assets")

    # 提供 favicon.svg
    @app.get("/ui/favicon.svg", include_in_schema=False)
    async def serve_favicon():
        return FileResponse(os.path.join(frontend_dist, "favicon.svg"))

    # Vue Router history mode fallback — 所有 /ui/* 非静态路径返回 index.html
    @app.get("/ui/{full_path:path}", include_in_schema=False)
    async def serve_spa(full_path: str):
        index_path = os.path.join(frontend_dist, "index.html")
        if os.path.exists(index_path):
            return FileResponse(index_path)
        return JSONResponse(status_code=404, content={"message": "Frontend not found"})


@app.get("/")
async def root():
    """根路径"""
    return {
        "service": "StockWinner",
        "version": VERSION,
        "status": "running"
    }


@app.get("/api/v1/health")
async def health_check():
    """健康检查（不带账户参数）"""
    return {
        "status": "healthy",
        "version": VERSION
    }
