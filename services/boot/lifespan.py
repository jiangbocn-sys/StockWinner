"""
应用生命周期管理 — 启动/关闭流程提取。
"""

from contextlib import asynccontextmanager
from fastapi import FastAPI
import json


def create_lifespan():
    """返回 asynccontextmanager，包含 startup/shutdown 逻辑"""
    from services._version import VERSION, set_start_time
    from services.common.database import get_db_manager, reset_db_manager
    from services.common.account_manager import get_account_manager, reset_account_manager
    from services.common.structured_logger import get_logger
    from services.common.timezone import get_china_time
    import asyncio
    import os
    import json

    MIGRATION_VERSION = 13

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        set_start_time()
        log = get_logger("core")
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

        # 启动 Agent 审计日志后台队列
        from services.agent.audit import start_audit_consumer
        start_audit_consumer()
        log.log_event("audit_consumer_started", "审计日志后台队列已启动")

        # 启动 SDK 子进程
        try:
            from services.common.sdk_proxy_client import get_subprocess_manager
            sub_mgr = get_subprocess_manager()
            if sub_mgr.start_subprocess():
                log.log_event("sdk_subprocess_started", "SDK 子进程已启动")
            else:
                log.log_event("sdk_subprocess_start_fail", "SDK 子进程启动失败（将使用其他数据源兜底）")
        except Exception as e:
            log.error("sdk_subprocess", f"SDK 子进程初始化失败: {e}")

        # 启动调度服务
        from services.common.scheduler_service import start_scheduler, get_scheduler, _set_fastapi_loop
        start_scheduler()
        log.log_event("scheduler_started", "调度服务已启动")

        # 启动行情调度器
        from services.trading.gateway_dispatcher import get_gateway_dispatcher
        dispatcher = get_gateway_dispatcher()
        await dispatcher.start()
        log.log_event("dispatcher_started", "行情调度器已启动")

        # 初始化 PriceCache TTL
        try:
            from services.common.price_cache import get_price_cache
            from services.data.local_data_service import is_trading_hours
            cache = get_price_cache()
            if is_trading_hours():
                cache.set_ttl(600)
            else:
                cache.set_ttl(43200)
                log.log_event("price_cache_ttl_extended", f"非交易时段 PriceCache TTL 延长至 12 小时")
        except Exception as e:
            log.error("price_cache_ttl", f"初始化 PriceCache TTL 失败: {e}")

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

        # ========== 数据库迁移 ==========
        await _run_migrations(db_manager, log, MIGRATION_VERSION)

        # 初始化多数据源 ChannelRouter
        await _init_channel_router(db_manager, log)

        yield

        # ========== 关闭流程 ==========
        log.log_event("shutdown", "StockWinner 关闭中...")

        # 停止交易监控
        try:
            from services.monitoring.service import get_trading_monitor
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

        # 停止行情调度器
        try:
            dispatcher = get_gateway_dispatcher()
            await dispatcher.stop()
            log.log_event("dispatcher_stopped", "行情调度器已停止")
        except Exception as e:
            log.error("shutdown", f"停止行情调度器失败: {e}")

        # 关闭 Agent 审计日志消费者
        try:
            from services.agent.audit import stop_audit_consumer
            await stop_audit_consumer()
            log.log_event("audit_consumer_stopped", "审计日志后台队列已关闭")
        except Exception as e:
            log.error("shutdown", f"关闭审计消费者失败: {e}")

        # 断开 SDK 连接，停止子进程
        try:
            from services.common.sdk_proxy_client import get_subprocess_manager
            sub_mgr = get_subprocess_manager()
            sub_mgr.stop_subprocess()
            log.log_event("sdk_subprocess_stopped", "SDK 子进程已停止")
        except Exception as e:
            log.error("sdk_subprocess", f"停止 SDK 子进程失败: {e}")

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

    return lifespan


async def _run_migrations(db_manager, log, migration_version: int):
    """执行数据库迁移"""
    from services.common.timezone import get_china_time

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
        applied_v = 9999

    async def run_migration(v: int, desc: str, sql_blocks: list):
        if v <= applied_v:
            return
        log.log_event("db_migration_start", desc, version=v)
        for sql in sql_blocks:
            try:
                await db_manager.execute(sql)
            except Exception:
                pass
        try:
            await db_manager.execute("INSERT INTO migration_version (version) VALUES (?)", (v,))
            log.log_event("db_migration_complete", desc, version=v)
        except Exception as e:
            log.error("db_migration", f"版本记录写入失败 ({e})", version=v)

    # v1: 初始迁移
    await run_migration(1, "初始迁移（表结构扩展 + 新表创建）", [
        "ALTER TABLE watchlist ADD COLUMN source_type TEXT DEFAULT 'screening'",
        "ALTER TABLE watchlist ADD COLUMN group_id INTEGER",
        "ALTER TABLE watchlist ADD COLUMN current_price REAL DEFAULT 0",
        "ALTER TABLE watchlist ADD COLUMN bought INTEGER DEFAULT 0",
        "ALTER TABLE watchlist ADD COLUMN buy_trade_id INTEGER",
        "ALTER TABLE strategies ADD COLUMN code TEXT",
        "ALTER TABLE strategies ADD COLUMN code_type TEXT DEFAULT 'config'",
        "ALTER TABLE strategies ADD COLUMN target_scope TEXT DEFAULT 'group'",
        "ALTER TABLE strategies ADD COLUMN function_name TEXT DEFAULT 'run'",
        "ALTER TABLE strategies ADD COLUMN code_scope TEXT DEFAULT 'screening'",
        "ALTER TABLE trade_records ADD COLUMN trigger_source TEXT",
        "ALTER TABLE trade_records ADD COLUMN notification_sent INTEGER DEFAULT 0",
        "ALTER TABLE trade_records ADD COLUMN strategy_id INTEGER",
        "ALTER TABLE trade_records ADD COLUMN signal_id INTEGER",
        "ALTER TABLE accounts ADD COLUMN commission_rate REAL DEFAULT 0.0003",
        "ALTER TABLE accounts ADD COLUMN stamp_tax REAL DEFAULT 0.0005",
        "ALTER TABLE accounts ADD COLUMN transfer_fee REAL DEFAULT 0.00002",
        "ALTER TABLE accounts ADD COLUMN min_commission REAL DEFAULT 5.0",
        "ALTER TABLE accounts ADD COLUMN is_mock INTEGER DEFAULT 1",
        "ALTER TABLE accounts ADD COLUMN trade_mode TEXT DEFAULT 'mock'",
        "ALTER TABLE trading_strategies ADD COLUMN strategy_type TEXT DEFAULT 'fixed'",
        "ALTER TABLE trading_strategies ADD COLUMN config TEXT DEFAULT '{}'",
        "ALTER TABLE stock_positions ADD COLUMN highest_price REAL DEFAULT 0",
        "ALTER TABLE strategy_tasks ADD COLUMN task_type TEXT DEFAULT 'strategy'",
        "ALTER TABLE strategy_tasks ADD COLUMN module TEXT DEFAULT NULL",
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

    # v1 后的索引和数据操作
    if applied_v < 1:
        for sql in [
            "CREATE INDEX IF NOT EXISTS idx_candidate_groups_account ON candidate_groups(account_id)",
            "CREATE INDEX IF NOT EXISTS idx_strategy_tasks_account ON strategy_tasks(account_id)",
            "CREATE INDEX IF NOT EXISTS idx_strategy_tasks_enabled ON strategy_tasks(enabled)",
            "CREATE INDEX IF NOT EXISTS idx_notification_config_account ON notification_config(account_id)",
            "CREATE INDEX IF NOT EXISTS idx_notification_history_account ON notification_history(account_id)",
            "CREATE INDEX IF NOT EXISTS idx_notification_history_created ON notification_history(created_at)",
            "CREATE INDEX IF NOT EXISTS idx_trading_strategy_account ON trading_strategy_config(account_id)",
            "CREATE INDEX IF NOT EXISTS idx_orders_account ON orders(account_id)",
            "CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status)",
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_llm_config_account ON llm_config(account_id)",
        ]:
            try:
                await db_manager.execute(sql)
            except Exception:
                pass

        # 归集未分组 watchlist
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
                        "account_id": aid, "name": "未分组", "group_type": "manual",
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

        # 迁移旧版 LLM 配置
        try:
            from pathlib import Path as _Path
            legacy_path = _Path(__file__).parent.parent.parent / "config" / "llm.json"
            if legacy_path.exists():
                existing_count = await db_manager.fetchone("SELECT COUNT(*) as cnt FROM llm_config")
                if existing_count and existing_count.get("cnt", 0) == 0:
                    with open(legacy_path, 'r') as f:
                        legacy = json.load(f)
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

    # 清理遗留 running 状态
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

    # 清理遗留回测任务 running 状态
    try:
        result = await db_manager.execute(
            "UPDATE backtest_runs SET status = 'error', error_message = '服务重启，任务中断', completed_at = ? WHERE status = 'running'",
            (get_china_time().isoformat(),)
        )
        bt_reset_count = getattr(result, 'rowcount', 0) if hasattr(result, 'rowcount') else 0
        if bt_reset_count > 0:
            log.log_event("backtest_running_cleanup", f"已清理 {bt_reset_count} 个遗留 running 回测任务", count=bt_reset_count)
    except Exception as e:
        log.error("db_migration", f"清理 backtest running 状态跳过 ({e})")

    # 恢复遗留未完成订单
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

    # T+1 解冻
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

    # 创建内置任务
    if applied_v < 1:
        builtin_defaults = [
            {"task_type": "builtin", "module": "kline_check", "account_id": "SYSTEM", "cron_expression": "0 1 * * *", "enabled": 1},
            {"task_type": "builtin", "module": "monthly_factors", "account_id": "SYSTEM", "cron_expression": "0 1 5 * *", "enabled": 1},
            {"task_type": "builtin", "module": "weekly_kline", "account_id": "SYSTEM", "cron_expression": "0 2 * * 6", "enabled": 1},
            {"task_type": "builtin", "module": "industry_download", "account_id": "SYSTEM", "cron_expression": "0 3 * * mon-fri", "enabled": 0},
            {"task_type": "builtin", "module": "post_market_analysis", "account_id": "SYSTEM", "cron_expression": "30 15 * * mon-fri", "enabled": 1},
        ]
        for t in builtin_defaults:
            existing = await db_manager.fetchone(
                "SELECT id FROM strategy_tasks WHERE task_type = 'builtin' AND module = ?",
                (t["module"],)
            )
            if not existing:
                await db_manager.insert("strategy_tasks", {
                    "task_type": "builtin", "module": t["module"],
                    "strategy_id": None, "group_id": None,
                    "account_id": t["account_id"], "cron_expression": t["cron_expression"],
                    "enabled": t["enabled"],
                })
                log.log_event("migration_builtin_task", f"内置任务 {t['module']} 已创建", module=t['module'])

    # v2: watchlist 复合索引
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

    # v3: Agent 协作框架
    await run_migration(3, "Agent 协作框架", [
        """CREATE TABLE IF NOT EXISTS agent_accounts (
            id INTEGER PRIMARY KEY AUTOINCREMENT, agent_id TEXT UNIQUE NOT NULL,
            user_id TEXT NOT NULL DEFAULT '', name TEXT NOT NULL,
            agent_type TEXT DEFAULT 'generic', api_key_hash TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT 'viewer', allowed_account_ids TEXT DEFAULT '["*"]',
            allowed_permissions TEXT DEFAULT '[]', denied_permissions TEXT DEFAULT '[]',
            rate_limit_per_min INTEGER DEFAULT 60, enabled INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, last_used_at TIMESTAMP
        )""",
        "CREATE INDEX IF NOT EXISTS idx_agent_accounts_user ON agent_accounts(user_id)",
        """CREATE TABLE IF NOT EXISTS agent_audit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT, agent_id TEXT NOT NULL,
            user_id TEXT NOT NULL DEFAULT '', action TEXT NOT NULL,
            resource_type TEXT, resource_id TEXT, account_id TEXT,
            status TEXT NOT NULL, request_payload TEXT, response_summary TEXT,
            risk_level TEXT NOT NULL, confirmation_id TEXT, ip_address TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""",
        "CREATE INDEX IF NOT EXISTS idx_agent_audit_agent ON agent_audit_log(agent_id)",
        "CREATE INDEX IF NOT EXISTS idx_agent_audit_created ON agent_audit_log(created_at)",
        "CREATE INDEX IF NOT EXISTS idx_agent_audit_user ON agent_audit_log(user_id)",
        """CREATE TABLE IF NOT EXISTS agent_confirmations (
            id INTEGER PRIMARY KEY AUTOINCREMENT, confirmation_id TEXT UNIQUE NOT NULL,
            agent_id TEXT NOT NULL, action TEXT NOT NULL, account_id TEXT,
            request_payload TEXT, risk_level TEXT NOT NULL, status TEXT DEFAULT 'pending',
            reviewed_by TEXT, review_notes TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            expires_at TIMESTAMP
        )""",
    ])

    # v4: account_id 不可变触发器
    await run_migration(4, "account_id 不可变触发器", [
        """CREATE TRIGGER IF NOT EXISTS prevent_account_id_update
        BEFORE UPDATE OF account_id ON accounts
        BEGIN SELECT RAISE(ABORT, 'account_id 创建后不可变更'); END;"""
    ])

    # v5: 信号表 order_type
    await run_migration(5, "trading_signals 新增 order_type", [
        """ALTER TABLE trading_signals ADD COLUMN order_type TEXT DEFAULT 'day'""",
        """CREATE INDEX IF NOT EXISTS idx_signals_order_type ON trading_signals(account_id, status, order_type)""",
    ])

    # v6: 回测系统
    await run_migration(6, "回测系统四张新表", [
        """CREATE TABLE IF NOT EXISTS backtest_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT, account_id TEXT NOT NULL,
            strategy_id INTEGER, name TEXT NOT NULL, mode TEXT NOT NULL DEFAULT 'simulated',
            start_date TEXT NOT NULL, end_date TEXT NOT NULL,
            initial_capital REAL NOT NULL DEFAULT 1000000, commission_rate REAL NOT NULL DEFAULT 0.0003,
            stamp_tax REAL NOT NULL DEFAULT 0.0005, transfer_fee REAL NOT NULL DEFAULT 0.00002,
            min_commission REAL NOT NULL DEFAULT 5.0, slippage_pct REAL NOT NULL DEFAULT 0.0,
            max_total_position_pct REAL DEFAULT 0.80, max_single_position_pct REAL DEFAULT 0.15,
            cash_reserve_pct REAL DEFAULT 0.10, config TEXT DEFAULT '{}',
            status TEXT NOT NULL DEFAULT 'pending', progress REAL DEFAULT 0, error_message TEXT,
            data_gap_report TEXT, result_summary TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            started_at TIMESTAMP, completed_at TIMESTAMP, pool_schedule TEXT, markets TEXT,
            group_ids TEXT, stock_pool TEXT, description TEXT DEFAULT '',
            stop_loss_pct REAL, take_profit_pct REAL, trailing_stop_pct REAL,
            stop_execution_price TEXT DEFAULT 'close', liquidate_at_end INTEGER DEFAULT 1,
            current_trade_date TEXT
        )""",
        "CREATE INDEX IF NOT EXISTS idx_backtest_runs_account ON backtest_runs(account_id)",
        "CREATE INDEX IF NOT EXISTS idx_backtest_runs_status ON backtest_runs(status)",
        """CREATE TABLE IF NOT EXISTS backtest_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT, backtest_run_id INTEGER NOT NULL,
            stock_code TEXT NOT NULL, stock_name TEXT, buy_date TEXT NOT NULL,
            buy_price REAL NOT NULL, buy_quantity INTEGER NOT NULL, buy_commission REAL NOT NULL,
            sell_date TEXT, sell_price REAL, sell_commission REAL, sell_reason TEXT,
            pnl REAL, pnl_pct REAL, holding_days INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""",
        "CREATE INDEX IF NOT EXISTS idx_backtest_trades_run ON backtest_trades(backtest_run_id)",
        "CREATE INDEX IF NOT EXISTS idx_backtest_trades_stock ON backtest_trades(backtest_run_id, stock_code)",
        """CREATE TABLE IF NOT EXISTS backtest_daily_nav (
            id INTEGER PRIMARY KEY AUTOINCREMENT, backtest_run_id INTEGER NOT NULL,
            trade_date TEXT NOT NULL, nav REAL NOT NULL, total_value REAL NOT NULL,
            cash REAL NOT NULL, positions_value REAL NOT NULL, position_count INTEGER DEFAULT 0,
            drawdown REAL DEFAULT 0, max_drawdown REAL DEFAULT 0, daily_return REAL DEFAULT 0
        )""",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_backtest_daily_nav_run_date ON backtest_daily_nav(backtest_run_id, trade_date)",
        """CREATE TABLE IF NOT EXISTS backtest_daily_positions (
            id INTEGER PRIMARY KEY AUTOINCREMENT, backtest_run_id INTEGER NOT NULL,
            trade_date TEXT NOT NULL, stock_code TEXT NOT NULL, stock_name TEXT,
            quantity INTEGER NOT NULL, avg_cost REAL NOT NULL, close_price REAL NOT NULL,
            market_value REAL NOT NULL, unrealized_pnl REAL DEFAULT 0
        )""",
        "CREATE INDEX IF NOT EXISTS idx_backtest_daily_positions_run ON backtest_daily_positions(backtest_run_id, trade_date)",
    ])

    # v7: 多数据源接入
    await run_migration(7, "多数据源接入", [
        """CREATE TABLE IF NOT EXISTS data_source_config (
            provider_id TEXT PRIMARY KEY, display_name TEXT NOT NULL,
            is_enabled INTEGER DEFAULT 0, channel_priority_json TEXT,
            system_config_json TEXT, capabilities_json TEXT,
            requires_config INTEGER DEFAULT 0, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""",
    ])

    # v8: 动态股票池回测支持
    await run_migration(8, "动态股票池", [
        "ALTER TABLE backtest_runs ADD COLUMN pool_schedule TEXT",
        "ALTER TABLE backtest_runs ADD COLUMN liquidate_at_end INTEGER DEFAULT 1",
        "ALTER TABLE backtest_runs ADD COLUMN current_trade_date TEXT",
    ])

    # v9: 策略链路追踪
    await run_migration(9, "策略链路追踪", [
        "ALTER TABLE stock_positions ADD COLUMN strategy_id INTEGER",
    ])

    # v10: 策略版本存档
    await run_migration(10, "策略版本存档", [
        """CREATE TABLE IF NOT EXISTS strategy_versions (
            id INTEGER PRIMARY KEY AUTOINCREMENT, strategy_id INTEGER NOT NULL,
            account_id TEXT NOT NULL, version INTEGER NOT NULL, action TEXT NOT NULL,
            name TEXT, description TEXT, strategy_type TEXT, config TEXT, code TEXT,
            code_type TEXT, code_scope TEXT, function_name TEXT, target_scope TEXT,
            status TEXT, match_score_threshold REAL, buy_strategy_id INTEGER,
            sell_strategy_id INTEGER, diff_summary TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""",
        "CREATE INDEX IF NOT EXISTS idx_strategy_versions_strategy ON strategy_versions(strategy_id)",
        "CREATE INDEX IF NOT EXISTS idx_strategy_versions_account ON strategy_versions(account_id, strategy_id)",
    ])

    # v11: watchlist 添加 signal_type
    await run_migration(11, "watchlist 添加买卖信号类型", [
        "ALTER TABLE watchlist ADD COLUMN signal_type TEXT DEFAULT 'buy'",
        "UPDATE watchlist SET signal_type = 'sell' WHERE source_type = 'manual' AND target_quantity > 0 AND stock_code IN (SELECT stock_code FROM stock_positions WHERE quantity > 0 AND account_id = watchlist.account_id)",
    ])

    # v12: watchlist 重命名 buy_price → trigger_price
    await run_migration(12, "watchlist 字段重命名", [
        "ALTER TABLE watchlist RENAME COLUMN buy_price TO trigger_price",
    ])

    # v13: watchlist 添加 is_active
    await run_migration(13, "watchlist 添加 is_active", [
        "ALTER TABLE watchlist ADD COLUMN is_active INTEGER DEFAULT 1",
        "CREATE INDEX IF NOT EXISTS idx_watchlist_active_status ON watchlist(stock_code, status) WHERE is_active = 1",
    ])

    # v7 数据源配置 seed
    try:
        from services.data.channel.config_manager import seed_provider_configs, add_account_role_column
        await seed_provider_configs()
        await add_account_role_column()
        log.log_event("db_migration_v7", "数据源配置已初始化")
    except Exception as e:
        log.error("db_migration_v7", f"数据源初始化失败: {e}")


async def _init_channel_router(db_manager, log):
    """初始化多数据源 ChannelRouter"""
    try:
        from services.data.channel.router import get_channel_router, ChannelType, ChannelConfig
        from services.data.channel.config_manager import load_channel_order
        from services.data.providers.amazingdata_provider import AmazingDataProvider
        from services.data.providers.eastmoney_provider import EastmoneyDataProvider
        from services.data.providers.tushare_provider import TushareDataProvider
        from services.data.providers.sina_provider import SinaDataProvider
        from services.data.providers.tencent_provider import TencentDataProvider
        from services.data.providers.akshare_provider import AkshareDataProvider

        router = get_channel_router()

        # AmazingData
        router.register_provider(AmazingDataProvider())

        # Eastmoney
        eastmoney_provider = EastmoneyDataProvider()
        await eastmoney_provider.initialize({})
        router.register_provider(eastmoney_provider)

        # Tushare
        tushare_provider = TushareDataProvider()
        tushare_cfg = await db_manager.fetchone(
            "SELECT system_config_json FROM data_source_config WHERE provider_id = 'tushare'"
        )
        if tushare_cfg and tushare_cfg.get("system_config_json"):
            await tushare_provider.initialize(json.loads(tushare_cfg["system_config_json"]))
            log.log_event("data_source_init", "Tushare provider 已从 DB 配置初始化")
        else:
            log.log_event("data_source_init", "Tushare provider 未配置 API Token，跳过初始化")
        router.register_provider(tushare_provider)

        # Sina
        sina_provider = SinaDataProvider()
        await sina_provider.initialize({})
        router.register_provider(sina_provider)

        # Tencent
        tencent_provider = TencentDataProvider()
        await tencent_provider.initialize({})
        router.register_provider(tencent_provider)

        # Akshare
        akshare_provider = AkshareDataProvider()
        await akshare_provider.initialize({})
        router.register_provider(akshare_provider)

        for ct in [ChannelType.TRADING, ChannelType.MARKET_DATA, ChannelType.DATA_DOWNLOAD]:
            provider_order = await load_channel_order(ct.value)
            if provider_order:
                router.set_channel_config(ct, ChannelConfig(
                    channel_type=ct, provider_order=provider_order,
                    timeout_seconds=15.0,
                ))

        log.log_event("data_source_init", "多数据源 ChannelRouter 已初始化（amazingdata + eastmoney + tushare + sina + tencent + akshare）")
    except Exception as e:
        log.error("data_source_init", f"ChannelRouter 初始化失败: {e}")
