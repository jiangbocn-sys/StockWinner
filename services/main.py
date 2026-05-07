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
import os

from services.common.timezone import get_china_time
from services.common.database import get_db_manager, reset_db_manager
from services.common.account_manager import get_account_manager, reset_account_manager
from services.ui import dashboard, accounts, positions, trades, strategies, screening, monitoring, market_data, data_explorer, position_rules, factors, scheduler, notifications, trading_strategies
from services.strategy.api import router as strategy_v2_router
from services.account_management.api import router as account_management_router
from services.auth.api import router as auth_router
from services.llm.api import router as llm_router

from services._version import VERSION, set_start_time
_server_start_time = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期"""
    set_start_time()

    # 启动时初始化
    print(f"StockWinner v{VERSION} 启动中...")

    # 初始化数据库连接
    db_manager = get_db_manager()
    await db_manager.connect()
    print("数据库连接已建立")

    # 从数据库加载账户列表
    account_manager = get_account_manager()
    accounts = await account_manager.list_accounts()
    active_accounts = [a for a in accounts if a.get('is_active')]
    print(f"已加载 {len(active_accounts)} 个激活账户：{', '.join([a['account_id'] for a in active_accounts])}")

    # 启动调度服务（每天凌晨1点检查K线，每月5日检查月频因子）
    from services.common.scheduler_service import start_scheduler
    start_scheduler()
    print("调度服务已启动")

    # 数据库迁移：为 watchlist 表添加新列
    try:
        await db_manager.execute(
            "ALTER TABLE watchlist ADD COLUMN source_type TEXT DEFAULT 'screening'"
        )
        print("数据库迁移: watchlist.source_type 已添加")
    except Exception:
        pass  # 列已存在

    try:
        await db_manager.execute(
            "ALTER TABLE watchlist ADD COLUMN group_id INTEGER"
        )
        print("数据库迁移: watchlist.group_id 已添加")
    except Exception:
        pass  # 列已存在

    # 创建 candidate_groups 表
    try:
        await db_manager.execute("""
            CREATE TABLE IF NOT EXISTS candidate_groups (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_id TEXT NOT NULL,
                name TEXT NOT NULL,
                group_type TEXT NOT NULL DEFAULT 'manual',
                screening_strategy_id INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (screening_strategy_id) REFERENCES strategies(id)
            )
        """)
        await db_manager.execute("CREATE INDEX IF NOT EXISTS idx_candidate_groups_account ON candidate_groups(account_id)")
        print("数据库迁移: candidate_groups 表已创建")
    except Exception:
        pass  # 表已存在

    # 创建「未分组」默认组，归集所有 group_id 为 NULL 的记录
    try:
        # 为每个有未分组记录的账户创建默认组
        accounts_with_ungrouped = await db_manager.fetchall(
            "SELECT DISTINCT account_id FROM watchlist WHERE group_id IS NULL"
        )
        for row in accounts_with_ungrouped:
            aid = row['account_id']
            # 检查是否已有未分组组
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
        print("数据库迁移: 「未分组」默认组已创建，未分组记录已归集")
    except Exception as e:
        print(f"数据库迁移: 未分组归集跳过 ({e})")

    # 扩展 strategies 表：支持代码型策略
    try:
        await db_manager.execute("ALTER TABLE strategies ADD COLUMN code TEXT")
        print("数据库迁移: strategies.code 已添加")
    except Exception:
        pass
    try:
        await db_manager.execute("ALTER TABLE strategies ADD COLUMN code_type TEXT DEFAULT 'config'")
        print("数据库迁移: strategies.code_type 已添加")
    except Exception:
        pass
    try:
        await db_manager.execute("ALTER TABLE strategies ADD COLUMN target_scope TEXT DEFAULT 'group'")
        print("数据库迁移: strategies.target_scope 已添加")
    except Exception:
        pass
    try:
        await db_manager.execute("ALTER TABLE strategies ADD COLUMN function_name TEXT DEFAULT 'run'")
        print("数据库迁移: strategies.function_name 已添加")
    except Exception:
        pass
    try:
        await db_manager.execute("ALTER TABLE strategies ADD COLUMN code_scope TEXT DEFAULT 'screening'")
        print("数据库迁移: strategies.code_scope 已添加")
    except Exception:
        pass

    # 创建 strategy_tasks 表
    try:
        await db_manager.execute("""
            CREATE TABLE IF NOT EXISTS strategy_tasks (
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
            )
        """)
        await db_manager.execute("CREATE INDEX IF NOT EXISTS idx_strategy_tasks_account ON strategy_tasks(account_id)")
        await db_manager.execute("CREATE INDEX IF NOT EXISTS idx_strategy_tasks_enabled ON strategy_tasks(enabled)")
        print("数据库迁移: strategy_tasks 表已创建")
    except Exception:
        pass  # 表已存在

    # 扩展 strategy_tasks 支持内置功能任务
    try:
        await db_manager.execute("ALTER TABLE strategy_tasks ADD COLUMN task_type TEXT DEFAULT 'strategy'")
        print("数据库迁移: strategy_tasks.task_type 已添加")
    except Exception:
        pass
    try:
        await db_manager.execute("ALTER TABLE strategy_tasks ADD COLUMN module TEXT DEFAULT NULL")
        print("数据库迁移: strategy_tasks.module 已添加")
    except Exception:
        pass

    # === 交易模块 + 消息推送模块 数据库迁移 ===

    # 扩展 accounts 表：佣金费率 + 交易模式
    try:
        await db_manager.execute("ALTER TABLE accounts ADD COLUMN commission_rate REAL DEFAULT 0.0003")
        print("数据库迁移: accounts.commission_rate 已添加")
    except Exception:
        pass
    try:
        await db_manager.execute("ALTER TABLE accounts ADD COLUMN stamp_tax REAL DEFAULT 0.0005")
        print("数据库迁移: accounts.stamp_tax 已添加")
    except Exception:
        pass
    try:
        await db_manager.execute("ALTER TABLE accounts ADD COLUMN transfer_fee REAL DEFAULT 0.00002")
        print("数据库迁移: accounts.transfer_fee 已添加")
    except Exception:
        pass
    try:
        await db_manager.execute("ALTER TABLE accounts ADD COLUMN min_commission REAL DEFAULT 5.0")
        print("数据库迁移: accounts.min_commission 已添加")
    except Exception:
        pass
    try:
        await db_manager.execute("ALTER TABLE accounts ADD COLUMN is_mock INTEGER DEFAULT 1")
        print("数据库迁移: accounts.is_mock 已添加")
    except Exception:
        pass
    try:
        await db_manager.execute("ALTER TABLE accounts ADD COLUMN trade_mode TEXT DEFAULT 'mock'")
        print("数据库迁移: accounts.trade_mode 已添加")
    except Exception:
        pass

    # 扩展 trade_records 表
    try:
        await db_manager.execute("ALTER TABLE trade_records ADD COLUMN trigger_source TEXT")
        print("数据库迁移: trade_records.trigger_source 已添加")
    except Exception:
        pass
    try:
        await db_manager.execute("ALTER TABLE trade_records ADD COLUMN notification_sent INTEGER DEFAULT 0")
        print("数据库迁移: trade_records.notification_sent 已添加")
    except Exception:
        pass

    # 创建 notification_config 表
    try:
        await db_manager.execute("""
            CREATE TABLE IF NOT EXISTS notification_config (
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
            )
        """)
        await db_manager.execute("CREATE INDEX IF NOT EXISTS idx_notification_config_account ON notification_config(account_id)")
        print("数据库迁移: notification_config 表已创建")
    except Exception:
        pass

    # 创建 notification_history 表
    try:
        await db_manager.execute("""
            CREATE TABLE IF NOT EXISTS notification_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_id TEXT NOT NULL,
                channel TEXT NOT NULL,
                event_type TEXT NOT NULL,
                title TEXT,
                content TEXT,
                status TEXT DEFAULT 'pending',
                response TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await db_manager.execute("CREATE INDEX IF NOT EXISTS idx_notification_history_account ON notification_history(account_id)")
        await db_manager.execute("CREATE INDEX IF NOT EXISTS idx_notification_history_created ON notification_history(created_at)")
        print("数据库迁移: notification_history 表已创建")
    except Exception:
        pass

    # 创建 trading_strategy_config 表
    try:
        await db_manager.execute("""
            CREATE TABLE IF NOT EXISTS trading_strategy_config (
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
            )
        """)
        await db_manager.execute("CREATE INDEX IF NOT EXISTS idx_trading_strategy_account ON trading_strategy_config(account_id)")
        print("数据库迁移: trading_strategy_config 表已创建")
    except Exception:
        pass

    # === 交易模块增强：个股动态策略支持 ===

    # 扩展 trading_strategies 表：支持动态策略类型
    try:
        await db_manager.execute("ALTER TABLE trading_strategies ADD COLUMN strategy_type TEXT DEFAULT 'fixed'")
        print("数据库迁移: trading_strategies.strategy_type 已添加")
    except Exception:
        pass
    try:
        await db_manager.execute("ALTER TABLE trading_strategies ADD COLUMN config TEXT DEFAULT '{}'")
        print("数据库迁移: trading_strategies.config 已添加")
    except Exception:
        pass

    # 扩展 stock_positions 表：记录持仓期间最高价（用于回撤止盈策略）
    try:
        await db_manager.execute("ALTER TABLE stock_positions ADD COLUMN highest_price REAL DEFAULT 0")
        print("数据库迁移: stock_positions.highest_price 已添加")
    except Exception:
        pass

    # 启动时清理遗留的 running 状态（服务重启后未完成的任务）
    try:
        result = await db_manager.execute(
            "UPDATE strategy_tasks SET last_status = 'error', last_output = '{\"error\": \"服务重启，任务中断\"}', updated_at = ? WHERE last_status = 'running'",
            (get_china_time().isoformat(),)
        )
        reset_count = getattr(result, 'rowcount', 0) if hasattr(result, 'rowcount') else 0
        if reset_count > 0:
            print(f"数据库迁移: 已清理 {reset_count} 个遗留 running 状态")
    except Exception as e:
        print(f"数据库迁移: 清理 running 状态跳过 ({e})")

    # === 交易模块增强：订单状态机 + 持仓管理 ===

    # 创建 orders 表（订单状态机）
    try:
        await db_manager.execute("""
            CREATE TABLE IF NOT EXISTS orders (
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
            )
        """)
        await db_manager.execute("CREATE INDEX IF NOT EXISTS idx_orders_account ON orders(account_id)")
        await db_manager.execute("CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status)")
        print("数据库迁移: orders 表已创建")
    except Exception:
        pass  # 表已存在

    # 启动时恢复：将 pending/submitted 状态的订单标记为 cancelled（服务重启后旧订单已失效）
    try:
        result = await db_manager.execute(
            "UPDATE orders SET status = 'cancelled', reject_reason = '服务重启，订单已失效', updated_at = ? WHERE status IN ('pending', 'submitted')",
            (get_china_time().isoformat(),)
        )
        cancelled_count = getattr(result, 'rowcount', 0) if hasattr(result, 'rowcount') else 0
        if cancelled_count > 0:
            print(f"数据库迁移: 已取消 {cancelled_count} 个遗留未完成订单")
    except Exception:
        pass  # 表可能不存在

    # 启动时执行 T+1 解冻 + 重置 pending 状态（服务重启后自动解冻昨日买入的持仓）
    try:
        positions = await db_manager.fetchall(
            "SELECT DISTINCT account_id FROM stock_positions WHERE available_quantity < quantity"
        )
        for row in positions:
            await db_manager.execute(
                "UPDATE stock_positions SET available_quantity = quantity, updated_at = ? WHERE account_id = ? AND available_quantity < quantity",
                (get_china_time().isoformat(), row["account_id"])
            )
            # 同步重置该账户 watchlist 中所有 pending 状态为 watching
            await db_manager.execute(
                "UPDATE watchlist SET status = 'watching' WHERE account_id = ? AND status = 'pending'",
                (row["account_id"],)
            )
        if positions:
            print(f"数据库迁移: 已解冻 {len(positions)} 个账户的 T+1 持仓，pending 状态已重置为 watching")
    except Exception as e:
        print(f"数据库迁移: T+1 解冻跳过 ({e})")

    # 创建 llm_config 表（用户级 LLM API 配置）
    try:
        await db_manager.execute("""
            CREATE TABLE IF NOT EXISTS llm_config (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_id TEXT NOT NULL,
                provider TEXT DEFAULT 'openai',
                base_url TEXT NOT NULL,
                api_key TEXT NOT NULL,
                model_name TEXT NOT NULL,
                enabled INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await db_manager.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_llm_config_account ON llm_config(account_id)")
        print("数据库迁移: llm_config 表已创建")
    except Exception:
        pass

    # 迁移旧版 config/llm.json 配置到数据库（如果存在且数据库中尚无记录）
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
                    print("数据库迁移: 已从 config/llm.json 迁移系统级配置")
    except Exception as e:
        print(f"数据库迁移: LLM 旧配置迁移失败 ({e})")

    # 扫描并注册任务插件
    try:
        from services.tasks import scan_tasks
        scan_tasks()
        print("数据库迁移: 任务插件扫描完成")
    except Exception as e:
        print(f"数据库迁移: 任务插件扫描失败: {e}")

    # 创建默认内置功能任务（如不存在）
    try:
        builtin_defaults = [
            {
                "task_type": "builtin",
                "module": "kline_check",
                "account_id": "SYSTEM",
                "cron_expression": "0 1 * * *",
                "enabled": 1,
            },
            {
                "task_type": "builtin",
                "module": "monthly_factors",
                "account_id": "SYSTEM",
                "cron_expression": "0 1 5 * *",
                "enabled": 1,
            },
            {
                "task_type": "builtin",
                "module": "weekly_kline",
                "account_id": "SYSTEM",
                "cron_expression": "0 2 * * 6",
                "enabled": 1,
            },
            {
                "task_type": "builtin",
                "module": "industry_download",
                "account_id": "SYSTEM",
                "cron_expression": "0 3 * * 1-5",
                "enabled": 0,
            },
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
                print(f"数据库迁移: 内置任务 {t['module']} 已创建")
    except Exception as e:
        print(f"数据库迁移: 内置任务创建失败: {e}")

    # 预加载 Kronos 模型（非阻塞，失败不影响启动）
    try:
        from services.common.kronos_service import load_kronos_on_startup
        load_kronos_on_startup()
    except Exception as e:
        print(f"Kronos 预加载跳过: {e}")

    yield

    # 关闭时清理
    print("StockWinner 关闭中...")
    from services.common.scheduler_service import stop_scheduler
    stop_scheduler()
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


# 全局异常处理
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """全局异常处理"""
    print(f"异常：{request.method} {request.url} - {str(exc)}")
    return JSONResponse(
        status_code=500,
        content={"success": False, "message": f"服务器内部错误：{str(exc)}"}
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

# 挂载前端静态文件
frontend_dist = "/home/bobo/StockWinner/frontend/dist"
if os.path.exists(frontend_dist):
    app.mount("/ui", StaticFiles(directory=frontend_dist, html=True), name="frontend")


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
