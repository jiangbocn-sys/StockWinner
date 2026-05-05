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
from services.ui import dashboard, accounts, positions, trades, strategies, screening, monitoring, market_data, data_explorer, position_rules, factors, scheduler
from services.strategy.api import router as strategy_v2_router
from services.account_management.api import router as account_management_router
from services.auth.api import router as auth_router
from services.llm.api import router as llm_router

VERSION = "6.2.5"


@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期"""
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

    # 扫描并注册任务插件
    try:
        from services.tasks import scan_tasks
        scan_tasks()
        print("数据库迁移: 任务插件扫描完成")
    except Exception as e:
        print(f"数据库迁移: 任务插件扫描失败: {e}")

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
app.include_router(strategy_v2_router)  # 策略管理 API v2
# 添加账户管理API路由
app.include_router(account_management_router)
app.include_router(auth_router)
app.include_router(llm_router)  # LLM 配置路由

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
