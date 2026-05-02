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
from datetime import datetime, timezone, timedelta

# 设置默认时区为中国时区 (Asia/Shanghai)
CHINA_TZ = timezone(timedelta(hours=8))

def get_china_time():
    """获取中国时区时间"""
    return datetime.now(CHINA_TZ).replace(tzinfo=None)

# 覆盖 datetime.now 以使用中国时区
original_datetime = datetime

class ChinaDateTime(original_datetime):
    """使用中国时区的 datetime 类"""
    @classmethod
    def now(cls, tz=None):
        if tz is None:
            return original_datetime.now(CHINA_TZ).replace(tzinfo=None)
        return original_datetime.now(tz)

datetime = ChinaDateTime

from services.common.database import get_db_manager, reset_db_manager
from services.common.account_manager import get_account_manager, reset_account_manager
from services.ui import dashboard, accounts, positions, trades, strategies, screening, monitoring, market_data, data_explorer, position_rules, factors, scheduler
from services.strategy.api import router as strategy_v2_router
from services.account_management.api import router as account_management_router
from services.auth.api import router as auth_router
from services.llm.api import router as llm_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期"""
    # 启动时初始化
    print("StockWinner v6.2.4 启动中...")

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
    description="智能股票交易系统 v6.2.5 - 调度服务增强",
    version="6.2.5",
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
        "version": "6.2.4",
        "status": "running"
    }


@app.get("/api/v1/health")
async def health_check():
    """健康检查（不带账户参数）"""
    return {
        "status": "healthy",
        "version": "6.2.4"
    }
