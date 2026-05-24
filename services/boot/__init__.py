"""
Boot 模块 — 应用工厂入口。
"""

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.exceptions import HTTPException

from services.boot.lifespan import create_lifespan
from services.boot.middleware import register_middleware
from services.boot.routers import register_routers
from services.boot.static_files import mount_static_files
from services.common.structured_logger import get_logger
from services._version import VERSION


def create_app() -> FastAPI:
    """创建 FastAPI 应用实例"""
    app = FastAPI(
        title="StockWinner",
        description=f"智能股票交易系统 v{VERSION} - 调度服务增强",
        version=VERSION,
        lifespan=create_lifespan(),
    )

    register_middleware(app)
    register_routers(app)
    mount_static_files(app)

    _register_exception_handlers(app)

    @app.get("/")
    async def root():
        return {"service": "StockWinner", "version": VERSION, "status": "running"}

    @app.get("/api/v1/health")
    async def health_check():
        return {"status": "healthy", "version": VERSION}

    return app


def _register_exception_handlers(app: FastAPI):
    """注册全局异常处理器"""

    @app.exception_handler(HTTPException)
    async def http_exception_handler(request: Request, exc: HTTPException):
        return JSONResponse(
            status_code=exc.status_code,
            content={"success": False, "message": exc.detail}
        )

    @app.exception_handler(Exception)
    async def global_exception_handler(request: Request, exc: Exception):
        log = get_logger("core")
        log.error("exception", f"{request.method} {request.url}", context={"error": str(exc)})
        return JSONResponse(
            status_code=500,
            content={"success": False, "message": "服务器内部错误"}
        )
