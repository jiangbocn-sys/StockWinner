"""
静态文件挂载 — 前端 dist 目录 + SPA history fallback。
"""

import os
from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles


def mount_static_files(app: FastAPI):
    """挂载前端静态文件并注册 SPA 路由"""
    frontend_dist = os.environ.get("FRONTEND_DIST", "/home/bobo/StockWinner/frontend/dist")
    if not os.path.exists(frontend_dist):
        return

    app.mount("/ui/assets", StaticFiles(directory=os.path.join(frontend_dist, "assets")), name="ui-assets")

    @app.get("/ui/favicon.svg", include_in_schema=False)
    async def serve_favicon():
        return FileResponse(os.path.join(frontend_dist, "favicon.svg"))

    @app.get("/ui/{full_path:path}", include_in_schema=False)
    async def serve_spa(full_path: str):
        index_path = os.path.join(frontend_dist, "index.html")
        if os.path.exists(index_path):
            return FileResponse(index_path)
        return JSONResponse(status_code=404, content={"message": "Frontend not found"})
