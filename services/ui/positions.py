"""
持仓管理 API
"""

import httpx
import time
from fastapi import APIRouter, HTTPException, Path, Query, Body
from typing import Optional
from services.common.database import get_db_manager

router = APIRouter()

DSA_BASE_URL = "http://localhost:8000"


@router.get("/api/v1/ui/{account_id}/positions")
async def get_positions(
    account_id: str = Path(..., description="账户 ID"),
    stock_code: Optional[str] = Query(None, description="股票代码过滤")
):
    """获取持仓列表（包含可用资金）"""
    db = get_db_manager()

    # 从数据库验证账户
    account = await db.fetchone(
        "SELECT 1 FROM accounts WHERE account_id = ? AND is_active = 1",
        (account_id,)
    )
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    if stock_code:
        positions = await db.fetchall(
            "SELECT * FROM stock_positions WHERE account_id = ? AND stock_code = ? AND quantity > 0",
            (account_id, stock_code)
        )
    else:
        positions = await db.fetchall(
            "SELECT * FROM stock_positions WHERE account_id = ? AND quantity > 0 ORDER BY stock_code",
            (account_id,)
        )

    # 获取账户可用资金
    account_data = await db.fetchone(
        "SELECT available_cash FROM accounts WHERE account_id = ?",
        (account_id,)
    )
    available_cash = float(account_data["available_cash"]) if account_data else 0.0

    return {
        "account_id": account_id,
        "positions": positions,
        "available_cash": available_cash
    }


@router.get("/api/v1/ui/{account_id}/positions/{stock_code}")
async def get_position(
    account_id: str = Path(..., description="账户 ID"),
    stock_code: str = Path(..., description="股票代码")
):
    """获取单只股票持仓"""
    db = get_db_manager()

    # 从数据库验证账户
    account = await db.fetchone(
        "SELECT 1 FROM accounts WHERE account_id = ? AND is_active = 1",
        (account_id,)
    )
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在：{account_id}")

    position = await db.fetchone(
        "SELECT * FROM stock_positions WHERE account_id = ? AND stock_code = ?",
        (account_id, stock_code)
    )

    if not position:
        raise HTTPException(status_code=404, detail=f"持仓不存在：{stock_code}")

    return {"position": position}


@router.post("/api/v1/ui/{account_id}/positions/{stock_code}/dsa-analyze")
async dsa_analyze_position(
    account_id: str = Path(...),
    stock_code: str = Path(...),
):
    """对指定持仓股调用 DSA 分析，等待完成后返回结果"""
    db = get_db_manager()
    account = await db.fetchone(
        "SELECT 1 FROM accounts WHERE account_id = ? AND is_active = 1",
        (account_id,)
    )
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在：{account_id}")

    # 提交 DSA 分析任务
    with httpx.Client(timeout=30) as client:
        resp = client.post(
            f"{DSA_BASE_URL}/api/v1/analysis/analyze",
            json={"stock_code": stock_code, "report_type": "detailed", "async_mode": True}
        )
        if resp.status_code not in (200, 202):
            raise HTTPException(status_code=502, detail=f"DSA 服务响应异常: {resp.status_code}")

        task_data = resp.json()
        task_id = task_data.get("task_id")
        if not task_id:
            raise HTTPException(status_code=502, detail="DSA 未返回任务 ID")

    # 轮询等待分析完成（最多 5 分钟）
    max_wait = 300
    waited = 0
    interval = 5

    with httpx.Client(timeout=30) as client:
        while waited < max_wait:
            time.sleep(interval)
            waited += interval

            status_resp = client.get(f"{DSA_BASE_URL}/api/v1/analysis/status/{task_id}")
            if status_resp.status_code != 200:
                continue

            status_data = status_resp.json()
            status = status_data.get("status")

            if status == "completed":
                result = status_data.get("result", {})
                report = result.get("report", {})
                return {
                    "success": True,
                    "stock_code": stock_code,
                    "stock_name": result.get("stock_name", ""),
                    "summary": report.get("summary", {}),
                    "strategy": report.get("strategy", {}),
                    "meta": report.get("meta", {}),
                }
            elif status == "failed":
                raise HTTPException(
                    status_code=502,
                    detail=f"DSA 分析失败: {status_data.get('error', 'unknown')}"
                )

    raise HTTPException(status_code=504, detail="DSA 分析超时，请稍后重试")
