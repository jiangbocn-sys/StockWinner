"""
持仓管理 API
"""

import httpx
import asyncio
import datetime
from fastapi import APIRouter, HTTPException, Path, Query, Body
from typing import Optional
from services.common.database import get_db_manager
from services.common.timezone import get_china_time

router = APIRouter()

DSA_BASE_URL = "http://localhost:8000"


def _get_latest_price(stock_code: str) -> Optional[float]:
    """通过 SDK 日K线查询获取股票最新价格（同步调用）

    query_kline(period=day) 包含当天K线数据，返回最新成交价/收盘价。
    比 snapshot 快 6-25 倍，且支持批量查询。
    """
    try:
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()

        code = stock_code
        if '.' not in code:
            code = f"{code}.SH" if code.startswith('6') else f"{code}.SZ"

        end_dt = get_china_time()
        begin_dt = end_dt - datetime.timedelta(days=2)
        end_date = int((end_dt + datetime.timedelta(days=1)).strftime('%Y%m%d'))
        begin_date = int(begin_dt.strftime('%Y%m%d'))

        kline_data = sdk_mgr.query_kline(
            code_list=[code],
            begin_date=begin_date,
            end_date=end_date,
            period=10008,  # Period.day
            task_type="query"
        )

        if kline_data and code in kline_data:
            df = kline_data[code]
            if len(df) > 0:
                price = float(df.iloc[-1].get('close', 0))
                if price > 0:
                    return price
    except Exception as e:
        print(f"获取 {stock_code} 行情失败: {e}")
    return None


@router.post("/api/v1/ui/{account_id}/positions/refresh-prices")
async def refresh_position_prices(
    account_id: str = Path(..., description="账户 ID"),
):
    """刷新持仓当前价为最新行情，并返回更新后的持仓列表"""
    db = get_db_manager()

    account = await db.fetchone(
        "SELECT 1 FROM accounts WHERE account_id = ? AND is_active = 1",
        (account_id,)
    )
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    positions = await db.fetchall(
        "SELECT * FROM stock_positions WHERE account_id = ? AND quantity > 0 ORDER BY stock_code",
        (account_id,)
    )

    updated = []
    for pos in positions:
        price = _get_latest_price(pos["stock_code"])
        if price and price > 0:
            # 更新数据库中的 current_price 及相关字段
            quantity = pos["quantity"]
            avg_cost = pos["avg_cost"]
            new_market_value = price * quantity
            new_profit_loss = new_market_value - (avg_cost * quantity)
            await db.execute(
                "UPDATE stock_positions SET current_price = ?, market_value = ?, profit_loss = ?, updated_at = ? WHERE id = ?",
                (price, new_market_value, new_profit_loss, datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8))).isoformat(), pos["id"])
            )
            updated.append({**pos, "current_price": price, "market_value": new_market_value, "profit_loss": new_profit_loss})
        else:
            updated.append(pos)

    # 重新读取最新数据返回
    positions = await db.fetchall(
        "SELECT * FROM stock_positions WHERE account_id = ? AND quantity > 0 ORDER BY stock_code",
        (account_id,)
    )

    account_data = await db.fetchone(
        "SELECT available_cash FROM accounts WHERE account_id = ?",
        (account_id,)
    )
    available_cash = float(account_data["available_cash"]) if account_data else 0.0

    return {
        "success": True,
        "account_id": account_id,
        "positions": positions,
        "available_cash": available_cash
    }


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
async def dsa_analyze_position(
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
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            f"{DSA_BASE_URL}/api/v1/analysis/analyze",
            json={"stock_code": stock_code, "report_type": "detailed", "async_mode": True}
        )
        if resp.status_code == 409:
            return {
                "success": False,
                "code": 409,
                "message": "该股票正在分析中，请稍后再查",
                "stock_code": stock_code,
            }
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

    async with httpx.AsyncClient(timeout=30) as client:
        while waited < max_wait:
            await asyncio.sleep(interval)
            waited += interval

            status_resp = await client.get(f"{DSA_BASE_URL}/api/v1/analysis/status/{task_id}")
            if status_resp.status_code != 200:
                continue

            status_data = status_resp.json()
            status = status_data.get("status")

            if status == "completed":
                # DSA 的 status 接口 result 为 null，需从 history 获取报告
                async with httpx.AsyncClient(timeout=30) as hist_client:
                    hist_resp = await hist_client.get(
                        f"{DSA_BASE_URL}/api/v1/history",
                        params={"query_id": task_id, "limit": 1}
                    )
                    if hist_resp.status_code == 200:
                        hist_data = hist_resp.json()
                        items = hist_data.get("items", [])
                        if items:
                            record_id = items[0]["id"]
                            report_resp = await hist_client.get(
                                f"{DSA_BASE_URL}/api/v1/history/{record_id}"
                            )
                            if report_resp.status_code == 200:
                                report = report_resp.json()
                                return {
                                    "success": True,
                                    "stock_code": stock_code,
                                    "stock_name": report.get("meta", {}).get("stock_name", ""),
                                    "summary": report.get("summary", {}),
                                    "strategy": report.get("strategy", {}),
                                    "meta": report.get("meta", {}),
                                }

                raise HTTPException(status_code=502, detail="DSA 分析完成但无法获取报告")
            elif status == "failed":
                raise HTTPException(
                    status_code=502,
                    detail=f"DSA 分析失败: {status_data.get('error', 'unknown')}"
                )

    raise HTTPException(status_code=504, detail="DSA 分析超时，请稍后重试")


@router.post("/api/v1/ui/{account_id}/stocks/{stock_code}/dsa-analyze")
async def dsa_analyze_any_stock(
    account_id: str = Path(...),
    stock_code: str = Path(...),
):
    """对任意股票调用 DSA 分析（不要求有持仓）"""
    db = get_db_manager()
    account = await db.fetchone(
        "SELECT 1 FROM accounts WHERE account_id = ? AND is_active = 1",
        (account_id,)
    )
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在：{account_id}")

    # 提交 DSA 分析任务
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            f"{DSA_BASE_URL}/api/v1/analysis/analyze",
            json={"stock_code": stock_code, "report_type": "detailed", "async_mode": True}
        )
        if resp.status_code == 409:
            return {
                "success": False,
                "code": 409,
                "message": "该股票正在分析中，请稍后再查",
                "stock_code": stock_code,
            }
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

    async with httpx.AsyncClient(timeout=30) as client:
        while waited < max_wait:
            await asyncio.sleep(interval)
            waited += interval

            status_resp = await client.get(f"{DSA_BASE_URL}/api/v1/analysis/status/{task_id}")
            if status_resp.status_code != 200:
                continue

            status_data = status_resp.json()
            status = status_data.get("status")

            if status == "completed":
                async with httpx.AsyncClient(timeout=30) as hist_client:
                    hist_resp = await hist_client.get(
                        f"{DSA_BASE_URL}/api/v1/history",
                        params={"query_id": task_id, "limit": 1}
                    )
                    if hist_resp.status_code == 200:
                        hist_data = hist_resp.json()
                        items = hist_data.get("items", [])
                        if items:
                            record_id = items[0]["id"]
                            report_resp = await hist_client.get(
                                f"{DSA_BASE_URL}/api/v1/history/{record_id}"
                            )
                            if report_resp.status_code == 200:
                                report = report_resp.json()
                                return {
                                    "success": True,
                                    "stock_code": stock_code,
                                    "stock_name": report.get("meta", {}).get("stock_name", ""),
                                    "summary": report.get("summary", {}),
                                    "strategy": report.get("strategy", {}),
                                    "meta": report.get("meta", {}),
                                }

                raise HTTPException(status_code=502, detail="DSA 分析完成但无法获取报告")
            elif status == "failed":
                raise HTTPException(
                    status_code=502,
                    detail=f"DSA 分析失败: {status_data.get('error', 'unknown')}"
                )

    raise HTTPException(status_code=504, detail="DSA 分析超时，请稍后重试")
