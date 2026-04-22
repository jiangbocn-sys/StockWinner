"""
市场行情数据 API
提供实时行情查询、批量行情查询、K 线历史数据查询等功能
"""

from fastapi import APIRouter, HTTPException, Path, Body, Query
from typing import List, Optional, Dict, Any
from services.common.database import get_db_manager
from services.trading.gateway import get_gateway

router = APIRouter()


@router.get("/api/v1/ui/{account_id}/market/quote/{stock_code}")
async def get_stock_quote(
    account_id: str = Path(..., description="账户 ID"),
    stock_code: str = Path(..., description="股票代码，支持格式：600519 或 600519.SH")
):
    """获取单只股票实时行情"""
    db = get_db_manager()

    # 验证账户
    account = await db.fetchone(
        "SELECT 1 FROM accounts WHERE account_id = ? AND is_active = 1",
        (account_id,)
    )
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    try:
        gateway = await get_gateway()
        market_data = await gateway.get_market_data(stock_code)

        if not market_data:
            raise HTTPException(status_code=404, detail=f"无法获取 {stock_code} 的行情数据")

        return {
            "success": True,
            "data": {
                "stock_code": stock_code,
                "stock_name": market_data.stock_name,
                "current_price": market_data.current_price,
                "change_percent": market_data.change_percent,
                "high": market_data.high,
                "low": market_data.low,
                "open_price": market_data.open_price,
                "prev_close": market_data.prev_close,
                "volume": market_data.volume,
                "amount": market_data.amount,
                "bid": market_data.bid,
                "ask": market_data.ask
            }
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取行情失败：{str(e)}")


@router.post("/api/v1/ui/{account_id}/market/quotes")
async def get_batch_quotes(
    account_id: str = Path(..., description="账户 ID"),
    stock_codes: List[str] = Body(..., description="股票代码列表", embed=True)
):
    """批量获取股票实时行情"""
    db = get_db_manager()

    # 验证账户
    account = await db.fetchone(
        "SELECT 1 FROM accounts WHERE account_id = ? AND is_active = 1",
        (account_id,)
    )
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    if not stock_codes or len(stock_codes) == 0:
        raise HTTPException(status_code=400, detail="股票代码列表不能为空")

    if len(stock_codes) > 50:
        raise HTTPException(status_code=400, detail="单次最多查询 50 只股票")

    try:
        gateway = await get_gateway()

        # 批量获取行情
        results = await gateway.get_batch_market_data(stock_codes)

        quotes = []
        errors = []
        for code, data in results.items():
            if data:
                quotes.append({
                    "stock_code": code,
                    "stock_name": data.stock_name,
                    "current_price": data.current_price,
                    "change_percent": data.change_percent,
                    "high": data.high,
                    "low": data.low,
                    "open_price": data.open_price,
                    "prev_close": data.prev_close,
                    "volume": data.volume,
                    "amount": data.amount
                })
            else:
                errors.append(code)

        return {
            "success": True,
            "data": {
                "quotes": quotes,
                "count": len(quotes),
                "failed": errors,
                "failed_count": len(errors)
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"批量获取行情失败：{str(e)}")


@router.get("/api/v1/ui/{account_id}/market/kline")
async def get_kline_data(
    account_id: str = Path(..., description="账户 ID"),
    stock_code: str = Query(..., description="股票代码，支持格式：600519 或 600519.SH"),
    period: str = Query("day", description="K 线周期：1m/3m/5m/10m/15m/30m/60m/120m/day/week/month"),
    start_date: Optional[str] = Query(None, description="开始日期，格式：YYYYMMDD"),
    end_date: Optional[str] = Query(None, description="结束日期，格式：YYYYMMDD"),
    limit: int = Query(100, description="返回数量限制，默认 100", ge=1, le=10000),
    time_range: Optional[str] = Query(None, description="快捷时间范围：7d/30d/90d/180d/1y/2y/5y/10y/all")
):
    """
    获取 K 线历史数据

    支持的周期：
    - 分钟线：1m, 3m, 5m, 10m, 15m, 30m, 60m, 120m
    - 日线：day
    - 周线：week
    - 月线：month

    支持的时间范围快捷选择：
    - 7d: 最近 7 天
    - 30d: 最近 30 天
    - 90d: 最近 90 天
    - 180d: 最近 180 天
    - 1y: 最近 1 年
    - 2y: 最近 2 年
    - 5y: 最近 5 年
    - 10y: 最近 10 年
    - all: 全部可用数据

    优先级：start_date/end_date > time_range > limit
    """
    db = get_db_manager()

    # 验证账户
    account = await db.fetchone(
        "SELECT 1 FROM accounts WHERE account_id = ? AND is_active = 1",
        (account_id,)
    )
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    # 验证周期参数 - 扩展支持更多周期
    valid_periods = ["1m", "3m", "5m", "10m", "15m", "30m", "60m", "120m", "day", "week", "month"]
    if period not in valid_periods:
        raise HTTPException(
            status_code=400,
            detail=f"无效的周期参数，支持：{', '.join(valid_periods)}"
        )

    # 处理时间范围参数
    from datetime import datetime, timedelta
    actual_start_date = start_date
    actual_end_date = end_date

    if time_range and not start_date and not end_date:
        end_dt = datetime.now()
        if time_range == "7d":
            start_dt = end_dt - timedelta(days=7)
        elif time_range == "30d":
            start_dt = end_dt - timedelta(days=30)
        elif time_range == "90d":
            start_dt = end_dt - timedelta(days=90)
        elif time_range == "180d":
            start_dt = end_dt - timedelta(days=180)
        elif time_range == "1y":
            start_dt = end_dt - timedelta(days=365)
        elif time_range == "2y":
            start_dt = end_dt - timedelta(days=730)
        elif time_range == "5y":
            start_dt = end_dt - timedelta(days=1825)
        elif time_range == "10y":
            start_dt = end_dt - timedelta(days=3650)
        elif time_range == "all":
            start_dt = datetime(1990, 1, 1)
        else:
            raise HTTPException(
                status_code=400,
                detail=f"无效的时间范围参数，支持：7d/30d/90d/180d/1y/2y/5y/10y/all"
            )
        actual_start_date = start_dt.strftime("%Y%m%d")
        actual_end_date = end_dt.strftime("%Y%m%d")

    # 验证日期格式
    if actual_start_date:
        try:
            datetime.strptime(actual_start_date, "%Y%m%d")
        except ValueError:
            raise HTTPException(status_code=400, detail="开始日期格式错误，应为 YYYYMMDD")

    if actual_end_date:
        try:
            datetime.strptime(actual_end_date, "%Y%m%d")
        except ValueError:
            raise HTTPException(status_code=400, detail="结束日期格式错误，应为 YYYYMMDD")

    try:
        gateway = await get_gateway()
        kline_data = await gateway.get_kline_data(
            stock_code=stock_code,
            period=period,
            start_date=actual_start_date,
            end_date=actual_end_date,
            limit=limit
        )

        return {
            "success": True,
            "data": {
                "stock_code": stock_code,
                "period": period,
                "count": len(kline_data),
                "start_date": actual_start_date,
                "end_date": actual_end_date,
                "kline": kline_data
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取 K 线数据失败：{str(e)}")


@router.get("/api/v1/ui/{account_id}/market/kline/latest")
async def get_latest_kline(
    account_id: str = Path(..., description="账户 ID"),
    stock_code: str = Query(..., description="股票代码"),
    period: str = Query("day", description="K 线周期：1m/3m/5m/10m/15m/30m/60m/120m/day/week/month")
):
    """获取最新一根 K 线数据"""
    db = get_db_manager()

    # 验证账户
    account = await db.fetchone(
        "SELECT 1 FROM accounts WHERE account_id = ? AND is_active = 1",
        (account_id,)
    )
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    # 验证周期参数 - 扩展支持更多周期
    valid_periods = ["1m", "3m", "5m", "10m", "15m", "30m", "60m", "120m", "day", "week", "month"]
    if period not in valid_periods:
        raise HTTPException(
            status_code=400,
            detail=f"无效的周期参数，支持：{', '.join(valid_periods)}"
        )

    try:
        gateway = await get_gateway()
        kline_data = await gateway.get_kline_data(
            stock_code=stock_code,
            period=period,
            limit=1
        )

        if not kline_data or len(kline_data) == 0:
            raise HTTPException(status_code=404, detail="未找到 K 线数据")

        return {
            "success": True,
            "data": {
                "stock_code": stock_code,
                "period": period,
                "kline": kline_data[0]
            }
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取 K 线数据失败：{str(e)}")
