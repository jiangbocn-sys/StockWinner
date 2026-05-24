"""
市场行情数据 API
提供实时行情查询、批量行情查询、K 线历史数据查询等功能
"""

from fastapi import APIRouter, HTTPException, Path, Body, Query
from typing import List, Optional, Dict, Any
from services.common.database import get_db_manager, get_sync_connection
from services.trading.gateway import get_gateway

router = APIRouter()

import asyncio

@router.get("/api/v1/ui/{account_id}/market/test-query-kline")
async def test_query_kline_batch(
    account_id: str = Path(...),
    count: int = Query(100, ge=1, le=10000, description="测试批量大小"),
    direct: bool = Query(False, description="是否直接调用 SDK（绕过 SDKManager）"),
):
    """
    临时测试端点：直接调用 SDKManager.query_kline 测试不同批量耗时
    通过 gateway 内部调用，在后端进程上下文中执行
    """
    import time
    from pathlib import Path
    from datetime import datetime, timedelta
    from services.common.timezone import get_china_time
    from services.common.sdk_manager import get_sdk_manager

    # 获取股票代码
    conn = get_sync_connection("kline")
    c = conn.cursor()
    c.execute(
        "SELECT DISTINCT stock_code FROM kline_data WHERE stock_code NOT LIKE '%.BJ' LIMIT ?",
        (count,)
    )
    codes = [r[0] for r in c.fetchall()]

    sdk = get_sdk_manager()
    end_dt = get_china_time()
    begin_dt = end_dt - timedelta(days=2)
    end_date = int((end_dt + timedelta(days=1)).strftime('%Y%m%d'))
    begin_date = int(begin_dt.strftime('%Y%m%d'))

    def _run_query():
        """在后台线程中执行 SDK 调用"""
        start = time.time()
        if direct:
            md = sdk.get_market_data()
            result = md.query_kline(
                code_list=codes,
                begin_date=begin_date,
                end_date=end_date,
                period=10008,
                task_type="download"
            )
        else:
            result = sdk.query_kline(
                code_list=codes,
                begin_date=begin_date,
                end_date=end_date,
                period=10008,
                task_type="download"
            )
        return result, time.time() - start

    try:
        # 在后台线程执行，600s 超时保护
        result, elapsed = await asyncio.wait_for(
            asyncio.to_thread(_run_query),
            timeout=600.0
        )
    except asyncio.TimeoutError:
        return {
            "success": False,
            "message": f"SDK 调用超时（>600s）",
            "requested": count,
            "actual_stocks": len(codes),
        }
    except Exception as e:
        return {
            "success": False,
            "message": f"{type(e).__name__}: {e}",
            "requested": count,
        }

    data_count = sum(len(df) if df is not None else 0 for df in result.values()) if isinstance(result, dict) else 0
    return {
        "success": True,
        "requested": count,
        "actual_stocks": len(codes),
        "success_stocks": len(result) if isinstance(result, dict) else 0,
        "total_records": data_count,
        "elapsed_seconds": round(elapsed, 2),
        "begin_date": begin_date,
        "end_date": end_date,
        "direct_call": direct,
    }


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
        from services.trading.gateway import get_gateway
        gw = await get_gateway()
        sub_id = f"api:{account_id}"
        gw.subscribe(sub_id, set(stock_codes), refresh_interval=0, priority=2)
        results = await gw.refresh_now(sub_id)
        gw.unsubscribe(sub_id)

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
    from services.common.timezone import get_china_time
    actual_start_date = start_date
    actual_end_date = end_date

    if time_range and not start_date and not end_date:
        end_dt = get_china_time()
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


@router.get("/api/v1/ui/{account_id}/stocks/{stock_code}/kline-local")
async def get_local_kline(
    account_id: str = Path(..., description="账户 ID"),
    stock_code: str = Path(..., description="股票代码，如 600519 或 600519.SH"),
    months: int = Query(6, ge=1, le=24, description="回溯月数，默认 6 个月"),
):
    """
    从本地 kline.db 读取 K 线数据（快速，不占用 SDK 连接）
    用于 Watchlist 弹窗、回测弹窗等需要频繁切换查看的场景
    """
    db = get_db_manager()
    account = await db.fetchone(
        "SELECT 1 FROM accounts WHERE account_id = ? AND is_active = 1", (account_id,)
    )
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    # 规范化股票代码
    if "." not in stock_code:
        stock_code = f"{stock_code}.SH" if stock_code.startswith("6") else f"{stock_code}.SZ"

    from datetime import datetime, timedelta, time as dt_time
    from services.common.timezone import get_china_time
    end_dt = get_china_time()
    start_dt = end_dt - timedelta(days=months * 30)
    start_str = start_dt.strftime("%Y-%m-%d")
    end_str = end_dt.strftime("%Y-%m-%d")

    try:
        conn = get_sync_connection("kline")
        cursor = conn.cursor()
        cursor.execute("""
            SELECT trade_date, open, close, low, high, volume, amount
            FROM kline_data
            WHERE stock_code = ? AND trade_date >= ? AND trade_date <= ?
            ORDER BY trade_date ASC
        """, (stock_code, start_str, end_str))
        rows = cursor.fetchall()

        kline = []
        seen_today = False
        today_str = end_dt.strftime("%Y-%m-%d")
        for r in rows:
            td = str(r["trade_date"])
            if td == today_str:
                seen_today = True
            kline.append({
                "trade_date": td.replace("-", ""),
                "open": float(r["open"]),
                "close": float(r["close"]),
                "low": float(r["low"]),
                "high": float(r["high"]),
                "volume": float(r["volume"]) if r["volume"] else 0,
                "amount": float(r["amount"]) if r["amount"] else 0,
            })

        # 交易时段拼接当日实时 K 线（周一~周五 9:30~15:00）
        weekday = end_dt.weekday()  # 0=Mon .. 4=Fri
        now_time = end_dt.time()
        is_trading_hours = (
            weekday < 5
            and dt_time(9, 30) <= now_time <= dt_time(15, 0)
        )
        if is_trading_hours and not seen_today:
            try:
                from services.trading.gateway import get_gateway
                gateway = await get_gateway()
                today_kline = await gateway.get_kline_data(
                    stock_code=stock_code,
                    period="day",
                    start_date=today_str.replace("-", ""),
                    end_date=today_str.replace("-", ""),
                    limit=1,
                )
                if today_kline and len(today_kline) > 0:
                    row = today_kline[0]
                    kline.append({
                        "trade_date": today_str.replace("-", ""),
                        "open": float(row.get("open", 0)),
                        "close": float(row.get("close", 0)),
                        "low": float(row.get("low", 0)),
                        "high": float(row.get("high", 0)),
                        "volume": float(row.get("volume", 0)),
                        "amount": float(row.get("amount", 0)),
                    })
            except Exception as e:
                # SDK 拼接失败不影响返回历史数据
                import logging
                logging.getLogger("market_data").debug(f"拼接当日 K 线失败: {e}")

        return {"success": True, "stock_code": stock_code, "kline": kline, "count": len(kline)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"读取本地 K 线数据失败：{str(e)}")


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


@router.get("/api/v1/ui/{account_id}/market/stock-info/{stock_code}")
async def get_stock_info(
    account_id: str = Path(...),
    stock_code: str = Path(..., description="股票代码（带后缀）"),
):
    """从本地 kline.db 查询股票名称和最新价格"""
    db = get_db_manager()
    account = await db.fetchone("SELECT 1 FROM accounts WHERE account_id = ? AND is_active = 1", (account_id,))
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    try:
        conn = get_sync_connection("kline")
        cursor = conn.cursor()
        # 优先从 monthly_factors 查（有 stock_name 和最新数据）
        cursor.execute(
            "SELECT stock_name, total_market_cap FROM stock_monthly_factors WHERE stock_code = ? LIMIT 1",
            (stock_code,)
        )
        row = cursor.fetchone()
        if row and row['stock_name']:
            # 再查最新K线价格
            cursor.execute(
                "SELECT close, trade_date FROM kline_data WHERE stock_code = ? ORDER BY trade_date DESC LIMIT 1",
                (stock_code,)
            )
            kline_row = cursor.fetchone()
            result = {
                "success": True,
                "stock_code": stock_code,
                "stock_name": row['stock_name'],
            }
            if kline_row:
                result["latest_price"] = kline_row['close']
                result["latest_date"] = kline_row['trade_date']
            return result

        # 备用：从 kline_data 查
        cursor.execute(
            "SELECT DISTINCT stock_name, close, trade_date FROM kline_data WHERE stock_code = ? ORDER BY trade_date DESC LIMIT 1",
            (stock_code,)
        )
        row = cursor.fetchone()
        if row and row['stock_name']:
            return {
                "success": True,
                "stock_code": stock_code,
                "stock_name": row['stock_name'],
                "latest_price": row['close'],
                "latest_date": row['trade_date'],
            }
    except Exception as e:
        pass

    return {"success": False, "message": "未找到股票信息"}
