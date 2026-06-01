"""
市场行情数据 API
提供实时行情查询、批量行情查询、K 线历史数据查询等功能
"""

from fastapi import APIRouter, HTTPException, Path, Body, Query
from typing import List, Optional, Dict, Any
from datetime import timedelta
from services.common.database import get_db_manager, get_sync_connection
from services.trading.gateway import get_gateway

router = APIRouter()

import asyncio


# ============================================================
# K线合成辅助函数
# ============================================================

def _synthesize_week_from_daily(
    stock_code: str, week_start: str, week_end: str, today_dt
) -> Optional[Dict]:
    """从日线数据合成当周K线

    Args:
        stock_code: 股票代码
        week_start: 周起始日期 YYYY-MM-DD（周一）
        week_end: 周结束日期 YYYY-MM-DD（周五）
        today_dt: 今日日期对象

    Returns:
        合成的周K线数据，包含 trade_date, open, high, low, close, volume, amount
    """
    from services.common.timezone import get_china_time
    from services.trading.trading_hours import is_today_trading_day

    conn = get_sync_connection("kline")
    cursor = conn.cursor()

    # 查询本周已有日线数据
    today_str = today_dt.strftime("%Y-%m-%d")
    cursor.execute("""
        SELECT trade_date, open, close, low, high, volume, amount
        FROM kline_data
        WHERE stock_code = ? AND trade_date >= ? AND trade_date <= ?
        ORDER BY trade_date ASC
    """, (stock_code, week_start, today_str))
    rows = cursor.fetchall()

    if not rows:
        return None

    # 从日线数据聚合
    first_open = float(rows[0]["open"])
    last_close = float(rows[-1]["close"])
    max_high = max(float(r["high"]) for r in rows)
    min_low = min(float(r["low"]) for r in rows)
    total_volume = sum(float(r["volume"]) if r["volume"] else 0 for r in rows)
    total_amount = sum(float(r["amount"]) if r["amount"] else 0 for r in rows)

    # 如果是交易日且当日数据不完整（PriceCache中有最新行情），补充当日数据
    if is_today_trading_day() and today_str > rows[-1]["trade_date"]:
        try:
            from services.common.price_cache import get_price_cache
            cache = get_price_cache()
            ohlcv = cache.get_ohlcv(stock_code)
            if ohlcv and ohlcv.get('close', 0) > 0:
                today_open = ohlcv.get('open', ohlcv.get('close', 0))
                today_high = ohlcv.get('high', ohlcv.get('close', 0))
                today_low = ohlcv.get('low', ohlcv.get('close', 0))
                today_close = ohlcv.get('close', 0)
                today_volume = ohlcv.get('volume', 0)
                today_amount = ohlcv.get('amount', 0)

                # 更新聚合结果
                last_close = today_close
                max_high = max(max_high, today_high)
                min_low = min(min_low, today_low)
                total_volume += today_volume
                total_amount += today_amount
        except Exception:
            pass

    return {
        "trade_date": week_end.replace("-", ""),
        "open": round(first_open, 2),
        "close": round(last_close, 2),
        "low": round(min_low, 2),
        "high": round(max_high, 2),
        "volume": round(total_volume, 0),
        "amount": round(total_amount, 2),
    }


def _synthesize_month_from_weekly(
    stock_code: str, month_start: str, month_end: str, today_dt, km
) -> Optional[Dict]:
    """从周线数据合成当月K线

    Args:
        stock_code: 股票代码
        month_start: 月起始日期 YYYY-MM-DD
        month_end: 月结束日期 YYYY-MM-DD
        today_dt: 今日日期对象
        km: KlineManager 实例

    Returns:
        合成的月K线数据，包含 trade_date, open, high, low, close, volume, amount
    """
    from services.common.timezone import get_china_time
    from services.trading.trading_hours import is_today_trading_day
    import pandas as pd

    # 查询当月已有周线数据
    weekly_df = km.get_weekly_data(stock_code, limit=None)

    # 筛选当月周线（week_end_date 在 month_start 到 month_end 之间）
    current_month_weeks = []
    today_str = today_dt.strftime("%Y-%m-%d")
    for _, row in weekly_df.iterrows():
        week_end = row.get('week_end_date', '')
        if week_end and week_end >= month_start and week_end <= today_str:
            current_month_weeks.append(row)

    if not current_month_weeks:
        return None

    # 从周线聚合
    first_open = float(current_month_weeks[0]["open"])
    last_close = float(current_month_weeks[-1]["close"])
    max_high = max(float(w["high"]) for w in current_month_weeks)
    min_low = min(float(w["low"]) for w in current_month_weeks)
    total_volume = sum(float(w["volume"]) if w["volume"] else 0 for w in current_month_weeks)
    total_amount = sum(float(w["amount"]) if w["amount"] else 0 for w in current_month_weeks)

    # 如果是交易日且当日数据不完整，需要补充当日数据
    # 但周线可能还没更新到本周，所以需要从本周日线合成
    if is_today_trading_day():
        # 计算本周起始和结束
        now = get_china_time()
        current_week_end = now - timedelta(days=(now.isoweekday() - 5) % 7)
        current_week_start = current_week_end - timedelta(days=4)
        current_week_start_str = current_week_start.strftime("%Y-%m-%d")
        current_week_end_str = current_week_end.strftime("%Y-%m-%d")

        # 检查本周是否已在周线中
        week_end_dates = [w.get('week_end_date', '') for w in current_month_weeks]
        if current_week_end_str not in week_end_dates:
            # 合成本周数据并追加到月线聚合
            current_week_kline = _synthesize_week_from_daily(
                stock_code, current_week_start_str, current_week_end_str, today_dt
            )
            if current_week_kline:
                last_close = current_week_kline["close"]
                max_high = max(max_high, current_week_kline["high"])
                min_low = min(min_low, current_week_kline["low"])
                total_volume += current_week_kline["volume"]
                total_amount += current_week_kline["amount"]

    return {
        "trade_date": month_start[:7].replace("-", ""),  # YYYYMM
        "open": round(first_open, 2),
        "close": round(last_close, 2),
        "low": round(min_low, 2),
        "high": round(max_high, 2),
        "volume": round(total_volume, 0),
        "amount": round(total_amount, 2),
    }


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
    """获取单只股票实时行情（缓存优先 + 后台刷新）"""
    db = get_db_manager()

    # 验证账户
    account = await db.fetchone(
        "SELECT 1 FROM accounts WHERE account_id = ? AND is_active = 1",
        (account_id,)
    )
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    try:
        from services.common.price_cache import get_price_cache
        from services.common.stock_code import normalize_stock_code
        cache = get_price_cache()
        norm_code = normalize_stock_code(stock_code)

        # 读缓存
        ohlcv = cache.get_ohlcv_with_ttl(norm_code)

        if ohlcv:
            data = ohlcv['data']
            is_fresh = ohlcv['is_fresh']
            result_data = {
                "stock_code": norm_code,
                "stock_name": "",  # 缓存中无名称
                "current_price": data.get('close'),
                "change_percent": data.get('change_pct'),
                "high": data.get('high'),
                "low": data.get('low'),
                "open_price": data.get('open'),
                "prev_close": data.get('prev_close'),
                "volume": data.get('volume'),
                "amount": data.get('amount'),
                "bid": [],
                "ask": [],
            }
        else:
            result_data = {
                "stock_code": norm_code,
                "stock_name": "",
                "current_price": None,
                "change_percent": None,
                "high": None,
                "low": None,
                "open_price": None,
                "prev_close": None,
                "volume": None,
                "amount": None,
                "bid": [],
                "ask": [],
            }
            is_fresh = False

        # 缓存不新鲜时触发后台刷新（用户请求，high priority）
        if not is_fresh:
            async def _bg_refresh_single():
                try:
                    gw = await get_gateway()
                    gw.subscribe(f"quote:{norm_code}", {norm_code}, refresh_interval=0, priority=1)
                    await gw.refresh_now(f"quote:{norm_code}")
                    gw.unsubscribe(f"quote:{norm_code}")
                except Exception:
                    pass
            asyncio.create_task(_bg_refresh_single())

            # 后台刷新后再次从缓存读取补全
            async def _bg_update():
                try:
                    await asyncio.sleep(2)
                    updated = cache.get_ohlcv(norm_code)
                    if updated:
                        result_data['current_price'] = updated.get('close')
                        result_data['change_percent'] = updated.get('change_pct')
                        result_data['high'] = updated.get('high')
                        result_data['low'] = updated.get('low')
                        result_data['open_price'] = updated.get('open')
                        result_data['volume'] = updated.get('volume')
                        result_data['amount'] = updated.get('amount')
                except Exception:
                    pass
            asyncio.create_task(_bg_update())

        return {
            "success": True,
            "data": result_data,
            "price_fresh": is_fresh
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
    """批量获取股票实时行情（缓存优先 + 后台刷新）"""
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
        from services.common.price_cache import get_price_cache
        from services.common.stock_code import normalize_stock_code
        cache = get_price_cache()

        norm_codes = [normalize_stock_code(c) for c in stock_codes]
        freshness = cache.get_batch_freshness(set(norm_codes))
        stale_codes = {c for c, fresh in freshness.items() if not fresh}

        quotes = []
        for code in norm_codes:
            ohlcv = cache.get_ohlcv(code)
            if ohlcv:
                quotes.append({
                    "stock_code": code,
                    "stock_name": "",
                    "current_price": ohlcv.get('close'),
                    "change_percent": ohlcv.get('change_pct'),
                    "high": ohlcv.get('high'),
                    "low": ohlcv.get('low'),
                    "open_price": ohlcv.get('open'),
                    "prev_close": ohlcv.get('prev_close'),
                    "volume": ohlcv.get('volume'),
                    "amount": ohlcv.get('amount'),
                })
            else:
                quotes.append({
                    "stock_code": code,
                    "stock_name": "",
                    "current_price": None,
                    "change_percent": None,
                    "high": None,
                    "low": None,
                    "open_price": None,
                    "prev_close": None,
                    "volume": None,
                    "amount": None,
                })

        # 后台刷新 stale codes（用户请求，high priority）
        if stale_codes:
            async def _bg_refresh_batch():
                try:
                    gw = await get_gateway()
                    sub_id = f"api:{account_id}"
                    gw.subscribe(sub_id, stale_codes, refresh_interval=0, priority=1)
                    await gw.refresh_now(sub_id)
                    gw.unsubscribe(sub_id)
                except Exception:
                    pass
            asyncio.create_task(_bg_refresh_batch())

        return {
            "success": True,
            "data": {
                "quotes": quotes,
                "count": len(quotes),
                "failed": list(stale_codes) if stale_codes else [],
                "failed_count": len(stale_codes),
                "price_freshness": {
                    "total": len(norm_codes),
                    "fresh": len(norm_codes) - len(stale_codes),
                    "stale_count": len(stale_codes)
                }
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
            limit=limit,
            priority=1  # 用户请求，high priority
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
    months: int = Query(12, ge=0, le=60, description="回溯月数，默认 12 个月（1年），0 表示不限制"),
    period: str = Query("day", description="周期: day/week/month"),
    adjust: str = Query("none", description="复权: none/forward"),
    include_factors: bool = Query(False, description="是否包含因子数据（技术指标）"),
    factor_fields: Optional[str] = Query(None, description="因子字段，逗号分隔，默认主要技术指标"),
):
    """
    从本地 kline.db 读取 K 线数据（快速，不占用 SDK 连接）
    用于 Watchlist 弹窗、回测弹窗等需要频繁切换查看的场景

    Args:
        period: 周期类型
            - day: 日线（从 kline_data 表）
            - week: 周线（从 weekly_kline_data 表）
            - month: 月线（从周线合成）
        adjust: 复权类型
            - none: 不复权（原始价格）
            - forward: 前复权（历史价格调整，以当前价格为基准）
        include_factors: 是否同时返回因子数据（用于技术指标叠加）
        factor_fields: 指定因子字段，如 "ma5,ma10,boll_upper"
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

    # 验证参数
    if period not in ["day", "week", "month"]:
        raise HTTPException(status_code=400, detail=f"不支持的周期: {period}")
    if adjust not in ["none", "forward"]:
        raise HTTPException(status_code=400, detail=f"不支持的复权类型: {adjust}")

    from datetime import datetime, timedelta
    from services.common.timezone import get_china_time
    from services.factors.kline_manager import get_kline_manager

    km = get_kline_manager()
    end_dt = get_china_time()
    start_dt = end_dt - timedelta(days=months * 30)
    start_str = start_dt.strftime("%Y-%m-%d")
    end_str = end_dt.strftime("%Y-%m-%d")

    try:
        kline = []

        if period == "day":
            # 日线：从 kline_data 表查询
            conn = get_sync_connection("kline")
            cursor = conn.cursor()
            cursor.execute("""
                SELECT trade_date, open, close, low, high, volume, amount
                FROM kline_data
                WHERE stock_code = ? AND trade_date >= ? AND trade_date <= ?
                ORDER BY trade_date ASC
            """, (stock_code, start_str, end_str))
            rows = cursor.fetchall()

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

            # 拼接当日实时 K 线（从 PriceCache 取，不调 SDK）
            # 交易日过了开盘时间（9:30）就拼接当日数据（包括盘后时间）
            # 非交易日不拼接
            if not seen_today:
                try:
                    from services.common.price_cache import get_price_cache
                    from services.trading.trading_hours import is_today_trading_day, get_trading_phase
                    from services.common.timezone import get_china_time

                    # 交易日且过了开盘时间（9:30之后）才拼接当日数据
                    if is_today_trading_day():
                        now = get_china_time()
                        # 开盘时间是 9:30，过了这个时间点就有当日数据
                        market_open_time = now.replace(hour=9, minute=30, second=0, microsecond=0)
                        if now >= market_open_time:
                            cache = get_price_cache()
                            ohlcv = cache.get_ohlcv(stock_code)
                            if ohlcv and ohlcv.get('close', 0) > 0:
                                kline.append({
                                    "trade_date": today_str.replace("-", ""),
                                    "open": ohlcv.get('open', ohlcv.get('close', 0)),
                                    "close": ohlcv.get('close', 0),
                                    "low": ohlcv.get('low', ohlcv.get('close', 0)),
                                    "high": ohlcv.get('high', ohlcv.get('close', 0)),
                                    "volume": ohlcv.get('volume', 0),
                                    "amount": ohlcv.get('amount', 0),
                                })
                except Exception:
                    pass

        elif period == "week":
            # 周线：从 weekly_kline_data 表查询（limit 参数控制根数）
            # months 参数转为 limit（默认 250 根约 5 年）
            limit = max(months * 5, 250) if months > 0 else 250
            weekly_df = km.get_weekly_data(stock_code, limit=limit)
            for _, row in weekly_df.iterrows():
                week_end = row.get('week_end_date', '')
                if not week_end:
                    continue
                kline.append({
                    "trade_date": week_end.replace("-", ""),
                    "open": float(row.get('open', 0)),
                    "close": float(row.get('close', 0)),
                    "low": float(row.get('low', 0)),
                    "high": float(row.get('high', 0)),
                    "volume": float(row.get('volume', 0)) if row.get('volume') else 0,
                    "amount": float(row.get('amount', 0)) if row.get('amount') else 0,
                })

            # 检查是否有当周数据，如果没有则合成
            if kline:
                from datetime import datetime
                from services.common.timezone import get_china_time

                now = get_china_time()
                # 计算本周的 ISO 周结束日期（周五）
                # ISO weekday: 1=周一, 7=周日。本周五是 weekday=5
                current_week_end = now - timedelta(days=(now.isoweekday() - 5) % 7)
                current_week_end_str = current_week_end.strftime("%Y-%m-%d")

                # 最新周线的 week_end_date
                latest_week_end = kline[-1]["trade_date"][:8]  # YYYYMMDD
                latest_week_end_str = f"{latest_week_end[:4]}-{latest_week_end[4:6]}-{latest_week_end[6:8]}"

                # 如果最新周线不是本周，合成当周数据
                if latest_week_end_str < current_week_end_str:
                    # 计算本周起始日期（周一）
                    current_week_start = current_week_end - timedelta(days=4)
                    current_week_start_str = current_week_start.strftime("%Y-%m-%d")

                    # 从日线数据合成本周K线
                    current_week_kline = _synthesize_week_from_daily(
                        stock_code, current_week_start_str, current_week_end_str, end_dt
                    )
                    if current_week_kline:
                        kline.append(current_week_kline)

        elif period == "month":
            # 月线：从全部周线合成（不限制）
            monthly_data = km.get_monthly_data(stock_code, limit=None)
            for m in monthly_data:
                kline.append({
                    "trade_date": m['month'].replace("-", ""),  # YYYYMM
                    "open": float(m['open']),
                    "close": float(m['close']),
                    "low": float(m['low']),
                    "high": float(m['high']),
                    "volume": float(m['volume']),
                    "amount": float(m['amount']),
                })

            # 检查是否有当月数据，如果没有则合成
            if kline:
                from datetime import datetime
                from services.common.timezone import get_china_time

                now = get_china_time()
                current_month_str = now.strftime("%Y-%m")  # YYYY-MM

                # 最新月线的月份
                latest_month = kline[-1]["trade_date"][:6]  # YYYYMM
                latest_month_str = f"{latest_month[:4]}-{latest_month[4:6]}"

                # 如果最新月线不是当月，合成当月数据
                if latest_month_str < current_month_str:
                    # 计算当月起止日期
                    month_start_str = f"{current_month_str}-01"
                    # 月末日期：下月第一天减1天
                    import calendar
                    year, month = now.year, now.month
                    month_end_day = calendar.monthrange(year, month)[1]
                    month_end_str = f"{current_month_str}-{month_end_day}"

                    # 从周线/日线合成当月K线
                    current_month_kline = _synthesize_month_from_weekly(
                        stock_code, month_start_str, month_end_str, end_dt, km
                    )
                    if current_month_kline:
                        kline.append(current_month_kline)

        # 应用前复权（使用累计复权因子）
        if adjust == "forward" and kline:
            try:
                from services.trading.gateway import get_gateway
                gateway = await get_gateway()
                adj_factors = await gateway.get_adj_factor([stock_code])

                if adj_factors:
                    # SDK 返回单次复权因子（只有除权除息日才不为 1.0）
                    # 需要先计算累计复权因子：cumprod(single_factor)
                    # 按日期升序排列
                    sorted_factors = sorted(adj_factors, key=lambda x: str(x.get('trade_date', '')))

                    # 计算累计因子
                    cumulative = 1.0
                    cumulative_map = {}  # 日期 → 累计因子
                    for f in sorted_factors:
                        td = str(f.get('trade_date', '')).replace("-", "")[:8]  # YYYYMMDD
                        single_factor = float(f.get('adj_factor', 1.0))
                        cumulative *= single_factor
                        cumulative_map[td] = cumulative

                    # 最新累计因子（用于前复权基准）
                    latest_cumulative = cumulative

                    # 应用前复权公式
                    # 前复权价格 = 原价 × (当日累计因子 / 最新累计因子)
                    for k in kline:
                        td = k['trade_date'][:8]  # YYYYMMDD

                        # 找到该日期的累计因子（如果没有精确匹配，找最近的早期日期）
                        # 因为累计因子是单调递增的，没有除权的日子因子与前一交易日相同
                        if td in cumulative_map:
                            factor = cumulative_map[td]
                        else:
                            # 找该日期之前最近的累计因子（因子在此期间不变）
                            earlier_dates = [d for d in cumulative_map.keys() if d <= td]
                            factor = cumulative_map[max(earlier_dates)] if earlier_dates else latest_cumulative

                        adj_ratio = factor / latest_cumulative
                        k['open'] = round(k['open'] * adj_ratio, 2)
                        k['high'] = round(k['high'] * adj_ratio, 2)
                        k['low'] = round(k['low'] * adj_ratio, 2)
                        k['close'] = round(k['close'] * adj_ratio, 2)
            except Exception as e:
                # 复权失败，返回未复权数据（不影响主流程）
                pass

        # 并行查询因子数据（如果需要）
        factors = None
        if include_factors and period == "day" and kline:
            import asyncio
            # 确定日期范围
            dates = [k['trade_date'][:8] for k in kline]
            start_date_raw = dates[0]
            end_date_raw = dates[-1]
            start_date_factor = f"{start_date_raw[:4]}-{start_date_raw[4:6]}-{start_date_raw[6:8]}"
            end_date_factor = f"{end_date_raw[:4]}-{end_date_raw[4:6]}-{end_date_raw[6:8]}"

            # 因子字段
            default_factor_fields = [
                "trade_date", "ma5", "ma10", "ma20", "ma60", "ma120", "ma250",
                "ema12", "ema26", "boll_upper", "boll_middle", "boll_lower"
            ]
            if factor_fields:
                requested_fields = factor_fields.split(",")
                if "trade_date" not in requested_fields:
                    requested_fields.insert(0, "trade_date")
                query_fields = requested_fields
            else:
                query_fields = default_factor_fields

            # 并行查询因子（使用线程池）
            def query_factors_sync():
                try:
                    conn = get_sync_connection("kline")
                    cursor = conn.cursor()
                    sql = f"SELECT {', '.join(query_fields)} FROM stock_daily_factors WHERE stock_code = ? AND trade_date >= ? AND trade_date <= ? ORDER BY trade_date ASC"
                    cursor.execute(sql, (stock_code, start_date_factor, end_date_factor))
                    rows = cursor.fetchall()
                    conn.close()
                    return [dict(row) for row in rows]
                except Exception:
                    return []

            factors = await asyncio.to_thread(query_factors_sync)

        return {
            "success": True,
            "stock_code": stock_code,
            "period": period,
            "adjust": adjust,
            "kline": kline,
            "count": len(kline),
            "factors": factors,
            "factor_count": len(factors) if factors else 0
        }
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
            limit=1,
            priority=1  # 用户请求，high priority
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


@router.get("/api/v1/ui/{account_id}/factors/{stock_code}")
async def get_stock_factors(
    account_id: str = Path(..., description="账户 ID"),
    stock_code: str = Path(..., description="股票代码"),
    start_date: Optional[str] = Query(None, description="开始日期 YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="结束日期 YYYY-MM-DD"),
    fields: Optional[str] = Query(None, description="指定字段，逗号分隔"),
):
    """获取股票因子数据（用于 K 线指标叠加）"""
    db = get_db_manager()

    # 验证账户
    account = await db.fetchone("SELECT * FROM accounts WHERE account_id = ? AND is_active = 1", (account_id,))
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    conn = get_sync_connection("kline")
    cursor = conn.cursor()

    # 构建查询
    try:
        # 默认字段：主要技术指标
        default_fields = [
            "trade_date", "ma5", "ma10", "ma20", "ma60", "ma120", "ma250",
            "ema12", "ema26", "boll_upper", "boll_middle", "boll_lower"
        ]

        if fields:
            # 用户指定字段
            requested_fields = fields.split(",")
            # 确保 trade_date 在列表中
            if "trade_date" not in requested_fields:
                requested_fields.insert(0, "trade_date")
            query_fields = requested_fields
        else:
            query_fields = default_fields

        # 构建 SQL
        sql = f"SELECT {', '.join(query_fields)} FROM stock_daily_factors WHERE stock_code = ?"
        params = [stock_code]

        # 日期格式转换：支持 YYYYMMDD 和 YYYY-MM-DD
        def normalize_date(d: str) -> str:
            if d and len(d) == 8 and d.isdigit():
                return f"{d[:4]}-{d[4:6]}-{d[6:8]}"
            return d

        if start_date:
            sql += " AND trade_date >= ?"
            params.append(normalize_date(start_date))
        if end_date:
            sql += " AND trade_date <= ?"
            params.append(normalize_date(end_date))

        sql += " ORDER BY trade_date ASC"

        cursor.execute(sql, params)
        rows = cursor.fetchall()

        factors = []
        for row in rows:
            factor_dict = {}
            for field in query_fields:
                factor_dict[field] = row[field]
            factors.append(factor_dict)

        return {
            "success": True,
            "stock_code": stock_code,
            "count": len(factors),
            "factors": factors
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"查询因子数据失败: {str(e)}")
    finally:
        conn.close()
