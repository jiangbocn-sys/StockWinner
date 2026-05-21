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
    """刷新持仓当前价为最新行情，并返回更新后的持仓列表

    使用 gateway 批量获取行情（一次 SDK 调用），替代逐个查询。
    """
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

    if not positions:
        account_data = await db.fetchone(
            "SELECT available_cash FROM accounts WHERE account_id = ?",
            (account_id,)
        )
        available_cash = float(account_data["available_cash"]) if account_data else 0.0
        return {
            "success": True,
            "account_id": account_id,
            "positions": [],
            "available_cash": available_cash
        }

    # 批量获取所有持仓股票的行情（一次 SDK 调用）
    stock_codes = [pos["stock_code"] for pos in positions]
    try:
        from services.trading.gateway import get_gateway
        gateway = await get_gateway()
        market_data = await gateway.get_batch_market_data(stock_codes)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"批量获取行情失败: {e}")

    # 更新每个持仓的价格
    update_list = []
    price_map = {}
    for pos in positions:
        code = pos["stock_code"]
        md = market_data.get(code)
        price = md.current_price if md and md.current_price and md.current_price > 0 else None

        if price and price > 0:
            quantity = pos["quantity"]
            avg_cost = pos["avg_cost"]
            new_market_value = price * quantity
            new_profit_loss = new_market_value - (avg_cost * quantity)
            update_list.append((
                price, new_market_value, new_profit_loss,
                datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8))).isoformat(),
                pos["id"]
            ))
            price_map[code] = price
        else:
            price_map[code] = pos.get("current_price")

    # 批量更新数据库
    if update_list:
        await db.executemany(
            "UPDATE stock_positions SET current_price = ?, market_value = ?, profit_loss = ?, updated_at = ? WHERE id = ?",
            update_list
        )

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

    # 从内存价格缓存注入实时现价
    try:
        from services.common.price_cache import get_price_cache
        cache = get_price_cache()
        cached_prices = cache.get_all_for_account(account_id)
        for pos in positions:
            code = pos.get("stock_code")
            if code and code in cached_prices:
                pos["current_price"] = cached_prices[code]
                # 重新计算市值和盈亏
                qty = pos.get("quantity", 0)
                avg = pos.get("avg_cost", 0)
                price = cached_prices[code]
                pos["market_value"] = round(price * qty, 2)
                pos["profit_loss"] = round((price - avg) * qty, 2)
    except Exception:
        pass

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


@router.get("/api/v1/ui/{account_id}/positions/strategy-stats")
async def get_strategy_position_stats(
    account_id: str = Path(..., description="账户 ID"),
):
    """按策略分组统计持仓"""
    db = get_db_manager()

    account = await db.fetchone(
        "SELECT available_cash FROM accounts WHERE account_id = ? AND is_active = 1",
        (account_id,)
    )
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在：{account_id}")

    available_cash = account.get("available_cash", 0)

    # 查询总资产（持仓市值 + 可用资金）
    pos = await db.fetchone(
        "SELECT SUM(market_value) as total_mv FROM stock_positions WHERE account_id = ? AND quantity > 0",
        (account_id,)
    )
    total_mv = pos["total_mv"] if pos and pos["total_mv"] else 0
    total_assets = total_mv + available_cash

    # 按策略分组统计
    rows = await db.fetchall("""
        SELECT strategy_id,
               COUNT(*) as position_count,
               SUM(market_value) as total_mv,
               SUM(profit_loss) as total_pnl
        FROM stock_positions
        WHERE account_id = ? AND quantity > 0
        GROUP BY strategy_id
    """, (account_id,))

    # 关联策略名称
    stats = []
    for row in rows:
        sid = row["strategy_id"]
        if sid:
            strategy = await db.fetchone(
                "SELECT name, config FROM strategies WHERE id = ? AND account_id = ?",
                (sid, account_id)
            )
            strategy_name = strategy["name"] if strategy else f"策略#{sid}"
            strategy_config = strategy.get("config", {}) if strategy else {}
            if isinstance(strategy_config, str):
                try:
                    import json
                    strategy_config = json.loads(strategy_config)
                except (json.JSONDecodeError, TypeError):
                    strategy_config = {}
        else:
            strategy_name = "手动买入"
            strategy_config = {}

        max_position_amount = strategy_config.get("max_position_amount")

        stats.append({
            "strategy_id": sid,
            "strategy_name": strategy_name,
            "position_count": row["position_count"],
            "total_mv": round(row["total_mv"] or 0, 2),
            "total_pnl": round(row["total_pnl"] or 0, 2),
            "position_pct": round((row["total_mv"] or 0) / total_assets * 100, 2) if total_assets > 0 else 0,
            "max_position_amount": max_position_amount,
        })

    return {
        "account_id": account_id,
        "total_assets": round(total_assets, 2),
        "total_mv": round(total_mv, 2),
        "available_cash": available_cash,
        "strategy_stats": stats,
    }


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


@router.get("/api/v1/ui/{account_id}/closed-positions")
async def get_closed_positions(
    account_id: str = Path(..., description="账户 ID"),
    limit: int = Query(50, description="返回数量限制")
):
    """获取已清仓股票明细

    通过 trade_records 聚合买卖记录，找出已全部卖出的股票，
    计算持有时间、交易成本、清仓收益、收益率、年化收益率。
    """
    db = get_db_manager()

    account = await db.fetchone(
        "SELECT 1 FROM accounts WHERE account_id = ? AND is_active = 1",
        (account_id,)
    )
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    # 1. 找出所有有买入记录的股票
    buy_stocks = await db.fetchall("""
        SELECT stock_code, stock_name,
               SUM(quantity) as total_buy_qty,
               SUM(amount) as total_buy_amount,
               SUM(commission) as total_buy_commission,
               MIN(trade_time) as first_buy_time,
               AVG(price) as avg_buy_price
        FROM trade_records
        WHERE account_id = ? AND trade_type = 'buy'
        GROUP BY stock_code
    """, (account_id,))

    if not buy_stocks:
        return {"success": True, "account_id": account_id, "closed_positions": []}

    # 2. 对每只股票，检查是否已清仓（卖出数量 = 买入数量）
    closed = []
    for stock in buy_stocks:
        code = stock["stock_code"]
        name = stock["stock_name"] or ""
        buy_qty = stock["total_buy_qty"]
        buy_amount = stock["total_buy_amount"]
        buy_commission = stock["total_buy_commission"]
        first_buy = stock["first_buy_time"]

        # 查询卖出记录
        sell_record = await db.fetchone("""
            SELECT SUM(quantity) as total_sell_qty,
                   SUM(amount) as total_sell_amount,
                   SUM(commission) as total_sell_commission,
                   MAX(trade_time) as last_sell_time
            FROM trade_records
            WHERE account_id = ? AND stock_code = ? AND trade_type = 'sell'
        """, (account_id, code))

        if not sell_record:
            continue

        sell_qty = sell_record["total_sell_qty"] or 0
        sell_amount = sell_record["total_sell_amount"] or 0.0
        sell_commission = sell_record["total_sell_commission"] or 0.0
        last_sell = sell_record["last_sell_time"]

        # 只展示已清仓的（卖出数量 >= 买入数量）
        if sell_qty < buy_qty:
            continue

        # 交易成本 = 买入佣金 + 卖出佣金 + 印花税等（卖出金额已扣除）
        total_cost = buy_amount + buy_commission + sell_commission
        total_revenue = sell_amount
        net_profit = total_revenue - total_cost
        profit_pct = (net_profit / total_cost * 100) if total_cost > 0 else 0.0

        # 持有天数
        try:
            if isinstance(first_buy, str):
                buy_dt = datetime.datetime.fromisoformat(first_buy.replace('+08:00', '+08:00'))
            else:
                buy_dt = first_buy
            if isinstance(last_sell, str):
                sell_dt = datetime.datetime.fromisoformat(last_sell.replace('+08:00', '+08:00'))
            else:
                sell_dt = last_sell
            holding_days = max((sell_dt - buy_dt).days, 1)
        except Exception:
            buy_dt = None
            sell_dt = None
            holding_days = 1

        # 年化收益率
        annualized_pct = ((1 + profit_pct / 100) ** (365 / holding_days) - 1) * 100 if holding_days > 0 else 0.0

        closed.append({
            "stock_code": code,
            "stock_name": name,
            "buy_quantity": buy_qty,
            "avg_buy_price": round(buy_amount / buy_qty, 3) if buy_qty > 0 else 0,
            "avg_sell_price": round(sell_amount / sell_qty, 3) if sell_qty > 0 else 0,
            "first_buy_time": str(first_buy)[:19],
            "last_sell_time": str(last_sell)[:19],
            "_sell_dt": sell_dt,
            "holding_days": holding_days,
            "total_cost": round(total_cost, 2),
            "total_revenue": round(total_revenue, 2),
            "total_commission": round(buy_commission + sell_commission, 2),
            "net_profit": round(net_profit, 2),
            "profit_pct": round(profit_pct, 2),
            "annualized_pct": round(annualized_pct, 2),
        })

    # 按清仓卖出时间倒序
    closed.sort(key=lambda x: x["_sell_dt"] or datetime.datetime.min, reverse=True)

    # 移除内部字段
    for item in closed:
        item.pop("_sell_dt", None)

    return {
        "success": True,
        "account_id": account_id,
        "closed_positions": closed[:limit],
        "total": len(closed)
    }


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
