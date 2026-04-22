"""
交易记录 API
"""

from fastapi import APIRouter, HTTPException, Path, Query
from typing import Optional
from datetime import datetime, timezone, timedelta, timedelta
from services.common.account_manager import get_account_manager
from services.common.database import get_db_manager

# 中国时区
CHINA_TZ = timezone(timedelta(hours=8))

def get_china_time():
    """获取中国时区时间"""
    return datetime.now(CHINA_TZ).replace(tzinfo=None)

router = APIRouter()


@router.get("/api/v1/ui/{account_id}/trades/today")
async def get_trades_today(account_id: str = Path(..., description="账户 ID")):
    """获取今日交易记录"""
    account_manager = get_account_manager()

    if not await account_manager.validate_account(account_id):
        raise HTTPException(status_code=404, detail=f"账户不存在：{account_id}")

    db = get_db_manager()
    today = get_china_time().strftime("%Y-%m-%d")

    # 统计汇总
    stats = await db.fetchone("""
        SELECT
            COUNT(*) as total_count,
            SUM(CASE WHEN trade_type = 'buy' THEN 1 ELSE 0 END) as buy_count,
            SUM(CASE WHEN trade_type = 'sell' THEN 1 ELSE 0 END) as sell_count,
            SUM(amount) as total_amount
        FROM trade_records
        WHERE account_id = ? AND DATE(trade_time) = ?
    """, (account_id, today))

    # 明细列表
    trades = await db.fetchall("""
        SELECT * FROM trade_records
        WHERE account_id = ? AND DATE(trade_time) = ?
        ORDER BY trade_time DESC LIMIT 50
    """, (account_id, today))

    return {
        "account_id": account_id,
        "date": today,
        "stats": stats,
        "trades": trades
    }


@router.get("/api/v1/ui/{account_id}/trades")
async def get_trades(
    account_id: str = Path(..., description="账户 ID"),
    start_date: Optional[str] = Query(None, description="开始日期 YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="结束日期 YYYY-MM-DD"),
    stock_code: Optional[str] = Query(None, description="股票代码过滤"),
    trade_type: Optional[str] = Query(None, description="交易类型 buy/sell"),
    limit: int = Query(50, description="返回数量限制")
):
    """获取交易记录（支持筛选）"""
    account_manager = get_account_manager()

    if not await account_manager.validate_account(account_id):
        raise HTTPException(status_code=404, detail=f"账户不存在：{account_id}")

    db = get_db_manager()

    # 构建查询条件
    conditions = ["account_id = ?"]
    params = [account_id]

    if start_date:
        conditions.append("DATE(trade_time) >= ?")
        params.append(start_date)

    if end_date:
        conditions.append("DATE(trade_time) <= ?")
        params.append(end_date)

    if stock_code:
        conditions.append("stock_code = ?")
        params.append(stock_code)

    if trade_type:
        conditions.append("trade_type = ?")
        params.append(trade_type)

    where_clause = " AND ".join(conditions)

    trades = await db.fetchall(
        f"SELECT * FROM trade_records WHERE {where_clause} ORDER BY trade_time DESC LIMIT ?",
        params + [limit]
    )

    return {
        "account_id": account_id,
        "trades": trades,
        "count": len(trades)
    }
