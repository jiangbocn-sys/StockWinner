"""
扩展数据服务 API

通过 SDK 提供大盘指数、行业板块、财报数据、龙虎榜、融资融券、大宗交易、国债收益率等数据查询。

路径前缀：/api/v1/ui/{account_id}/data/
"""

from fastapi import APIRouter, HTTPException, Path, Query
from typing import List, Optional
from datetime import datetime

from services.common.database import get_db_manager
from services.common.sdk_manager import get_sdk_manager

router = APIRouter()


# ================================================================
# 大盘指数
# ================================================================

@router.get("/api/v1/ui/{account_id}/data/index/list")
async def get_index_list(
    account_id: str = Path(..., description="账户 ID"),
):
    """指数代码列表（上交所+深交所指数）"""
    db = get_db_manager()
    account = await db.fetchone(
        "SELECT 1 FROM accounts WHERE account_id = ? AND is_active = 1", (account_id,)
    )
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    try:
        from services.trading.gateway import get_gateway
        gateway = await get_gateway()
        indices = await gateway.get_index_list()
        return {"success": True, "data": indices, "count": len(indices)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取指数列表失败：{str(e)}")


@router.get("/api/v1/ui/{account_id}/data/index/kline")
async def get_index_kline(
    account_id: str = Path(..., description="账户 ID"),
    index_code: str = Query(..., description="指数代码，如 000001.SH / 000300.SH"),
    period: str = Query("day", description="周期：day/week/month"),
    limit: int = Query(100, description="返回数量限制"),
):
    """指数 K 线数据"""
    db = get_db_manager()
    account = await db.fetchone(
        "SELECT 1 FROM accounts WHERE account_id = ? AND is_active = 1", (account_id,)
    )
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    try:
        from services.trading.gateway import get_gateway
        gateway = await get_gateway()
        kline_data = await gateway.get_kline_data(
            stock_code=index_code, period=period, limit=limit
        )
        return {"success": True, "data": {"index_code": index_code, "period": period, "kline": kline_data}}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取指数K线失败：{str(e)}")


# ================================================================
# 行业/板块
# ================================================================

@router.get("/api/v1/ui/{account_id}/data/industry/list")
async def get_industry_list(
    account_id: str = Path(..., description="账户 ID"),
    level: int = Query(1, description="行业级别：1=一级, 2=二级, 3=三级"),
):
    """申万行业分类列表"""
    db = get_db_manager()
    account = await db.fetchone(
        "SELECT 1 FROM accounts WHERE account_id = ? AND is_active = 1", (account_id,)
    )
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    try:
        sdk_mgr = get_sdk_manager()
        df = sdk_mgr.get_industry_base_info()
        if df.empty:
            return {"success": True, "data": [], "count": 0}
        filtered = df[df["LEVEL_TYPE"] == level]
        records = filtered.to_dict(orient="records")
        return {"success": True, "data": records, "count": len(records)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取行业列表失败：{str(e)}")


@router.get("/api/v1/ui/{account_id}/data/industry/kline")
async def get_industry_kline(
    account_id: str = Path(..., description="账户 ID"),
    index_code: str = Query(..., description="行业指数代码，如 801010.SI"),
):
    """行业指数日行情数据（含 OHLCV + PE/PB）"""
    db = get_db_manager()
    account = await db.fetchone(
        "SELECT 1 FROM accounts WHERE account_id = ? AND is_active = 1", (account_id,)
    )
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    try:
        sdk_mgr = get_sdk_manager()
        result = sdk_mgr.get_industry_daily(code_list=[index_code])
        if not result or index_code not in result:
            return {"success": True, "data": {"index_code": index_code, "kline": []}}
        df = result[index_code]
        # 索引转列，列名小写
        df = df.reset_index()
        df.columns = df.columns.str.lower()
        if "trade_date" in df.columns:
            df["trade_date"] = df["trade_date"].apply(
                lambda x: x.strftime("%Y-%m-%d") if hasattr(x, "strftime") else str(x)[:10]
            )
        records = df.to_dict(orient="records")
        return {"success": True, "data": {"index_code": index_code, "kline": records, "count": len(records)}}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取行业行情失败：{str(e)}")


@router.get("/api/v1/ui/{account_id}/data/industry/constituent")
async def get_industry_constituent(
    account_id: str = Path(..., description="账户 ID"),
    index_code: str = Query(..., description="行业指数代码，如 801010.SI"),
):
    """行业成分股列表"""
    db = get_db_manager()
    account = await db.fetchone(
        "SELECT 1 FROM accounts WHERE account_id = ? AND is_active = 1", (account_id,)
    )
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    try:
        sdk_mgr = get_sdk_manager()
        df = sdk_mgr.get_industry_constituent(index_codes=[index_code])
        if df.empty:
            return {"success": True, "data": {"index_code": index_code, "constituents": []}}
        df.columns = df.columns.str.lower()
        records = df.to_dict(orient="records")
        return {"success": True, "data": {"index_code": index_code, "constituents": records, "count": len(records)}}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取行业成分股失败：{str(e)}")


@router.get("/api/v1/ui/{account_id}/data/index/constituent")
async def get_index_constituent(
    account_id: str = Path(..., description="账户 ID"),
    index_code: str = Query(..., description="指数代码，如 000300.SH"),
):
    """指数成分股列表"""
    db = get_db_manager()
    account = await db.fetchone(
        "SELECT 1 FROM accounts WHERE account_id = ? AND is_active = 1", (account_id,)
    )
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    try:
        sdk_mgr = get_sdk_manager()
        df = sdk_mgr.get_index_constituent(index_codes=[index_code])
        if df.empty:
            return {"success": True, "data": {"index_code": index_code, "constituents": []}}
        df.columns = df.columns.str.lower()
        records = df.to_dict(orient="records")
        return {"success": True, "data": {"index_code": index_code, "constituents": records, "count": len(records)}}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取指数成分股失败：{str(e)}")


# ================================================================
# 财报数据
# ================================================================

def _financial_to_records(df, stock_code: str):
    """将财务 DataFrame 转为 JSON records"""
    if df.empty:
        return []
    df.columns = df.columns.str.lower()
    records = df[df["market_code"] == stock_code].to_dict(orient="records")
    return records


@router.get("/api/v1/ui/{account_id}/data/financial/income")
async def get_income_statement(
    account_id: str = Path(..., description="账户 ID"),
    stock_code: str = Query(..., description="股票代码，如 600000.SH"),
):
    """利润表（三表之一）"""
    db = get_db_manager()
    account = await db.fetchone(
        "SELECT 1 FROM accounts WHERE account_id = ? AND is_active = 1", (account_id,)
    )
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    try:
        sdk_mgr = get_sdk_manager()
        df = sdk_mgr.get_income_statement(stock_codes=[stock_code])
        records = _financial_to_records(df, stock_code)
        return {"success": True, "data": {"stock_code": stock_code, "records": records, "count": len(records)}}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取利润表失败：{str(e)}")


@router.get("/api/v1/ui/{account_id}/data/financial/balance")
async def get_balance_sheet(
    account_id: str = Path(..., description="账户 ID"),
    stock_code: str = Query(..., description="股票代码，如 600000.SH"),
):
    """资产负债表（三表之一）"""
    db = get_db_manager()
    account = await db.fetchone(
        "SELECT 1 FROM accounts WHERE account_id = ? AND is_active = 1", (account_id,)
    )
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    try:
        sdk_mgr = get_sdk_manager()
        df = sdk_mgr.get_balance_sheet(stock_codes=[stock_code])
        records = _financial_to_records(df, stock_code)
        return {"success": True, "data": {"stock_code": stock_code, "records": records, "count": len(records)}}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取资产负债表失败：{str(e)}")


@router.get("/api/v1/ui/{account_id}/data/financial/cashflow")
async def get_cash_flow(
    account_id: str = Path(..., description="账户 ID"),
    stock_code: str = Query(..., description="股票代码，如 600000.SH"),
):
    """现金流量表（三表之一）"""
    db = get_db_manager()
    account = await db.fetchone(
        "SELECT 1 FROM accounts WHERE account_id = ? AND is_active = 1", (account_id,)
    )
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    try:
        sdk_mgr = get_sdk_manager()
        df = sdk_mgr.get_cash_flow_statement(stock_codes=[stock_code])
        records = _financial_to_records(df, stock_code)
        return {"success": True, "data": {"stock_code": stock_code, "records": records, "count": len(records)}}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取现金流量表失败：{str(e)}")


@router.get("/api/v1/ui/{account_id}/data/financial/profit-notice")
async def get_profit_notice(
    account_id: str = Path(..., description="账户 ID"),
    stock_code: str = Query(..., description="股票代码，如 600000.SH"),
):
    """业绩预告"""
    db = get_db_manager()
    account = await db.fetchone(
        "SELECT 1 FROM accounts WHERE account_id = ? AND is_active = 1", (account_id,)
    )
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    try:
        sdk_mgr = get_sdk_manager()
        df = sdk_mgr.get_profit_notice(stock_codes=[stock_code])
        if df.empty:
            return {"success": True, "data": {"stock_code": stock_code, "records": []}}
        df.columns = df.columns.str.lower()
        records = df[df["market_code"] == stock_code].to_dict(orient="records")
        return {"success": True, "data": {"stock_code": stock_code, "records": records, "count": len(records)}}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取业绩预告失败：{str(e)}")


@router.get("/api/v1/ui/{account_id}/data/financial/profit-express")
async def get_profit_express(
    account_id: str = Path(..., description="账户 ID"),
    stock_code: str = Query(..., description="股票代码，如 600000.SH"),
):
    """业绩快报"""
    db = get_db_manager()
    account = await db.fetchone(
        "SELECT 1 FROM accounts WHERE account_id = ? AND is_active = 1", (account_id,)
    )
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    try:
        sdk_mgr = get_sdk_manager()
        df = sdk_mgr.get_profit_express(stock_codes=[stock_code])
        if df.empty:
            return {"success": True, "data": {"stock_code": stock_code, "records": []}}
        df.columns = df.columns.str.lower()
        records = df[df["market_code"] == stock_code].to_dict(orient="records")
        return {"success": True, "data": {"stock_code": stock_code, "records": records, "count": len(records)}}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取业绩快报失败：{str(e)}")


# ================================================================
# 龙虎榜
# ================================================================

@router.get("/api/v1/ui/{account_id}/data/dragon-tiger")
async def get_dragon_tiger(
    account_id: str = Path(..., description="账户 ID"),
    stock_code: str = Query(..., description="股票代码，如 600000.SH"),
    start_date: int = Query(..., description="开始日期，格式 YYYYMMDD（必填）"),
    end_date: int = Query(..., description="结束日期，格式 YYYYMMDD（必填）"),
):
    """龙虎榜数据

    需显式传入 start_date 和 end_date，格式 YYYYMMDD。
    """
    db = get_db_manager()
    account = await db.fetchone(
        "SELECT 1 FROM accounts WHERE account_id = ? AND is_active = 1", (account_id,)
    )
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    try:
        sdk_mgr = get_sdk_manager()
        df = sdk_mgr.get_long_hu_bang(stock_codes=[stock_code], begin_date=start_date, end_date=end_date)
        if df.empty:
            return {"success": True, "data": {"stock_code": stock_code, "records": []}}
        df.columns = df.columns.str.lower()
        records = df.to_dict(orient="records")
        return {"success": True, "data": {"stock_code": stock_code, "records": records, "count": len(records)}}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取龙虎榜数据失败：{str(e)}")


# ================================================================
# 融资融券
# ================================================================

@router.get("/api/v1/ui/{account_id}/data/margin/summary")
async def get_margin_summary(
    account_id: str = Path(..., description="账户 ID"),
    start_date: int = Query(..., description="开始日期，格式 YYYYMMDD（必填）"),
    end_date: int = Query(..., description="结束日期，格式 YYYYMMDD（必填）"),
):
    """融资融券汇总数据

    需显式传入 start_date 和 end_date，格式 YYYYMMDD。
    """
    db = get_db_manager()
    account = await db.fetchone(
        "SELECT 1 FROM accounts WHERE account_id = ? AND is_active = 1", (account_id,)
    )
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    try:
        sdk_mgr = get_sdk_manager()
        df = sdk_mgr.get_margin_summary(begin_date=start_date, end_date=end_date)
        if df.empty:
            return {"success": True, "data": {"records": []}}
        df.columns = df.columns.str.lower()
        records = df.to_dict(orient="records")
        return {"success": True, "data": {"records": records, "count": len(records)}}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取融资融券汇总失败：{str(e)}")


@router.get("/api/v1/ui/{account_id}/data/margin/detail")
async def get_margin_detail(
    account_id: str = Path(..., description="账户 ID"),
    stock_code: str = Query(..., description="股票代码，如 600000.SH"),
    start_date: int = Query(..., description="开始日期，格式 YYYYMMDD（必填）"),
    end_date: int = Query(..., description="结束日期，格式 YYYYMMDD（必填）"),
):
    """融资融券明细数据

    需显式传入 start_date 和 end_date，格式 YYYYMMDD。
    """
    db = get_db_manager()
    account = await db.fetchone(
        "SELECT 1 FROM accounts WHERE account_id = ? AND is_active = 1", (account_id,)
    )
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    try:
        sdk_mgr = get_sdk_manager()
        df = sdk_mgr.get_margin_detail(stock_codes=[stock_code], begin_date=start_date, end_date=end_date)
        if df.empty:
            return {"success": True, "data": {"stock_code": stock_code, "records": []}}
        df.columns = df.columns.str.lower()
        records = df.to_dict(orient="records")
        return {"success": True, "data": {"stock_code": stock_code, "records": records, "count": len(records)}}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取融资融券明细失败：{str(e)}")


# ================================================================
# 大宗交易
# ================================================================

@router.get("/api/v1/ui/{account_id}/data/block-trading")
async def get_block_trading(
    account_id: str = Path(..., description="账户 ID"),
    stock_code: str = Query(..., description="股票代码，如 600000.SH"),
    start_date: int = Query(..., description="开始日期，格式 YYYYMMDD（必填）"),
    end_date: int = Query(..., description="结束日期，格式 YYYYMMDD（必填）"),
):
    """大宗交易数据

    需显式传入 start_date 和 end_date，格式 YYYYMMDD。
    """
    db = get_db_manager()
    account = await db.fetchone(
        "SELECT 1 FROM accounts WHERE account_id = ? AND is_active = 1", (account_id,)
    )
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    try:
        sdk_mgr = get_sdk_manager()
        df = sdk_mgr.get_block_trading(stock_codes=[stock_code], begin_date=start_date, end_date=end_date)
        if df.empty:
            return {"success": True, "data": {"stock_code": stock_code, "records": []}}
        df.columns = df.columns.str.lower()
        records = df.to_dict(orient="records")
        return {"success": True, "data": {"stock_code": stock_code, "records": records, "count": len(records)}}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取大宗交易数据失败：{str(e)}")


# ================================================================
# 国债收益率
# ================================================================

@router.get("/api/v1/ui/{account_id}/data/treasury-yield")
async def get_treasury_yield(
    account_id: str = Path(..., description="账户 ID"),
):
    """国债收益率曲线"""
    db = get_db_manager()
    account = await db.fetchone(
        "SELECT 1 FROM accounts WHERE account_id = ? AND is_active = 1", (account_id,)
    )
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    try:
        sdk_mgr = get_sdk_manager()
        df = sdk_mgr.get_treasury_yield()
        if df.empty:
            return {"success": True, "data": {"records": []}}
        df.columns = df.columns.str.lower()
        records = df.to_dict(orient="records")
        return {"success": True, "data": {"records": records, "count": len(records)}}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取国债收益率失败：{str(e)}")
