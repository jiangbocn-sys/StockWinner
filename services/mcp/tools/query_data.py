# -*- coding: utf-8 -*-
"""
SDK 数据查询工具

通过 Agent API 的 /query/data/* 端点获取 SDK 原始数据。
"""

import sys
from pathlib import Path

# 添加项目根目录
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from fastmcp import FastMCP
from services.mcp.utils import get_api_client

# 获取全局 MCP 实例（从 server.py 导入）
from services.mcp import mcp

api = get_api_client()


# ================================================================
# K线与行情查询
# ================================================================

@mcp.tool()
async def mcp_kline(stock_code: str, period: str = "day", limit: int = 60) -> dict:
    """
    查询股票 K 线数据

    Args:
        stock_code: 股票代码，如 600000.SH 或 000001.SZ
        period: K 线周期，可选值：day(日线), week(周线), month(月线)
        limit: 返回记录数，默认 60

    Returns:
        K 线数据列表，包含 trade_date, open, high, low, close, volume 等
    """
    return await api.get("/query/kline", {
        "stock_code": stock_code,
        "period": period,
        "limit": limit
    })


@mcp.tool()
async def mcp_market(stock_code: str) -> dict:
    """
    查询股票实时行情（来自 PriceCache）

    Args:
        stock_code: 股票代码，如 600000.SH

    Returns:
        行情数据，包含 current_price, open, high, low, volume, change_percent 等
    """
    return await api.get("/query/market", {"stock_code": stock_code})


@mcp.tool()
async def mcp_factors(stock_code: str, date: str = None) -> dict:
    """
    查询股票日频因子数据

    Args:
        stock_code: 股票代码
        date: 日期（YYYY-MM-DD），默认最新

    Returns:
        因子数据，包含 MA、RSI、MACD、BOLL、KDJ 等技术指标
    """
    params = {"stock_code": stock_code}
    if date:
        params["date"] = date
    return await api.get("/query/factors", params)


@mcp.tool()
async def mcp_stock_code(stock_code: str = None, stock_name: str = None) -> dict:
    """
    股票代码与名称互查

    Args:
        stock_code: 股票代码（精确查询）
        stock_name: 股票名称（模糊查询）

    Returns:
        代码→名称：返回股票信息
        名称→代码：返回匹配列表
    """
    if stock_code:
        return await api.get("/query/stock-code", {"stock_code": stock_code})
    elif stock_name:
        return await api.get("/query/stock-code", {"stock_name": stock_name})
    else:
        return {"success": False, "message": "请提供 stock_code 或 stock_name"}


# ================================================================
# 代码列表/日历/快照
# ================================================================

@mcp.tool()
async def mcp_code_list(security_type: str = "EXTRA_STOCK_A") -> dict:
    """
    获取证券代码列表

    Args:
        security_type: 证券类型，可选值：
            - EXTRA_STOCK_A: 沪深北A股（默认）
            - EXTRA_INDEX_A: 沪深北指数
            - EXTRA_ETF: 沪深ETF
            - EXTRA_KZZ: 沪深可转债

    Returns:
        代码列表，如 ['600000.SH', '000001.SZ', ...]
    """
    return await api.get("/query/data/code-list", {"security_type": security_type})


@mcp.tool()
async def mcp_code_info(security_type: str = "EXTRA_STOCK_A") -> dict:
    """
    获取证券信息（每日更新，含涨跌停价、昨收价）

    Args:
        security_type: 证券类型，同 mcp_code_list

    Returns:
        证券信息列表，包含 CODE, NAME, STATUS, PRE_CLOSE, LIMIT_UP, LIMIT_DOWN 等
    """
    return await api.get("/query/data/code-info", {"security_type": security_type})


@mcp.tool()
async def mcp_calendar(market: str = "SH") -> dict:
    """
    获取交易日历

    Args:
        market: 市场类型，可选值：SH(上交所), SZ(深交所), BJ(北交所)

    Returns:
        交易日历列表，YYYYMMDD 格式的整数
    """
    return await api.get("/query/data/calendar", {"market": market})


@mcp.tool()
async def mcp_snapshot(codes: str, begin_date: int = None, end_date: int = None) -> dict:
    """
    查询历史快照数据（自动识别资产类型）

    Args:
        codes: 股票代码列表，逗号分隔，如 "600000.SH,000001.SZ"
        begin_date: 开始日期（YYYYMMDD），默认当天
        end_date: 结束日期（YYYYMMDD），默认当天

    Returns:
        快照数据，包含实时行情信息
    """
    params = {"codes": codes}
    if begin_date:
        params["begin_date"] = begin_date
    if end_date:
        params["end_date"] = end_date
    return await api.get("/query/data/snapshot", params)


# ================================================================
# 指数数据查询
# ================================================================

@mcp.tool()
async def mcp_index_list() -> dict:
    """
    获取指数代码列表

    Returns:
        指数列表，包含上证指数、深证指数、创业板指数等
    """
    return await api.get("/query/data/index/list")


@mcp.tool()
async def mcp_index_kline(index_code: str, period: str = "day", limit: int = 60) -> dict:
    """
    查询指数 K 线数据

    Args:
        index_code: 指数代码，如 000001.SH（上证指数）
        period: 周期，可选 day/week/month
        limit: 返回记录数

    Returns:
        指数 K 线数据
    """
    return await api.get("/query/data/index/kline", {
        "index_code": index_code,
        "period": period,
        "limit": limit
    })


@mcp.tool()
async def mcp_index_constituent(index_code: str) -> dict:
    """
    查询指数成分股

    Args:
        index_code: 指数代码

    Returns:
        成分股列表
    """
    return await api.get("/query/data/index/constituent", {"index_code": index_code})


# ================================================================
# 行业数据查询
# ================================================================

@mcp.tool()
async def mcp_industry_list(level: int = 1) -> dict:
    """
    获取申万行业分类列表

    Args:
        level: 行业层级，1=一级行业，2=二级行业，3=三级行业

    Returns:
        行业分类列表
    """
    return await api.get("/query/data/industry/list", {"level": level})


@mcp.tool()
async def mcp_industry_kline(index_code: str) -> dict:
    """
    查询行业指数行情

    Args:
        index_code: 行业指数代码

    Returns:
        行业指数 K 线数据
    """
    return await api.get("/query/data/industry/kline", {"index_code": index_code})


@mcp.tool()
async def mcp_industry_constituent(index_code: str) -> dict:
    """
    查询行业成分股

    Args:
        index_code: 行业指数代码

    Returns:
        行业成分股列表
    """
    return await api.get("/query/data/industry/constituent", {"index_code": index_code})


# ================================================================
# 财务数据查询
# ================================================================

@mcp.tool()
async def mcp_financial_income(stock_code: str) -> dict:
    """
    查询利润表数据

    Args:
        stock_code: 股票代码

    Returns:
        利润表数据，包含营业收入、净利润、毛利率等
    """
    return await api.get("/query/data/financial/income", {"stock_code": stock_code})


@mcp.tool()
async def mcp_financial_balance(stock_code: str) -> dict:
    """
    查询资产负债表数据

    Args:
        stock_code: 股票代码

    Returns:
        资产负债表数据
    """
    return await api.get("/query/data/financial/balance", {"stock_code": stock_code})


@mcp.tool()
async def mcp_financial_cashflow(stock_code: str) -> dict:
    """
    查询现金流量表数据

    Args:
        stock_code: 股票代码

    Returns:
        现金流量表数据
    """
    return await api.get("/query/data/financial/cashflow", {"stock_code": stock_code})


@mcp.tool()
async def mcp_profit_notice(stock_code: str) -> dict:
    """
    查询业绩预告

    Args:
        stock_code: 股票代码

    Returns:
        业绩预告数据
    """
    return await api.get("/query/data/financial/profit-notice", {"stock_code": stock_code})


@mcp.tool()
async def mcp_profit_express(stock_code: str) -> dict:
    """
    查询业绩快报

    Args:
        stock_code: 股票代码

    Returns:
        业绩快报数据
    """
    return await api.get("/query/data/financial/profit-express", {"stock_code": stock_code})


# ================================================================
# 交易数据查询
# ================================================================

@mcp.tool()
async def mcp_dragon_tiger(stock_code: str, start_date: str = None, end_date: str = None) -> dict:
    """
    查询龙虎榜数据

    Args:
        stock_code: 股票代码
        start_date: 开始日期（YYYYMMDD）
        end_date: 结束日期（YYYYMMDD）

    Returns:
        龙虎榜数据
    """
    params = {"stock_code": stock_code}
    if start_date:
        params["start_date"] = start_date
    if end_date:
        params["end_date"] = end_date
    return await api.get("/query/data/dragon-tiger", params)


@mcp.tool()
async def mcp_margin_summary(start_date: str = None, end_date: str = None) -> dict:
    """
    查询融资融券汇总数据

    Args:
        start_date: 开始日期（YYYYMMDD）
        end_date: 结束日期（YYYYMMDD）

    Returns:
        融资融券汇总数据
    """
    params = {}
    if start_date:
        params["start_date"] = start_date
    if end_date:
        params["end_date"] = end_date
    return await api.get("/query/data/margin/summary", params)


@mcp.tool()
async def mcp_margin_detail(stock_code: str, start_date: str = None, end_date: str = None) -> dict:
    """
    查询融资融券明细

    Args:
        stock_code: 股票代码
        start_date: 开始日期（YYYYMMDD）
        end_date: 结束日期（YYYYMMDD）

    Returns:
        融资融券明细数据
    """
    params = {"stock_code": stock_code}
    if start_date:
        params["start_date"] = start_date
    if end_date:
        params["end_date"] = end_date
    return await api.get("/query/data/margin/detail", params)


@mcp.tool()
async def mcp_block_trading(stock_code: str, start_date: str = None, end_date: str = None) -> dict:
    """
    查询大宗交易数据

    Args:
        stock_code: 股票代码
        start_date: 开始日期（YYYYMMDD）
        end_date: 结束日期（YYYYMMDD）

    Returns:
        大宗交易数据
    """
    params = {"stock_code": stock_code}
    if start_date:
        params["start_date"] = start_date
    if end_date:
        params["end_date"] = end_date
    return await api.get("/query/data/block-trading", params)


@mcp.tool()
async def mcp_treasury_yield() -> dict:
    """
    查询国债收益率数据

    Returns:
        国债收益率曲线数据
    """
    return await api.get("/query/data/treasury-yield")


# ================================================================
# 复权因子/基础数据/股东/股权/分红
# ================================================================

@mcp.tool()
async def mcp_backward_factor(stock_codes: str) -> dict:
    """
    查询后复权因子

    Args:
        stock_codes: 股票代码列表，逗号分隔

    Returns:
        复权因子数据，用于计算复权价格
    """
    return await api.get("/query/data/backward-factor", {"stock_codes": stock_codes})


@mcp.tool()
async def mcp_stock_basic(stock_codes: str = None, summary_only: bool = False) -> dict:
    """
    查询股票基础信息（上市日期、退市日期、板块）

    Args:
        stock_codes: 股票代码列表，逗号分隔（可选）
        summary_only: 仅返回统计摘要

    Returns:
        summary_only=True: 统计摘要（总数、上市数、退市数、市场分布）
        summary_only=False: 详细数据列表
    """
    params = {}
    if stock_codes:
        params["stock_codes"] = stock_codes
    if summary_only:
        params["summary_only"] = "true"
    return await api.get("/query/data/stock-basic", params)


@mcp.tool()
async def mcp_history_code_list(date: int) -> dict:
    """
    查询历史代码列表

    Args:
        date: 历史日期（YYYYMMDD）

    Returns:
        该日期有效的股票代码列表
    """
    return await api.get("/query/data/history-code-list", {"date": date})


@mcp.tool()
async def mcp_bj_code_mapping() -> dict:
    """
    查询北交所代码对照表

    Returns:
        北交所股票代码映射数据
    """
    return await api.get("/query/data/bj-code-mapping")


@mcp.tool()
async def mcp_shareholder(stock_codes: str) -> dict:
    """
    查询十大股东

    Args:
        stock_codes: 股票代码列表，逗号分隔

    Returns:
        十大股东数据
    """
    return await api.get("/query/data/shareholder", {"stock_codes": stock_codes})


@mcp.tool()
async def mcp_holder_num(stock_codes: str) -> dict:
    """
    查询股东户数

    Args:
        stock_codes: 股票代码列表，逗号分隔

    Returns:
        股东户数变化数据
    """
    return await api.get("/query/data/holder-num", {"stock_codes": stock_codes})


@mcp.tool()
async def mcp_equity_structure(stock_codes: str) -> dict:
    """
    查询股本结构

    Args:
        stock_codes: 股票代码列表，逗号分隔

    Returns:
        股本结构数据（总股本、流通股等）
    """
    return await api.get("/query/data/equity-structure", {"stock_codes": stock_codes})


@mcp.tool()
async def mcp_equity_pledge_freeze(stock_codes: str) -> dict:
    """
    查询股权质押冻结

    Args:
        stock_codes: 股票代码列表，逗号分隔

    Returns:
        股权质押冻结数据
    """
    return await api.get("/query/data/equity-pledge-freeze", {"stock_codes": stock_codes})


@mcp.tool()
async def mcp_equity_restricted(stock_codes: str) -> dict:
    """
    查询限售股解禁

    Args:
        stock_codes: 股票代码列表，逗号分隔

    Returns:
        限售股解禁数据（解禁日期、解禁数量）
    """
    return await api.get("/query/data/equity-restricted", {"stock_codes": stock_codes})


@mcp.tool()
async def mcp_dividend(stock_codes: str) -> dict:
    """
    查询分红数据

    Args:
        stock_codes: 股票代码列表，逗号分隔

    Returns:
        分红数据（分红金额、分红日期）
    """
    return await api.get("/query/data/dividend", {"stock_codes": stock_codes})


@mcp.tool()
async def mcp_right_issue(stock_codes: str) -> dict:
    """
    查询配股数据

    Args:
        stock_codes: 股票代码列表，逗号分隔

    Returns:
        配股数据（配股比例、配股价格）
    """
    return await api.get("/query/data/right-issue", {"stock_codes": stock_codes})


@mcp.tool()
async def mcp_index_weight(index_code: str) -> dict:
    """
    查询指数成分权重

    Args:
        index_code: 指数代码，如 000300.SH

    Returns:
        成分股权重数据
    """
    return await api.get("/query/data/index-weight", {"index_code": index_code})


@mcp.tool()
async def mcp_industry_weight(index_code: str) -> dict:
    """
    查询行业成分权重

    Args:
        index_code: 行业指数代码，如 801010.SI

    Returns:
        行业成分股权重数据
    """
    return await api.get("/query/data/industry-weight", {"index_code": index_code})


# ================================================================
# ETF 专项数据
# ================================================================

@mcp.tool()
async def mcp_etf_pcf(etf_codes: str) -> dict:
    """
    查询 ETF 申赎数据（PCF清单+成分股）

    Args:
        etf_codes: ETF代码列表，逗号分隔，如 "510050.SH,510300.SH"

    Returns:
        pcf_info: ETF基本信息（申购赎回代码、最小申购单位等）
        constituents: 各ETF的成分股清单
    """
    return await api.get("/query/data/etf/pcf", {"etf_codes": etf_codes})


@mcp.tool()
async def mcp_etf_share(etf_codes: str) -> dict:
    """
    查询 ETF 基金份额

    Args:
        etf_codes: ETF代码列表，逗号分隔

    Returns:
        各ETF的历史份额变动数据
    """
    return await api.get("/query/data/etf/share", {"etf_codes": etf_codes})


@mcp.tool()
async def mcp_etf_iopv(etf_codes: str) -> dict:
    """
    查询 ETF IOPV 实时净值（盘中计算）

    Args:
        etf_codes: ETF代码列表，逗号分隔

    Returns:
        各ETF的IOPV净值数据（Indicative Optimized Portfolio Value）
    """
    return await api.get("/query/data/etf/iopv", {"etf_codes": etf_codes})