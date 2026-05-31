# -*- coding: utf-8 -*-
"""
证券类型识别模块

根据证券代码判断其类型：股票、ETF、指数、行业指数等
"""

def get_security_type(stock_code: str) -> str:
    """
    根据代码判断证券类型

    Args:
        stock_code: 证券代码，格式如 '600000.SH', '510050.SH', '159915.SZ'

    Returns:
        'stock': A股股票
        'etf': ETF基金
        'index': 指数
        'industry': 行业指数
    """
    if not stock_code:
        return 'stock'

    # 分离代码和交易所
    parts = stock_code.split('.')
    code_part = parts[0] if len(parts) > 0 else stock_code
    market = parts[1] if len(parts) > 1 else ''

    # 行业指数：80xxxx.SI 或 .SI 后缀
    if market == 'SI' or code_part.startswith('80'):
        return 'industry'

    # 上交所 ETF：51xxxx, 58xxxx
    # 如：510050.SH (50ETF), 510300.SH (300ETF), 588000.SH (科创50ETF)
    if market == 'SH' and (code_part.startswith('51') or code_part.startswith('58') or code_part.startswith('56')):
        return 'etf'

    # 深交所 ETF：159xxx
    # 如：159915.SZ (创业板ETF), 159919.SZ (300ETF深市版)
    if market == 'SZ' and code_part.startswith('159'):
        return 'etf'

    # 指数（非行业指数）：000xxx.SH, 399xxx.SZ
    # 如：000001.SH (上证指数), 399001.SZ (深证成指)
    if market == 'SH' and code_part.startswith('000'):
        return 'index'
    if market == 'SZ' and code_part.startswith('399'):
        return 'index'

    # 其他默认为股票
    return 'stock'


def is_etf_code(stock_code: str) -> bool:
    """判断是否为 ETF 代码"""
    return get_security_type(stock_code) == 'etf'


def is_stock_code(stock_code: str) -> bool:
    """判断是否为股票代码"""
    return get_security_type(stock_code) == 'stock'


def is_index_code(stock_code: str) -> bool:
    """判断是否为指数代码"""
    return get_security_type(stock_code) == 'index'


def is_factor_applicable_to_etf(factor_name: str) -> bool:
    """
    判断因子是否适用于 ETF

    ETF 适用因子：技术指标、价格表现、成交量类
    ETF 不适用因子：市值、估值、财务、上市天数

    Args:
        factor_name: 因子名称（小写）

    Returns:
        True: 因子适用于 ETF
        False: 因子不适用于 ETF
    """
    # ETF 不适用的因子列表
    ETF_INAPPLICABLE_FACTORS = {
        # 市值类
        'circ_market_cap', 'total_market_cap',
        # 估值类
        'pe_ttm', 'pb', 'ps_ttm', 'pcf',
        'pe_inverse', 'pb_inverse',
        # 财务类
        'roe', 'roa', 'gross_margin', 'net_margin',
        'revenue_growth', 'net_profit_growth',
        'operating_cash_flow', 'net_assets',
        # 上市相关
        'days_since_ipo', 'list_date',
        # 行业分类
        'sw_level1', 'sw_level2', 'sw_level3',
    }

    return factor_name.lower() not in ETF_INAPPLICABLE_FACTORS