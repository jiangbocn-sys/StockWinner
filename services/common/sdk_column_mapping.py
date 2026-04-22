"""
SDK列名映射模块

统一处理AmazingData SDK返回数据的列名转换：
- SDK财务数据返回大写列名 (MARKET_CODE, TOT_SHARE等)
- SDK基本数据返回小写列名 (symbol, pre_close等)
- 数据库使用小写无下划线列名 (stock_code, total_share, ma5等)

使用方法:
    from services.common.sdk_column_mapping import map_sdk_columns, map_tech_columns

    df = sdk.get_equity_structure(['000001.SZ'])
    df = map_sdk_columns(df)  # 转换SDK列名

    df = add_all_extended_technical_indicators_to_df(df)
    df = map_tech_columns(df)  # 转换计算库列名
"""

import pandas as pd
from typing import Dict


# ==================== SDK返回列名 -> 数据库列名映射 ====================

# 股本结构数据映射 (get_equity_structure)
EQUITY_COLUMN_MAPPING = {
    'MARKET_CODE': 'stock_code',
    'TOT_SHARE': 'total_share',          # 总股本(股)
    'FLOAT_SHARE': 'float_share',        # 流通股本(股)
    'FLOAT_A_SHARE': 'float_a_share',    # A股流通股
    'ANN_DATE': 'ann_date',              # 公告日期
    'CHANGE_DATE': 'change_date',        # 变动日期
}

# 利润表数据映射 (get_income_statement)
INCOME_COLUMN_MAPPING = {
    'MARKET_CODE': 'stock_code',
    'SECURITY_NAME': 'stock_name',
    'REPORT_TYPE': 'report_type',        # 1=年报,2=中报,3=季报,4=一季报
    'REPORTING_PERIOD': 'reporting_period',
    'ANN_DATE': 'ann_date',
    'NET_PRO_INCL_MIN_INT_INC': 'net_profit',           # 净利润(含少数股东)
    'NET_PRO_AFTER_DED_NR_GL': 'net_profit_deducted',   # 扣非净利润
    'NET_PRO_EXCL_MIN_INT_INC': 'net_profit_parent',    # 归母净利润
    'OPERA_REV': 'total_revenue',         # 营业收入
    'OPERA_PROFIT': 'operating_profit',   # 营业利润
    'OPERA_COST': 'operating_cost',       # 营业成本
    'BASIC_EPS': 'basic_eps',             # 基本每股收益
    'DILUTED_EPS': 'diluted_eps',         # 稀释每股收益
    'TOTAL_PROFIT': 'total_profit',       # 利润总额
    'TOTAL_OPERA_REV': 'total_operating_revenue',  # 总营收
}

# 资产负债表数据映射 (get_balance_sheet)
BALANCE_COLUMN_MAPPING = {
    'MARKET_CODE': 'stock_code',
    'SECURITY_NAME': 'stock_name',
    'REPORT_TYPE': 'report_type',
    'REPORTING_PERIOD': 'reporting_period',
    'ANN_DATE': 'ann_date',
    'TOT_ASSETS': 'total_assets',                           # 总资产
    'TOT_SHARE_EQUITY_EXCL_MIN_INT': 'net_assets',          # 归母净资产
    'TOT_LIAB': 'total_liability',                          # 总负债
    'TOT_EQUITY': 'total_equity',                           # 所有者权益
    'ACC_RECEIVABLE': 'accounts_receivable',                # 应收账款
    'ACC_PAYABLE': 'accounts_payable',                      # 应付账款
    'INVENTORY': 'inventory',                               # 存货
    'FIXED_ASSETS': 'fixed_assets',                         # 固定资产
    'CASH_EQUIV': 'cash_equivalents',                       # 现金及等价物
}

# 现金流量表数据映射 (get_cash_flow_statement)
CASHFLOW_COLUMN_MAPPING = {
    'MARKET_CODE': 'stock_code',
    'SECURITY_NAME': 'stock_name',
    'REPORT_TYPE': 'report_type',
    'REPORTING_PERIOD': 'reporting_period',
    'ANN_DATE': 'ann_date',
    'NET_CASH_FLOWS_OPERA_ACT': 'operating_cashflow',       # 经营现金流净额
    'NET_CASH_FLOWS_INV_ACT': 'investing_cashflow',         # 投资现金流净额
    'NET_CASH_FLOWS_FIN_ACT': 'financing_cashflow',         # 筹资现金流净额
    'CASH_RECEIVE_INV_ACT': 'cash_received_investing',      # 投资活动现金流入
    'CASH_PAY_INV_ACT': 'cash_paid_investing',              # 投资活动现金流出
}

# 行业分类数据映射 (get_industry_base_info)
INDUSTRY_COLUMN_MAPPING = {
    'INDEX_CODE': 'index_code',            # 行业指数代码
    'INDUSTRY_CODE': 'industry_code',      # 行业代码
    'LEVEL_TYPE': 'level_type',            # 级别类型
    'LEVEL1_NAME': 'sw_level1',            # 申万一级
    'LEVEL2_NAME': 'sw_level2',            # 申万二级
    'LEVEL3_NAME': 'sw_level3',            # 申万三级
    'IS_PUB': 'is_public',
    'CHANGE_REASON': 'change_reason',
}

# 股票基本信息映射 (get_code_info - 注意此函数返回小写列名)
CODE_INFO_COLUMN_MAPPING = {
    'symbol': 'stock_name',          # 股票名称
    'code': 'stock_code',            # 股票代码 (有些版本可能是code字段)
    'pre_close': 'pre_close',        # 昨收价
    'high_limited': 'high_limited',  # 涨停价
    'low_limited': 'low_limited',    # 跌停价
    'price_tick': 'price_tick',      # 最小变动价位
    'list_day': 'list_date',         # 上市日期
    'security_status': 'security_status',  # 证券状态
}

# 综合SDK列名映射（合并所有SDK数据源的映射）
SDK_COLUMN_MAPPING = {}
for mapping in [EQUITY_COLUMN_MAPPING, INCOME_COLUMN_MAPPING, BALANCE_COLUMN_MAPPING,
                CASHFLOW_COLUMN_MAPPING, INDUSTRY_COLUMN_MAPPING, CODE_INFO_COLUMN_MAPPING]:
    SDK_COLUMN_MAPPING.update(mapping)


# ==================== 技术指标计算库列名 -> 数据库列名映射 ====================

# technical_indicators.py 产生带下划线的列名，数据库需要不带下划线
TECH_COLUMN_MAPPING = {
    # MA类
    'ma_5': 'ma5',
    'ma_10': 'ma10',
    'ma_20': 'ma20',
    'ma_60': 'ma60',

    # EMA类
    'ema_12': 'ema12',
    'ema_26': 'ema26',

    # 已匹配的列名（无需转换，但保留在映射中以便查询）
    'rsi_14': 'rsi_14',
    'cci_20': 'cci_20',
    'atr_14': 'atr_14',
    'hv_20': 'hv_20',
    'kdj_k': 'kdj_k',
    'kdj_d': 'kdj_d',
    'kdj_j': 'kdj_j',
    'dif': 'dif',
    'dea': 'dea',
    'macd': 'macd',
    'boll_upper': 'boll_upper',
    'boll_middle': 'boll_middle',
    'boll_lower': 'boll_lower',
    'adx': 'adx',
    'obv': 'obv',
    'volume_ratio': 'volume_ratio',
    'momentum_10d': 'momentum_10d',
    'momentum_20d': 'momentum_20d',
    'golden_cross': 'golden_cross',
    'death_cross': 'death_cross',
    'ema12': 'ema12',
    'ema26': 'ema26',
}


# ==================== 映射函数 ====================

def map_sdk_columns(df: pd.DataFrame, mapping: Dict = None) -> pd.DataFrame:
    """
    将SDK返回的列名转换为数据库列名

    Args:
        df: SDK返回的DataFrame
        mapping: 自定义映射字典，默认使用SDK_COLUMN_MAPPING

    Returns:
        转换后的DataFrame
    """
    if df.empty:
        return df

    if mapping is None:
        mapping = SDK_COLUMN_MAPPING

    # 只转换存在的列
    cols_to_rename = {k: v for k, v in mapping.items() if k in df.columns}

    if cols_to_rename:
        df = df.rename(columns=cols_to_rename)

    return df


def map_equity_columns(df: pd.DataFrame) -> pd.DataFrame:
    """股本结构数据列名映射"""
    return map_sdk_columns(df, EQUITY_COLUMN_MAPPING)


def map_income_columns(df: pd.DataFrame) -> pd.DataFrame:
    """利润表数据列名映射"""
    return map_sdk_columns(df, INCOME_COLUMN_MAPPING)


def map_balance_columns(df: pd.DataFrame) -> pd.DataFrame:
    """资产负债表数据列名映射"""
    return map_sdk_columns(df, BALANCE_COLUMN_MAPPING)


def map_cashflow_columns(df: pd.DataFrame) -> pd.DataFrame:
    """现金流量表数据列名映射"""
    return map_sdk_columns(df, CASHFLOW_COLUMN_MAPPING)


def map_industry_columns(df: pd.DataFrame) -> pd.DataFrame:
    """行业分类数据列名映射"""
    return map_sdk_columns(df, INDUSTRY_COLUMN_MAPPING)


def map_code_info_columns(df: pd.DataFrame) -> pd.DataFrame:
    """股票基本信息列名映射"""
    return map_sdk_columns(df, CODE_INFO_COLUMN_MAPPING)


def map_tech_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    将技术指标计算库产生的列名转换为数据库列名

    technical_indicators.py 产生带下划线的列名如 ma_5
    数据库使用不带下划线的列名如 ma5

    Args:
        df: 计算技术指标后的DataFrame

    Returns:
        转换后的DataFrame
    """
    if df.empty:
        return df

    # 只转换存在的列
    cols_to_rename = {k: v for k, v in TECH_COLUMN_MAPPING.items()
                      if k in df.columns and k != v}

    if cols_to_rename:
        df = df.rename(columns=cols_to_rename)

    return df


def get_db_column_name(sdk_column: str) -> str:
    """
    获取SDK列名对应的数据库列名

    Args:
        sdk_column: SDK返回的列名

    Returns:
        数据库列名（如果不存在映射则返回原列名）
    """
    return SDK_COLUMN_MAPPING.get(sdk_column, sdk_column)


def get_tech_db_column_name(tech_column: str) -> str:
    """
    获取技术指标计算库列名对应的数据库列名

    Args:
        tech_column: 技术指标计算库产生的列名

    Returns:
        数据库列名（如果不存在映射则返回原列名）
    """
    return TECH_COLUMN_MAPPING.get(tech_column, tech_column)


# ==================== 测试代码 ====================

if __name__ == "__main__":
    import pandas as pd

    # 测试SDK列名映射
    print("=== SDK列名映射测试 ===")

    test_equity = pd.DataFrame({
        'MARKET_CODE': ['000001.SZ'],
        'TOT_SHARE': [1000000000],
        'FLOAT_SHARE': [500000000],
    })

    mapped = map_equity_columns(test_equity)
    print(f"股本结构: {test_equity.columns.tolist()} -> {mapped.columns.tolist()}")

    test_income = pd.DataFrame({
        'MARKET_CODE': ['000001.SZ'],
        'NET_PRO_INCL_MIN_INT_INC': [100000000],
        'OPERA_REV': [500000000],
    })

    mapped = map_income_columns(test_income)
    print(f"利润表: {test_income.columns.tolist()} -> {mapped.columns.tolist()}")

    # 测试技术指标列名映射
    print("\n=== 技术指标列名映射测试 ===")

    test_tech = pd.DataFrame({
        'ma_5': [10.5],
        'ma_10': [11.2],
        'ma_20': [12.0],
        'rsi_14': [60.5],
    })

    mapped = map_tech_columns(test_tech)
    print(f"技术指标: {test_tech.columns.tolist()} -> {mapped.columns.tolist()}")