"""
股票代码工具模块

提供股票代码格式化和解析功能
"""

from typing import Optional, Tuple


# 市场前缀常量
SH_PREFIXES = ('60', '68', '65')  # 沪市前缀
SZ_PREFIXES = ('00', '20', '30')  # 深市前缀
BJ_PREFIXES = ('43', '83', '87')  # 北交所前缀


def normalize_stock_code(code: str) -> str:
    """
    规范化股票代码格式为 CODE.SH 或 CODE.SZ 或 CODE.BJ

    规则：
    - 6xxxxx, 68xxxx, 65xxxx, 9xxxxx → .SH (沪市)
    - 0xxxxx, 20xxxx, 30xxxx → .SZ (深市)
    - 4xxxxx, 8xxxxx → .BJ (北交所)
    - 已有后缀的保持大写

    Args:
        code: 股票代码

    Returns:
        规范化后的股票代码
    """
    if not code:
        return code

    code = code.strip().upper()

    # 如果已有后缀，直接返回
    if '.' in code:
        return code

    # 根据代码前缀判断市场（使用切片提高效率）
    prefix = code[:2] if len(code) >= 2 else ''

    if prefix in SH_PREFIXES or code.startswith('9'):
        return f"{code}.SH"
    elif prefix in SZ_PREFIXES:
        return f"{code}.SZ"
    elif prefix.startswith('4') or prefix.startswith('8'):
        return f"{code}.BJ"
    else:
        # 无法判断，默认 BJ
        return f"{code}.BJ"


def parse_stock_code(code: str) -> Tuple[str, str]:
    """
    解析股票代码，返回 (代码，市场)

    Args:
        code: 股票代码（带或不带后缀）

    Returns:
        (代码，市场) 元组，市场为 SH 或 SZ
    """
    normalized = normalize_stock_code(code)
    parts = normalized.split('.')
    return parts[0], parts[1]


def get_market(code: str) -> str:
    """
    获取股票市场

    Args:
        code: 股票代码

    Returns:
        'SH' 或 'SZ'
    """
    _, market = parse_stock_code(code)
    return market


def strip_market(code: str) -> str:
    """
    去除股票市场后缀

    Args:
        code: 股票代码（带后缀）

    Returns:
        纯数字代码
    """
    normalized = normalize_stock_code(code)
    return normalized.split('.')[0]


def is_sh_stock(code: str) -> bool:
    """判断是否是沪市股票"""
    return get_market(code) == 'SH'


def is_sz_stock(code: str) -> bool:
    """判断是否是深市股票"""
    return get_market(code) == 'SZ'


def is_bj_stock(code: str) -> bool:
    """判断是否是北交所股票"""
    return get_market(code) == 'BJ'


def format_stock_code_list(codes: list) -> list:
    """
    批量格式化股票代码列表

    Args:
        codes: 股票代码列表

    Returns:
        格式化后的股票代码列表
    """
    return [normalize_stock_code(code) for code in codes]
