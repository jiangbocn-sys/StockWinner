"""
股票代码工具模块

提供股票代码格式化、解析、市场识别和过滤功能。
支持 SH/SZ/BJ/SI 四种市场类型。
"""

from typing import Optional, Tuple, List


# 市场前缀常量
SH_PREFIXES = ('60', '68', '65', '50', '51', '52', '53', '54', '55', '56', '57', '58', '69')  # 沪市前缀
SZ_PREFIXES = ('00', '20', '30')  # 深市前缀
BJ_PREFIXES = ('43', '83', '87', '82', '88', '89', '92')  # 北交所前缀


def normalize_stock_code(code: str) -> str:
    """
    规范化股票代码格式为 CODE.SH / CODE.SZ / CODE.BJ

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


def normalize(code: str) -> str:
    """normalize_stock_code 的别名"""
    return normalize_stock_code(code)


def parse_stock_code(code: str) -> Tuple[str, str]:
    """
    解析股票代码，返回 (代码，市场)

    Args:
        code: 股票代码（带或不带后缀）

    Returns:
        (代码，市场) 元组，市场为 SH/SZ/BJ/SI
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
        'SH' / 'SZ' / 'BJ' / 'SI'
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


def is_si_index(code: str) -> bool:
    """判断是否是申万行业指数（后缀为 SI）"""
    code_upper = code.strip().upper()
    return '.' in code_upper and code_upper.split('.')[-1] == 'SI'


def format_stock_code_list(codes: list) -> list:
    """
    批量格式化股票代码列表

    Args:
        codes: 股票代码列表

    Returns:
        格式化后的股票代码列表
    """
    return [normalize_stock_code(code) for code in codes]


def filter_codes(codes: List[str], markets: Optional[List[str]] = None,
                 exclude: Optional[List[str]] = None) -> List[str]:
    """
    按市场筛选股票代码。

    Args:
        codes: 股票代码列表
        markets: 保留的市场列表，如 ["SH", "SZ"]
        exclude: 排除的市场列表，如 ["BJ"]

    Returns:
        过滤后的代码列表
    """
    result = []
    for code in codes:
        m = get_market(code)
        if markets and m not in markets:
            continue
        if exclude and m in exclude:
            continue
        result.append(code)
    return result


def batch_normalize(codes: List[str]) -> List[str]:
    """批量标准化股票代码"""
    return [normalize_stock_code(c) for c in codes]
