"""
统一因子计算管道

从 local_data_service.py 的 3 份重复代码中提取单一实现：
1. calculate_and_save_factors_for_dates() L1224-1274
2. fill_empty_factor_values()    L1667-1696
3. smart_update_factors()        L1943-1974

输入：原始 K 线 DataFrame（trade_date, open, high, low, close, volume, amount）
输出：带所有技术指标的完整因子 DataFrame

使用示例:
    from services.factors.factor_pipeline import calculate_technical_factors
    df_with_factors = calculate_technical_factors(raw_kline_df)

    from services.factors.factor_pipeline import add_signal_indicators
    df_complete = add_signal_indicators(df_with_factors)
"""

import pandas as pd
import logging

logger = logging.getLogger(__name__)


def calculate_technical_factors(df: pd.DataFrame) -> pd.DataFrame:
    """
    计算技术指标因子：价格表现 + KDJ + MACD + 扩展指标。

    Args:
        df: 原始 K 线 DataFrame，需包含 trade_date, open, high, low, close, volume, amount
            建议先计算 df['change_pct'] = df['close'].pct_change()

    Returns:
        添加了技术指标的 DataFrame（原地修改）
    """
    if df.empty:
        return df

    # 计算价格表现因子
    df = _calculate_price_performance(df)

    # 计算 KDJ 指标
    df = _calculate_kdj(df)

    # 计算 MACD 指标
    df = _calculate_macd(df)

    # 计算扩展技术指标（MA5/10/20/60, RSI14, 布林带, ATR, CCI, ADX, OBV 等）
    from services.common.technical_indicators import add_all_extended_technical_indicators_to_df
    from services.common.sdk_column_mapping import map_tech_columns
    df = add_all_extended_technical_indicators_to_df(df)
    df = map_tech_columns(df)

    return df


def add_signal_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    计算补充信号指标：金叉/死叉、涨停统计、大涨大跌、价格位置、缺口比例。

    必须在 calculate_technical_factors() 之后调用（依赖 ma5/ma10/high/low/close 等列）。

    Args:
        df: 已包含技术指标的 DataFrame

    Returns:
        添加了信号指标的 DataFrame（原地修改）
    """
    if df.empty:
        return df

    # 1. 金叉死叉信号
    df['golden_cross'] = ((df['ma5'] > df['ma10']) & (df['ma5'].shift(1) <= df['ma10'].shift(1))).astype(int)
    df['death_cross'] = ((df['ma5'] < df['ma10']) & (df['ma5'].shift(1) >= df['ma10'].shift(1))).astype(int)

    # 2. 涨停统计（涨幅>9.5%视为涨停）
    df['is_limit_up'] = (df['change_pct'] * 100 > 9.5).astype(int)
    df['limit_up_count_10d'] = df['is_limit_up'].rolling(10, min_periods=1).sum()
    df['limit_up_count_20d'] = df['is_limit_up'].rolling(20, min_periods=1).sum()
    df['limit_up_count_30d'] = df['is_limit_up'].rolling(30, min_periods=1).sum()

    # 3. 连续涨停天数
    df['consecutive_limit_up'] = 0
    consec_count = 0
    for i in range(len(df)):
        if df['is_limit_up'].iloc[i] == 1:
            consec_count += 1
        else:
            consec_count = 0
        df.loc[df.index[i], 'consecutive_limit_up'] = consec_count

    # 4. 大涨大跌统计（涨幅>5%或跌幅>5%）
    df['large_gain_5d_count'] = (df['change_pct'] * 100 > 5).rolling(5, min_periods=1).sum().astype(int)
    df['large_loss_5d_count'] = (df['change_pct'] * 100 < -5).rolling(5, min_periods=1).sum().astype(int)

    # 5. 价格位置（距250日高低点比例）
    high_250 = df['high'].rolling(250, min_periods=1).max()
    low_250 = df['low'].rolling(250, min_periods=1).min()
    range_val = high_250 - low_250
    df['close_to_high_250d'] = (df['close'] - low_250) / range_val * 100
    df['close_to_low_250d'] = (high_250 - df['close']) / range_val * 100

    # 6. 缺口比例（跳空高开统计）
    df['gap_up'] = ((df['open'] > df['high'].shift(1)) & (df['open'] > df['low'].shift(1))).astype(int)
    df['gap_up_ratio'] = df['gap_up'].rolling(20, min_periods=1).sum() / 20 * 100

    # 7. 次日涨跌（训练用标签）
    df['next_period_change'] = df['close'].pct_change().shift(-1)
    df['is_traded'] = (df['volume'] > 0).astype(int)

    return df


# ================================================================
# 内部指标计算方法（从 calculator 提取，避免重复实例化）
# ================================================================

def _calculate_price_performance(df: pd.DataFrame) -> pd.DataFrame:
    """计算价格表现：涨幅、振幅、涨跌额、波动率、动量"""
    if df.empty:
        return df

    # 涨跌幅
    df['change_pct'] = df['close'].pct_change()

    # 涨跌额
    df['change'] = df['close'] - df['close'].shift(1)

    # 振幅
    df['amplitude'] = (df['high'] - df['low']) / df['close'].shift(1) * 100

    # 20日波动率
    df['volatility'] = df['change_pct'].rolling(20, min_periods=1).std()

    # 10日动量
    df['momentum_10d'] = df['close'].pct_change(periods=10)

    return df


def _calculate_kdj(df: pd.DataFrame, n: int = 9, m1: int = 3, m2: int = 3) -> pd.DataFrame:
    """计算 KDJ 指标"""
    if df.empty:
        return df

    low_list = df['low'].rolling(n, min_periods=1).min()
    high_list = df['high'].rolling(n, min_periods=1).max()
    rsv = (df['close'] - low_list) / (high_list - low_list) * 100

    k = rsv.ewm(com=m1 - 1, adjust=False).mean()
    d = k.ewm(com=m2 - 1, adjust=False).mean()
    j = 3 * k - 2 * d

    df['kdj_k'] = k
    df['kdj_d'] = d
    df['kdj_j'] = j

    return df


def _calculate_macd(df: pd.DataFrame, fast: int = 12, slow: int = 26, signal: int = 9) -> pd.DataFrame:
    """计算 MACD 指标"""
    if df.empty:
        return df

    ema_fast = df['close'].ewm(span=fast, adjust=False).mean()
    ema_slow = df['close'].ewm(span=slow, adjust=False).mean()
    dif = ema_fast - ema_slow
    dea = dif.ewm(span=signal, adjust=False).mean()
    macd = (dif - dea) * 2

    df['dif'] = dif
    df['dea'] = dea
    df['macd'] = macd

    return df


# ================================================================
# 便捷函数：从 K 线原始数据到完整因子（一步到位）
# ================================================================

def process_kline_to_factors(df: pd.DataFrame) -> pd.DataFrame:
    """
    一键计算所有技术指标 + 信号指标。

    输入要求：包含 trade_date, open, high, low, close, volume, amount 的 DataFrame
    输出：包含所有 62 个因子列的完整 DataFrame
    """
    if df.empty:
        return df

    df['stock_code'] = df.get('stock_code', df.get('code', ''))
    df = calculate_technical_factors(df)
    df = add_signal_indicators(df)

    return df
