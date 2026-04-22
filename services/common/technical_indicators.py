"""
技术指标计算核心模块

提供统一的技术指标计算实现，基于 pandas 向量化计算
所有技术指标计算都应该使用这个模块，确保结果一致性

Usage:
    from services.common.technical_indicators import calculate_ma, calculate_macd, ...
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Optional, Union


# ==================== 基础指标计算 ====================

def calculate_ma(prices: Union[List[float], pd.Series], period: int) -> Optional[float]:
    """
    简单移动平均线 (Moving Average)

    Args:
        prices: 收盘价列表或 Series（从旧到新）
        period: 周期

    Returns:
        MA 值，如果数据不足则返回 None
    """
    if len(prices) < period:
        return None

    if isinstance(prices, list):
        prices = pd.Series(prices)

    return prices.tail(period).mean()


def calculate_ema(prices: Union[List[float], pd.Series], period: int) -> Optional[float]:
    """
    指数移动平均线 (Exponential Moving Average)

    Args:
        prices: 收盘价列表或 Series
        period: 周期

    Returns:
        EMA 值
    """
    if len(prices) < period:
        return None

    if isinstance(prices, list):
        prices = pd.Series(prices)

    return prices.ewm(span=period, adjust=False).mean().iloc[-1]


def calculate_rsi(prices: Union[List[float], pd.Series], period: int = 14) -> Optional[float]:
    """
    相对强弱指数 (Relative Strength Index)

    Args:
        prices: 收盘价列表或 Series
        period: 周期（默认 14）

    Returns:
        RSI 值 (0-100)，数据不足返回 None
    """
    if len(prices) < period + 1:
        return None

    if isinstance(prices, list):
        prices = pd.Series(prices)

    # 计算价格变化
    delta = prices.diff()

    # 分离涨跌幅
    gains = delta.where(delta > 0, 0)
    losses = (-delta).where(delta < 0, 0)

    # 计算平均涨幅和跌幅
    avg_gain = gains.tail(period).mean()
    avg_loss = losses.tail(period).mean()

    if avg_loss == 0:
        return 100

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))

    return rsi


def calculate_macd(
    prices: Union[List[float], pd.Series],
    fast: int = 12,
    slow: int = 26,
    signal: int = 9
) -> Optional[Dict[str, float]]:
    """
    MACD (Moving Average Convergence Divergence)

    Args:
        prices: 收盘价列表或 Series
        fast: 快线周期（默认 12）
        slow: 慢线周期（默认 26）
        signal: 信号线周期（默认 9）

    Returns:
        包含 MACD 线、信号线、柱状图的字典：
        - dif: DIF 线（快线 - 慢线）
        - dea: DEA 线（DIF 的 signal 日 EMA）
        - macd: MACD 柱状图（2 * (DIF - DEA)）
    """
    if len(prices) < slow + signal:
        return None

    if isinstance(prices, list):
        prices = pd.Series(prices)

    # 计算快慢 EMA
    ema_fast = prices.ewm(span=fast, adjust=False).mean()
    ema_slow = prices.ewm(span=slow, adjust=False).mean()

    # 计算 DIF
    dif = ema_fast - ema_slow

    # 计算 DEA（DIF 的 signal 日 EMA）
    dea = dif.ewm(span=signal, adjust=False).mean()

    # 计算 MACD 柱状图
    macd_bar = 2 * (dif - dea)

    return {
        "dif": dif.iloc[-1],
        "dea": dea.iloc[-1],
        "macd": macd_bar.iloc[-1]
    }


def calculate_kdj(
    highs: Union[List[float], pd.Series],
    lows: Union[List[float], pd.Series],
    closes: Union[List[float], pd.Series],
    n: int = 9,
    m1: int = 3,
    m2: int = 3
) -> Optional[Dict[str, float]]:
    """
    KDJ 指标

    Args:
        highs: 最高价列表或 Series
        lows: 最低价列表或 Series
        closes: 收盘价列表或 Series
        n: RSV 计算周期（默认 9）
        m1: K 值平滑周期（默认 3）
        m2: D 值平滑周期（默认 3）

    Returns:
        包含 K、D、J 值的字典
    """
    if len(closes) < n:
        return None

    if isinstance(highs, list):
        highs = pd.Series(highs)
    if isinstance(lows, list):
        lows = pd.Series(lows)
    if isinstance(closes, list):
        closes = pd.Series(closes)

    # 计算 N 日最高价和最低价
    lowest_low = lows.rolling(window=n).min()
    highest_high = highs.rolling(window=n).max()

    # 计算 RSV
    rsv = (closes - lowest_low) / (highest_high - lowest_low) * 100
    rsv = rsv.dropna()

    if len(rsv) < m1:
        return None

    # 计算 K、D、J（使用指数移动平均）
    k = rsv.ewm(com=m1-1, adjust=False).mean()
    d = k.ewm(com=m2-1, adjust=False).mean()
    j = 3 * k - 2 * d

    return {
        "k": k.iloc[-1],
        "d": d.iloc[-1],
        "j": j.iloc[-1]
    }


def calculate_bollinger_bands(
    prices: Union[List[float], pd.Series],
    period: int = 20,
    std_dev: float = 2.0
) -> Optional[Dict[str, float]]:
    """
    布林带 (Bollinger Bands)

    Args:
        prices: 收盘价列表或 Series
        period: 周期（默认 20）
        std_dev: 标准差倍数（默认 2）

    Returns:
        包含上轨、中轨、下轨的字典
    """
    if len(prices) < period:
        return None

    if isinstance(prices, list):
        prices = pd.Series(prices)

    # 中轨 = N 日移动平均
    middle = prices.rolling(window=period).mean().iloc[-1]

    # 计算标准差
    std = prices.rolling(window=period).std().iloc[-1]

    return {
        "upper": middle + (std_dev * std),
        "middle": middle,
        "lower": middle - (std_dev * std),
        "std": std
    }


def calculate_atr(
    highs: Union[List[float], pd.Series],
    lows: Union[List[float], pd.Series],
    closes: Union[List[float], pd.Series],
    period: int = 14
) -> Optional[float]:
    """
    平均真实波幅 (Average True Range)

    Args:
        highs: 最高价列表或 Series
        lows: 最低价列表或 Series
        closes: 收盘价列表或 Series
        period: 周期（默认 14）

    Returns:
        ATR 值
    """
    if len(highs) < period + 1:
        return None

    if isinstance(highs, list):
        highs = pd.Series(highs)
    if isinstance(lows, list):
        lows = pd.Series(lows)
    if isinstance(closes, list):
        closes = pd.Series(closes)

    # 计算真实波幅
    tr1 = highs - lows  # 当日最高 - 最低
    tr2 = (highs - closes.shift(1)).abs()  # 当日最高 - 昨收
    tr3 = (lows - closes.shift(1)).abs()  # 当日最低 - 昨收

    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    return tr.tail(period).mean()


def calculate_adx(
    highs: Union[List[float], pd.Series],
    lows: Union[List[float], pd.Series],
    closes: Union[List[float], pd.Series],
    period: int = 14
) -> Optional[float]:
    """
    平均趋向指标 (Average Directional Index)

    Args:
        highs: 最高价列表或 Series
        lows: 最低价列表或 Series
        closes: 收盘价列表或 Series
        period: 周期（默认 14）

    Returns:
        ADX 值 (0-100)
    """
    if len(highs) < period + 1:
        return None

    if isinstance(highs, list):
        highs = pd.Series(highs)
    if isinstance(lows, list):
        lows = pd.Series(lows)
    if isinstance(closes, list):
        closes = pd.Series(closes)

    # 计算 +DM 和 -DM
    diff_high = highs.diff()
    diff_low = lows.diff()

    plus_dm = diff_high.copy()
    minus_dm = diff_low.copy()

    plus_dm[(diff_high <= 0) | (diff_high < diff_low.abs())] = 0
    minus_dm[(diff_low <= 0) | (diff_low.abs() < diff_high)] = 0

    # 计算 TR
    tr1 = highs - lows
    tr2 = (highs - closes.shift(1)).abs()
    tr3 = (lows - closes.shift(1)).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    # 平滑计算
    plus_tr14 = plus_dm.ewm(span=period, adjust=False).mean()
    minus_tr14 = minus_dm.ewm(span=period, adjust=False).mean()
    tr14 = tr.ewm(span=period, adjust=False).mean()

    # 计算 +DI 和 -DI
    plus_di = 100 * plus_tr14 / tr14
    minus_di = 100 * minus_tr14 / tr14

    # 计算 DX
    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di)

    # 计算 ADX (DX 的平滑)
    adx = dx.ewm(span=period, adjust=False).mean()

    return adx.iloc[-1] if len(adx) > 0 else None


def calculate_obv(
    closes: Union[List[float], pd.Series],
    volumes: Union[List[float], pd.Series]
) -> Optional[float]:
    """
    能量潮 (On-Balance Volume)

    Args:
        closes: 收盘价列表或 Series
        volumes: 成交量列表或 Series

    Returns:
        最新 OBV 值
    """
    if len(closes) < 2 or len(volumes) < 2:
        return None

    if isinstance(closes, list):
        closes = pd.Series(closes)
    if isinstance(volumes, list):
        volumes = pd.Series(volumes)

    # 计算价格方向
    price_diff = closes.diff()

    # 计算 OBV
    obv = volumes.copy()
    obv[price_diff > 0] = volumes[price_diff > 0]
    obv[price_diff < 0] = -volumes[price_diff < 0]
    obv[price_diff == 0] = 0

    # 返回float类型，避免numpy.int64被SQLite存储为blob
    return float(obv.cumsum().iloc[-1])


def calculate_historical_volatility(
    closes: Union[List[float], pd.Series],
    period: int = 20,
    annualize: bool = True
) -> Optional[float]:
    """
    历史波动率 (Historical Volatility)

    Args:
        closes: 收盘价列表或 Series
        period: 计算周期（默认 20）
        annualize: 是否年化（默认 True，A 股年化因子约 252）

    Returns:
        历史波动率（百分比）
    """
    if len(closes) < period + 1:
        return None

    if isinstance(closes, list):
        closes = pd.Series(closes)

    # 计算对数收益率
    returns = closes.pct_change().apply(lambda x: np.log(1 + x) if pd.notna(x) and x != -1 else np.nan)

    # 计算标准差
    std = returns.tail(period).std()

    if annualize:
        std *= np.sqrt(252)  # A 股年化处理

    return std * 100  # 转换为百分比


# ==================== DataFrame 批量计算（用于因子计算） ====================

def add_price_performance_to_df(
    df: pd.DataFrame,
    windows: List[int] = [5, 10, 20]
) -> pd.DataFrame:
    """
    为 DataFrame 添加市场表现类因子

    添加的列：
    - change_Nd: N 日涨跌幅
    - bias_N: N 日乖离率
    - amplitude_N: N 日振幅
    - change_std_N: N 日涨跌幅标准差
    - amount_std_N: N 日成交额标准差

    Args:
        df: 包含 close, high, low, amount 列的 DataFrame
        windows: 周期窗口列表

    Returns:
        添加了因子列的 DataFrame
    """
    df = df.copy()
    df = df.sort_index().reset_index(drop=True)

    for window in windows:
        # N 日涨跌幅
        df[f'change_{window}d'] = df['close'].pct_change(window)

        # N 日乖离率
        ma_n = df['close'].rolling(window=window).mean()
        df[f'bias_{window}'] = (df['close'] - ma_n) / ma_n

        # N 日振幅
        highest = df['high'].rolling(window=window).max()
        lowest = df['low'].rolling(window=window).min()
        prev_close = df['close'].shift(window)
        df[f'amplitude_{window}'] = (highest - lowest) / prev_close

        # N 日涨跌幅标准差
        df[f'change_std_{window}'] = df['close'].pct_change().rolling(window=window).std()

        # N 日成交额标准差
        df[f'amount_std_{window}'] = df['amount'].rolling(window=window).std()

    return df


def add_kdj_to_df(
    df: pd.DataFrame,
    n: int = 9,
    m1: int = 3,
    m2: int = 3
) -> pd.DataFrame:
    """
    为 DataFrame 添加 KDJ 指标

    添加的列：
    - kdj_k: K 值
    - kdj_d: D 值
    - kdj_j: J 值

    Args:
        df: 包含 high, low, close 列的 DataFrame
        n: RSV 计算周期
        m1: K 值平滑周期
        m2: D 值平滑周期

    Returns:
        添加了 KDJ 列的 DataFrame
    """
    df = df.copy()

    # 计算 N 日最高价和最低价
    lowest_low = df['low'].rolling(window=n).min()
    highest_high = df['high'].rolling(window=n).max()

    # 计算 RSV
    rsv = (df['close'] - lowest_low) / (highest_high - lowest_low) * 100

    # 计算 K、D、J
    df['kdj_k'] = rsv.ewm(com=m1-1, adjust=False).mean()
    df['kdj_d'] = df['kdj_k'].ewm(com=m2-1, adjust=False).mean()
    df['kdj_j'] = 3 * df['kdj_k'] - 2 * df['kdj_d']

    return df


def add_macd_to_df(
    df: pd.DataFrame,
    fast: int = 12,
    slow: int = 26,
    signal: int = 9
) -> pd.DataFrame:
    """
    为 DataFrame 添加 MACD 指标

    添加的列：
    - dif: DIF 线
    - dea: DEA 线
    - macd: MACD 柱状图

    Args:
        df: 包含 close 列的 DataFrame
        fast: 快线周期
        slow: 慢线周期
        signal: 信号线周期

    Returns:
        添加了 MACD 列的 DataFrame
    """
    df = df.copy()

    # 计算快慢 EMA
    ema_fast = df['close'].ewm(span=fast, adjust=False).mean()
    ema_slow = df['close'].ewm(span=slow, adjust=False).mean()

    # 计算 DIF、DEA、MACD
    df['dif'] = ema_fast - ema_slow
    df['dea'] = df['dif'].ewm(span=signal, adjust=False).mean()
    df['macd'] = 2 * (df['dif'] - df['dea'])

    return df


def add_all_technical_indicators_to_df(
    df: pd.DataFrame,
    ma_windows: List[int] = [5, 10, 20],
    kdj_n: int = 9,
    macd_fast: int = 12,
    macd_slow: int = 26,
    macd_signal: int = 9
) -> pd.DataFrame:
    """
    为 DataFrame 添加所有技术指标

    Args:
        df: 包含 open, high, low, close, volume, amount 列的 DataFrame
        ma_windows: MA 周期窗口
        kdj_n: KDJ 的 N 周期
        macd_fast: MACD 快线周期
        macd_slow: MACD 慢线周期
        macd_signal: MACD 信号线周期

    Returns:
        添加了所有技术指标的 DataFrame
    """
    df = df.copy()
    df = df.sort_index().reset_index(drop=True)

    # 计算基础涨跌幅
    df['change_pct'] = df['close'].pct_change()

    # 添加 MA 指标
    for window in ma_windows:
        df[f'ma_{window}'] = df['close'].rolling(window=window).mean()

    # 添加 KDJ
    df = add_kdj_to_df(df, n=kdj_n)

    # 添加 MACD
    df = add_macd_to_df(df, fast=macd_fast, slow=macd_slow, signal=macd_signal)

    # 添加价格表现因子
    df = add_price_performance_to_df(df)

    return df


def add_all_extended_technical_indicators_to_df(
    df: pd.DataFrame,
    ma_windows: List[int] = [5, 10, 20, 60],
    boll_period: int = 20,
    boll_std: float = 2.0,
    atr_period: int = 14,
    rsi_period: int = 14,
    cci_period: int = 20,
    adx_period: int = 14,
    hv_period: int = 20
) -> pd.DataFrame:
    """
    为 DataFrame 添加所有扩展技术指标

    添加的因子：
    - 趋势类：MA5/10/20/60, EMA12/26, ADX
    - 动量类：RSI, CCI, 动量 (10d/20d)
    - 波动类：布林带、ATR、历史波动率
    - 成交量类：OBV, 量比
    - 形态类：金叉/死叉状态

    Args:
        df: 包含 open, high, low, close, volume 列的 DataFrame
        ma_windows: MA 周期窗口
        boll_period: 布林带周期
        boll_std: 布林带标准差倍数
        atr_period: ATR 周期
        rsi_period: RSI 周期
        cci_period: CCI 周期
        adx_period: ADX 周期
        hv_period: 历史波动率周期

    Returns:
        添加了所有扩展技术指标的 DataFrame
    """
    df = df.copy()
    df = df.sort_index().reset_index(drop=True)

    # 基础涨跌幅
    df['change_pct'] = df['close'].pct_change()

    # ==================== 趋势类 ====================
    # MA
    for window in ma_windows:
        df[f'ma_{window}'] = df['close'].rolling(window=window).mean()

    # EMA
    df['ema12'] = df['close'].ewm(span=12, adjust=False).mean()
    df['ema26'] = df['close'].ewm(span=26, adjust=False).mean()

    # ADX
    df['adx'] = np.nan
    if len(df) >= adx_period + 1:
        adx_values = []
        for i in range(len(df)):
            if i < adx_period:
                adx_values.append(np.nan)
            else:
                adx = calculate_adx(
                    df['high'].iloc[:i+1],
                    df['low'].iloc[:i+1],
                    df['close'].iloc[:i+1],
                    adx_period
                )
                adx_values.append(adx)
        df['adx'] = adx_values

    # ==================== 动量类 ====================
    # RSI
    def calc_rsi(series):
        if len(series) < 15:
            return np.nan
        delta = series.diff()
        gains = delta.where(delta > 0, 0)
        losses = (-delta).where(delta < 0, 0)
        avg_gain = gains.tail(14).mean()
        avg_loss = losses.tail(14).mean()
        if pd.isna(avg_gain) or pd.isna(avg_loss):
            return np.nan
        if avg_loss == 0:
            return 100
        rs = avg_gain / avg_loss
        return 100 - (100 / (1 + rs))

    df['rsi_14'] = df['close'].rolling(window=30).apply(calc_rsi, raw=False)

    # CCI
    typical_price = (df['high'] + df['low'] + df['close']) / 3
    df['cci_20'] = (typical_price - typical_price.rolling(window=20).mean()) / \
                   (0.015 * typical_price.rolling(window=20).apply(lambda x: np.mean(np.abs(x - x.mean()))))

    # 动量 (价格变化率)
    df['momentum_10d'] = df['close'].pct_change(10)
    df['momentum_20d'] = df['close'].pct_change(20)

    # ==================== 波动类 ====================
    # 布林带
    df['boll_middle'] = df['close'].rolling(window=boll_period).mean()
    boll_std_val = df['close'].rolling(window=boll_period).std()
    df['boll_upper'] = df['boll_middle'] + (boll_std_val * boll_std)
    df['boll_lower'] = df['boll_middle'] - (boll_std_val * boll_std)

    # ATR
    df['atr_14'] = np.nan
    if len(df) >= atr_period + 1:
        tr1 = df['high'] - df['low']
        tr2 = (df['high'] - df['close'].shift(1)).abs()
        tr3 = (df['low'] - df['close'].shift(1)).abs()
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        df['atr_14'] = tr.ewm(span=atr_period, adjust=False).mean()

    # 历史波动率
    df['returns'] = df['close'].pct_change().apply(lambda x: np.log(1 + x) if pd.notna(x) and x != -1 else np.nan)
    df['hv_20'] = df['returns'].rolling(window=hv_period).std() * np.sqrt(252) * 100

    # ==================== 成交量类 ====================
    # OBV (注意：结果需转换为Python原生float，否则numpy类型会被SQLite存储为blob)
    df['obv'] = 0.0
    price_diff = df['close'].diff()
    obv_values = []
    current_obv = 0.0
    for i in range(len(df)):
        if i == 0:
            obv_values.append(0.0)
        else:
            vol = float(df['volume'].iloc[i])
            if price_diff.iloc[i] > 0:
                current_obv += vol
            elif price_diff.iloc[i] < 0:
                current_obv -= vol
            obv_values.append(float(current_obv))  # 强制转为Python float
    df['obv'] = [float(v) for v in obv_values]  # 确保所有值为Python float

    # 量比 (当日成交量 / 5 日均量)
    df['volume_ratio'] = df['volume'] / df['volume'].rolling(window=5).mean()

    # ==================== 形态类 ====================
    # 金叉/死叉状态 (MA5 与 MA10, MA20)
    df['golden_cross'] = 0
    df['death_cross'] = 0

    if 'ma5' in df.columns and 'ma10' in df.columns:
        # 金叉：MA5 上穿 MA10
        ma5_above_ma10 = df['ma5'] > df['ma10']
        ma5_below_ma10_prev = ma5_above_ma10.shift(1) == False
        df.loc[ma5_above_ma10 & ma5_below_ma10_prev, 'golden_cross'] = 1

        # 死叉：MA5 下穿 MA10
        ma5_below_ma10 = df['ma5'] < df['ma10']
        ma5_above_ma10_prev = ma5_below_ma10.shift(1) == False
        df.loc[ma5_below_ma10 & ma5_above_ma10_prev, 'death_cross'] = 1

    # 清理临时列
    if 'returns' in df.columns:
        df.drop(columns=['returns'], inplace=True)

    return df


# ==================== 选股模块兼容接口 ====================

def calculate_indicators_for_screening(
    closes: List[float],
    highs: Optional[List[float]] = None,
    lows: Optional[List[float]] = None,
    volumes: Optional[List[float]] = None
) -> Dict[str, float]:
    """
    为选股模块计算技术指标（兼容原有接口）

    Args:
        closes: 收盘价列表
        highs: 最高价列表（可选，用于 KDJ）
        lows: 最低价列表（可选，用于 KDJ）
        volumes: 成交量列表（可选）

    Returns:
        指标字典：ma5, ma10, ma20, rsi, kdj_k, kdj_d, kdj_j, macd, dif, dea
    """
    indicators = {}

    # MA
    indicators['ma5'] = calculate_ma(closes, 5)
    indicators['ma10'] = calculate_ma(closes, 10)
    indicators['ma20'] = calculate_ma(closes, 20)
    indicators['ma60'] = calculate_ma(closes, 60)

    # RSI
    indicators['rsi'] = calculate_rsi(closes, 14)

    # KDJ
    if highs is not None and lows is not None:
        kdj = calculate_kdj(highs, lows, closes)
        if kdj:
            indicators['kdj_k'] = kdj['k']
            indicators['kdj_d'] = kdj['d']
            indicators['kdj_j'] = kdj['j']

    # MACD
    macd = calculate_macd(closes)
    if macd:
        indicators['macd'] = macd['macd']
        indicators['dif'] = macd['dif']
        indicators['dea'] = macd['dea']

    # 成交量均线
    if volumes is not None and len(volumes) >= 5:
        indicators['ma_vol5'] = calculate_ma(volumes, 5)
        indicators['ma_vol'] = indicators['ma_vol5']

    # 最新价格
    if closes:
        indicators['price'] = closes[-1]

    # 最新成交量
    if volumes is not None:
        indicators['volume'] = volumes[-1]

    return indicators
