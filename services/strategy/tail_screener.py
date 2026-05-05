#!/usr/bin/env python3
"""
尾盘策略选股扫描 — 系统适配版

调度服务设定运行时间和候选股票分组，返回符合条件的股票信号。

数据路由：
- 盘中（09:00-16:00）：本地历史K线 + TGW当日实时OHLCV拼接 → 重算指标
- 盘后（≥16:00/非交易日）：纯本地kline.db数据（当日数据已下载完成）
- 技术指标（RSI/MACD/BOLL/ADX）：从K线自行计算
- Kronos预测：仅对初筛通过的 top 候选执行
"""
import sys, os, json, statistics, datetime

# ── Kronos 路径 ──────────────────────────────────────────
KRONOS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "deps", "Kronos")
if KRONOS_DIR not in sys.path:
    sys.path.insert(0, KRONOS_DIR)

os.environ['HF_ENDPOINT'] = 'https://hf-mirror.com'

import pandas as pd
import numpy as np
import safetensors, torch
from model import Kronos, KronosTokenizer, KronosPredictor

# ── 指标函数 ─────────────────────────────────────────
def calc_rsi_wilder(closes, period=14):
    if len(closes) < period + 1:
        return None
    gains, losses = [], []
    for i in range(1, len(closes)):
        diff = closes[i] - closes[i-1]
        gains.append(diff if diff > 0 else 0)
        losses.append(abs(diff) if diff < 0 else 0)
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    if avg_loss == 0:
        return 100.0
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

def calc_adx(highs, lows, closes, period=14):
    if len(highs) < period * 2:
        return None
    trs, plus_dm, minus_dm = [], [], []
    for i in range(1, len(highs)):
        high_diff = highs[i] - highs[i-1]
        low_diff  = lows[i-1] - lows[i]
        plus_dm.append(high_diff if high_diff > low_diff and high_diff > 0 else 0)
        minus_dm.append(low_diff  if low_diff > high_diff and low_diff > 0 else 0)
        tr = max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1]))
        trs.append(tr)
    atr = sum(trs[-period:]) / period
    plus_di  = (sum(plus_dm[-period:])  / atr) * 100 if atr > 0 else 0
    minus_di = (sum(minus_dm[-period:]) / atr) * 100 if atr > 0 else 0
    dx = abs(plus_di - minus_di) / (plus_di + minus_di) * 100 if (plus_di + minus_di) > 0 else 0
    return dx

def calc_boll_pos(closes, period=20):
    if len(closes) < period:
        return None
    recent = closes[-period:]
    mean = statistics.mean(recent)
    std = statistics.stdev(recent)
    lower = mean - 2 * std
    upper = mean + 2 * std
    pos = (closes[-1] - lower) / (upper - lower) * 100 if (upper - lower) > 0 else 50
    return pos

def get_tail_score(res):
    score = 0
    reasons = []
    chg = res.get('change_pct', 0)
    rsi = res.get('rsi14')
    boll = res.get('boll_pos')

    if 1 <= chg <= 5:
        score += 20
        reasons.append(f'涨幅+{chg:.2f}%(+20)')
    if rsi and 40 <= rsi <= 65:
        score += 20
        reasons.append(f'RSI={rsi:.1f}(+20)')
    if boll and 30 <= boll <= 70:
        score += 10
        reasons.append(f'BOLL={boll:.1f}%(+10)')
    if res.get('ma_gold_cross'):
        score += 10
        reasons.append('MA金叉(+10)')
    if res.get('tail_up'):
        score += 15
        reasons.append('尾盘拉升(+15)')
    if res.get('adx') and res['adx'] >= 25:
        score += 10
        reasons.append(f'ADX={res["adx"]:.1f}(+10)')
    if res.get('macd') is not None and res.get('macd_signal') is not None and res['macd'] > res['macd_signal']:
        score += 5
        reasons.append('MACD多头(+5)')

    return score, reasons


# ── Kronos 加载 ───────────────────────────────────────────
WEIGHTS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "deps", "Kronos", "weights")

def find_safetensors_cached(repo_name):
    """在本地权重目录查找模型文件"""
    weight_path = os.path.join(WEIGHTS_DIR, repo_name)
    if os.path.exists(weight_path):
        for f in os.listdir(weight_path):
            if f.endswith('.safetensors'):
                return os.path.join(weight_path, f)
    return None

def load_kronos(use_base=False):
    """加载 Kronos 模型"""
    TOKENIZER_CONFIG = {
        'd_in': 6, 'd_model': 256, 'n_heads': 4, 'ff_dim': 512,
        'n_enc_layers': 4, 'n_dec_layers': 4, 'ffn_dropout_p': 0.0,
        'attn_dropout_p': 0.0, 'resid_dropout_p': 0.0,
        's1_bits': 10, 's2_bits': 10, 'beta': 0.05,
        'gamma0': 1.0, 'gamma': 1.1, 'zeta': 0.05, 'group_size': 4
    }
    MODEL_CONFIG = {
        's1_bits': 10, 's2_bits': 10, 'n_layers': 6,
        'd_model': 512, 'n_heads': 8, 'ff_dim': 1024,
        'ffn_dropout_p': 0.0, 'attn_dropout_p': 0.0,
        'resid_dropout_p': 0.0, 'token_dropout_p': 0.0, 'learn_te': False
    }

    tok_path = find_safetensors_cached('Kronos-Tokenizer-base')
    model_name = 'Kronos-base' if use_base else 'Kronos-small'
    model_path = find_safetensors_cached(model_name)
    print(f'  Tokenizer: {tok_path}')
    print(f'  Model: {model_path}')

    tokenizer = KronosTokenizer(**TOKENIZER_CONFIG)
    model = Kronos(**MODEL_CONFIG)
    if tok_path:
        tokenizer.load_state_dict(safetensors.torch.load_file(tok_path), strict=False)
    if model_path:
        model.load_state_dict(safetensors.torch.load_file(model_path), strict=False)

    device = 'cuda:0' if torch.cuda.is_available() else 'cpu'
    model, tokenizer = model.to(device), tokenizer.to(device)
    predictor = KronosPredictor(model, tokenizer, max_context=512)
    print(f'  Kronos loaded on {device}')
    return predictor


# ── 主策略函数 ──────────────────────────────────────────
def run(context):
    """
    尾盘选股策略入口

    Args:
        context: {
            "stocks": [...],  # 候选组股票（含 stock_code, stock_name）
            "account_id": str,
            "today": str,     # YYYY-MM-DD
            "get_kline_smart": fn,   # 智能K线获取：盘中=本地历史+TGW实时拼接， 盘后=纯本地
        }

    Returns:
        signals: [{
            "action": "buy",
            "stock_code": "600519.SH",
            "stock_name": "贵州茅台",
            "buy_price": 1800.00,
            "stop_loss_pct": 0.03,
            "take_profit_pct": 0.10,
            "reason": "尾盘评分+理由",
            "target_quantity": 100,
        }]
    """
    # 获取候选股票
    stocks = context.get("stocks", [])
    if not stocks:
        print("尾盘策略：候选组无股票，跳过")
        return []

    stock_codes = [s.get("stock_code", "") for s in stocks if s.get("stock_code")]
    stock_names = {s["stock_code"]: s.get("stock_name", s["stock_code"]) for s in stocks}

    today_str = context.get("today", datetime.date.today().strftime('%Y-%m-%d'))
    today_int = int(today_str.replace('-', ''))

    # ── 获取 K 线数据（自动判断盘中/盘后）───
    print(f'尾盘策略：获取 {len(stock_codes)} 只股票的K线数据...')
    kline_data = get_kline_smart(stock_codes, lookback=100)
    print(f'尾盘策略：获取到 {len(kline_data)} 只股票的K线')

    # 加载 Kronos
    try:
        predictor = load_kronos()
        future_dates = pd.bdate_range(
            start=pd.Timestamp(str(today_int)) + pd.Timedelta(days=1),
            periods=5
        )
    except Exception as e:
        print(f"  Kronos 加载失败: {e}")
        predictor = None
        future_dates = None

    # ── 计算指标 ─────────────────────────────────────
    results = {}
    for code, rows in kline_data.items():
        if not rows or len(rows) < 30:
            continue
        try:
            # 本地数据格式: List[Dict] with trade_date
            df = pd.DataFrame(rows)
            df['date'] = pd.to_datetime(df['trade_date']).dt.date
            daily = df.groupby('date').agg(
                open=('open', 'first'), high=('high', 'max'), low=('low', 'min'),
                close=('close', 'last'), volume=('volume', 'sum'), amount=('amount', 'sum')
            ).reset_index()

            if len(daily) < 30:
                continue

            daily['timestamps'] = pd.to_datetime(daily['date'].values)

            closes = daily['close'].tolist()
            highs  = daily['high'].tolist()
            lows   = daily['low'].tolist()
            opens  = daily['open'].tolist()

            today_row = daily.iloc[-1]
            yest_row  = daily.iloc[-2] if len(daily) >= 2 else None

            cur_close = float(today_row['close'])
            cur_open  = float(today_row['open'])
            pre_close = float(yest_row['close']) if yest_row is not None else cur_close
            chg_pct   = (cur_close - pre_close) / pre_close * 100 if pre_close > 0 else 0

            rsi14   = calc_rsi_wilder(closes)
            boll_pos = calc_boll_pos(closes)
            adx_val = calc_adx(highs, lows, closes)

            ma5  = sum(closes[-5:]) / 5
            ma20 = sum(closes[-20:]) / 20 if len(closes) >= 20 else None
            ma_gold = (ma20 is not None) and (ma5 > ma20)

            s = pd.Series(closes)
            ema12 = s.ewm(span=12, adjust=False).mean()
            ema26 = s.ewm(span=26, adjust=False).mean()
            macd_val = float(ema12.iloc[-1] - ema26.iloc[-1])
            macd_sig = float(s.ewm(span=9, adjust=False).mean().iloc[-1])

            tail_up = cur_close > opens[-1] * 1.002 if len(opens) > 0 else False

            res = {
                'close': cur_close, 'open': cur_open, 'pre_close': pre_close,
                'change_pct': chg_pct, 'rsi14': rsi14, 'boll_pos': boll_pos,
                'ma_gold_cross': ma_gold, 'adx': adx_val,
                'macd': macd_val, 'macd_signal': macd_sig,
                'tail_up': tail_up, 'daily': daily,
            }

            results[code] = res

        except Exception as e:
            print(f'  {code} 处理错误: {e}')

    print(f'尾盘策略：成功处理 {len(results)} 只')

    # ── 初筛打分（不含 Kronos）─────────────────────────
    prelim = []
    for code, res in results.items():
        chg = res.get('change_pct', 0)
        rsi = res.get('rsi14', 50)
        boll = res.get('boll_pos', 50)

        # 过滤
        if chg < 0.5 or chg > 9:
            continue
        if rsi and (rsi < 20 or rsi > 80):
            continue
        if boll and (boll > 85 or boll < 10):
            continue

        tail_score, reasons = get_tail_score(res)
        prelim.append({
            'code': code, 'close': res['close'], 'change_pct': chg,
            'rsi14': rsi, 'boll_pos': boll, 'tail_score': tail_score,
            'reasons': reasons, 'daily': res.get('daily'),
        })

    prelim.sort(key=lambda x: x['tail_score'], reverse=True)
    print(f'尾盘策略：初筛通过 {len(prelim)} 只，对 top 候选进行 Kronos 预测')

    # ── Kronos 预测（仅对初筛通过的 top 候选）──────────
    if predictor is not None and future_dates is not None and prelim:
        for item in prelim[:10]:  # 最多对前10只做Kronos
            try:
                daily = item['daily']
                lookback = len(daily) - 2
                if lookback < 20:
                    item['kronos_pred'] = None
                    continue

                df_hist = daily.loc[:lookback-1, ['open','high','low','close','volume','amount']]
                x_ts = pd.Series(daily.loc[:lookback-1, 'timestamps'].values, dtype='datetime64[ns]')
                y_ts = pd.Series(pd.to_datetime(future_dates), dtype='datetime64[ns]')

                pred_df = predictor.predict(
                    df=df_hist.reset_index(drop=True),
                    x_timestamp=x_ts.reset_index(drop=True),
                    y_timestamp=y_ts.reset_index(drop=True),
                    pred_len=5, T=1.0, top_p=0.9, sample_count=10
                )

                if pred_df is not None and len(pred_df) > 0:
                    last_close = float(daily['close'].iloc[lookback-1])
                    avg_pred = pred_df['close'].mean()
                    item['kronos_pred'] = (avg_pred - last_close) / last_close * 100
                else:
                    item['kronos_pred'] = None
            except Exception as e:
                item['kronos_pred'] = None

    kronos_count = sum(1 for p in prelim if p.get('kronos_pred') is not None)
    print(f'尾盘策略：Kronos预测完成 {kronos_count} 只')

    # ── 综合评分 & 取 top5 ────────────────────────────
    scored = []
    for item in prelim:
        kronos_pred = item.get('kronos_pred')
        if kronos_pred is not None:
            composite = item['tail_score'] * 0.5 + kronos_pred * 2
        else:
            composite = item['tail_score'] * 0.5

        scored.append({
            'code': item['code'], 'close': item['close'], 'change_pct': item['change_pct'],
            'rsi14': item['rsi14'], 'boll_pos': item['boll_pos'],
            'tail_score': item['tail_score'], 'kronos_pred': kronos_pred,
            'composite': composite, 'reasons': item['reasons'],
        })

    scored.sort(key=lambda x: x['composite'], reverse=True)
    top5 = scored[:5]

    # ── 大盘情绪 ────────────────────────────────────────
    market_chg = None
    market_rsi = None
    mood = '未知'
    for bench in ['600300.SH', '000300.SH', '600016.SH', '600030.SH', '601318.SH']:
        if bench in results:
            market_chg = results[bench]['change_pct']
            market_rsi = results[bench]['rsi14']
            break

    if market_chg is not None:
        if market_chg > 1:
            mood = '偏多'
        elif market_chg < -1:
            mood = '偏空'
        else:
            mood = '震荡'

    print(f'尾盘策略：大盘情绪 {mood} | 符合条件 {len(scored)} 只')

    # ── 返回信号 ────────────────────────────────────────
    signals = []
    for s in top5:
        code = s['code']
        name = stock_names.get(code, code)
        buy_price = s['close']

        # 根据综合评分计算止损止盈比例
        if s['composite'] >= 55:
            stop_loss_pct = 0.03
            take_profit_pct = 0.10
        elif s['composite'] >= 35:
            stop_loss_pct = 0.05
            take_profit_pct = 0.15
        else:
            stop_loss_pct = 0.05
            take_profit_pct = 0.08

        reason_parts = s['reasons'] if s['reasons'] else ['尾盘策略信号']
        reason_parts.append(f'综合评分:{s["composite"]:.1f}')
        reason_parts.append(f'大盘:{mood}')
        if s['kronos_pred'] is not None:
            reason_parts.append(f'Kronos预测:{s["kronos_pred"]:+.2f}%')

        signals.append({
            "action": "buy",
            "stock_code": code,
            "stock_name": name,
            "buy_price": round(buy_price, 2),
            "stop_loss_pct": stop_loss_pct,
            "take_profit_pct": take_profit_pct,
            "reason": " | ".join(reason_parts),
            "target_quantity": 100,
        })

    return signals
