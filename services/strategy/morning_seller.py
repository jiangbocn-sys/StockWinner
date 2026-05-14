#!/usr/bin/env python3
"""
短线卖出 — 早盘跟踪策略

配合"尾盘策略"使用：前一交易日尾盘买入的股票，次日早盘由本策略跟踪卖出。

运行时间：每个交易日 09:30 ~ 10:00（建议每分钟执行一次）

卖出规则（满足任一即触发）：
1. 盈利 ≥ 2%，且从早盘最高价回落 ≥ 0.2% → 卖出
2. 亏损 ≥ 1.5%，且分钟线跌速 ≥ 0.8% → 卖出
3. 亏损 ≥ 2.5% → 卖出
4. 以上都不成立，到 10:00 强制卖出

策略参数（保存在 strategies.config 字段 JSON 中）：
  profit_trigger_pct: 0.02       盈利触发阈值
  drop_from_high_pct: 0.002      从最高价回落阈值
  loss_drop_trigger_pct: 0.015   亏损+跌速阈值
  price_drop_speed_pct: 0.008    分钟线跌速阈值
  max_loss_pct: 0.025            最大亏损阈值
  exit_hour: 10                  强制卖出时间（时）
  exit_minute: 0                 强制卖出时间（分）
"""
import json
import sqlite3

from services.common.timezone import get_china_time

def calc_minute_drop_speed(stock_code, current_price):
    """计算分钟线跌速（最近 10 根 1 分钟 K 线的跌幅）"""
    try:
        klines = get_kline_local(stock_code, period="1m", limit=10)
        if not klines or len(klines) < 3:
            return 0.0
        if isinstance(klines, list) and len(klines) > 0:
            base_price = float(klines[0].get("close", 0))
        else:
            return 0.0
        if base_price <= 0:
            return 0.0
        drop = (base_price - current_price) / base_price
        return max(0.0, drop)
    except Exception as e:
        print(f"  {stock_code} 跌速计算失败: {e}")
        return 0.0


def update_highest_price(account_id, stock_code, highest_price):
    """同步更新 stock_positions 的最高价（直接 SQLite 连接）"""
    try:
        from services.common.database import get_sync_connection
        conn = get_sync_connection()
        conn.execute(
            "UPDATE stock_positions SET highest_price = ? WHERE account_id = ? AND stock_code = ?",
            (highest_price, account_id, stock_code)
        )
        conn.commit()
    except Exception as e:
        print(f"  更新最高价失败 {stock_code}: {e}")


def run(context):
    """
    短线卖出策略入口

    Args:
        context: {
            "account_id": str,
            "today": str,
            "stocks": [...],          # 已买入股票（watchlist status='bought'）
            "positions": [...],       # 同 stocks，显式标注
            "strategy": dict,         # 策略记录（含 config 字段）
            "get_kline_local": fn,    # 本地 K 线（同步）
            "get_realtime_quote": fn, # 预取当日实时行情（同步）
            "code_scope": "trading",
        }

    Returns:
        signals: [{
            "action": "sell",
            "stock_code": "600519.SH",
            "stock_name": "贵州茅台",
            "buy_price": 1800.00,
            "reason": "早盘跟踪: 盈利达标后回落",
        }]
    """
    account_id = context.get("account_id", "")
    today = context.get("today", "")
    strategy = context.get("strategy", {})

    # 解析策略参数
    config_raw = strategy.get("config", "{}") if strategy else "{}"
    try:
        config = json.loads(config_raw) if isinstance(config_raw, str) else (config_raw or {})
    except Exception:
        config = {}

    profit_trigger = config.get("profit_trigger_pct", 0.02)
    drop_from_high = config.get("drop_from_high_pct", 0.002)
    loss_drop_trigger = config.get("loss_drop_trigger_pct", 0.015)
    drop_speed_threshold = config.get("price_drop_speed_pct", 0.008)
    max_loss = config.get("max_loss_pct", 0.025)
    exit_hour = config.get("exit_hour", 10)
    exit_minute = config.get("exit_minute", 0)

    now = get_china_time()
    print(f"[短线卖出] {today} 当前时间 {now.hour:02d}:{now.minute:02d}")

    # 获取已买入的股票
    stocks = context.get("stocks", [])
    if not stocks:
        print("[短线卖出] 无已买入股票，跳过")
        return []

    # 获取实时行情（预取）
    stock_codes = [s.get("stock_code", "") for s in stocks if s.get("stock_code")]
    realtime_quotes = {}
    for code in stock_codes:
        quote = get_realtime_quote(code)
        if quote:
            realtime_quotes[code] = quote

    signals = []

    for s in stocks:
        stock_code = s.get("stock_code", "")
        if not stock_code:
            continue
        stock_name = s.get("stock_name", stock_code)

        # 成本价
        avg_cost = s.get("buy_price", 0)
        if not avg_cost:
            print(f"  {stock_code} 无成本价，跳过")
            continue

        # 当前价：优先用实时行情，其次用记录中的价格
        quote = realtime_quotes.get(stock_code)
        if quote:
            current_price = float(quote.get("current_price", quote.get("close", avg_cost)))
        else:
            current_price = s.get("current_price", avg_cost)
            if current_price:
                current_price = float(current_price)
            else:
                current_price = avg_cost

        if avg_cost <= 0:
            continue

        # 盈亏比例
        pnl_pct = (current_price - avg_cost) / avg_cost

        # 更新早盘最高价
        session_high = s.get("highest_price", 0) or 0
        if current_price > session_high:
            session_high = current_price
            update_highest_price(account_id, stock_code, session_high)

        # ── 条件 1：盈利达标后回落 ──
        if pnl_pct >= profit_trigger and session_high > 0:
            drop_pct = (session_high - current_price) / session_high
            if drop_pct >= drop_from_high:
                signals.append({
                    "action": "sell",
                    "stock_code": stock_code,
                    "stock_name": stock_name,
                    "buy_price": avg_cost,
                    "reason": f"早盘跟踪: 盈利{pnl_pct*100:.1f}%达标后回落{drop_pct*100:.2f}%",
                })
                print(f"  {stock_code} 卖出: 盈利{pnl_pct*100:.1f}% 回落{drop_pct*100:.2f}%")
                continue

        # ── 条件 2：亏损 + 分钟线加速下跌 ──
        if pnl_pct <= -loss_drop_trigger:
            speed = calc_minute_drop_speed(stock_code, current_price)
            if speed >= drop_speed_threshold:
                signals.append({
                    "action": "sell",
                    "stock_code": stock_code,
                    "stock_name": stock_name,
                    "buy_price": avg_cost,
                    "reason": f"早盘跟踪: 亏损{abs(pnl_pct)*100:.1f}% 跌速{speed*100:.2f}%",
                })
                print(f"  {stock_code} 卖出: 亏损{abs(pnl_pct)*100:.1f}% 跌速{speed*100:.2f}%")
                continue

        # ── 条件 3：触及最大亏损 ──
        if pnl_pct <= -max_loss:
            signals.append({
                "action": "sell",
                "stock_code": stock_code,
                "stock_name": stock_name,
                "buy_price": avg_cost,
                "reason": f"早盘跟踪: 触及最大亏损{abs(pnl_pct)*100:.1f}%",
            })
            print(f"  {stock_code} 卖出: 最大亏损{abs(pnl_pct)*100:.1f}%")
            continue

        # ── 条件 4：到时间强制卖出 ──
        if now.hour > exit_hour or (now.hour == exit_hour and now.minute >= exit_minute):
            signals.append({
                "action": "sell",
                "stock_code": stock_code,
                "stock_name": stock_name,
                "buy_price": avg_cost,
                "reason": f"早盘跟踪: 10:00 强制卖出 (盈亏{pnl_pct*100:.1f}%)",
            })
            print(f"  {stock_code} 强制卖出: 10:00 时间到 盈亏{pnl_pct*100:.1f}%")
            continue

        # 未触发
        print(f"  {stock_code} 持有: 盈亏{pnl_pct*100:.1f}% 最高{session_high:.2f} 当前{current_price:.2f}")

    print(f"[短线卖出] 共 {len(stocks)} 只持仓, 卖出 {len(signals)} 只")
    return signals
