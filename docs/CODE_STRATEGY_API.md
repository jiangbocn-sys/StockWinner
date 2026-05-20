# 代码型策略 (Python Strategy) 开发指南

## 概述

代码型策略允许用户用 Python 代码编写选股逻辑，通过沙盒安全执行。策略代码在受限环境中运行，系统注入数据获取函数和技术指标计算函数。

## API 端点

### 创建代码型策略

```
POST /api/v1/ui/{account_id}/strategies
```

Body:
```json
{
  "name": "金叉多头",
  "description": "MACD多头 + MA5金叉MA10 + 均线多头排列",
  "strategy_type": "python",
  "code_scope": "screening",
  "function_name": "run",
  "code": "def run(context):\n    ..."
}
```

### 更新代码型策略

```
PUT /api/v1/ui/{account_id}/code-strategies/{strategy_id}
```

### 查看代码型策略列表

```
GET /api/v1/ui/{account_id}/code-strategies?code_scope=screening
```

`code_scope` 可选值：`screening`（选股策略）、`trading`（交易/卖出策略）

### 验证策略代码

```
POST /api/v1/ui/{account_id}/strategies/validate-code
```

Body: `{ "code": "def run(context): ..." }`

### 试运行策略

```
POST /api/v1/ui/{account_id}/strategies/test-run
```

Body:
```json
{
  "code": "def run(context): ...",
  "function_name": "run",
  "test_stocks": ["600000.SH", "000001.SZ"]
}
```

## 沙盒上下文 (context)

策略入口函数签名固定为 `def run(context):`，返回信号列表。

### context 中的字段

| 字段 | 类型 | 说明 |
|------|------|------|
| `stocks` | list[dict] | 待筛选股票列表，每项含 `stock_code`, `stock_name` |
| `today` | str | 今日日期，格式 `YYYY-MM-DD` |
| `account_id` | str | 账户 ID |
| `indicators` | dict | 技术指标计算函数字典（见下方） |

### 数据获取函数

| 函数 | 签名 | 说明 | 返回值 |
|------|------|------|--------|
| `get_factors` | `(stock_code: str, date: str = None) -> dict` | 从 stock_daily_factors 表获取指定日期的日频因子 | 单只股票的因子数据字典，不存在返回 None |
| `get_factors_batch` | `(codes: list, date: str = None) -> dict` | 批量获取多只股票的日频因子 | `{stock_code: {factors}}` |
| `get_kline_local` | `(stock_code: str, limit: int = 100, start_date: str = None) -> list` | 从本地 kline.db 获取 K 线历史数据 | `[{trade_date, open, high, low, close, volume, amount}]` |
| `get_batch_kline` | `(codes: list, limit: int = 100) -> dict` | 批量获取多只股票的 K 线数据 | `{stock_code: [kline_records]}` |
| `get_kline_spliced` | `(codes: list, lookback: int = 100) -> dict` | 获取拼接后的 K 线数据（本地+实时） | `{stock_code: [kline_records]}` |
| `get_kline_smart` | `(codes: list, lookback: int = 100) -> dict` | 智能获取 K 线（自动判断盘中/盘后） | `{stock_code: [kline_records]}` |
| `get_realtime_quote` | `(stock_code: str) -> MarketData` | 获取实时行情（异步函数，需 await） | MarketData 对象 |
| `query_kline_db` | `(sql: str, params: tuple = None) -> list[dict]` | 直接查询 kline.db（只读，**推荐用于取最近 K 线**） | SQL 结果列表 |
| `query_db` | `(sql: str, params: tuple = None) -> list[dict]` | 直接查询 stockwinner.db（只读） | SQL 结果列表 |

> **注意**：`get_kline_local` 按日期升序返回最早的 N 条记录。如需获取**最近**的 K 线数据（如计算前一日均线），请使用 `query_kline_db`：
> ```python
> rows = query_kline_db("SELECT close FROM kline_data WHERE stock_code = ? ORDER BY trade_date DESC LIMIT 300", (code,))
> closes = [r["close"] for r in reversed(rows)]  # 从旧到新
> ```

### 技术指标计算函数

以下函数直接在沙盒中可用，**无需从 `indicators` 字典获取，直接调用即可**：

```python
# 示例：计算 MA120
ma120 = calculate_ma(closes, 120)
```

| 函数 | 签名 | 说明 |
|------|------|------|
| `calculate_ma` | `(prices: list, period: int) -> float` | 简单移动平均线，数据不足返回 None |
| `calculate_ema` | `(prices: list, period: int) -> float` | 指数移动平均线 |
| `calculate_rsi` | `(prices: list, period: int = 14) -> float` | 相对强弱指数 |
| `calculate_macd` | `(prices: list, fast: int = 12, slow: int = 26, signal: int = 9) -> dict` | MACD 指标，返回 `{dif, dea, macd}` |
| `calculate_kdj` | `(highs: list, lows: list, closes: list, n: int = 9, m1: int = 3, m2: int = 3) -> dict` | KDJ 指标，返回 `{k, d, j}` |
| `calculate_bollinger_bands` | `(prices: list, period: int = 20, std_dev: float = 2.0) -> dict` | 布林带，返回 `{upper, mid, lower}` |
| `calculate_atr` | `(highs: list, lows: list, closes: list, period: int = 14) -> float` | 平均真实波幅 |
| `calculate_adx` | `(highs: list, lows: list, closes: list, period: int = 14) -> float` | 平均趋向指标 |

> **重要提示**：`stock_daily_factors` 表中仅包含 ma5/ma10/ma20/ma60 等短周期均线。
> 如需 MA120、MA250 等长周期均线，请通过 `query_kline_db` 获取 K 线后使用 `calculate_ma(closes, period)` 自行计算。
> MACD 字段名为 `macd`、`dif`、`dea`（非 macd_hist/macd_dif/macd_dea）。

## 返回值格式

策略函数必须返回信号列表，每个信号为字典：

```python
return [{
    "action": "buy",           # "buy" / "sell" / "watch"
    "stock_code": "600000.SH",
    "stock_name": "浦发银行",
    "reason": "MACD金叉 | MA5>MA10",
    "buy_price": 12.34,        # 可选
    "target_quantity": 100,    # 可选
    "stop_loss_pct": 0.05,     # 可选，默认 5%
    "take_profit_pct": 0.15,   # 可选，默认 15%
}]
```

## 代码示例

### 示例 1：金叉多头策略

```python
def run(context):
    stocks = context.get("stocks", [])
    today = context.get("today", "")
    get_factors = context.get("get_factors")
    get_kline_local = context.get("get_kline_local")
    if not get_factors or not stocks:
        return []

    signals = []
    for s in stocks:
        code = s.get("stock_code", "")
        name = s.get("stock_name", code)
        factors = get_factors(code, today)
        if not factors:
            continue

        ma5 = factors.get("ma5")
        ma10 = factors.get("ma10")
        ma20 = factors.get("ma20")
        macd = factors.get("macd_hist")

        # MA120/MA250 从 K 线计算
        ma120 = factors.get("ma120")
        ma250 = factors.get("ma250")
        if ma120 is None or ma250 is None:
            kline_data = get_kline_local(code, limit=260)
            if kline_data and len(kline_data) >= 120:
                closes = [k["close"] for k in kline_data]
                if ma120 is None and len(closes) >= 120:
                    ma120 = calculate_ma(closes, 120)
                if ma250 is None and len(closes) >= 250:
                    ma250 = calculate_ma(closes, 250)

        if not all([ma5, ma10]):
            continue

        # 条件：MACD > 0 且 MA5 > MA10
        cond_macd = macd is not None and macd > 0
        cond_cross = ma5 > ma10
        if not (cond_macd and cond_cross):
            continue

        # 多头排列判断
        short_bull = all([ma20]) and ma5 > ma10 and ma10 > ma20
        long_bull = all([ma60, ma120, ma250]) and ma60 > ma120 and ma120 > ma250
        if not (short_bull or long_bull):
            continue

        phase = "短期多头" if short_bull else "长期多头"
        signals.append({
            "action": "buy",
            "stock_code": code,
            "stock_name": name,
            "reason": "MACD多头 | MA5金叉MA10 | " + phase,
        })

    return signals
```

### 示例 2：RSI 超卖反弹

```python
def run(context):
    stocks = context.get("stocks", [])
    get_factors = context.get("get_factors")
    if not get_factors or not stocks:
        return []

    signals = []
    for s in stocks:
        code = s.get("stock_code", "")
        name = s.get("stock_name", code)
        factors = get_factors(code, context.get("today"))
        if not factors:
            continue

        rsi = factors.get("rsi_14")
        if rsi is not None and rsi < 30:
            signals.append({
                "action": "buy",
                "stock_code": code,
                "stock_name": name,
                "reason": "RSI_14 = %.2f (超卖)" % rsi,
            })

    return signals
```

## 沙盒限制

### 允许使用的模块

`pandas`, `numpy`, `datetime`, `statistics`, `json`, `math`, `re`, `collections`, `itertools`, `functools`, `dataclasses`, `typing`, `time`, `calendar`, `decimal`, `copy`, `string`, `sys`, `os`

### 禁止使用的操作

- 直接调用 SDK（`AmazingData`、`get_sdk_manager`）
- 使用 `subprocess`、`requests`、`torch`、`safetensors`
- 使用 `open()`、`eval()`、`exec()`、`globals()`、`locals()`
- 访问网络或文件系统

## 注意事项

1. **MA120/MA250 等长周期均线**：`stock_daily_factors` 表不包含这些字段，需要通过 `get_kline_local(code, limit=260)` 获取 K 线后用 `calculate_ma(closes, 120)` 计算
2. **字符串格式化**：建议使用 `%` 格式化而非 f-string，避免嵌套问题
3. **性能**：全市场筛选时股票数量可达数千只，避免在循环中做耗时操作
4. **数据不足**：计算技术指标时，如果 K 线数据不足（如 MA250 需要至少 250 条），函数会返回 None
5. **异步函数**：`get_realtime_quote` 是异步函数，策略代码中直接调用会返回协程对象而非结果，同步场景请使用 `get_kline_local` 等本地数据函数
