# StockWinner Agent API 规范

> 版本: v7.8.20 | 日期: 2026-06-01

本文档定义 Agent 与 StockWinner 系统交互的完整规范，包括变量命名、数据类型、API 端点、因子字段、回测系统等。

---

## 一、核心变量命名与类型规范

### 1.1 日期字段

**关键约定**：系统中所有日期字段统一命名为 `trade_date`，格式为字符串 `'YYYY-MM-DD'`。

| 字段名 | 格式 | 类型 | 示例 | 说明 |
|--------|------|------|------|------|
| `trade_date` | `'YYYY-MM-DD'` | str | `"2026-06-01"` | **标准日期字段名**，用于 K线、因子等所有数据 |
| `start_date` | `'YYYY-MM-DD'` 或 YYYYMMDD | str/int | `"2026-01-01"` 或 20260101 | 回测/查询开始日期 |
| `end_date` | `'YYYY-MM-DD'` 或 YYYYMMDD | str/int | `"2026-06-01"` 或 20260601 | 回测/查询结束日期 |
| `report_date` | YYYYMMDD | str | `"20251231"` | 财报报告日期（SDK原始格式） |
| `list_date` | YYYYMMDD | int | 20200101 | 上市日期（整数格式） |

**重要**：
- ❌ **禁止使用 `date` 作为日期字段名**（会导致 KeyError）
- ✅ **必须使用 `trade_date`** 作为交易日期字段名
- K线 DataFrame 列名是 `trade_date`，不是 `date`

### 1.2 股票代码

| 字段名 | 格式 | 类型 | 示例 | 说明 |
|--------|------|------|------|------|
| `stock_code` | `xxxxxx.XX` | str | `"600000.SH"` | **标准股票代码格式** |
| `stock_name` | 中文/英文 | str | `"浦发银行"` | 股票名称 |

代码规则：
- 沪市主板：`6xxxxx.SH`（如 600000.SH）
- 沪市科创板：`68xxxx.SH`（如 689009.SH）
- 深市主板：`0xxxxx.SZ`（如 000001.SZ）
- 深市创业板：`3xxxxx.SZ`（如 300750.SZ）
- 北交所：`4xxxxx.BJ` 或 `8xxxxx.BJ`
- ETF深市：`159xxx.SZ`
- ETF沪市：`51xxxx.SH`、`56xxxx.SH`、`58xxxx.SH`
- 行业指数：`801xxx.SI`

---

## 二、K线数据结构

### 2.1 K线 DataFrame 列名

```python
# K线数据标准列名（务必使用 trade_date 而非 date）
kline_columns = ['trade_date', 'open', 'high', 'low', 'close', 'volume', 'amount']

# 示例数据
kline_data = [
    {'trade_date': '2026-06-01', 'open': 10.5, 'high': 11.0, 'low': 10.3, 'close': 10.8, 'volume': 1234567, 'amount': 12345678},
    {'trade_date': '2026-05-30', 'open': 10.2, 'high': 10.6, 'low': 10.0, 'close': 10.5, 'volume': 987654, 'amount': 9876543},
]
```

**错误示例**（导致 KeyError）：
```python
# ❌ 错误：使用 date 字段名
dates = klines_df['date'].tolist()  # KeyError: 'date'

# ✅ 正确：使用 trade_date 字段名
dates = klines_df['trade_date'].tolist()
```

### 2.2 K线查询方法

| 方法 | 签名 | 说明 |
|------|------|------|
| `get_kline_smart(codes, lookback)` | `(list, int) -> dict` | 智能获取 K 线（盘中实时/盘后本地） |
| `get_batch_kline(codes, limit)` | `(list, int) -> dict` | 批量获取 K 线 |
| `get_kline_local(code, limit, start_date)` | `(str, int, str) -> list` | 本地 K 线（同步） |
| `query_kline_db(sql, params)` | `(str, tuple) -> list[dict]` | 直接查询 kline.db |

**推荐用法**：
```python
# 获取最近 100 条 K 线（按日期降序）
rows = query_kline_db(
    "SELECT trade_date, close, volume FROM kline_data WHERE stock_code = ? ORDER BY trade_date DESC LIMIT 100",
    (stock_code,)
)
# 注意：rows 是降序，如需从旧到新，请反转
closes = [r["close"] for r in reversed(rows)]
```

---

## 三、因子字段完整列表

### 3.1 stock_daily_factors 表字段

| 字段名 | 类型 | 含义 | 说明 |
|--------|------|------|------|
| `stock_code` | TEXT | 股票代码 | |
| `stock_name` | TEXT | 股票名称 | |
| `trade_date` | NUM | 交易日期 | 格式 `'YYYY-MM-DD'` |
| `circ_market_cap` | REAL | 流通市值（亿元） | ETF/指数为 NULL |
| `total_market_cap` | REAL | 总市值（亿元） | ETF/指数为 NULL |
| `days_since_ipo` | INT | 上市天数 | ETF/指数为 NULL |
| **价格表现因子** | | | |
| `change_5d` | REAL | 5日涨跌幅 (%) | |
| `change_10d` | REAL | 10日涨跌幅 (%) | |
| `change_20d` | REAL | 20日涨跌幅 (%) | |
| `bias_5` | REAL | 5日乖离率 (%) | 股价与 MA5 偏离度 |
| `bias_10` | REAL | 10日乖离率 (%) | 股价与 MA10 偏离度 |
| `bias_20` | REAL | 20日乖离率 (%) | 价与 MA20 偏离度 |
| `amplitude_5` | REAL | 5日振幅 (%) | |
| `amplitude_10` | REAL | 10日振幅 (%) | |
| `amplitude_20` | REAL | 20日振幅 (%) | |
| `change_std_5` | REAL | 5日涨跌幅标准差 | |
| `change_std_10` | REAL | 10日涨跌幅标准差 | |
| `change_std_20` | REAL | 20日涨跌幅标准差 | |
| `amount_std_5` | REAL | 5日成交额标准差 | |
| `amount_std_10` | REAL | 10日成交额标准差 | |
| `amount_std_20` | REAL | 20日成交额标准差 | |
| **均线因子** | | | |
| `ma5` | REAL | 5日均线 | |
| `ma10` | REAL | 10日均线 | |
| `ma20` | REAL | 20日均线 | |
| `ma60` | REAL | 60日均线 | |
| `ma120` | REAL | 120日均线 | **已包含**，可直接获取 |
| `ma250` | REAL | 250日均线 | **已包含**，可直接获取 |
| `ema12` | REAL | 12日指数均线 | |
| `ema26` | REAL | 26日指数均线 | |
| **技术指标因子** | | | |
| `kdj_k` | REAL | KDJ-K 值 | |
| `kdj_d` | REAL | KDJ-D 值 | |
| `kdj_j` | REAL | KDJ-J 值 | |
| `dif` | REAL | MACD-DIF | |
| `dea` | REAL | MACD-DEA | |
| `macd` | REAL | MACD 柱 | `2 * (dif - dea)` |
| `rsi_14` | REAL | 14日 RSI | |
| `cci_20` | REAL | 20日 CCI | |
| `adx` | REAL | ADX 趋向指标 | |
| `atr_14` | REAL | 14日 ATR | |
| `boll_upper` | REAL | 布林带上轨 | |
| `boll_middle` | REAL | 布林带中轨 | |
| `boll_lower` | REAL | 布林带下轨 | |
| `hv_20` | REAL | 20日历史波动率 | |
| `obv` | REAL | OBV 累积量 | |
| `volume_ratio` | REAL | 量比 | |
| `momentum_10d` | REAL | 10日动量 | |
| `momentum_20d` | REAL | 20日动量 | |
| **信号因子** | | | |
| `golden_cross` | INT | 金叉信号 | 0/1，MA5 上穿 MA10 |
| `death_cross` | INT | 死叉信号 | 0/1，MA5 下穿 MA10 |
| `limit_up_count_10d` | INT | 10日内涨停次数 | |
| `limit_up_count_20d` | INT | 20日内涨停次数 | |
| `limit_up_count_30d` | INT | 30日内涨停次数 | |
| `consecutive_limit_up` | INT | 连续涨停天数 | |
| `large_gain_5d_count` | INT | 5日内大涨天数 | 涨幅>5% |
| `large_loss_5d_count` | INT | 5日内大跌天数 | 跌幅>5% |
| `close_to_high_250d` | REAL | 距250日高点比例 | 0-100 |
| `close_to_low_250d` | REAL | 距250日低点比例 | 0-100 |
| `gap_up_ratio` | REAL | 向上跳空比例 | |
| `next_period_change` | REAL | 下期收益率 | 用于回测评估 |
| `is_traded` | INT | 是否有成交 | 0/1 |

### 3.2 因子获取方法

```python
# 获取单只股票因子
factors = get_factors(stock_code, trade_date)  # 返回 dict

# 批量获取因子
factors_dict = get_factors_batch(stock_codes, trade_date)  # 返回 {stock_code: factors_dict}

# 因子字段访问
rsi = factors.get('rsi_14')  # 使用 .get() 避免 KeyError
ma5 = factors.get('ma5')
macd = factors.get('macd')   # 注意：字段名是 'macd'，不是 'macd_hist'
```

---

## 四、策略沙盒上下文 (context)

### 4.1 context 字典内容

| 字段名 | 类型 | 说明 |
|--------|------|------|
| `stocks` | list[dict] | 候选股票列表，`[{stock_code, stock_name}]` |
| `today` | str | 今日日期，`'YYYY-MM-DD'` |
| `account_id` | str | 账户 ID |
| `get_factors` | function | 获取单只股票因子 |
| `get_factors_batch` | function | 批量获取因子 |
| `get_kline_smart` | function | 智能获取 K 线 |
| `get_batch_kline` | function | 批量获取 K 线 |
| `get_kline_local` | function | 本地 K 线 |
| `query_kline_db` | function | 直接查询 kline.db |
| `query_db` | function | 直接查询 stockwinner.db |

### 4.2 策略返回格式

```python
def run(context):
    # ... 计算逻辑
    return [
        {
            "action": "buy",           # 必填: "buy" / "sell" / "watch"
            "stock_code": "600000.SH", # 必填: 股票代码
            "stock_name": "浦发银行",   # 可选: 股票名称
            "reason": "MACD金叉",      # 可选: 买入理由
            "buy_price": 12.34,        # 可选: 买入价格
            "target_quantity": 100,    # 可选: 目标数量
            "stop_loss_pct": 0.05,     # 可选: 止损比例，默认 5%
            "take_profit_pct": 0.15,   # 可选: 止盈比例，默认 15%
        }
    ]
```

---

## 五、回测系统

### 5.1 回测 API 端点

```
POST /api/v1/ui/{account_id}/backtest/runs
```

### 5.2 回测参数

| 参数名 | 类型 | 必填 | 说明 |
|--------|------|------|------|
| `name` | str | 是 | 回测名称 |
| `strategy_id` | int | 是 | 策略 ID（选股策略） |
| `mode` | str | 是 | 回测模式：`simulated`（撮合模拟）或 `accumulated`（收益率累积） |
| `start_date` | str | 是 | 开始日期，格式 `'YYYY-MM-DD'` |
| `end_date` | str | 是 | 结束日期，格式 `'YYYY-MM-DD'` |
| `initial_capital` | float | 否 | 初始资金，默认 100万 |
| `stock_pool` | list[str] | 否 | 股票池代码列表 |
| `markets` | list[str] | 否 | 市场过滤：`['SH', 'SZ']` |
| `slippage_pct` | float | 否 | 滑点比例，默认 0 |
| `stop_loss_pct` | float | 否 | 止损比例 |
| `take_profit_pct` | float | 否 | 止盈比例 |
| `trailing_stop_pct` | float | 否 | 移动止损比例 |

### 5.3 回测请求示例

```json
{
  "name": "MTV4策略回测",
  "strategy_id": 76,
  "mode": "simulated",
  "start_date": "2025-01-01",
  "end_date": "2026-05-31",
  "initial_capital": 1000000,
  "markets": ["SH", "SZ"]
}
```

### 5.4 回测结果字段

| 字段名 | 类型 | 说明 |
|--------|------|------|
| `run_id` | int | 回测运行 ID |
| `status` | str | 状态：`pending`/`running`/`completed`/`failed` |
| `total_return` | float | 总收益率 (%) |
| `annual_return` | float | 年化收益率 (%) |
| `max_drawdown` | float | 最大回撤 (%) |
| `sharpe_ratio` | float | 夏普比率 |
| `win_rate` | float | 胜率 (%) |
| `trade_count` | int | 交易次数 |

---

## 六、Agent API 端点

### 6.1 认证

所有 Agent API 需携带 `X-Agent-Key` 请求头：
```bash
curl -H "X-Agent-Key: sk-agent-xxx" http://localhost:8080/api/v1/agent/...
```

### 6.2 核心 API 端点

| 端点 | 方法 | 说明 |
|------|------|------|
| `/api/v1/agent/me` | GET | 获取当前 Agent 信息 |
| `/api/v1/agent/watchlist` | GET | 获取 Watchlist |
| `/api/v1/agent/watchlist/add` | POST | 添加股票到 Watchlist |
| `/api/v1/agent/watchlist/remove` | POST | 移除股票 |
| `/api/v1/agent/positions` | GET | 获取持仓 |
| `/api/v1/agent/signals` | GET | 获取交易信号 |
| `/api/v1/agent/strategies` | GET | 获取策略列表 |
| `/api/v1/agent/strategies/{id}/run` | POST | 执行策略 |
| `/api/v1/agent/backtest/runs` | POST | 创建回测任务 |
| `/api/v1/agent/backtest/runs/{id}` | GET | 获取回测结果 |
| `/api/v1/agent/query/data/*` | GET | 数据查询（见下方） |

### 6.3 数据查询 API

| 端点 | 必填参数 | 说明 |
|------|----------|------|
| `/query/data/index/list` | 无 | 指数列表 |
| `/query/data/index/kline` | `index_code` | 指数 K 线 |
| `/query/data/industry/list` | 无（可选 `level`） | 行业分类 |
| `/query/data/industry/kline` | `index_code` | 行业指数行情 |
| `/query/data/dragon-tiger` | `stock_code`, `start_date`, `end_date` | 龙虎榜 |
| `/query/data/margin/summary` | `start_date`, `end_date` | 融资融券汇总 |
| `/query/data/margin/detail` | `stock_code`, `start_date`, `end_date` | 融资融券明细 |
| `/query/data/block-trading` | `stock_code`, `start_date`, `end_date` | 大宗交易 |
| `/query/data/etf/pcf` | `etf_codes` | ETF 申赎数据 |
| `/query/data/etf/share` | `etf_codes` | ETF 份额 |
| `/query/data/etf/iopv` | `etf_codes` | ETF IOPV |

**日期参数格式**：
- 龙虎榜/融资融券/大宗交易：`start_date`/`end_date` 使用整数 YYYYMMDD（如 20260101）
- 其他 API：字符串 `'YYYY-MM-DD'` 或整数均可

---

## 七、常见错误与修复

### 7.1 日期字段名错误

```python
# ❌ 错误：使用 date 字段名
dates = kline_df['date'].tolist()  # KeyError: 'date'

# ✅ 正确：使用 trade_date
dates = kline_df['trade_date'].tolist()
```

### 7.2 MACD 字段名错误

```python
# ❌ 错误
macd_hist = factors.get('macd_hist')  # 返回 None

# ✅ 正确
macd_val = factors.get('macd')        # 正确
dif = factors.get('dif')
dea = factors.get('dea')
```

### 7.3 K线顺序错误

```python
# ❌ 错误：query_kline_db 返回降序，直接用于计算 MA
rows = query_kline_db("SELECT close FROM kline_data ORDER BY trade_date DESC LIMIT 100", (code,))
ma5 = calculate_ma([r['close'] for r in rows], 5)  # 数据顺序错误

# ✅ 正确：反转数据从旧到新
rows = query_kline_db("SELECT close FROM kline_data ORDER BY trade_date DESC LIMIT 100", (code,))
closes = [r['close'] for r in reversed(rows)]
ma5 = calculate_ma(closes, 5)
```

### 7.4 因子字段访问

```python
# ✅ 推荐：直接从因子表获取 MA120/MA250（已包含）
ma120 = factors.get('ma120')
ma250 = factors.get('ma250')

# 注意：因子表包含 ma5/ma10/ma20/ma60/ma120/ma250
# 如果某只股票历史数据不足，该因子值可能为 None

# ✅ 安全访问方式
if factors.get('ma120') and factors.get('ma250'):
    ma120 = factors['ma120']
    ma250 = factors['ma250']
else:
    # 数据不足时的备用方案：从 K 线计算
    rows = query_kline_db("SELECT close FROM kline_data WHERE stock_code = ? ORDER BY trade_date DESC LIMIT 260", (code,))
    closes = [r['close'] for r in reversed(rows)]
    if len(closes) >= 120:
        ma120 = calculate_ma(closes, 120)
```

---

## 八、数据库表概览

| 数据库 | 表名 | 主要字段 | 说明 |
|--------|------|----------|------|
| kline.db | `kline_data` | `stock_code`, `trade_date`, OHLCV | K 线数据 |
| kline.db | `stock_daily_factors` | 62 个因子字段 | 日频因子 |
| kline.db | `weekly_kline_data` | `stock_code`, `week_start_date` | 周K线 |
| stockwinner.db | `strategies` | `id`, `name`, `code`, `code_type` | 策略定义 |
| stockwinner.db | `trading_signals` | `stock_code`, `action`, `reason` | 交易信号 |
| stockwinner.db | `stock_positions` | `stock_code`, `quantity`, `avg_cost` | 持仓 |
| stockwinner.db | `backtest_runs` | `run_id`, `status`, `start_date` | 回测任务 |
| stockwinner.db | `watchlist` | `stock_code`, `added_at` | 监控列表 |

---

## 九、沙盒限制

### 允许的模块
`pandas`, `numpy`, `datetime`, `statistics`, `json`, `math`, `re`, `collections`, `itertools`, `functools`, `dataclasses`, `typing`, `time`, `calendar`, `decimal`, `copy`, `string`

### 禁止的模块
`os`, `sys`, `subprocess`, `socket`, `http`, `requests`, `urllib`, `sqlite3`, `torch`, `safetensors`

### 禁止的函数
`eval`, `exec`, `compile`, `open`, `input`, `breakpoint`, `getattr`, `setattr`, `delattr`, `globals`, `locals`, `__import__`

---

## 十、版本历史

| 版本 | 日期 | 变更 |
|------|------|------|
| v7.8.20 | 2026-06-01 | ETF 数据支持 + 因子计算修复 |
| v7.8.19 | 2026-05-31 | 复权因子管理优化 |
| v7.8.18 | 2026-05-28 | SDK 优先级队列实现 |

---

**维护者**: Claude Opus 4.7
**最后更新**: 2026-06-01