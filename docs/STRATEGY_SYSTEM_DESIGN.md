# 策略系统设计文档

## 概述

策略系统支持三种独立的策略类型：持仓策略、买卖策略、选股策略。三种策略各司其职，通过关联实现完整的投资决策流程。

## 重要设计决策记录

### 1. 选股策略结构命名统一 (2026-04-24)

**决策**：将 `conditions.buy/sell` 改为 `buy_conditions`

**原因**：
- sell卖出条件语义上属于交易策略，不属于选股策略
- 选股策略只负责筛选股票和识别买入信号
- 卖出条件（止损、止盈）在交易策略中管理

**最终结构**：
```json
{
  "stock_filters": {"total_market_cap_max": 50, "roe_min": 15},
  "buy_conditions": ["DIF_CROSS_UP_DEA", "VOLUME_RATIO > 1.5"]
}
```

**兼容性**：screening service 支持三种格式向后兼容：
- `buy_conditions`（新格式，推荐）
- `buy`（旧格式1）
- `conditions.buy`（旧格式2）

### 2. 因子管理方式 (2026-04-24)

**决策**：从硬编码 FACTOR_MAPPING 改为数据库驱动

**原因**：
- 硬编码难以维护和扩展
- 新增因子需要修改代码并重新部署
- 数据库驱动支持动态配置和元数据管理

**实现**：
- 新建 `factor_metadata` 表存储因子配置（68条记录）
- `FactorRegistry` 类从数据库动态加载因子
- `is_filterable` 字段区分静态筛选因子和动态信号因子

### 3. stock_filters vs buy_conditions 执行顺序

**决策**：明确区分两类条件的执行顺序和作用

| 类型 | 作用 | 执行时机 | 数据来源 |
|------|------|----------|----------|
| stock_filters | 静态筛选（基本面） | 第一步，先执行 | monthly_factors表 |
| buy_conditions | 动态信号（技术指标） | 第二步，后执行 | daily_factors表 + 计算 |

**执行流程**：
```
全市场4000股 → stock_filters筛选(200股) → buy_conditions计算(10股) → 最终结果
```

**is_filterable 分类**：
- `is_filterable=1`：静态筛选因子（市值、PE、ROE、行业等）
- `is_filterable=0`：动态信号因子（MACD金叉、量比等）

## 策略类型定义

### 1. 持仓策略 (Position Strategy)

**作用**：管理账户整体仓位，控制风险敞口。

**特性**：
- 独立存储，支持多条策略记录
- 根据市场条件动态切换激活策略
- 记录调整历史，便于复盘分析

**配置参数**：
```json
{
  "type": "position",
  "name": "激进持仓策略",
  "config": {
    "max_total_position_pct": 0.80,      // 总仓位上限（股票占总资产比例）
    "max_single_position_pct": 0.15,     // 单只股票最大仓位比例
    "cash_reserve_pct": 0.20             // 现金保留比例
  },
  "activate_conditions": [
    "INDEX_000001_SH_MACD_CROSS_UP_DEA"  // 上证指数MACD金叉时激活
  ],
  "deactivate_conditions": [
    "INDEX_000001_SH_MACD_CROSS_DOWN_DEA" // 上证指数MACD死叉时停用
  ],
  "status": "active"                     // 当前激活状态
}
```

**动态切换机制**：
- 系统定时监控指定指数的技术指标
- 当触发激活条件时，自动切换持仓策略
- 当触发停用条件时，切换到备用策略（或发出预警）
- 仓位超出当前策略限制时，向用户发出预警提示

### 2. 买卖策略 (Trading Strategy)

**作用**：定义单只股票的交易规则，作为模板供选股策略引用。

**特性**：
- 模板化存储，可被多个选股策略共用
- 选出的股票自动获得买卖建议值
- 用户可在watchlist确认时修改建议值

**配置参数**：
```json
{
  "type": "trading",
  "name": "短线交易模板",
  "config": {
    "entry_price_method": "current_price",  // 建仓价计算方式
    "entry_price_adjust_pct": -0.02,        // 建仓价调整比例（当前价-2%）
    "stop_loss_pct": 0.05,                  // 止损比例（成本价-5%）
    "stop_loss_method": "cost_pct",         // 止损计算方式：成本价百分比
    "take_profit_pct": 0.15,                // 止盈比例（成本价+15%）
    "take_profit_method": "cost_pct"        // 止盈计算方式：成本价百分比
  }
}
```

**建仓价计算方式选项**：
- `current_price`: 当前价
- `current_price_adjust`: 当前价±调整比例
- `break_ma5`: 突破MA5时买入
- `limit_price`: 限价买入

**止损/止盈计算方式选项**：
- `cost_pct`: 成本价±百分比
- `ma_line`: 跌破/突破均线
- `atr_based`: 基于ATR动态计算

### 3. 选股策略 (Screening Strategy)

**作用**：筛选有投资潜力的股票，选出的股票放入watchlist。

**特性**：
- 结合静态筛选(stock_filters)和动态信号(buy_conditions)
- 关联买卖策略模板，自动生成买卖建议
- 支持LLM自然语言生成

**配置参数**（注意命名统一）：
```json
{
  "type": "screening",
  "name": "MACD金叉小市值选股",
  "config": {
    "stock_filters": {
      "total_market_cap_max": 50,          // 总市值上限（亿元）
      "circ_market_cap_max": 30,           // 流通市值上限（亿元）
      "pe_ttm_max": 30,                    // PE上限
      "roe_min": 15,                       // ROE下限（%）
      "gross_margin_min": 20,              // 毛利率下限（%）
      "sw_level1": "电子"                  // 申万一级行业
    },
    "buy_conditions": [
      "DIF_CROSS_UP_DEA",                   // MACD金叉（专用穿越信号）
      "RSI_14 < 60",                        // RSI未超买
      "VOLUME_RATIO > 1.5"                  // 成交量放大
    ],
    "trading_strategy_id": 123             // 关联的买卖策略模板ID
  }
}
```

**注意**：
- ❌ 已废弃：`conditions: {buy: [...], sell: [...]}`（sell条件移入交易策略）
- ✅ 新格式：`stock_filters` + `buy_conditions`
- screening service 向后兼容三种格式

## 数据流向

```
┌─────────────────┐
│   持仓策略       │ ← 监控指数指标，动态切换激活策略
│  (Position)     │
└────────┬────────┘
         │ 仓位限制
         ↓
┌─────────────────┐     ┌─────────────────┐
│   选股策略       │ ←── │   买卖策略       │
│  (Screening)    │     │  (Trading)      │ (模板)
└────────┬────────┘     └─────────────────┘
         │ 筛选结果 + 买卖建议
         ↓
┌─────────────────┐
│   Watchlist     │ ← 用户确认，填入建议值
└────────┬────────┘
         │ 监控信号
         ↓
┌─────────────────┐
│   交易执行       │ ← 检查持仓策略仓位限制
│  (Trading)      │
└─────────────────┘
```

**流程说明**：
1. **持仓策略** 监控市场指数，根据激活条件动态调整仓位上限
2. **选股策略** 筛选股票，引用买卖策略模板，生成买卖建议值
3. **Watchlist** 用户确认股票，自动填入建议的建仓价、止损、止盈
4. **交易执行** 买入前检查持仓策略的仓位限制，超出则预警

## 数据库设计（最终方案）

### accounts 表扩展

持仓策略参数存储在账户表，一个账户一套持仓策略：

```sql
-- 新增字段
ALTER TABLE accounts ADD COLUMN max_total_position_pct REAL DEFAULT 0.80;    -- 总仓位上限
ALTER TABLE accounts ADD COLUMN max_single_position_pct REAL DEFAULT 0.15;   -- 单股仓位上限
ALTER TABLE accounts ADD COLUMN cash_reserve_pct REAL DEFAULT 0.20;          -- 现金保留比例
```

### trading_strategies 表（新建）

每只股票的交易策略，账户+股票代码唯一索引：

```sql
CREATE TABLE trading_strategies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id TEXT NOT NULL,
    stock_code TEXT NOT NULL,
    entry_price REAL,                   -- 建仓价/建议买入价（固定价格）
    stop_loss_price REAL,               -- 止损价（固定价格，可选）
    take_profit_price REAL,             -- 止盈价（固定价格，可选）
    stop_loss_pct REAL,                 -- 止损比例（成本价的百分比，如 0.05）
    take_profit_pct REAL,               -- 止盈比例（成本价的百分比，如 0.15）
    max_trade_quantity INTEGER,         -- 单次买卖最大数量（股数）
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(account_id, stock_code)      -- 一个账户一只股票一条交易策略
);
```

**执行逻辑**：
- 优先使用固定价格（stop_loss_price/take_profit_price）
- 如果固定价格为空，用比例计算：`entry_price * (1 - stop_loss_pct)` 或 `entry_price * (1 + take_profit_pct)`
- 系统日志记录每次修改

### position_adjust_rules 表（新建）

持仓策略动态调整规则：

```sql
CREATE TABLE position_adjust_rules (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id TEXT NOT NULL,
    trigger_condition TEXT NOT NULL,    -- 触发条件表达式
    target_max_total_pct REAL,          -- 目标总仓位比例
    target_max_single_pct REAL,         -- 目标单股仓位比例（可选）
    description TEXT,                   -- 规则描述
    priority INTEGER DEFAULT 0,         -- 规则优先级
    is_active INTEGER DEFAULT 1,        -- 是否启用
    UNIQUE(account_id, trigger_condition)
);
```

**触发条件示例**：
- `INDEX_000001_SH_MACD_CROSS_UP_DEA` - 上证指数MACD金叉
- `INDEX_000001_SH_MACD_CROSS_DOWN_DEA` - 上证指数MACD死叉

### strategies 表

选股策略和买卖策略模板：

```sql
-- strategy_type: 'screening'（选股策略） | 'trading'（买卖策略模板）
-- 历史数据已迁移：manual/llm → screening
```

### watchlist 表扩展

```sql
ALTER TABLE watchlist ADD COLUMN trading_strategy_id INTEGER REFERENCES trading_strategies(id);
```

关联交易策略，候选股可引用已设置的交易策略。

### factor_metadata 表（新建）

因子元数据管理，替代硬编码FACTOR_MAPPING：

```sql
CREATE TABLE factor_metadata (
    factor_id TEXT PRIMARY KEY,          -- 因子ID（如 MA5, DIF, PE_TTM）
    factor_name TEXT,                    -- 因子名称
    category TEXT,                       -- 分类：technical/valuation/profitability/growth
    data_table TEXT,                     -- 数据表：stock_daily_factors / stock_monthly_factors
    data_column TEXT,                    -- 数据列名
    update_freq TEXT DEFAULT 'daily',    -- 更新频率：daily / monthly
    is_filterable INTEGER DEFAULT 0,     -- 是否可用于静态筛选：1=可筛选, 0=仅动态信号
    unit TEXT,                           -- 单位：亿元 / % / 倍
    description TEXT,                    -- 因子描述
    is_enabled INTEGER DEFAULT 1         -- 是否启用
);
```

**is_filterable 分类说明**：
- `is_filterable=1`（静态筛选因子）：市值、PE、ROE、行业等基本面属性
  - 在 `stock_filters` 中使用
  - 第一步筛选，缩小股票范围
- `is_filterable=0`（动态信号因子）：MACD金叉、量比、RSI等技术指标
  - 在 `buy_conditions` 中使用
  - 第二步计算，生成买入信号

**FactorRegistry 改为数据库驱动**：
```python
# 从 factor_metadata 表动态加载因子配置
def _load_from_db(self):
    cursor.execute("""
        SELECT factor_id, factor_name, category, data_table, data_column,
               update_freq, is_filterable, unit, description
        FROM factor_metadata WHERE is_enabled = 1
    """)
```

## LLM策略生成接口

### 前端交互

用户选择策略类型后输入描述：
```
策略类型：[持仓策略] [买卖策略] [选股策略]

策略描述：根据市场氛围动态调整仓位，当上证指数MACD金叉时提高到80%，死叉时降到40%
```

### API接口

```
POST /api/v1/ui/{account_id}/strategies/generate
{
  "strategy_type": "position",    // 显式指定类型
  "description": "...",
  "risk_level": "medium"
}
```

### LLM返回格式（按类型）

**持仓策略**：
```json
{
  "risk_level": "medium",
  "config": {
    "max_total_position_pct": 0.80,
    "max_single_position_pct": 0.15,
    "cash_reserve_pct": 0.20
  },
  "activate_conditions": ["INDEX_000001_SH_MACD_CROSS_UP_DEA"],
  "deactivate_conditions": ["INDEX_000001_SH_MACD_CROSS_DOWN_DEA"]
}
```

**买卖策略**：
```json
{
  "risk_level": "medium",
  "config": {
    "entry_price_method": "current_price",
    "entry_price_adjust_pct": -0.02,
    "stop_loss_pct": 0.05,
    "stop_loss_method": "cost_pct",
    "take_profit_pct": 0.15,
    "take_profit_method": "cost_pct"
  }
}
```

**选股策略**（使用新的 buy_conditions 格式）：
```json
{
  "risk_level": "medium",
  "config": {
    "stock_filters": {
      "total_market_cap_max": 50,
      "roe_min": 15,
      "pe_ttm_max": 30
    },
    "buy_conditions": ["DIF_CROSS_UP_DEA", "VOLUME_RATIO > 1.5"],
    "trading_strategy_id": null   // 用户后续选择关联
  }
}
```

**LLM SYSTEM_PROMPT 关键规则**：
```
【重要】条件解析规则：

一、市值/估值/盈利条件 → 放入 stock_filters：
- "总市值小于X亿" → total_market_cap_max: X
- "ROE大于X%" → roe_min: X
- "PE小于X倍" → pe_ttm_max: X
- "电子行业" → sw_level1: "电子"

二、技术信号条件 → 放入 buy_conditions：
- "MACD金叉" → "DIF_CROSS_UP_DEA"
- "成交量放大" → "VOLUME_RATIO > 2"
- "价格站上5日均线" → "PRICE > MA5"

【核心原则】：
1. 基本面条件放入 stock_filters
2. 技术信号放入 buy_conditions
3. 不要遗漏任何用户条件
```

## 实现要点

### 1. 持仓策略动态切换

- 新增定时任务，监控指数技术指标
- 当指数触发激活/停用条件时，切换持仓策略
- 发送通知提醒用户仓位策略已调整
- 交易执行前检查当前激活策略的仓位限制

### 2. 买卖策略模板引用

- 选股策略创建时，用户选择关联的买卖模板
- 选股结果写入watchlist时，自动计算建议买卖价格
- 用户可在确认时修改建议值

### 3. LLM生成优化

- SYSTEM_PROMPT 按策略类型区分
- 用户显式选择类型，LLM返回对应格式
- 前端展示时区分三种策略的配置项

### 4. 条件表达式标准化

支持的指数条件：
- `INDEX_000001_SH_MACD_CROSS_UP_DEA` - 上证指数MACD金叉
- `INDEX_000001_SH_MACD_CROSS_DOWN_DEA` - 上证指数MACD死叉
- `INDEX_399001_SZ_MA5_CROSS_UP_MA10` - 深证成指MA5金叉MA10

支持的股票条件（见 `services/common/indicators.py`）：
- 穿越信号：`DIF_CROSS_UP_DEA`, `MA5_CROSS_UP_MA10`, `K_CROSS_UP_D`
- 比较条件：`RSI_14 < 30`, `VOLUME_RATIO > 2`, `PRICE > MA5`

## 开发进度

### 已完成 ✅

| 任务 | 完成时间 | 说明 |
|------|----------|------|
| factor_metadata 表创建 | 2026-04-24 | 68条因子记录，替代硬编码 |
| trading_strategies 表创建 | 2026-04-24 | 每股交易策略表 |
| position_adjust_rules 表创建 | 2026-04-24 | 仓位动态调整规则表 |
| accounts 表扩展 | 2026-04-24 | 添加仓位参数字段 |
| buy_conditions 命名统一 | 2026-04-24 | 移除 sell，统一为 buy_conditions |
| screening service 向后兼容 | 2026-04-24 | 支持三种格式 |
| trading_strategies API | 2026-04-24 | CRUD 接口完成 |
| position_rules API | 2026-04-24 | 规则管理接口完成 |

### 待开发 ❌

| 任务 | 优先级 | 说明 |
|------|--------|------|
| 前端三策略类型界面 | P1 | 持仓策略/选股策略/交易策略三个Tab |
| FactorRegistry 改数据库驱动 | P1 | 从 factor_metadata 加载，替代硬编码 |
| LLM SYSTEM_PROMPT 更新 | P1 | 使用 buy_conditions 格式 |
| 持仓策略动态切换 | P2 | 指数监控任务、条件触发 |
| 买卖策略模板引用 | P2 | 选股策略关联模板、自动填充建议值 |
| strategies.ui/strategies.py 完善 | P2 | validated_conditions 改为 buy_conditions |
| 前端策略详情显示 | P3 | buy_conditions 显示优化 |