# 策略系统设计文档

## 概述

策略系统支持三种独立的策略类型：持仓策略、买卖策略、选股策略。三种策略各司其职，通过关联实现完整的投资决策流程。

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
- 结合基本面筛选和技术指标条件
- 关联买卖策略模板，自动生成买卖建议
- 支持LLM自然语言生成

**配置参数**：
```json
{
  "type": "screening",
  "name": "MACD金叉小市值选股",
  "config": {
    "stock_filters": {
      "market": ["SH", "SZ"],              // 市场筛选
      "total_market_cap_max": 50,          // 总市值上限（亿元）
      "total_market_cap_min": null,        // 总市值下限
      "circ_market_cap_max": null,         // 流通市值上限
      "circ_market_cap_min": null,         // 流通市值下限
      "industry": ["电子", "计算机"]        // 行业筛选
    },
    "technical_conditions": [
      "DIF_CROSS_UP_DEA",                   // MACD金叉
      "RSI_14 < 60",                        // RSI未超买
      "VOLUME_RATIO > 1.5"                  // 成交量放大
    ],
    "trading_strategy_id": 123             // 关联的买卖策略模板ID
  },
  "suggested_entry_fields": {              // 选出股票时自动填入的建议值
    "entry_price": "current_price * 0.98", // 建议建仓价公式
    "stop_loss_price": "entry_price * 0.95",
    "take_profit_price": "entry_price * 1.15"
  }
}
```

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

**选股策略**：
```json
{
  "risk_level": "medium",
  "config": {
    "stock_filters": {"total_market_cap_max": 50},
    "technical_conditions": ["DIF_CROSS_UP_DEA", "VOLUME_RATIO > 1.5"],
    "trading_strategy_id": null   // 用户后续选择关联
  }
}
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

## 待开发任务

1. **数据库表结构调整**
   - 扩展 strategies 表
   - 扩展 watchlist 表

2. **持仓策略模块**
   - 实现动态切换逻辑
   - 指数指标监控任务
   - 仓位预警通知

3. **买卖策略模板**
   - 模板管理API
   - 计算建议价格逻辑

4. **选股策略优化**
   - 关联买卖模板
   - 自动填充建议值到watchlist

5. **LLM生成接口**
   - 分类型SYSTEM_PROMPT
   - 前端类型选择UI

6. **前端UI调整**
   - 策略列表按类型分组显示
   - 各类型策略的配置表单
   - 策略详情展示优化