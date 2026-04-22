# 选股与交易监控模块文档

## 概述

本模块实现了完整的选股→Watchlist→交易信号→交易执行的自动化流程。

### 模块架构

```
策略管理 → 选股模块 → Watchlist → 交易监控 → 交易信号 → 交易执行
   ↓          ↓           ↓           ↓           ↓          ↓
strategies  screening  watchlist  monitoring  signals   execution
```

## 数据库表结构

### 1. Watchlist 表（候选股票池）

```sql
CREATE TABLE watchlist (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id TEXT NOT NULL,           -- 账户 ID
    strategy_id INTEGER,                -- 关联策略 ID
    stock_code TEXT NOT NULL,           -- 股票代码
    stock_name TEXT,                    -- 股票名称
    reason TEXT,                        -- 入选原因
    buy_price REAL,                     -- 买入价格
    stop_loss_price REAL,               -- 止损价格
    take_profit_price REAL,             -- 止盈价格
    target_quantity INTEGER,            -- 目标数量
    status TEXT DEFAULT 'pending',      -- 状态
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);
```

**状态说明：**
- `pending` - 待观察：刚入选 watchlist，等待买入时机
- `watching` - 观察中：价格接近买入价，准备买入
- `bought` - 已买入：已执行买入操作
- `sold` - 已卖出：已执行卖出操作
- `ignored` - 已忽略：手动忽略该股票

### 2. Trading Signals 表（交易信号）

```sql
CREATE TABLE trading_signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id TEXT NOT NULL,           -- 账户 ID
    strategy_id INTEGER,                -- 关联策略 ID
    stock_code TEXT NOT NULL,           -- 股票代码
    stock_name TEXT,                    -- 股票名称
    signal_type TEXT NOT NULL,          -- 信号类型
    price REAL,                         -- 信号价格
    target_quantity INTEGER,            -- 目标数量
    status TEXT DEFAULT 'pending',      -- 状态
    created_at TIMESTAMP,
    executed_at TIMESTAMP
);
```

**信号类型：**
- `buy` - 买入信号
- `sell_stop_loss` - 止损卖出信号
- `sell_take_profit` - 止盈卖出信号

## API 端点

### 选股服务

| 方法 | 端点 | 说明 |
|------|------|------|
| POST | `/api/v1/ui/{account_id}/screening/start` | 启动选股服务 |
| POST | `/api/v1/ui/{account_id}/screening/stop` | 停止选股服务 |
| GET | `/api/v1/ui/{account_id}/screening/status` | 获取服务状态 |
| POST | `/api/v1/ui/{account_id}/screening/run` | 立即执行选股 |

### Watchlist 管理

| 方法 | 端点 | 说明 |
|------|------|------|
| GET | `/api/v1/ui/{account_id}/watchlist` | 获取 watchlist |
| GET | `/api/v1/ui/{account_id}/watchlist/{stock_code}` | 获取单只股票 |
| DELETE | `/api/v1/ui/{account_id}/watchlist/{stock_code}` | 移除股票 |
| PUT | `/api/v1/ui/{account_id}/watchlist/{stock_code}/status` | 更新状态 |
| PUT | `/api/v1/ui/{account_id}/watchlist/{stock_code}/prices` | 更新价格参数 |
| POST | `/api/v1/ui/{account_id}/watchlist/clear` | 清空 watchlist |

### 交易监控服务

| 方法 | 端点 | 说明 |
|------|------|------|
| POST | `/api/v1/ui/{account_id}/monitoring/start` | 启动监控服务 |
| POST | `/api/v1/ui/{account_id}/monitoring/stop` | 停止监控服务 |
| GET | `/api/v1/ui/{account_id}/monitoring/status` | 获取服务状态 |

### 交易信号管理

| 方法 | 端点 | 说明 |
|------|------|------|
| GET | `/api/v1/ui/{account_id}/signals` | 获取交易信号 |
| GET | `/api/v1/ui/{account_id}/signals/{signal_id}` | 获取信号详情 |
| POST | `/api/v1/ui/{account_id}/signals/{signal_id}/execute` | 执行信号 |
| POST | `/api/v1/ui/{account_id}/signals/{signal_id}/cancel` | 取消信号 |
| POST | `/api/v1/ui/{account_id}/signals/clear` | 清空信号 |

## 使用示例

### 1. 启动选股服务

```bash
# 启动选股服务，每 60 秒扫描一次
curl -X POST http://localhost:8080/api/v1/ui/bobo/screening/start \
  -H "Content-Type: application/json" \
  -d '{"interval": 60}'

# 立即执行一次选股
curl -X POST http://localhost:8080/api/v1/ui/bobo/screening/run
```

### 2. 查看 watchlist

```bash
# 获取所有 watchlist 股票
curl http://localhost:8080/api/v1/ui/bobo/watchlist

# 获取 pending 状态的股票
curl http://localhost:8080/api/v1/ui/bobo/watchlist?status=pending

# 移除某只股票
curl -X DELETE http://localhost:8080/api/v1/ui/bobo/watchlist/600519.SH
```

### 3. 启动交易监控

```bash
# 启动监控服务，每 30 秒检查一次
curl -X POST http://localhost:8080/api/v1/ui/bobo/monitoring/start \
  -H "Content-Type: application/json" \
  -d '{"interval": 30}'

# 获取交易信号
curl http://localhost:8080/api/v1/ui/bobo/signals

# 执行交易信号
curl -X POST http://localhost:8080/api/v1/ui/bobo/signals/1/execute
```

### 4. 完整流程示例

```bash
# 1. 创建并激活策略
curl -X POST http://localhost:8080/api/v1/ui/bobo/strategies \
  -H "Content-Type: application/json" \
  -d '{"name":"MA 金叉策略","strategy_type":"manual"}'
curl -X POST http://localhost:8080/api/v1/ui/bobo/strategies/1/activate

# 2. 启动选股服务
curl -X POST http://localhost:8080/api/v1/ui/bobo/screening/start \
  -d '{"interval": 60}'

# 3. 启动交易监控
curl -X POST http://localhost:8080/api/v1/ui/bobo/monitoring/start \
  -d '{"interval": 30}'

# 4. 查看服务状态
curl http://localhost:8080/api/v1/ui/bobo/dashboard | jq '.system_health.services'
```

## 服务状态

### 仪表盘服务状态

```json
{
  "galaxy_api": "disconnected",
  "screening": "running",
  "monitoring": "running",
  "notification": "ok"
}
```

## 工作流程

```
┌──────────────┐
│  策略管理    │ 激活的策略
└──────┬───────┘
       │
       ▼
┌──────────────┐
│  选股服务    │ 根据策略条件扫描股票
│  (Screening) │
└──────┬───────┘
       │
       ▼
┌──────────────┐
│  Watchlist   │ 候选股票池
│  (股票池)    │ pending → watching
└──────┬───────┘
       │
       ▼
┌──────────────┐
│  交易监控    │ 监控价格，触发信号
│  (Monitoring)│
└──────┬───────┘
       │
       ▼
┌──────────────┐
│  交易信号    │ buy/sell_stop_loss/
│  (Signals)   │ sell_take_profit
└──────┬───────┘
       │
       ▼
┌──────────────┐
│  交易执行    │ 执行交易（待实现）
│  (Execution) │
└──────────────┘
```

## 当前实现状态

### 已完成 ✅

- [x] Watchlist 数据库表
- [x] 交易信号数据库表
- [x] 选股服务（后台循环扫描）
- [x] Watchlist 管理 API
- [x] 交易监控服务（后台循环监控）
- [x] 交易信号管理 API
- [x] 仪表盘服务状态集成

### 待实现 📋

- [ ] 真实行情数据接入（当前使用模拟数据）
- [ ] 银河 SDK 集成
- [ ] 真实交易执行
- [ ] 选股条件解析引擎（MA/RSI 等技术指标）
- [ ] 前端 UI 页面（Watchlist/Signals 管理）

## 文件位置

- 选股服务：`services/screening/service.py`
- 监控服务：`services/monitoring/service.py`
- 选股 API: `services/ui/screening.py`
- 监控 API: `services/ui/monitoring.py`
- 数据库初始化：`scripts/init_db.py`
