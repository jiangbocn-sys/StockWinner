# StockWinner 系统架构说明

> 版本：v7.8.20
> 更新日期：2026-05-31

---

## 目录

1. [后端模块结构](#后端模块结构)
2. [UI 端点一览](#ui-端点一览)
3. [数据库表结构](#数据库表结构)
4. [前端页面一览](#前端页面一览)
5. [SDK 子进程架构](#sdk-子进程架构)
6. [MCP 服务](#mcp-服务)
7. [系统服务配置](#系统服务配置)

---

## 后端模块结构

services/ 目录结构：

| 模块 | 职责 |
|------|------|
| **boot/** | App 入口、lifespan、middleware、router 注册 |
| **common/** | 共享工具（database/sdk_manager/scheduler_service 等） |
| **agent/** | AI Agent 服务（handlers/middleware/audit） |
| **auth/** | 用户认证（JWT token、密码管理） |
| **mcp/** | MCP 服务（stdio + streamable-http 双模式） |
| **trading/** | 交易网关（gateway/kline_service/gateway_dispatcher） |
| **monitoring/** | 交易监控（service/signal_evaluator/signal_executor） |
| **factors/** | 因子计算（daily/monthly/kline_manager） |
| **screening/** | 选股引擎 |
| **strategy/** | 策略执行引擎 |
| **backtest/** | 回测模块 |
| **llm/** | LLM 服务（策略生成） |
| **tasks/** | 定时任务插件（内置 + 用户自定义） |
| **notifications/** | 通知服务（飞书 webhook） |
| **ui/** | FastAPI 端点 |
| **data/** | 数据下载 + 多数据源 Provider |

### common/ 核心文件

| 文件 | 功能 |
|------|------|
| `database.py` | SQLite async/sync 连接管理（WAL mode） |
| `sdk_manager.py` | SDK 调用单例入口 |
| `sdk_proxy_client.py` | IPC 客户端（Unix socket、优先级队列） |
| `sdk_subprocess_server.py` | SDK 子进程服务端（四级优先级） |
| `scheduler_service.py` | APScheduler 任务调度 |
| `price_cache.py` | 内存 OHLCV 行情缓存 |
| `task_manager.py` | 后台任务状态管理 |
| `technical_indicators.py` | 技术指标计算（MA/EMA/BOLL/MACD/RSI） |
| `timezone.py` | 中国时区唯一合法来源 |
| `stock_code.py` | 股票代码规范化 |
| `circuit_breaker.py` | 熔断器 |

---

## UI 端点一览

services/ui/*.py：

| 端点文件 | 功能 |
|----------|------|
| `dashboard.py` | 仪表盘（数据源健康检测、系统状态） |
| `positions.py` | 持仓分析（K线弹窗、技术指标叠加） |
| `screening.py` | 选股监控（股票池、因子筛选） |
| `market_data.py` | 行情查询 + K线（日线/周线/月线、前复权） |
| `strategies.py` | 策略管理（配置、执行历史） |
| `trading_strategies.py` | 个股止损止盈 |
| `scheduler.py` | 定时任务管理 |
| `data_explorer.py` | 数据浏览（表查询、导出） |
| `accounts.py` | 账户管理 |
| `factors.py` | 因子数据查询 |
| `notifications.py` | 通知配置 |
| `signal_allocation.py` | 信号分配 |
| `system_config.py` | 系统配置 |

---

## 数据库表结构

data/kline.db：

| 表名 | 用途 |
|------|------|
| `kline_data` | 日 K 线（OHLCV） |
| `weekly_kline_data` | 周 K 线 |
| `stock_base_info` | 股票基本信息（每日更新） |
| `stock_daily_factors` | 日因子（技术指标） |
| `stock_monthly_factors` | 月因子（财务数据） |
| `factor_metadata` | 因子元数据注册 |
| `stock_metadata` | 股票元信息 |

### 因子字段示例

**stock_daily_factors 技术指标字段**：
- MA 系列：ma5/ma10/ma20/ma60/ma120/ma250
- EMA 系列：ema12/ema26
- BOLL 系列：boll_upper/boll_middle/boll_lower
- MACD 系列：macd/macd_signal/macd_hist
- 其他：rsi6/rsi12/rsi24/atr/kdj_k/kdj_d/kdj_j

**stock_monthly_factors 财务字段**：
- 估值：pe_ttm/pb/ps_ttm/pcf
- 盈利：roe/roa/gross_margin/net_margin
- 成长： revenue_growth/profit_growth

---

## 前端页面一览

frontend/src/views/*.vue：

| 页面 | 功能 |
|------|------|
| `Dashboard.vue` | 仪表盘 |
| `Positions.vue` | 持仓管理（K线弹窗、指标叠加） |
| `Watchlist.vue` | 自选股管理 |
| `Strategies.vue` | 策略配置 |
| `Backtest.vue` | 回测分析 |
| `DataExplorer.vue` | 数据浏览 |
| `DataManagement.vue` | 数据管理 |
| `Settings.vue` | 系统设置 |
| `Trades.vue` | 交易记录 |
| `Signals.vue` | 信号记录 |
| `AccountManagement.vue` | 账户管理 |
| `Login.vue` | 登录 |
| `ChangePassword.vue` | 修改密码 |

### 前端核心组件

**KlineChart.vue**（K 线图表组件）：

| 功能 | 说明 |
|------|------|
| 日线渲染 | ECharts candlestick + 成交量 + 技术指标叠加 |
| **钻取功能** | 双击日线柱 → 显示分钟 K 线（1m/5m/15m/30m/60m） |
| 周期切换 | 分钟模式下支持 5 个周期切换 |
| 居中显示 | 双击日期的分钟 K 线居中于 500 条窗口 |
| 高亮区域 | 目标日期区域淡粉色背景（markArea） |
| 返回按钮 | 退出钻取返回日线视图 |
| 复用页面 | Watchlist、Positions、Backtest 共用 |

### Pinia Stores

| Store | 功能 |
|-------|------|
| `account.js` | 账户状态（currentAccountId/isAdmin） |
| `dashboard.js` | 仪表盘数据缓存 |
| `positions.js` | 持仓数据缓存 |
| `watchlist.js` | 自选股数据缓存 |

---

## SDK 子进程架构

```
主进程 (FastAPI)
  └─ SDKManager → SDKProxyClient (IPC, RLock 串行化)
       └─ Unix socket (/tmp/stockwinner_sdk.sock)
            └─ SDK 子进程 (sdk_subprocess_server.py)
                 └─ AmazingData SDK → TGW TCP (单用户单连接)
```

**关键约束**：
- TGW 单连接限制：所有 SDK 调用必须串行化
- 四级优先级队列：query(1) > batch(2) > background(3) > maintenance(4)
- asyncio.to_thread：async 调用需包装在线程池中

---

## MCP 服务

MCP (Model Context Protocol) 提供 AI Agent 接入：

- **传输模式**：stdio + streamable-http 双模式
- **HTTP 端口**：9000
- **工具集**：SDK 数据查询、策略操作、因子计算等 13+ 工具
- **权限控制**：Agent 安全中间件校验写操作权限
- **配置文件**：config/mcp.json

---

## 系统服务配置

| 服务 | 端口 | systemd 名称 |
|------|------|-------------|
| Backend | 8080 | stockwinner-backend |
| MCP HTTP | 9000 | stockwinner-mcp |

**管理命令**：
```bash
sudo systemctl restart stockwinner-backend
sudo systemctl restart stockwinner-mcp
sudo systemctl status stockwinner-backend stockwinner-mcp
```

---

## 配置文件

| 文件 | 用途 |
|------|------|
| `config/llm.json` | LLM provider 配置 |
| `config/mcp.json` | MCP 服务配置 |
| `config/screening_templates.json` | 选股模板 |
| `config/sdk_credentials.env` | SDK 凭证（不在仓库） |