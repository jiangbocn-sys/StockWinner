# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.
1.不要假设我清楚自己想要什么。动机或目标不清晰时，停下来讨论。
2.目标清晰但路径不是最短的，直接告诉我并建议更好的办法。
3.遇到问题追根因，不打补丁。每个决策都要能回答"为什么"。
4.输出说重点，砍掉一切不改变决策的信息。
5.解决任何出现的问题时，必须考虑这个解决方案对全局的影响。

## Project Overview

StockWinner is a multi-account intelligent stock trading system (v6.2.4) with real-time market data integration via the AmazingData SDK (银河证券). It provides a FastAPI backend with Vue 3 frontend for stock screening, trading signals, factor analysis, and portfolio monitoring.

## Development Commands

### Backend
```bash
# Start backend service (recommended)
./start_backend.sh start     # Start service
./start_backend.sh stop      # Stop service
./start_backend.sh restart   # Restart service
./start_backend.sh status    # Check status
./start_backend.sh logs      # View live logs

# Manual start (development)
python3 -m uvicorn services.main:app --host 0.0.0.0 --port 8080

# Database initialization
python3 scripts/init_db.py
```

### Frontend
```bash
cd frontend
npm install        # Install dependencies
npm run dev        # Development server
npm run build      # Production build
```

### Tests
```bash
python3 tests/test_all.py
```

### API Testing
```bash
curl http://localhost:8080/api/v1/health
curl http://localhost:8080/api/v1/ui/accounts
```

## Architecture

### Backend Structure (services/)
- **boot/**: App entry point, lifespan (startup/shutdown), middleware, router registration
- **common/**: Shared utilities
  - `database.py`: SQLite async/sync connection managers (WAL mode, busy_timeout)
  - `sdk_manager.py`: SDK 调用入口单例，所有 SDK 数据查询通过 IPC 代理 → 子进程
  - `sdk_proxy_client.py`: IPC 客户端（Unix socket），自动重连，RLock 串行化
  - `sdk_subprocess_server.py`: SDK 子进程服务端，login + 所有 SDK 调用
  - `sdk_connection_manager.py`: TGW 连接生命周期管理（按需连接+grace period 释放）
  - `sdk_ipc.py`: IPC 协议定义（base64 pickle 序列化 DataFrame）
  - `price_cache.py`: 内存 OHLCV 行情缓存，source 标记 + TTL 管理
  - `structured_logger.py`: 结构化日志（JSON 格式）
  - `timezone.py`: 中国时区（唯一合法来源，禁止各模块自己定义）
  - `stock_code.py`: 股票代码规范化
  - `task_manager.py`: 后台任务状态管理
- **trading/**: 交易网关
  - `gateway.py`: 门面，委托给子服务模块
  - `gateway_dispatcher.py`: 行情调度器，管理订阅/去重/批量刷新/PriceCache 写入
  - `market_data_service.py`: 行情数据（缓存优先→并发 SDK+备用→kline.db 兜底）
  - `kline_service.py`: K 线数据
  - `trading_hours.py`: 交易时间判断（日历缓存 24h）
  - `models.py`: MarketData, OrderResult, TradingGatewayInterface
- **monitoring/**: 交易监控
  - `service.py`: 监控循环（60s 刷新全量 watchlist+持仓），委托给子模块
  - `signal_evaluator.py`: 信号评估（三层策略优先级）
  - `signal_executor.py`: 信号执行
  - `price_cache_manager.py`: 持仓盈亏刷新+DB 刷盘
  - `health_tracker.py`: SDK 健康跟踪
- **ui/**: FastAPI 端点
  - `dashboard.py`: 仪表盘（数据源并行健康检测）
  - `positions.py`: 持仓分析（JOIN watchlist 止盈止损价）
  - `screening.py`: 选股监控（缓存优先+后台异步刷新）
  - `market_data.py`: 行情查询+K 线拼接
  - `trading_strategies.py`: 个股止损止盈（trading_strategies 表）
  - `trades.py`: 交易策略配置（trading_strategy_config 表）
- **data/**: 数据下载+多数据源 Provider
  - `data_download.py`: K 线增量下载（连续失败 3 批自动中止）
  - `local_data_service.py`: 本地 K 线保存/查询
  - `providers/`: AmazingData/Eastmoney/Tushare/Sina/Tencent/AKShare

### SDK 子进程隔离架构（核心）

```
主进程 (FastAPI)
  └─ SDKManager → SDKProxyClient (IPC, RLock 串行化)
       └─ Unix socket (/tmp/stockwinner_sdk.sock)
            └─ SDK 子进程 (sdk_subprocess_server.py)
                 └─ AmazingData SDK → TGW TCP (单用户单连接)
```

关键约束：
- **TGW 单连接限制**：所有 SDK 调用必须串行化，RLock 确保不重入死锁
- **Snapshot 永久禁用**：pandas 2.x 不兼容 'S' 频率别名，snapshot 已默认跳过
- **Kline fallback**：所有实时行情通过日 K 线（period=10008）获取，batch 200 只/次
- **asyncio.to_thread**：所有 async 路径的 SDK 调用必须包装在线程池中，防止阻塞事件循环
- **is_connected 不重连**：只检查状态，不自动重连（避免主线程死锁），重连由 _call_ipc 内部完成
- **子进程生命周期**：kill -9 直接发 TCP RST 释放 TGW 连接槽，start_backend.sh 重启时强杀清理

### 数据源架构

```
GatewayDispatcher._query_sdk(codes)
  ├─ ① snapshot: 已永久跳过 (pandas 不兼容)
  ├─ ② kline fallback: SDK query_kline(period=day, batch=200)
  │      → _build_market_data_from_kline() → MarketData(source="kline")
  └─ ③ kline.db 兜底: 仅非交易时段，source="kline_db"
       → _write_to_price_cache() → PriceCache
```

### PriceCache 行情缓存

- 线程安全的内存 OHLCV 单例，全局共享（不按账户隔离）
- `source` 标记：`snapshot`(3) > `kline`(2) > `kline_db`(1)，低优先级不覆盖高优先级
- TTL：交易时段 600s，非交易时段 43200s
- `is_tradable()`：策略执行只使用 source=snapshot/kline 的数据，kline_db 不可用于交易决策
- 更新路径：
  1. Monitor 循环（每 60s 刷新全量 watchlist+持仓）
  2. UI 后台异步刷新（用户打开页面时按需触发）
  3. 启动预热（持仓+活跃 watchlist，一次）

### K 线拼接

`GET /stocks/{code}/kline-local` 端点：
- 从 kline.db 读历史数据
- 当日数据从 PriceCache 拼接（不调 SDK）
- 若 PriceCache 中 OHLC 全等（不完整日线），跳过当日拼接

### 监控策略三层结构

```
_evaluate_sell_decision (每只股票):
  ① 个股止损止盈 (trading_strategies 表) — 优先级最高
     ├─ trailing_stop: 最高价 × (1-take_profit_pct)
     ├─ stop_loss_pct: avg_cost × (1-stop_loss_pct)
     └─ fixed: 固定 stop_loss_price / take_profit_price
  ② 代码卖出策略 (strategies 表) — 中间层
  ③ watchlist 止盈止损 (watchlist 表) — 兜底

evaluate_trading_strategies (股票池):
  → trading_strategy_config 表 → 条件评估 → 批量买卖
```

### 策略表区分

| 表 | 用途 | UI 标签 |
|---|------|--------|
| `trading_strategies` | 个股止损止盈（固定价/百分比/移动止损）| 策略管理→"个股止损止盈" |
| `trading_strategy_config` | 代码策略的条件配置（股票池级别）| 策略管理→"代码策略" |
| `watchlist` | 候选股止损止盈价（仅固定价格）| 选股监控 |

### 前端性能

- **Pinia stores**：positions/dashboard/watchlist 数据跨页面持久化，切标签不重载
- **AbortController**：组件卸载时取消未完成的 fetch，防止快速切换竞争
- **轮询间隔**：持仓/自选静默刷新 10s→30s
- **并行加载**：Dashboard onMounted 中 loadAccounts+loadDashboard 并行
- **健康检测并行**：6 个数据源并行检测→完成即更新缓存→逐个亮灯

## Key Patterns

### SDK Usage
Always use `get_sdk_manager()` to obtain SDK instances - never create independent instances to avoid TGW connection limit errors:
```python
from services.common.sdk_manager import get_sdk_manager
sdk_mgr = get_sdk_manager()
market_data = sdk_mgr.get_market_data()  # Cached instance
base_data = sdk_mgr.get_base_data()      # Cached instance
```

### Async Database
Use the global `DatabaseManager` singleton:
```python
from services.common.database import get_db_manager
db = get_db_manager()
await db.fetchall("SELECT * FROM accounts WHERE is_active = 1")
```

### Gateway Pattern
Trading gateways implement `TradingGatewayInterface`. Use `create_gateway()` factory function:
```python
from services.trading.gateway import create_gateway, get_gateway
gateway = await get_gateway()  # Returns cached instance
await gateway.get_market_data("600000.SH")
await gateway.get_kline_data("600000.SH", period="day", start_date="20260101")
```

### Stock Code Format
Stock codes use `.SH` (Shanghai) or `.SZ` (Shenzhen) suffix format: `600000.SH`, `000001.SZ`

### Timezone
All timestamps use China timezone (Asia/Shanghai, UTC+8). **唯一合法来源**：
```python
from services.common.timezone import get_china_time, CHINA_TZ
```
- 禁止在任何文件中定义 `CHINA_TZ` 或 `get_china_time()`
- 禁止使用 `datetime.now()`（返回系统时区），必须用 `get_china_time()`
- 数据库统一存中国时间的 naive ISO string（不带 `+08:00`）
- 前端收到 naive string 后应附加 `+08:00` 再解析

## 数据访问架构（强制遵守）

### 分层架构（自上而下，禁止跨层/逆向调用）

```
┌─────────────────────────────────────────────────┐
│ UI 层 (services/ui/*.py)                        │
│ 职责：接收请求、参数校验、路由                    │
│ 禁止：直接调用 SDK / 直接连数据库取行情数据       │
├─────────────────────────────────────────────────┤
│ 业务层 (services/trading/execution_service.py)   │
│         services/monitoring/service.py           │
│         services/screening/*.py                  │
│ 职责：业务逻辑、计算、状态管理                    │
│ 允许：调用 gateway / database / SDKManager        │
├─────────────────────────────────────────────────┤
│ 网关层 (services/trading/gateway.py)             │
│ 职责：交易相关 SDK 调用的统一抽象层               │
│ 必须：内部通过 SDKManager 调用 SDK               │
├─────────────────────────────────────────────────┤
│ 数据层 (services/common/sdk_manager.py)          │
│         services/common/database.py              │
│         services/common/sdk_connection_manager.py│
│ 职责：SDK 实例缓存、连接管理、并发串行化          │
│         TGW 连接数限制的唯一管理者               │
├─────────────────────────────────────────────────┤
│ SDK 层 (AmazingData)                             │
│ TGW TCP 连接（单用户单连接限制）                  │
└─────────────────────────────────────────────────┘
```

### 强制规则

**规则 1：所有实时行情/K 线/选股数据查询必须走 gateway → SDKManager**
- UI 端点调用 `gateway.get_market_data()` / `gateway.get_kline_data()`
- 禁止在 UI 层或业务层直接 `from AmazingData import xxx` 或创建 SDK 实例
- 禁止用本地 DB 缓存替代 SDK 实时数据（交易相关场景）
- SDK 连接失败时必须提示用户"券商服务器连接失败"，不得静默降级

**规则 2：所有数据库访问必须走 DatabaseManager 单例**
```python
from services.common.database import get_db_manager
db = get_db_manager()
await db.fetchall("SELECT ...")
```
- 禁止直接 `sqlite3.connect()` / `asyncio.connect()` / `aiosqlite`
- 禁止在多个模块中各自创建数据库连接

**规则 3：SDK 调用必须走 SDKManager 单例**
```python
from services.common.sdk_manager import get_sdk_manager
sdk_mgr = get_sdk_manager()
result = sdk_mgr.query_kline(...)
```
- 禁止 `from AmazingData import InfoData, BaseData, MarketData` 直接创建实例
- TGW 限制单用户单连接，绕过 SDKManager 会导致连接数超限

**规则 4：网关是交易类数据的唯一出口**
- 行情查询：`await gateway.get_market_data(stock_code)`
- K 线查询：`await gateway.get_kline_data(stock_code, period=..., start_date=..., end_date=...)`
- 批量行情：`await gateway.get_batch_market_data(codes)`
- Gateway 内部自动走 SDKManager，无需调用方关心连接管理

**规则 5：股票格式统一**
- 内部统一使用 `600000.SH` / `000001.SZ` 格式
- 使用 `services/common/stock_code.py` 的 `normalize_stock_code()` 规范化
- 禁止各模块各自实现后缀判断逻辑

**规则 6：APScheduler cron 表达式 day_of_week 必须用命名格式**
- `CronTrigger.from_crontab()` 的 `day_of_week` 使用 Python weekday 映射（0=周一），与标准 cron（0=周日）不一致
- 必须使用 `mon-fri`、`sat`、`sun` 等命名，禁止使用数字范围如 `1-5`、`0-4`
- 适用于数据库 `strategy_tasks.cron_expression` 和代码中所有 cron 表达式

## SDK Integration (AmazingData/银河证券)

The system integrates with Galaxy Securities SDK for real-time market data:
- **Host**: `101.230.159.234:8600`
- **Credentials**: Configured in `sdk_manager.py`
- **Capabilities**: K-line data (11 periods: 1m/3m/5m/10m/15m/30m/60m/120m/day/week/month), stock lists, index data, industry classification, financial statements

Connection limits exist - the SDKManager and sdk_connection_manager.py handle rate limiting automatically.

## Configuration Files
- `config/llm.json`: LLM provider configuration (API keys, model selection)
- `config/screening_templates.json`: Preset stock screening templates
- `requirements.txt`: Python dependencies
- `frontend/package.json`: Frontend dependencies

## API Base Path
- API prefix: `/api/v1/ui/`
- Frontend served at: `/ui/`
- Health check: `/api/v1/health`
