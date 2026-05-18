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
- **main.py**: FastAPI app entry point, router registration, lifespan management
- **common/**: Shared utilities
  - `database.py`: SQLite async database manager (WAL mode)
  - `sdk_manager.py`: Centralized AmazingData SDK instance management (singleton pattern - avoids TGW connection limits)
  - `sdk_connection_manager.py`: Rate limiting for SDK queries (semaphore-based)
  - `account_manager.py`: Multi-account management
  - `technical_indicators.py`: MA, RSI, MACD, BOLL, KDJ, ATR, CCI, ADX
  - `timezone.py`: China timezone (Asia/Shanghai) utilities
- **trading/gateway.py**: Trading gateway abstraction with AmazingData/银河 SDK implementations
- **screening/**: Stock screening service with factor registry
- **monitoring/**: Trading signal monitoring service
- **ui/**: REST API endpoints for frontend (accounts, dashboard, positions, trades, strategies, screening, monitoring, market_data, data_explorer, factors)
- **auth/**: User authentication service
- **account_management/**: Broker account management API
- **strategy/**: Strategy management API
- **llm/**: LLM integration for strategy generation
- **data/**: Local data service and download utilities
- **factors/**: Factor calculators (daily, monthly, fundamental)

### Frontend Structure (frontend/src/)
- Vue 3 + Vite + Element Plus + Pinia + Vue Router + ECharts
- **views/**: Dashboard, Trades, Positions, Strategies, DataExplorer, Watchlist, Signals, Accounts, Settings, Login
- **components/**: Reusable Vue components
- **router/**: Route definitions with auth guards
- **stores/**: Pinia state management

### Database Tables
Located in `data/stockwinner.db` (core business) and `data/kline.db` (market data):
- `accounts`: Account information and credentials
- `positions`: Holdings records
- `trades`: Trade history
- `strategies`: Investment strategies
- `watchlist`: Watch list
- `candidate_stocks`: Screening candidates
- `trading_signals`: Trade signals
- `stock_daily_factors`: Daily factors (~5.8M records)
- `stock_monthly_factors`: Monthly factors (~290K records)
- `kline_data`: K-line historical data

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
