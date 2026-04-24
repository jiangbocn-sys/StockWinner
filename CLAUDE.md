# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.
1.不要假设我清楚自己想要什么。动机或目标不清晰时，停下来讨论。
2.目标清晰但路径不是最短的，直接告诉我并建议更好的办法。
3.遇到问题追根因，不打补丁。每个决策都要能回答"为什么"。
4.输出说重点，砍掉一切不改变决策的信息。

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
All timestamps use China timezone (Asia/Shanghai, UTC+8). Use `get_china_time()` helper.

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
