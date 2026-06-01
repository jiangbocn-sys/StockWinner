# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

1. 不要假设我清楚自己想要什么。动机或目标不清晰时，停下来讨论。
2. 目标清晰但路径不是最短的，直接告诉我并建议更好的办法。
3. 遇到问题追根因，不打补丁。每个决策都要能回答"为什么"。
4. 输出说重点，砍掉一切不改变决策的信息。
5. 解决任何出现的问题时，必须考虑这个解决方案对全局的影响。

## Project Overview

StockWinner v7.8.20 - 多账户智能股票交易系统，集成 AmazingData SDK（银河证券）实时行情。FastAPI + Vue 3，支持选股、策略、回测、MCP 接入。

**详细架构文档**：[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)

## Development Commands

### 系统服务（生产环境）
```bash
sudo systemctl restart stockwinner-backend   # 重启后端（端口 8080）
sudo systemctl restart stockwinner-mcp       # 重启 MCP（端口 9000）
sudo systemctl status stockwinner-backend stockwinner-mcp
```

**重要**：前端构建后重启系统服务，不要用 `./start_backend.sh`（会冲突）。

### 前端
```bash
cd frontend && npm run build   # 构建后重启 backend 服务
npm run dev                     # 开发服务器
```

## 运行环境

**试运行阶段**：生产与开发在同一主机，代码修改直接影响生产环境。

## 强制规则

### 规则 1：数据访问分层（自上而下，禁止跨层）

```
UI 层 → 业务层 → gateway → SDKManager → SDK
```

- UI 禁止直接调用 SDK / 直接连数据库取行情
- 所有行情/K线查询必须走 `gateway.get_market_data()` / `gateway.get_kline_data()`

### 规则 2：数据库访问

```python
from services.common.database import get_db_manager
db = get_db_manager()
await db.fetchall("SELECT ...")
```
禁止直接 `sqlite3.connect()` / `aiosqlite`

### 规则 3：SDK 调用

```python
from services.common.sdk_manager import get_sdk_manager
sdk_mgr = get_sdk_manager()
```
禁止直接 `from AmazingData import xxx` 创建实例

### 规则 4：时区

```python
from services.common.timezone import get_china_time, CHINA_TZ
```
- 禁止在其他文件定义时区
- 禁止用 `datetime.now()`
- 数据库存 naive ISO string（不带 +08:00）

### 规则 5：股票代码格式

统一使用 `600000.SH` / `000001.SZ` 格式，用 `normalize_stock_code()` 规范化。

### 规则 6：APScheduler cron

`day_of_week` 必须用命名格式 `mon-fri`，禁止数字（与标准 cron 不一致）。

## Key Patterns

### Gateway 使用
```python
from services.trading.gateway import get_gateway
gateway = await get_gateway()
await gateway.get_market_data("600000.SH")
await gateway.get_kline_data("600000.SH", period="day", start_date="20260101")
```

### 前端认证

所有 `/api/v1/ui/` 端点需要 Authorization header：
```javascript
const token = localStorage.getItem('auth_token')
fetch(url, { headers: { 'Authorization': 'Bearer ' + token } })
```

## 配置文件

| 文件 | 用途 |
|------|------|
| `config/llm.json` | LLM 配置 |
| `config/mcp.json` | MCP 工具配置 |
| `config/sdk_credentials.env` | SDK 凭证 |

## API 端点

- API prefix: `/api/v1/ui/`
- Frontend: `/ui/`
- Health: `/api/v1/health`
- MCP: `/mcp` 或 `http://localhost:9000`

---

**详细信息见**：
- [架构文档](docs/ARCHITECTURE.md) - 模块结构、表结构、前端页面
- [API 参考](docs/DATA_API_REFERENCE.md) - 数据 API
- [部署指南](docs/DEPLOYMENT.md) - 部署说明