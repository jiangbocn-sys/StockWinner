# 扩展数据 API 文档

> 版本: v7.1.6 | 日期: 2026-05-13

## 概述

通过 SDK（AmazingData / 银河证券）获取大盘指数、行业板块、财报数据、龙虎榜、融资融券、大宗交易、国债收益率等数据，经 SDKManager 封装后通过 UI API 和 Agent API 两层暴露。

## 架构

```
SDK Layer (AmazingData)
    ↓
SDKManager Layer (sdk_manager.py — 登录 + 令牌 acquire/release + 超时保护)
    ↓
UI/API Layer   → services/ui/data_service.py     → /api/v1/ui/{account_id}/data/...
Agent API Layer → services/agent/handlers.py       → /api/v1/agent/query/data/...
```

所有 SDK 调用统一通过 `SDKManager`（自动 acquire/release token），不绕过连接管理器。

## 文件清单

| 文件 | 说明 |
|------|------|
| `services/common/sdk_manager.py` | 新增 10 个 SDK 封装方法 |
| `services/ui/data_service.py` | 17 个 UI API 端点 |
| `services/agent/handlers.py` | 17 个 Agent 查询端点（含审计日志） |
| `services/main.py` | 路由注册 |

---

## UI API

**认证**: 无需额外认证（通过 `{account_id}` 路径参数 + 数据库校验 is_active）
**前缀**: `/api/v1/ui/{account_id}/data`

### 大盘指数

#### GET /data/index/list

指数代码列表（上交所+深交所）。

| 参数 | 位置 | 必填 | 说明 |
|------|------|------|------|
| account_id | path | 是 | 账户 ID |

**示例**:
```bash
curl http://localhost:8080/api/v1/ui/acc123/data/index/list
```

**响应**:
```json
{
  "success": true,
  "data": [{"index_code": "000001.SH", "index_name": "上证指数"}, ...],
  "count": 2
}
```

#### GET /data/index/kline

指数 K 线数据。

| 参数 | 位置 | 必填 | 说明 |
|------|------|------|------|
| account_id | path | 是 | 账户 ID |
| index_code | query | 是 | 指数代码，如 000300.SH |
| period | query | 否 | day / week / month，默认 day |
| limit | query | 否 | 返回数量，默认 100 |

**示例**:
```bash
curl "http://localhost:8080/api/v1/ui/acc123/data/index/kline?index_code=000300.SH&period=day&limit=50"
```

### 行业/板块

#### GET /data/industry/list

申万行业分类列表。

| 参数 | 位置 | 必填 | 说明 |
|------|------|------|------|
| account_id | path | 是 | 账户 ID |
| level | query | 否 | 行业级别：1=一级, 2=二级, 3=三级，默认 1 |

**示例**:
```bash
curl "http://localhost:8080/api/v1/ui/acc123/data/industry/list?level=1"
```

**响应**:
```json
{
  "success": true,
  "data": [{"INDEX_CODE": "801010.SI", "INDEX_NAME": "农林牧渔", "LEVEL_TYPE": 1}, ...],
  "count": 31
}
```

#### GET /data/industry/kline

行业指数日行情数据（含 OHLCV + PE/PB）。

| 参数 | 位置 | 必填 | 说明 |
|------|------|------|------|
| account_id | path | 是 | 账户 ID |
| index_code | query | 是 | 行业指数代码，如 801010.SI |

**示例**:
```bash
curl "http://localhost:8080/api/v1/ui/acc123/data/industry/kline?index_code=801010.SI"
```

**响应**:
```json
{
  "success": true,
  "data": {
    "index_code": "801010.SI",
    "kline": [
      {"trade_date": "2026-05-13", "open": 1234.5, "high": 1240.0, "low": 1230.0, "close": 1238.0, "volume": 1234567, "pe": 15.2, "pb": 1.8},
      ...
    ],
    "count": 120
  }
}
```

#### GET /data/industry/constituent

行业成分股列表。

| 参数 | 位置 | 必填 | 说明 |
|------|------|------|------|
| account_id | path | 是 | 账户 ID |
| index_code | query | 是 | 行业指数代码，如 801010.SI |

#### GET /data/index/constituent

指数成分股列表。

| 参数 | 位置 | 必填 | 说明 |
|------|------|------|------|
| account_id | path | 是 | 账户 ID |
| index_code | query | 是 | 指数代码，如 000300.SH |

### 财报数据

#### GET /data/financial/income

利润表。

| 参数 | 位置 | 必填 | 说明 |
|------|------|------|------|
| account_id | path | 是 | 账户 ID |
| stock_code | query | 是 | 股票代码，如 600000.SH |

#### GET /data/financial/balance

资产负债表。参数同上。

#### GET /data/financial/cashflow

现金流量表。参数同上。

#### GET /data/financial/profit-notice

业绩预告。参数同上。

#### GET /data/financial/profit-express

业绩快报。参数同上。

**财报响应示例**:
```json
{
  "success": true,
  "data": {
    "stock_code": "600000.SH",
    "records": [
      {"market_code": "600000.SH", "report_date": "20251231", "total_operate_income": 12345678900.0, ...},
      ...
    ],
    "count": 4
  }
}
```

### 龙虎榜

#### GET /data/dragon-tiger

龙虎榜数据。

| 参数 | 位置 | 必填 | 说明 |
|------|------|------|------|
| account_id | path | 是 | 账户 ID |
| stock_code | query | 是 | 股票代码 |
| start_date | query | 是 | 开始日期 YYYYMMDD |
| end_date | query | 是 | 结束日期 YYYYMMDD |

> **注意**: `start_date` 和 `end_date` 为必填参数，格式 `YYYYMMDD`（整数）。

**示例**:
```bash
curl "http://localhost:8080/api/v1/ui/acc123/data/dragon-tiger?stock_code=600000.SH&start_date=20250101&end_date=20260512"
```

### 融资融券

#### GET /data/margin/summary

融资融券汇总数据。

| 参数 | 位置 | 必填 | 说明 |
|------|------|------|------|
| account_id | path | 是 | 账户 ID |
| start_date | query | 是 | 开始日期 YYYYMMDD |
| end_date | query | 是 | 结束日期 YYYYMMDD |

#### GET /data/margin/detail

融资融券明细数据。

| 参数 | 位置 | 必填 | 说明 |
|------|------|------|------|
| account_id | path | 是 | 账户 ID |
| stock_code | query | 是 | 股票代码 |
| start_date | query | 是 | 开始日期 YYYYMMDD |
| end_date | query | 是 | 结束日期 YYYYMMDD |

### 大宗交易

#### GET /data/block-trading

大宗交易数据。

| 参数 | 位置 | 必填 | 说明 |
|------|------|------|------|
| account_id | path | 是 | 账户 ID |
| stock_code | query | 是 | 股票代码 |
| start_date | query | 是 | 开始日期 YYYYMMDD |
| end_date | query | 是 | 结束日期 YYYYMMDD |

### 国债收益率

#### GET /data/treasury-yield

国债收益率曲线（无需额外参数）。

| 参数 | 位置 | 必填 | 说明 |
|------|------|------|------|
| account_id | path | 是 | 账户 ID |

**示例**:
```bash
curl "http://localhost:8080/api/v1/ui/acc123/data/treasury-yield"
```

---

## Agent API

**认证**: `X-Agent-Key: sk-agent-xxxx` 请求头
**前缀**: `/api/v1/agent/query/data`

### 大盘指数

#### GET /query/data/index/list

指数代码列表。

```bash
curl -H "X-Agent-Key: sk-agent-xxx" \
  http://localhost:8080/api/v1/agent/query/data/index/list
```

#### GET /query/data/index/kline

| 参数 | 必填 | 说明 |
|------|------|------|
| index_code | 是 | 指数代码 |
| period | 否 | day/week/month |
| limit | 否 | 默认 100 |

### 行业/板块

| 端点 | 必填参数 | 说明 |
|------|----------|------|
| `/query/data/industry/list` | 无（可选 level） | 行业分类 |
| `/query/data/industry/kline?index_code=` | index_code | 行业行情 |
| `/query/data/industry/constituent?index_code=` | index_code | 行业成分股 |
| `/query/data/index/constituent?index_code=` | index_code | 指数成分股 |

### 财报数据

| 端点 | 必填参数 | 说明 |
|------|----------|------|
| `/query/data/financial/income?stock_code=` | stock_code | 利润表 |
| `/query/data/financial/balance?stock_code=` | stock_code | 资产负债表 |
| `/query/data/financial/cashflow?stock_code=` | stock_code | 现金流量表 |
| `/query/data/financial/profit-notice?stock_code=` | stock_code | 业绩预告 |
| `/query/data/financial/profit-express?stock_code=` | stock_code | 业绩快报 |

### 龙虎榜

#### GET /query/data/dragon-tiger

| 参数 | 必填 | 说明 |
|------|------|------|
| stock_code | 是 | 股票代码 |
| start_date | 是 | YYYYMMDD |
| end_date | 是 | YYYYMMDD |

### 融资融券

| 端点 | 必填参数 | 说明 |
|------|----------|------|
| `/query/data/margin/summary` | start_date, end_date | 汇总 |
| `/query/data/margin/detail?stock_code=` | stock_code, start_date, end_date | 明细 |

### 大宗交易

#### GET /query/data/block-trading

| 参数 | 必填 | 说明 |
|------|------|------|
| stock_code | 是 | 股票代码 |
| start_date | 是 | YYYYMMDD |
| end_date | 是 | YYYYMMDD |

### 国债收益率

#### GET /query/data/treasury-yield

无需额外参数，自动审计日志记录。

### Agent 响应格式

所有 Agent 端点统一返回:

```json
{
  "success": true,
  "data": { ... },
  "count": 42
}
```

或失败时:
```json
{
  "success": false,
  "error": "错误信息"
}
```

---

## SDKManager 新增方法

以下方法已添加到 `services/common/sdk_manager.py`，供 UI 和 Agent 端点调用：

| 方法 | 参数 | 返回 |
|------|------|------|
| `get_profit_notice(stock_codes)` | list | DataFrame |
| `get_profit_express(stock_codes)` | list | DataFrame |
| `get_long_hu_bang(stock_codes, begin_date, end_date)` | list, int, int | DataFrame |
| `get_margin_summary(begin_date, end_date)` | int, int | DataFrame |
| `get_margin_detail(stock_codes, begin_date, end_date)` | list, int, int | DataFrame |
| `get_block_trading(stock_codes, begin_date, end_date)` | list, int, int | DataFrame |
| `get_treasury_yield()` | 无 | DataFrame |
| `get_industry_constituent(index_codes)` | list | DataFrame |
| `get_index_constituent(index_codes)` | list | DataFrame |

每个方法内部模式：`_acquire_sync("download")` → SDK 调用 → `_release_sync(token)`，自动处理排队和令牌释放。

## 端点总览

| 分类 | UI 端点数 | Agent 端点数 | 日期参数必填 |
|------|-----------|-------------|-------------|
| 大盘指数 | 2 | 2 | 否 |
| 行业/板块 | 4 | 4 | 否 |
| 财报数据 | 5 | 5 | 否 |
| 龙虎榜 | 1 | 1 | **是** |
| 融资融券 | 2 | 2 | **是** |
| 大宗交易 | 1 | 1 | **是** |
| 国债收益率 | 1 | 1 | 否 |
| **合计** | **16** | **17** | |

> UI 端点 16 个（指数行情无 account_id 校验独立参数），Agent 端点 17 个（完全镜像，含审计日志）。
