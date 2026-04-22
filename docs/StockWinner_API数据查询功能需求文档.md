# StockWinner API数据查询功能需求文档

**文档版本**：v1.0  
**编写日期**：2026年4月6日  
**需求方**：loui（研究伙计）  
**开发方**：Claude Code  

---

## 一、需求背景

### 1.1 现状问题

在使用StockWinner API进行量化选股时，遇到以下问题：

| 问题 | 描述 | 影响 |
|-----|------|------|
| **数据查询限制** | API默认只返回20条数据，无法获取完整数据集 | 无法进行全市场筛选 |
| **筛选功能不完善** | 无法按股票代码、日期、行业等条件精确筛选 | 查询效率低 |
| **数据一致性问题** | 选股扫描与K线数据存在价格差异 | 决策依据不可靠 |
| **分页功能缺失** | 无法通过分页获取大量数据 | 数据获取困难 |
| **排序功能不完善** | 无法按指定字段排序 | 无法获取最新/最旧数据 |

### 1.2 需求目标

提供一套完整的数据查询API，支持：
- 灵活的筛选条件（股票代码、日期范围、行业、市值等）
- 分页查询（支持大数据集）
- 多字段排序
- 聚合统计（分组、计数、求和、平均等）
- 数据导出（CSV、JSON）

---

## 二、功能需求

### 2.1 数据表查询API

#### 2.1.1 基础查询

**端点**：`GET /api/v1/ui/databases/{db_name}/tables/{table_name}/data`

**请求参数**：

| 参数名 | 类型 | 必填 | 说明 | 示例 |
|-------|------|------|------|------|
| `stock_code` | string | 否 | 股票代码（支持单个或多个） | `603118.SH` 或 `603118.SH,002380.SZ` |
| `stock_codes` | string[] | 否 | 股票代码列表（POST方式） | `["603118.SH", "002380.SZ"]` |
| `trade_date` | string | 否 | 交易日期（单日） | `2026-04-03` |
| `start_date` | string | 否 | 开始日期 | `2026-01-01` |
| `end_date` | string | 否 | 结束日期 | `2026-04-03` |
| `date_range` | string | 否 | 日期范围（快捷方式） | `last_30d`, `last_90d`, `ytd` |
| `industry` | string | 否 | 行业分类（申万一级） | `机械设备` |
| `limit` | int | 否 | 返回条数（默认100，最大10000） | `1000` |
| `offset` | int | 否 | 偏移量（分页用） | `0` |
| `page` | int | 否 | 页码（与limit配合） | `1` |
| `sort` | string | 否 | 排序字段 | `trade_date` |
| `order` | string | 否 | 排序方向 | `asc` 或 `desc` |
| `fields` | string | 否 | 返回字段（逗号分隔） | `stock_code,trade_date,close` |

**响应示例**：

```json
{
    "success": true,
    "data": [
        {
            "stock_code": "603118.SH",
            "stock_name": "万盛股份",
            "trade_date": "2026-04-03",
            "open": 13.32,
            "high": 13.36,
            "low": 12.84,
            "close": 13.25,
            "volume": 53228085,
            "amount": 699264955.0
        }
    ],
    "pagination": {
        "page": 1,
        "page_size": 100,
        "total": 5841837,
        "total_pages": 58419
    },
    "query_info": {
        "execution_time_ms": 45,
        "filters_applied": ["stock_code=603118.SH", "trade_date>=2026-01-01"]
    }
}
```

---

#### 2.1.2 高级筛选（POST方式）

**端点**：`POST /api/v1/ui/databases/{db_name}/tables/{table_name}/query`

**请求体**：

```json
{
    "filters": {
        "stock_code": {"in": ["603118.SH", "002380.SZ"]},
        "trade_date": {"between": ["2026-01-01", "2026-04-03"]},
        "circ_market_cap": {"gte": 2000000000, "lte": 30000000000},
        "pe_ttm": {"gt": 0, "lt": 25},
        "pb": {"gt": 0, "lt": 3},
        "is_traded": {"eq": 1}
    },
    "fields": ["stock_code", "stock_name", "trade_date", "close", "circ_market_cap", "pe_ttm", "pb"],
    "sort": [{"field": "trade_date", "order": "desc"}, {"field": "circ_market_cap", "order": "asc"}],
    "limit": 1000,
    "offset": 0
}
```

**支持的筛选操作符**：

| 操作符 | 说明 | 示例 |
|-------|------|------|
| `eq` | 等于 | `{"is_traded": {"eq": 1}}` |
| `ne` | 不等于 | `{"is_traded": {"ne": 0}}` |
| `gt` | 大于 | `{"pe_ttm": {"gt": 0}}` |
| `gte` | 大于等于 | `{"circ_market_cap": {"gte": 2000000000}}` |
| `lt` | 小于 | `{"pe_ttm": {"lt": 25}}` |
| `lte` | 小于等于 | `{"pb": {"lte": 3}}` |
| `in` | 在列表中 | `{"stock_code": {"in": ["603118.SH", "002380.SZ"]}}` |
| `not_in` | 不在列表中 | `{"stock_code": {"not_in": ["688001.SH"]}}` |
| `between` | 区间 | `{"trade_date": {"between": ["2026-01-01", "2026-04-03"]}}` |
| `like` | 模糊匹配 | `{"stock_name": {"like": "%银行%"}}` |
| `is_null` | 为空 | `{"pe_ttm": {"is_null": true}}` |
| `is_not_null` | 不为空 | `{"pe_ttm": {"is_not_null": true}}` |

---

#### 2.1.3 聚合统计

**端点**：`POST /api/v1/ui/databases/{db_name}/tables/{table_name}/aggregate`

**请求体**：

```json
{
    "group_by": ["trade_date", "industry"],
    "aggregations": {
        "stock_count": {"count": "stock_code"},
        "avg_pe": {"avg": "pe_ttm"},
        "total_market_cap": {"sum": "circ_market_cap"},
        "max_close": {"max": "close"},
        "min_close": {"min": "close"}
    },
    "filters": {
        "trade_date": {"gte": "2026-01-01"},
        "pe_ttm": {"gt": 0}
    },
    "sort": [{"field": "trade_date", "order": "desc"}],
    "limit": 100
}
```

**响应示例**：

```json
{
    "success": true,
    "data": [
        {
            "trade_date": "2026-04-03",
            "industry": "机械设备",
            "stock_count": 245,
            "avg_pe": 18.5,
            "total_market_cap": 1234567890123.45,
            "max_close": 255.86,
            "min_close": 2.12
        }
    ]
}
```

---

### 2.2 股票基本信息查询API

#### 2.2.1 获取股票列表

**端点**：`GET /api/v1/ui/stocks`

**请求参数**：

| 参数名 | 类型 | 说明 |
|-------|------|------|
| `market` | string | 市场（SH/SZ/BJ） |
| `industry` | string | 行业分类 |
| `min_market_cap` | float | 最小市值（元） |
| `max_market_cap` | float | 最大市值（元） |
| `list_status` | string | 上市状态（L上市/D退市/P暂停） |
| `limit` | int | 返回条数 |

**响应示例**：

```json
{
    "success": true,
    "data": [
        {
            "stock_code": "603118.SH",
            "stock_name": "万盛股份",
            "market": "SH",
            "industry": "基础化工",
            "list_date": "2014-10-10",
            "total_shares": 457891234,
            "circ_shares": 457891234,
            "is_active": true
        }
    ],
    "pagination": {...}
}
```

---

#### 2.2.2 获取单只股票详情

**端点**：`GET /api/v1/ui/stocks/{stock_code}`

**响应示例**：

```json
{
    "success": true,
    "data": {
        "stock_code": "603118.SH",
        "stock_name": "万盛股份",
        "market": "SH",
        "industry": "基础化工",
        "industry_l2": "塑料",
        "list_date": "2014-10-10",
        "total_shares": 457891234,
        "circ_shares": 457891234,
        "latest_price": 13.25,
        "circ_market_cap": 6067075849.5,
        "pe_ttm": 18.5,
        "pb": 2.3,
        "roe": 12.5,
        "description": "浙江万盛股份有限公司主营业务为...",
        "main_business": ["功能性化学品", "胺助剂", "阻燃剂"]
    }
}
```

---

### 2.3 选股筛选API

#### 2.3.1 按条件筛选股票

**端点**：`POST /api/v1/ui/screening/query`

**请求体**：

```json
{
    "filters": {
        "circ_market_cap": {"between": [2000000000, 30000000000]},
        "pe_ttm": {"between": [0, 25]},
        "pb": {"between": [0, 3]},
        "ma_uptrend": {"eq": true},
        "kdj_j": {"lt": 100},
        "volume_ratio": {"gte": 1.5},
        "trade_date": {"eq": "2026-04-03"}
    },
    "sort": [
        {"field": "total_score", "order": "desc"},
        {"field": "circ_market_cap", "order": "asc"}
    ],
    "fields": [
        "stock_code", "stock_name", "close", "circ_market_cap", 
        "pe_ttm", "pb", "ma5", "ma10", "ma20", "kdj_k", "kdj_d", "kdj_j",
        "macd", "volume_ratio", "roe", "net_profit_growth_yoy"
    ],
    "limit": 100
}
```

**响应示例**：

```json
{
    "success": true,
    "data": [
        {
            "stock_code": "603118.SH",
            "stock_name": "万盛股份",
            "close": 13.25,
            "circ_market_cap": 6067075849.5,
            "pe_ttm": 18.5,
            "pb": 2.3,
            "ma5": 13.19,
            "ma10": 12.75,
            "ma20": 12.67,
            "kdj_k": 75.2,
            "kdj_d": 68.5,
            "kdj_j": 88.6,
            "macd": 0.216,
            "volume_ratio": 1.85,
            "roe": 12.5,
            "net_profit_growth_yoy": 0.156,
            "total_score": 85.5,
            "rank": 1
        }
    ],
    "summary": {
        "total_matched": 156,
        "avg_pe": 15.2,
        "avg_pb": 1.8,
        "avg_roe": 10.5
    }
}
```

---

#### 2.3.2 预设筛选模板

**端点**：`GET /api/v1/ui/screening/templates`

**响应示例**：

```json
{
    "success": true,
    "templates": [
        {
            "template_id": "small_cap_value",
            "template_name": "小市值低估值",
            "description": "流通市值20-300亿，PE<25，PB<3",
            "filters": {
                "circ_market_cap": {"between": [2000000000, 30000000000]},
                "pe_ttm": {"between": [0, 25]},
                "pb": {"between": [0, 3]}
            }
        },
        {
            "template_id": "momentum_breakthrough",
            "template_name": "动量突破",
            "description": "均线多头排列，放量突破，MACD金叉",
            "filters": {
                "ma_uptrend": {"eq": true},
                "volume_ratio": {"gte": 1.5},
                "macd": {"gt": 0}
            }
        },
        {
            "template_id": "quality_growth",
            "template_name": "优质成长",
            "description": "ROE>15%，净利润增长>20%，PE<30",
            "filters": {
                "roe": {"gte": 0.15},
                "net_profit_growth_yoy": {"gte": 0.20},
                "pe_ttm": {"between": [0, 30]}
            }
        }
    ]
}
```

**使用模板筛选**：

**端点**：`POST /api/v1/ui/screening/template/{template_id}`

```json
{
    "trade_date": "2026-04-03",
    "additional_filters": {
        "industry": {"in": ["机械设备", "基础化工", "电子"]}
    },
    "limit": 50
}
```

---

### 2.4 数据导出API

**端点**：`POST /api/v1/ui/databases/{db_name}/tables/{table_name}/export`

**请求体**：

```json
{
    "filters": {...},
    "fields": ["stock_code", "trade_date", "close", "volume"],
    "format": "csv",
    "filename": "stock_data_20260403"
}
```

**响应**：文件下载

---

## 三、数据一致性需求

### 3.1 统一数据源

**问题**：选股扫描与K线数据存在价格差异

**需求**：
1. 选股扫描应使用`kline_data`表的真实交易数据
2. 所有API返回的价格数据应保持一致
3. 提供数据来源说明字段

### 3.2 数据时间戳

**需求**：
1. 所有数据记录应包含`data_timestamp`字段，表示数据的采集/计算时间
2. 提供数据新鲜度检查接口

**端点**：`GET /api/v1/ui/data/freshness`

**响应**：

```json
{
    "success": true,
    "data": {
        "kline_data": {
            "latest_date": "2026-04-03",
            "latest_update": "2026-04-03T17:30:00",
            "record_count": 5841837,
            "delay_minutes": 0
        },
        "stock_daily_factors": {
            "latest_date": "2026-04-03",
            "latest_update": "2026-04-03T18:00:00",
            "record_count": 5841837,
            "delay_minutes": 30
        }
    }
}
```

---

## 四、性能需求

### 4.1 查询性能

| 查询类型 | 响应时间要求 | 说明 |
|---------|-------------|------|
| 单只股票查询 | < 100ms | 包含最新行情和基本面 |
| 批量股票查询（100只） | < 500ms | 包含最新行情和基本面 |
| 全市场筛选 | < 3s | 带复杂筛选条件 |
| 聚合统计 | < 5s | 分组聚合查询 |

### 4.2 并发支持

- 支持至少10个并发查询
- 查询队列管理，避免资源争抢

### 4.3 缓存策略

- 高频查询数据缓存（如最新行情）
- 缓存过期时间可配置
- 提供缓存清理接口

---

## 五、错误处理

### 5.1 错误码规范

| 错误码 | 说明 |
|-------|------|
| 400 | 请求参数错误 |
| 404 | 数据不存在 |
| 422 | 筛选条件无效 |
| 429 | 请求过于频繁 |
| 500 | 服务器内部错误 |

### 5.2 错误响应格式

```json
{
    "success": false,
    "error": {
        "code": 400,
        "message": "无效的股票代码格式",
        "details": "stock_code '603118' 缺少交易所后缀",
        "suggestion": "请使用 '603118.SH' 格式"
    }
}
```

---

## 六、使用示例

### 示例1：筛选小市值低估值股票

```python
import requests

# 方式1：使用预设模板
response = requests.post(
    "http://host.docker.internal:8080/api/v1/ui/screening/template/small_cap_value",
    json={
        "trade_date": "2026-04-03",
        "limit": 50
    }
)
stocks = response.json()["data"]

# 方式2：自定义筛选
response = requests.post(
    "http://host.docker.internal:8080/api/v1/ui/databases/kline/tables/stock_daily_factors/query",
    json={
        "filters": {
            "trade_date": {"eq": "2026-04-03"},
            "circ_market_cap": {"between": [2000000000, 30000000000]},
            "pe_ttm": {"between": [0, 25]},
            "pb": {"between": [0, 3]},
            "is_traded": {"eq": 1}
        },
        "sort": [{"field": "circ_market_cap", "order": "asc"}],
        "limit": 50
    }
)
stocks = response.json()["data"]
```

### 示例2：获取多只股票的最新因子数据

```python
response = requests.post(
    "http://host.docker.internal:8080/api/v1/ui/databases/kline/tables/stock_daily_factors/query",
    json={
        "filters": {
            "stock_code": {"in": ["603118.SH", "002380.SZ", "001360.SZ"]},
            "trade_date": {"eq": "2026-04-03"}
        },
        "fields": [
            "stock_code", "stock_name", "trade_date", "close",
            "circ_market_cap", "pe_ttm", "pb", "ma5", "ma10", "ma20",
            "kdj_k", "kdj_d", "kdj_j", "macd", "volume_ratio", "roe"
        ]
    }
)
```

### 示例3：获取股票历史数据

```python
response = requests.get(
    "http://host.docker.internal:8080/api/v1/ui/databases/kline/tables/kline_data/data",
    params={
        "stock_code": "603118.SH",
        "start_date": "2026-01-01",
        "end_date": "2026-04-03",
        "limit": 1000,
        "sort": "kline_time",
        "order": "asc"
    }
)
```

---

## 七、优先级排序

### P0（必须实现）
1. 数据表查询API - 支持按`stock_code`、`trade_date`筛选
2. 高级筛选功能 - 支持`in`、`between`等操作符
3. 分页功能 - 支持`limit`和`offset`
4. 数据一致性修复 - 选股扫描使用真实K线数据

### P1（重要）
1. 聚合统计API
2. 预设筛选模板
3. 股票基本信息查询
4. 性能优化（索引、缓存）

### P2（可选）
1. 数据导出功能
2. 数据新鲜度检查
3. 查询性能监控
4. WebSocket实时推送

---

## 八、验收标准

### 功能验收
- [ ] 能按股票代码精确查询
- [ ] 能按日期范围查询
- [ ] 能按多条件组合筛选
- [ ] 能分页获取数据
- [ ] 选股扫描与K线数据价格一致

### 性能验收
- [ ] 单只股票查询 < 100ms
- [ ] 批量查询（100只）< 500ms
- [ ] 全市场筛选 < 3s

### 稳定性验收
- [ ] 连续运行24小时无崩溃
- [ ] 并发10个请求正常响应
- [ ] 错误请求返回清晰错误信息

---

**文档结束**

如有疑问，请联系需求方：loui（研究伙计）