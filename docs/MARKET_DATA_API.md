# 市场行情数据 API 文档

## 概述

本系统使用 **AmazingData SDK** 作为主要行情数据源，提供实时行情查询、批量行情查询、K 线历史数据等 API 接口。

## 数据源说明

### AmazingData SDK（主要数据源）

- **SDK 版本**: AmazingData v1.0.30
- **连接方式**: 互联网模式 (kInternetMode)
- **服务器**: 140.206.44.234:8600
- **数据范围**: A 股实时行情、K 线历史数据

### 银河 SDK（备用数据源）

- **SDK 版本**: tgw v4.2.9
- **状态**: 已集成，但订阅接口存在兼容性问题
- **建议**: 优先使用 AmazingData SDK

## API 接口列表

### 1. 获取单只股票实时行情

**接口**: `GET /api/v1/ui/{account_id}/market/quote/{stock_code}`

**参数**:
| 参数名 | 类型 | 必填 | 说明 |
|--------|------|------|------|
| account_id | path string | 是 | 账户 ID |
| stock_code | path string | 是 | 股票代码（支持格式：600519 或 600519.SH） |

**响应示例**:
```json
{
  "success": true,
  "data": {
    "stock_code": "600519.SH",
    "stock_name": "贵州茅台",
    "current_price": 1453.09,
    "change_percent": 0.00,
    "high": 1479.93,
    "low": 1451.89,
    "open_price": 1468.0,
    "prev_close": 1453.09,
    "volume": 4212397,
    "amount": 6165193597,
    "bid": [1452.5, 1452.0, 1451.5, 1451.0, 1450.5],
    "ask": [1453.5, 1454.0, 1454.5, 1455.0, 1455.5]
  }
}
```

**前端调用示例**:
```javascript
// Vue/React
async function getStockQuote(accountId, stockCode) {
  const response = await fetch(
    `http://localhost:8080/api/v1/ui/${accountId}/market/quote/${stockCode}`
  );
  const result = await response.json();
  return result.data;
}

// 使用示例
const quote = await getStockQuote('bobo', '600519');
console.log(`当前价格：${quote.current_price} 元`);
```

---

### 2. 批量获取股票实时行情

**接口**: `POST /api/v1/ui/{account_id}/market/quotes`

**请求体**:
```json
{
  "stock_codes": ["600519", "000001", "601398"]
}
```

**参数**:
| 参数名 | 类型 | 必填 | 说明 |
|--------|------|------|------|
| account_id | path string | 是 | 账户 ID |
| stock_codes | body array | 是 | 股票代码列表（单次最多 50 只） |

**响应示例**:
```json
{
  "success": true,
  "data": {
    "quotes": [
      {
        "stock_code": "600519",
        "stock_name": "贵州茅台",
        "current_price": 1453.09,
        "change_percent": 0.00,
        "high": 1479.93,
        "low": 1451.89,
        "open_price": 1468.0,
        "prev_close": 1453.09,
        "volume": 4212397,
        "amount": 6165193597
      },
      {
        "stock_code": "000001",
        "stock_name": "平安银行",
        "current_price": 11.14,
        "change_percent": -0.54,
        "high": 11.28,
        "low": 11.09,
        "open_price": 11.20,
        "prev_close": 11.20,
        "volume": 1234567,
        "amount": 13765432
      }
    ],
    "count": 2,
    "failed": [],
    "failed_count": 0
  }
}
```

**前端调用示例**:
```javascript
async function getBatchQuotes(accountId, stockCodes) {
  const response = await fetch(
    `http://localhost:8080/api/v1/ui/${accountId}/market/quotes`,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ stock_codes: stockCodes })
    }
  );
  const result = await response.json();
  return result.data;
}

// 使用示例
const quotes = await getBatchQuotes('bobo', ['600519', '000001', '601398']);
quotes.quotes.forEach(q => {
  console.log(`${q.stock_name}: ${q.current_price}元 (${q.change_percent}%)`);
});
```

---

### 3. 获取 K 线历史数据

**接口**: `GET /api/v1/ui/{account_id}/market/kline`

**参数**:
| 参数名 | 类型 | 必填 | 默认值 | 说明 |
|--------|------|------|--------|------|
| account_id | path string | 是 | - | 账户 ID |
| stock_code | query string | 是 | - | 股票代码 |
| period | query string | 否 | day | K 线周期 |
| start_date | query string | 否 | - | 开始日期 (YYYYMMDD) |
| end_date | query string | 否 | - | 结束日期 (YYYYMMDD) |
| limit | query number | 否 | 100 | 返回数量 (1-1000) |

**支持的 K 线周期**:
| 周期值 | 说明 |
|--------|------|
| 1m | 1 分钟 K 线 |
| 5m | 5 分钟 K 线 |
| 15m | 15 分钟 K 线 |
| 30m | 30 分钟 K 线 |
| 60m | 60 分钟 K 线 |
| day | 日线 |
| week | 周线 |
| month | 月线 |

**响应示例**:
```json
{
  "success": true,
  "data": {
    "stock_code": "600519",
    "period": "day",
    "count": 5,
    "kline": [
      {
        "stock_code": "600519.SH",
        "time": "2026-03-31",
        "open": 1468.0,
        "high": 1479.93,
        "low": 1451.89,
        "close": 1453.09,
        "volume": 4212397,
        "amount": 6165193597
      },
      {
        "stock_code": "600519.SH",
        "time": "2026-03-28",
        "open": 1460.0,
        "high": 1470.0,
        "low": 1455.0,
        "close": 1453.09,
        "volume": 3800000,
        "amount": 5500000000
      }
    ]
  }
}
```

**前端调用示例**:
```javascript
async function getKlineData(accountId, stockCode, period = 'day', limit = 100) {
  const params = new URLSearchParams({
    stock_code: stockCode,
    period: period,
    limit: limit
  });
  
  const response = await fetch(
    `http://localhost:8080/api/v1/ui/${accountId}/market/kline?${params}`
  );
  const result = await response.json();
  return result.data;
}

// 使用示例 - 获取日线数据
const kline = await getKlineData('bobo', '600519', 'day', 30);
console.log(`获取 ${kline.count} 条 K 线数据`);
kline.kline.forEach(day => {
  console.log(`${day.time}: 开${day.open} 收${day.close}`);
});
```

---

### 4. 获取最新 K 线数据

**接口**: `GET /api/v1/ui/{account_id}/market/kline/latest`

**参数**:
| 参数名 | 类型 | 必填 | 默认值 | 说明 |
|--------|------|------|--------|------|
| account_id | path string | 是 | - | 账户 ID |
| stock_code | query string | 是 | - | 股票代码 |
| period | query string | 否 | day | K 线周期 |

**响应示例**:
```json
{
  "success": true,
  "data": {
    "stock_code": "600519",
    "period": "day",
    "kline": {
      "stock_code": "600519.SH",
      "time": "2026-03-31",
      "open": 1468.0,
      "high": 1479.93,
      "low": 1451.89,
      "close": 1453.09,
      "volume": 4212397,
      "amount": 6165193597
    }
  }
}
```

**前端调用示例**:
```javascript
async function getLatestKline(accountId, stockCode, period = 'day') {
  const params = new URLSearchParams({
    stock_code: stockCode,
    period: period
  });
  
  const response = await fetch(
    `http://localhost:8080/api/v1/ui/${accountId}/market/kline/latest?${params}`
  );
  const result = await response.json();
  return result.data;
}

// 使用示例
const latest = await getLatestKline('bobo', '600519', 'day');
console.log(`最新价格：${latest.kline.close} 元`);
```

---

## 错误处理

所有 API 错误统一返回以下格式：

```json
{
  "detail": "错误描述信息"
}
```

**常见错误码**:
| HTTP 状态码 | 说明 |
|-------------|------|
| 200 | 成功 |
| 400 | 参数错误 |
| 404 | 账户不存在或数据不存在 |
| 500 | 服务器内部错误 |

**前端错误处理示例**:
```javascript
try {
  const quote = await getStockQuote('bobo', '600519');
  console.log(quote);
} catch (error) {
  if (error.response?.status === 404) {
    console.error('股票不存在或未找到数据');
  } else if (error.response?.status === 500) {
    console.error('服务器错误，请稍后重试');
  } else {
    console.error('请求失败:', error.message);
  }
}
```

---

## 股票代码格式说明

系统自动处理股票代码格式，支持以下输入格式：
- `600519` → 自动识别为 `600519.SH`
- `000001` → 自动识别为 `000001.SZ`
- `600519.SH` → 直接使用
- `000001.SZ` → 直接使用

**规则**:
- 以 `6` 开头的股票代码自动添加 `.SH` 后缀（上海市场）
- 其他数字开头自动添加 `.SZ` 后缀（深圳市场）

---

## 非交易时间行为

在非交易时间（夜间、周末、节假日）调用行情接口：
- 返回最近一个交易日的有效价格
- 不会报错，但数据可能不是实时价格
- 系统维护期间会返回具体错误信息

---

## 性能建议

1. **批量查询优先**: 查询多只股票时，使用批量接口而非多次单只查询
2. **合理设置 limit**: K 线数据查询根据实际需要设置 limit，避免返回过多数据
3. **缓存策略**: 前端可缓存行情数据，建议缓存时间：
   - 交易时间：30 秒
   - 非交易时间：5 分钟

---

## 技术实现说明

### AmazingData SDK 调用流程

```python
# 1. 登录
from AmazingData import login
token = login(username, password, host, port)

# 2. 获取交易日历
from AmazingData import BaseData
base_data = BaseData()
calendar = base_data.get_calendar()

# 3. 创建 MarketData 对象
from AmazingData import MarketData
md = MarketData(calendar)

# 4. 查询 K 线数据
kline_data = md.query_kline(
    code_list=['600519.SH'],
    begin_date=20260331,
    end_date=20260331,
    period=constant.Period.day.value
)

# 5. 解析结果
if kline_data and '600519.SH' in kline_data:
    df = kline_data['600519.SH']
    latest = df.iloc[-1]
    price = latest['close']
```

### 异步封装

由于 AmazingData SDK 是同步 API，系统使用 `asyncio.to_thread()` 将其封装为异步接口，避免阻塞事件循环：

```python
async def get_market_data(self, stock_code: str):
    # 在线程池中执行同步 SDK 调用
    result = await asyncio.to_thread(
        self._query_market_data_sync, stock_code
    )
    return result
```

---

## 更新日志

- 2026-03-31: 初始版本，基于 AmazingData SDK v1.0.30
- 修复股票代码格式处理逻辑
- 修复时区处理问题（统一使用中国标准时间）
