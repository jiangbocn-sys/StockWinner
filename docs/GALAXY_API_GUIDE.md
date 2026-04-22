# 银河 SDK（AmazingData）API 调用指南

## 概述

本系统使用 **AmazingData SDK v1.0.30** 作为底层行情数据源，通过统一的银河 SDK 接口封装对外提供服务。

## 架构说明

```
┌─────────────────┐
│   前端应用       │
│  (Vue/React)    │
└────────┬────────┘
         │ HTTP API
         ▼
┌─────────────────┐
│  FastAPI 后端    │
│  (services/)    │
└────────┬────────┘
         │ 异步调用
         ▼
┌─────────────────┐
│  交易网关层      │
│  (gateway.py)   │
└────────┬────────┘
         │ asyncio.to_thread()
         ▼
┌─────────────────┐
│ AmazingData SDK │
│  (同步 API)      │
└─────────────────┘
```

## 为什么使用 AmazingData SDK？

银河 SDK (tgw) 的订阅接口存在 API 兼容性问题：
- `SubscribeSnapshot` 方法不存在
- `Subscribe` 方法参数复杂，需要特定的 SPI 设置
- `QuerySnapshot` 需要正确的 API 模式设置

AmazingData SDK 提供了更稳定可靠的接口：
- 简单的 `query_kline()` API
- 返回 pandas DataFrame 格式数据
- 支持异步封装

## 后端实现

### 1. 银河网关类（使用 AmazingData SDK）

```python
class GalaxyTradingGateway(TradingGatewayInterface):
    """银河交易网关（使用 AmazingData SDK 作为底层实现）"""
    
    def __init__(self, app_id: str = "", password: str = ""):
        self.app_id = app_id or "REDACTED_SDK_USERNAME"
        self.password = password or "REDACTED_SDK_PASSWORD"
        self.server_ip = "140.206.44.234"
        self.server_port = 8600
        
        # 导入 AmazingData SDK
        from AmazingData import login, logout, BaseData, MarketData, constant
        self.login = login
        self.logout = logout
        self.BaseData = BaseData
        self.MarketData = MarketData
        self.constant = constant
    
    async def connect(self) -> bool:
        """登录连接"""
        self._token = self.login(
            self.app_id, 
            self.password, 
            self.server_ip, 
            self.server_port
        )
        self.connected = self._token is not None
        return self.connected
    
    async def get_market_data(self, stock_code: str) -> MarketData:
        """获取行情数据（异步接口）"""
        result = await asyncio.to_thread(
            self._query_market_data_sync, stock_code
        )
        return result
    
    def _query_market_data_sync(self, stock_code: str) -> MarketData:
        """同步查询（在线程池中执行）"""
        # 1. 获取交易日历
        base_data = self.BaseData()
        calendar = base_data.get_calendar()
        
        # 2. 创建 MarketData 对象
        md = self.MarketData(calendar)
        
        # 3. 格式化股票代码
        if '.' not in stock_code:
            if stock_code.startswith('6'):
                stock_code = f"{stock_code}.SH"
            else:
                stock_code = f"{stock_code}.SZ"
        
        # 4. 查询 K 线数据
        kline_data = md.query_kline(
            code_list=[stock_code],
            begin_date=20260331,
            end_date=20260331,
            period=self.constant.Period.day.value
        )
        
        # 5. 解析结果
        if kline_data and stock_code in kline_data:
            df = kline_data[stock_code]
            last_row = df.iloc[-1]
            return MarketData(
                stock_code=stock_code,
                stock_name=last_row.get('name', stock_code),
                current_price=float(last_row['close']),
                change_percent=...,
                high=float(last_row['high']),
                low=float(last_row['low']),
                ...
            )
```

### 2. API 路由层

```python
# services/ui/market_data.py
@router.get("/api/v1/ui/{account_id}/market/quote/{stock_code}")
async def get_stock_quote(account_id: str, stock_code: str):
    """获取单只股票实时行情"""
    # 验证账户
    # ...
    
    # 获取网关
    gateway = await get_gateway()
    
    # 获取行情
    market_data = await gateway.get_market_data(stock_code)
    
    return {
        "success": True,
        "data": {
            "stock_code": market_data.stock_code,
            "stock_name": market_data.stock_name,
            "current_price": market_data.current_price,
            "change_percent": market_data.change_percent,
            ...
        }
    }
```

## 前端调用方法

### 方法 1: 原生 Fetch

```javascript
const API_BASE = '/api/v1/ui/8229DE7E/market';

// 获取单只股票行情
async function getQuote(stockCode) {
  const res = await fetch(`${API_BASE}/quote/${stockCode}`);
  const data = await res.json();
  return data.data;
}

// 使用
const quote = await getQuote('600519');
console.log(`当前价格：${quote.current_price}元`);
```

### 方法 2: Vue 3 组合式 API

```vue
<script setup>
import { ref, onMounted } from 'vue';

const quote = ref(null);
const loading = ref(false);

const loadQuote = async (stockCode) => {
  loading.value = true;
  try {
    const res = await fetch(`/api/v1/ui/8229DE7E/market/quote/${stockCode}`);
    const data = await res.json();
    quote.value = data.data;
  } finally {
    loading.value = false;
  }
};

onMounted(() => {
  loadQuote('600519');
});
</script>
```

### 方法 3: React Hooks

```jsx
import { useState, useEffect } from 'react';

function useStockQuote(stockCode) {
  const [quote, setQuote] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    async function fetchQuote() {
      setLoading(true);
      try {
        const res = await fetch(`/api/v1/ui/8229DE7E/market/quote/${stockCode}`);
        const data = await res.json();
        setQuote(data.data);
      } finally {
        setLoading(false);
      }
    }
    fetchQuote();
  }, [stockCode]);

  return { quote, loading };
}

// 使用
function QuoteCard() {
  const { quote, loading } = useStockQuote('600519');
  
  if (loading) return <div>加载中...</div>;
  if (!quote) return <div>加载失败</div>;
  
  return (
    <div>
      <h3>{quote.stock_name}</h3>
      <p className={quote.change_percent >= 0 ? 'up' : 'down'}>
        {quote.current_price} ({quote.change_percent}%)
      </p>
    </div>
  );
}
```

## API 接口列表

### 1. 单只股票行情
```
GET /api/v1/ui/{account_id}/market/quote/{stock_code}
```

### 2. 批量行情
```
POST /api/v1/ui/{account_id}/market/quotes
Body: { "stock_codes": ["600519", "000001"] }
```

### 3. K 线历史数据
```
GET /api/v1/ui/{account_id}/market/kline?stock_code=600519&period=day&limit=100
```

### 4. 最新 K 线
```
GET /api/v1/ui/{account_id}/market/kline/latest?stock_code=600519&period=day
```

## 股票代码格式

系统自动处理股票代码格式：
- `600519` → `600519.SH` (上海市场)
- `000001` → `000001.SZ` (深圳市场)
- `600519.SH` → 直接使用

## 错误处理

所有 API 错误统一返回格式：
```json
{
  "detail": "错误描述信息"
}
```

前端处理示例：
```javascript
try {
  const quote = await getQuote('600519');
} catch (error) {
  if (error.response?.status === 404) {
    alert('股票不存在或数据不存在');
  } else if (error.response?.status === 500) {
    alert('服务器错误，请稍后重试');
  } else {
    alert(`获取失败：${error.message}`);
  }
}
```

## 非交易时间行为

- 返回最近一个交易日的有效价格
- 不会报错，但数据可能不是实时价格
- 系统维护期间返回具体错误信息

## 性能建议

1. **批量查询**: 多只股票使用批量接口
2. **缓存策略**: 
   - 交易时间缓存 30 秒
   - 非交易时间缓存 5 分钟
3. **限流**: 单用户每秒不超过 10 次请求

## 相关文档

- [MARKET_DATA_API.md](./MARKET_DATA_API.md) - 完整 API 文档
- [frontend_api_example.js](./frontend_api_example.js) - JavaScript 调用示例
- [MarketQuoteCard.vue](./MarketQuoteCard.vue) - Vue 3 组件示例

## 更新日期

2026-03-31
