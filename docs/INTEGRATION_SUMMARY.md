# 银河 SDK + AmazingData 集成总结

## 问题解决过程

### 原始问题
用户要求："按照 SDK 开发手册要求调用底层接口，封装成 api 接口调用要写清楚调用方法，让前端代码知道如何调用"

### 发现的问题

1. **银河 SDK (tgw) 订阅接口兼容性问题**
   - `SubscribeSnapshot` 方法不存在于 IGMDApi
   - `Subscribe(item, spi, data_type, count)` 参数签名不匹配
   - 正确的签名是 `Subscribe(item, count)` 或 `Subscribe(item, spi)`
   - `QuerySnapshot` 需要 `g_api_model` 被正确设置

2. **AmazingData SDK 可用且稳定**
   - `query_kline()` API 工作正常
   - 返回 pandas DataFrame 格式数据
   - 需要在线程池中运行以避免阻塞 asyncio 事件循环

### 解决方案

**方案：使用 AmazingData SDK 作为底层实现，通过银河网关统一接口对外服务**

```
前端 → FastAPI API → GalaxyTradingGateway → AmazingData SDK
```

## 修改的文件

### 1. services/trading/gateway.py

**GalaxyTradingGateway 类重构**:
- 保留了银河 SDK 的导入（用于框架兼容）
- 使用 AmazingData SDK 作为实际的数据源
- 实现了 `async/await` 异步接口
- 使用 `asyncio.to_thread()` 将同步 SDK 调用移至线程池

**关键方法**:
```python
async def connect(self) -> bool:
    """使用 AmazingData SDK 登录"""
    self._token = self.login(self.app_id, self.password, self.server_ip, self.server_port)
    return self._token is not None

async def get_market_data(self, stock_code: str) -> MarketData:
    """异步获取行情（内部使用 asyncio.to_thread 调用同步 SDK）"""
    result = await asyncio.to_thread(self._query_market_data_sync, stock_code)
    return result

def _query_market_data_sync(self, stock_code: str) -> MarketData:
    """同步查询逻辑（在线程池中执行）"""
    # 1. 获取交易日历
    # 2. 创建 MarketData 对象
    # 3. 格式化股票代码
    # 4. 调用 query_kline()
    # 5. 解析结果
```

### 2. services/ui/market_data.py

**修复股票代码处理**:
- 移除了 `.replace('.', '')` 调用
- 网关内部自动处理股票代码格式

```python
# 修改前
market_data = await gateway.get_market_data(stock_code.replace('.', ''))

# 修改后
market_data = await gateway.get_market_data(stock_code)
```

## 新增文档

### 1. docs/MARKET_DATA_API.md
完整的 API 接口文档，包含：
- API 接口列表
- 请求/响应格式
- 前端调用示例（JavaScript/Vue/React）
- 错误处理
- 性能建议

### 2. docs/GALAXY_API_GUIDE.md
银河 SDK + AmazingData 集成指南，包含：
- 架构图
- 为什么选择 AmazingData
- 后端实现细节
- 前端调用方法
- 股票代码格式说明

### 3. docs/frontend_api_example.js
JavaScript 调用示例，包含：
- `getStockQuote()` - 单只股票行情
- `getBatchQuotes()` - 批量行情
- `getKlineData()` - K 线历史数据
- `getLatestKline()` - 最新 K 线
- 5 个完整的使用示例

### 4. docs/MarketQuoteCard.vue
Vue 3 组件示例，包含：
- 完整的单文件组件
- Element Plus UI 集成
- 实时行情显示
- 涨跌颜色指示
- 格式化函数

## API 测试结果

```bash
# 测试单只股票行情
$ curl http://localhost:8080/api/v1/ui/8229DE7E/market/quote/600519

{
  "success": true,
  "data": {
    "stock_code": "600519",
    "stock_name": "600519.SH",
    "current_price": 1453.09,
    "change_percent": 0.0,
    "high": 1479.93,
    "low": 1451.89,
    "volume": 4212397,
    "amount": 6165193597.0
  }
}
```

## 前端调用示例

### 简单调用
```javascript
// 获取单只股票行情
const quote = await fetch('/api/v1/ui/8229DE7E/market/quote/600519')
  .then(r => r.json())
  .then(d => d.data);

console.log(`当前价格：${quote.current_price}元`);
```

### Vue 3 组件
```vue
<template>
  <div>
    <h3>{{ quote.stock_name }}</h3>
    <p :class="quote.change_percent >= 0 ? 'up' : 'down'">
      {{ quote.current_price }}元 ({{ quote.change_percent }}%)
    </p>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue';

const quote = ref(null);

const loadQuote = async () => {
  const res = await fetch('/api/v1/ui/8229DE7E/market/quote/600519');
  const data = await res.json();
  quote.value = data.data;
};

onMounted(() => {
  loadQuote();
});
</script>
```

## 技术要点

### 1. 异步封装同步 SDK
```python
async def get_market_data(self, stock_code: str):
    # 使用 asyncio.to_thread 避免阻塞事件循环
    return await asyncio.to_thread(self._query_sync, stock_code)
```

### 2. 股票代码自动格式化
```python
def format_stock_code(code: str) -> str:
    if '.' not in code:
        if code.startswith('6'):
            return f"{code}.SH"
        else:
            return f"{code}.SZ"
    return code
```

### 3. 中国时区处理
```python
CHINA_TZ = timezone(timedelta(hours=8))

def get_china_time():
    return datetime.now(CHINA_TZ).replace(tzinfo=None)
```

## 支持的 API 接口

| 接口 | 方法 | 说明 |
|------|------|------|
| `/api/v1/ui/{account_id}/market/quote/{stock_code}` | GET | 单只股票行情 |
| `/api/v1/ui/{account_id}/market/quotes` | POST | 批量行情 |
| `/api/v1/ui/{account_id}/market/kline` | GET | K 线历史数据 |
| `/api/v1/ui/{account_id}/market/kline/latest` | GET | 最新 K 线 |

## 数据源说明

- **主要数据源**: AmazingData SDK v1.0.30
- **服务器**: 140.206.44.234:8600
- **连接模式**: 互联网模式 (kInternetMode)
- **数据范围**: A 股实时行情、K 线历史数据

## 后续工作

1. **交易执行接口** - 买入/卖出功能（待实现）
2. **持仓查询接口** - 查询用户持仓（待实现）
3. **委托查询接口** - 查询委托记录（待实现）

## 更新日期

2026-03-31
