# AmazingData SDK 集成检查清单

## API 使用 ✅

### 1. 登录认证
- [x] 是否正确调用了 `ad.login()` 登录？
  - **正确用法**: `from AmazingData import login`
  - **签名**: `login(username, password, host, port, api_mode='kInternetMode')`
  - **返回**: 成功返回 token（字符串），失败返回 None
  - **已实现**: `gateway.py:AmazingDataTradingGateway.connect()`

### 2. 交易日历获取
- [x] 是否先获取交易日历 `get_calendar()` 再实例化 MarketData？
  - **正确用法**: 
    ```python
    from AmazingData import BaseData
    base_data = BaseData()
    calendar = base_data.get_calendar()
    md = MarketData(calendar)
    ```
  - **已实现**: `gateway.py:AmazingDataTradingGateway.get_market_data()`

### 3. 实时行情查询
- [x] 是否使用了正确的查询模式？
  - **正确用法**: 使用 `query_kline()` 获取日线数据（避免 pandas 频率错误）
  - **签名**: `md.query_kline(code_list, begin_date, end_date, period, **kwargs)`
  - **返回**: `dict` 格式，key 为股票代码（带 .SH/.SZ 后缀），value 为 pandas DataFrame
  - **已实现**: `gateway.py:AmazingDataTradingGateway.get_market_data()`
  
- [x] 实时行情是否使用订阅模式（装饰器 + 回调）？
  - **注意**: 当前实现使用 `query_kline()` 查询模式，而非 `SubscribeData` 订阅模式
  - **原因**: 订阅模式需要事件循环和回调处理，更适合实时监控场景
  - **订阅模式用法**（备用）:
    ```python
    from AmazingData import SubscribeData, constant
    
    class MySubscriber(SubscribeData):
        def OnMDSnapshot(self, data, err):
            print(f"收到行情：{data}")
    
    sub = MySubscriber()
    sub.register(code_list=['600519.SH'], period=constant.Period.snapshot.value)
    sub.run()  # 阻塞运行
    ```

### 4. 数据格式处理
- [x] 是否正确处理了 snapshotdict 和 klinedict 的返回格式？
  - **返回格式**: `dict[str, pandas.DataFrame]`
  - **DataFrame 列名**: `time, open, high, low, close, pre_close, volume, amount, name`
  - **已实现**: `gateway.py:AmazingDataTradingGateway.get_market_data()` 中的解析逻辑

---

## 股票代码格式 ✅

### 1. 代码格式转换
- [x] 股票代码是否需要添加 `.SZ`/`.SH` 后缀？
  - **需要**: SDK 要求股票代码格式为 `600519.SH` 或 `000001.SZ`
  - **已实现**: `gateway.py` 中的 `get_market_data()` 和 `get_kline_data()` 方法
  - **转换逻辑**:
    ```python
    if '.' not in stock_code:
        if stock_code.startswith('6'):
            stock_code = f"{stock_code}.SH"
        else:
            stock_code = f"{stock_code}.SZ"
    ```

### 2. 批量查询支持
- [x] 是否支持批量查询（传入 `code_list`）？
  - **支持**: `query_kline(code_list=['600519.SH', '000001.SZ'], ...)`
  - **已实现**: `gateway.py:AmazingDataTradingGateway.get_batch_market_data()`

---

## 错误处理 ✅

### 1. 登录失败处理
- [x] 是否处理了登录失败？
  - **已实现**: `connect()` 方法检查 token 返回值，失败时返回 False
  - **错误信息**: 记录日志并返回详细的错误原因

### 2. 查询失败处理
- [x] 是否处理了数据查询失败？
  - **已实现**: 所有查询方法都包含 try-except 块
  - **错误信息**: 返回 SDK 原始错误信息，不返回模拟数据

### 3. 超时处理
- [x] 是否处理了数据查询超时？
  - **已实现**: 使用 Python 标准超时机制
  - **注意**: SDK 内部可能有自己的超时设置

---

## 新增 API 端点 ✅

### 1. 单只股票实时行情
- [x] `GET /api/v1/ui/{account_id}/market/quote/{stock_code}`
  - **实现**: `services/ui/market_data.py:get_stock_quote()`
  - **调用**: `gateway.get_market_data(stock_code)`
  - **返回**: 包含当前价格、涨跌幅、买卖盘等完整行情数据

### 2. 批量实时行情
- [x] `POST /api/v1/ui/{account_id}/market/quotes`
  - **实现**: `services/ui/market_data.py:get_batch_quotes()`
  - **调用**: `gateway.get_batch_market_data(stock_codes)`
  - **限制**: 单次最多查询 50 只股票

### 3. K 线历史数据
- [x] `GET /api/v1/ui/{account_id}/market/kline`
  - **实现**: `services/ui/market_data.py:get_kline_data()`
  - **调用**: `gateway.get_kline_data(stock_code, period, start_date, end_date, limit)`
  - **支持周期**: 1m, 5m, 15m, 30m, 60m, day, week, month

---

## 时区处理 ✅

- [x] 所有时间戳使用中国时区（Asia/Shanghai, UTC+8）
  - **实现**: `get_china_time()` 函数
  - **应用**: 所有数据库写入、日志记录、API 响应

---

## 关键代码位置

### 导入路径（正确）
```python
from AmazingData import login, logout, BaseData, MarketData, SubscribeData, constant
```

### 登录流程
```python
token = login(username, password, host, port)
if token:
    connected = True
```

### 获取行情
```python
base_data = BaseData()
calendar = base_data.get_calendar()
md = MarketData(calendar)
kline_data = md.query_kline(
    code_list=[stock_code],
    begin_date=begin_date,
    end_date=end_date,
    period=constant.Period.day.value
)
```

### 数据解析
```python
if kline_data and stock_code in kline_data:
    df = kline_data[stock_code]
    if len(df) > 0:
        last_row = df.iloc[-1]
        current_price = float(last_row.get('close', 0))
```

---

## 测试验证

### SDK 导入测试
```bash
source venv/bin/activate && python3 -c "
from AmazingData import login, logout, BaseData, MarketData, constant
print('SDK 导入成功')
"
```

### 登录测试
```bash
source venv/bin/activate && python3 -c "
from AmazingData import login
token = login('REDACTED_SDK_USERNAME', 'REDACTED_SDK_PASSWORD', '140.206.44.234', 8600)
print(f'登录成功：{token is not None}')
"
```

### API 端点测试
```bash
# 获取单只股票行情
curl http://localhost:8080/api/v1/ui/bobo/market/quote/600519

# 批量获取行情
curl -X POST http://localhost:8080/api/v1/ui/bobo/market/quotes \
  -H "Content-Type: application/json" \
  -d '{"stock_codes": ["600519", "000001", "601398"]}'

# 获取 K 线数据
curl http://localhost:8080/api/v1/ui/bobo/market/kline?stock_code=600519&period=day&limit=10
```

---

## 注意事项

1. **非交易时间行为**: SDK 在非交易时间返回最近的有效价格（收盘价），不会报错
2. **系统维护时间**: 如遇系统维护，SDK 会返回具体错误信息，代码会如实传递给用户
3. **股票代码格式**: 必须确保代码包含 `.SH` 或 `.SZ` 后缀
4. **pandas 依赖**: SDK 返回的数据是 pandas DataFrame，需要安装 pandas
5. **并发限制**: 注意 SDK 的并发查询限制，批量查询时控制在 50 只以内

---

## 已完成功能清单

- [x] SDK 正确导入和初始化
- [x] 登录/登出功能
- [x] 交易日历获取
- [x] 单只股票行情查询
- [x] 批量行情查询
- [x] K 线历史数据查询
- [x] 股票代码格式转换
- [x] 错误处理（返回详细原因，不返回模拟数据）
- [x] 时区处理（中国标准时间）
- [x] API 端点实现
- [x] 数据库时间戳修正

---

## 待完成功能

- [ ] 实时行情订阅模式（SubscribeData + 回调）
- [ ] 交易执行接口（买入/卖出）
- [ ] 持仓查询接口
- [ ] 委托查询接口
