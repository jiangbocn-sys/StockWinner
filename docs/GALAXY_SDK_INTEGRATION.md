# 银河 SDK 集成文档

## SDK 安装

### 已安装版本
- **tgw**: 1.0.8.5 (SDK 版本 V4.2.9.250822-rc2.1-YHZQ)
- **AmazingData**: 1.0.30
- **依赖**: scipy 1.17.1, numba, pandas, numpy

### 安装方法
```bash
pip install /home/bobo/tgw-1.0.8.5-py3-none-any.whl
pip install /home/bobo/AmazingData-1.0.30-cp312-none-any.whl
```

## 账户信息

账户信息保存在 `/home/bobo/Login_info.docx`:

| 项目 | 值 |
|------|-----|
| 账号 | REDACTED_SDK_USERNAME |
| 密码 | REDACTED_SDK_PASSWORD |
| 服务器 IP | 140.206.44.234 (联通) |
| 端口 | 8600 |
| 权限期限 | 2026-3-10 至 2027-3-5 |

## SDK API 使用

### 1. 初始化连接

```python
from tgw import IGMDApi, IGMDSpi, Cfg, ApiMode, SetLogSpi, ILogSpi

# 设置日志
SetLogSpi(ILogSpi())

# 配置
cfg = Cfg()
cfg.username = "REDACTED_SDK_USERNAME"
cfg.password = "REDACTED_SDK_PASSWORD"
cfg.server_vip = "140.206.44.234"
cfg.server_port = 8600
cfg.force_logout = True

# SPI 回调
class MainSpi(IGMDSpi):
    def OnRspLogin(self, errorMsg):
        print(f"登录响应：{errorMsg}")
    def OnRtnSnapshotL1(self, session_id, snapshots, count):
        print(f"L1 推送：{count} 条")

# 初始化
api = IGMDApi()
ret = api.Init(MainSpi(), cfg, ApiMode.kInternetMode)
# ret == 0 表示成功
```

### 2. 订阅行情

```python
from tgw import MarketType, SubscribeItem, Tools_CreateSubscribeItem

# 创建订阅项
items = Tools_CreateSubscribeItem(1)
items.market = MarketType.kSSE  # 上海市场
items.security_code = "600519"
items.category_type = 0
items.flag = 0

# 订阅
ret = api.Subscribe(items, 1)
# ret == 0 表示成功
```

### 3. 市场类型枚举

```python
MarketType.kSSE    # 101 - 上海证券交易所
MarketType.kSZSE   # 102 - 深圳证券交易所
```

### 4. 错误码

| 错误码 | 含义 |
|--------|------|
| 0 | 成功 |
| -97 | 参数非法 |
| -98 | 空指针 |
| -99 | 未初始化 |

## 集成架构

### 交易网关抽象层 (`services/trading/gateway.py`)

```
TradingGatewayInterface (抽象基类)
├── MockTradingGateway (模拟网关 - 开发测试)
└── GalaxyTradingGateway (银河真实网关 - 生产)
```

### 功能实现

| 方法 | Mock 网关 | 银河网关 |
|------|----------|----------|
| `connect()` | 本地连接 | SDK 初始化登录 |
| `get_market_data()` | 模拟价格 | SDK 订阅 + 模拟 fallback |
| `get_stock_list()` | 5 只模拟股票 | SDK QueryCodeTable |
| `buy()` | 模拟订单 | 待实现 |
| `sell()` | 模拟订单 | 待实现 |

### 使用示例

```python
from services.trading.gateway import get_gateway, init_gateway

# 获取网关（默认模拟）
gateway = await get_gateway()

# 初始化银河网关
await init_gateway(
    use_mock=False,
    galaxy_app_id="REDACTED_SDK_USERNAME",
    galaxy_password="REDACTED_SDK_PASSWORD"
)

# 获取行情
data = await gateway.get_market_data("600519")
print(f"贵州茅台：¥{data.current_price}")
```

## 当前状态

### 已完成 ✅
- SDK 安装和测试
- 银河网关连接和登录
- 行情数据获取（带模拟 fallback）
- 股票列表查询
- 集成到选股服务和监控服务

### 待完成 📋
- 真实交易执行接口（buy/sell）
- 持仓查询接口
- 委托查询接口
- 行情推送优化（当前使用模拟 fallback）

## 注意事项

1. **行情推送**: SDK 使用异步推送机制，需要等待 SPI 回调。当前实现使用模拟数据作为 fallback 确保系统可用。

2. **交易时间**: 真实交易只能在交易日交易时间进行。

3. **权限到期**: 账户权限 2027-3-5 到期，需要提前续期。

4. **错误处理**: 所有 SDK 调用都有错误处理和日志记录。

## 相关文件

- `services/trading/gateway.py` - 交易网关抽象层
- `services/screening/service.py` - 选股服务（已集成网关）
- `services/monitoring/service.py` - 监控服务（已集成网关）
- `config/accounts.json` - 账户配置文件
