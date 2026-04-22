# get_code_info 接口修复文档

## 问题描述

之前代码没有正确使用 AmazingData SDK 的 `get_code_info` 接口获取股票名称，而是试图从数据库表（stock_monthly_factors 或 kline_data）中交叉引用获取股票名称。

根据 SDK handbook 3.5.2.1 节说明：
- **函数接口**: `get_code_info`
- **功能描述**: 获取每日最新证券信息，交易日早上 9 点前更新当日最新
- **输入参数**: `security_type` (str, 可选) - 默认 'EXTRA_STOCK_A'
- **返回值**: DataFrame，index 为股票代码，包含以下列：
  - `symbol`: 证券简称（股票名称）
  - `security_status`: 产品状态标志
  - `pre_close`: 昨收价
  - `high_limited`: 涨停价
  - `low_limited`: 跌停价
  - `price_tick`: 最小价格变动单位
  - `list_day`: 上市日期

## 修复内容

### 1. SDK Manager (`services/common/sdk_manager.py`)

新增两个方法：

```python
def get_code_info(self, security_type: str = 'EXTRA_STOCK_A') -> pd.DataFrame:
    """获取每日最新证券信息（包含股票名称、昨收价、涨跌停价等）"""
    self._ensure_login()
    try:
        from AmazingData import BaseData
        base_data = BaseData()
        result = base_data.get_code_info(security_type=security_type)
        if isinstance(result, pd.DataFrame):
            return result
        return pd.DataFrame()
    except Exception as e:
        print(f"[SDK] 获取证券信息失败：{e}")
        return pd.DataFrame()

def get_code_list(self, security_type: str = 'EXTRA_STOCK_A') -> list:
    """获取每日最新代码表"""
    self._ensure_login()
    try:
        from AmazingData import BaseData
        base_data = BaseData()
        result = base_data.get_code_list(security_type=security_type)
        if isinstance(result, list):
            return result
        return []
    except Exception as e:
        print(f"[SDK] 获取代码列表失败：{e}")
        return []
```

### 2. Trading Gateway (`services/trading/gateway.py`)

更新 `_query_stock_list_sync()` 方法：

```python
def _query_stock_list_sync(self) -> List[Dict[str, str]]:
    """同步查询股票列表（在线程池中执行）"""
    from services.common.sdk_manager import get_sdk_manager

    sdk_mgr = get_sdk_manager()
    stock_list = []

    try:
        # 使用 get_code_info 获取股票信息，返回 DataFrame
        code_info = sdk_mgr.get_code_info(security_type='EXTRA_STOCK_A')

        if code_info is not None and len(code_info) > 0:
            # 遍历 DataFrame，提取股票代码和名称
            for idx, row in code_info.iterrows():
                code = str(idx)  # index 是股票代码
                symbol = row.get('symbol', '')  # symbol 是证券简称
                security_status = row.get('security_status', '')  # 产品状态标志

                # 代码格式转换
                if '.' in code:
                    code_without_suffix = code.split('.')[0]
                    market = code.split('.')[1]
                else:
                    code_without_suffix = code
                    market = "SH" if code.startswith('6') else "SZ"

                stock_list.append({
                    "code": code_without_suffix,
                    "name": symbol if symbol else code_without_suffix,
                    "market": market,
                    "status": security_status
                })

            return stock_list

    except Exception as e:
        logger.warning(f"使用 get_code_info 失败：{e}, 尝试使用 get_code_list")

    # 备用方案...
```

### 3. Local Data Service (`services/data/local_data_service.py`)

更新 `save_kline_data_batch()` 方法：

```python
def save_kline_data_batch(self, kline_batch: List[Tuple[str, str, pd.DataFrame]]) -> int:
    """批量保存多只股票的 K 线数据到数据库"""
    # ...

    # 提前通过 AmazingData SDK 的 get_code_info 获取所有股票名称缓存
    from services.common.sdk_manager import get_sdk_manager
    sdk_mgr = get_sdk_manager()
    stock_names_cache = {}

    try:
        code_info = sdk_mgr.get_code_info(security_type='EXTRA_STOCK_A')
        if code_info is not None:
            for idx, row in code_info.iterrows():
                stock_names_cache[idx] = row.get('symbol', idx)
    except Exception as e:
        print(f"[LocalData] 获取股票名称失败：{e}，使用传入的股票名称")

    for stock_code, stock_name, df in kline_batch:
        if df is None or len(df) == 0:
            continue

        # 如果股票名称为空或等于股票代码，从缓存获取
        if not stock_name or stock_name == stock_code:
            stock_name = stock_names_cache.get(stock_code, stock_code)

        # ... 保存数据
```

## 数据验证

```python
from services.common.sdk_manager import get_sdk_manager

sdk_mgr = get_sdk_manager()
code_info = sdk_mgr.get_code_info(security_type='EXTRA_STOCK_A')

# 返回结果:
# - 类型：pandas.DataFrame
# - 行数：10563
# - 列名：['symbol', 'security_status', 'pre_close', 'high_limited', 'low_limited', 'price_tick', 'list_day']
# - Index 名称：['code_market']
# - Index 格式：'300334.SZ', '600519.SH' 等
```

## 使用方式

### 在代码中调用

```python
from services.common.sdk_manager import get_sdk_manager

sdk_mgr = get_sdk_manager()

# 获取股票信息（包含名称）
code_info = sdk_mgr.get_code_info(security_type='EXTRA_STOCK_A')

# 获取代码列表
code_list = sdk_mgr.get_code_list(security_type='EXTRA_STOCK_A')
```

### CLI 测试

```bash
source venv/bin/activate
python3 -c "
from services.common.sdk_manager import get_sdk_manager
sdk_mgr = get_sdk_manager()
code_info = sdk_mgr.get_code_info(security_type='EXTRA_STOCK_A')
print(f'获取到 {len(code_info)} 条股票信息')
print(code_info.head())
"
```

## 修改日期

2026-04-08
