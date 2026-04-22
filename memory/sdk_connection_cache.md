---
name: SDK 连接缓存优化
description: 在 gateway.py 中添加 BaseData 和 MarketData 单例缓存，防止因重复创建实例导致连接数超限
type: feedback
---

**日期：** 2026-04-09

## 问题

SDK 连接数超限错误："Connections of this user exceed the max limitation"

**根本原因：** GalaxyTradingGateway 和 AmazingDataTradingGateway 每次调用 `_query_batch_kline_data_sync`、`_query_kline_data_sync`、`_query_market_data_sync` 时都创建新的 `BaseData()` 和 `MarketData()` 实例，导致 TGW 连接数快速累积并超限。

## 解决方案

在两个网关类中添加 SDK 实例缓存机制：

```python
# __init__ 中初始化
self._base_data = None
self._market_data = None

# 单例访问方法
def _get_base_data(self):
    if self._base_data is None:
        self._base_data = self.BaseData()
    return self._base_data

def _get_market_data(self):
    if self._market_data is None:
        bd = self._get_base_data()
        calendar = bd.get_calendar()
        self._market_data = self.MarketData(calendar)
    return self._market_data

def _clear_cache(self):
    self._base_data = None
    self._market_data = None
```

## 修改的方法

**GalaxyTradingGateway:**
- `_query_batch_kline_data_sync` - 改用 `self._get_market_data()`
- `_query_kline_data_sync` - 改用 `self._get_market_data()`
- `_query_market_data_sync` - 改用 `self._get_market_data()`
- `_query_stock_list_sync` - 改用 `self._get_market_data()`

**AmazingDataTradingGateway:**
- 所有方法已使用缓存（之前已实现）

## 为什么有效

Handbook 说明：
- 登录只需要一次（`login()`）
- 一个 `MarketData` 实例可以查询多个股票
- 问题是我们之前每次查询都创建新实例，每个实例都建立独立的 TGW 连接

缓存后：
- 每个网关实例只创建一次 `BaseData` 和 `MarketData`
- 所有查询复用同一个 `MarketData` 实例
- 连接数大幅降低，不再触发限制

## 配套优化

配合 `local_data_service.py` 的按市场分批下载：
1. SH/SZ 股票优先下载（约 5000 只）
2. BJ 股票单独批次（约 300 只，可能无数据）
3. 其他市场最后处理

BJ 市场允许更高的无数据率，不会因无数据股票多而触发失败阈值。
