---
name: 2026-04-09 下载稳定性修复
description: 修复 SDK 连接数超限和 BJ 股票导致下载中断问题
type: project
---

**日期：** 2026-04-09

## 问题背景

用户报告下载数据时遇到两个主要问题：
1. **SDK 连接数超限** - "Connections of this user exceed the max limitation"
2. **BJ 股票导致下载中断** - 批次失败率过高（实际是 BJ 股票无数据）

## 根本原因分析

### 问题 1：SDK 连接数超限
**原因：** `GalaxyTradingGateway` 的每次查询方法都创建新的 `BaseData()` 和 `MarketData()` 实例，导致 TGW 连接快速累积并超限。

**代码位置：** `services/trading/gateway.py`
- `_query_batch_kline_data_sync` (第 555-557 行)
- `_query_kline_data_sync` (第 664-666 行)
- `_query_market_data_sync` (第 274-278 行)
- `_query_stock_list_sync` (第 392-394 行)

### 问题 2：BJ 股票导致下载中断
**原因：** 所有股票混合在一起下载，BJ 股票（约 300 只）大量返回"无数据"，触发 50% 失败率阈值，导致整个下载中止。

## 解决方案

### 1. SDK 连接缓存

在 `GalaxyTradingGateway` 和 `AmazingDataTradingGateway` 中添加单例缓存：

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

**修改的方法：**
- `_query_batch_kline_data_sync` → 改用 `self._get_market_data()`
- `_query_kline_data_sync` → 改用 `self._get_market_data()`
- `_query_market_data_sync` → 改用 `self._get_market_data()`
- `_query_stock_list_sync` → 改用 `self._get_market_data()`

### 2. 按市场分批下载

修改 `services/data/local_data_service.py` 的 `download_all_kline_data` 函数：

1. **股票分类：**
   - SH 股票 → `sh_stocks`
   - SZ 股票 → `sz_stocks`
   - BJ 股票 → `bj_stocks`
   - 其他 → `other_stocks`

2. **下载顺序：**
   - 第一批：SH/SZ（约 5000 只）
   - 第二批：BJ（约 300 只，可能无数据）
   - 第三批：其他

3. **失败率独立统计：**
   - BJ 市场：只关注下载错误，无数据视为正常
   - SH/SZ 市场：下载错误率超过 50% 才中止

### 3. 字段名统一验证

验证了 `kline_time → trade_date` 字段统一：
- 数据库表结构：✅ 使用 `trade_date`
- 后端 API：✅ 返回 `trade_date`
- 前端代码：✅ 使用 `trade_date`
- SDK 字段重命名：✅ gateway.py 中有重命名逻辑

### 4. 进度跟踪器验证

验证了下载进度跟踪器完整集成：
- 下载阶段：✅ 显示批次进度
- 因子计算阶段：✅ 状态切换为 `CALCULATING_FACTORS`
- 完成状态：✅ 标记为 `COMPLETED`

## 验证结果

**下载前数据库状态：**
- 记录数：5,852,349
- 股票数：5,390
- 最新日期：2026-04-08

**下载中（约 10 秒后）：**
- 记录数：5,852,358
- 股票数：5,399
- 增长：+9 条记录，+9 只股票

**结论：** 下载功能正常工作，数据持续增长。

## 修改文件列表

1. `services/trading/gateway.py`
   - 添加缓存机制到 `GalaxyTradingGateway`
   - 修改 4 个查询方法使用缓存实例

2. `services/data/local_data_service.py`
   - 按市场分类股票
   - 分市场批次下载
   - 独立失败率统计

3. `services/ui/data_explorer.py`
   - 确认使用 `trade_date` 字段（无需修改）

## 参考文档

- [SDK 连接缓存优化](sdk_connection_cache.md)
- [AmazingData SDK Handbook](docs/AmazingData_SDK_handbook.pdf) - 4.4 本地数据缓存方案

## 后续建议

1. **监控 SDK 连接数** - 观察是否还有连接超限错误
2. **BJ 股票处理** - 考虑是否需要单独的 BJ 股票下载策略
3. **进度显示优化** - 前端弹窗显示分市场进度
