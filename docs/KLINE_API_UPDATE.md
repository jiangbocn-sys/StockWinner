# K 线数据 API 增强 - v6.2.4 更新报告

**更新日期**: 2026-04-07  
**版本**: v6.2.4

---

## 更新概述

本次更新为 8080 端口的 API 服务增加了获取不同周期和时间长度 K 线数据的功能，通过在现有接口上增加参数的方式实现，并同步更新了前端调用时的参数传递。

---

## 后端变更

### 1. market_data.py

**文件**: `services/ui/market_data.py`

#### 变更 1: GET /api/v1/ui/{account_id}/market/kline

**新增参数**:
- `period`: 支持周期从 8 种扩展到 11 种
  - 新增：`3m`, `10m`, `120m`
  - 完整列表：`1m`, `3m`, `5m`, `10m`, `15m`, `30m`, `60m`, `120m`, `day`, `week`, `month`
- `time_range`: 快捷时间范围选择
  - 可选值：`7d`, `30d`, `90d`, `180d`, `1y`, `2y`, `5y`, `10y`, `all`, `custom`
- `limit`: 上限从 1000 提升到 10000

**参数优先级**:
```
start_date/end_date > time_range > limit
```

**响应增强**:
- 新增 `start_date` 字段
- 新增 `end_date` 字段

**代码变更**:
```python
# 新增时间范围处理逻辑
if time_range and not start_date and not end_date:
    end_dt = datetime.now()
    if time_range == "7d":
        start_dt = end_dt - timedelta(days=7)
    elif time_range == "1y":
        start_dt = end_dt - timedelta(days=365)
    # ... 其他范围
    actual_start_date = start_dt.strftime("%Y%m%d")
    actual_end_date = end_dt.strftime("%Y%m%d")
```

#### 变更 2: GET /api/v1/ui/{account_id}/market/kline/latest

- 支持周期扩展到 11 种

### 2. gateway.py

**文件**: `services/trading/gateway.py`

#### 周期映射更新

```python
period_map = {
    "1m": self.constant.Period.min1.value,      # 10000
    "3m": self.constant.Period.min3.value,      # 10001 (新增)
    "5m": self.constant.Period.min5.value,      # 10002
    "10m": self.constant.Period.min10.value,    # 10003 (新增)
    "15m": self.constant.Period.min15.value,    # 10004
    "30m": self.constant.Period.min30.value,    # 10005
    "60m": self.constant.Period.min60.value,    # 10006
    "120m": self.constant.Period.min120.value,  # 10007 (新增)
    "day": self.constant.Period.day.value,      # 10008
    "week": self.constant.Period.week.value,    # 10009
    "month": self.constant.Period.month.value,  # 10010
}
```

#### 日期计算逻辑更新

```python
# 根据 limit 和 period 推算开始日期
if period in ["1m", "3m", "5m", "10m", "15m", "30m", "60m", "120m"]:
    start_dt = end_dt - dt.timedelta(days=limit // 240)
elif period == "day":
    start_dt = end_dt - dt.timedelta(days=limit)
elif period == "week":
    start_dt = end_dt - dt.timedelta(weeks=limit)
elif period == "month":
    start_dt = end_dt - dt.timedelta(days=limit * 30)
```

---

## 前端变更

### DataExplorer.vue

**文件**: `frontend/src/views/DataExplorer.vue`

#### 新增 UI 组件

1. **K 线查询卡片**:
   - 股票代码输入框
   - 周期选择器（11 种周期）
   - 时间范围选择器（7d/30d/90d/180d/1y/2y/5y/10y/all/custom）
   - 自定义日期范围选择器（条件显示）
   - 数量限制输入框
   - 查询按钮

2. **K 线结果表格**:
   - 显示时间、开盘、最高、最低、收盘、成交量、成交额
   - 智能格式化：成交量/成交额以"万"、"亿"单位显示
   - CSV 导出按钮

#### 新增状态变量

```javascript
const klineData = ref([])
const klineResultInfo = reactive({
  stock_code: '',
  period: '',
  count: 0
})

const klineParams = reactive({
  stock_code: '',
  period: 'day',
  time_range: '90d',
  start_date: '',
  end_date: '',
  limit: 500
})
```

#### 新增函数

1. **onTimeRangeChange()**: 时间范围切换处理
2. **loadKlineData()**: 加载 K 线数据
3. **formatVolume()**: 成交量格式化
4. **formatAmount()**: 成交额格式化
5. **exportKlineToCSV()**: CSV 导出

---

## API 使用示例

### 示例 1: 获取日线 K 线（最近 7 天）

```bash
curl "http://localhost:8080/api/v1/ui/8229DE7E/market/kline?stock_code=600519.SH&period=day&time_range=7d"
```

**响应**:
```json
{
  "success": true,
  "data": {
    "stock_code": "600519.SH",
    "period": "day",
    "count": 63,
    "start_date": null,
    "end_date": null,
    "kline": [...]
  }
}
```

### 示例 2: 获取 60 分钟 K 线（最近 30 天）

```bash
curl "http://localhost:8080/api/v1/ui/8229DE7E/market/kline?stock_code=600519.SH&period=60m&time_range=30d"
```

**响应**:
```json
{
  "success": true,
  "data": {
    "stock_code": "600519.SH",
    "period": "60m",
    "count": 3,
    "kline": [...]
  }
}
```

### 示例 3: 获取周线 K 线（最近 2 年）

```bash
curl "http://localhost:8080/api/v1/ui/8229DE7E/market/kline?stock_code=600519.SH&period=week&time_range=2y"
```

**响应**:
```json
{
  "success": true,
  "data": {
    "stock_code": "600519.SH",
    "period": "week",
    "count": 100,
    "kline": [...]
  }
}
```

### 示例 4: 自定义日期范围

```bash
curl "http://localhost:8080/api/v1/ui/8229DE7E/market/kline?stock_code=600519.SH&period=day&start_date=20250101&end_date=20260407&limit=500"
```

---

## 测试验证

### 测试环境
- 后端版本：v6.2.4
- 账户 ID: 8229DE7E
- 测试股票：600519.SH (贵州茅台)

### 测试结果

| 周期 | 时间范围 | 记录数 | 状态 |
|------|---------|--------|------|
| day | 7d | 63 | ✅ |
| 60m | 30d | 3 | ✅ |
| week | 2y | 100 | ✅ |

---

## 周期枚举值对照表

| 周期代码 | SDK 枚举 | 值 | 说明 |
|---------|---------|-----|------|
| 1m | min1 | 10000 | 1 分钟 K 线 |
| 3m | min3 | 10001 | 3 分钟 K 线 |
| 5m | min5 | 10002 | 5 分钟 K 线 |
| 10m | min10 | 10003 | 10 分钟 K 线 |
| 15m | min15 | 10004 | 15 分钟 K 线 |
| 30m | min30 | 10005 | 30 分钟 K 线 |
| 60m | min60 | 10006 | 60 分钟 K 线 |
| 120m | min120 | 10007 | 120 分钟 K 线 |
| day | day | 10008 | 日线 |
| week | week | 10009 | 周线 |
| month | month | 10010 | 月线 |

---

## 时间范围快捷选择对照表

| 参数值 | 时间跨度 | 说明 |
|-------|---------|------|
| 7d | 7 天 | 最近 7 个交易日 |
| 30d | 30 天 | 最近 30 个交易日 |
| 90d | 90 天 | 最近 90 个交易日 |
| 180d | 180 天 | 最近 180 个交易日 |
| 1y | 365 天 | 最近 1 年 |
| 2y | 730 天 | 最近 2 年 |
| 5y | 1825 天 | 最近 5 年 |
| 10y | 3650 天 | 最近 10 年 |
| all | 全部 | 所有可用数据 |
| custom | 自定义 | 需配合 start_date/end_date 使用 |

---

## 注意事项

1. **日期格式**: 必须使用 YYYYMMDD 格式，如 `20250101`
2. **股票代码格式**: 支持 `600519` 或 `600519.SH` 格式，自动补充后缀
3. **账户验证**: 需要有效的账户 ID，否则返回 404
4. **返回数量限制**: 最大 10000 条记录
5. **数据源**: 通过 AmazingData SDK 获取实时 K 线数据

---

## 前端使用说明

1. 访问数据浏览器页面 (`/data-explorer`)
2. 在 K 线数据查询卡片中输入股票代码
3. 选择 K 线周期（11 种可选）
4. 选择时间范围（快捷选择或自定义）
5. 点击"查询"按钮
6. 查看结果表格，支持 CSV 导出

---

**报告生成时间**: 2026-04-07
