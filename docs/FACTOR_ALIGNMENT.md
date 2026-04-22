# 因子数据对齐和补全工具

## 功能说明

该工具用于对齐 `kline_data` 和 `stock_daily_factors` 表的数据，确保每只股票的每个交易日都有对应的因子记录。

### 主要功能

1. **数据差距分析**: 检测两个表之间的记录差异
2. **孤儿记录清理**: 删除 `stock_daily_factors` 中存在但 `kline_data` 中不存在的记录
3. **缺失记录插入**: 为 `kline_data` 中有但 `stock_daily_factors` 中没有的记录创建空行
4. **智能因子计算**: 
   - 按股票代码遍历计算
   - 自动检测数据量要求（如 MACD 需要 26 天数据）
   - 已有非空数据的记录跳过不计算
   - 数据量不足时字段值留空

## API 使用

### 1. 查看数据差距分析

```bash
curl http://localhost:8080/api/v1/ui/factors/gap-analysis
```

响应示例：
```json
{
  "success": true,
  "data": {
    "kline_total": 5852153,      // kline_data 总记录数
    "factor_total": 5841837,     // stock_daily_factors 总记录数
    "missing_records": 10316,    // 缺失的因子记录数
    "orphan_records": 0,         // 孤儿记录数
    "latest_kline_date": "2026-04-07",
    "latest_factor_date": "2026-04-03"
  }
}
```

### 2. 查看缺失记录

```bash
# 查看所有缺失记录（前 100 条）
curl http://localhost:8080/api/v1/ui/factors/missing-records

# 查看特定股票的缺失记录
curl http://localhost:8080/api/v1/ui/factors/missing-records?stock_code=600519.SH
```

### 3. 查看孤儿记录

```bash
curl http://localhost:8080/api/v1/ui/factors/orphan-records
```

### 4. 删除孤儿记录

```bash
# 预览（不实际删除）
curl http://localhost:8080/api/v1/ui/factors/orphan-records?dry_run=true

# 实际删除
curl -X DELETE http://localhost:8080/api/v1/ui/factors/orphan-records?dry_run=false
```

### 5. 启动因子计算任务

```bash
# 默认配置（插入缺失记录 + 计算因子）
curl -X POST http://localhost:8080/api/v1/ui/factors/calculate \
  -H "Content-Type: application/json" \
  -d '{}'

# 完整配置
curl -X POST http://localhost:8080/api/v1/ui/factors/calculate \
  -H "Content-Type: application/json" \
  -d '{
    "delete_orphans": false,
    "insert_missing": true,
    "calculate_factors": true,
    "recalculate": false,
    "stock_codes": ["600519.SH", "000001.SZ"]
  }'
```

参数说明：
- `delete_orphans`: 是否删除孤儿记录
- `insert_missing`: 是否插入缺失记录
- `calculate_factors`: 是否计算因子
- `recalculate`: 是否重新计算已有数据的记录
- `stock_codes`: 指定股票代码列表（可选，不传则处理所有股票）

### 6. 查看计算进度

```bash
curl http://localhost:8080/api/v1/ui/factors/status
```

响应示例：
```json
{
  "running": true,
  "progress": 65,
  "message": "计算中... 3250/5194 (600100.SH)",
  "started_at": "2026-04-08T15:30:00",
  "completed_at": null,
  "result": null
}
```

## 命令行使用

### 运行完整对齐和计算

```bash
cd /home/bobo/StockWinner
source venv/bin/activate
python3 -m services.factors.factor_alignment
```

### 命令行参数

```bash
# 完整功能
python3 -m services.factors.factor_alignment --delete-orphans

# 只分析不计算
python3 -m services.factors.factor_alignment --no-insert --no-calculate

# 只计算特定股票
python3 -m services.factors.factor_alignment --stocks 600519.SH 000001.SZ

# 重新计算已有数据
python3 -m services.factors.factor_alignment --recalculate

# 不显示进度
python3 -m services.factors.factor_alignment --no-progress
```

## 因子计算逻辑

### 数据量要求

| 因子类别 | 最小数据量 | 说明 |
|---------|----------|------|
| MA5 | 5 天 | 5 日均线 |
| MA10 | 10 天 | 10 日均线 |
| MA20 | 20 天 | 20 日均线 |
| MA60 | 60 天 | 60 日均线 |
| EMA12 | 12 天 | 12 日指数均线 |
| EMA26/DIF/DEA/MACD | 26 天 | MACD 指标 |
| KDJ | 9 天 | KDJ 指标 |
| RSI_14 | 14 天 | 相对强弱指标 |
| CCI_20 | 20 天 | 商品通道指标 |
| ATR_14 | 14 天 | 平均真实波幅 |
| BOLL | 20 天 | 布林带 |

### 计算顺序

1. 按股票代码分组
2. 对每只股票：
   - 获取 K 线数据（最多 500 天）
   - 检查 `stock_daily_factors` 中已有记录
   - 对缺失或空记录计算因子
   - 数据量不足时留空对应字段

### 跳过规则

- 已有非空数据的记录默认跳过（除非 `--recalculate`）
- K 线数据不足时，对应因子留空

## 当前状态

```
kline_data 记录数：5,852,153
stock_daily_factors 记录数：5,841,837
缺失记录数：10,316
孤儿记录数：0
最新 K 线日期：2026-04-07
最新因子日期：2026-04-03
```

**文档更新时间**: 2026-04-08
**版本**: v6.2.5
