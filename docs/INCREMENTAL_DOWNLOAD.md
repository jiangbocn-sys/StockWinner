# 增量 K 线数据下载说明

## 概述

增量下载模块负责从 AmazingData SDK 下载 K 线数据到本地 SQLite 数据库，并在下载完成后自动计算所有技术因子。

## 主要功能

### 1. 增量更新

- 自动检测本地已有数据
- 只下载缺失或需要更新的数据
- 支持完整下载和增量更新

### 2. 交易时间检查

下载结束日期根据当前时间自动确定：

| 当前时间 | 结束日期 |
|---------|---------|
| 交易日 < 16:00 | 前一日（不下载当日数据） |
| 交易日 >= 16:00 | 当日（下载包含当日数据） |
| 非交易日 | 最近一个交易日 |

**原理**：A 股收盘时间为 15:00，数据处理和清算通常需要 1 小时左右。16:00 后 SDK 数据基本稳定，适合下载。

### 3. 自动因子计算

下载完成后自动调用因子计算函数，**仅计算新增日期的因子**（不是全部重算）：

**市场表现类**
- `change_5d`, `change_10d`, `change_20d` - N 日涨跌幅
- `bias_5`, `bias_10`, `bias_20` - N 日乖离率
- `amplitude_5`, `amplitude_10`, `amplitude_20` - N 日振幅
- `change_std_5`, `change_std_10`, `change_std_20` - N 日涨跌幅标准差
- `amount_std_5`, `amount_std_10`, `amount_std_20` - N 日成交额标准差

**技术指标类**
- `kdj_k`, `kdj_d`, `kdj_j` - KDJ 指标
- `dif`, `dea`, `macd` - MACD 指标

**其他**
- `next_period_change` - 下期收益率
- `is_traded` - 是否交易

**市值类**（自动从 SDK 获取股本数据计算）
- `circ_market_cap` - 流通市值（元）
- `total_market_cap` - 总市值（元）

**上市天数**
- `days_since_ipo` - 从 stock_factors 表推算 IPO 日期后计算

### 4. 增量计算逻辑

因子计算采用智能增量策略：

1. **检测已有数据**：查询 `stock_daily_factors` 表，找到该股票已有哪些日期的数据
2. **只算新增日期**：如果已有最大值是 2026-04-01，本次下载了 2026-04-02 的数据，则只计算 2026-04-02 这一天
3. **填补历史缺失**：如果历史有缺失，会自动补充缺失日期的因子
4. **避免重复计算**：已存在的因子数据不会被重新计算

**示例**：
```
股票 A 已有因子数据：2024-01-01 至 2026-04-01
本次下载 K 线数据：2024-01-01 至 2026-04-02
实际计算范围：仅 2026-04-02（新增一天）
```

## 使用方法

### 方式一：后台脚本（推荐）

```bash
cd /home/bobo/StockWinner

# 设置环境变量（首次使用）
export BROKER_ACCOUNT=your_account
export BROKER_PASSWORD=your_password

# 增量下载（自动计算因子）
./scripts/download_incremental_kline.sh

# 增量下载（不计算因子）
./scripts/download_incremental_kline.sh no-calc
```

### 方式二：Python 直接调用

```python
from services.data.local_data_service import download_incremental_kline_data_sync

# 增量下载（默认 6 个月，自动计算因子）
result = download_incremental_kline_data_sync(
    batch_size=50,
    months=6,
    broker_account="your_account",
    broker_password="your_password",
    calculate_factors=True
)
```

### 方式三：完整下载

```python
from services.data.local_data_service import download_all_kline_data_sync

# 完整下载（默认 24 个月）
result = download_all_kline_data_sync(
    batch_size=50,
    months=24,
    broker_account="your_account",
    broker_password="your_password",
    calculate_factors=True
)

# 自定义日期范围
result = download_all_kline_data_sync(
    start_date="2024-01-01",
    end_date="2024-12-31",
    broker_account="your_account",
    broker_password="your_password",
    calculate_factors=True
)
```

## 实现方案对比

### 方案 A：先下载 k 线，再批量计算因子（已采用）

**优点**：
- kline 数据一次性批量插入，效率高
- 因子计算可以批量进行，减少数据库读写次数
- 代码结构清晰，易于维护
- 下载失败不影响已有因子数据

**缺点**：
- 需要额外的因子计算步骤
- 总体时间 = 下载时间 + 计算时间

### 方案 B：下载同时计算并插入因子

**优点**：
- 流程一体化

**缺点**：
- 每只股票需要等待 SDK 返回后立即计算，串行时间长
- 频繁读写数据库，性能较差
- 代码复杂度高

### 结论

采用方案 A，"下载优先，计算后置"，效率更高，代码更清晰。

## 数据流程

```
┌─────────────┐     ┌──────────────┐     ┌─────────────┐
│ SDK API     │────▶│ kline_data   │────▶│ 因子计算     │
│ (TGW/银河)    │     │ (K 线原始数据) │     │ DailyFactor │
└─────────────┘     └──────────────┘     └─────────────┘
                                                │
                                                ▼
                                         ┌──────────────┐
                                         │ stock_daily_ │
                                         │ factors      │
                                         │ (技术因子表)  │
                                         └──────────────┘
```

## 进度查看

```bash
# 查看下载进度
ps aux | grep download_incremental

# 查看最新日志
tail -f logs/incremental_kline_*.log

# 查看数据库统计
python3 << 'EOF'
import sqlite3
conn = sqlite3.connect('data/kline.db')
cursor = conn.cursor()

# K 线数据统计
cursor.execute("""
    SELECT COUNT(DISTINCT stock_code) as stocks,
           COUNT(*) as records,
           MAX(kline_time) as latest_date,
           MIN(kline_time) as earliest_date
    FROM kline_data
""")
row = cursor.fetchone()
print(f"K 线数据：{row[0]} 只股票，{row[1]:,} 条记录")
print(f"日期范围：{row[3]} 至 {row[2]}")

# 因子数据统计
cursor.execute("""
    SELECT COUNT(DISTINCT stock_code) as stocks,
           COUNT(*) as records,
           COUNT(DISTINCT source) as sources
    FROM stock_daily_factors
""")
row = cursor.fetchone()
print(f"\n因子数据：{row[0]} 只股票，{row[1]:,} 条记录，{row[2]} 种来源")
conn.close()
EOF
```

## 文件说明

| 文件 | 说明 |
|------|------|
| `services/data/local_data_service.py` | 本地 K 线数据服务主程序 |
| `services/factors/daily_factor_calculator.py` | 日频因子计算器 |
| `scripts/download_incremental_kline.sh` | 增量下载后台脚本 |
| `logs/incremental_kline_*.log` | 增量下载日志 |

## 性能优化

1. **批量下载**：每次下载 50 只股票（SDK 限制）
2. **批量插入**：单事务批量插入，减少数据库写入次数
3. **WAL 模式**：SQLite WAL 模式，支持并发读写
4. **索引优化**：`stock_code + kline_time` 联合索引

## 注意事项

1. **Broker Credentials**：需要配置银河证券资金账号和密码
2. **下载频率**：建议每日收盘后运行一次增量下载
3. **因子计算**：默认自动计算因子，可传递 `no-calc` 参数跳过
4. **4PM 规则**：交易日 16:00 前不下载当日数据，确保数据稳定性

## 常见问题

**Q: 如何确认下载完成？**

A: 运行查看日志，看到"下载完成"和"因子计算完成"消息。

**Q: 下载后发现数据异常怎么办？**

A: 可以重新运行脚本，增量逻辑会自动检测并修复缺失数据。

**Q: 因子计算需要多长时间？**

A: 约 5000 只股票，6 个月数据，计算时间约 2-5 分钟。

**Q: 可以只下载某只股票的数据吗？**

A: 可以修改代码，传递 `stock_codes` 参数指定股票列表。

---

**创建时间**: 2026-04-03
**版本**: v6.2.2
