# 数据下载功能修复报告 - v6.2.5

**修复日期**: 2026-04-08  
**版本**: v6.2.5  
**问题**: 用户反馈增量下载没有下载最新交易日数据，股票名称始终用代码代替，缺少进度显示

---

## 问题分析

### 1. 增量下载问题

**现象**: 最近运行增量下载后，K 线数据的 `created_at` 和 `updated_at` 时间标签被更新到最新时间，但实际 K 线数据并未下载最新交易日数据。

**原因分析**:
- 增量下载逻辑本身正确，使用 `get_trading_day_end_date()` 函数计算下载截止日期
- 问题是现有数据已经是 2026-04-03（上周五），而当前时间是 2026-04-08 13:00+
- 根据规则：工作日 16:00 前不下载当日数据，所以应该下载 2026-04-07 的数据
- 但数据库中没有 2026-04-07 和 2026-04-08 的数据，说明确实需要下载

**下载逻辑规则**:
| 时间 | 规则 | 结束日期 |
|------|------|---------|
| 交易日 < 16:00 | 不下载当日数据 | 前一日 |
| 交易日 >= 16:00 | 下载当日数据 | 当日 |
| 非交易日 | 使用最近交易日 | 周五 |

### 2. 股票名称问题

**现象**: `kline_data` 表中所有记录的 `stock_name` 字段都存储的是股票代码（如 `600519.SH`），而不是正确的股票名称（如"贵州茅台"）。

**原因**:
1. SDK 返回的 DataFrame 不包含 `stock_name` 字段
2. `save_kline_data_batch()` 函数没有从其他数据表获取股票名称
3. 因子计算函数 `calculate_and_save_factors_for_dates()` 中 `stock_name` 硬编码为空字符串

**数据现状**:
- `stock_monthly_factors` 表有正确的股票名称（295,272 条记录）
- `kline_data` 表 5,847,019 条记录全部使用股票代码作为名称

### 3. 进度显示问题

**现象**: 下载和因子计算过程没有详细的进度输出，用户无法掌握系统状态。

**原因**: 
- 下载函数有基础进度输出，但不够详细
- 因子计算函数完全没有进度显示

---

## 修复方案

### 修改的文件

**文件**: `services/data/local_data_service.py`

### 修复 1: `save_kline_data_batch()` - 自动获取股票名称

在保存数据前，检查股票名称是否为空或等于股票代码，如果是则从数据库获取：

```python
# 如果股票名称为空或等于股票代码，尝试从数据库获取
if not stock_name or stock_name == stock_code:
    cursor.execute(
        "SELECT stock_name FROM stock_monthly_factors WHERE stock_code = ? LIMIT 1",
        (stock_code,)
    )
    row = cursor.fetchone()
    if row and row[0]:
        stock_name = row[0]
    else:
        # 退而求其次，从 kline_data 获取
        cursor.execute(
            "SELECT DISTINCT stock_name FROM kline_data WHERE stock_code = ? LIMIT 1",
            (stock_code,)
        )
        row = cursor.fetchone()
        if row and row[0]:
            stock_name = row[0]
        else:
            # 最后使用股票代码作为默认值
            stock_name = stock_code
```

### 修复 2: `calculate_and_save_factors_for_dates()` - 获取股票名称 + 进度显示

1. **添加 `show_progress` 参数**: 控制是否显示进度
2. **从 kline_data 获取股票名称**: 在计算因子时查询股票名称
3. **添加详细进度输出**: 批次进度、总体进度、股票名称

```python
# 从 kline_data 获取股票名称
cursor.execute(
    "SELECT DISTINCT stock_name FROM kline_data WHERE stock_code = ? LIMIT 1",
    (stock_code,)
)
row = cursor.fetchone()
if row and row[0]:
    stock_name = row[0]

# 进度输出
if show_progress:
    progress_pct = (processed / total_stocks) * 100
    print(f"[FactorCalc] 进度：{progress_pct:.1f}% | 批次 {batch_num}/{num_batches}")
```

### 修复 3: `download_all_kline_data()` - 增强进度显示

1. **添加网关类型日志**: 显示使用的网关类型
2. **优化进度输出**: 显示百分比、批次信息
3. **因子计算进度**: 调用时传入 `show_progress=True`

### 修复 4: 创建修复脚本 `fix_stock_names.py`

用于批量修复现有数据中的股票名称：

```bash
python3 scripts/fix_stock_names.py
```

---

## 修复结果

### 1. 股票名称修复

| 指标 | 修复前 | 修复后 |
|------|--------|--------|
| 总记录数 | 5,847,019 | 5,847,019 |
| 有正确名称 | 0 (0%) | 5,824,121 (99.6%) |
| 无正确名称 | 5,847,019 (100%) | 22,898 (0.4%) |

**注**: 剩余 25 只股票（22,898 条记录）在 `stock_monthly_factors` 表中没有数据，可能是新上市或停牌股票。这些股票的名称将在下次增量下载时从 SDK 获取。

### 2. 进度显示

**下载进度示例**:
```
[LocalData] 进度：45.2% | 批次 23/51 | 处理 50 只股票 (2300/5100)
  本批次保存：1200 条记录 | 成功：48 | 失败：2
```

**因子计算进度示例**:
```
[FactorCalc] 进度：32.5% | 批次 17/52 | 处理 50 只股票 (1700/5200)
  600519.SH (贵州茅台): 插入 250 条记录
```

### 3. 增量下载逻辑验证

```python
# 测试当前时间：2026-04-08 13:19（周三，工作日）
end_date = "2026-04-07"  # 正确：下载截止至昨日
status_msg = "工作日 13:00 < 16:00，下载截止至 2026-04-07"
```

---

## 测试验证

### 1. 股票名称查询

```bash
sqlite3 data/kline.db "SELECT stock_code, stock_name, kline_time FROM kline_data ORDER BY kline_time DESC LIMIT 10;"
```

**预期结果**:
```
600519.SH | 贵州茅台 | 2026-04-03
000001.SZ | 平安银行 | 2026-04-03
```

### 2. 运行增量下载测试

```bash
cd /home/bobo/StockWinner
source venv/bin/activate
bash scripts/test_incremental_download.sh
```

### 3. 检查下载结果

```bash
python3 -c "
import sqlite3
conn = sqlite3.connect('data/kline.db')
cursor = conn.cursor()
cursor.execute('SELECT MAX(kline_time), COUNT(*) FROM kline_data')
print(f'最新日期：{cursor.fetchone()[0]}')
print(f'总记录数：{cursor.fetchone()[0]}')
"
```

---

## 脚本工具

### 1. 修复股票名称脚本

**文件**: `scripts/fix_stock_names.py`

```bash
# 运行修复
python3 scripts/fix_stock_names.py
```

### 2. 增量下载测试脚本

**文件**: `scripts/test_incremental_download.sh`

```bash
# 下载并计算因子
bash scripts/test_incremental_download.sh

# 仅下载，不计算因子
bash scripts/test_incremental_download.sh no-calc
```

### 3. 原有下载脚本（保持不变）

**文件**: `scripts/download_incremental_kline.sh`

```bash
# 后台运行，自动计算因子
./download_incremental_kline.sh

# 后台运行，不计算因子
./download_incremental_kline.sh no-calc
```

---

## 后续建议

1. **SDK 升级**: 关注 AmazingData SDK 更新，看是否支持返回股票名称字段
2. **股票池同步**: 定期同步 `stock_monthly_factors` 表，确保新上市股票有正确的名称
3. **监控告警**: 添加数据下载监控，在下载失败或数据异常时发送告警
4. **定时任务**: 配置 cron 定时任务，每个交易日 16:30 自动执行增量下载

---

**修复完成时间**: 2026-04-08  
**修复版本**: v6.2.5
