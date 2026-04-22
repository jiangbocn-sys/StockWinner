# stock_daily_factors 数据校正说明

## 问题背景

在数据迁移过程中发现以下问题：

1. **原始迁移脚本** (`migrate_factors.py`) 只从 kline 数据计算了 `next_period_change` 字段
2. **其他技术因子**（如 `change_10d`, `bias_5`, `kdj_k`, `dif` 等）直接从 `stock_factors` 表复制
3. **问题**：这些因子的值在月内被错误地复制为相同值，而实际上应该每天不同

## 校正方案

采用**按股票批量计算**的方案：

### 方案优势
- **效率高**：每只股票的 kline 数据只读取一次
- **递归计算**：使用 pandas 一次性计算所有因子
- **支持中断恢复**：进度持久化，可随时停止和继续

### 预计处理时间
- 股票数量：~4,944 只
- 每只股票记录数：~1,153 条
- 单只股票处理时间：0.5-2 秒
- **总时间：1.5-3 小时**

## 使用方法

### 1. 启动校正（后台运行）

```bash
cd /home/bobo/StockWinner

# 继续之前的进度（如果有）
./scripts/run_correction.sh

# 从头开始（重置进度）
./scripts/run_correction.sh reset
```

### 2. 查看进度

```bash
./scripts/check_correction_progress.sh
```

### 3. 查看日志

```bash
# 查看最新日志
tail -f logs/correction_*.log

# 查看特定日志文件
tail -f logs/correction_20260403_140000.log
```

### 4. 停止校正

```bash
# 查找进程
ps aux | grep correct_daily_factors

# 停止进程
kill <PID>
```

### 5. 手动运行（前台）

```bash
source venv/bin/activate

# 继续进度
python3 services/factors/correct_daily_factors.py

# 从头开始
python3 services/factors/correct_daily_factors.py --reset
```

## 文件说明

| 文件 | 说明 |
|------|------|
| `services/factors/correct_daily_factors.py` | 校正脚本主程序 |
| `scripts/run_correction.sh` | 后台启动脚本 |
| `scripts/check_correction_progress.sh` | 进度查看脚本 |
| `data/correction_progress.json` | 进度持久化文件 |
| `logs/correction_*.log` | 运行日志 |

## 校正内容

重新计算的因子包括：

### 市场表现类
- `change_5d`, `change_10d`, `change_20d` - N 日涨跌幅
- `bias_5`, `bias_10`, `bias_20` - N 日乖离率
- `amplitude_5`, `amplitude_10`, `amplitude_20` - N 日振幅
- `change_std_5`, `change_std_10`, `change_std_20` - N 日涨跌幅标准差
- `amount_std_5`, `amount_std_10`, `amount_std_20` - N 日成交额标准差

### 技术指标类
- `kdj_k`, `kdj_d`, `kdj_j` - KDJ 指标
- `dif`, `dea`, `macd` - MACD 指标

### 其他
- `next_period_change` - 下期收益率（重新计算）
- `is_traded` - 是否交易（重新计算）
- `source` - 标记为 'recalculated'

## 注意事项

1. **2026-04-03 数据**：脚本会自动删除该日数据（未收盘，数据为 0）
2. **进度保存**：每处理 10 只股票保存一次进度
3. **断点续传**：中断后再次运行会自动从断点继续
4. **数据库锁**：使用 SQLite WAL 模式，支持并发读取

## 验证结果

校正完成后，运行以下命令验证：

```bash
python3 << 'EOF'
import sqlite3
conn = sqlite3.connect('data/kline.db')
cursor = conn.cursor()

# 统计
cursor.execute("""
    SELECT 
        COUNT(*) as total,
        SUM(CASE WHEN source = 'recalculated' THEN 1 ELSE 0 END) as recalculated,
        COUNT(DISTINCT stock_code) as stocks
    FROM stock_daily_factors
""")
row = cursor.fetchone()
print(f"总记录数：{row[0]:,}")
print(f"已重新计算：{row[1]:,} ({row[1]/row[0]*100:.1f}%)")
print(f"股票数量：{row[2]:,}")

# 随机抽查
cursor.execute("""
    SELECT stock_code, trade_date, change_10d, bias_5, kdj_k, source
    FROM stock_daily_factors
    WHERE source = 'recalculated'
    ORDER BY RANDOM()
    LIMIT 5
""")
print("\n随机抽查:")
for row in cursor.fetchall():
    print(f"  {row[0]} {row[1]}: change_10d={row[2]:.4f}, bias_5={row[3]:.4f}, kdj_k={row[4]:.4f}")

conn.close()
EOF
```

## 常见问题

**Q: 中途关闭终端会影响吗？**
A: 不会。脚本使用 nohup 后台运行，关闭终端不影响。

**Q: 如何确认校正完成？**
A: 运行 `./scripts/check_correction_progress.sh`，看到 100% 即完成。

**Q: 校正过程中可以正常使用系统吗？**
A: 可以。校正脚本使用 SQLite 读锁，不影响 API 读取。

**Q: 校正后发现数据异常怎么办？**
A: 可以运行 `./scripts/run_correction.sh reset` 重新从头开始。

---

**创建时间**: 2026-04-03
**版本**: v6.2.1
