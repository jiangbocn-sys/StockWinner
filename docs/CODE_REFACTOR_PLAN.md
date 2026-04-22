# 代码重构计划

**创建时间**: 2026-04-03
**版本**: v1.1
**最后更新**: 2026-04-03

---

## 概述

本文档记录了代码梳理过程中发现的重复逻辑和不一致之处，用于指导后续重构工作。

---

## 已完成工作摘要

本次重构完成了 P0-P3 所有优先级的问题修复：

| 类别 | 新增/修改文件 | 说明 |
|------|-------------|------|
| **新增统一模块** | `services/common/timezone.py` | 统一时区处理 |
| | `services/common/technical_indicators.py` | 统一技术指标计算（基于 pandas） |
| | `services/common/sdk_manager.py` | 统一 SDK 登录管理（单例） |
| | `services/common/stock_code.py` | 统一股票代码格式化 |
| | `services/common/config.py` | 统一配置常量 |
| | `services/common/logging_config.py` | 统一日志配置 |
| | `services/common/progress.py` | 统一进度追踪 |
| **修改文件** | `services/common/indicators.py` | 改为调用统一技术模块（兼容层） |
| | `services/factors/daily_factor_calculator.py` | 使用统一技术指标和 SDK 管理器 |
| | `services/factors/sdk_api.py` | 使用统一 SDK 管理器（兼容层） |
| | `services/screening/service.py` | 使用统一时区和技术指标模块 |
| | `services/data/local_data_service.py` | 使用统一时区和 SDK 管理器 |
| **修复** | `services/trading/gateway.py` | 字段命名统一为 `kline_time` |

---

## 问题清单

### 1. K 线数据下载 - 字段命名不一致 ✅ 已修复

**文件**: `services/trading/gateway.py`

**问题描述**: 
- 批量下载返回 DataFrame 使用 `kline_time` 字段
- 单个下载转换为 dict 列表时使用 `time` 字段
- 数据库列名为 `kline_time`

**影响**: 单只股票下载后保存失败（NOT NULL 约束），需要额外重命名字段

**修复状态**: ✅ 已在 gateway.py:1118 修复，统一为 `kline_time`

---

### 2. 时区处理 - 代码重复 ✅ 已完成

**文件**: 
- `services/data/local_data_service.py`
- `services/screening/service.py`
- 多处重复定义 `CHINA_TZ`

**问题描述**: 
每个模块都独立定义中国时区常量和 `get_china_time()` 函数

**解决方案**: 
创建 `services/common/timezone.py` 统一提供时区功能

**修复状态**: ✅ 已完成，所有模块已迁移使用 `services/common/timezone.py`

---

### 3. SDK 登录逻辑 - 重复 ✅ 已完成

**文件**: 
- `services/factors/sdk_api.py`
- `services/trading/gateway.py`

**问题描述**: 
SDK 登录状态没有全局共享，每个类都独立实现登录逻辑

**解决方案**: 
创建 `services/common/sdk_manager.py` 单例管理类

**修复状态**: ✅ 已完成，`sdk_api.py` 改为调用 `SDKManager`

---

### 4. 数据库连接管理 - 不一致 📝 待处理

**文件**: 多个文件

**当前状态**: 
三种不同的数据库连接管理模式并存

**影响**: 
- 并发性能不一致
- 代码难以统一维护

**建议方案**: 
扩展 `DatabaseManager` 支持同步 SQLite，因子计算等同步代码逐步迁移

**优先级**: P2 (待后续处理)

---

### 5. 技术指标计算 - 重复实现 ✅ 已完成

**文件**: 
- `services/common/indicators.py` - 选股模块使用
- `services/factors/daily_factor_calculator.py` - 因子计算模块使用

**问题描述**: 
两套独立的技术指标计算实现：

| 指标 | indicators.py | daily_factor_calculator.py |
|------|--------------|---------------------------|
| MA | ✅ | ✅ |
| RSI | ✅ | ✅ (通过 pct_change 间接计算) |
| MACD | ✅ (简化版) | ✅ (标准版) |
| KDJ | ✅ | ✅ |
| 振幅/波动率 | ❌ | ✅ |

**关键差异**: 
- `indicators.py` 的 `macd()` 信号线计算不准确（第 130 行简化计算）
- `indicators.py` 的 `ema()` 使用简单循环，`daily_factor_calculator.py` 使用 pandas.ewm()

**影响**: 
- 选股和技术分析使用不同计算逻辑，结果可能不一致
- 维护两套代码成本高

**建议方案**: 
1. 保留 `daily_factor_calculator.py` 的 pandas 实现（更准确）
2. `indicators.py` 改为调用 `daily_factor_calculator.py` 的函数
3. 或者统一使用 pandas 实现，消除差异

**优先级**: P0

---

### 6. 因子计算逻辑 - 分散 ⚠️ P1

**文件**: 
- `services/factors/daily_factor_calculator.py` - 因子计算器
- `services/factors/correct_market_cap.py` - 市值校正脚本
- `services/data/local_data_service.py:591-801` - `calculate_and_save_factors_for_dates()`

**问题描述**: 
`local_data_service.py:calculate_and_save_factors_for_dates()` 内部重复实现了因子计算逻辑（第 699-704 行），而不是调用 `DailyFactorCalculator`：

```python
# local_data_service.py:699-704
df = calculator.calculate_price_performance(df)
df = calculator.calculate_kdj(df)
df = calculator.calculate_macd(df)
df['next_period_change'] = df['close'].pct_change().shift(-1)
df['is_traded'] = (df['volume'] > 0).astype(int)
```

同时，市值计算逻辑也在两处实现。

**影响**: 
- 代码重复，维护成本高
- 容易引入 bug（一处修改另一处忘记改）

**建议方案**: 
1. `calculate_and_save_factors_for_dates()` 应直接调用 `calculator.calculate_all_daily_factors()`
2. 删除重复的因子计算代码
3. 市值校正逻辑应集成到因子计算器中作为可选步骤

**优先级**: P1

---

### 7. 股票代码格式处理 - 不一致

**文件**: 
- `services/trading/gateway.py:1073-1078` - 单只股票下载时规范化代码
- `services/data/local_data_service.py` - 期望带 `.SH/.SZ` 后缀
- `services/factors/sdk_api.py` - 依赖 SDK 返回格式

**问题描述**: 
没有统一的股票代码格式化函数，每个模块自己处理

**影响**: 
- 代码冗余
- 可能导致格式不一致引发 bug

**建议方案**: 
在 `services/common/` 下添加统一函数:
```python
def normalize_stock_code(code: str) -> str:
    """规范化股票代码格式为 CODE.SH 或 CODE.SZ"""
    if '.' in code:
        return code.upper()
    if code.startswith('6') or code.startswith('9'):
        return f"{code}.SH"
    return f"{code}.SZ"
```

**优先级**: P3

---

### 8. 批量处理批次大小 - 不一致

**文件**: 多个文件

| 位置 | 批次大小 | 说明 |
|------|---------|------|
| `local_data_service.py:955` | 50 | SDK 批量下载 |
| `calculate_and_save_factors_for_dates():639` | 50 | 因子计算批次 |
| `correct_market_cap.py:210` | 500 | 市值更新批次 |
| `correct_market_cap.py:274` | 100 | SDK 批量查询参数 |

**问题描述**: 
批次大小在各处独立定义，没有统一配置

**影响**: 
- 性能优化空间有限
- 难以统一管理

**建议方案**: 
在配置文件中统一定义批次大小常量:
```python
BATCH_SIZE = {
    'download': 50,      # SDK 限制
    'factor_calc': 50,   # 因子计算
    'market_cap': 500,   # 市值更新
    'sdk_query': 100,    # SDK 查询
}
```

**优先级**: P3

---

### 9. 进度追踪 - 缺失

**文件**: 多个文件

**当前状态**:

| 模块 | 进度追踪 | 持久化 |
|------|---------|--------|
| screening/service.py | ✅ `_progress` 字典 | ❌ |
| correct_market_cap.py | ✅ 控制台输出 | ✅ JSON 文件 |
| download_incremental | ❌ | ❌ |
| factor calculation | ❌ | ❌ |

**问题描述**: 
长时间运行的任务（下载、因子计算）缺乏统一的进度追踪接口

**影响**: 
- 用户无法了解后台任务进度
- 中断后无法恢复

**建议方案**: 
创建统一的进度追踪接口:
```python
class ProgressTracker:
    def __init__(self, task_id: str, total: int):
        self.task_id = task_id
        self.current = 0
        self.total = total
        self.status = "pending"
        
    def update(self, current: int, status: str = ""):
        ...
        
    def save_to_file(self):
        ...
```

**优先级**: P3

---

### 10. 错误处理 - 不一致

**文件**: 多个文件

**当前模式**:

| 模式 | 代码示例 | 文件 |
|------|---------|------|
| A | `print(f"错误：{e}"); continue` | 多处 |
| B | `raise Exception(f"失败：{str(e)}")` | 多处 |
| C | `logger.error(f"失败：{e}"); return {}` | gateway.py |

**问题描述**: 
三种错误处理模式并存，没有统一策略

**影响**: 
- 错误日志格式不一致
- 难以统一监控和告警

**建议方案**: 
1. 引入 `logging` 模块替代 `print`
2. 定义错误处理策略:
   - 关键路径：抛出异常
   - 批量操作中的单项：记录错误，继续处理
3. 统一日志格式

**优先级**: P2

---

### 11. 选股策略执行要求 active 状态 - 不灵活 ✅ 已修复

**文件**: 
- `services/screening/service.py` - `_run_screening()` 函数
- `services/ui/screening.py` - `run_screening_once()` API

**问题描述**: 
当用户通过 UI 选择 LLM 策略后点击执行选股时，要求策略必须是 `active` 状态，导致新创建的策略无法直接测试

**影响**: 
- 用户创建策略后必须先"激活"才能测试，流程繁琐
- LLM 生成的策略默认是 `draft` 状态，无法直接执行

**解决方案**: 
1. 为 `_run_screening()` 添加 `require_active` 参数（默认 True）
2. 当 `require_active=False` 时，允许执行任意状态的策略
3. UI API 添加 `allow_draft` 参数供前端控制

**修复状态**: ✅ 已完成

---

## 实施计划

### 第一阶段 - 高优先级 (P0)

1. **统一技术指标计算**
   - 评估两套实现的差异
   - 确定保留哪套实现
   - 迁移调用方到统一接口
   - 删除重复代码

预计工作量: 4-6 小时

---

### 第二阶段 - 中高优先级 (P1)

1. **统一因子计算逻辑**
   - 重构 `calculate_and_save_factors_for_dates()` 调用 `DailyFactorCalculator`
   - 删除重复计算代码
   - 集成市值校正逻辑

预计工作量: 3-4 小时

---

### 第三阶段 - 中优先级 (P2)

1. **创建统一时区模块** `services/common/timezone.py`
2. **统一 SDK 登录管理** 创建 `SDKManager` 单例
3. **统一错误处理** 引入 logging 模块
4. **数据库连接管理** 扩展 DatabaseManager

预计工作量: 4-5 小时

---

### 第四阶段 - 低优先级 (P3)

1. **统一股票代码格式化** 创建 `normalize_stock_code()` 函数
2. **统一批次大小配置** 创建配置文件
3. **进度追踪接口** 创建 `ProgressTracker` 类
4. **代码审查和文档更新**

预计工作量: 3-4 小时

---

## 总体工作量估算

| 阶段 | 任务数 | 预计工时 |
|------|--------|---------|
| P0 | 1 | 4-6 小时 |
| P1 | 1 | 3-4 小时 |
| P2 | 4 | 4-5 小时 |
| P3 | 4 | 3-4 小时 |
| **总计** | **10** | **14-19 小时** |

---

## 注意事项

1. **测试覆盖**: 每个重构步骤后需要运行现有测试确保功能正常
2. **渐进式重构**: 优先处理影响功能的问题， Cosmetic 问题可以延后
3. **用户通知**: 如果重构影响 API 接口，需要提前通知用户
4. **版本控制**: 每个重构步骤创建独立的 git 提交

---

## 变更记录

| 日期 | 变更内容 | 作者 |
|------|---------|------|
| 2026-04-03 | 初始版本，记录代码梳理发现的问题 | - |
