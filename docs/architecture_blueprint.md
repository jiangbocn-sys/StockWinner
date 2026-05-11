# StockWinner 架构改造蓝图

> 生成日期：2026-05-11
> 版本：v7.1.11

---

## 一、当前系统冗余清单

### 1.1 P0：巨型模块内重复实现

| 问题 | 位置 | 重复次数 | 影响 |
|------|------|----------|------|
| 因子计算管道 | `local_data_service.py` L1224 / L1667 / L1943 | 3 份 | price_perf + kdj + macd + extended + signal 完全重复 |
| 财务数据 SDK 调用 | 4 个模块各自独立调用 | 4 份 | PE_TTM 有 4 种实现，ROE 有 2 种，每次独立调 SDK |
| 中国时区定义 | `download_weekly_kline.py:19`, `monthly_factor_calculator.py:21` | 2 处 | 一个用 ZoneInfo，一个用 timedelta(hours=8)（错误做法） |
| 事件循环检测 | `download_weekly_kline.py`, `scheduler_service.py`, `local_data_service.py` | 5 种实现 | 逻辑相似但细节不一致 |

### 1.2 P1：12 个模块自建数据库连接

全部不使用 `DatabaseManager`，各自管理连接、WAL mode、busy timeout：

```
local_data_service.py         ← 20+ 处 inline sqlite3.connect()
daily_factor_calculator.py    ← _get_connection()
monthly_factor_calculator.py  ← _get_connection()
fundamental_factor_calculator.py ← _get_connection()
monthly_factor_updater.py     ← _get_connection()
monthly_factor_filler.py      ← _get_connection()
stock_base_info_service.py    ← _get_connection()
factor_registry.py            ← sqlite3.connect() inline
download_weekly_kline.py      ← sqlite3.connect() inline
scheduler_service.py          ← 10+ 处 inline sqlite3.connect()
gateway.py                    ← sqlite3.connect() inline
monitoring/service.py         ← sqlite3.connect() inline
```

### 1.3 P2：其他冗余

- **K线 period_map**：`gateway.py` 中定义 2 遍（L707 / L783），完全相同
- **策略上下文构建**：`scheduler_service.py` (300行) 和 `monitoring/service.py` 各自一套
- **策略编译缓存**：`monitoring/service.py:35` 和 `strategy/engine.py:84` 各自一套
- **股票代码后缀提取**：4 处不同实现（local_data_service, download_weekly_kline, gateway, stock_base_info_service）
- **日志格式**：`print()` 和 `logger.warning/error` 混用，无统一格式

---

## 二、目标分层架构

```
┌─────────────────────────────────────────────────────────────────┐
│                        上层：API 对接层                           │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐           │
│  │ services/ui/ │  │services/agent│  │ services/auth│           │
│  │  UI REST端点 │  │ Agent REST端点│  │  认证管理    │           │
│  └──────────────┘  └──────────────┘  └──────────────┘           │
├─────────────────────────────────────────────────────────────────┤
│                      中间层：业务编排层                            │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐           │
│  │  scheduler   │  │  monitoring  │  │  screening   │           │
│  │  任务调度编排 │  │  监控编排    │  │  选股编排    │           │
│  └──────────────┘  └──────────────┘  └──────────────┘           │
│  ┌──────────────┐                                               │
│  │  strategy/   │  策略执行引擎 + 沙盒                            │
│  └──────────────┘                                               │
├─────────────────────────────────────────────────────────────────┤
│                    底层：基础服务层 (改造重点)                      │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │
│  │ data_provider│  │ kline_manager│  │factor_pipeline│          │
│  │ SDK连接管理  │  │ K线下载+写入 │  │ 因子计算管道  │          │
│  │ ✅ 已完成    │  │ [新模块]     │  │ [新模块]     │          │
│  └──────────────┘  └──────────────┘  └──────────────┘          │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │
│  │financial_svc │  │ db_connector │  │ async_helper │          │
│  │ 财务数据批处理│  │ 统一DB连接   │  │ 事件循环兼容 │          │
│  │ [新模块]     │  │ [扩展现有]   │  │ [新模块]     │          │
│  └──────────────┘  └──────────────┘  └──────────────┘          │
│  ┌──────────────┐                                               │
│  │ stock_code   │  代码格式标准化 + 市场识别                     │
│  │ [扩展现有]   │                                               │
│  └──────────────┘                                               │
├─────────────────────────────────────────────────────────────────┤
│                    横切：系统级风控                               │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │
│  │circuit_breaker│ │ rate_limiter │  │ error_reporter│          │
│  │ SDK失败熔断   │  │ API调用限速  │  │ 统一错误上报  │          │
│  └──────────────┘  └──────────────┘  └──────────────┘          │
│  ┌──────────────┐                                               │
│  │ audit_logger │  操作审计日志 (audit.py 已有)                  │
│  └──────────────┘                                               │
└─────────────────────────────────────────────────────────────────┘
```

**核心原则：**
- 底层模块只依赖其他底层模块，不依赖中间层或上层
- 中间层只调用底层模块，不调用其他中间层
- 上层只做 HTTP 协议转换，不调用底层模块（通过中间层间接调用）
- 横切模块可被任何层调用

---

## 三、底层模块详细设计

### 3.1 kline_manager（新模块）

**文件**：`services/common/kline_manager.py`

**职责**：K线数据的获取、清洗、入库，提供统一 API

| 方法 | 输入 | 输出 | 说明 |
|------|------|------|------|
| `download_incremental()` | market_filter | success_count, fail_count | 增量下载（替代 local_data_service 的对应方法） |
| `download_full()` | market_filter | success_count, fail_count | 全量下载 |
| `download_weekly()` | years, batch_size | success_count | 周K线下载 |
| `get_latest_price(code)` | stock_code | price | 最新成交价 |
| `get_kline(code, period, start, end, limit)` | 查询参数 | DataFrame | K线历史数据 |
| `get_batch_kline(codes, period, start, end, limit)` | 批量查询 | Dict[code, DataFrame] | 批量K线 |

**从何处提取：**
- `local_data_service.py` 的 K线下载/查询方法
- `gateway.py` 的 period_map 和日期范围计算
- `download_weekly_kline.py` 的周K线逻辑

### 3.2 factor_pipeline（新模块）

**文件**：`services/factors/factor_pipeline.py`

**职责**：统一的因子计算管道，消除 3 份重复代码

```python
class FactorPipeline:
    """因子计算管道 - 单一实现"""

    def calculate_technical_factors(df: DataFrame) -> DataFrame:
        """输入原始K线DataFrame，输出带所有技术指标的DataFrame"""
        # 统一调用：price_performance -> kdj -> macd -> extended_indicators
        # 返回带 golden_cross, death_cross, limit_up 等信号的完整DataFrame
        ...

    def calculate_for_date(trade_date: str, stock_codes: list) -> dict:
        """为指定日期的指定股票批量计算因子"""
        # 内部调用 kline_manager 获取K线
        # 调用 calculate_technical_factors 计算
        # 返回因子数据（不写DB，由调用方决定）
        ...
```

**消除的重复：**
- `local_data_service.py:1224-1274` (calculate_and_save_factors_for_dates)
- `local_data_service.py:1667-1696` (fill_empty_factor_values)
- `local_data_service.py:1943-1974` (smart_update_factors)

### 3.3 financial_service（新模块）

**文件**：`services/factors/financial_service.py`

**职责**：财务数据批获取 + 比值计算，消除 N+1 SDK 调用

```python
class FinancialService:
    """财务数据服务 - 批量获取 + 比值计算"""

    def fetch_batch(stock_codes: list) -> FinancialBatch:
        """批量获取 income/balance/cashflow，内部只调一次 SDK"""
        ...

    def calc_pe_ttm(stock_code: str, data: FinancialBatch) -> float:
        """使用已获取的财务数据计算 PE_TTM"""
        ...

    def calc_roe(stock_code: str, data: FinancialBatch) -> float:
        """使用已获取的财务数据计算 ROE"""
        ...

    # 所有比值计算：PE_TTM, PB, PS_TTM, PCF, ROE, ROA,
    # GROSS_MARGIN, NET_MARGIN, REVENUE_GROWTH, PROFIT_GROWTH
```

**消除的重复：**
- `fundamental_factor_calculator.py` 每个方法独立调 SDK（10+ 次调用）
- `daily_factor_calculator.py:148/305/317/328` 独立调 SDK
- `monthly_factor_calculator.py:80/110/131` 独立调 SDK
- PE_TTM 的 4 种不同实现统一为 1 种

### 3.4 db_connector（扩展现有 database.py）

**文件**：`services/common/database.py`（扩展）

**当前问题**：已有 `DatabaseManager` 但只被少量模块使用，大部分模块自建连接。

**扩展内容：**

```python
class DatabaseManager:
    """已有的异步 DB 管理器 + 新增同步连接池"""

    def get_sync_connection(self, db_name: str = "kline") -> sqlite3.Connection:
        """返回预配置的 sqlite3 连接（已设 WAL mode, busy timeout）"""
        ...

    def get_connection_context(self, db_name: str = "kline"):
        """上下文管理器，自动关闭连接"""
        ...
```

**改造路径**：逐步将 12 个模块的 `sqlite3.connect()` 替换为 `get_sync_connection()`，最终消除所有 inline connect。

### 3.5 async_helper（新模块）

**文件**：`services/common/async_helper.py`

**职责**：统一的事件循环兼容执行器，替代 5 种不同实现

```python
def run_sync_in_thread(sync_fn, *args, **kwargs):
    """在有运行事件循环的上下文中，安全执行同步阻塞调用"""
    # 检测是否有运行中的事件循环
    # 如果有 → 在新线程+新事件循环中执行
    # 如果没有 → 直接执行
    ...

def run_async_safe(coro_fn, *args, **kwargs):
    """在非异步上下文中安全调用异步函数"""
    ...

def ensure_async(fn, *args, **kwargs):
    """将同步函数包装为异步执行（自动线程池）"""
    ...
```

**替代的实现：**
1. `download_weekly_kline.py:download_weekly_kline_sync()`
2. `scheduler_service.py:_run_kline_download()`
3. `local_data_service.py:calculate_and_save_factors_for_dates_async()`
4. `scheduler_service.py:_execute_strategy_task_job()`
5. `scheduler_service.py` 的多个 `run_manual_*` 方法

### 3.6 stock_code（扩展现有）

**文件**：`services/common/stock_code.py`（如不存在则新建）

**职责**：股票代码格式标准化

```python
def normalize(code: str) -> str:
    """600000 → 600000.SH, 000001 → 000001.SZ"""
    ...

def get_market(code: str) -> str:
    """600000.SH → SH"""
    ...

def is_sh(code: str) -> bool:
def is_sz(code: str) -> bool:
def is_bj(code: str) -> bool:
def filter_codes(codes: list, markets: list = None) -> list:
    """按市场筛选股票代码"""
    ...
```

---

## 四、横切模块设计

### 4.1 circuit_breaker

**文件**：`services/common/circuit_breaker.py`

SDK 连续失败 N 次后自动熔断，避免无限重试消耗连接资源：

```python
class CircuitBreaker:
    def __init__(self, failure_threshold=5, recovery_timeout=300):
        ...

    def execute(self, fn, *args, **kwargs):
        """执行函数，自动熔断/恢复"""
        ...
```

### 4.2 error_reporter

**文件**：`services/common/error_reporter.py`

统一错误上报格式，替代 `print()` / `logger.warning()` / `logger.error()` 混用：

```python
class ErrorReporter:
    def report(self, component: str, error: Exception, context: dict = None):
        """统一格式：[组件] 错误描述 + 上下文 + 时间戳"""
        ...

    def warn(self, component: str, message: str, context: dict = None):
        ...
```

---

## 五、改造实施优先级

| 阶段 | 模块 | 工作量 | 收益 | 风险 |
|------|------|--------|------|------|
| 1 | **db_connector** | 中 | 所有模块受益 | 需逐步迁移，不可一次性替换 |
| 2 | **async_helper** | 小 | 消除 5 种事件循环实现 | 低风险，纯工具函数 |
| 3 | **factor_pipeline** | 大 | 消除 3 份重复代码，最直观 | 需确保因子计算结果完全一致 |
| 4 | **financial_service** | 中 | 消除 N+1 SDK 调用 | 需统一比值计算逻辑 |
| 5 | **kline_manager** | 大 | 统一 K 线下载/查询/入库 | 涉及数据完整性，需充分测试 |
| 6 | **stock_code** | 小 | 代码格式统一 | 低风险 |
| 7 | **circuit_breaker** | 小 | SDK 稳定性提升 | 需调优阈值 |
| 8 | **error_reporter** | 小 | 日志格式统一 | 低风险 |

---

## 六、已完成的基础改造

| 改造项 | 提交 | 说明 |
|--------|------|------|
| SDKManager 集成连接管理器 | `b798120` | 所有 SDK 数据调用自动排队 |
| 修复 4 处 TGW bypass | `b798120` | gateway/positions/agent/scheduler |
| 数据库连接弹性 | `b798120` | `_ensure_connection()` 检测损坏连接 |
| 任务调度状态管理 | `27aabc2` | reset_task → fail_task，plugin 真实状态反馈 |
| Phase 3 handler bug 修复 | `9850f87` | log_action 缺 await/risk_level |
