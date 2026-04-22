# StockWinner 系统设计文档

**版本**: v6.2.4
**最后更新**: 2026-04-07
**项目**: 智能股票交易系统

---

## 1. 系统概述

StockWinner 是一个基于 Ubuntu 的多账户智能股票交易系统，集成了银河证券 SDK，支持股票数据下载、因子计算、选股策略执行和交易监控。

### 1.1 核心架构
- **平台**: Ubuntu 22.04/24.04 LTS (x86_64)
- **语言**: Python 3.10+
- **数据库**: SQLite 3.x (WAL 模式)
- **SDK**: AmazingData-1.0.30 / tgw-1.0.8.5 (银河证券交易网关)
- **后端**: FastAPI + Uvicorn (端口 8080)
- **前端**: Vue 3 + Vite + Element Plus

### 1.2 设计原则
1. **多账户隔离**: 共享数据库，逻辑隔离，每个账户独立的数据视图
2. **因子优先**: 预计算因子入库，选股时按需读取，减少重复计算
3. **SDK 在线模式**: 避免本地数据下载，直接调用在线 API 获取行情和财务数据
4. **进度可恢复**: 长时间任务支持断点续跑
5. **API 化数据服务**: 统一数据查询接口，支持高级筛选、聚合统计、数据导出

### 1.3 v6.2.3 新增功能 (2026-04-07)
- ✅ 高级筛选 API（12 种操作符）
- ✅ 聚合统计 API（GROUP BY + 聚合函数）
- ✅ 预设筛选模板（5 个策略模板）
- ✅ 股票基本信息查询
- ✅ 数据导出功能（CSV/JSON）
- ✅ 数据新鲜度检查
- ✅ K 线数据 API 增强（支持 11 种周期、灵活时间范围）

### 1.4 K 线数据 API 详情

#### 支持的 K 线周期

| 周期代码 | 说明 | SDK Period 枚举 | 值 |
|---------|------|----------------|-----|
| 1m | 1 分钟 K 线 | min1 | 10000 |
| 3m | 3 分钟 K 线 | min3 | 10001 |
| 5m | 5 分钟 K 线 | min5 | 10002 |
| 10m | 10 分钟 K 线 | min10 | 10003 |
| 15m | 15 分钟 K 线 | min15 | 10004 |
| 30m | 30 分钟 K 线 | min30 | 10005 |
| 60m | 60 分钟 K 线 | min60 | 10006 |
| 120m | 120 分钟 K 线 | min120 | 10007 |
| day | 日线 | day | 10008 |
| week | 周线 | week | 10009 |
| month | 月线 | month | 10010 |

#### 时间范围快捷选择

| 参数值 | 说明 | 对应天数 |
|-------|------|---------|
| 7d | 最近 7 天 | 7 |
| 30d | 最近 30 天 | 30 |
| 90d | 最近 90 天 | 90 |
| 180d | 最近 180 天 | 180 |
| 1y | 最近 1 年 | 365 |
| 2y | 最近 2 年 | 730 |
| 5y | 最近 5 年 | 1825 |
| 10y | 最近 10 年 | 3650 |
| all | 全部可用数据 | - |
| custom | 自定义范围（需配合 start_date/end_date） | - |

#### API 端点

```
GET /api/v1/ui/{account_id}/market/kline

参数:
- account_id: 路径参数，账户 ID
- stock_code: 查询参数，股票代码（支持 600519 或 600519.SH 格式）
- period: 查询参数，K 线周期（默认：day）
- start_date: 查询参数，开始日期（YYYYMMDD 格式）
- end_date: 查询参数，结束日期（YYYYMMDD 格式）
- limit: 查询参数，返回数量限制（默认：100，最大：10000）
- time_range: 查询参数，快捷时间范围（优先级低于 start_date/end_date）

响应示例:
{
    "success": true,
    "data": {
        "stock_code": "600519.SH",
        "period": "day",
        "count": 100,
        "start_date": "20260101",
        "end_date": "20260407",
        "kline": [
            {
                "stock_code": "600519.SH",
                "kline_time": "2026-04-07",
                "open": 1500.00,
                "high": 1520.00,
                "low": 1495.00,
                "close": 1510.00,
                "volume": 1234567,
                "amount": 1850000000.00
            }
        ]
    }
}
```

---

## 2. 数据库设计

### 2.1 数据库文件

| 数据库 | 文件 | 用途 |
|--------|------|------|
| kline.db | data/kline.db | 行情数据和因子数据 |
| stockwinner.db | data/stockwinner.db | 业务数据（账户、策略、交易） |

### 2.2 kline.db 核心表

#### kline_data (K 线行情表)
```sql
CREATE TABLE kline_data (
    id INTEGER PRIMARY KEY,
    stock_code TEXT NOT NULL,
    stock_name TEXT,
    kline_time TEXT NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume REAL,
    amount REAL
);
```
- 数据范围：2021-04-02 至 2026-04-03
- 记录数：约 600 万 +

#### stock_daily_factors (日频因子表)
```sql
CREATE TABLE stock_daily_factors (
    id INTEGER PRIMARY KEY,
    stock_code TEXT NOT NULL,
    stock_name TEXT,
    trade_date TEXT NOT NULL,
    -- 市值类 (3)
    circ_market_cap REAL,      -- 流通市值（元）
    total_market_cap REAL,     -- 总市值（元）
    days_since_ipo INTEGER,    -- 上市天数
    -- 市场表现类 (14)
    change_10d REAL, change_20d REAL,
    bias_5 REAL, bias_10 REAL, bias_20 REAL,
    amplitude_5 REAL, amplitude_10 REAL, amplitude_20 REAL,
    change_std_5 REAL, change_std_10 REAL, change_std_20 REAL,
    amount_std_5 REAL, amount_std_10 REAL, amount_std_20 REAL,
    -- 技术指标类 (3)
    kdj_k REAL, kdj_d REAL, kdj_j REAL,
    dif REAL, dea REAL, macd REAL,
    -- 扩展技术指标 (17)
    ma5 REAL, ma10 REAL, ma20 REAL, ma60 REAL,
    ema12 REAL, ema26 REAL, adx REAL, rsi_14 REAL, cci_20 REAL,
    momentum_10d REAL, momentum_20d REAL,
    boll_upper REAL, boll_middle REAL, boll_lower REAL,
    atr_14 REAL, hv_20 REAL, obv REAL, volume_ratio REAL,
    golden_cross INTEGER, death_cross INTEGER,
    -- 特色因子 (10)
    limit_up_count_10d INTEGER, limit_up_count_20d INTEGER, limit_up_count_30d INTEGER,
    consecutive_limit_up INTEGER, first_limit_up_days INTEGER,
    highest_board_10d INTEGER, large_gain_5d_count INTEGER, large_loss_5d_count INTEGER,
    gap_up_ratio REAL, close_to_high_250d REAL, close_to_low_250d REAL,
    -- 估值类 (2)
    pe_inverse REAL, pb_inverse REAL,
    -- 目标变量
    next_period_change REAL,
    is_traded INTEGER,
    -- 基本面因子 (12，预留)
    pe_ttm REAL, pb REAL, ps_ttm REAL, pcf REAL,
    roe REAL, roa REAL, gross_margin REAL, net_margin REAL,
    revenue_growth_yoy REAL, revenue_growth_qoq REAL,
    net_profit_growth_yoy REAL, net_profit_growth_qoq REAL,
    -- 元数据
    source TEXT,
    updated_at TEXT
);
```
- 记录数：5,841,837
- 数据范围：2021-04-02 至 2026-04-03
- 覆盖率：100%

#### stock_monthly_factors (月频因子表)
```sql
CREATE TABLE stock_monthly_factors (
    id INTEGER PRIMARY KEY,
    stock_code TEXT NOT NULL,
    stock_name TEXT,
    trade_date TEXT NOT NULL,
    -- 财报时间 (2)
    report_quarter TEXT,
    report_year INTEGER,
    -- 利润类 (7)
    net_profit REAL,
    net_profit_ttm REAL,
    net_profit_ttm_yoy REAL,
    net_profit_single REAL,
    net_profit_single_yoy REAL,
    net_profit_single_qoq REAL,
    -- 现金流类 (7)
    operating_cash_flow REAL,
    operating_cash_flow_ttm REAL,
    operating_cash_flow_ttm_yoy REAL,
    operating_cash_flow_single REAL,
    operating_cash_flow_single_yoy REAL,
    operating_cash_flow_single_qoq REAL,
    -- 资产类 (1)
    net_assets REAL,
    -- 行业分类 (3)
    sw_level1 TEXT,
    sw_level2 TEXT,
    sw_level3 TEXT
);
```
- 记录数：295,320
- 日期：每月最后一个交易日

### 2.3 stockwinner.db 核心表

#### accounts (账户表)
```sql
CREATE TABLE accounts (
    id INTEGER PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    broker TEXT DEFAULT '银河证券',
    status TEXT DEFAULT 'active',
    credentials_encrypted BLOB,
    created_at TEXT,
    updated_at TEXT
);
```

#### strategies (策略表)
```sql
CREATE TABLE strategies (
    id INTEGER PRIMARY KEY,
    account_id INTEGER,
    name TEXT NOT NULL,
    description TEXT,
    conditions TEXT,  -- JSON 格式存储选股条件
    match_threshold REAL DEFAULT 0.8,
    stop_loss_ratio REAL DEFAULT 0.1,
    take_profit_ratio REAL DEFAULT 0.2,
    status TEXT DEFAULT 'draft',  -- draft/active/inactive
    created_at TEXT,
    updated_at TEXT,
    FOREIGN KEY (account_id) REFERENCES accounts(id)
);
```

#### temp_candidates (临时候选股票表)
```sql
CREATE TABLE temp_candidates (
    id INTEGER PRIMARY KEY,
    account_id INTEGER,
    strategy_id INTEGER,
    stock_code TEXT NOT NULL,
    stock_name TEXT,
    match_time TEXT,
    match_score REAL,
    FOREIGN KEY (strategy_id) REFERENCES strategies(id) ON DELETE CASCADE
);
```

---

## 3. 后端服务架构

### 3.1 目录结构
```
services/
├── main.py                    # FastAPI 主入口
├── common/                    # 公共模块
│   ├── account_manager.py     # 账户管理
│   ├── database.py            # 数据库连接管理
│   ├── indicators.py          # 技术指标（兼容层）
│   ├── timezone.py            # 时区处理
│   ├── sdk_manager.py         # SDK 登录管理（单例）
│   ├── stock_code.py          # 股票代码格式化
│   ├── config.py              # 配置管理
│   └── logging_config.py      # 日志配置
├── trading/                   # 交易网关
│   └── gateway.py             # 银河 SDK 集成（模拟 + 真实）
├── screening/                 # 选股服务
│   ├── service.py             # 选股执行
│   └── factor_registry.py     # 因子注册表
├── monitoring/                # 监控服务
│   └── service.py             # 交易监控
├── data/                      # 数据服务
│   ├── local_data_service.py  # 本地数据管理
│   └── download.py            # 数据下载
├── factors/                   # 因子计算模块
│   ├── sdk_api.py             # SDK API 封装
│   ├── daily_factor_calculator.py   # 日频因子计算器
│   ├── monthly_factor_calculator.py # 月频因子计算器
│   ├── fundamental_factor_calculator.py # 基本面因子
│   ├── migrate_factors.py     # 数据迁移工具
│   ├── extend_factors_table.py # 表结构扩展
│   ├── correct_market_cap.py  # 市值校正工具
│   ├── correct_daily_factors.py # 因子校正工具
│   ├── batch_update_factors.py # 批量更新工具
│   └── incremental_update_factors.py # 增量更新工具
└── ui/                        # UI API
    ├── accounts.py            # 账户管理 API
    ├── dashboard.py           # 仪表盘 API
    ├── positions.py           # 持仓 API
    ├── trades.py              # 交易 API
    ├── strategies.py          # 策略 API
    ├── screening.py           # 选股 API
    ├── monitoring.py          # 监控 API
    ├── stock_factors_viewer.py # 因子数据查看 API
    └── data_explorer.py       # 数据浏览器 API
```

### 3.2 核心服务流程

#### 3.2.1 因子计算流程
```
1. incremental_update_factors.py
   └─→ 获取缺失数据的股票列表
   └─→ 对每只股票调用 DailyFactorCalculator
       └─→ 从 kline_data 获取 K 线
       └─→ 从 SDK 获取股本结构（计算市值）
       └─→ 计算技术指标（KDJ, MACD, 均线等）
       └─→ 计算市场表现因子（涨跌幅、乖离率、振幅等）
       └─→ 计算特色因子（涨停统计、异动统计等）
       └─→ 更新 stock_daily_factors 表
```

#### 3.2.2 选股执行流程
```
1. 用户触发选股 → POST /api/v1/ui/{account_id}/screening/run
2. screening/service.py
   └─→ 读取策略条件（如 "MA5 > MA20 AND RSI < 30"）
   └─→ 解析条件，提取需要的因子
   └─→ 从 stock_daily_factors 读取预计算因子
   └─→ 动态计算缺失因子（如 MA5, MA20, RSI）
   └─→ 筛选符合条件的股票
   └─→ 写入 temp_candidates 表
   └─→ 更新选股进度
```

---

## 4. SDK 集成

### 4.1 SDK 信息
- **名称**: AmazingData (银河证券行情数据 SDK)
- **版本**: 1.0.30
- **登录**: 在线模式（is_local=False）
- **账号**: REDACTED_SDK_USERNAME
- **有效期**: 2026-03-10 至 2027-03-05

### 4.2 已封装的 API

```python
# services/common/sdk_manager.py
class SDKManager:
    def get_equity_structure(stock_codes: list) -> pd.DataFrame
        # 返回：MARKET_CODE, ANN_DATE, TOT_SHARE, FLOAT_SHARE 等
    
    def get_income_statement(stock_codes: list) -> pd.DataFrame
        # 返回：NET_PRO_INCL_MIN_INT_INC 等利润表数据
    
    def get_balance_sheet(stock_codes: list) -> pd.DataFrame
        # 返回：TOT_SHARE_EQUITY_EXCL_MIN_INT 等资产负债表数据
    
    def get_cash_flow_statement(stock_codes: list) -> pd.DataFrame
        # 返回：经营现金流等现金流量表数据
    
    def get_industry_base_info() -> pd.DataFrame
        # 返回：行业分类基础信息
```

### 4.3 单位转换规则

| 数据类型 | SDK 单位 | 内部单位 | 转换公式 |
|---------|---------|---------|---------|
| 股本 | 万股 | 万股 | 无需转换 |
| 财务数据 | 元 | 亿元 | ÷ 100,000,000 |
| 市值 | - | 元 | 股本 (万股) × 股价 (元) × 10000 |

---

## 5. API 端点

### 5.1 账户管理
- `GET /api/v1/ui/accounts` - 获取账户列表
- `GET /api/v1/ui/accounts/{account_id}` - 获取账户详情

### 5.2 仪表盘
- `GET /api/v1/ui/{account_id}/dashboard` - 仪表盘总览
- `GET /api/v1/ui/{account_id}/health` - 健康检查

### 5.3 策略管理
- `GET /api/v1/ui/{account_id}/strategies` - 策略列表
- `POST /api/v1/ui/{account_id}/strategies` - 创建策略
- `POST /api/v1/ui/{account_id}/strategies/generate` - LLM 生成策略
- `PUT /api/v1/ui/{account_id}/strategies/{strategy_id}` - 更新策略
- `DELETE /api/v1/ui/{account_id}/strategies/{strategy_id}` - 删除策略

### 5.4 选股服务
- `POST /api/v1/ui/{account_id}/screening/run` - 执行选股
- `GET /api/v1/ui/{account_id}/screening/status` - 服务状态
- `GET /api/v1/ui/{account_id}/watchlist` - 候选股票池

### 5.5 因子数据管理
- `GET /api/v1/ui/stock-factors` - 获取因子数据列表
- `GET /api/v1/ui/stock-factors/{stock_code}` - 获取单只股票因子数据
- `GET /api/v1/ui/stock-factors/stats/overview` - 因子数据统计概览
- `GET /api/v1/ui/stock-factors/search` - 搜索因子数据
- `PUT /api/v1/ui/stock-factors/{stock_code}/update` - 更新因子数据
- `DELETE /api/v1/ui/stock-factors/{stock_code}/delete` - 删除因子数据

### 5.6 数据浏览器 (v6.2.3 新增)

**基础查询**:
- `GET /api/v1/ui/databases` - 获取数据库列表
- `GET /api/v1/ui/databases/{db_name}/tables` - 获取表列表
- `GET /api/v1/ui/databases/{db_name}/tables/{table_name}/stats` - 获取表统计
- `GET /api/v1/ui/databases/{db_name}/tables/{table_name}/columns` - 获取表结构
- `GET /api/v1/ui/databases/{db_name}/tables/{table_name}/data` - 获取表数据（增强版，支持筛选）

**高级筛选**:
- `POST /api/v1/ui/databases/{db_name}/tables/{table_name}/query` - 高级筛选查询
  - 支持操作符：`eq`, `ne`, `gt`, `gte`, `lt`, `lte`, `in`, `not_in`, `between`, `like`, `is_null`, `is_not_null`
  - 支持字段选择、多字段排序、分页

**聚合统计**:
- `POST /api/v1/ui/databases/{db_name}/tables/{table_name}/aggregate` - 聚合统计
  - 支持 `GROUP BY` 分组
  - 支持聚合函数：`count`, `sum`, `avg`, `max`, `min`

**数据导出**:
- `POST /api/v1/ui/databases/{db_name}/tables/{table_name}/export` - 数据导出
  - 支持格式：CSV, JSON

### 5.7 筛选模板 (v6.2.3 新增)
- `GET /api/v1/ui/screening/templates` - 获取预设模板列表
- `GET /api/v1/ui/screening/templates/{template_id}` - 获取模板详情
- `POST /api/v1/ui/screening/template/{template_id}` - 应用筛选模板

### 5.8 股票信息 (v6.2.3 新增)
- `GET /api/v1/ui/stocks` - 获取股票列表（支持行业筛选、模糊搜索）
- `GET /api/v1/ui/stocks/{stock_code}` - 获取单只股票详情

### 5.9 数据新鲜度 (v6.2.3 新增)
- `GET /api/v1/ui/data/freshness` - 检查各数据表的新鲜度

---

## 6. 前端页面

| 页面 | 路由 | 功能 |
|------|------|------|
| 仪表盘 | `/dashboard` | 系统健康度、交易统计、资源开销 |
| 选股监控 | `/watchlist` | 候选股票池、服务状态 |
| 交易信号 | `/signals` | 交易信号列表、执行/取消 |
| 交易监控 | `/trades` | 交易流水、统计汇总 |
| 持仓分析 | `/positions` | 持仓列表、盈亏分布 |
| 策略管理 | `/strategies` | 策略列表、创建、回测 |
| 因子数据 | `/ui/stock-factors.html` | 因子数据查看、编辑、统计 |
| 数据浏览 | `/data-explorer` | 数据库表浏览、数据查询 |
| 系统设置 | `/settings` | API 配置、账户信息 |

---

## 7. 部署指南

### 7.1 环境准备
```bash
# 创建虚拟环境
python3 -m venv venv
source venv/bin/activate

# 安装依赖
pip install -r requirements.txt

# 配置账户
cp config/accounts.json.example config/accounts.json
# 编辑 config/accounts.json

# 初始化数据库
python scripts/init_db.py
```

### 7.2 启动服务
```bash
# 后端（端口 8080）
python -m uvicorn services.main:app --host 0.0.0.0 --port 8080

# 后台运行
nohup python -m uvicorn services.main:app --host 0.0.0.0 --port 8080 > logs/app.log 2>&1 &

# 停止服务
lsof -ti:8080 | xargs kill -9
```

### 7.3 前端
```bash
cd frontend
npm install
npm run build

# 访问：http://localhost:8080/ui/
```

---

## 8. 运维监控

### 8.1 健康检查
```bash
# API 健康检查
curl http://localhost:8080/api/v1/health

# 数据库检查
sqlite3 data/kline.db "SELECT COUNT(*) FROM stock_daily_factors;"
sqlite3 data/stockwinner.db "SELECT COUNT(*) FROM accounts;"
```

### 8.2 日志查看
```bash
# 应用日志
tail -f logs/app.log

# 错误日志
grep ERROR logs/app.log | tail -20
```

### 8.3 数据完整性检查
```bash
# 检查因子数据覆盖率
python3 << 'EOF'
import sqlite3
conn = sqlite3.connect("data/kline.db")
cursor = conn.cursor()
cursor.execute("""
    SELECT 
        COUNT(*) as total,
        SUM(CASE WHEN circ_market_cap IS NOT NULL THEN 1 ELSE 0 END) as has_cap,
        SUM(CASE WHEN days_since_ipo IS NOT NULL THEN 1 ELSE 0 END) as has_days
    FROM stock_daily_factors
""")
row = cursor.fetchone()
print(f"总记录：{row[0]}")
print(f"市值覆盖率：{row[1]/row[0]*100:.1f}%")
print(f"上市天数覆盖率：{row[2]/row[0]*100:.1f}%")
conn.close()
EOF
```

---

## 9. 故障排查

### 9.1 SDK 不可用
**症状**: 因子计算时市值数据为 NULL
**原因**: SDK 需要在虚拟环境中运行
**解决**: 
```bash
source venv/bin/activate
PYTHONPATH=/home/bobo/StockWinner python3 services/factors/correct_market_cap.py
```

### 9.2 选股进度不更新
**症状**: 前端进度条卡在 0%
**原因**: `screening.running` 状态未正确设置
**解决**: 检查 `frontend/src/views/Watchlist.vue` 轮询逻辑，确保使用 `current_phase` 判断

### 9.3 删除策略失败
**症状**: 删除策略时外键约束报错
**原因**: temp_candidates 表有引用
**解决**: 
```sql
DELETE FROM temp_candidates WHERE strategy_id = ?;
DELETE FROM strategies WHERE id = ?;
```

---

## 10. 待完成工作

### 高优先级
- [ ] 基本面因子计算（PE-TTM, PB, ROE 等）
- [ ] 行业分类数据完善
- [ ] 因子有效性检验（IC 计算、分层回测）

### 中优先级
- [ ] 真实交易执行接口
- [ ] 持仓查询接口
- [ ] 委托查询接口
- [ ] 行情推送优化

### 低优先级
- [ ] 部署测试
- [ ] 性能基准测试
- [ ] 代码重构（统一数据库连接管理）

---

## 附录

### A. 版本历史
| 版本 | 日期 | 核心功能 |
|------|------|----------|
| v6.2.3 | 2026-04-07 | API 数据查询功能增强（高级筛选、聚合统计、筛选模板、数据导出） |
| v6.2.2 | 2026-04-06 | 因子数据完整性修复、文档整理 |
| v6.2.1 | 2026-04-03 | 因子数据迁移、SDK 集成 |
| v6.2.0 | 2026-03-30 | 选股优化、代码重构 |
| v6.1.2 | 2026-03-29 | 多账户支持、数据隔离 |

### B. 关键文件清单
- `services/factors/daily_factor_calculator.py` - 日频因子计算器（核心）
- `services/factors/incremental_update_factors.py` - 增量更新工具
- `services/screening/service.py` - 选股服务
- `services/common/sdk_manager.py` - SDK 管理器
- `services/ui/data_explorer.py` - 数据浏览器 API（v6.2.3 新增）
- `config/screening_templates.json` - 筛选模板配置（v6.2.3 新增）
- `data/kline.db` - 行情和因子数据库
- `data/stockwinner.db` - 业务数据库

---

**文档结束**
