# StockWinner 智能股票交易系统 v6.2.4

多账户智能股票交易系统 - 共享数据库 + 逻辑隔离 + UI 监控仪表盘 + 高级数据查询

## 版本信息
- **版本**: v6.2.4
- **平台**: Ubuntu 22.04/24.04 LTS (x86_64)
- **Python**: 3.10+
- **数据库**: SQLite 3.x (WAL 模式)
- **SDK**: AmazingData-1.0.30 / tgw-1.0.8.5 (银河证券交易网关)

## 核心特性
- ✅ 多账户支持（2-10 个账户）
- ✅ 数据库多租户隔离
- ✅ API 多账户路由
- ✅ UI 账户切换功能
- ✅ 银河 SDK 集成（真实行情 + 模拟 fallback）
- ✅ 技术指标库（MA/RSI/MACD/BOLL/KDJ/ATR/CCI/ADX）
- ✅ 选股服务 + 交易监控
- ✅ Tailscale 远程访问支持
- ✅ 因子数据管理（日频/月频因子表）
- ✅ SDK 数据集成（财务数据、股本结构、行业分类）
- 🆕 高级筛选 API（12 种操作符）
- 🆕 聚合统计 API（GROUP BY + 聚合函数）
- 🆕 预设筛选模板（5 个策略模板）
- 🆕 股票基本信息查询
- 🆕 数据导出功能（CSV/JSON）
- 🆕 数据新鲜度检查
- 🆕 K 线数据 API 增强（支持 11 种周期、灵活时间范围）

## 快速开始

### 1. 安装依赖
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. 配置账户
```bash
cp config/accounts.json.example config/accounts.json
# 编辑 config/accounts.json，填入账户信息
```

### 3. 初始化数据库
```bash
python scripts/init_db.py
```

### 4. 启动服务
```bash
python -m uvicorn services.main:app --host 0.0.0.0 --port 8080
```

### 5. 前端开发
```bash
cd frontend
npm install
npm run dev
```

## 目录结构
```
StockWinner/
├── config/                  # 配置文件
│   └── accounts.json        # 账户配置
├── data/                    # 数据库文件
│   └── stockwinner.db       # SQLite 数据库
├── docs/                    # 文档
│   ├── GALAXY_SDK_INTEGRATION.md  # SDK 集成文档
│   ├── COMPLETION_SUMMARY.md      # 完成总结
│   └── DEV_LOG_20260330.md        # 开发日志
├── frontend/                # Vue 3 前端
│   ├── src/
│   │   ├── components/      # 组件
│   │   ├── views/           # 页面
│   │   ├── router/          # 路由
│   │   └── stores/          # 状态管理
│   └── dist/                # 构建输出
├── logs/                    # 日志文件
├── memory/                  # 记忆文档
├── scripts/                 # 脚本工具
│   └── init_db.py           # 数据库初始化
├── services/                # 后端服务
│   ├── common/              # 公共模块
│   │   ├── account_manager.py
│   │   ├── database.py
│   │   ├── indicators.py    # 技术指标
│   │   ├── timezone.py      # 时区工具
│   │   ├── sdk_manager.py   # SDK 管理器
│   │   ├── stock_code.py    # 股票代码格式化
│   │   ├── config.py        # 配置管理
│   │   └── logging_config.py # 日志配置
│   ├── trading/             # 交易网关
│   │   └── gateway.py       # 银河 SDK 集成
│   ├── screening/           # 选股服务
│   │   ├── service.py
│   │   └── factor_registry.py # 因子注册表
│   ├── monitoring/          # 监控服务
│   │   └── service.py
│   ├── ui/                  # UI API
│   │   ├── accounts.py
│   │   ├── dashboard.py
│   │   ├── positions.py
│   │   ├── trades.py
│   │   ├── strategies.py
│   │   ├── screening.py
│   │   ├── monitoring.py
│   │   ├── stock_factors_viewer.py # 因子数据查看
│   │   └── data_explorer.py  # 数据浏览器
│   ├── data/                # 数据服务
│   │   ├── local_data_service.py
│   │   └── download.py
│   └── factors/             # 因子计算模块
│       ├── sdk_api.py       # SDK API 封装
│       ├── daily_factor_calculator.py  # 日频因子计算器
│       ├── monthly_factor_calculator.py # 月频因子计算器
│       ├── fundamental_factor_calculator.py # 基本面因子计算器
│       ├── migrate_factors.py # 数据迁移工具
│       ├── extend_factors_table.py # 表结构扩展
│       ├── correct_market_cap.py # 市值校正工具
│       ├── correct_daily_factors.py # 因子校正工具
│       ├── batch_update_factors.py # 批量更新工具
│       └── incremental_update_factors.py # 增量更新工具
│   └── main.py              # 主入口
├── tests/                   # 测试文件
│   └── test_all.py          # 全系统测试
├── venv/                    # Python 虚拟环境
├── PROGRESS.md              # 开发进度
├── README.md                # 项目说明
└── requirements.txt         # Python 依赖
```

## API 端点

### 账户管理
- `GET /api/v1/ui/accounts` - 获取账户列表
- `GET /api/v1/ui/accounts/{account_id}` - 获取账户详情

### 仪表盘
- `GET /api/v1/ui/{account_id}/dashboard` - 仪表盘总览
- `GET /api/v1/ui/{account_id}/health` - 健康检查

### 持仓管理
- `GET /api/v1/ui/{account_id}/positions` - 持仓列表
- `GET /api/v1/ui/{account_id}/positions/{stock_code}` - 单只股票持仓

### 交易记录
- `GET /api/v1/ui/{account_id}/trades/today` - 今日交易
- `GET /api/v1/ui/{account_id}/trades` - 交易记录（支持筛选）

### 策略管理
- `GET /api/v1/ui/{account_id}/strategies` - 策略列表
- `POST /api/v1/ui/{account_id}/strategies` - 创建策略
- `POST /api/v1/ui/{account_id}/strategies/generate` - LLM 生成策略

### 选股服务
- `POST /api/v1/ui/{account_id}/screening/run` - 执行选股
- `GET /api/v1/ui/{account_id}/screening/status` - 服务状态
- `GET /api/v1/ui/{account_id}/watchlist` - 候选股票池

### 交易监控
- `GET /api/v1/ui/{account_id}/monitoring/status` - 服务状态
- `GET /api/v1/ui/{account_id}/signals` - 交易信号列表

### 因子数据管理
- `GET /api/v1/ui/stock-factors` - 获取因子数据列表
- `GET /api/v1/ui/stock-factors/{stock_code}` - 获取单只股票因子数据
- `GET /api/v1/ui/stock-factors/stats/overview` - 因子数据统计概览
- `GET /api/v1/ui/stock-factors/search` - 搜索因子数据

### 数据浏览器 (v6.2.3 新增)
**基础查询**:
- `GET /api/v1/ui/databases` - 获取数据库列表
- `GET /api/v1/ui/databases/{db_name}/tables` - 获取表列表
- `GET /api/v1/ui/databases/{db_name}/tables/{table_name}/stats` - 获取表统计
- `GET /api/v1/ui/databases/{db_name}/tables/{table_name}/columns` - 获取表结构
- `GET /api/v1/ui/databases/{db_name}/tables/{table_name}/data` - 获取表数据（增强版）

**高级筛选**:
- `POST /api/v1/ui/databases/{db_name}/tables/{table_name}/query` - 高级筛选查询
  - 支持 12 种操作符：eq, ne, gt, gte, lt, lte, in, not_in, between, like, is_null, is_not_null

**聚合统计**:
- `POST /api/v1/ui/databases/{db_name}/tables/{table_name}/aggregate` - 聚合统计
  - 支持 GROUP BY 和 count, sum, avg, max, min

**数据导出**:
- `POST /api/v1/ui/databases/{db_name}/tables/{table_name}/export` - 数据导出（CSV/JSON）

### 筛选模板 (v6.2.3 新增)
- `GET /api/v1/ui/screening/templates` - 获取预设模板列表
- `GET /api/v1/ui/screening/templates/{template_id}` - 获取模板详情
- `POST /api/v1/ui/screening/template/{template_id}` - 应用筛选模板

### 股票信息 (v6.2.3 新增)
- `GET /api/v1/ui/stocks` - 获取股票列表
- `GET /api/v1/ui/stocks/{stock_code}` - 获取单只股票详情

### 数据新鲜度 (v6.2.3 新增)
- `GET /api/v1/ui/data/freshness` - 检查数据表新鲜度

### K 线数据 (v6.2.4 新增)
- `GET /api/v1/ui/{account_id}/market/kline` - 获取 K 线历史数据
  - 支持周期：1m, 3m, 5m, 10m, 15m, 30m, 60m, 120m, day, week, month
  - 支持时间范围快捷选择：7d, 30d, 90d, 180d, 1y, 2y, 5y, 10y, all, custom
  - 支持自定义日期范围：start_date, end_date (YYYYMMDD 格式)
- `GET /api/v1/ui/{account_id}/market/kline/latest` - 获取最新一根 K 线数据

## 前端页面

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

## 文档

### 核心文档
- [设计文档](docs/StockWinner_Linux_v6.1.2_完整版.md) - 完整系统设计
- [开发进度](PROGRESS.md) - 详细开发日志
- [代码重构计划](docs/CODE_REFACTOR_PLAN.md) - 重构路线图

### SDK 集成
- [AmazingData SDK 检查清单](docs/AMAZING_DATA_CHECKLIST.md)
- [Galaxy SDK 集成](docs/GALAXY_SDK_INTEGRATION.md)
- [Galaxy API 指南](docs/GALAXY_API_GUIDE.md)

### API 文档
- [市场数据 API](docs/MARKET_DATA_API.md)
- [用户管理](docs/USER_MANAGEMENT.md)
- [账户管理](docs/ACCOUNT_MANAGEMENT.md)

### 部署和运维
- [部署指南](docs/DEPLOYMENT.md)
- [增量下载](docs/INCREMENTAL_DOWNLOAD.md)
- [选股监控](docs/SCREENING_MONITORING.md)

## 开发进度

### v6.2.4 (2026-04-07) - K 线数据 API 增强
- [x] K 线 API 支持 11 种周期（1m/3m/5m/10m/15m/30m/60m/120m/day/week/month）
- [x] K 线 API 支持灵活时间范围（7d/30d/90d/180d/1y/2y/5y/10y/all/custom）
- [x] 前端数据浏览器集成 K 线查询功能
- [x] gateway.py 周期映射更新
- [x] 系统设计文档更新
- [x] README.md 版本和历史更新

### v6.2.3 (2026-04-07) - API 数据查询功能增强
- [x] 高级筛选 API（12 种操作符：eq, ne, gt, gte, lt, lte, in, not_in, between, like, is_null, is_not_null）
- [x] 聚合统计 API（GROUP BY + count, sum, avg, max, min）
- [x] 筛选模板功能（5 个预设模板：小市值低估值、动量突破、优质成长、技术面买入、涨停潜力）
- [x] 股票基本信息查询（按行业筛选、模糊搜索）
- [x] 数据导出功能（CSV/JSON 格式）
- [x] 数据新鲜度检查
- [x] 后端服务重启并验证

### v6.2.2 (2026-04-06) - 因子数据完整性修复和文档整理
- [x] 因子数据完整性修复（市值、上市天数 100% 覆盖）
- [x] 代码清理（删除冗余脚本和临时文件）
- [x] 文档整理（SYSTEM_DESIGN.md, PROGRESS.md, README.md）

### v6.2.1 (2026-04-03) - 因子数据迁移和 SDK 集成
- [x] 因子数据迁移工具（stock_daily_factors / stock_monthly_factors）
- [x] AmazingData SDK 集成（在线模式）
- [x] 日频因子计算器（市值、估值、技术指标）
- [x] 月频因子计算器（财务数据、行业分类）
- [x] 通用数据浏览器
- [x] 因子数据查看页面

### v6.2.0 (2026-03-30) - 选股优化和代码重构
- [x] 选股服务优化（因子优先，按需计算）
- [x] 因子注册表模块
- [x] 代码重构（统一技术指标、时区处理、SDK 管理）

### v6.1.3 (2026-03-29) - 多账户支持和 UI 监控
- [x] 项目基础架构
- [x] 多账户管理器
- [x] 数据库层
- [x] FastAPI 主应用和路由
- [x] 仪表盘 API
- [x] 持仓/交易 API
- [x] 前端 UI 项目
- [x] 账户切换组件
- [x] 仪表盘页面
- [x] 策略管理模块
- [x] 选股服务 + 监控服务
- [x] 技术指标模块
- [x] 银河 SDK 集成
- [x] 全系统测试 (5/5 通过)
- [ ] 真实交易执行
- [ ] 部署测试

## 测试

```bash
# 全系统测试
python tests/test_all.py

# API 测试
curl http://localhost:8080/api/v1/health
curl http://localhost:8080/api/v1/ui/accounts
curl http://localhost:8080/api/v1/ui/bobo/dashboard

# 前端访问
http://localhost:8080/ui/
```

## 系统服务

```bash
# 启动服务（后台运行）
cd /home/bobo/StockWinner
source venv/bin/activate
nohup python3 -m uvicorn services.main:app --host 0.0.0.0 --port 8080 > logs/app.log 2>&1 &

# 查看服务状态
ps aux | grep uvicorn

# 停止服务
lsof -ti:8080 | xargs kill -9
```

## 技术栈

**后端**:
- FastAPI + Uvicorn
- SQLite (WAL 模式)
- tgw (银河证券 SDK)
- AmazingData (银河证券行情数据 SDK)

**前端**:
- Vue 3 + Vite
- Element Plus
- Pinia
- Vue Router
- ECharts

**部署**:
- Tailscale (远程访问)
- Ubuntu 22.04/24.04 LTS

## 数据库表结构

### 核心业务表 (stockwinner.db)
- `accounts` - 账户信息
- `positions` - 持仓记录
- `trades` - 交易流水
- `strategies` - 投资策略
- `watchlist` - 关注列表
- `candidate_stocks` - 候选股票
- `trading_signals` - 交易信号

### 行情数据表 (kline.db)
- `kline_data` - K 线行情数据
- `stock_daily_factors` - 日频因子表（580 万 + 记录）
- `stock_monthly_factors` - 月频因子表（29 万 + 记录）
- `stock_factors` - 历史因子表（ legacy，69 万 + 记录）

## 版本历史

| 版本 | 日期 | 核心功能 |
|------|------|----------|
| v6.2.4 | 2026-04-07 | K 线数据 API 增强（支持 11 种周期：1m/3m/5m/10m/15m/30m/60m/120m/day/week/month，灵活时间范围：7d/30d/90d/180d/1y/2y/5y/10y/all/custom） |
| v6.2.3 | 2026-04-07 | API 数据查询功能增强（高级筛选、聚合统计、筛选模板、数据导出、数据新鲜度） |
| v6.2.2 | 2026-04-06 | 因子数据完整性修复、文档整理 |
| v6.2.1 | 2026-04-03 | 因子数据迁移、SDK 集成 |
| v6.2.0 | 2026-03-30 | 选股优化、代码重构 |
| v6.1.3 | 2026-03-29 | 多账户支持、UI 监控 |

---

**项目状态**: ✅ 核心 API 功能开发完成（90% 完成率）
