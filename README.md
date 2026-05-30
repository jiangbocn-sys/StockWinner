# StockWinner 智能股票交易系统 v7.8.19

多账户智能股票交易系统 - Agent API + SDK子进程隔离 + 策略沙箱 + 优先级队列 + 系统配置管理 + 信号分配 + MCP服务 + K线增强

## 版本信息
- **版本**: v7.8.19
- **平台**: Ubuntu 22.04/24.04 LTS (x86_64)
- **Python**: 3.10+
- **数据库**: SQLite 3.x (WAL 模式) - 双库架构
- **SDK**: AmazingData-1.0.30 / tgw-1.0.8.5 (银河证券交易网关)

---

## 核心特性

### 系统架构
- ✅ SDK 子进程隔离架构（Unix socket IPC + 4 级优先级队列）
- ✅ 多账户逻辑隔离（UI 账户切换 + 策略虚拟账户）
- ✅ Agent API 系统（12 端点分类，70+ API，完整审计）
- ✅ 用户认证系统（JWT + 角色权限）
- ✅ 策略沙箱执行（AST 校验 + 受限 builtins + 版本管理）
- ✅ 系统配置管理（管理员可调 TTL/监控间隔/熔断器参数）

### 数据能力
- ✅ PriceCache 内存行情缓存（动态 TTL + source 优先级 + 容量管理）
- ✅ 多数据源架构（6 个 Provider 自动切换降级）
- ✅ 因子数据管理（日频 62 字段 + 月频 23 字段）
- ✅ K 线数据（日/周/月，11 种周期，增量同步）
- ✅ 技术指标库（MA/RSI/MACD/BOLL/KDJ/ATR/CCI/ADX）

### 交易能力
- ✅ 选股服务（配置型条件筛选 + 代码型策略）
- ✅ 交易监控（三层策略优先级：个股止盈止损 > 代码策略 > watchlist）
- ✅ 回测系统（撮合模拟盘 + 收益率累积 + 止盈止损 + T+1）
- ✅ 止盈止损管理（固定价/百分比/移动止损/触发价成交）
- ✅ 策略资金管理（借入/归还/现金调整）

### Agent API
- ✅ 4 级角色权限（viewer/strategist/operator/admin）
- ✅ 操作风险分级（low/medium/high/critical）
- ✅ 幂等请求缓存（5 分钟去重）
- ✅ 速率限制（角色差异化 Token Bucket）

### MCP 服务
- ✅ Claude Desktop/Code 自然语言交互支持
- ✅ 69 个 MCP 工具（SDK数据 + 业务数据 + 系统管理）
- ✅ 3 个 MCP Resources（API规格、工具概览、系统状态）
- ✅ 统一 TGW 连接管理（解决 MCP 与 Backend 连接冲突）

---

## 系统架构

```
┌─────────────────────────────────────────────────────────────┐
│                      FastAPI 主进程                          │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐           │
│  │ UI API  │ │Agent API│ │ Auth API│ │Backtest │           │
│  └─────────┘ └─────────┘ └─────────┘ └─────────┘           │
│         │           │                                       │
│         └───────────┼───────────────────────────────────    │
│                     ▼                                       │
│  ┌─────────────────────────────────────────────────────┐   │
│  │              SDK Proxy Client (IPC)                  │   │
│  │         Unix Socket + RLock 串行化 + 优先级队列       │   │
│  └─────────────────────────────────────────────────────┘   │
│                     │                                       │
└─────────────────────┼─────────────────────────────────────┘
                      ▼ Unix Socket
┌─────────────────────────────────────────────────────────────┐
│                    SDK 子进程                                │
│  ┌─────────────────────────────────────────────────────┐   │
│  │              AmazingData SDK                         │   │
│  │         TGW TCP 连接（单用户单连接限制）              │   │
│  └─────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

---

## 快速开始

### 1. 启动后端服务
```bash
./start_backend.sh start     # 启动服务
./start_backend.sh stop      # 停止服务
./start_backend.sh restart   # 重启服务
./start_backend.sh status    # 查看状态
./start_backend.sh logs      # 查看日志
```

### 2. 前端开发
```bash
cd frontend
npm install
npm run dev     # 开发模式 http://localhost:5173
npm run build   # 生产构建
```

### 3. Agent API 使用
```bash
# 获取系统规格（Agent 首次连接必读）
curl -H "X-Agent-Key: sk-agent-xxx" \
  http://localhost:8080/api/v1/agent/spec

# 查询持仓
curl -H "X-Agent-Key: sk-agent-xxx" \
  "http://localhost:8080/api/v1/agent/query/positions?account_id=bobo"

# 创建策略（strategist+）
curl -X POST -H "X-Agent-Key: sk-agent-xxx" \
  -H "Content-Type: application/json" \
  -d '{"name":"RSI超卖","config":{"buy_conditions":[{"condition":"RSI_14 < 30"}]}}'
  "http://localhost:8080/api/v1/agent/submit/screening?account_id=bobo"
```

---

## 目录结构

```
StockWinner/
├── config/                    # 配置文件
│   ├── llm.json               # LLM API 配置
│   └── screening_templates.json  # 选股模板
│
├── data/                      # 数据库
│   ├── stockwinner.db         # 业务数据（账户/策略/持仓/信号）
│   └── kline.db               # 行情数据（K线/因子/基本信息）
│
├── frontend/                  # Vue 3 前端
│   └── src/
│       ├── views/             # 14 个页面组件
│       ├── stores/            # Pinia 状态管理
│       └── router/            # 路由配置
│
├── logs/                      # 日志文件
│
├── services/                  # 后端服务
│   ├── boot/                  # 启动模块
│   │   ├── app.py             # FastAPI 入口
│   │   ├── lifespan.py        # 生命周期（启动/关闭流程）
│   │   └── middleware.py      # 中间件（认证/权限/审计）
│   │
│   ├── common/                # 公共模块
│   │   ├── database.py        # 双库管理器（异步池+同步缓存）
│   │   ├── sdk_manager.py     # SDK 实例缓存
│   │   ├── sdk_proxy_client.py  # IPC 客户端（RLock 串行化）
│   │   ├── sdk_subprocess_server.py  # SDK 子进程服务端
│   │   ├── price_cache.py     # OHLCV 内存缓存（TTL+优先级）
│   │   ├── scheduler_service.py  # APScheduler 调度器
│   │   ├── timezone.py        # 中国时区（唯一合法来源）
│   │   └── stock_code.py      # 股票代码规范化
│   │
│   ├── trading/               # 交易网关
│   │   ├── gateway.py         # SDK 封装门面
│   │   ├── gateway_dispatcher.py  # 行情订阅分发器
│   │   ├── execution_service.py   # 交易执行（买入/卖出/止损）
│   │   ├── position_manager.py    # 持仓管理（冻结/T+1）
│   │   ├── trading_hours.py       # 交易时段判断
│   │   └── risk_service.py        # 风控服务
│   │
│   ├── monitoring/            # 监控服务
│   │   ├── service.py         # 监控循环（60s 刷新）
│   │   ├── signal_evaluator.py  # 信号评估（三层策略）
│   │   └── signal_executor.py   # 信号执行
│   │
│   ├── screening/             # 选股服务
│   │   ├── service.py         # 选股循环
│   │   ├── sandbox.py         # 策略沙箱执行
│   │   ├── condition_parser.py  # 条件解析
│   │   └── factor_registry.py   # 因子注册表
│   │
│   ├── agent/                 # Agent API
│   │   ├── handlers.py        # 70+ 端点处理器
│   │   ├── middleware.py      # Agent 认证/权限
│   │   ├── models.py          # 角色常量/权限矩阵
│   │   └── audit.py           # 审计日志
│   │
│   ├── auth/                  # 用户认证
│   │   ├── service.py         # 登录/登出/密码
│   │   └── middleware.py      # JWT 认证
│   │
│   ├── ui/                    # 前端 API
│   │   ├── dashboard.py       # 仪表盘
│   │   ├── positions.py       # 持仓分析（建仓日期+策略分组）
│   │   ├── screening.py       # 选股执行/Watchlist 管理
│   │   ├── strategies.py      # 策略 CRUD
│   │   ├── trading_strategies.py  # 止盈止损
│   │   ├── capital.py         # 策略资金管理
│   │   └── scheduler.py       # 任务管理
│   │
│   ├── data/                  # 数据服务
│   │   ├── data_download.py   # K 线增量下载
│   │   ├── local_data_service.py  # 本地数据查询
│   │   └── providers/         # 6 个数据源 Provider
│   │
│   ├── factors/               # 因子计算
│   │   ├── daily_factor_calculator.py   # 日频（62 字段）
│   │   └── monthly_factor_calculator.py # 月频（23 字段）
│   │
│   ├── backtest/              # 回测系统
│   │   ├── engine.py          # 回测引擎
│   │   ├── execution.py       # 交易执行模拟
│   │   └ metrics.py           # 绩效指标
│   │
│   └── tasks/                 # 任务插件
│       ├── builtin/           # 4 个内置任务
│       └ user_custom/         # 用户自定义任务
│
├── tests/                     # 测试文件
├── CLAUDE.md                  # Claude Code 指导文件
├── README.md                  # 本文档
└ requirements.txt             # Python 依赖
```

---

## 前端页面

| 路由 | 页面 | 功能 |
|------|------|------|
| `/login` | Login | 用户登录 |
| `/dashboard` | Dashboard | 系统健康度、数据通道状态、仓位概览、今日交易 |
| `/positions` | Positions | 持仓分析（按策略分组、建仓日期、盈亏分布） |
| `/trades` | Trades | 交易记录（筛选、统计） |
| `/strategies` | Strategies | 策略管理（持仓策略、选股策略、交易策略） |
| `/watchlist` | Watchlist | 选股监控（候选组管理、选股执行、股票搜索） |
| `/signals` | Signals | 交易信号列表 |
| `/data-explorer` | DataExplorer | 数据浏览器（双库查询、筛选、导出） |
| `/data-management` | DataManagement | 数据维护（K线统计、因子计算进度） |
| `/performance` | Performance | 策略效能（选股记录、交易明细、净值曲线） |
| `/backtest` | Backtest | 策略回测（配置、执行、对比） |
| `/accounts` | AccountManagement | 账户管理（用户 CRUD、Agent 绑定） |
| `/settings` | Settings | 系统设置（通知配置、LLM、数据源） |
| `/change-password` | ChangePassword | 修改密码 |

---

## 数据库表结构

### stockwinner.db（业务数据）

**核心业务表**：
| 表名 | 功能 |
|------|------|
| `accounts` | 用户账户（account_id, name, role, available_cash） |
| `stock_positions` | 当前持仓（quantity, avg_cost, current_price, highest_price） |
| `trade_records` | 交易流水（trade_type, price, quantity, profit_loss, trigger_source） |
| `strategies` | 策略配置（strategy_type, config, code, allocated_capital, strategy_cash） |
| `strategy_versions` | 策略版本历史 |
| `watchlist` | 候选股票（group_id, status, trigger_price, stop_loss_price） |
| `candidate_groups` | 候选组（group_type, screening_strategy_id） |
| `trading_signals` | 交易信号（signal_type, price, reason, order_type） |
| `trading_strategies` | 个股止盈止损（take_profit_pct, stop_loss_pct, trailing_stop_pct） |

**回测系统表**：
| 表名 | 功能 |
|------|------|
| `backtest_runs` | 回测任务（mode, status, result_summary） |
| `backtest_trades` | 回测交易明细 |
| `backtest_daily_nav` | 回测每日净值 |
| `backtest_daily_positions` | 回测每日持仓快照 |

**Agent 系统表**：
| 表名 | 功能 |
|------|------|
| `agent_accounts` | Agent 账户（role, api_key_hash, allowed_account_ids） |
| `agent_audit_log` | 审计日志（action, status, risk_level） |
| `agent_confirmations` | 人工确认流程 |

**调度与通知表**：
| 表名 | 功能 |
|------|------|
| `strategy_tasks` | 定时任务（task_type, cron_expression, enabled） |
| `notification_config` | 飞书 Webhook 配置 |
| `notification_history` | 通知发送历史 |
| `strategy_cash_borrows` | 策略资金借用 |
| `strategy_cash_transactions` | 策略资金流水 |

### kline.db（行情数据）

| 表名 | 功能 | 记录数 |
|------|------|--------|
| `kline_data` | 日 K 线 | ~580 万 |
| `weekly_kline_data` | 周 K 线 | ~120 万 |
| `stock_daily_factors` | 日频因子（62 字段） | ~580 万 |
| `stock_monthly_factors` | 月频因子（23 字段） | ~29 万 |
| `stock_base_info` | 股票基础信息（名称/行业） | ~5000 |

---

## Agent API 端点

### 查询端点（viewer+）
| 路径 | 功能 |
|------|------|
| `/query/dashboard` | 仪表盘数据 |
| `/query/positions` | 当前持仓 |
| `/query/trades` | 交易记录 |
| `/query/signals` | 交易信号 |
| `/query/watchlist` | 观察清单 |
| `/query/candidates` | 候选股 |
| `/query/strategies` | 策略列表 |
| `/query/strategy/{id}` | 策略详情 |
| `/query/market` | 实时行情 |
| `/query/kline` | K 线数据（11 种周期） |
| `/query/factors` | 日频因子 |
| `/query/notifications` | 通知历史 |
| `/query/stock-code` | 代码↔名称转换 |

### 外部数据端点（viewer+）
| 路径 | 功能 |
|------|------|
| `/query/data/index/list` | 指数代码列表 |
| `/query/data/index/kline` | 指数 K 线 |
| `/query/data/industry/list` | 申万行业分类 |
| `/query/data/industry/kline` | 行业指数行情 |
| `/query/data/financial/*` | 财务报表（利润表/资产负债表/现金流量表） |
| `/query/data/dragon-tiger` | 龙虎榜 |
| `/query/data/margin/*` | 融资融券 |
| `/query/data/block-trading` | 大宗交易 |

### 提交端点（strategist+）
| 路径 | 功能 |
|------|------|
| `/submit/screening` | 创建配置型选股策略 |
| `/submit/strategy-code` | 创建代码型策略（AST 校验） |
| `/submit/trading-strategy` | 创建交易策略 |
| `/strategy/{id}` PUT | 修改策略 |
| `/strategy/{id}` DELETE | 删除策略 |
| `/submit/strategy/{id}/restore` | 恢复历史版本 |

### 止盈止损端点
| 路径 | 权限 | 功能 |
|------|------|------|
| `/query/trading-strategy/stock/{code}` | viewer | 获取个股配置 |
| `/query/trading-strategy/stock-list` | viewer | 配置列表 |
| `/submit/trading-strategy/stock` | operator | 设置止盈止损 |
| `/submit/trading-strategy/stock/{code}` DELETE | operator | 删除配置 |

### 系统管理端点（operator+）
| 路径 | 权限 | 功能 |
|------|------|------|
| `/manage/scheduler/run-now` | scheduler:start | 立即执行任务 |
| `/manage/scheduler/toggle` | scheduler:stop | 启停任务 |
| `/manage/monitoring/start` | monitoring:start | 启动监控 |
| `/manage/monitoring/stop` | monitoring:stop | 停止监控 |
| `/manage/screening/start` | screening:create | 启动选股 |
| `/manage/strategy/execute` | strategy:execute | 执行策略 |

### 管理员端点（admin only）
| 路径 | 功能 |
|------|------|
| `/admin/audit` | 审计日志查询 |
| `/admin/agents` CRUD | Agent 管理 |
| `/admin/agents/{id}/rotate-key` | 重置 API Key |

---

## 权限系统

### Agent 角色权限矩阵

| 权限 | viewer | strategist | operator | admin |
|------|:------:|:----------:|:--------:|:-----:|
| query:* | ✓ | ✓ | ✓ | ✓ |
| strategy:create | - | ✓ | ✓ | ✓ |
| strategy:update | - | ✓ | ✓ | ✓ |
| strategy:delete | - | - | ✓ | ✓ |
| strategy:version | - | ✓ | ✓ | ✓ |
| watchlist:manage | - | ✓ | ✓ | ✓ |
| trading_strategy:manage | - | - | ✓ | ✓ |
| capital:manage | - | - | ✓ | ✓ |
| scheduler:start/stop | - | - | ✓ | ✓ |
| monitoring:start/stop | - | - | ✓ | ✓ |
| trading:execute | - | - | - | ✓ |
| agent:manage | - | - | - | ✓ |

### 操作风险分级

| 等级 | 操作类型 | 处理方式 |
|------|---------|---------|
| **low** | 只读查询 | 自动放行 |
| **medium** | 创建/修改策略 | 审计 + AST 校验 |
| **high** | 删除/启停服务 | 审计，operator+ |
| **critical** | 交易/账户变更 | 人工确认 |

### Agent 速率限制

| 角色 | 请求/分钟 |
|------|----------|
| viewer | 120 |
| strategist | 60 |
| operator | 30 |
| admin | 120 |

---

## MCP 服务

### MCP 工具分类

| 类别 | 工具数 | 功能示例 |
|------|--------|----------|
| SDK 数据查询 | 37 | mcp_kline, mcp_market, mcp_factors, mcp_code_list, mcp_backward_factor |
| 业务数据查询 | 17 | mcp_positions, mcp_signals, mcp_strategies, mcp_dashboard |
| 系统管理 | 7 | mcp_monitoring_start/stop, mcp_scheduler_run_now |
| 策略操作 | 8 | mcp_create_strategy, mcp_set_trading_strategy |

### MCP Resources

| Resource URI | 内容 |
|--------------|------|
| `stockwinner://doc/spec` | Agent API 规格文档 |
| `stockwinner://doc/api-summary` | MCP 工具概览 |
| `stockwinner://system/status` | 系统运行状态（实时） |

### Claude Desktop 配置

```json
{
  "mcpServers": {
    "stockwinner": {
      "command": "python3",
      "args": ["services/mcp/server.py"],
      "env": {
        "AGENT_API_KEY": "sk-agent-xxx",
        "AGENT_API_BASE_URL": "http://localhost:8080/api/v1/agent"
      }
    }
  }
}
```

### 使用示例

```
用户: 查询我的持仓
Claude: [mcp_positions] → 返回持仓列表

用户: 分析工商银行最近走势
Claude: [mcp_kline + mcp_factors] → K线 + 技术指标

用户: 启动监控服务
Claude: [mcp_monitoring_start] → 启动结果
```

---

## 调度任务

### 内置任务

| 任务 | Cron | 功能 |
|------|------|------|
| K 线增量检查 | `0 1 * * *` | 每日 01:00 检查下载缺失 K 线 |
| 月频因子更新 | `0 1 5 * *` | 每月 5 日更新月频因子 |
| 周K线下载 | `0 2 * * sat` | 每周六下载周K线 |
| 行业指数下载 | `0 3 * * mon-fri` | 每交易日下载行业指数（默认禁用） |

### 执行流水线

```
K线检查 → K线下载 → 日频因子计算 → 月频因子更新
```

### 用户自定义任务

放入 `services/tasks/user_custom/*.py`，文件头包含元数据：

```python
# ---
# name: 任务名称
# description: 任务描述
# category: screening/trading/data
# ---
async def execute(task_id: int = None, **kwargs):
    # 任务逻辑
```

---

## 多数据源架构

| Provider | 数据类型 | 特性 |
|----------|---------|------|
| AmazingData | K线、因子、财务 | SDK 直连，实时行情 |
| Eastmoney | 行情、财务 | 免费 HTTP API |
| Tushare | K线、财务 | 需要 API Token |
| Sina | 实时行情 | 免费 HTTP API |
| Tencent | 实时行情 | 免费 HTTP API |
| Akshare | 行业指数 | 开源 Python 库 |

ChannelRouter 自动切换和降级，按配置优先级尝试。

---

## 回测系统

### 回测模式

| 模式 | 特性 |
|------|------|
| `simulated` | 撮合模拟盘（考虑仓位、现金、T+1、止盈止损） |
| `return_accumulation` | 收益率累积（快速信号配对） |

### 止盈止损参数

| 参数 | 说明 |
|------|------|
| `stop_loss_pct` | 止损百分比 |
| `take_profit_pct` | 止盈百分比 |
| `trailing_stop_pct` | 移动止损百分比 |
| `stop_execution_price` | 成交价模式：`close`=收盘价，`trigger`=触发价 |

### 绩效指标

- 总收益率、年化收益率
- 最大回撤、夏普比率
- 胜率、盈亏比
- 持仓天数分布

---

## 监控策略三层优先级

```
个股止盈止损 (trading_strategies 表) — 最高优先级
    ↓ 未匹配
代码型策略 (strategies 表, code_scope=trading)
    ↓ 未匹配
Watchlist 止盈止损 (watchlist 表) — 兜底
```

---

## 开发进度

### v7.8.9 (2026-05-28)
- [x] Agent API 全面开放（止盈止损、版本管理、绩效分析、资金管理）
- [x] SDK 优先级队列（4 级：highest/high/medium/low）
- [x] watchlist 端点 Agent 权限检查
- [x] Spec 文档更新（12 端点分类）
- [x] 持仓页面建仓日期显示与排序
- [x] 止盈止损策略自动删除（持仓清零联动）
- [x] 前端全市场选项 + 交易时段校验
- [x] 回测止盈止损 trigger 成交价模式 + T+1 执行

### v7.1.5 (2026-05-27)
- [x] 系统资源管理优化（连接老化、PriceCache 容量上限）
- [x] APScheduler 事件循环修复

### v7.1.0 (2026-05-05)
- [x] 策略沙箱执行（AST 校验 + 受限 builtins）
- [x] 任务插件系统（内置 + 用户自定义）
- [x] 策略版本管理

---

## 版本历史

| 版本 | 日期 | 核心功能 |
|------|------|----------|
| v7.8.19 | 2026-05-30 | K线弹窗增强：前复权累计因子修复、周线/月线当周当月合成、非交易日K线重复修复 |
| v7.8.18 | 2026-05-29 | MCP 服务完善（69工具），复权因子/股东/股权/分红等13个新工具 |
| v7.8.17 | 2026-05-29 | MCP 服务完善（56工具+4新增端点） |
| v7.8.16 | 2026-05-29 | MCP 服务初始版本（52工具） |
| v7.8.11 | 2026-05-29 | 系统配置管理、PriceCache TTL 动态调整、已清仓策略筛选 |
| v7.8.9 | 2026-05-28 | Agent API 全面开放、SDK 优先级队列 |
| v7.1.5 | 2026-05-27 | 系统资源管理优化 |
| v7.1.0 | 2026-05-05 | 策略沙箱执行、任务插件系统 |
| v6.2.4 | 2026-04-07 | K 线数据 API 增强 |

---

**项目状态**: ✅ 系统完整度 95%

**待完善**: 真实交易执行、部署文档完善