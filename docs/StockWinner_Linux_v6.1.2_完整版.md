# StockWinner 智能股票交易系统 Linux 版设计文档 v6.2.1

> **因子数据与 SDK 集成版本** — 日频/月频因子表 + AmazingData SDK 集成

**文档版本**: v6.2.1  
**创建时间**: 2026-03-28  
**更新时间**: 2026-04-03  
**基于版本**: v6.1.2 + v6.2.0  
**目标平台**: Ubuntu 22.04/24.04 LTS (x86_64)  
**Python 版本**: 3.10+  
**数据库**: SQLite 3.x (WAL 模式)

---

## 📋 文档说明

### 版本演进

| 版本 | 日期 | 核心变更 |
|------|------|----------|
| v5.0 | 2026-03-23 | 批量查询、熔断器、并发控制、UI 监控 |
| v5.0.4 | 2026-03-24 | 导航栏标签页修复 + 账户管理功能 |
| v6.1 | 2026-03-28 | Linux 原生合并架构 |
| v6.1.1 | 2026-03-28 | 增强异常处理、缓存保护 |
| v6.1.2 | 2026-03-29 | 多账户支持、数据隔离、UI 适配 |
| v6.2.0 | 2026-03-30 | 认证模块、券商 credentials 支持 |
| **v6.2.1** | **2026-04-03** | **因子数据迁移、SDK 集成** |

### v6.2.1 核心改进

**新增功能：**
- ✅ 因子数据迁移工具（stock_daily_factors, stock_monthly_factors）
- ✅ AmazingData SDK 集成（在线模式调用）
- ✅ 市值类因子计算（流通市值、总市值）
- ✅ 估值类因子计算（PE 倒数、PB 倒数）
- ✅ 财务数据获取（利润表、资产负债表、现金流量表）

**数据规模**:
- ✅ 日频因子表：5,735,525 条记录
- ✅ 月频因子表：295,320 条记录
- ✅ 日期范围：2021-04-02 至 2026-04-02

**v6.1.2 核心改进（保留）**
- ✅ 多账户管理器（支持 2-10 个账户）
- ✅ 数据库多租户隔离（account_id 字段）
- ✅ API 多账户路由（`/api/{account_id}/*`）
- ✅ UI 账户切换功能
- ✅ Tailscale 远程访问支持

---

## 🎯 重构目标

### 1. 解决核心问题

| 问题 | v5.0 解决方案 | v6.1.2 增强 |
|------|-------------|----------|
| **数据一致性** | SQLite 数据库 + 事务机制 | 多账户隔离 |
| **单账户限制** | 单账户运行 | 多账户并行 |
| **远程访问** | 局域网访问 | Tailscale 远程访问 |
| **UI 监控** | 单账户 UI | 多账户切换 UI |

### 2. 多账户场景

**典型用户：**
- **波哥** — 主账户，全功能使用
- **浩哥** — 邻居账户，共享服务器资源
- **未来扩展** — 支持更多用户

**隔离要求：**
- ✅ 持仓数据隔离
- ✅ 交易记录隔离
- ✅ 策略配置隔离
- ✅ UI 界面隔离

---

## 🏗️ 系统架构

### 1. 整体架构图

```
┌─────────────────────────────────────────────────────────┐
│              邻居 Ubuntu x86 服务器                      │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  ┌─────────────────────────────────────────────────┐   │
│  │         StockWinner v6.1.2 (端口 8080)           │   │
│  │  ┌───────────────────────────────────────────┐ │   │
│  │  │  FastAPI Web Server (uvicorn + uvloop)   │ │   │
│  │  └───────────────────────────────────────────┘ │   │
│  │                                                 │   │
│  │  ┌───────────────────────────────────────────┐ │   │
│  │  │  多账户管理器 (AccountManager)             │ │   │
│  │  │  - 账户配置加载 (config/accounts.json)    │ │   │
│  │  │  - 银河 SDK 客户端管理                      │ │   │
│  │  │  - API 路由分发                            │ │   │
│  │  └───────────────────────────────────────────┘ │   │
│  │                                                 │   │
│  │  ┌───────────────────────────────────────────┐ │   │
│  │  │  应用层 (交易/策略/选股/监控)              │ │   │
│  │  └───────────────────────────────────────────┘ │   │
│  │                                                 │   │
│  │  ┌───────────────────────────────────────────┐ │   │
│  │  │  银河数据层 (Galaxy SDK 原生模块)           │ │   │
│  │  └───────────────────────────────────────────┘ │   │
│  └─────────────────────────────────────────────────┘   │
│                         │                               │
│                         ▼                               │
│  ┌─────────────────────────────────────────────────┐   │
│  │         SQLite 数据库 (单库多账户)                │   │
│  │  - stock_positions (account_id, ...)            │   │
│  │  - trade_records (account_id, ...)              │   │
│  │  - orders (account_id, ...)                     │   │
│  └─────────────────────────────────────────────────┘   │
│                                                         │
└─────────────────────────────────────────────────────────┘
                         │
                         │ Tailscale (100.x.x.x)
                         ▼
┌─────────────────────────────────────────────────────────┐
│                    用户 Mac Mini                         │
│  - 通过 Tailscale IP 访问邻居服务器                       │
│  - curl http://<tailscale-ip>:8080/api/bobo/health     │
│  - UI 访问 http://<tailscale-ip>:8080/ui               │
└─────────────────────────────────────────────────────────┘
```

### 2. 多账户架构

**配置驱动的多账户管理：**

```json
// config/accounts.json
{
  "bobo": {
    "username": "波哥的用户名",
    "password": "波哥的密码",
    "display_name": "波哥",
    "is_active": true
  },
  "haoge": {
    "username": "浩哥的用户名",
    "password": "浩哥的密码",
    "display_name": "浩哥",
    "is_active": true
  }
}
```

**API 路由：**
```
GET /api/v1/ui/bobo/dashboard      → 波哥的仪表盘
GET /api/v1/ui/haoge/dashboard     → 浩哥的仪表盘
GET /api/v1/ui/bobo/positions      → 波哥的持仓
GET /api/v1/ui/haoge/positions     → 浩哥的持仓
```

**UI 账户切换：**
- 导航栏右上角账户下拉框
- 切换账户后自动刷新数据
- 各账户数据完全隔离

---

## 📊 功能需求

### 1. 多账户管理（P0 - 必需）

| 功能 | 描述 | 优先级 |
|------|------|--------|
| **账户配置加载** | 从 `config/accounts.json` 加载账户信息 | P0 |
| **账户验证** | 验证账户是否存在且激活 | P0 |
| **银河 SDK 客户端管理** | 为每个账户创建独立的银河 SDK 客户端 | P0 |
| **API 路由分发** | 根据 URL 中的 account_id 路由到对应账户 | P0 |

### 2. 数据隔离（P0 - 必需）

| 功能 | 描述 | 优先级 |
|------|------|--------|
| **持仓隔离** | `stock_positions` 表加 `account_id` 字段 | P0 |
| **交易隔离** | `trade_records` 表加 `account_id` 字段 | P0 |
| **订单隔离** | `orders` 表加 `account_id` 字段 | P0 |
| **查询隔离** | 所有查询必须带 `WHERE account_id = ?` | P0 |

### 3. UI 监控仪表盘（P0 - 必需）

**页面列表：**
| 页面 | 功能 | 状态 |
|------|------|------|
| 仪表盘 | 系统健康度、交易统计、资源开销、控制面板 | ✅ 设计完成 |
| 交易监控 | 实时交易流水、统计汇总、明细查询 | ✅ 设计完成 |
| 持仓分析 | 持仓列表、盈亏分布、T+1 提示 | ✅ 设计完成 |
| 策略管理 | 策略列表、回测结果、LLM 生成 | ✅ 设计完成 |
| 系统设置 | API 配置、日志查看 | ✅ 设计完成 |
| 账户管理 | 账户切换、账户信息查看 | ✅ 新增 |

**技术选型：**
- 前端框架：Vue 3 + Vite
- UI 组件库：Element Plus
- 图表库：ECharts
- 状态管理：Pinia
- HTTP 客户端：Axios
- 实时通信：WebSocket

### 4. API 接口（P0 - 必需）

**多账户 API 端点：**

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/v1/ui/{account_id}/dashboard` | 仪表盘总览 |
| GET | `/api/v1/ui/{account_id}/trades/today` | 今日交易 |
| GET | `/api/v1/ui/{account_id}/positions` | 持仓列表 |
| POST | `/api/v1/ui/{account_id}/trades/execute` | 执行交易 |
| GET | `/api/v1/ui/{account_id}/strategies` | 策略列表 |
| POST | `/api/v1/ui/{account_id}/screening/run` | 运行选股 |
| POST | `/api/v1/ui/{account_id}/monitoring/start` | 启动监控 |

### 5. Tailscale 网络（P0 - 必需）

| 功能 | 描述 | 优先级 |
|------|------|--------|
| **Tailscale 安装** | 在邻居 Ubuntu 上安装 Tailscale | P0 |
| **远程访问** | 通过 Tailscale IP 访问服务 | P0 |
| **网络测试** | 测试 Mac 到邻居服务器的连接 | P0 |

---

## 💾 数据库设计

### 1. 数据库 Schema 变更

**所有表加 `account_id` 字段：**

```sql
-- 持仓表
CREATE TABLE stock_positions (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    account_id VARCHAR(32) NOT NULL,  -- 新增：账户 ID
    user_id INT NOT NULL,
    stock_code VARCHAR(20) NOT NULL,
    stock_name VARCHAR(100),
    quantity INT NOT NULL DEFAULT 0,
    available_quantity INT NOT NULL DEFAULT 0,
    avg_cost DECIMAL(10,3) NOT NULL DEFAULT 0.000,
    market_value DECIMAL(12,3) NOT NULL DEFAULT 0.000,
    current_price DECIMAL(10,3) NOT NULL DEFAULT 0.000,
    profit_loss DECIMAL(12,3) NOT NULL DEFAULT 0.000,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_account_stock (account_id, stock_code),  -- 新增：复合索引
    INDEX idx_account (account_id)  -- 新增：账户索引
);

-- 交易记录表
CREATE TABLE trade_records (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    account_id VARCHAR(32) NOT NULL,  -- 新增：账户 ID
    user_id INT NOT NULL,
    order_id VARCHAR(50) NOT NULL,
    stock_code VARCHAR(20) NOT NULL,
    stock_name VARCHAR(100),
    trade_type ENUM('buy', 'sell') NOT NULL,
    quantity INT NOT NULL,
    price DECIMAL(10,3) NOT NULL,
    amount DECIMAL(12,3) NOT NULL,
    commission DECIMAL(10,3) NOT NULL DEFAULT 0.000,
    trade_time TIMESTAMP NOT NULL,
    status ENUM('success', 'failed', 'pending') DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_account_trade (account_id, trade_time),  -- 新增
    INDEX idx_account (account_id)  -- 新增
);

-- 订单表
CREATE TABLE orders (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    account_id VARCHAR(32) NOT NULL,  -- 新增：账户 ID
    user_id INT NOT NULL,
    stock_code VARCHAR(20) NOT NULL,
    order_type VARCHAR(20),
    quantity INT NOT NULL,
    price DECIMAL(10,3),
    status VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_account (account_id)  -- 新增
);

-- 账户表（新增）
CREATE TABLE accounts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id VARCHAR(32) NOT NULL UNIQUE,  -- 账户 ID
    name TEXT NOT NULL,                      -- 账户名称
    username TEXT NOT NULL UNIQUE,           -- 登录用户名
    password_hash TEXT NOT NULL,             -- 密码哈希
    display_name TEXT,                       -- 显示名称
    is_active INTEGER DEFAULT 1,             -- 是否激活
    created_at DATETIME NOT NULL DEFAULT (datetime('now')),
    updated_at DATETIME NOT NULL DEFAULT (datetime('now'))
);
```

### 2. 数据库迁移脚本

```sql
-- migration_add_account_id.sql
-- 为现有表添加 account_id 字段

-- 1. 添加 account_id 字段
ALTER TABLE stock_positions ADD COLUMN account_id VARCHAR(32) DEFAULT 'default';
ALTER TABLE trade_records ADD COLUMN account_id VARCHAR(32) DEFAULT 'default';
ALTER TABLE orders ADD COLUMN account_id VARCHAR(32) DEFAULT 'default';

-- 2. 创建账户表
CREATE TABLE IF NOT EXISTS accounts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id VARCHAR(32) NOT NULL UNIQUE,
    name TEXT NOT NULL,
    username TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    display_name TEXT,
    is_active INTEGER DEFAULT 1,
    created_at DATETIME NOT NULL DEFAULT (datetime('now')),
    updated_at DATETIME NOT NULL DEFAULT (datetime('now'))
);

-- 3. 创建索引
CREATE INDEX idx_account_stock ON stock_positions(account_id, stock_code);
CREATE INDEX idx_account ON stock_positions(account_id);
CREATE INDEX idx_account_trade ON trade_records(account_id, trade_time);
CREATE INDEX idx_account ON trade_records(account_id);
CREATE INDEX idx_account ON orders(account_id);

-- 4. 更新现有数据（如果有）
UPDATE stock_positions SET account_id = 'default' WHERE account_id IS NULL;
UPDATE trade_records SET account_id = 'default' WHERE account_id IS NULL;
UPDATE orders SET account_id = 'default' WHERE account_id IS NULL;
```

---

## 🖥️ UI 界面设计（多账户适配版）

### 1. 账户切换功能

**位置：** 导航栏右上角

**设计：**
```
┌─────────────────────────────────────────────────────────────┐
│  StockWinner v6.1.2                    [账户▼] [设置] [用户] │
│                                          ┌──────────────┐   │
│                                          │ 波哥         │   │
│                                          │ 浩哥         │   │
│                                          │ ────────     │   │
│                                          │ 添加账户...  │   │
│                                          └──────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

**交互：**
- 点击账户下拉框
- 选择要切换的账户
- 页面自动刷新，加载对应账户数据
- URL 更新为 `/ui/{account_id}/dashboard`

### 2. 仪表盘页面（多账户版）

**功能模块：**

| 模块 | 内容 | 更新频率 | 账户隔离 |
|------|------|----------|---------|
| 系统健康度 | 整体状态、运行时长、服务状态 | 实时 | 共享 |
| 交易统计 | 今日笔数、金额、盈亏 | 1 分钟 | 隔离 |
| 资源开销 | CPU、内存、磁盘、网络 | 5 秒 | 共享 |
| 价格监控 | 候选股票实时价格 | 10 秒 | 隔离 |
| 持仓分布 | 持仓股票盈亏分布图 | 1 分钟 | 隔离 |
| 控制面板 | 服务启停控制 | 手动 | 共享 |
| 交易记录 | 最近 10 笔交易 | 实时 | 隔离 |

**API 端点：**
```
GET /api/v1/ui/{account_id}/dashboard        # 仪表盘总览数据
GET /api/v1/ui/{account_id}/health           # 健康检查详情
GET /api/v1/ui/{account_id}/resources        # 资源开销
GET /api/v1/ui/{account_id}/trades/today     # 今日交易统计
GET /api/v1/ui/{account_id}/positions        # 持仓概览
POST /api/v1/ui/{account_id}/services/{id}/start  # 启动服务
POST /api/v1/ui/{account_id}/services/{id}/stop   # 停止服务
```

### 3. 交易监控页面（多账户版）

**界面布局：**
```
┌─────────────────────────────────────────────────────────────┐
│  交易监控                              [账户▼] [导出] [刷新] │
├─────────────────────────────────────────────────────────────┤
│  统计卡片（当前账户）                                         │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐   │
│  │ 总交易笔数│  │ 买入笔数 │  │ 卖出笔数 │  │ 成功率   │   │
│  │   156    │  │    89    │  │    67    │  │  94.2%   │   │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘   │
├─────────────────────────────────────────────────────────────┤
│  交易明细表（当前账户）                                       │
│  [时间▼] [股票代码▼] [操作▼] [数量] [价格] [金额] [状态]    │
│  ─────────────────────────────────────────────────────────   │
│  11:25:30  600481.SH  买入   1000   ¥7.32   ¥7,320   ✅    │
│  11:20:15  600519.SH  卖出   500    ¥1,685  ¥842,500 ✅    │
│  ...                                                         │
│                                                              │
│  [<上一页 1/15 下一页>]                           [每页 20 条]│
└─────────────────────────────────────────────────────────────┘
```

### 4. 持仓分析页面（多账户版）

**界面布局：**
```
┌─────────────────────────────────────────────────────────────┐
│  持仓分析                              [账户▼] [刷新] [调仓] │
├─────────────────────────────────────────────────────────────┤
│  总体概览（当前账户）                                         │
│  总资产：¥1,245,600  |  可用资金：¥234,500  |  持仓市值：¥1,011,100 │
│  总盈亏：+¥45,600 (+3.8%)  |  今日盈亏：+¥3,200 (+0.3%)    │
├─────────────────────────────────────────────────────────────┤
│  持仓明细（当前账户）                                         │
│  ┌───────────────────────────────────────────────────────┐  │
│  │ 股票   数量   成本价   当前价   市值    盈亏    盈亏% │  │
│  ├───────────────────────────────────────────────────────┤  │
│  │ 600481 1000   ¥7.20   ¥7.35   ¥7,350  +¥150  +2.1%  │  │
│  │      T+1 可卖：0                 [加仓] [减仓] [清仓] │  │
│  ├───────────────────────────────────────────────────────┤  │
│  │ 600519 500    ¥1,650  ¥1,685  ¥842,500 +¥17,500 +2.1%│  │
│  │      T+1 可卖：300               [加仓] [减仓] [清仓] │  │
│  └───────────────────────────────────────────────────────┘  │
├─────────────────────────────────────────────────────────────┤
│  持仓分布               盈亏趋势                              │
│  [行业分布饼图]         [7 日盈亏折线图]                      │
└─────────────────────────────────────────────────────────────┘
```

### 5. 策略管理页面（多账户版）

**功能模块：**
- 策略列表（当前账户）
- 策略详情（配置参数、历史迭代）
- 策略回测结果
- LLM 策略生成入口

**账户隔离：**
- 每个账户有自己的策略列表
- 策略配置完全隔离
- 回测数据独立

### 6. 系统设置页面（多账户版）

**功能模块：**
- 银河 API 配置（按账户配置）
- 飞书通知配置（按账户配置）
- LLM API 配置（按账户配置）
- 交易参数配置（止损/止盈比例）
- 系统日志查看（共享）
- 账户管理（添加/删除账户）

---

## 🔧 后端 API 设计（多账户适配）

### 1. 仪表盘 API

```python
# services/ui/dashboard.py
from fastapi import APIRouter, HTTPException, Path
from typing import Dict
from services.common.account_manager import get_account_manager

router = APIRouter()

@router.get("/api/v1/ui/{account_id}/dashboard")
async def get_dashboard(account_id: str = Path(..., description="账户 ID")):
    """仪表盘总览数据"""
    account_manager = get_account_manager()
    account = account_manager.get_account(account_id)
    if not account:
        raise HTTPException(status_code=404, detail="账户不存在")
    
    return {
        "system_health": {
            "status": "healthy",
            "uptime_hours": 2.5,
            "services": {
                "galaxy_api": "ok",
                "screening": "running",
                "monitoring": "running",
                "notification": "ok"
            }
        },
        "today_trading": {
            "trade_count": 12,
            "total_amount": 85400,
            "total_pnl": 3200,
            "win_rate": 0.94
        },
        "resources": {
            "cpu_percent": 35,
            "memory_mb": 1228,
            "disk_percent": 45
        }
    }


@router.get("/api/v1/ui/{account_id}/trades/today")
async def get_trades_today(account_id: str):
    """今日交易统计（按账户隔离）"""
    today = datetime.now().date()
    
    async with db_manager.transaction() as conn:
        # 统计汇总（带 account_id 过滤）
        cursor = await conn.execute("""
            SELECT
                COUNT(*) as total_count,
                SUM(CASE WHEN action = 'buy' THEN 1 ELSE 0 END) as buy_count,
                SUM(CASE WHEN action = 'sell' THEN 1 ELSE 0 END) as sell_count,
                SUM(total_amount) as total_amount,
                AVG(total_amount) as avg_amount
            FROM trades
            WHERE account_id = ? AND DATE(trade_time) = ?
        """, (account_id, today))
        stats = await cursor.fetchone()
        
        # 明细列表（带 account_id 过滤）
        cursor = await conn.execute("""
            SELECT * FROM trades
            WHERE account_id = ? AND DATE(trade_time) = ?
            ORDER BY trade_time DESC LIMIT 50
        """, (account_id, today))
        trades = await cursor.fetchall()
        
        return {
            "stats": dict(stats),
            "trades": [dict(t) for t in trades]
        }
```

### 2. 服务控制 API

```python
@router.post("/api/v1/ui/{account_id}/services/{service_id}/start")
async def start_service(account_id: str, service_id: str):
    """启动服务"""
    from services.screening.service import screening_service
    from services.monitoring.service import trading_monitor
    
    # 验证账户
    account_manager = get_account_manager()
    account = account_manager.get_account(account_id)
    if not account:
        raise HTTPException(status_code=404, detail="账户不存在")
    
    if service_id == "screening":
        if not screening_service._running:
            asyncio.create_task(screening_service.start_screening())
            return {"success": True, "message": "选股服务已启动"}
        return {"success": False, "message": "服务已在运行"}
    
    elif service_id == "monitoring":
        if not trading_monitor._running:
            asyncio.create_task(trading_monitor.start_monitoring(account_id, 120))
            return {"success": True, "message": "监控服务已启动"}
        return {"success": False, "message": "服务已在运行"}
    
    return {"success": False, "message": "未知服务"}
```

### 3. 账户管理 API

```python
# services/ui/accounts.py
from fastapi import APIRouter, HTTPException
from typing import List
from services.common.account_manager import get_account_manager

router = APIRouter()

@router.get("/api/v1/ui/accounts")
async def list_accounts():
    """获取账户列表"""
    account_manager = get_account_manager()
    accounts = account_manager.list_accounts()
    return {
        "accounts": [
            {
                "account_id": acc.account_id,
                "name": acc.name,
                "display_name": acc.display_name,
                "is_active": acc.is_active
            }
            for acc in accounts
        ]
    }

@router.get("/api/v1/ui/accounts/{account_id}")
async def get_account(account_id: str):
    """获取账户详情"""
    account_manager = get_account_manager()
    account = account_manager.get_account(account_id)
    if not account:
        raise HTTPException(status_code=404, detail="账户不存在")
    return {"account": account}
```

---

## 📦 部署步骤

### 1. 环境准备

**在邻居 Ubuntu 服务器上：**

```bash
# 1. 更新系统
sudo apt update

# 2. 安装基础依赖
sudo apt install -y python3 python3-pip python3-venv nodejs npm git curl wget

# 3. 安装 NVM（Node 版本管理）
export NVM_NODEJS_ORG_MIRROR=https://mirrors.cloud.tencent.com/nvm
curl -o- https://mirrors.cloud.tencent.com/nvm/install.sh | bash
source ~/.bashrc

# 4. 安装 Node.js 22+
nvm install 22
nvm use 22
nvm alias default 22

# 5. 安装 Tailscale
curl -fsSL https://tailscale.com/install.sh | sh
sudo tailscale up
# 会输出登录链接，用手机/电脑打开授权

# 6. 获取 Tailscale IP
tailscale ip
```

### 2. 安装 OpenClaw

```bash
# 1. 安装 OpenClaw
npm install -g openclaw

# 2. 验证
openclaw --version

# 3. 安装微信插件（可选）
bash ~/WeChatInstall.sh

# 4. 重启 Gateway
openclaw gateway restart
```

### 3. 部署 StockWinner

```bash
# 1. 克隆代码
cd /home/haoge
git clone <repo> StockWinner
cd StockWinner

# 2. 创建虚拟环境
python3 -m venv venv
source venv/bin/activate

# 3. 安装依赖
pip install -r requirements.txt
pip install tgw
# + 银河 SDK 安装包

# 4. 配置账户
mkdir -p config
cp config/accounts.json.example config/accounts.json
# 编辑 config/accounts.json，填入账户信息
```

### 4. 配置文件

**创建 `config/accounts.json`：**

```json
{
  "bobo": {
    "username": "波哥的用户名",
    "password": "波哥的密码",
    "display_name": "波哥",
    "is_active": true
  },
  "haoge": {
    "username": "浩哥的用户名",
    "password": "浩哥的密码",
    "display_name": "浩哥",
    "is_active": true
  }
}
```

### 5. 数据库迁移

```bash
# 执行数据库迁移
sqlite3 data/stockwinner.db < migration_add_account_id.sql
```

### 6. 启动服务

```bash
# 启动服务
python3 -m uvicorn services.main:app --host 0.0.0.0 --port 8080

# 或者后台运行
nohup python3 -m uvicorn services.main:app --host 0.0.0.0 --port 8080 > logs/app.log 2>&1 &
```

### 7. 测试验证

**在你的 Mac 上：**

```bash
# 1. 安装 Tailscale（如果没装）
brew install tailscale
tailscale up

# 2. 测试健康检查
curl http://<邻居 Tailscale IP>:8080/api/v1/ui/bobo/health

# 3. 测试获取持仓
curl http://<邻居 Tailscale IP>:8080/api/v1/ui/bobo/positions

# 4. 测试浩哥的账户
curl http://<邻居 Tailscale IP>:8080/api/v1/ui/haoge/health
```

---

## 🧪 测试计划

### 1. 功能测试

| 测试项 | 测试步骤 | 预期结果 |
|--------|---------|---------|
| **账户配置加载** | 启动服务，检查日志 | 成功加载 2 个账户 |
| **API 路由** | 访问 `/api/bobo/health` 和 `/api/haoge/health` | 都返回 healthy |
| **持仓隔离** | 分别查询两个账户的持仓 | 数据不串 |
| **交易隔离** | 分别查询两个账户的交易记录 | 数据不串 |
| **Tailscale 访问** | 从 Mac 访问邻居服务器 | 连接成功 |
| **UI 账户切换** | 在 UI 中切换账户 | 数据正确刷新 |

### 2. 性能测试

| 测试项 | 测试方法 | 预期结果 |
|--------|---------|---------|
| **并发查询** | 同时查询两个账户的持仓 | 响应时间 < 1 秒 |
| **数据库查询** | 查询带 account_id 的持仓 | 使用索引，速度快 |

### 3. 安全测试

| 测试项 | 测试方法 | 预期结果 |
|--------|---------|---------|
| **账户验证** | 访问不存在的账户 | 返回 404 |
| **SQL 注入** | 尝试 SQL 注入攻击 | 被阻止 |
| **账户隔离** | 尝试访问其他账户数据 | 被拒绝 |

---

## 📅 项目计划

### 阶段 1：环境准备（已完成）

- [x] 邻居 Ubuntu 安装 Tailscale
- [x] 获取 Tailscale IP
- [x] 测试 Mac 到邻居的连接

### 阶段 2：代码开发（1-2 天）

- [ ] 多账户管理器代码
- [ ] API 路由改造
- [ ] 数据库迁移脚本
- [ ] UI 账户切换功能
- [ ] Cloud Code 测试修复

### 阶段 3：部署测试（1 天）

- [ ] 部署到邻居服务器
- [ ] 配置账户信息
- [ ] 执行数据库迁移
- [ ] 功能测试

### 阶段 4：验收上线（半天）

- [ ] 性能测试
- [ ] 安全测试
- [ ] 用户验收
- [ ] 正式上线

---

## 📝 附录

### A. 配置文件示例

**config/accounts.json**
```json
{
  "bobo": {
    "username": "your_username",
    "password": "your_password",
    "display_name": "波哥"
  },
  "haoge": {
    "username": "neighbor_username",
    "password": "neighbor_password",
    "display_name": "浩哥"
  }
}
```

### B. 常用命令

```bash
# 查看服务状态
ps aux | grep uvicorn

# 查看日志
tail -f logs/app.log

# 重启服务
pkill -f uvicorn && nohup python3 -m uvicorn services.main:app --host 0.0.0.0 --port 8080 > logs/app.log 2>&1 &

# 查看 Tailscale 状态
tailscale status

# 查看 Tailscale IP
tailscale ip
```

### C. 故障排查

| 问题 | 可能原因 | 解决方法 |
|------|---------|---------|
| 无法连接邻居服务器 | Tailscale 未运行 | `sudo tailscale up` |
| 账户不存在 | 配置文件错误 | 检查 `config/accounts.json` |
| 数据库查询慢 | 缺少索引 | 执行迁移脚本创建索引 |
| Node 版本不对 | NVM 未生效 | `source ~/.bashrc` 或重启终端 |
| UI 账户切换无效 | 前端未适配 | 检查前端账户切换逻辑 |

---

## 附录 F. 因子数据架构 (v6.2.1 新增)

### 1. 日频因子表 (stock_daily_factors)

**表结构**:
```sql
CREATE TABLE stock_daily_factors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stock_code TEXT NOT NULL,
    stock_name TEXT,
    trade_date DATE NOT NULL,

    -- 市值类因子
    circ_market_cap REAL,       -- 流通市值（亿元）
    total_market_cap REAL,      -- 总市值（亿元）
    days_since_ipo INTEGER,     -- 上市天数

    -- 市场表现类因子
    change_10d REAL,            -- 10 日涨跌幅
    change_20d REAL,            -- 20 日涨跌幅
    bias_5 REAL,                -- 5 日乖离率
    bias_10 REAL,               -- 10 日乖离率
    bias_20 REAL,               -- 20 日乖离率
    amplitude_5 REAL,           -- 5 日振幅
    amplitude_10 REAL,          -- 10 日振幅
    amplitude_20 REAL,          -- 20 日振幅
    change_std_5 REAL,          -- 5 日涨跌幅标准差
    change_std_10 REAL,         -- 10 日涨跌幅标准差
    change_std_20 REAL,         -- 20 日涨跌幅标准差
    amount_std_5 REAL,          -- 5 日成交额标准差
    amount_std_10 REAL,         -- 10 日成交额标准差
    amount_std_20 REAL,         -- 20 日成交额标准差

    -- 技术指标类因子
    kdj_k REAL,                 -- KDJ 指标 K 值
    kdj_d REAL,                 -- KDJ 指标 D 值
    kdj_j REAL,                 -- KDJ 指标 J 值
    dif REAL,                   -- MACD 的 DIF
    dea REAL,                   -- MACD 的 DEA
    macd REAL,                  -- MACD 值

    -- 估值类因子
    pe_inverse REAL,            -- PE 倒数
    pb_inverse REAL,            -- PB 倒数

    -- 下期收益率
    next_period_change REAL,    -- 下一交易日收益率

    -- 标记
    is_traded INTEGER,          -- 是否交易 (1=交易，0=未交易)

    -- 元数据
    source TEXT DEFAULT 'migrated',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(stock_code, trade_date)
);
```

**数据规模**:
- 总记录数：5,735,525
- 股票数量：~1,500
- 日期范围：2021-04-02 至 2026-04-02

### 2. 月频因子表 (stock_monthly_factors)

**表结构**:
```sql
CREATE TABLE stock_monthly_factors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stock_code TEXT NOT NULL,
    stock_name TEXT,
    report_date DATE NOT NULL,

    -- 财报时间
    report_quarter INTEGER,     -- 报告期月份 (3/6/9/12)
    report_year INTEGER,        -- 年份

    -- 利润类因子
    net_profit REAL,            -- 净利润（元）
    net_profit_ttm REAL,        -- 净利润 TTM（元）
    net_profit_ttm_yoy REAL,    -- 净利润 TTM 同比增速
    net_profit_single REAL,     -- 单季度净利润（元）
    net_profit_single_yoy REAL, -- 单季度净利润同比增速
    net_profit_single_qoq REAL, -- 单季度净利润环比增速

    -- 现金流类因子
    operating_cash_flow REAL,           -- 经营现金流（元）
    operating_cash_flow_ttm REAL,       -- 经营现金流 TTM（元）
    operating_cash_flow_ttm_yoy REAL,   -- 经营现金流 TTM 同比增速
    operating_cash_flow_single REAL,    -- 单季度经营现金流（元）
    operating_cash_flow_single_yoy REAL,-- 单季度经营现金流同比增速
    operating_cash_flow_single_qoq REAL,-- 单季度经营现金流环比增速

    -- 资产类因子
    net_assets REAL,            -- 净资产（元）

    -- 行业分类因子
    sw_level1 TEXT,             -- 申万一级行业
    sw_level2 TEXT,             -- 申万二级行业
    sw_level3 TEXT,             -- 申万三级行业

    -- 元数据
    source TEXT DEFAULT 'migrated',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(stock_code, report_date)
);
```

**数据规模**:
- 总记录数：295,320
- 股票数量：~1,500
- 日期范围：2021-04-02 至 2026-04-02

### 3. 因子计算公式

**市值计算**:
```python
# SDK 返回的股本单位：万股
# 收盘价单位：元
流通市值 = 流通股本 × 收盘价 / 10000  # 结果：亿元
总市值 = 总股本 × 收盘价 / 10000      # 结果：亿元
```

**估值计算**:
```python
# SDK 返回的财务数据单位：元
# 需要转换为亿元
PE 倒数 = 净利润 TTM / 100000000 / 市值  # 结果：1/PE
PB 倒数 = 净资产 / 100000000 / 市值      # 结果：1/PB
```

**下期收益率计算**:
```python
next_period_change = (次日收盘价 - 当日收盘价) / 当日收盘价
```

---

## 附录 G. AmazingData SDK 集成 (v6.2.1 新增)

### 1. SDK 信息

| 项目 | 值 |
|------|-----|
| SDK 名称 | AmazingData |
| 版本 | 1.0.30 |
| 登录账号 | REDACTED_SDK_USERNAME |
| 密码 | REDACTED_SDK_PASSWORD |
| 服务器 | 101.230.159.234:8600 |
| 权限期限 | 2026-3-10 至 2027-3-5 |

### 2. API 调用方式

**在线模式** (推荐，避免 Linux 路径问题):
```python
from AmazingData import login, InfoData

# 登录
login(username, password, host, port)

# 获取数据
info = InfoData()
equity_df = info.get_equity_structure(["689009.SH"], is_local=False)
income_df = info.get_income(["689009.SH"], is_local=False)
balance_df = info.get_balance_sheet(["689009.SH"], is_local=False)
cashflow_df = info.get_cash_flow(["689009.SH"], is_local=False)
```

**封装层调用**:
```python
from services.factors.sdk_api import AmazingDataAPI

api = AmazingDataAPI()

# 获取股本数据
equity = api.get_equity_structure(["689009.SH"])

# 获取财务数据
income = api.get_income_statement(["689009.SH"])
balance = api.get_balance_sheet(["689009.SH"])
cashflow = api.get_cash_flow_statement(["689009.SH"])
```

### 3. 数据单位说明

| 数据类型 | SDK 单位 | 计算单位 | 转换公式 |
|----------|---------|---------|----------|
| 股本 | 万股 | 亿股 | ÷ 10000 |
| 市值 | - | 亿元 | 股本 (万股) × 收盘价 / 10000 |
| 财务数据 | 元 | 亿元 | ÷ 100000000 |
| 净利润 TTM | 元 | 亿元 | ÷ 100000000 |
| 净资产 | 元 | 亿元 | ÷ 100000000 |
| 经营现金流 | 元 | 亿元 | ÷ 100000000 |

### 4. 限制与注意事项

1. **Linux 路径兼容性**: SDK 硬编码 Windows 路径，必须使用 `is_local=False` 在线模式

2. **行业分类数据**: SDK 暂无单只股票行业分类接口，需预留空值或从外部获取

3. **登录状态缓存**: 使用类变量缓存登录状态，避免重复登录

4. **数据更新频率**:
   - 股本数据：不定期更新（发生股本变动时）
   - 财务数据：季度更新（财报披露后）
   - 行情数据：实时/日线

---

**文档版本：v6.2.1**  
**创建时间：2026-03-28**  
**最后更新：2026-04-03**  
**作者：Jessie + 波哥** 🚀

---

*本方案为股票交易系统重构的完整设计方案 v6.2.1，在 v6.1.2 基础上增加了因子数据迁移、AmazingData SDK 集成、市值和估值因子计算功能。系统现在具备了完整的因子数据管理和计算能力，支持日频和月频因子的存储和查询。建议组织相关团队进行评审后实施。*
