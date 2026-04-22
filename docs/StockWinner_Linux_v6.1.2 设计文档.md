# StockWinner 智能股票交易系统 Linux 版设计文档 v6.1.2

> **多账户支持版本** — 共享数据库 + 逻辑隔离架构

**文档版本**: v6.1.2  
**创建时间**: 2026-03-28  
**目标平台**: Ubuntu 22.04/24.04 LTS (x86_64)  
**Python 版本**: 3.10+  
**数据库**: SQLite 3.x (WAL 模式)

---

## 📋 文档说明

### 版本演进

| 版本 | 日期 | 核心变更 |
|------|------|----------|
| v6.0 | 2026-03-24 | Linux 原生，双服务架构 |
| v6.1 | 2026-03-24 | 合并服务，消除 HTTP 通信 |
| v6.1.1 | 2026-03-24 | 增强异常处理、缓存保护、熔断器 |
| **v6.1.2** | **2026-03-28** | **多账户支持、数据隔离、Tailscale 远程访问** |

### v6.1.2 核心改进

**新增功能：**
- ✅ 多账户管理器（支持 2-10 个账户）
- ✅ 数据库多租户隔离（account_id 字段）
- ✅ API 多账户路由（`/api/{account_id}/*`）
- ✅ Tailscale 远程访问支持

**简化部署（现阶段）：**
- ⚠️ 密码明文存储（v6.2.0 加密）
- ⚠️ 无审计日志（v6.2.0 添加）
- ⚠️ 无监控告警（v6.2.0 添加）

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
GET /api/bobo/positions      → 波哥的持仓
GET /api/haoge/positions     → 浩哥的持仓
GET /api/bobo/trades         → 波哥的交易记录
GET /api/haoge/trades        → 浩哥的交易记录
```

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

### 3. API 接口（P0 - 必需）

**新增/修改的 API：**

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/{account_id}/health` | 健康检查 |
| GET | `/api/{account_id}/positions` | 获取持仓 |
| GET | `/api/{account_id}/trades` | 获取交易记录 |
| POST | `/api/{account_id}/trades/execute` | 执行交易 |
| GET | `/api/{account_id}/strategies` | 获取策略 |
| POST | `/api/{account_id}/screening/run` | 运行选股 |
| POST | `/api/{account_id}/monitoring/start` | 启动监控 |

### 4. Tailscale 网络（P0 - 必需）

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
```

### 2. 数据库迁移脚本

```sql
-- migration_add_account_id.sql
-- 为现有表添加 account_id 字段

-- 1. 添加 account_id 字段
ALTER TABLE stock_positions ADD COLUMN account_id VARCHAR(32) DEFAULT 'default';
ALTER TABLE trade_records ADD COLUMN account_id VARCHAR(32) DEFAULT 'default';
ALTER TABLE orders ADD COLUMN account_id VARCHAR(32) DEFAULT 'default';

-- 2. 创建索引
CREATE INDEX idx_account_stock ON stock_positions(account_id, stock_code);
CREATE INDEX idx_account ON stock_positions(account_id);
CREATE INDEX idx_account_trade ON trade_records(account_id, trade_time);
CREATE INDEX idx_account ON trade_records(account_id);
CREATE INDEX idx_account ON orders(account_id);

-- 3. 更新现有数据（如果有）
UPDATE stock_positions SET account_id = 'default' WHERE account_id IS NULL;
UPDATE trade_records SET account_id = 'default' WHERE account_id IS NULL;
UPDATE orders SET account_id = 'default' WHERE account_id IS NULL;
```

---

## 🔧 代码实现

### 1. 多账户管理器

```python
# services/common/account_manager.py
"""多账户管理器 v6.1.2"""

import json
import os
from typing import Dict, Optional
from dataclasses import dataclass
from services.common import get_logger

logger = get_logger("account_manager")

@dataclass
class AccountConfig:
    """账户配置"""
    account_id: str
    username: str
    password: str
    display_name: str
    is_active: bool = True

class AccountManager:
    """多账户管理器"""
    
    def __init__(self, config_path: str = "config/accounts.json"):
        self.config_path = config_path
        self.accounts: Dict[str, AccountConfig] = {}
        self._load_accounts()
    
    def _load_accounts(self):
        """加载账户配置"""
        if not os.path.exists(self.config_path):
            logger.error(f"账户配置文件不存在：{self.config_path}")
            return
        
        with open(self.config_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        for account_id, config in data.items():
            self.accounts[account_id] = AccountConfig(
                account_id=account_id,
                username=config.get('username', ''),
                password=config.get('password', ''),
                display_name=config.get('display_name', account_id),
                is_active=config.get('is_active', True)
            )
        
        logger.info(f"加载了 {len(self.accounts)} 个账户")
    
    def get_account(self, account_id: str) -> Optional[AccountConfig]:
        """获取账户配置"""
        return self.accounts.get(account_id)
    
    def list_accounts(self) -> Dict[str, AccountConfig]:
        """列出所有账户"""
        return self.accounts
    
    def validate_account(self, account_id: str) -> bool:
        """验证账户是否存在"""
        return account_id in self.accounts

# 全局账户管理器实例
_account_manager: Optional[AccountManager] = None

def get_account_manager() -> AccountManager:
    """获取账户管理器单例"""
    global _account_manager
    if _account_manager is None:
        _account_manager = AccountManager()
    return _account_manager
```

### 2. API 路由改造

```python
# services/api/routes.py
"""API 路由 v6.1.2 - 多账户支持"""

from fastapi import APIRouter, HTTPException
from services.common import get_account_manager
from services.galaxy import get_galaxy_client

router = APIRouter()

@router.get("/api/{account_id}/health")
async def health_check(account_id: str):
    """健康检查"""
    account_manager = get_account_manager()
    if not account_manager.validate_account(account_id):
        raise HTTPException(status_code=404, detail="账户不存在")
    
    return {
        "status": "healthy",
        "account_id": account_id
    }

@router.get("/api/{account_id}/positions")
async def get_positions(account_id: str):
    """获取持仓"""
    account_manager = get_account_manager()
    account = account_manager.get_account(account_id)
    if not account:
        raise HTTPException(status_code=404, detail="账户不存在")
    
    # 使用对应账户的银河客户端
    galaxy_client = get_galaxy_client(account.username, account.password)
    positions = await galaxy_client.get_positions()
    
    # 过滤数据库中的持仓（确保 account_id 匹配）
    # positions = [p for p in positions if p.account_id == account_id]
    
    return {"positions": positions}

@router.get("/api/{account_id}/trades")
async def get_trades(account_id: str):
    """获取交易记录"""
    # 类似 get_positions 的实现
    pass

@router.post("/api/{account_id}/trades/execute")
async def execute_trade(account_id: str, trade_data: dict):
    """执行交易"""
    # 使用对应账户的银河客户端执行交易
    pass
```

### 3. 数据库查询改造

```python
# models/database.py
"""数据库模型 v6.1.2 - 多账户支持"""

from sqlalchemy import Column, String, BigInteger, ...

class StockPosition(Base):
    __tablename__ = "stock_positions"
    
    id = Column(BigInteger, primary_key=True)
    account_id = Column(String(32), nullable=False, index=True)  # 新增
    stock_code = Column(String(20), nullable=False)
    quantity = Column(Integer, default=0)
    # ... 其他字段
    
    @classmethod
    def get_by_account(cls, db, account_id: str):
        """获取指定账户的持仓"""
        return db.query(cls).filter(cls.account_id == account_id).all()
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
curl http://<邻居 Tailscale IP>:8080/api/bobo/health

# 3. 测试获取持仓
curl http://<邻居 Tailscale IP>:8080/api/bobo/positions

# 4. 测试浩哥的账户
curl http://<邻居 Tailscale IP>:8080/api/haoge/health
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

---

**文档版本：v6.1.2**  
**创建时间：2026-03-28**  
**最后更新：2026-03-28**

**作者：Jessie + 波哥 + 浩哥** 🚀
