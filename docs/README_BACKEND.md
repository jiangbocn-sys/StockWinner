# StockWinner 后端服务

## 目录结构

```
services/
├── main.py              # FastAPI 主应用入口
├── common/              # 公共模块
│   ├── __init__.py
│   ├── account_manager.py  # 多账户管理器
│   └── database.py         # 数据库管理器
└── ui/                  # UI API 模块
    ├── __init__.py
    ├── accounts.py         # 账户管理 API
    ├── dashboard.py        # 仪表盘 API
    ├── positions.py        # 持仓管理 API
    └── trades.py           # 交易记录 API
```

## 模块说明

### 1. AccountManager 多账户管理器

**文件**: `services/common/account_manager.py`

**功能**:
- 从 `config/accounts.json` 加载账户配置
- 账户验证（是否存在且激活）
- 获取账户列表/详情
- 单例模式实现

**使用示例**:
```python
from services.common.account_manager import get_account_manager

account_manager = get_account_manager()

# 验证账户
if account_manager.validate_account('bobo'):
    print("账户有效")

# 获取账户信息
account = account_manager.get_account('bobo')
print(account.display_name)

# 获取账户列表
accounts = account_manager.list_accounts()
```

### 2. DatabaseManager 数据库管理器

**文件**: `services/common/database.py`

**功能**:
- SQLite 异步数据库连接
- WAL 模式配置（提升并发性能）
- 事务上下文管理
- 通用 CRUD 方法

**使用示例**:
```python
from services.common.database import get_db_manager

db = get_db_manager()

# 查询
rows = await db.fetchall(
    "SELECT * FROM stock_positions WHERE account_id = ?",
    ('bobo',)
)

# 插入
await db.insert('orders', {
    'account_id': 'bobo',
    'stock_code': '600519.SH',
    'quantity': 100
})

# 更新
await db.update('stock_positions',
    {'current_price': 1685.0},
    'account_id = ? AND stock_code = ?',
    ('bobo', '600519.SH')
)
```

### 3. UI API 模块

#### 账户管理 API (`/api/v1/ui/accounts`)

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/v1/ui/accounts` | 获取账户列表 |
| GET | `/api/v1/ui/accounts/{account_id}` | 获取账户详情 |

#### 仪表盘 API (`/api/v1/ui/{account_id}/dashboard`)

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/v1/ui/{account_id}/dashboard` | 仪表盘总览 |
| GET | `/api/v1/ui/{account_id}/health` | 健康检查 |

#### 持仓管理 API (`/api/v1/ui/{account_id}/positions`)

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/v1/ui/{account_id}/positions` | 获取持仓列表 |
| GET | `/api/v1/ui/{account_id}/positions/{stock_code}` | 获取单只股票持仓 |

#### 交易记录 API (`/api/v1/ui/{account_id}/trades`)

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/v1/ui/{account_id}/trades/today` | 今日交易记录 |
| GET | `/api/v1/ui/{account_id}/trades` | 交易记录（支持筛选） |

## 启动服务

```bash
# 安装依赖
pip install -r requirements.txt

# 初始化数据库
python scripts/init_db.py

# 启动服务
python -m uvicorn services.main:app --host 0.0.0.0 --port 8080

# 后台运行
nohup python -m uvicorn services.main:app --host 0.0.0.0 --port 8080 > logs/app.log 2>&1 &
```

## 测试

```bash
# 健康检查
curl http://localhost:8080/api/v1/health

# 账户健康检查
curl http://localhost:8080/api/v1/ui/bobo/health

# 获取账户列表
curl http://localhost:8080/api/v1/ui/accounts

# 获取仪表盘
curl http://localhost:8080/api/v1/ui/bobo/dashboard

# 获取持仓
curl http://localhost:8080/api/v1/ui/bobo/positions
```

## 多账户隔离机制

所有数据查询都通过 `account_id` 参数进行隔离：

```python
# 所有查询都带 account_id 过滤
cursor = await conn.execute("""
    SELECT * FROM stock_positions
    WHERE account_id = ? AND quantity > 0
""", (account_id,))
```

API 路由格式：`/api/v1/ui/{account_id}/*`
