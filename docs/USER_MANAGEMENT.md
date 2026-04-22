# 用户管理逻辑说明 (v6.2.0)

## 架构概述

系统采用 **单表结构** 管理用户账户，所有用户信息存储在 `accounts` 表中。

**重要变更 (v6.2.0)**:
- 移除了 `username` 字段，统一使用 `name` 作为用户登录名
- 移除了配置文件 `config/accounts.json` 的依赖，所有账户信息从数据库读取
- 明确区分**系统登录用户名**和**券商账户名**

## 数据模型

### 数据库表：`accounts`

| 字段名 | 类型 | 说明 |
|--------|------|------|
| `id` | INTEGER | 主键 ID |
| `account_id` | TEXT | 账户唯一标识（用于 API 调用） |
| `name` | TEXT | **用户名/账户名**（登录用，唯一） |
| `password_hash` | TEXT | SHA256 密码哈希 |
| `display_name` | TEXT | UI 显示名称 |
| `is_active` | INTEGER | 是否激活 |
| `available_cash` | REAL | 可用资金 |
| `broker_account` | TEXT | **券商资金账号**（用于登录券商服务器） |
| `broker_password` | TEXT | **券商交易密码** |
| `broker_company` | TEXT | 开户券商名称 |
| `broker_server_ip` | TEXT | 券商行情服务器 IP |
| `broker_server_port` | INTEGER | 券商行情服务器端口 |
| `broker_status` | TEXT | 券商账户状态 |
| `notes` | TEXT | 备注 |
| `created_at` | DATETIME | 创建时间 |
| `updated_at` | DATETIME | 更新时间 |

### 字段说明

**系统用户名字段 (`name`)**:
- 用于登录 StockWinner 系统
- 必须唯一
- 例如：`bobo`, `hao`

**券商账户字段 (`broker_account`)**:
- 用于登录券商行情/交易系统
- 由券商分配的资金账号
- 例如：`REDACTED_SDK_USERNAME`

**重要**: 系统用户名和券商账户名是**完全不同的概念**，不要混淆。

## 认证流程

### 1. 登录 (`POST /api/auth/login`)

```json
请求:
{
  "name": "bobo",
  "password": "your_password"
}

响应:
{
  "success": true,
  "message": "登录成功",
  "token": "uuid-token",
  "account": {
    "account_id": "8229DE7E",
    "name": "bobo",
    "display_name": "account1",
    ...
  }
}
```

### 2. 认证 (`X-Auth-Token` 请求头)

所有需要认证的 API 请求需在 Header中携带:
```
X-Auth-Token: <token>
```

### 3. 登出 (`POST /api/auth/logout`)

使 token 失效。

## 关键组件

### `services/auth/service.py` - AuthService

- `login(name, password)`: 登录验证（从数据库查询）
- `logout(token)`: 登出
- `validate_token(token)`: 验证会话
- `get_broker_credentials(token)`: 获取券商账户信息

### `services/common/account_manager.py` - AccountManager

- 单例模式，基于数据库管理账户
- `get_account(account_id)`: 从数据库获取账户
- `validate_account(account_id)`: 验证账户是否存在且激活
- `list_accounts()`: 获取所有账户

### `services/common/database.py` - DatabaseManager

- SQLite 数据库连接管理
- 提供 `fetchone`, `fetchall`, `execute` 等方法

## 前端集成

### 登录表单

```javascript
{
  name: '',      // 用户名
  password: ''   // 密码
}
```

### 用户信息显示

```javascript
// NavBar.vue
{{ currentUser?.name }}  // 显示用户名
{{ currentUser?.display_name }}  // 显示昵称
```

## 账户管理

### 创建账户

```bash
POST /api/v1/ui/accounts/create
{
  "name": "newuser",
  "password": "password123",
  "display_name": "新用户",
  "available_cash": 1000000,
  "broker_account": "123456789",
  "broker_password": "broker_password",
  "broker_company": "银河证券",
  ...
}
```

### 更新账户

```bash
PUT /api/v1/ui/accounts/{account_id}
{
  "display_name": "新显示名",
  "available_cash": 2000000,
  "is_active": 1
}
```

## 安全注意事项

- 密码使用 SHA256 哈希存储在数据库
- 会话 token 存储在内存中，重启后失效
- 会话过期时间：3600 秒（1 小时）
- 券商密码敏感信息应妥善保管

## 迁移指南 (v6.1.x → v6.2.0)

如果您从旧版本升级，需要执行以下迁移：

### 1. 数据库迁移

```sql
-- 将 username 字段的值复制到 name 字段
UPDATE accounts SET name = username;

-- 删除 username 字段（需要重建表）
-- （系统会自动处理）
```

### 2. 更新登录方式

- 旧：使用 `username` 字段登录
- 新：使用 `name` 字段登录（实际是同一个值）

### 3. 配置文件

**v6.2.0 之后**: `config/accounts.json` 已删除，不再需要此文件。

系统仅剩的配置文件是 `config/llm.json`，用于存储 LLM API 密钥。

## 常见问题

**Q: `name` 和 `broker_account` 有什么区别？**

A: `name` 是用于登录 StockWinner 系统的用户名；`broker_account` 是券商分配的资金账号，用于连接券商交易系统。两者完全独立。

**Q: 如何修改密码？**

A: 通过前端"修改密码"功能，或直接调用 `PUT /api/auth/password` API。

**Q: 忘记密码怎么办？**

A: 需要直接操作数据库重置密码哈希。
