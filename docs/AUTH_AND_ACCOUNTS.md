# 账户管理和认证系统使用说明

## 概述

StockWinner v6.2.0 引入了基于数据库的多账户认证系统，支持：
- 用户登录/登出
- 会话管理
- 不同用户使用各自的券商账号连接银河证券 SDK

## 系统账户 vs 券商账户

系统中有两种账户概念：

### 1. 系统账户（用于登录本系统）
- **用户名**：`username` 字段
- **密码**：`password_hash` 字段（SHA256 哈希存储）
- **用途**：登录 StockWinner 系统

### 2. 券商账户（用于连接银河证券）
- **资金账号**：`broker_account` 字段（明文存储）
- **资金密码**：`broker_password` 字段（明文存储）
- **用途**：登录银河证券 SDK 获取行情数据

## 快速开始

### 1. 创建系统账户

访问账户管理页面：`http://localhost:8080/ui/#/accounts`

点击"新增账户"，填写以下信息：

**必填字段（系统登录用）：**
- 账户名称：唯一标识，如 `account_001`
- 用户名：登录系统用的用户名
- 密码：登录系统用的密码（至少 6 位）
- 显示名称：界面上显示的名称

**可选字段（银河证券用）：**
- 资金账号：银河证券资金账号
- 资金密码：银河证券交易密码
- 开户券商：如"银河证券北京分公司"
- 服务器 IP：如 `140.206.44.234`
- 服务器端口：如 `8600`
- 账户状态：正常/冻结/销户
- 备注：其他备注信息

### 2. 登录系统

访问登录页面：`http://localhost:8080/ui/#/login`

输入用户名和密码，点击登录。

登录后：
- Token 存储在 localStorage 中
- 会话有效期 1 小时
- 访问其他页面会自动携带 token

### 3. 使用券商账号获取行情

当用户登录后，系统会自动：
1. 从数据库读取用户的 `broker_account` 和 `broker_password`
2. 使用这些 credentials 创建银河 SDK 连接
3. 获取行情数据时使用该用户的专属连接

这样不同用户可以使用各自不同的券商账号。

## API 使用

### 登录
```bash
curl -X POST http://localhost:8080/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username": "test", "password": "test123"}'
```

响应：
```json
{
  "success": true,
  "message": "登录成功",
  "token": "uuid-token-here",
  "account": {
    "username": "test",
    "display_name": "测试账户",
    "broker_account": "123456789",
    ...
  }
}
```

### 获取当前用户
```bash
curl http://localhost:8080/api/auth/me \
  -H "X-Auth-Token: your-token"
```

### 获取券商 credentials
```bash
curl http://localhost:8080/api/auth/broker-credentials \
  -H "X-Auth-Token: your-token"
```

### 登出
```bash
curl -X POST http://localhost:8080/api/auth/logout \
  -H "X-Auth-Token: your-token"
```

## 数据库结构

```sql
-- 系统登录相关
username       TEXT     -- 登录用户名
password_hash  TEXT     -- SHA256 哈希的密码
is_active      INTEGER  -- 是否激活 (1/0)

-- 券商账户相关（明文存储）
broker_account     TEXT     -- 银河证券资金账号
broker_password    TEXT     -- 银河证券交易密码
broker_company     TEXT     -- 开户券商
broker_server_ip   TEXT     -- 服务器 IP
broker_server_port INTEGER  -- 服务器端口
broker_status      TEXT     -- normal/frozen/closed
notes              TEXT     -- 备注
```

## 测试账户

系统预置了测试账户：
- 用户名：`test`
- 密码：`test123`

## 安全说明

1. **系统密码**：使用 SHA256 哈希存储，相对安全
2. **券商密码**：明文存储，因为需要传递给银河 SDK
3. **会话管理**：Token 存储在内存中，1 小时过期
4. **CORS**：当前允许所有来源，生产环境应限制具体域名

## 故障排除

### 登录失败
- 检查用户名和密码是否正确
- 检查账户是否被禁用（`is_active = 1`）

### 行情数据获取失败
- 检查是否配置了券商账号和密码
- 检查券商账号是否有效
- 检查网络连接

### Token 过期
- Token 有效期 1 小时
- 过期后需要重新登录
- 前端会自动重定向到登录页
