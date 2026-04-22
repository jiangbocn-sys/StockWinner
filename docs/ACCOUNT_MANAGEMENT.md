# 账户管理模块说明

## 概述
账户管理模块提供了完整的账户 CRUD（创建、读取、更新、删除）功能，允许用户通过 API 或前端界面管理交易账户。

## API 接口

### 1. 创建账户
```
POST /api/accounts/create
```
请求体：
```json
{
  "name": "账户名称",
  "username": "用户名",
  "password": "密码",
  "display_name": "显示名称",
  "is_active": 1
}
```

### 2. 获取所有账户
```
GET /api/accounts/
```

### 3. 获取单个账户
```
GET /api/accounts/{account_id}
```

### 4. 更新账户
```
PUT /api/accounts/{account_id}
```
请求体：
```json
{
  "name": "新账户名称",
  "display_name": "新显示名称",
  "is_active": 0
}
```

### 5. 删除账户
```
DELETE /api/accounts/{account_id}
```

### 6. 搜索账户
```
POST /api/accounts/search
```
请求体：
```json
{
  "is_active": 1,
  "name": "搜索关键词"
}
```

### 7. 获取统计信息
```
GET /api/accounts/statistics
```

## 前端界面
访问 `/ui/#/accounts` 进入账户管理页面。

功能：
- 账户列表展示
- 搜索和过滤
- 创建新账户
- 编辑账户信息
- 删除账户
- 统计信息展示

## 数据库结构
accounts 表字段：
- `id`: 自增主键
- `account_id`: 账户唯一标识（8 位大写字母+数字）
- `name`: 账户名称（唯一）
- `username`: 用户名
- `password_hash`: 密码（SHA256 哈希）
- `display_name`: 显示名称
- `is_active`: 是否激活（1=激活，0=禁用）
- `created_at`: 创建时间
- `updated_at`: 更新时间

## 使用示例

### 创建账户
```bash
curl -X POST http://localhost:8080/api/accounts/create \
  -H "Content-Type: application/json" \
  -d '{
    "name": "test_account",
    "username": "test_user",
    "password": "test_password",
    "display_name": "测试账户"
  }'
```

### 获取所有账户
```bash
curl http://localhost:8080/api/accounts/
```

### 更新账户
```bash
curl -X PUT http://localhost:8080/api/accounts/{account_id} \
  -H "Content-Type: application/json" \
  -d '{
    "display_name": "更新后的名称",
    "is_active": 0
  }'
```

### 删除账户
```bash
curl -X DELETE http://localhost:8080/api/accounts/{account_id}
```

## 版本历史
- v6.1.4 (2026-03-30): 新增账户管理模块
  - 支持账户增删改查
  - 支持账户状态管理
  - 支持密码哈希存储
  - 前端账户管理界面