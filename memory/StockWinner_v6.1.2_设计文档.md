---
name: StockWinner v6.1.2 设计文档
description: StockWinner 智能股票交易系统 v6.1.2 多账户支持版本设计文档摘要
type: reference
---

# StockWinner v6.1.2 设计文档摘要

## 项目概述
- **目标**: Ubuntu 22.04/24.04 LTS 上运行智能股票交易系统
- **部署位置**: 邻居 (浩哥) 家的 Ubuntu x86 服务器
- **访问方式**: 通过 Tailscale 远程访问
- **端口**: 8080

## v6.1.2 核心特性
1. **多账户支持** — 配置驱动，支持 2-10 个账户
2. **数据隔离** — SQLite 单库多租户，通过 account_id 字段隔离
3. **API 路由** — `/api/v1/ui/{account_id}/*` 格式
4. **UI 账户切换** — 导航栏右上角下拉框切换账户

## 配置文件
**config/accounts.json**:
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

## 数据库变更
- `stock_positions`, `trade_records`, `orders` 表新增 `account_id VARCHAR(32)` 字段
- 新增 `accounts` 表存储账户信息
- 索引：`idx_account_stock`, `idx_account_trade`, `idx_account`

## UI 页面
| 页面 | 路径 | 功能 |
|------|------|------|
| 仪表盘 | `/ui/{account_id}/dashboard` | 系统健康度、交易统计、资源开销 |
| 交易监控 | `/ui/{account_id}/trades` | 交易流水、统计汇总 |
| 持仓分析 | `/ui/{account_id}/positions` | 持仓列表、盈亏分布 |
| 策略管理 | `/ui/{account_id}/strategies` | 策略列表、回测结果 |
| 系统设置 | `/ui/{account_id}/settings` | API 配置、日志查看 |

## 待完成工作
- [ ] 多账户管理器代码 (AccountManager)
- [ ] API 路由改造 (支持 account_id 参数)
- [ ] 数据库迁移脚本执行
- [ ] UI 账户切换功能
- [ ] Tailscale 安装配置

## 技术栈
- **后端**: FastAPI + uvicorn + uvloop
- **前端**: Vue 3 + Vite + Element Plus + ECharts + Pinia
- **数据库**: SQLite 3.x (WAL 模式)
- **网络**: Tailscale 远程访问

## 用户信息
- **波哥** - 主账户用户
- **浩哥** - 邻居，提供服务器资源
