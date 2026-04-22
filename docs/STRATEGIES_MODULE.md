# 策略管理模块文档

## 概述

策略管理模块提供完整的投资策略创建、管理、回测功能，支持手动创建和 LLM 自动生成两种模式。

## 功能特性

### 后端 API

| 方法 | 端点 | 说明 |
|------|------|------|
| GET | `/api/v1/ui/{account_id}/strategies` | 获取策略列表 |
| GET | `/api/v1/ui/{account_id}/strategies?status=active` | 按状态筛选策略 |
| GET | `/api/v1/ui/{account_id}/strategies/{strategy_id}` | 获取策略详情 |
| POST | `/api/v1/ui/{account_id}/strategies` | 创建新策略 |
| PUT | `/api/v1/ui/{account_id}/strategies/{strategy_id}` | 更新策略 |
| DELETE | `/api/v1/ui/{account_id}/strategies/{strategy_id}` | 删除策略 |
| POST | `/api/v1/ui/{account_id}/strategies/{strategy_id}/activate` | 激活策略 |
| POST | `/api/v1/ui/{account_id}/strategies/{strategy_id}/deactivate` | 停用策略 |
| GET | `/api/v1/ui/{account_id}/strategies/{strategy_id}/backtest` | 获取回测结果 |
| POST | `/api/v1/ui/{account_id}/strategies/generate` | LLM 生成策略 |

### 策略状态

- `draft` - 草稿：策略已创建但未激活
- `active` - 激活：策略正在运行中
- `inactive` - 停用：策略已停止

### 策略类型

- `manual` - 手动创建：用户手动配置策略参数
- `llm` - LLM 生成：由 AI 自动生成的策略

## 使用示例

### 1. 创建策略

```bash
curl -X POST http://localhost:8080/api/v1/ui/bobo/strategies \
  -H "Content-Type: application/json" \
  -d '{
    "name": "MA 金叉策略",
    "description": "5 日均线上穿 20 日均线时买入",
    "strategy_type": "manual",
    "config": {
      "buy": ["MA5>MA20"],
      "sell": ["MA5<MA20"],
      "stop_loss": 0.05
    }
  }'
```

### 2. 获取策略列表

```bash
# 获取全部策略
curl http://localhost:8080/api/v1/ui/bobo/strategies

# 只获取激活的策略
curl http://localhost:8080/api/v1/ui/bobo/strategies?status=active
```

### 3. 激活策略

```bash
curl -X POST http://localhost:8080/api/v1/ui/bobo/strategies/1/activate
```

### 4. 获取回测结果

```bash
curl http://localhost:8080/api/v1/ui/bobo/strategies/1/backtest
```

### 5. LLM 生成策略

```bash
curl -X POST http://localhost:8080/api/v1/ui/bobo/strategies/generate \
  -H "Content-Type: application/json" \
  -d '{
    "description": "低估值蓝筹股策略，PE<10，股息率>5%",
    "risk_level": "low"
  }'
```

## 数据库结构

### strategies 表

```sql
CREATE TABLE strategies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id TEXT NOT NULL,           -- 账户 ID（多账户隔离）
    name TEXT NOT NULL,                 -- 策略名称
    description TEXT,                   -- 策略描述
    strategy_type TEXT DEFAULT 'manual',-- 策略类型
    config TEXT,                        -- 策略配置（JSON）
    status TEXT DEFAULT 'draft',        -- 策略状态
    is_active INTEGER DEFAULT 1,        -- 是否激活
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## 回测指标

| 指标 | 说明 |
|------|------|
| total_return | 总收益率 |
| annual_return | 年化收益率 |
| sharpe_ratio | 夏普比率（风险调整后收益） |
| max_drawdown | 最大回撤 |
| win_rate | 胜率 |
| total_trades | 交易次数 |

## 前端功能

### 策略列表页面

- 策略列表展示
- 状态筛选（全部/草稿/激活/停用）
- 策略类型标识（手动/LLM）

### 操作功能

- **新建策略** - 手动创建策略
- **LLM 生成** - AI 自动生成策略配置
- **详情查看** - 查看策略完整配置
- **激活/停用** - 控制策略运行状态
- **回测** - 查看历史回测结果
- **删除** - 删除不需要的策略

## 多账户隔离

策略数据通过 `account_id` 字段实现完全隔离：

- 波哥 (`bobo`) 的策略列表与浩哥 (`haoge`) 完全独立
- API 访问时自动验证账户权限
- 无法跨账户访问策略数据

## 待实现功能 (v6.2.0)

- [ ] 真实回测引擎
- [ ] 策略实盘运行
- [ ] 策略绩效排行
- [ ] 策略信号推送
- [ ] 策略版本管理

## 文件位置

- 后端 API: `services/ui/strategies.py`
- 前端页面: `frontend/src/views/Strategies.vue`
- 数据库初始化：`scripts/init_db.py`
