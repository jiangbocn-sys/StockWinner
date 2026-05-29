# StockWinner MCP 服务

基于 Agent API 的 MCP 服务，为 Claude Desktop / Claude Code 提供自然语言交互能力。

## 功能

### SDK 数据查询
- K 线数据（日/周/月）
- 实时行情（PriceCache）
- 因子数据（MA/RSI/MACD/BOLL/KDJ）
- 指数与行业数据
- 财务报表
- 龙虎榜、融资融券、大宗交易

### 业务数据查询
- 持仓与交易记录
- 交易信号
- 候选股列表
- 策略详情与版本历史
- 绩效分析
- 资金概况

### 系统管理
- 调度任务启停
- 监控服务控制
- 选股服务控制
- 策略执行

### 策略操作
- 创建选股/代码策略
- 更新/删除策略
- 止盈止损设置

## 安装

```bash
# 安装依赖
pip install fastmcp httpx
```

## 配置

### 1. 创建 MCP Agent

在 StockWinner 中创建专用 Agent（或使用已有 Agent）：

```bash
# 通过 API 创建（需要 admin 权限）
curl -X POST http://localhost:8080/api/v1/agent/admin/agents \
  -H "X-Agent-Key: sk-admin-xxx" \
  -H "Content-Type: application/json" \
  -d '{"name":"mcp-user","role":"operator","allowed_account_ids":["*"]}'
```

返回的 `api_key` 用于 MCP 配置。

### 2. 配置 Claude Desktop

编辑 Claude Desktop 配置文件：
- macOS: `~/Library/Application Support/Claude/claude_desktop_config.json`
- Windows: `%APPDATA%\Claude\claude_desktop_config.json`
- Linux: `~/.config/Claude/claude_desktop_config.json`

```json
{
  "mcpServers": {
    "stockwinner": {
      "command": "python3",
      "args": ["/home/bobo/StockWinner/services/mcp/server.py"],
      "env": {
        "AGENT_API_KEY": "sk-agent-xxx",
        "AGENT_API_BASE_URL": "http://localhost:8080/api/v1/agent"
      }
    }
  }
}
```

### 3. 重启 Claude Desktop

配置生效后，Claude Desktop 会自动加载 MCP 服务。

## 使用示例

### 数据查询

```
用户: 查询工商银行最近30天的K线
Claude: [调用 mcp_kline] → 返回 K 线数据 + 走势分析

用户: 我的持仓情况如何？
Claude: [调用 mcp_positions] → 返回持仓列表 + 盈亏分析

用户: 今天有什么买入信号？
Claude: [调用 mcp_signals] → 返回待执行信号
```

### 系统管理

```
用户: 启动监控服务
Claude: [调用 mcp_monitoring_start] → 启动结果

用户: 执行选股策略
Claude: [调用 mcp_strategy_execute] → 生成的信号列表
```

### 策略操作

```
用户: 为工商银行设置5%止损
Claude: [调用 mcp_set_trading_strategy] → 设置结果
```

## 权限说明

| Agent 角色 | 可用功能 |
|-----------|---------|
| viewer | 所有查询工具 |
| strategist | 查询 + 策略创建/更新 |
| operator | 查询 + 策略管理 + 系统管理 |
| admin | 全部功能 |

## 架构

```
Claude Desktop / Claude Code
    ↓ MCP 协议 (stdio)
StockWinner MCP Server
    ↓ HTTP REST
Agent API (/api/v1/agent/*)
    ↓ IPC
SDK 子进程 → TGW (单连接)
```

**优势**：
- 解决 TGW 连接冲突
- 继承 PriceCache 缓存
- 复用权限校验与审计日志
- 统一管理 SDK 调用

## 工具列表

| 工具名 | 功能 |
|--------|------|
| mcp_kline | K 线查询 |
| mcp_market | 实时行情 |
| mcp_factors | 因子数据 |
| mcp_positions | 持仓查询 |
| mcp_signals | 信号查询 |
| mcp_strategies | 策略列表 |
| mcp_dashboard | 仪表盘 |
| mcp_scheduler_run_now | 执行任务 |
| mcp_monitoring_start/stop | 监控控制 |
| mcp_create_screening_strategy | 创建策略 |
| mcp_set_trading_strategy | 设置止盈止损 |

共 50+ 工具。

## 故障排查

### MCP 服务无法启动

```bash
# 检查 Backend 是否运行
curl http://localhost:8080/api/v1/health

# 检查 Agent Key 是否有效
curl -H "X-Agent-Key: sk-agent-xxx" http://localhost:8080/api/v1/agent/me
```

### 工具调用失败

检查 Agent 权限是否足够：
- viewer: 只能查询
- strategist: 可创建策略
- operator: 可管理服务

### Claude Desktop 不识别 MCP

确保配置文件路径正确，重启 Claude Desktop。