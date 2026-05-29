# -*- coding: utf-8 -*-
"""
MCP Resources

提供静态/动态资源供 AI 助手获取系统上下文。
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from fastmcp import FastMCP
from services.mcp.utils import get_api_client
from services.mcp.server import mcp

api = get_api_client()


# ================================================================
# 文档资源
# ================================================================

@mcp.resource("stockwinner://doc/spec")
async def get_spec() -> str:
    """
    获取 Agent API 规格文档

    Returns:
        完整的 Agent API 规格（JSON 格式的文本）
    """
    result = await api.get("/spec")

    if result.get("success"):
        # 将 JSON 结构化为可读文本
        spec = result.get("data", result)

        text = "# StockWinner Agent API 规格\n\n"
        text += f"**系统**: {spec.get('system', {}).get('name', 'StockWinner')}\n"
        text += f"**版本**: {spec.get('system', {}).get('version', 'N/A')}\n\n"

        # 数据表
        text += "## 数据表\n\n"
        for table in spec.get("data_tables", []):
            text += f"- **{table.get('name')}**: {table.get('description')}\n"

        # Agent API 端点
        text += "\n## Agent API 端点\n\n"
        api_spec = spec.get("agent_api", {})
        for category, endpoints in api_spec.items():
            text += f"### {category}\n"
            for ep in endpoints:
                text += f"- `{ep.get('path')}` ({ep.get('method')}): {ep.get('description')}\n"

        return text
    else:
        return f"获取规格文档失败: {result.get('message', '未知错误')}"


@mcp.resource("stockwinner://doc/api-summary")
async def get_api_summary() -> str:
    """
    获取 MCP 工具概览

    Returns:
        MCP 工具分类和用途说明
    """
    return """# StockWinner MCP 工具概览

## 工具分类

### 1. SDK 数据查询 (query_data)
- `mcp_kline`: K 线数据（日线/周线/月线）
- `mcp_market`: 实时行情（PriceCache）
- `mcp_factors`: 日频因子（MA/RSI/MACD/BOLL/KDJ）
- `mcp_stock_code`: 代码与名称互查
- `mcp_index_list/kline/constituent`: 指数数据
- `mcp_industry_list/kline/constituent`: 行业数据
- `mcp_financial_income/balance/cashflow`: 财务报表
- `mcp_dragon_tiger`: 龙虎榜
- `mcp_margin_summary/detail`: 融资融券
- `mcp_block_trading`: 大宗交易
- `mcp_treasury_yield`: 国债收益率

### 2. 业务数据查询 (query_business)
- `mcp_dashboard`: 仪表盘
- `mcp_positions`: 当前持仓
- `mcp_trades`: 交易记录
- `mcp_signals`: 交易信号
- `mcp_watchlist`: 候选股列表
- `mcp_strategies`: 策略列表
- `mcp_strategy_detail`: 策略详情
- `mcp_strategy_versions`: 版本历史
- `mcp_trading_strategy_stock/list`: 止盈止损
- `mcp_performance_summary`: 绩效汇总
- `mcp_capital_overview`: 资金概况
- `mcp_health`: 系统健康

### 3. 系统管理 (manage) - 需要 operator+
- `mcp_scheduler_run_now`: 立即执行任务
- `mcp_scheduler_toggle`: 启禁用任务
- `mcp_monitoring_start/stop`: 监控服务
- `mcp_screening_start/stop`: 选股服务
- `mcp_strategy_execute`: 执行策略

### 4. 策略操作 (submit) - 需要 strategist+
- `mcp_create_screening_strategy`: 创建选股策略
- `mcp_create_code_strategy`: 创建代码策略
- `mcp_update_strategy`: 更新策略
- `mcp_delete_strategy`: 删除策略
- `mcp_restore_strategy_version`: 恢复版本
- `mcp_set/delete_trading_strategy`: 止盈止损

## 权限要求

| 角色 | 可用工具 |
|------|---------|
| viewer | 所有查询工具 |
| strategist | 查询 + 策略创建/更新 |
| operator | 查询 + 策略管理 + 系统管理 |
| admin | 全部 |

## 使用示例

```
# 查询持仓
mcp_positions(account_id="bobo")

# 查询 K 线
mcp_kline(stock_code="600000.SH", period="day", limit=30)

# 设置止损
mcp_set_trading_strategy(account_id="bobo", stock_code="600000.SH", stop_loss_pct=0.05)
```
"""


@mcp.resource("stockwinner://system/status")
async def get_system_status() -> str:
    """
    获取系统实时状态

    Returns:
        系统运行状态（监控、选股、调度等）
    """
    # 尝试获取健康状态
    result = await api.get("/query/health", {"account_id": "bobo"})

    if result.get("success"):
        return f"""# 系统状态

**SDK 连接**: {result.get('sdk_connected', '未知')}
**持仓数量**: {result.get('position_count', 0)}
**待执行信号**: {result.get('pending_signals', 0)}
**可用现金**: {result.get('available_cash', 0)}
**账户状态**: {result.get('account_active', '未知')}
"""
    else:
        return f"获取系统状态失败: {result.get('message', '未知错误')}"