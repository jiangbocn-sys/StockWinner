# -*- coding: utf-8 -*-
"""
StockWinner MCP 服务 - 主入口（双模式支持）

基于 Agent API 的 MCP 服务，提供：
- SDK 数据查询（K线、行情、因子等）
- 业务数据查询（持仓、信号、策略等）
- 系统管理（调度、监控启停）
- 策略操作（创建、更新、删除）

传输模式：
- stdio: Claude Desktop/Code 本地集成
- http: OpenClaw/Hermes 外部 Agent 远程访问

所有调用通过 Agent API → SDK 子进程 → 单 TGW 连接。
"""

import os
import sys
import logging
import argparse
from pathlib import Path

# 添加项目根目录到 Python 路径
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from services.mcp import mcp

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# 创建 FastMCP 实例
# 导入所有工具模块（工具定义在各自的模块中）
# 使用绝对导入路径
import services.mcp.tools.query_data
import services.mcp.tools.query_business
import services.mcp.tools.manage
import services.mcp.tools.submit
import services.mcp.tools.resources

# ================================================================
# 启动入口
# ================================================================

def main():
    """启动 MCP 服务（支持双模式）"""
    # 解析命令行参数
    parser = argparse.ArgumentParser(description="StockWinner MCP Server")
    parser.add_argument("--http", action="store_true", help="使用 HTTP 模式（用于外部 Agent）")
    parser.add_argument("--port", type=int, default=9000, help="HTTP 端口（默认 9000）")
    parser.add_argument("--host", default="0.0.0.0", help="绑定主机（默认 0.0.0.0，所有接口）")
    parser.add_argument("--stdio", action="store_true", help="使用 stdio 模式（默认）")
    args = parser.parse_args()

    # 打印启动信息
    logger.info("=" * 50)
    logger.info("StockWinner MCP 服务启动")
    logger.info("=" * 50)
    logger.info(f"Agent API Base URL: {os.getenv('AGENT_API_BASE_URL', 'http://localhost:8080/api/v1/agent')}")
    logger.info(f"Agent API Key: {os.getenv('AGENT_API_KEY', 'sk-mcp-proxy')[:10]}...")
    logger.info("工具模块: query_data, query_business, manage, submit, resources")

    # 确定传输模式
    if args.http:
        logger.info(f"传输模式: streamable-http ({args.host}:{args.port})")
        logger.info("适用场景: OpenClaw, Hermes, 其他外部 Agent")
        logger.info("=" * 50)
        # 使用 streamable-http（MCP 2025-03-26 规范）
        mcp.run(transport="streamable-http", host=args.host, port=args.port)
    else:
        # 默认 stdio 模式（Claude Desktop 不需要 --stdio 参数）
        logger.info("传输模式: stdio")
        logger.info("适用场景: Claude Desktop, Claude Code")
        logger.info("=" * 50)
        mcp.run(transport="stdio")


if __name__ == "__main__":
    main()