#!/bin/bash
# 安装 StockWinner 系统服务
# 需要 sudo 权限

PROJECT_DIR="/home/bobo/StockWinner"

echo "安装 StockWinner 系统服务..."

# 1. 复制服务文件到 systemd 目录
echo "复制服务文件..."
sudo cp "$PROJECT_DIR/config/stockwinner-backend.service" /etc/systemd/system/
sudo cp "$PROJECT_DIR/config/stockwinner-mcp.service" /etc/systemd/system/

# 2. 确保日志目录存在
mkdir -p "$PROJECT_DIR/logs"

# 3. 重载 systemd 配置
echo "重载 systemd 配置..."
sudo systemctl daemon-reload

# 4. 启用服务（开机自启）
echo "启用开机自启..."
sudo systemctl enable stockwinner-backend
sudo systemctl enable stockwinner-mcp

# 5. 显示服务状态
echo ""
echo "服务配置完成，当前状态："
echo "--- Backend ---"
sudo systemctl status stockwinner-backend --no-pager || true
echo ""
echo "--- MCP ---"
sudo systemctl status stockwinner-mcp --no-pager || true

echo ""
echo "使用命令："
echo "  启动: sudo systemctl start stockwinner-backend stockwinner-mcp"
echo "  停止: sudo systemctl stop stockwinner-backend stockwinner-mcp"
echo "  重启: sudo systemctl restart stockwinner-backend stockwinner-mcp"
echo "  状态: sudo systemctl status stockwinner-backend stockwinner-mcp"
echo "  日志: tail -f $PROJECT_DIR/logs/backend.log"
echo "        tail -f $PROJECT_DIR/logs/mcp.log"