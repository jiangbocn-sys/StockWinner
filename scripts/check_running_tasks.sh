#!/bin/bash
# StockWinner 重启前任务状态检查脚本
# 用于 systemctl restart 或手动重启前检查是否有正在执行的任务

PROJECT_DIR="/home/bobo/StockWinner"
API_BASE="http://localhost:8080/api/v1/ui"

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "=== StockWinner 任务状态检查 ==="

# 检查服务是否运行
if ! curl -s --max-time 2 "${API_BASE}/system/running-tasks" > /dev/null 2>&1; then
    echo -e "${YELLOW}服务未运行或无法访问，跳过任务检查${NC}"
    exit 0
fi

# 获取正在运行的任务
RESULT=$(curl -s --max-time 5 "${API_BASE}/system/running-tasks")

# 解析 JSON（使用 python3）
HAS_RUNNING=$(python3 -c "
import json, sys
try:
    data = json.loads('$RESULT')
    print(data.get('has_running', False))
except:
    print('false')
")

BLOCKING_COUNT=$(python3 -c "
import json, sys
try:
    data = json.loads('$RESULT')
    print(data.get('blocking_count', 0))
except:
    print('0')
")

MESSAGE=$(python3 -c "
import json, sys
try:
    data = json.loads('$RESULT')
    print(data.get('message', ''))
except:
    print('')
")

if [ "$HAS_RUNNING" = "True" ]; then
    echo -e "${RED}警告：${MESSAGE}${NC}"
    echo ""
    echo "正在运行的任务列表："

    # 打印任务详情
    python3 -c "
import json
try:
    data = json.loads('$RESULT')
    for task in data.get('tasks', []):
        t_type = task.get('type', 'unknown')
        t_name = task.get('name', 'unnamed')
        t_progress = task.get('progress', {})
        if isinstance(t_progress, dict):
            pct = t_progress.get('percent', t_progress.get('progress', 0))
            msg = t_progress.get('message', '')
            print(f'  - [{t_type}] {t_name}: {pct}% {msg}')
        else:
            print(f'  - [{t_type}] {t_name}: {t_progress}')
except:
    pass
"
    echo ""

    # 如果是交互式终端，询问用户
    if [ -t 0 ]; then
        echo -e "${YELLOW}是否继续重启？这将中断上述任务。${NC}"
        read -p "输入 'yes' 确认继续，或 'no' 取消: " CONFIRM
        if [ "$CONFIRM" != "yes" ]; then
            echo "已取消重启"
            exit 1
        fi
        echo -e "${GREEN}用户已确认，继续重启...${NC}"
    else
        # 非交互式（如 systemd），直接退出错误
        echo -e "${RED}非交互式终端，无法确认。请手动检查后使用 --force 参数强制重启${NC}"
        exit 1
    fi
else
    echo -e "${GREEN}系统中无正在执行的任务，可以安全重启${NC}"
fi

exit 0