#!/bin/bash
# stock_daily_factors 数据校正 - 后台运行脚本
# 使用方法：
#   ./run_correction.sh          # 继续之前的进度
#   ./run_correction.sh reset    # 重置进度从头开始

cd /home/bobo/StockWinner

# 日志文件
LOG_FILE="logs/correction_$(date +%Y%m%d_%H%M%S).log"

# 创建日志目录
mkdir -p logs

echo "========================================"
echo "stock_daily_factors 数据校正"
echo "========================================"
echo "启动时间：$(date '+%Y-%m-%d %H:%M:%S')"
echo "日志文件：$LOG_FILE"
echo ""

# 检查是否有进度文件
if [ -f "data/correction_progress.json" ]; then
    echo "检测到进度文件，将从中断处继续"
    cat data/correction_progress.json | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'已处理：{len(d.get(\"processed_stocks\", []))} 只股票')" 2>/dev/null || true
    echo ""
fi

# 参数
RESET_PARAM=""
if [ "$1" == "reset" ]; then
    RESET_PARAM="--reset"
    echo "将重置进度，从头开始处理"
    echo ""
fi

# 启动后台进程
echo "启动后台进程..."
nohup python3 -u services/factors/correct_daily_factors.py $RESET_PARAM > "$LOG_FILE" 2>&1 &

PID=$!
echo "进程 ID: $PID"
echo ""
echo "查看日志：tail -f $LOG_FILE"
echo "查看进度：cat data/correction_progress.json"
echo "停止进程：kill $PID"
echo ""

# 等待 5 秒显示初始输出
sleep 5
echo "=== 最新日志 (最近 20 行) ==="
tail -20 "$LOG_FILE"
