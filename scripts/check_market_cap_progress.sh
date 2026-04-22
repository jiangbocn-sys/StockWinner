#!/bin/bash
# 查看市值校正进度

LOG_DIR="/home/bobo/StockWinner/logs"
PROGRESS_FILE="/home/bobo/StockWinner/data/market_cap_progress.json"

echo "========================================"
echo "市值数据校正进度"
echo "========================================"
echo ""

# 检查进程
PID=$(pgrep -f correct_market_cap.py)
if [ -n "$PID" ]; then
    echo "✓ 进程运行中 (PID: $PID)"
else
    echo "✗ 进程未运行"
fi
echo ""

# 检查进度文件
if [ -f "$PROGRESS_FILE" ]; then
    echo "进度信息:"
    python3 << EOF
import json
from datetime import datetime

with open("$PROGRESS_FILE", 'r') as f:
    progress = json.load(f)

processed = len(progress.get('processed_months', []))
total = 61
current = progress.get('current_month_index', 0)
start = progress.get('start_time')
last = progress.get('last_updated')

print(f"  已处理：{processed}/{total} ({processed/total*100:.1f}%)")
print(f"  当前索引：{current}")

if start:
    start_dt = datetime.fromisoformat(start)
    elapsed = (datetime.now() - start_dt).total_seconds() / 60
    print(f"  已运行：{elapsed:.1f} 分钟")

if last:
    last_dt = datetime.fromisoformat(last)
    ago = (datetime.now() - last_dt).total_seconds()
    print(f"  最后更新：{ago:.0f} 秒前")

# 估算剩余时间
if processed > 0 and start:
    start_dt = datetime.fromisoformat(start)
    elapsed = (datetime.now() - start_dt).total_seconds()
    avg_per_month = elapsed / processed
    remaining = total - processed
    eta_minutes = remaining * avg_per_month / 60
    print(f"  预计剩余：{eta_minutes:.1f} 分钟")
    print(f"  平均速度：{avg_per_month:.1f} 秒/月份")
EOF
else
    echo "进度文件不存在"
fi

echo ""
echo "最新日志 (最近 30 行):"
echo "----------------------------------------"

LATEST_LOG=$(ls -t "$LOG_DIR"/market_cap_correction_*.log 2>/dev/null | head -1)

if [ -n "$LATEST_LOG" ]; then
    tail -30 "$LATEST_LOG"
else
    echo "暂无日志文件"
fi
