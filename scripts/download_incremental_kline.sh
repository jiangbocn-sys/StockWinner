#!/bin/bash
# 增量下载 K 线数据（带交易时间检查）
# 使用方法：
#   ./download_incremental_kline.sh          # 后台运行，自动计算因子
#   ./download_incremental_kline.sh no-calc  # 后台运行，不计算因子

cd /home/bobo/StockWinner

# 日志文件
LOG_FILE="logs/incremental_kline_$(date +%Y%m%d_%H%M%S).log"

# 创建日志目录
mkdir -p logs

echo "========================================"
echo "增量下载 K 线数据"
echo "========================================"
echo "启动时间：$(date '+%Y-%m-%d %H:%M:%S')"
echo "日志文件：$LOG_FILE"
echo ""

# 检查是否有 Broker 环境变量
if [ -z "$BROKER_ACCOUNT" ] || [ -z "$BROKER_PASSWORD" ]; then
    echo "警告：BROKER_ACCOUNT 或 BROKER_PASSWORD 未设置"
    echo "请在环境变量中配置银河证券资金账号和密码"
    echo ""
    echo "使用方法："
    echo "  export BROKER_ACCOUNT=your_account"
    echo "  export BROKER_PASSWORD=your_password"
    echo "  ./download_incremental_kline.sh"
    echo ""
fi

# 参数
CALC_FACTORS="True"
if [ "$1" == "no-calc" ]; then
    CALC_FACTORS="False"
    echo "将不计算因子，仅下载 K 线数据"
    echo ""
fi

# 启动后台进程
echo "启动后台进程..."
nohup python3 -c "
import sys
sys.path.insert(0, '/home/bobo/StockWinner')
from services.data.local_data_service import download_incremental_kline_data_sync
import os

result = download_incremental_kline_data_sync(
    batch_size=50,
    months=6,
    broker_account=os.environ.get('BROKER_ACCOUNT', ''),
    broker_password=os.environ.get('BROKER_PASSWORD', ''),
    calculate_factors=$CALC_FACTORS
)
print(f'下载完成，结果：{\"成功\" if result else \"失败\"}')" > "$LOG_FILE" 2>&1 &

PID=$!
echo "进程 ID: $PID"
echo ""
echo "查看日志：tail -f $LOG_FILE"
echo "查看进度：ps aux | grep $PID"
echo "停止进程：kill $PID"
echo ""

# 等待 5 秒显示初始输出
sleep 5
echo "=== 最新日志 (最近 20 行) ==="
tail -20 "$LOG_FILE"
