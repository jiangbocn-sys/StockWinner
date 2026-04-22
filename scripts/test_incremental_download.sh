#!/bin/bash
# 测试增量下载 K 线数据（带进度显示）

cd /home/bobo/StockWinner

source venv/bin/activate

# 日志文件
LOG_FILE="logs/test_incremental_$(date +%Y%m%d_%H%M%S).log"

mkdir -p logs

echo "========================================"
echo "测试增量下载 K 线数据（带进度显示）"
echo "========================================"
echo "启动时间：$(date '+%Y-%m-%d %H:%M:%S')"
echo "日志文件：$LOG_FILE"
echo ""

# 参数
CALC_FACTORS="${1:-True}"
if [ "$1" == "no-calc" ]; then
    echo "将不计算因子，仅下载 K 线数据"
    echo ""
fi

# 直接运行（前台）
python3 -c "
import sys
sys.path.insert(0, '/home/bobo/StockWinner')
from services.data.local_data_service import download_incremental_kline_data_sync
import os

print('开始增量下载...')
result = download_incremental_kline_data_sync(
    batch_size=50,
    months=6,
    broker_account=os.environ.get('BROKER_ACCOUNT', ''),
    broker_password=os.environ.get('BROKER_PASSWORD', ''),
    calculate_factors=$CALC_FACTORS
)
print(f'下载完成，结果：{\"成功\" if result else \"失败\"}')
" 2>&1 | tee "$LOG_FILE"

echo ""
echo "完成时间：$(date '+%Y-%m-%d %H:%M:%S')"
echo "日志文件：$LOG_FILE"
