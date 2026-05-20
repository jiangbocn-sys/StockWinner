#!/bin/bash
# StockWinner 后端进程 Watchdog
# 监控后端进程，崩溃时自动重启，防 crash loop

PROJECT_DIR="/home/bobo/StockWinner"
PID_FILE="$PROJECT_DIR/logs/backend.pid"
LOG_FILE="$PROJECT_DIR/logs/watchdog.log"
CRASH_LOG="$PROJECT_DIR/logs/crash_history.log"
START_BACKEND="$PROJECT_DIR/start_backend.sh"

# Crash loop 防护：1 分钟内最多重启次数
MAX_RESTARTS=3
TIME_WINDOW=60  # 秒

# 确保日志目录存在
mkdir -p "$PROJECT_DIR/logs"

log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') [watchdog] $1" | tee -a "$LOG_FILE"
}

record_crash() {
    local pid=$1
    local now=$(date +%s)
    echo "$now $pid" >> "$CRASH_LOG"

    # 清理 1 分钟前的旧记录
    local cutoff=$((now - TIME_WINDOW))
    if [ -f "$CRASH_LOG" ]; then
        local tmp=$(mktemp)
        awk -v cutoff="$cutoff" '$1 >= cutoff' "$CRASH_LOG" > "$tmp"
        mv "$tmp" "$CRASH_LOG"
    fi

    # 统计最近重启次数
    local count=$(awk -v cutoff="$cutoff" '$1 >= cutoff' "$CRASH_LOG" 2>/dev/null | wc -l)
    echo "$count"
}

restart_backend() {
    log "正在重启后端服务..."

    # 清理残留 PID 文件
    rm -f "$PID_FILE"

    # 启动后端
    bash "$START_BACKEND" start >> "$LOG_FILE" 2>&1
    sleep 2

    # 验证启动
    if [ -f "$PID_FILE" ]; then
        local new_pid=$(cat "$PID_FILE")
        if kill -0 "$new_pid" 2>/dev/null; then
            log "后端重启成功，新 PID=$new_pid"
            return 0
        fi
    fi
    log "后端重启失败"
    return 1
}

# ============================================================
# 主循环
# ============================================================

log "Watchdog 启动"

# 等待初始 PID 文件出现
if [ ! -f "$PID_FILE" ]; then
    log "等待后端服务启动（PID 文件不存在）..."
    bash "$START_BACKEND" start >> "$LOG_FILE" 2>&1
    sleep 3
fi

while true; do
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")

        if ! kill -0 "$PID" 2>/dev/null; then
            # 进程已死亡
            log "检测到进程崩溃 (PID=$PID)"

            restart_count=$(record_crash "$PID")
            log "最近 ${TIME_WINDOW} 秒内重启次数: $restart_count / $MAX_RESTARTS"

            if [ "$restart_count" -gt "$MAX_RESTARTS" ]; then
                log "Crash loop 检测到！${TIME_WINDOW} 秒内重启超过 ${MAX_RESTARTS} 次，Watchdog 停止"
                log "请检查日志排查问题: $LOG_FILE"
                exit 1
            fi

            restart_backend
        fi
    else
        log "PID 文件不存在，尝试重启..."
        restart_backend
    fi

    sleep 5
done
