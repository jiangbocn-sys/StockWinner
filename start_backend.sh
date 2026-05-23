#!/bin/bash
# StockWinner 后端服务启动脚本
# 使用系统 python3 运行，支持后台运行

PROJECT_DIR="/home/bobo/StockWinner"
PYTHON_CMD="python3"
LOG_FILE="$PROJECT_DIR/logs/backend.log"
PID_FILE="$PROJECT_DIR/logs/backend.pid"

# 确保在项目目录
cd "$PROJECT_DIR"

# 创建日志目录
mkdir -p "$PROJECT_DIR/logs"

case "$1" in
    start)
        echo "启动 StockWinner 后端服务..."
        # 检查是否已在运行
        if [ -f "$PID_FILE" ]; then
            OLD_PID=$(cat "$PID_FILE")
            if kill -0 "$OLD_PID" 2>/dev/null; then
                echo "服务已在运行 (PID: $OLD_PID)"
                exit 0
            fi
        fi
        # 后台启动（默认不带 --reload，开发模式用 start --dev）
        if [ "$2" = "--dev" ]; then
            echo "  [开发模式] 启用热重载，文件变更将自动重启"
            "$PYTHON_CMD" -m uvicorn services.main:app \
                --host 0.0.0.0 \
                --port 8080 \
                --reload \
                > "$LOG_FILE" 2>&1 &
        else
            "$PYTHON_CMD" -m uvicorn services.main:app \
                --host 0.0.0.0 \
                --port 8080 \
                > "$LOG_FILE" 2>&1 &
        fi
        echo $! > "$PID_FILE"
        sleep 2
        if kill -0 $(cat "$PID_FILE") 2>/dev/null; then
            echo "服务启动成功 (PID: $(cat $PID_FILE))"
            echo "日志文件：$LOG_FILE"
        else
            echo "服务启动失败，请查看日志：$LOG_FILE"
            exit 1
        fi
        # 启动 Watchdog（如果请求了 --watch）
        if [ "$3" = "--watch" ]; then
            nohup bash "$PROJECT_DIR/services/process_watchdog.sh" > /dev/null 2>&1 &
            echo "  Watchdog 已启动"
        fi
        ;;
    stop)
        echo "停止 StockWinner 后端服务..."
        # 同时停止 Watchdog
        pkill -f "process_watchdog.sh" 2>/dev/null
        if [ -f "$PID_FILE" ]; then
            PID=$(cat "$PID_FILE")
            if kill -0 "$PID" 2>/dev/null; then
                # 先发送 SIGTERM 让应用执行 shutdown 钩子（调用 SDK logout 关闭 TGW 连接）
                kill -15 "$PID"
                echo "已发送 SIGTERM (PID: $PID)，等待优雅退出..."
                # 等待最多 10 秒让 shutdown 钩子执行
                for i in $(seq 1 10); do
                    sleep 1
                    if ! kill -0 "$PID" 2>/dev/null; then
                        echo "服务已优雅退出"
                        break
                    fi
                done
                # 如果进程仍然存在，强制 kill
                if kill -0 "$PID" 2>/dev/null; then
                    echo "超时，强制 kill (PID: $PID)"
                    kill -9 "$PID"
                    sleep 1
                fi
                rm -f "$PID_FILE"
                echo "服务已停止"
            else
                echo "服务未运行"
                rm -f "$PID_FILE"
            fi
        else
            echo "PID 文件不存在，服务可能未运行"
            # 尝试查找并停止 uvicorn 进程
            pkill -f "uvicorn services.main:app" && echo "已停止相关进程"
        fi
        # 确认 SDK TCP 连接已断开
        sleep 1
        if ss -tn | grep -q ':8600'; then
            echo "WARNING: SDK 连接 (port 8600) 仍处于连接状态，可能是服务端超时未释放"
            ss -tn | grep ':8600'
        fi
        ;;
    restart)
        $0 stop
        sleep 2
        $0 start
        ;;
    status)
        if [ -f "$PID_FILE" ]; then
            PID=$(cat "$PID_FILE")
            if kill -0 "$PID" 2>/dev/null; then
                echo "服务运行中 (PID: $PID)"
                ps aux | grep "uvicorn services.main" | grep -v grep
            else
                echo "服务未运行 (stale PID file)"
            fi
        else
            echo "服务未运行"
            # 检查是否有运行中的进程
            pgrep -f "uvicorn services.main:app" && echo "发现运行中的进程"
        fi
        ;;
    logs)
        tail -f "$LOG_FILE"
        ;;
    *)
        echo "用法：$0 {start|stop|restart|status|logs} [--dev] [--watch]"
        echo
        echo "  start          - 启动服务（生产模式，不热重载）"
        echo "  start --dev    - 启动服务（开发模式，文件变更自动重启）"
        echo "  start --watch  - 启动服务 + Watchdog 自动重启（崩溃时自动恢复）"
        echo "  start --dev --watch - 开发模式 + Watchdog"
        echo "  stop           - 停止服务（同时停止 Watchdog）"
        echo "  restart        - 重启服务"
        echo "  status         - 查看状态"
        echo "  logs           - 查看日志 (实时)"
        exit 1
        ;;
esac