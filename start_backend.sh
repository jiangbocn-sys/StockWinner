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
        # 后台启动
        nohup "$PYTHON_CMD" -m uvicorn services.main:app \
            --host 0.0.0.0 \
            --port 8080 \
            --reload \
            > "$LOG_FILE" 2>&1 &
        echo $! > "$PID_FILE"
        sleep 2
        if kill -0 $(cat "$PID_FILE") 2>/dev/null; then
            echo "服务启动成功 (PID: $(cat $PID_FILE))"
            echo "日志文件：$LOG_FILE"
        else
            echo "服务启动失败，请查看日志：$LOG_FILE"
            exit 1
        fi
        ;;
    stop)
        echo "停止 StockWinner 后端服务..."
        if [ -f "$PID_FILE" ]; then
            PID=$(cat "$PID_FILE")
            if kill -0 "$PID" 2>/dev/null; then
                kill "$PID"
                sleep 2
                if ! kill -0 "$PID" 2>/dev/null; then
                    echo "服务已停止"
                    rm -f "$PID_FILE"
                else
                    echo "强制停止服务..."
                    kill -9 "$PID"
                    rm -f "$PID_FILE"
                fi
            else
                echo "服务未运行"
                rm -f "$PID_FILE"
            fi
        else
            echo "PID 文件不存在，服务可能未运行"
            # 尝试查找并停止 uvicorn 进程
            pkill -f "uvicorn services.main:app" && echo "已停止相关进程"
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
        echo "用法：$0 {start|stop|restart|status|logs}"
        echo
        echo "  start   - 启动服务"
        echo "  stop    - 停止服务"
        echo "  restart - 重启服务"
        echo "  status  - 查看状态"
        echo "  logs    - 查看日志 (实时)"
        exit 1
        ;;
esac