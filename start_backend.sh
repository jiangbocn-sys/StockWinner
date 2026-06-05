#!/bin/bash
# StockWinner 后端服务启动脚本
# 支持 systemd 服务和手动启动两种模式

PROJECT_DIR="/home/bobo/StockWinner"
PYTHON_CMD="python3"
LOG_FILE="$PROJECT_DIR/logs/backend.log"
PID_FILE="$PROJECT_DIR/logs/backend.pid"
CHECK_SCRIPT="$PROJECT_DIR/scripts/check_running_tasks.sh"
SYSTEMD_SERVICE="stockwinner-backend"

# 确保在项目目录
cd "$PROJECT_DIR"

# 创建日志目录
mkdir -p "$PROJECT_DIR/logs"

# 任务检查函数
check_running_tasks() {
    FORCE_MODE="$1"
    if [ -f "$CHECK_SCRIPT" ]; then
        chmod +x "$CHECK_SCRIPT" 2>/dev/null
        if [ "$FORCE_MODE" = "--force" ]; then
            echo "跳过任务检查 (--force)"
        else
            "$CHECK_SCRIPT"
            CHECK_RESULT=$?
            if [ $CHECK_RESULT -ne 0 ]; then
                echo "任务检查失败，已取消重启"
                exit $CHECK_RESULT
            fi
        fi
    fi
}

# 检查 systemd 服务是否活跃
check_systemd_active() {
    systemctl is-active "$SYSTEMD_SERVICE" 2>/dev/null
}

case "$1" in
    start)
        # 如果 systemd 服务活跃，提示使用 systemctl
        SD_STATUS=$(check_systemd_active)
        if [ "$SD_STATUS" = "active" ]; then
            echo "systemd 服务 $SYSTEMD_SERVICE 已在运行，请使用:"
            echo "  sudo systemctl status $SYSTEMD_SERVICE"
            exit 0
        fi

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
        # 如果 systemd 服务活跃，提示使用 systemctl
        SD_STATUS=$(check_systemd_active)
        if [ "$SD_STATUS" = "active" ]; then
            echo "systemd 服务 $SYSTEMD_SERVICE 正在运行，请使用:"
            echo "  sudo systemctl stop $SYSTEMD_SERVICE"
            echo "或检查任务后停止:"
            echo "  curl http://localhost:8080/api/v1/ui/system/running-tasks"
            echo "  sudo systemctl stop $SYSTEMD_SERVICE"
            exit 0
        fi

        # 检查是否有正在运行的任务
        check_running_tasks "$2"

        echo "停止 StockWinner 后端服务..."
        # 同时停止 Watchdog
        pkill -f "process_watchdog.sh" 2>/dev/null
        if [ -f "$PID_FILE" ]; then
            PID=$(cat "$PID_FILE")
            if kill -0 "$PID" 2>/dev/null; then
                # 发送 SIGTERM 触发 FastAPI lifespan shutdown 钩子（调用 SDK logout）
                kill -15 "$PID"
                echo "已发送 SIGTERM (PID: $PID)，等待优雅退出..."
                # 等待最多 30 秒让 shutdown 钩子完成（SDK logout + 数据库关闭）
                for i in $(seq 1 30); do
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
                echo "服务未运行 (PID: $PID 已失效)"
                rm -f "$PID_FILE"
            fi
        else
            echo "PID 文件不存在，尝试查找 uvicorn 进程..."
            UVICORN_PID=$(pgrep -f "uvicorn services.main:app" | head -1)
            if [ -n "$UVICORN_PID" ]; then
                echo "发现 uvicorn 进程 (PID: $UVICORN_PID)，发送 SIGTERM..."
                kill -15 "$UVICORN_PID"
                for i in $(seq 1 30); do
                    sleep 1
                    if ! kill -0 "$UVICORN_PID" 2>/dev/null; then
                        echo "服务已优雅退出"
                        break
                    fi
                done
                if kill -0 "$UVICORN_PID" 2>/dev/null; then
                    echo "超时，强制 kill (PID: $UVICORN_PID)"
                    kill -9 "$UVICORN_PID"
                    sleep 1
                fi
                echo "服务已停止"
            else
                echo "未发现运行中的服务"
            fi
        fi
        # 强制清理 SDK 子进程 + socket（SIGKILL → TCP RST → TGW 立即释放）
        pkill -9 -f "sdk_subprocess_server" 2>/dev/null || true
        rm -f /tmp/stockwinner_sdk.sock 2>/dev/null || true
        # 确认 SDK TCP 连接已断开
        sleep 1
        if ss -tn | grep -q ':8600'; then
            echo "WARNING: SDK 连接 (port 8600) 仍未断开，等待超时..."
            sleep 3
            if ss -tn | grep -q ':8600'; then
                ss -tn | grep ':8600'
            else
                echo "SDK 连接已断开"
            fi
        else
            echo "SDK 连接已确认断开"
        fi
        ;;
    restart)
        # 如果 systemd 服务活跃，提示使用 systemctl
        SD_STATUS=$(check_systemd_active)
        if [ "$SD_STATUS" = "active" ]; then
            echo "systemd 服务 $SYSTEMD_SERVICE 正在运行，请使用:"
            echo "  # 1. 先检查任务状态"
            echo "  curl http://localhost:8080/api/v1/ui/system/running-tasks"
            echo "  # 2. 系统级重启"
            echo "  sudo systemctl restart $SYSTEMD_SERVICE"
            echo "或强制重启:"
            echo "  sudo systemctl restart $SYSTEMD_SERVICE --force"
            exit 0
        fi

        # 检查是否有正在运行的任务
        check_running_tasks "$2"

        echo "重启 StockWinner 后端服务..."
        echo "步骤 1/3: 停止服务（含 SDK logout + 实例清理）"
        # 内联 stop 逻辑，避免子 shell
        pkill -f "process_watchdog.sh" 2>/dev/null
        if [ -f "$PID_FILE" ]; then
            PID=$(cat "$PID_FILE")
            if kill -0 "$PID" 2>/dev/null; then
                kill -15 "$PID"
                echo "  已发送 SIGTERM (PID: $PID)，等待 shutdown 钩子执行..."
                for i in $(seq 1 30); do
                    sleep 1
                    if ! kill -0 "$PID" 2>/dev/null; then
                        echo "  服务已优雅退出"
                        break
                    fi
                done
                if kill -0 "$PID" 2>/dev/null; then
                    echo "  超时，强制 kill (PID: $PID)"
                    kill -9 "$PID"
                    sleep 1
                fi
                rm -f "$PID_FILE"
            else
                rm -f "$PID_FILE"
            fi
        else
            UVICORN_PID=$(pgrep -f "uvicorn services.main:app" | head -1)
            if [ -n "$UVICORN_PID" ]; then
                kill -15 "$UVICORN_PID"
                for i in $(seq 1 30); do
                    sleep 1
                    if ! kill -0 "$UVICORN_PID" 2>/dev/null; then
                        break
                    fi
                done
                if kill -0 "$UVICORN_PID" 2>/dev/null; then
                    kill -9 "$UVICORN_PID"
                    sleep 1
                fi
            fi
        fi
        # 强制清理 SDK 子进程 + socket（SIGKILL → TCP RST → TGW 立即释放）
        pkill -9 -f "sdk_subprocess_server" 2>/dev/null || true
        rm -f /tmp/stockwinner_sdk.sock 2>/dev/null || true
        # 确保 SDK TCP 连接断开
        sleep 1
        if ss -tn | grep -q ':8600'; then
            echo "  等待 SDK TCP 连接超时..."
            sleep 3
        fi
        echo "步骤 2/3: SDK 连接已清理"
        echo "步骤 3/3: 启动新服务..."
        $0 start
        ;;
    status)
        # 优先检查 systemd 服务状态
        SD_STATUS=$(check_systemd_active)
        if [ "$SD_STATUS" = "active" ]; then
            echo "systemd 服务 $SYSTEMD_SERVICE 正在运行"
            systemctl status "$SYSTEMD_SERVICE" --no-pager | head -10
        elif [ -f "$PID_FILE" ]; then
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
    check-tasks)
        # 直接调用任务检查脚本
        if [ -f "$CHECK_SCRIPT" ]; then
            chmod +x "$CHECK_SCRIPT" 2>/dev/null
            "$CHECK_SCRIPT"
        else
            echo "任务检查脚本不存在：$CHECK_SCRIPT"
            exit 1
        fi
        ;;
    *)
        echo "用法：$0 {start|stop|restart|status|logs|check-tasks} [--dev] [--watch] [--force]"
        echo
        echo "  start          - 启动服务（仅当 systemd 服务未活跃时）"
        echo "  start --dev    - 启动服务（开发模式，文件变更自动重启）"
        echo "  start --watch  - 启动服务 + Watchdog 自动重启（崩溃时自动恢复）"
        echo "  stop           - 停止服务（仅当 systemd 服务未活跃时）"
        echo "  stop --force   - 强制停止（跳过任务检查）"
        echo "  restart        - 重启服务（仅当 systemd 服务未活跃时）"
        echo "  restart --force - 强制重启（跳过任务检查）"
        echo "  status         - 查看状态（优先显示 systemd 服务状态）"
        echo "  logs           - 查看日志 (实时)"
        echo "  check-tasks    - 检查是否有正在运行的任务"
        echo
        echo "注意：如果 systemd 服务 $SYSTEMD_SERVICE 正在运行，"
        echo "      请使用 systemctl 命令进行系统级管理:"
        echo "      sudo systemctl {start|stop|restart|status} $SYSTEMD_SERVICE"
        exit 1
        ;;
esac