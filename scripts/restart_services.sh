#!/bin/bash

###############################################################################
# StockWinner 服务启动脚本
# 功能：检查并启动前端和后端服务
###############################################################################

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 配置
BACKEND_PORT=8080
FRONTEND_PORT=3000
BACKEND_LOG="/tmp/uvicorn.log"
FRONTEND_LOG="/tmp/stockwinner_frontend.log"
PROJECT_DIR="/home/bobo/StockWinner"
VENV_DIR="$PROJECT_DIR/venv"

# 计数器
MAX_RETRIES=3
RETRY_DELAY=3

###############################################################################
# 工具函数
###############################################################################

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

###############################################################################
# 检查服务状态
###############################################################################

check_backend_process() {
    # 检查 uvicorn 进程
    local pid=$(ps aux | grep "uvicorn services.main:app" | grep -v grep | awk '{print $2}' | head -1)
    if [ -n "$pid" ]; then
        echo "$pid"
        return 0
    fi
    return 1
}

check_frontend_process() {
    # 检查前端进程（npm/vite）
    local pid=$(ps aux | grep "npm run dev\|vite\|next dev" | grep -v grep | grep "frontend" | awk '{print $2}' | head -1)
    if [ -n "$pid" ]; then
        echo "$pid"
        return 0
    fi
    return 1
}

check_backend_health() {
    # 检查后端健康接口
    local response=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:$BACKEND_PORT/api/v1/health 2>/dev/null)
    if [ "$response" = "200" ]; then
        return 0
    fi
    return 1
}

check_frontend_health() {
    # 检查前端是否正常响应
    local response=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:$FRONTEND_PORT 2>/dev/null)
    if [ "$response" = "200" ]; then
        return 0
    fi
    return 1
}

###############################################################################
# 停止服务
###############################################################################

stop_backend() {
    log_info "正在停止后端服务..."
    local pid=$(check_backend_process)
    if [ -n "$pid" ]; then
        kill $pid 2>/dev/null
        sleep 2
        # 如果进程还在，强制终止
        if ps -p $pid > /dev/null 2>&1; then
            kill -9 $pid 2>/dev/null
            log_warning "强制终止后端进程"
        else
            log_success "后端服务已停止 (PID: $pid)"
        fi
    else
        log_info "未找到运行中的后端进程"
    fi
}

stop_frontend() {
    log_info "正在停止前端服务..."
    local pid=$(check_frontend_process)
    if [ -n "$pid" ]; then
        kill $pid 2>/dev/null
        sleep 2
        # 如果进程还在，强制终止
        if ps -p $pid > /dev/null 2>&1; then
            kill -9 $pid 2>/dev/null
            log_warning "强制终止前端进程"
        else
            log_success "前端服务已停止 (PID: $pid)"
        fi
    else
        log_info "未找到运行中的前端进程"
    fi
}

###############################################################################
# 启动服务
###############################################################################

start_backend() {
    log_info "正在启动后端服务..."

    cd "$PROJECT_DIR"
    source "$VENV_DIR/bin/activate"

    # 清理旧日志
    > "$BACKEND_LOG"

    # 启动 uvicorn
    nohup python3 -m uvicorn services.main:app \
        --host 0.0.0.0 \
        --port $BACKEND_PORT \
        > "$BACKEND_LOG" 2>&1 &

    local backend_pid=$!
    log_info "后端进程已启动 (PID: $backend_pid)"

    # 等待服务启动并检查健康状态
    log_info "等待后端服务启动..."
    for i in $(seq 1 $MAX_RETRIES); do
        sleep $RETRY_DELAY

        if check_backend_health; then
            log_success "后端服务启动成功 (端口：$BACKEND_PORT)"
            return 0
        else
            log_warning "后端服务启动中... ($i/$MAX_RETRIES)"
        fi
    done

    # 启动失败
    log_error "后端服务启动超时，检查日志：$BACKEND_LOG"
    tail -20 "$BACKEND_LOG"
    return 1
}

start_frontend() {
    log_info "正在启动前端服务..."

    cd "$PROJECT_DIR/frontend"

    # 清理旧日志
    > "$FRONTEND_LOG"

    # 启动前端（根据项目类型调整命令）
    if [ -f "package.json" ]; then
        nohup npm run dev > "$FRONTEND_LOG" 2>&1 &
        local frontend_pid=$!
        log_info "前端进程已启动 (PID: $frontend_pid)"

        # 等待服务启动并检查健康状态
        log_info "等待前端服务启动..."
        for i in $(seq 1 $MAX_RETRIES); do
            sleep $RETRY_DELAY

            if check_frontend_health; then
                log_success "前端服务启动成功 (端口：$FRONTEND_PORT)"
                return 0
            else
                log_warning "前端服务启动中... ($i/$MAX_RETRIES)"
            fi
        done

        # 启动失败
        log_error "前端服务启动超时，检查日志：$FRONTEND_LOG"
        tail -20 "$FRONTEND_LOG"
        return 1
    else
        log_error "前端目录未找到 package.json"
        return 1
    fi
}

###############################################################################
# 主流程
###############################################################################

main() {
    echo "=============================================="
    echo "  StockWinner 服务启动脚本"
    echo "=============================================="
    echo ""

    local backend_need_start=false
    local frontend_need_start=false
    local backend_need_restart=false
    local frontend_need_restart=false

    # 1. 检查后端服务状态
    log_info "检查后端服务状态..."
    backend_pid=$(check_backend_process)
    if [ -n "$backend_pid" ]; then
        log_info "后端进程运行中 (PID: $backend_pid)"

        if check_backend_health; then
            log_success "后端服务健康检查通过"
        else
            log_warning "后端进程运行但健康检查失败"
            backend_need_restart=true
        fi
    else
        log_warning "后端服务未运行"
        backend_need_start=true
    fi

    # 2. 检查前端服务状态
    log_info "检查前端服务状态..."
    frontend_pid=$(check_frontend_process)
    if [ -n "$frontend_pid" ]; then
        log_info "前端进程运行中 (PID: $frontend_pid)"

        if check_frontend_health; then
            log_success "前端服务健康检查通过"
        else
            log_warning "前端进程运行但健康检查失败"
            frontend_need_restart=true
        fi
    else
        log_warning "前端服务未运行"
        frontend_need_start=true
    fi

    echo ""

    # 3. 两个服务都正常，退出
    if [ "$backend_need_start" = false ] && [ "$frontend_need_start" = false ] && \
       [ "$backend_need_restart" = false ] && [ "$frontend_need_restart" = false ]; then
        log_success "所有服务运行正常，无需操作"
        echo ""
        echo "=============================================="
        echo "  服务状态"
        echo "=============================================="
        echo "  后端：http://localhost:$BACKEND_PORT (PID: $backend_pid)"
        echo "  前端：http://localhost:$FRONTEND_PORT (PID: $frontend_pid)"
        echo "=============================================="
        exit 0
    fi

    # 4. 需要重启的服务先停止
    if [ "$backend_need_restart" = true ]; then
        log_warning "后端服务异常，准备重启..."
        stop_backend
        backend_need_start=true
    fi

    if [ "$frontend_need_restart" = true ]; then
        log_warning "前端服务异常，准备重启..."
        stop_frontend
        frontend_need_start=true
    fi

    echo ""

    # 5. 启动需要的服务
    if [ "$backend_need_start" = true ]; then
        start_backend
        backend_result=$?
        echo ""
    fi

    if [ "$frontend_need_start" = true ]; then
        start_frontend
        frontend_result=$?
        echo ""
    fi

    # 6. 输出最终状态
    echo "=============================================="
    echo "  启动完成 - 服务状态"
    echo "=============================================="

    if [ "$backend_need_start" = true ] && [ $backend_result -eq 0 ]; then
        backend_pid=$(check_backend_process)
        echo -e "  后端：${GREEN}运行中${NC} (PID: $backend_pid, 端口：$BACKEND_PORT)"
    elif [ "$backend_need_start" = false ]; then
        echo -e "  后端：${GREEN}运行中${NC} (PID: $backend_pid, 端口：$BACKEND_PORT)"
    else
        echo -e "  后端：${RED}启动失败${NC}"
    fi

    if [ "$frontend_need_start" = true ] && [ $frontend_result -eq 0 ]; then
        frontend_pid=$(check_frontend_process)
        echo -e "  前端：${GREEN}运行中${NC} (PID: $frontend_pid, 端口：$FRONTEND_PORT)"
    elif [ "$frontend_need_start" = false ]; then
        echo -e "  前端：${GREEN}运行中${NC} (PID: $frontend_pid, 端口：$FRONTEND_PORT)"
    else
        echo -e "  前端：${RED}启动失败${NC}"
    fi

    echo "=============================================="
    echo ""
    echo "日志文件位置:"
    echo "  后端：$BACKEND_LOG"
    echo "  前端：$FRONTEND_LOG"
    echo ""

    # 返回状态码
    if [ "$backend_need_start" = true ] && [ $backend_result -ne 0 ]; then
        exit 1
    fi
    if [ "$frontend_need_start" = true ] && [ $frontend_result -ne 0 ]; then
        exit 1
    fi

    exit 0
}

# 执行主流程
main "$@"
