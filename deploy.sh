#!/bin/bash
# StockWinner 一键部署脚本
# 用途：在新服务器上从零搭建完整运行环境
# 需要 root 或 sudo 权限

set -e  # 遇到错误立即退出

PROJECT_DIR="/home/bobo/StockWinner"
LOG_FILE="$PROJECT_DIR/logs/deploy.log"

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log()  { echo -e "${GREEN}[OK]${NC} $1"; echo "[$(date '+%Y-%m-%d %H:%M:%S')] [OK] $1" >> "$LOG_FILE" 2>/dev/null || true; }
warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
err()  { echo -e "${RED}[FAIL]${NC} $1"; }
step() { echo ""; echo "=== $1 ==="; }

# 确保在正确目录
cd "$(dirname "$0")"

mkdir -p logs
echo "" >> "$LOG_FILE"
echo "=== 部署开始 $(date '+%Y-%m-%d %H:%M:%S') ===" >> "$LOG_FILE"

# ============================================================
# Step 0: 检查 root/sudo
# ============================================================
step "0. 检查权限"
if ! command -v sudo &>/dev/null && [ "$(id -u)" -ne 0 ]; then
    err "需要 sudo 权限，请使用 sudo 或以 root 运行"
    exit 1
fi
SUDO=""
if [ "$(id -u)" -ne 0 ]; then
    SUDO="sudo"
fi
log "权限检查通过"

# ============================================================
# Step 1: 系统依赖
# ============================================================
step "1. 安装系统依赖"

# 检测包管理器
if command -v apt-get &>/dev/null; then
    PKG_MGR="apt"
elif command -v yum &>/dev/null; then
    PKG_MGR="yum"
elif command -v dnf &>/dev/null; then
    PKG_MGR="dnf"
else
    warn "无法识别包管理器，跳过系统包安装"
    PKG_MGR=""
fi

if [ -n "$PKG_MGR" ]; then
    $SUDO $PKG_MGR update -y >/dev/null 2>&1 || true
    if [ "$PKG_MGR" = "apt" ]; then
        $SUDO apt-get install -y python3 python3-pip curl git build-essential 2>/dev/null || true
    else
        $SUDO $PKG_MGR install -y python3 python3-pip curl git gcc gcc-c++ 2>/dev/null || true
    fi
    log "系统包安装完成"
else
    warn "跳过系统包安装"
fi

# 检查 Python 版本
if command -v python3 &>/dev/null; then
    PY_VER=$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
    log "Python $PY_VER 已安装"
    # SDK 需要 Python 3.12+
    MAJOR=$(echo $PY_VER | cut -d. -f1)
    MINOR=$(echo $PY_VER | cut -d. -f2)
    if [ "$MAJOR" -lt 3 ] || ([ "$MAJOR" -eq 3 ] && [ "$MINOR" -lt 12 ]); then
        warn "Python 版本 $PY_VER 低于 3.12，SDK 可能不兼容"
        warn "请使用 pyenv 或从源码安装 Python 3.12+"
    fi
else
    err "Python 3 未安装，请手动安装 Python 3.12+"
    exit 1
fi

# ============================================================
# Step 2: 安装 AmazingData SDK
# ============================================================
step "2. 安装 AmazingData SDK"

if python3 -c "import AmazingData; import tgw" 2>/dev/null; then
    log "AmazingData SDK 已安装"
else
    log "正在安装 AmazingData SDK..."

    # 优先使用本地 wheel 文件
    WHEEL_DIR=$(dirname "$0")
    AMAZINGDATA_WHEEL=$(find "$WHEEL_DIR" -name "AmazingData-*.whl" 2>/dev/null | head -1)
    TGW_WHEEL=$(find "$WHEEL_DIR" -name "tgw-*.whl" 2>/dev/null | head -1)

    if [ -n "$AMAZINGDATA_WHEEL" ] && [ -n "$TGW_WHEEL" ]; then
        log "从本地 wheel 文件安装..."
        pip3 install --user --break-system-packages "$TGW_WHEEL" 2>&1 | tail -3
        pip3 install --user --break-system-packages "$AMAZINGDATA_WHEEL" 2>&1 | tail -3
    else
        warn "未找到本地 wheel 文件，尝试从 PyPI 安装..."
        warn "AmazingData SDK 通常需要从银河证券获取 wheel 文件"
        warn "请手动安装: pip3 install --user --break-system-packages AmazingData tgw"
    fi

    # 安装其他依赖
    pip3 install --user --break-system-packages scipy numba pandas 2>&1 | tail -3

    if python3 -c "import AmazingData; import tgw" 2>/dev/null; then
        log "SDK 安装成功"
    else
        warn "SDK 安装可能失败，请检查后手动安装"
    fi
fi

# 安装 Python 项目依赖
log "安装 Python 项目依赖..."
pip3 install --user --break-system-packages \
    fastapi uvicorn pydantic pydantic-settings python-multipart \
    requests psutil aiosqlite python-dotenv aiofiles 2>&1 | tail -3
log "Python 依赖安装完成"

# ============================================================
# Step 3: 安装 Node.js（如果未安装）
# ============================================================
step "3. 检查 Node.js"

if command -v node &>/dev/null; then
    NODE_VER=$(node -v)
    log "Node.js $NODE_VER 已安装"
else
    log "正在安装 Node.js..."
    # 使用 nvm 安装
    export NVM_DIR="$HOME/.nvm"
    if [ ! -s "$NVM_DIR/nvm.sh" ]; then
        curl -o- https://mirrors.cloud.tencent.com/nvm/install.sh | bash 2>/dev/null || \
        curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.1/install.sh | bash 2>/dev/null
    fi
    [ -s "$NVM_DIR/nvm.sh" ] && . "$NVM_DIR/nvm.sh"
    nvm install 22
    nvm alias default 22
    log "Node.js $(node -v) 已安装"
fi

if command -v npm &>/dev/null; then
    log "npm $(npm -v) 可用"
fi

# ============================================================
# Step 4: 配置文件
# ============================================================
step "4. 检查配置文件"

# .env
if [ -f "$PROJECT_DIR/.env" ]; then
    log ".env 已存在"
else
    warn ".env 不存在，创建模板..."
    cat > "$PROJECT_DIR/.env" << 'ENVEOF'
# AmazingData SDK 凭证（从银河证券获取）
SDK_USERNAME=your_username
SDK_PASSWORD=your_password
SDK_HOST=140.206.44.234
SDK_PORT=8600

# 后端配置
BACKEND_HOST=0.0.0.0
BACKEND_PORT=8080
ENVEOF
    warn "请编辑 .env 文件，填入真实的 SDK 账号密码！"
    warn "nano $PROJECT_DIR/.env"
fi

# llm.json
if [ -f "$PROJECT_DIR/config/llm.json" ]; then
    log "config/llm.json 已存在"
else
    if [ -f "$PROJECT_DIR/config/llm.json.example" ]; then
        cp "$PROJECT_DIR/config/llm.json.example" "$PROJECT_DIR/config/llm.json"
        log "已从 example 创建 config/llm.json（可选配置）"
    else
        cat > "$PROJECT_DIR/config/llm.json" << 'LLMEOF'
{
    "provider": "aliyun",
    "api_key": "",
    "base_url": "",
    "model": ""
}
LLMEOF
        log "已创建 config/llm.json 模板（LLM 功能可选）"
    fi
fi

# ============================================================
# Step 5: 初始化数据库
# ============================================================
step "5. 初始化数据库"

if [ -f "$PROJECT_DIR/data/stockwinner.db" ]; then
    log "数据库已存在，跳过初始化"
else
    log "创建数据目录..."
    mkdir -p "$PROJECT_DIR/data"
    log "运行数据库初始化脚本..."
    python3 "$PROJECT_DIR/scripts/init_db.py"
    log "数据库初始化完成"
fi

mkdir -p "$PROJECT_DIR/logs"

# ============================================================
# Step 6: 构建前端
# ============================================================
step "6. 构建前端"

if [ -f "$PROJECT_DIR/frontend/dist/index.html" ]; then
    log "前端已构建，跳过"
    read -p "是否重新构建前端？(y/N) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        log "跳过前端构建"
        SKIP_FRONTEND=true
    fi
else
    SKIP_FRONTEND=false
fi

if [ "$SKIP_FRONTEND" != "true" ]; then
    cd "$PROJECT_DIR/frontend"
    log "安装 npm 依赖（首次运行较慢）..."
    npm install --no-audit --no-fund 2>&1 | tail -5
    log "构建前端..."
    npm run build 2>&1 | tail -5
    log "前端构建完成"
    cd "$PROJECT_DIR"
fi

# ============================================================
# Step 7: 设置权限
# ============================================================
step "7. 设置权限"

chmod +x "$PROJECT_DIR/start_backend.sh"
if [ -f "$PROJECT_DIR/services/process_watchdog.sh" ]; then
    chmod +x "$PROJECT_DIR/services/process_watchdog.sh"
fi
log "权限设置完成"

# ============================================================
# Step 8: 可选 - Kronos 模型
# ============================================================
step "8. Kronos 模型（可选）"

KRONOS_DIR="$PROJECT_DIR/deps/Kronos/weights"
if [ -d "$KRONOS_DIR/Kronos-small" ]; then
    log "Kronos 模型已存在"
else
    warn "Kronos 模型未安装（可选，用于 AI 时间序列预测）"
    warn "如需安装，从 HuggingFace 下载或从备份恢复："
    warn "  pip3 install --user torch einops huggingface_hub safetensors"
    warn "  然后使用 tools/download_kronos_weights.py 下载"
fi

# ============================================================
# Step 9: 启动服务
# ============================================================
step "9. 启动服务"

cd "$PROJECT_DIR"
./start_backend.sh start

# 等待服务启动
sleep 3

# ============================================================
# Step 10: 验证
# ============================================================
step "10. 验证部署"

if curl -sf http://localhost:8080/api/v1/health >/dev/null 2>&1; then
    HEALTH=$(curl -sf http://localhost:8080/api/v1/health)
    log "健康检查通过: $HEALTH"
else
    err "健康检查失败，请查看日志: tail -f $PROJECT_DIR/logs/backend.log"
    warn "常见问题："
    warn "  1. .env 中 SDK 账号密码未正确配置"
    warn "  2. Python 版本不是 3.12+"
    warn "  3. AmazingData SDK 未正确安装"
fi

if curl -sf http://localhost:8080/ui/ >/dev/null 2>&1; then
    log "前端页面可访问: http://localhost:8080/ui/"
else
    warn "前端页面不可访问，检查 frontend/dist/ 是否存在"
fi

# ============================================================
# 完成
# ============================================================
echo ""
echo "========================================"
echo -e "${GREEN}部署完成！${NC}"
echo "========================================"
echo ""
echo "访问地址: http://<服务器IP>:8080/ui/"
echo ""
echo "后续操作："
echo "  1. 编辑 .env 填入真实的 SDK 账号密码"
echo "  2. ./start_backend.sh restart 重启服务"
echo "  3. 通过 UI 创建账户并下载 K 线历史数据"
echo ""
echo "管理命令："
echo "  ./start_backend.sh start   # 启动"
echo "  ./start_backend.sh stop    # 停止"
echo "  ./start_backend.sh restart # 重启"
echo "  ./start_backend.sh status  # 状态"
echo "  ./start_backend.sh logs    # 查看日志"
echo ""
echo "日志文件: $PROJECT_DIR/logs/backend.log"
echo ""
