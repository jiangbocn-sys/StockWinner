# StockWinner 部署指南

## 系统要求

- **操作系统**: Ubuntu 22.04/24.04 LTS (x86_64)
- **Python**: 3.12+
- **Node.js**: 18+ (用于前端构建)
- **磁盘空间**: 至少 1GB
- **内存**: 至少 512MB

## 部署步骤

### 1. 环境准备

```bash
# 更新系统
sudo apt update

# 安装基础依赖
sudo apt install -y python3 python3-pip nodejs npm git curl wget

# 安装 Python SDK（系统环境）
pip3 install --user --break-system-packages AmazingData tgw scipy numba uvicorn fastapi pandas

# 验证 SDK 安装
python3 -c "import AmazingData; import tgw; print('SDK 安装成功')"

# 安装 NVM（Node 版本管理）
export NVM_NODEJS_ORG_MIRROR=https://mirrors.cloud.tencent.com/nvm
curl -o- https://mirrors.cloud.tencent.com/nvm/install.sh | bash
source ~/.bashrc

# 安装 Node.js 22+
nvm install 22
nvm use 22
nvm alias default 22

# 安装 Tailscale（远程访问）
curl -fsSL https://tailscale.com/install.sh | sh
sudo tailscale up
# 会输出登录链接，用手机/电脑打开授权

# 获取 Tailscale IP
tailscale ip
```

### 2. 克隆代码

```bash
cd /home/bobo
git clone <repo> StockWinner
cd StockWinner
```

### 3. 配置账户

```bash
# 创建配置目录
mkdir -p config

# 复制配置模板
cp config/accounts.json.example config/accounts.json

# 编辑配置文件
nano config/accounts.json
```

### 4. 初始化数据库

```bash
# 执行数据库初始化脚本
python3 scripts/init_db.py
```

### 5. 构建前端（可选，仅首次部署）

```bash
cd frontend

# 安装依赖
npm install

# 构建生产版本
npm run build

cd ..
```

### 6. 启动服务

```bash
# 开发模式（前台运行）
python3 -m uvicorn services.main:app --host 0.0.0.0 --port 8080 --reload

# 生产模式（后台运行）
nohup python3 -m uvicorn services.main:app --host 0.0.0.0 --port 8080 --reload > logs/app.log 2>&1 &

# 查看日志
tail -f logs/app.log

# 查看进程
ps aux | grep uvicorn

# 停止服务
pkill -f "uvicorn services.main"
```

### 7. 测试验证

```bash
# 1. 测试健康检查
curl http://localhost:8080/api/v1/health

# 2. 访问前端 UI
# 浏览器打开：http://localhost:8080
```

## 系统服务配置（systemd）

创建服务文件：

```bash
sudo nano /etc/systemd/system/stockwinner.service
```

**服务配置**:
```ini
[Unit]
Description=StockWinner Stock Trading System
After=network.target

[Service]
Type=simple
User=bobo
WorkingDirectory=/home/bobo/StockWinner
ExecStart=/usr/bin/python3 -m uvicorn services.main:app --host 0.0.0.0 --port 8080 --reload
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

**管理服务**:
```bash
# 重载 systemd 配置
sudo systemctl daemon-reload

# 启动服务
sudo systemctl start stockwinner

# 设置开机自启
sudo systemctl enable stockwinner

# 查看状态
sudo systemctl status stockwinner

# 查看日志
sudo journalctl -u stockwinner -f
```

## 重要开发规范

⚠️ **TGW SDK 连接数限制**：每个用户只能有一个活跃连接

所有 SDK 调用必须通过 `sdk_manager.get_base_data()` 获取缓存实例，禁止直接创建 `BaseData()` / `InfoData()` / `MarketData()` 等新实例。

## 常用命令

```bash
# 查看服务状态
ps aux | grep uvicorn

# 查看日志
tail -f logs/app.log

# 重启服务
pkill -f uvicorn && nohup python3 -m uvicorn services.main:app --host 0.0.0.0 --port 8080 --reload > logs/app.log 2>&1 &

# 数据库备份
cp data/kline.db data/kline.db.bak

# 数据库恢复
cp data/kline.db.bak data/kline.db
```

## 故障排查

### 问题：无法连接服务器

**解决**:
```bash
# 检查服务是否运行
ps aux | grep uvicorn

# 查看端口占用
netstat -tlnp | grep 8080

# 重启服务
pkill -f uvicorn
python3 -m uvicorn services.main:app --host 0.0.0.0 --port 8080 --reload
```

### 问题：SDK 连接数超限

**错误日志**:
```
Connections of this user exceed the max limitation
```

**解决**: 确保 SDK 调用使用缓存实例，检查代码是否直接创建 `BaseData()` 等实例。

### 问题：服务启动失败

**检查**:
```bash
# 查看完整日志
tail -100 logs/app.log

# 检查 Python 版本
python3 --version

# 检查 SDK 是否可用
python3 -c "import AmazingData; import tgw"
```

## 安全建议

1. **防火墙配置**: 仅允许必要 IP 访问 8080 端口
2. **定期备份**: 建议每天备份数据库
3. **监控日志**: 定期检查异常访问