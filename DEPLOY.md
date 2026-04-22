# StockWinner 后端服务部署指南

## 系统环境要求

SDK 已安装到系统环境，无需虚拟环境：

```bash
pip3 install --user --break-system-packages AmazingData tgw scipy numba uvicorn fastapi pandas
```

## 启动方式

### 方式 1：使用启动脚本（推荐）

```bash
cd /home/bobo/StockWinner

# 启动服务
python3 start_server.py

# 或使用 uvicorn 直接启动
python3 -m uvicorn services.main:app --host 0.0.0.0 --port 8080 --reload
```

### 方式 2：后台运行

```bash
cd /home/bobo/StockWinner
nohup python3 -m uvicorn services.main:app --host 0.0.0.0 --port 8080 --reload > logs/backend.log 2>&1 &
```

## 开机自启动（systemd）

### 安装服务

```bash
# 复制服务文件到 systemd 目录
sudo cp /home/bobo/StockWinner/stockwinner-backend.service /etc/systemd/system/

# 重载 systemd 配置
sudo systemctl daemon-reload

# 启用服务（开机自启）
sudo systemctl enable stockwinner-backend

# 启动服务
sudo systemctl start stockwinner-backend

# 查看状态
sudo systemctl status stockwinner-backend
```

### systemd 服务文件内容

`stockwinner-backend.service`:

```ini
[Unit]
Description=StockWinner Backend Service
After=network.target

[Service]
Type=simple
User=bobo
WorkingDirectory=/home/bobo/StockWinner
ExecStart=/usr/bin/python3 -m uvicorn services.main:app --host 0.0.0.0 --port 8080 --reload
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

## 常见问题

### SDK 连接数限制

⚠️ **重要**：TGW SDK 每个用户只能有一个活跃连接

所有 SDK 调用必须通过 `sdk_manager.get_base_data()` 获取缓存实例，禁止直接创建 `BaseData()` 等新实例。

### 检查 SDK 是否可用

```bash
python3 -c "import AmazingData; import tgw; print('SDK 可用')"
```

### 查看日志

```bash
tail -f logs/backend.log
```

### 停止服务

```bash
pkill -f "uvicorn services.main:app"
```