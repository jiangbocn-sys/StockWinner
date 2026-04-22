# StockWinner 服务启动脚本

## 功能说明

`restart_services.sh` 脚本用于自动检查并启动 StockWinner 的前后端服务。

### 主要功能

1. **自动检测** - 检查前后端服务是否在运行
2. **健康检查** - 验证服务是否正常响应
3. **智能启动** - 仅启动未运行或异常的服务
4. **自动重启** - 对运行但健康检查失败的服务进行重启

## 使用方法

### 基本用法

```bash
# 直接执行脚本
/home/bobo/StockWinner/scripts/restart_services.sh
```

### 输出示例

**服务正常运行时：**
```
==============================================
  StockWinner 服务启动脚本
==============================================

[INFO] 检查后端服务状态...
[INFO] 后端进程运行中 (PID: 117357)
[SUCCESS] 后端服务健康检查通过
[INFO] 检查前端服务状态...
[INFO] 前端进程运行中 (PID: 24736)
[SUCCESS] 前端服务健康检查通过

[SUCCESS] 所有服务运行正常，无需操作

==============================================
  服务状态
==============================================
  后端：http://localhost:8080 (PID: 117357)
  前端：http://localhost:3000 (PID: 24736)
==============================================
```

**服务需要启动时：**
```
==============================================
  StockWinner 服务启动脚本
==============================================

[INFO] 检查后端服务状态...
[WARNING] 后端服务未运行
[INFO] 检查前端服务状态...
[WARNING] 前端服务未运行

[INFO] 正在启动后端服务...
[INFO] 后端进程已启动 (PID: 12345)
[INFO] 等待后端服务启动...
[SUCCESS] 后端服务启动成功 (端口：8080)

[INFO] 正在启动前端服务...
[INFO] 前端进程已启动 (PID: 12346)
[INFO] 等待前端服务启动...
[SUCCESS] 前端服务启动成功 (端口：3000)

==============================================
  启动完成 - 服务状态
==============================================
  后端：运行中 (PID: 12345, 端口：8080)
  前端：运行中 (PID: 12346, 端口：3000)
==============================================
```

## 配置参数

脚本中的可配置参数：

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `BACKEND_PORT` | 8080 | 后端服务端口 |
| `FRONTEND_PORT` | 3000 | 前端服务端口 |
| `MAX_RETRIES` | 3 | 启动重试次数 |
| `RETRY_DELAY` | 3 | 重试间隔（秒） |
| `BACKEND_LOG` | /tmp/uvicorn.log | 后端日志文件 |
| `FRONTEND_LOG` | /tmp/stockwinner_frontend.log | 前端日志文件 |

## 健康检查

### 后端健康检查
- 请求 `http://localhost:8080/api/v1/health`
- 期望 HTTP 200 响应

### 前端健康检查
- 请求 `http://localhost:3000/`
- 期望 HTTP 200 响应

## 日志文件

- **后端日志**: `/tmp/uvicorn.log`
- **前端日志**: `/tmp/stockwinner_frontend.log`

查看日志：
```bash
# 查看后端日志
tail -f /tmp/uvicorn.log

# 查看前端日志
tail -f /tmp/stockwinner_frontend.log
```

## 退出状态码

| 状态码 | 说明 |
|--------|------|
| 0 | 所有服务运行正常或启动成功 |
| 1 | 有服务启动失败 |

## 定时任务示例

在 crontab 中配置定期检查（可选）：

```bash
# 每 5 分钟检查一次服务状态
*/5 * * * * /home/bobo/StockWinner/scripts/restart_services.sh >> /var/log/stockwinner_restart.log 2>&1
```

## 注意事项

1. 脚本需要执行权限：`chmod +x /home/bobo/StockWinner/scripts/restart_services.sh`
2. 前端服务假设在 `frontend/` 目录下运行 `npm run dev`
3. 后端服务使用 uvicorn 运行 `services.main:app`
4. 需要 Python 虚拟环境在 `venv/` 目录下
