#!/usr/bin/env python3
"""
StockWinner 服务启动脚本
使用系统环境 Python（AmazingData SDK 已安装到系统环境）
"""

import subprocess
import sys
import os
from pathlib import Path

# 获取项目根目录
PROJECT_ROOT = Path(__file__).parent
os.chdir(PROJECT_ROOT)

# 检查系统环境是否有必要的 SDK
def check_system_sdk():
    """检查系统环境是否已安装 AmazingData SDK"""
    try:
        import AmazingData
        import tgw
        return True
    except ImportError:
        return False

# 使用系统 Python
PYTHON_PATH = sys.executable  # 当前运行的 Python（通常是 /usr/bin/python3）

if not check_system_sdk():
    print("错误：系统环境缺少 AmazingData SDK")
    print("请安装 SDK：pip3 install --user --break-system-packages AmazingData tgw scipy numba")
    sys.exit(1)

# 启动服务
print("=" * 60)
print("StockWinner v6.3.0")
print("=" * 60)
print(f"运行环境：系统环境")
print(f"Python 路径：{PYTHON_PATH}")
print()
print("访问地址:")
print("  - 前端界面：http://localhost:3000")
print("  - API 文档：http://localhost:8080/docs")
print()
print("按 Ctrl+C 停止服务")
print("-" * 60)

# 启动后端服务
subprocess.run([
    PYTHON_PATH,
    "-m",
    "uvicorn",
    "services.main:app",
    "--host", "0.0.0.0",
    "--port", "8080",
    "--reload"  # 开发模式，代码变化自动重启
])