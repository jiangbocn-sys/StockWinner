"""
FastAPI 主应用入口
"""

# 在任何其他导入之前加载环境变量
from dotenv import load_dotenv
load_dotenv()

from services.boot import create_app

app = create_app()
