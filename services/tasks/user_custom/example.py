# ---
# name: 自定义任务示例
# description: 复制此文件并修改为你自己的任务逻辑
# category: 自定义
# ---
"""
用户自定义任务模板

将此文件复制并修改为你的任务：
1. 修改头部元数据（name、description、category）
2. 修改 execute 函数中的业务逻辑
3. 重启后端或在前端点击"扫描插件"即可看到
"""


async def execute(**kwargs):
    """执行自定义任务

    Args:
        **kwargs: 可从调度系统传入的参数

    Returns:
        dict: 任务执行结果，包含 success 和 message 字段
    """
    return {
        "success": True,
        "message": "自定义任务执行成功（示例）",
    }
