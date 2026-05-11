"""
异步辅助工具

统一处理事件循环冲突问题：当 FastAPI 的事件循环正在运行时，
同步代码不能直接调用 asyncio.get_event_loop().run_until_complete()。

所有模块统一使用本模块的工具函数，禁止在各处手写事件循环检测逻辑。
"""

import asyncio
import concurrent.futures
import functools
from typing import Any, Callable, Optional, TypeVar

T = TypeVar('T')


def has_running_loop() -> bool:
    """检测当前线程是否有正在运行的事件循环"""
    try:
        asyncio.get_running_loop()
        return True
    except RuntimeError:
        return False


def run_sync_in_thread(sync_fn: Callable[..., T],
                       *args, **kwargs) -> T:
    """
    在可能有事件循环运行的上下文中，安全执行同步阻塞调用。

    原理：
    - 如果当前无事件循环 → 直接调用
    - 如果有事件循环 → 在新线程+新事件循环中执行

    使用场景：
    - 调度器方法中调用 asyncio.run_until_complete()
    - 后台任务中执行同步数据库操作
    - FastAPI 端点中调用阻塞 SDK

    示例:
        result = run_sync_in_thread(download_weekly_kline_sync)
    """
    if not has_running_loop():
        # 无事件循环，直接调用（如 APScheduler 独立线程、启动脚本等）
        return sync_fn(*args, **kwargs)

    # 有事件循环，在新线程中创建新事件循环执行
    result_container: list = [None]
    error_container: list = [None]

    def _run_in_new_loop():
        loop = asyncio.new_event_loop()
        try:
            result_container[0] = loop.run_until_complete(
                _to_async(sync_fn, *args, **kwargs)
            )
        except Exception as e:
            error_container[0] = e
        finally:
            loop.close()

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(_run_in_new_loop)
        future.result()  # 等待完成

    if error_container[0]:
        raise error_container[0]
    return result_container[0]


def run_async_safe(coro_fn: Callable, *args, **kwargs) -> Any:
    """
    在非异步上下文中安全调用异步函数。

    如果当前有运行中的事件循环，在新线程中执行。
    否则直接用 asyncio.run() 执行。

    示例:
        result = run_async_safe(async_db_query, "SELECT * FROM accounts")
    """
    if not has_running_loop():
        return asyncio.run(coro_fn(*args, **kwargs))

    result_container: list = [None]
    error_container: list = [None]

    def _run():
        loop = asyncio.new_event_loop()
        try:
            result_container[0] = loop.run_until_complete(coro_fn(*args, **kwargs))
        except Exception as e:
            error_container[0] = e
        finally:
            loop.close()

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(_run)
        future.result()

    if error_container[0]:
        raise error_container[0]
    return result_container[0]


async def ensure_async(sync_fn: Callable[..., T],
                       *args, **kwargs) -> T:
    """
    将同步函数包装为异步执行（自动线程池）。

    用于在 async def 中调用阻塞同步代码。

    示例:
        result = await ensure_async(sdk.query_kline, codes)
    """
    return await asyncio.to_thread(sync_fn, *args, **kwargs)


def _to_async(fn: Callable, *args, **kwargs):
    """将同步函数包装为协程（内部使用）"""
    return asyncio.to_thread(fn, *args, **kwargs)


def run_in_background(fn: Callable[..., T],
                      callback: Optional[Callable] = None,
                      *args, **kwargs) -> concurrent.futures.Future:
    """
    在后台线程执行函数，不阻塞当前线程。

    Args:
        fn: 同步函数
        callback: 完成后的回调函数 (fn_result) → None
        *args, **kwargs: 传递给 fn 的参数

    Returns:
        Future 对象，可用于等待结果

    示例:
        future = run_in_background(
            long_running_download,
            callback=lambda result: print(f"完成: {result}")
        )
    """
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    future = executor.submit(fn, *args, **kwargs)
    if callback:
        future.add_done_callback(lambda f: callback(f.result()))
    return future
