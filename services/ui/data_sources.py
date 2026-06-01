"""
数据源管理 API 端点

提供数据源的启用/禁用、配置修改、优先级调整和实时健康检查。
"""

from fastapi import APIRouter, HTTPException, Body
from fastapi.responses import StreamingResponse
from typing import Dict, List, Optional, Any
import json

router = APIRouter()


@router.get("/api/v1/ui/data-sources")
async def list_data_sources() -> Dict:
    """获取所有数据源配置"""
    from services.data.channel.config_manager import get_all_provider_configs
    from services.data.channel.router import get_channel_router
    from services.common.timezone import get_china_time

    configs = await get_all_provider_configs()

    # 获取 router 中已注册的 provider
    try:
        router = get_channel_router()
        registered = router.get_providers()
    except Exception:
        registered = {}

    # 为每个 provider 附加状态
    result = []
    for cfg in configs:
        pid = cfg["provider_id"]
        provider = registered.get(pid)

        if provider:
            try:
                hc = await provider.health_check()
                status = "connected" if hc.get("ok") else "error"
                error_msg = None if hc.get("ok") else hc.get("message", "")
            except Exception as e:
                status = "error"
                error_msg = str(e)
        elif cfg.get("is_enabled"):
            status = "disconnected"
            error_msg = None
        else:
            status = "not_configured"
            error_msg = None

        result.append({
            "provider_id": pid,
            "display_name": cfg.get("display_name", pid),
            "is_enabled": bool(cfg.get("is_enabled", False)),
            "requires_config": bool(cfg.get("requires_config", False)),
            "channel_priority_json": cfg.get("channel_priority_json", {}),
            "capabilities_json": cfg.get("capabilities_json", {}),
            "system_config_json": cfg.get("system_config_json", {}),
            "status": status,
            "error_message": error_msg,
        })

    return {"success": True, "data": result}


@router.post("/api/v1/ui/data-sources/{provider_id}/toggle")
async def toggle_data_source(
    provider_id: str,
    data: Dict[str, Any] = Body(...),
) -> Dict:
    """启用/禁用数据源"""
    from services.data.channel.config_manager import toggle_provider
    from services.common.scheduler_service import get_scheduler

    is_enabled = data.get("is_enabled", False)
    await toggle_provider(provider_id, is_enabled)

    # 重新加载调度器以应用新配置
    try:
        get_scheduler()._reload_channel_router()
    except Exception:
        pass

    action = "启用" if is_enabled else "禁用"
    return {"success": True, "message": f"已{action}数据源: {provider_id}"}


@router.post("/api/v1/ui/data-sources/{provider_id}/config")
async def update_data_source_config(
    provider_id: str,
    data: Dict[str, Any] = Body(...),
) -> Dict:
    """更新数据源系统配置"""
    from services.data.channel.config_manager import update_provider_config

    # 只允许修改 system_config 中的字段
    system_config = {k: v for k, v in data.items() if k != "provider_id"}
    await update_provider_config(provider_id, system_config)

    # 如果修改了 api_token，尝试重新初始化 tushare provider
    if "api_token" in system_config and provider_id == "tushare":
        try:
            from services.data.channel.router import get_channel_router
            router = get_channel_router()
            providers = router.get_providers()
            provider = providers.get("tushare")
            if provider:
                await provider.initialize(system_config)
        except Exception:
            pass

    return {"success": True, "message": f"已更新数据源配置: {provider_id}"}


@router.post("/api/v1/ui/data-sources/priority")
async def update_data_source_priority(
    data: Dict[str, Any] = Body(...),
) -> Dict:
    """更新通道优先级

    Body: {
        "channel_type": "trading" | "market_data" | "download",
        "provider_order": ["eastmoney", "amazingdata", "tushare"]
    }
    """
    from services.data.channel.config_manager import update_channel_priority
    from services.common.scheduler_service import get_scheduler

    channel_type = data.get("channel_type", "")
    provider_order = data.get("provider_order", [])

    if not channel_type or not provider_order:
        return {"success": False, "message": "缺少 channel_type 或 provider_order"}

    await update_channel_priority(channel_type, provider_order)

    # 重新加载 ChannelRouter
    try:
        get_scheduler()._reload_channel_router()
    except Exception:
        pass

    return {"success": True, "message": f"已更新 {channel_type} 通道优先级"}


@router.get("/api/v1/ui/data-sources/health")
async def check_data_sources_health() -> Dict:
    """获取所有 provider 的实时健康状态"""
    from services.data.channel.router import get_channel_router
    from services.common.timezone import get_china_time

    try:
        router = get_channel_router()
        providers = router.get_providers()
    except Exception:
        return {"success": False, "message": "ChannelRouter 未初始化"}

    results = {}
    for pid, provider in providers.items():
        try:
            hc = await provider.health_check()
            results[pid] = {
                "ok": hc.get("ok", False),
                "message": hc.get("message", ""),
                "latency_ms": hc.get("latency_ms", -1),
            }
        except Exception as e:
            results[pid] = {
                "ok": False,
                "message": str(e),
                "latency_ms": -1,
            }

    return {"success": True, "data": results}


@router.get("/api/v1/ui/data-sources/health/stream")
async def check_data_sources_health_stream():
    """SSE 流式健康检查 — 逐个检测并实时返回结果"""
    from services.data.channel.router import get_channel_router
    from services.common.timezone import get_china_time

    async def event_stream():
        try:
            router = get_channel_router()
            providers = router.get_providers()
        except Exception as e:
            yield f"data: {json.dumps({'provider_id': '__error__', 'ok': False, 'message': str(e), 'latency_ms': -1})}\n\n"
            return

        total = len(providers)
        # 先发送总数，前端用来初始化进度
        yield f"data: {json.dumps({'provider_id': '__meta__', 'total': total})}\n\n"

        for idx, (pid, provider) in enumerate(providers.items(), 1):
            # 发送开始检测信号
            yield f"data: {json.dumps({'provider_id': pid, 'status': 'checking', 'progress': idx, 'total': total})}\n\n"
            try:
                hc = await provider.health_check()
                yield f"data: {json.dumps({
                    'provider_id': pid,
                    'status': 'done',
                    'ok': hc.get('ok', False),
                    'message': hc.get('message', ''),
                    'latency_ms': hc.get('latency_ms', -1),
                    'progress': idx,
                    'total': total
                })}\n\n"
            except Exception as e:
                yield f"data: {json.dumps({
                    'provider_id': pid,
                    'status': 'done',
                    'ok': False,
                    'message': str(e),
                    'latency_ms': -1,
                    'progress': idx,
                    'total': total
                })}\n\n"

        # 发送完成信号
        yield f"data: {json.dumps({'provider_id': '__done__', 'total': total})}\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream", headers={
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",
    })


@router.get("/api/v1/ui/data-sources/usage-stats")
async def get_data_source_usage_stats(
    provider_id: Optional[str] = None,
    days: int = 7,
) -> Dict:
    """获取数据源使用统计

    Args:
        provider_id: 指定数据源（可选，不传则返回全部）
        days: 统计天数（默认7天）
    """
    from services.data.channel.router import get_channel_router

    try:
        router = get_channel_router()
        stats = await router.get_usage_stats(provider_id, days)
        # 内存统计（当前运行期）
        memory_stats = router.get_call_stats()
    except Exception as e:
        return {"success": False, "message": str(e)}

    return {
        "success": True,
        "db_stats": stats,
        "memory_stats": memory_stats,
        "query_params": {"provider_id": provider_id, "days": days},
    }
