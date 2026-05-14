"""
LLM 配置管理 API - 基于当前登录用户
"""

from fastapi import APIRouter, HTTPException, Path, Body
from typing import Optional
from services.common.database import get_db_manager
from services.common.timezone import get_china_time

router = APIRouter()

# 预置提供商模板
LLM_PROVIDERS = {
    "anthropic": {"default_base_url": "https://api.anthropic.com/v1/messages", "default_model": "claude-sonnet-4-20250514"},
    "openai": {"default_base_url": "https://api.openai.com/v1/chat/completions", "default_model": "gpt-4o"},
    "deepseek": {"default_base_url": "https://api.deepseek.com/v1/chat/completions", "default_model": "deepseek-chat"},
    "aliyun": {"default_base_url": "https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions", "default_model": "qwen-plus"},
    "moonshot": {"default_base_url": "https://api.moonshot.cn/v1/chat/completions", "default_model": "moonshot-v1-8k"},
    "zhipu": {"default_base_url": "https://open.bigmodel.cn/api/paas/v4/chat/completions", "default_model": "glm-4"},
    "custom": {"default_base_url": "", "default_model": ""},
}


@router.get("/api/v1/ui/{account_id}/llm/config")
async def get_llm_config(
    account_id: str = Path(..., description="账户 ID"),
):
    """获取当前账户的 LLM 配置"""
    db = get_db_manager()

    # 验证账户存在且激活
    account = await db.fetchone(
        "SELECT 1 FROM accounts WHERE account_id = ? AND is_active = 1",
        (account_id,)
    )
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    row = await db.fetchone(
        "SELECT * FROM llm_config WHERE account_id = ?",
        (account_id,)
    )

    if row:
        return {
            "success": True,
            "data": {
                "configured": True,
                "provider": row["provider"],
                "base_url": row["base_url"],
                "model_name": row["model_name"],
                "api_key_masked": row["api_key"][:8] + "..." + row["api_key"][-4:] if len(row["api_key"]) > 12 else "***",
            }
        }
    else:
        return {
            "success": True,
            "data": {
                "configured": False,
                "provider": "custom",
                "base_url": "",
                "model_name": "",
                "api_key_masked": "",
            }
        }


@router.post("/api/v1/ui/{account_id}/llm/config")
async def save_llm_config(
    account_id: str = Path(..., description="账户 ID"),
    provider: str = Body(...),
    base_url: str = Body(...),
    api_key: str = Body(...),
    model_name: str = Body(...),
):
    """保存当前账户的 LLM 配置"""
    db = get_db_manager()

    # 验证账户存在且激活
    account = await db.fetchone(
        "SELECT 1 FROM accounts WHERE account_id = ? AND is_active = 1",
        (account_id,)
    )
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    existing = await db.fetchone(
        "SELECT id, api_key FROM llm_config WHERE account_id = ?",
        (account_id,)
    )

    # 如果未提供新 API key，复用数据库中的旧值
    key_to_use = api_key.strip()
    if not key_to_use and existing:
        key_to_use = existing["api_key"]

    data = {
        "account_id": account_id,
        "provider": provider,
        "base_url": base_url.strip(),
        "api_key": key_to_use,
        "model_name": model_name.strip(),
        "enabled": 1,
        "updated_at": get_china_time().isoformat(),
    }

    if existing:
        await db.update("llm_config", data, "account_id = ?", (account_id,))
    else:
        data["created_at"] = get_china_time().isoformat()
        await db.insert("llm_config", data)

    # 重置策略生成器缓存
    from services.llm.strategy_generator import reset_strategy_generator
    reset_strategy_generator()

    # 测试连接
    test_result, error_detail = await _test_api_key(provider, base_url.strip(), key_to_use, model_name.strip())

    if test_result:
        message = "配置已保存 - API 测试成功"
    else:
        message = f"配置已保存 - API 测试失败：{error_detail}"

    return {
        "success": True,
        "message": message,
        "api_valid": test_result,
    }


@router.delete("/api/v1/ui/{account_id}/llm/config")
async def delete_llm_config(
    account_id: str = Path(..., description="账户 ID"),
):
    """删除当前账户的 LLM 配置"""
    db = get_db_manager()

    account = await db.fetchone(
        "SELECT 1 FROM accounts WHERE account_id = ? AND is_active = 1",
        (account_id,)
    )
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    await db.delete("llm_config", "account_id = ?", (account_id,))

    from services.llm.strategy_generator import reset_strategy_generator
    reset_strategy_generator()

    return {"success": True, "message": "配置已删除"}


async def _test_api_key(provider: str, base_url: str, api_key: str, model: str) -> tuple:
    """测试 API 密钥是否有效

    Returns:
        (bool, str): (是否成功, 错误信息)
    """
    if not api_key or not base_url:
        return False, "API Key 或 Base URL 为空"

    # 补全 URL（非 Anthropic 且未包含 /chat/completions 时）
    is_anthropic = provider == "anthropic"
    if not is_anthropic:
        if not base_url.endswith("/chat/completions"):
            if base_url.endswith("/v1"):
                base_url = base_url + "/chat/completions"
            elif base_url.endswith("/v1/"):
                base_url = base_url + "chat/completions"

    try:
        import urllib.request
        import urllib.error
        import json

        headers = {"Content-Type": "application/json"}

        if is_anthropic:
            headers["x-api-key"] = api_key
            headers["anthropic-version"] = "2023-06-01"
            data = {
                "model": model,
                "max_tokens": 10,
                "messages": [{"role": "user", "content": "Hi"}]
            }
        else:
            headers["Authorization"] = f"Bearer {api_key}"
            data = {
                "model": model,
                "messages": [{"role": "user", "content": "Hi"}],
                "max_tokens": 10
            }

        req = urllib.request.Request(
            base_url,
            data=json.dumps(data).encode("utf-8"),
            headers=headers,
            method="POST"
        )

        with urllib.request.urlopen(req, timeout=10) as response:
            return response.status in (200, 201), ""

    except urllib.error.HTTPError as e:
        error_body = e.read().decode("utf-8") if e.fp else ""
        error_msg = f"HTTP {e.code}: {error_body[:200]}"
        print(f"LLM API 测试 HTTP 错误 ({e.code}): {e.url} - {error_body[:200]}")
        return False, error_msg
    except Exception as e:
        error_msg = str(e)
        print(f"LLM API 测试失败：{e}")
        return False, error_msg


def get_user_llm_config(account_id: str) -> Optional[dict]:
    """
    同步获取用户的 LLM 配置（供 strategy_generator 使用）
    """
    from services.common.database import get_sync_connection
    try:
        conn = get_sync_connection()
        row = conn.execute(
            "SELECT * FROM llm_config WHERE account_id = ? AND enabled = 1",
            (account_id,)
        ).fetchone()
        if row:
            return dict(row)
    except Exception:
        pass
    return None
