"""
LLM 配置管理 API
"""

from fastapi import APIRouter
from services.common.database import get_db_manager
import json
from pathlib import Path

router = APIRouter()

CONFIG_PATH = Path(__file__).parent.parent.parent / "config" / "llm.json"


@router.get("/api/v1/ui/llm/config")
async def get_llm_config():
    """获取 LLM 配置"""
    try:
        if CONFIG_PATH.exists():
            with open(CONFIG_PATH, 'r') as f:
                config = json.load(f)
                # 不返回完整的 API 密钥，只返回是否已配置
                has_key = bool(config.get("api_key"))
                return {
                    "success": True,
                    "data": {
                        "configured": has_key,
                        "api_key_set": has_key
                    }
                }
        else:
            return {
                "success": True,
                "data": {
                    "configured": False,
                    "api_key_set": False
                }
            }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }


@router.post("/api/v1/ui/llm/config")
async def save_llm_config(api_key: str):
    """保存 LLM API 配置"""
    try:
        # 确保配置目录存在
        CONFIG_PATH.parent.mkdir(exist_ok=True)

        # 读取现有配置
        if CONFIG_PATH.exists():
            with open(CONFIG_PATH, 'r') as f:
                config = json.load(f)
        else:
            config = {}

        # 更新 API 密钥
        config["api_key"] = api_key.strip()

        # 保存配置
        with open(CONFIG_PATH, 'w') as f:
            json.dump(config, f, indent=2)

        # 重置策略生成器以使用新密钥
        from services.llm.strategy_generator import reset_strategy_generator
        reset_strategy_generator()

        # 测试 API 密钥是否有效
        test_result = await test_api_key(api_key.strip())

        return {
            "success": True,
            "message": "配置已保存" + (" - API 测试成功" if test_result else " - API 测试失败，请检查密钥"),
            "api_valid": test_result
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }


async def test_api_key(api_key: str) -> bool:
    """测试 API 密钥是否有效"""
    if not api_key:
        return False

    try:
        import urllib.request
        import urllib.error
        import json

        # 读取配置获取提供商信息
        provider = "anthropic"
        base_url = "https://api.anthropic.com/v1/messages"
        model = "claude-sonnet-4-20250514"

        if CONFIG_PATH.exists():
            with open(CONFIG_PATH, 'r') as f:
                config = json.load(f)
                provider = config.get("provider", "anthropic")
                if config.get("base_url"):
                    base_url = config["base_url"]
                if config.get("model"):
                    model = config["model"]

        # 获取提供商配置
        from services.llm.strategy_generator import LLM_PROVIDERS
        preset = LLM_PROVIDERS.get(provider, {})
        api_format = preset.get("format", "openai")

        headers = {"Content-Type": "application/json"}

        if api_format == "anthropic":
            headers["x-api-key"] = api_key
            headers["anthropic-version"] = "2023-06-01"
            data = {
                "model": model,
                "max_tokens": 10,
                "messages": [{"role": "user", "content": "Hi"}]
            }
        else:  # OpenAI 格式
            auth_header = preset.get("auth_header", "Authorization")
            auth_prefix = preset.get("auth_prefix", "Bearer ")
            headers[auth_header] = f"{auth_prefix}{api_key}"
            data = {
                "model": model,
                "messages": [{"role": "user", "content": "Hi"}],
                "max_tokens": 10
            }

        req = urllib.request.Request(
            base_url if config.get("base_url") else url,
            data=json.dumps(data).encode("utf-8"),
            headers=headers,
            method="POST"
        )

        with urllib.request.urlopen(req, timeout=10) as response:
            return response.status == 200

    except Exception as e:
        print(f"API 测试失败：{e}")
        return False
