"""
LLM 策略生成服务
支持任意兼容 OpenAI 格式的 LLM API（Claude、GPT、DeepSeek、通义千问等）
"""

import json
import os
from typing import Dict, Any, Optional
from datetime import datetime
from pathlib import Path

# 配置文件路径
CONFIG_PATH = Path(__file__).parent.parent.parent / "config" / "llm.json"


# 预置的 LLM 提供商配置
LLM_PROVIDERS = {
    "anthropic": {
        "name": "Anthropic Claude",
        "base_url": "https://api.anthropic.com/v1/messages",
        "model": "claude-sonnet-4-20250514",
        "auth_header": "x-api-key",
        "api_version_header": "anthropic-version",
        "api_version": "2023-06-01",
        "format": "anthropic"
    },
    "openai": {
        "name": "OpenAI GPT",
        "base_url": "https://api.openai.com/v1/chat/completions",
        "model": "gpt-4o",
        "auth_header": "Authorization",
        "auth_prefix": "Bearer ",
        "format": "openai"
    },
    "deepseek": {
        "name": "DeepSeek",
        "base_url": "https://api.deepseek.com/v1/chat/completions",
        "model": "deepseek-chat",
        "auth_header": "Authorization",
        "auth_prefix": "Bearer ",
        "format": "openai"
    },
    "aliyun": {
        "name": "阿里云通义千问",
        "base_url": "https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions",
        "model": "qwen-plus",
        "auth_header": "Authorization",
        "auth_prefix": "Bearer ",
        "format": "openai"
    },
    "moonshot": {
        "name": "月之暗面 Kimi",
        "base_url": "https://api.moonshot.cn/v1/chat/completions",
        "model": "moonshot-v1-8k",
        "auth_header": "Authorization",
        "auth_prefix": "Bearer ",
        "format": "openai"
    },
    "zhipu": {
        "name": "智谱 AI",
        "base_url": "https://open.bigmodel.cn/api/paas/v4/chat/completions",
        "model": "glm-4",
        "auth_header": "Authorization",
        "auth_prefix": "Bearer ",
        "format": "openai"
    },
    "bailian": {
        "name": "阿里云百炼",
        "base_url": "https://coding.dashscope.aliyuncs.com/v1/chat/completions",
        "model": "glm-5",
        "auth_header": "Authorization",
        "auth_prefix": "Bearer ",
        "format": "openai"
    },
    "custom": {
        "name": "自定义",
        "base_url": "",
        "model": "",
        "auth_header": "Authorization",
        "auth_prefix": "Bearer ",
        "format": "openai"
    }
}


class StrategyGenerator:
    """策略生成器 - 生成选股策略"""

    # 系统提示词
    SYSTEM_PROMPT = """你是一个专业的股票策略分析助手。你的任务是将用户的自然语言描述转换为【选股策略】配置。

选股策略用于筛选有投资潜力的股票，包含以下字段：

1. stock_filters: 静态筛选条件（基本面属性）
   - total_market_cap_max: 总市值上限（亿元）
   - total_market_cap_min: 总市值下限（亿元）
   - circ_market_cap_max: 流通市值上限（亿元）
   - pe_ttm_max: 市盈率TTM上限（倍）
   - pb_max: 市净率上限（倍）
   - roe_min: ROE下限（百分比，如15表示15%）
   - gross_margin_min: 毛利率下限（百分比）
   - revenue_growth_yoy_min: 营收同比增长下限（百分比）
   - sw_level1: 申万一级行业（如"电子"、"计算机"）

2. buy_conditions: 买点信号条件（技术指标）
   - 使用穿越信号或比较表达式

请严格按照以下 JSON 格式返回，不要包含任何额外说明：

{
    "stock_filters": {
        "total_market_cap_max": 50,
        "roe_min": 15
    },
    "buy_conditions": [
        "DIF_CROSS_UP_DEA",
        "VOLUME_RATIO > 1.5"
    ]
}

【重要】条件解析规则（必须严格遵守）：

**【第一条规则 - 市值/估值/盈利条件】**：基本面条件必须放入 stock_filters！

一、市值条件：
- "总市值小于X亿" → total_market_cap_max: X
- "流通市值小于X亿" → circ_market_cap_max: X

二、估值条件：
- "PE小于X" 或 "市盈率小于X倍" → pe_ttm_max: X
- "PB小于X" 或 "市净率小于X倍" → pb_max: X

三、盈利条件：
- "ROE大于X%" → roe_min: X
- "毛利率大于X%" → gross_margin_min: X

四、成长条件：
- "营收增长X%" → revenue_growth_yoy_min: X

五、行业条件：
- "电子行业" → sw_level1: "电子"

二、MACD金叉/死叉 → 使用专用条件名：
- "MACD金叉" → "DIF_CROSS_UP_DEA"
- "MACD死叉" → "DIF_CROSS_DOWN_DEA"

三、成交量条件：
- "成交量放大" → "VOLUME_RATIO > 2"
- "量比大于X" → "VOLUME_RATIO > X"

四、均线条件：
- "MA5上穿MA10" → "MA5_CROSS_UP_MA10"
- "价格站上5日均线" → "PRICE > MA5"

五、RSI条件：
- "RSI超卖" → "RSI_14 < 30"
- "RSI超买" → "RSI_14 > 70"

【核心原则】：
1. 基本面条件（市值、PE、ROE等）必须放入 stock_filters
2. 技术信号条件（金叉、量比等）放入 buy_conditions
3. 不要遗漏用户提到的任何条件"""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        初始化策略生成器

        Args:
            config: LLM 配置字典，包含 provider, api_key, base_url, model 等字段
        """
        self.config = config or self._load_config()
        self.provider = self.config.get("provider", "custom")
        self.api_key = self.config.get("api_key", "")
        self.base_url = self.config.get("base_url", "")
        self.model = self.config.get("model", "")

        # 如果指定了预置提供商，使用其默认配置
        if self.provider in LLM_PROVIDERS:
            preset = LLM_PROVIDERS[self.provider]
            if not self.base_url:
                self.base_url = preset["base_url"]
            if not self.model:
                self.model = preset["model"]

        # 确保 base_url 包含完整的路径 (对于 openai 格式的 API)
        if self.base_url and not self.base_url.endswith("/chat/completions"):
            if self.base_url.endswith("/v1"):
                self.base_url = self.base_url + "/chat/completions"
            elif self.base_url.endswith("/v1/"):
                self.base_url = self.base_url + "chat/completions"

    def _load_config(self) -> Dict[str, Any]:
        """从配置文件加载配置"""
        if CONFIG_PATH.exists():
            try:
                with open(CONFIG_PATH, 'r') as f:
                    return json.load(f)
            except:
                pass
        return {}

    def generate(self, description: str) -> Dict[str, Any]:
        """
        根据描述生成选股策略配置

        Args:
            description: 策略描述

        Returns:
            策略配置字典，失败时抛出异常
        """
        if not self.api_key or not self.base_url:
            raise Exception("LLM API 未配置，请在 Settings 页面配置 API Key 或编辑 config/llm.json 文件")

        # 调用 LLM API
        strategy_config = self._call_llm(description)
        return {
            "success": True,
            "config": strategy_config,
            "source": "llm",
            "provider": self.provider
        }

    def _call_llm(self, description: str) -> Dict[str, Any]:
        """
        调用 LLM API 生成选股策略

        Args:
            description: 策略描述

        Returns:
            策略配置字典
        """
        import urllib.request
        import urllib.error

        # 构建用户消息
        user_prompt = f"""请根据以下策略描述生成选股策略配置：

策略描述：{description}

请返回纯 JSON，不要包含 markdown 格式或其他说明文字。"""

        # 获取提供商配置
        preset = LLM_PROVIDERS.get(self.provider, {})
        api_format = preset.get("format", "openai")

        # 构建请求
        headers = {
            "Content-Type": "application/json"
        }

        # 设置认证头
        auth_header = preset.get("auth_header", "Authorization")
        auth_prefix = preset.get("auth_prefix", "")
        if auth_header == "x-api-key":
            headers["x-api-key"] = self.api_key
        else:
            headers[auth_header] = f"{auth_prefix}{self.api_key}"

        # Anthropic 需要额外的版本头
        if preset.get("api_version_header"):
            headers[preset["api_version_header"]] = preset.get("api_version", "")

        # 构建请求体
        if api_format == "anthropic":
            data = {
                "model": self.model,
                "max_tokens": 1024,
                "system": self.SYSTEM_PROMPT,
                "messages": [
                    {"role": "user", "content": user_prompt}
                ]
            }
        else:  # openai 格式
            data = {
                "model": self.model,
                "messages": [
                    {"role": "system", "content": self.SYSTEM_PROMPT},
                    {"role": "user", "content": user_prompt}
                ],
                "temperature": 0.7,
                "max_tokens": 1024
            }

        req = urllib.request.Request(
            self.base_url,
            data=json.dumps(data).encode("utf-8"),
            headers=headers,
            method="POST"
        )

        try:
            with urllib.request.urlopen(req, timeout=600) as response:
                result = json.loads(response.read().decode("utf-8"))

                # 解析响应
                if api_format == "anthropic":
                    content = result["content"][0]["text"]
                else:  # openai 格式
                    content = result["choices"][0]["message"]["content"]

                # 解析返回的 JSON
                content = self._parse_content(content)
                strategy_config = json.loads(content)

                # 验证必要字段
                self._validate_strategy_config(strategy_config)

                return strategy_config

        except urllib.error.HTTPError as e:
            error_body = e.read().decode("utf-8")
            raise Exception(f"LLM API 错误 ({e.code}): {error_body}")
        except json.JSONDecodeError as e:
            raise Exception(f"解析 LLM 响应失败：{e}")

    def _parse_content(self, content: str) -> str:
        """解析响应内容，移除 markdown 格式"""
        content = content.strip()

        # 移除 markdown 代码块标记
        if content.startswith("```json"):
            content = content[7:]
        elif content.startswith("```"):
            content = content[3:]
        if content.endswith("```"):
            content = content[:-3]

        return content.strip()

    def _validate_strategy_config(self, config: Dict[str, Any]) -> None:
        """验证选股策略配置的有效性"""
        # 设置默认值 - 使用新的 buy_conditions 格式
        if "buy_conditions" not in config:
            # 向后兼容：从旧格式迁移
            if "conditions" in config and isinstance(config["conditions"], dict):
                config["buy_conditions"] = config["conditions"].get("buy", [])
            else:
                config["buy_conditions"] = []
        if "stock_filters" not in config:
            config["stock_filters"] = {}

        # 确保 buy_conditions 是列表
        if not isinstance(config.get("buy_conditions"), list):
            config["buy_conditions"] = []

        # 确保 stock_filters 是字典
        if not isinstance(config["stock_filters"], dict):
            config["stock_filters"] = {}

# 全局单例
_strategy_generator: Optional[StrategyGenerator] = None


def get_strategy_generator(config: Optional[Dict[str, Any]] = None) -> StrategyGenerator:
    """获取策略生成器单例"""
    global _strategy_generator
    if _strategy_generator is None or config is not None:
        _strategy_generator = StrategyGenerator(config)
    return _strategy_generator


def reset_strategy_generator():
    """重置策略生成器（用于测试）"""
    global _strategy_generator
    _strategy_generator = None


def get_available_providers() -> Dict[str, Dict[str, str]]:
    """获取所有可用的 LLM 提供商列表"""
    return {
        key: {
            "name": value["name"],
            "default_model": value["model"],
            "default_base_url": value["base_url"]
        }
        for key, value in LLM_PROVIDERS.items()
    }
