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
    """策略生成器"""

    # 系统提示词
    SYSTEM_PROMPT = """你是一个专业的股票策略分析助手。你的任务是将用户的自然语言描述转换为结构化的股票交易策略配置。

请根据用户的描述，分析并生成包含以下字段的 JSON 策略配置：

1. risk_level: 风险等级 ("low", "medium", "high")
2. position_pct: 单次建仓比例 (0.01-0.5，默认 0.1)
3. stop_loss_pct: 止损比例 (0.01-0.2，默认 0.05)
4. take_profit_pct: 止盈比例 (0.05-0.5，默认 0.15)
5. conditions: 交易条件
   - buy: 买入条件列表（使用技术指标表达式）
   - sell: 卖出条件列表
6. stock_filters: 股票筛选条件（可选）
   - market: 市场筛选 (可选：SH, SZ, BJ)
   - circ_market_cap_max: 流通市值上限（亿元，如 50 表示小于50亿）
   - circ_market_cap_min: 流通市值下限（亿元）
   - total_market_cap_max: 总市值上限（亿元）
   - total_market_cap_min: 总市值下限（亿元）

请严格按照以下 JSON 格式返回，不要包含任何额外说明：

{
    "risk_level": "medium",
    "position_pct": 0.1,
    "stop_loss_pct": 0.05,
    "take_profit_pct": 0.15,
    "conditions": {
        "buy": ["DIF > DEA", "RSI_14 < 35", "VOLUME_RATIO > 2"],
        "sell": ["DIF < DEA", "RSI_14 > 70"]
    },
    "stock_filters": {
        "market": "SH",
        "circ_market_cap_max": 50
    }
}

【重要】技术指标条件说明：

一、MACD 相关条件（正确理解）：
- "DIF > DEA" - MACD金叉（DIF线在DEA线上方，看涨信号）
- "DIF < DEA" - MACD死叉（DIF线在DEA线下方，看跌信号）
- "MACD > 0" - MACD柱状图为正（DIF减DEA大于0，仅表示DIF高于DEA）
- "MACD < 0" - MACD柱状图为负
注意：MACD金叉/死叉判断应使用 DIF 和 DEA 的关系，而非 MACD 柱状图值！

二、市值条件（使用精确数值）：
- "CIRC_MARKET_CAP < 50" - 流通市值小于50亿元
- "TOTAL_MARKET_CAP < 100" - 总市值小于100亿元
- "CIRC_MARKET_CAP > 20" - 流通市值大于20亿元
注意：市值单位为亿元（人民币），使用 CIRC_MARKET_CAP 或 TOTAL_MARKET_CAP

三、成交量条件：
- "VOLUME_RATIO > 2" - 量比大于2（当日成交量/5日均量 > 2）
- "VOLUME > MA(VOLUME, 20)" - 成交量高于20日均量
- "OBV > 0" - OBV能量潮为正

四、均线条件：
- "MA5 > MA10" - 5日均线上穿10日均线
- "MA5 > MA20" - 5日均线上穿20日均线（短线上穿中线，看涨）
- "MA10 > MA20" - 10日均线上穿20日均线
- "PRICE > MA5" - 当前价格高于5日均线

五、KDJ条件：
- "K > D" - KDJ金叉（K线在D线上方）
- "K < D" - KDJ死叉
- "K < 20" - KDJ超卖区域
- "K > 80" - KDJ超买区域

六、RSI条件：
- "RSI_14 < 30" - RSI超卖（可能反弹）
- "RSI_14 > 70" - RSI超买（可能回调）
- "RSI_14 > 50" - RSI中性偏强

七、布林带条件：
- "PRICE < BOLL_LOWER" - 价格跌破布林下轨（可能反弹）
- "PRICE > BOLL_UPPER" - 价格突破布林上轨（强势）

【用户描述解析规则】：
1. 市值条件：将"市值小于X亿"转换为 circ_market_cap_max 或 total_market_cap_max 字段
2. MACD金叉：转换为 "DIF > DEA" 条件（不是 MACD > 0）
3. 成交量倍数：将"成交量是X倍"转换为 "VOLUME_RATIO > X" 或类似表达式
4. 保留所有用户提到的条件，不要遗漏任何条件"""

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

    def generate(self, description: str, risk_level: str = "medium") -> Dict[str, Any]:
        """
        根据描述生成策略配置

        Args:
            description: 策略描述
            risk_level: 风险等级提示

        Returns:
            策略配置字典，失败时抛出异常
        """
        if not self.api_key or not self.base_url:
            raise Exception("LLM API 未配置，请在 Settings 页面配置 API Key 或编辑 config/llm.json 文件")

        # 调用 LLM API
        strategy_config = self._call_llm(description, risk_level)
        return {
            "success": True,
            "config": strategy_config,
            "source": "llm",
            "provider": self.provider
        }

    def _call_llm(self, description: str, risk_level: str) -> Dict[str, Any]:
        """
        调用 LLM API 生成策略

        Args:
            description: 策略描述
            risk_level: 风险等级

        Returns:
            策略配置字典
        """
        import urllib.request
        import urllib.error

        # 构建用户消息
        user_prompt = f"""请根据以下策略描述生成配置：

风险等级偏好：{risk_level}
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
        """验证策略配置的有效性"""
        # 设置默认值
        if "position_pct" not in config:
            config["position_pct"] = 0.1
        if "stop_loss_pct" not in config:
            config["stop_loss_pct"] = 0.05
        if "take_profit_pct" not in config:
            config["take_profit_pct"] = 0.15
        if "conditions" not in config:
            config["conditions"] = {"buy": [], "sell": []}
        if "stock_filters" not in config:
            config["stock_filters"] = {}

        # 验证数值范围
        config["position_pct"] = max(0.01, min(0.5, float(config["position_pct"])))
        config["stop_loss_pct"] = max(0.01, min(0.2, float(config["stop_loss_pct"])))
        config["take_profit_pct"] = max(0.05, min(0.5, float(config["take_profit_pct"])))

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
