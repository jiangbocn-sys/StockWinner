"""
持仓调整规则 API
根据市场条件动态调整持仓策略参数
支持自然语言描述 → LLM翻译为可执行表达式
"""

from fastapi import APIRouter, HTTPException, Path as PathParam, Body
from typing import Optional, List, Dict, Any
from datetime import datetime, timezone, timedelta
from pathlib import Path
from services.common.database import get_db_manager
import json

# 中国时区
CHINA_TZ = timezone(timedelta(hours=8))

def get_china_time():
    """获取中国时区时间"""
    return datetime.now(CHINA_TZ).replace(tzinfo=None)

router = APIRouter()


# 可用的指数列表
AVAILABLE_INDICES = {
    "INDEX_000001_SH": {"name": "上证指数", "code": "000001.SH"},
    "INDEX_399001_SZ": {"name": "深证成指", "code": "399001.SZ"},
    "INDEX_399006_SZ": {"name": "创业板指", "code": "399006.SZ"},
    "INDEX_000300_SH": {"name": "沪深300", "code": "000300.SH"},
    "INDEX_000016_SH": {"name": "上证50", "code": "000016.SH"},
    "INDEX_399005_SZ": {"name": "中小板指", "code": "399005.SZ"},
}

# 可用的技术指标
AVAILABLE_FACTORS = {
    "RSI_14": {"name": "RSI(14)", "type": "value", "range": "0-100"},
    "MACD": {"name": "MACD柱", "type": "value"},
    "DIF": {"name": "DIF线", "type": "value"},
    "DEA": {"name": "DEA线", "type": "value"},
    "MA5": {"name": "5日均线", "type": "value"},
    "MA10": {"name": "10日均线", "type": "value"},
    "MA20": {"name": "20日均线", "type": "value"},
    "CLOSE": {"name": "收盘价", "type": "value"},
    "VOLUME": {"name": "成交量", "type": "value"},
    "VOLUME_RATIO": {"name": "量比", "type": "value", "hint": "当日成交量/5日平均成交量"},
    "AMPLITUDE": {"name": "振幅", "type": "value", "hint": "(最高-最低)/昨收"},
    # 穿越信号
    "MACD_CROSS_UP_DEA": {"name": "MACD金叉", "type": "signal", "hint": "DIF向上穿越DEA"},
    "MACD_CROSS_DOWN_DEA": {"name": "MACD死叉", "type": "signal", "hint": "DIF向下穿越DEA"},
    "MA5_CROSS_UP_MA10": {"name": "MA5金叉MA10", "type": "signal"},
    "MA5_CROSS_DOWN_MA10": {"name": "MA5死叉MA10", "type": "signal"},
    "CLOSE_BREAK_UP_MA5": {"name": "收盘价突破MA5", "type": "signal"},
    "CLOSE_BREAK_DOWN_MA5": {"name": "收盘价跌破MA5", "type": "signal"},
}


def get_available_params_hint() -> str:
    """生成可用参数提示信息"""
    hint = "【可用指数】\n"
    for key, info in AVAILABLE_INDICES.items():
        hint += f"  - {info['name']} ({key})\n"

    hint += "\n【可用指标】\n"
    hint += "数值类（可比较）：\n"
    for key, info in AVAILABLE_FACTORS.items():
        if info.get("type") == "value":
            range_hint = info.get("range", "")
            extra_hint = info.get("hint", "")
            hint += f"  - {info['name']} ({key})"
            if range_hint:
                hint += f" 范围:{range_hint}"
            if extra_hint:
                hint += f" [{extra_hint}]"
            hint += "\n"

    hint += "信号类（穿越/突破）：\n"
    for key, info in AVAILABLE_FACTORS.items():
        if info.get("type") == "signal":
            extra_hint = info.get("hint", "")
            hint += f"  - {info['name']} ({key})"
            if extra_hint:
                hint += f" [{extra_hint}]"
            hint += "\n"

    hint += "\n【示例表述】\n"
    hint += "  - \"上证指数RSI跌破30\"\n"
    hint += "  - \"大盘MACD金叉\"\n"
    hint += "  - \"创业板成交量放大两倍\"\n"
    hint += "  - \"沪深300跌破5日均线\"\n"

    return hint

# LLM SYSTEM_PROMPT
LLM_TRANSLATE_PROMPT = """你是一个触发条件翻译助手。将用户的自然语言描述翻译为可执行的触发条件表达式。

【可用指数】：
- INDEX_000001_SH = 上证指数
- INDEX_399001_SZ = 深证成指
- INDEX_399006_SZ = 创圳板指
- INDEX_000300_SH = 沪深300
- INDEX_000016_SH = 上证50

【可用指标】：
- RSI_14 = RSI指标(14日)
- MACD, DIF, DEA = MACD相关
- MA5, MA10, MA20 = 均线
- CLOSE = 收盘价
- VOLUME = 成交量
- VOLUME_RATIO = 量比

【穿越信号】（专用条件名）：
- MACD_CROSS_UP_DEA = MACD金叉
- MACD_CROSS_DOWN_DEA = MACD死叉
- MA5_CROSS_UP_MA10 = MA5金叉MA10
- CLOSE_BREAK_UP_MA5 = 收盘价突破MA5向上

【表达式格式】：
1. 比较表达式：INDEX_000001_SH.RSI_14 < 30
2. 穿越信号：INDEX_000001_SH.MACD_CROSS_UP_DEA
3. 成交量变化：INDEX_000001_SH.VOLUME_RATIO > 2

【翻译示例】：
用户："上证指数RSI跌破30" → INDEX_000001_SH.RSI_14 < 30
用户："大盘MACD金叉" → INDEX_000001_SH.MACD_CROSS_UP_DEA
用户："创业板成交量放大两倍" → INDEX_399006_SZ.VOLUME_RATIO > 2
用户："上证跌破5日均线" → INDEX_000001_SH.CLOSE < INDEX_000001_SH.MA5

请严格按照以下JSON格式返回，不要包含其他说明：
{
    "expression": "INDEX_000001_SH.RSI_14 < 30",
    "description": "上证指数RSI跌破30",
    "target_index": "INDEX_000001_SH",
    "factor": "RSI_14",
    "condition_type": "comparison"
}"""


async def translate_with_llm(description: str, provider_config: Dict[str, Any] = None) -> Dict[str, Any]:
    """
    调用LLM将自然语言翻译为触发条件表达式

    Args:
        description: 用户自然语言描述
        provider_config: LLM配置（可选，默认从llm.json加载）

    Returns:
        翻译结果：{expression, description, target_index, factor, condition_type}
    """
    import urllib.request
    import urllib.error

    # 加载LLM配置
    config_path = Path(__file__).parent.parent.parent / "config" / "llm.json"
    if config_path.exists():
        with open(config_path, 'r') as f:
            config = json.load(f)
    else:
        raise Exception("LLM配置文件不存在，请先在Settings页面配置")

    api_key = config.get("api_key", "")
    base_url = config.get("base_url", "")
    model = config.get("model", "")

    if not api_key or not base_url:
        raise Exception("LLM API未配置，请在Settings页面配置API Key")

    # 构建请求
    user_prompt = f"""请将以下触发条件描述翻译为可执行表达式：

用户描述：{description}

请返回纯JSON，不要包含markdown格式。"""

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}"
    }

    data = {
        "model": model,
        "messages": [
            {"role": "system", "content": LLM_TRANSLATE_PROMPT},
            {"role": "user", "content": user_prompt}
        ],
        "temperature": 0.3,
        "max_tokens": 256
    }

    req = urllib.request.Request(
        base_url,
        data=json.dumps(data).encode("utf-8"),
        headers=headers,
        method="POST"
    )

    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            result = json.loads(response.read().decode("utf-8"))
            content = result["choices"][0]["message"]["content"]

            # 解析JSON
            content = content.strip()
            if content.startswith("```json"):
                content = content[7:]
            elif content.startswith("```"):
                content = content[3:]
            if content.endswith("```"):
                content = content[:-3]

            translated = json.loads(content.strip())
            return translated

    except urllib.error.HTTPError as e:
        error_body = e.read().decode("utf-8")
        raise Exception(f"LLM API错误 ({e.code}): {error_body}")
    except json.JSONDecodeError as e:
        raise Exception(f"解析LLM响应失败: {e}")


@router.get("/api/v1/ui/{account_id}/position-rules")
async def list_position_rules(account_id: str = PathParam(..., description="账户 ID")):
    """获取账户的持仓调整规则列表"""
    db = get_db_manager()

    # 验证账户
    account = await db.fetchone(
        "SELECT 1 FROM accounts WHERE account_id = ? AND is_active = 1",
        (account_id,)
    )
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    rules = await db.fetchall(
        "SELECT * FROM position_adjust_rules WHERE account_id = ? ORDER BY priority DESC, id ASC",
        (account_id,)
    )

    return {
        "success": True,
        "rules": [
            {
                "id": r["id"],
                "account_id": r["account_id"],
                "trigger_expression": r.get("trigger_expression") or r.get("trigger_condition"),
                "trigger_description": r.get("trigger_description") or r.get("description"),
                "target_max_total_pct": float(r.get("target_max_total_pct") or 0),
                "target_max_single_pct": float(r.get("target_max_single_pct") or 0),
                "priority": int(r.get("priority") or 0),
                "is_active": bool(r.get("is_active"))
            }
            for r in rules
        ],
        "available_indices": AVAILABLE_INDICES,
        "available_factors": AVAILABLE_FACTORS
    }


@router.post("/api/v1/ui/{account_id}/position-rules/translate")
async def translate_trigger_condition(
    account_id: str = PathParam(..., description="账户 ID"),
    description: str = Body(..., description="自然语言触发条件描述")
):
    """将自然语言描述翻译为触发条件表达式（LLM翻译）"""
    db = get_db_manager()

    # 验证账户
    account = await db.fetchone(
        "SELECT 1 FROM accounts WHERE account_id = ? AND is_active = 1",
        (account_id,)
    )
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    try:
        # 调用LLM翻译
        translated = await translate_with_llm(description)

        return {
            "success": True,
            "translated": translated,
            "original_description": description
        }
    except Exception as e:
        # 翻译失败时返回可用参数提示
        error_msg = str(e)
        if "无法识别" in error_msg or "不支持" in error_msg or "JSON" in error_msg or "解析" in error_msg:
            # LLM无法理解用户描述，返回可用参数提示
            return {
                "success": False,
                "error": f"无法理解您的描述，请参考以下可用参数：\n{get_available_params_hint()}",
                "original_description": description,
                "available_params_hint": get_available_params_hint()
            }
        else:
            # 其他错误（如API连接失败）
            return {
                "success": False,
                "error": f"翻译失败：{error_msg}",
                "original_description": description
            }


@router.post("/api/v1/ui/{account_id}/position-rules")
async def create_position_rule(
    account_id: str = PathParam(..., description="账户 ID"),
    trigger_expression: str = Body(..., description="触发条件表达式"),
    trigger_description: str = Body(..., description="触发条件描述"),
    target_max_total_pct: Optional[float] = Body(None, description="目标总仓位比例"),
    target_max_single_pct: Optional[float] = Body(None, description="目标单股仓位比例"),
    priority: Optional[int] = Body(0, description="优先级"),
    is_active: Optional[int] = Body(1, description="是否启用")
):
    """创建持仓调整规则（使用表达式）"""
    db = get_db_manager()

    # 验证账户
    account = await db.fetchone(
        "SELECT 1 FROM accounts WHERE account_id = ? AND is_active = 1",
        (account_id,)
    )
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    # 验证参数范围
    if target_max_total_pct is not None and not (0.0 <= target_max_total_pct <= 1.0):
        raise HTTPException(status_code=400, detail="总仓位比例必须在0-1之间")
    if target_max_single_pct is not None and not (0.0 <= target_max_single_pct <= 1.0):
        raise HTTPException(status_code=400, detail="单股仓位比例必须在0-1之间")

    # 检查是否已存在相同表达式
    existing = await db.fetchone(
        "SELECT id FROM position_adjust_rules WHERE account_id = ? AND trigger_expression = ?",
        (account_id, trigger_expression)
    )

    if existing:
        raise HTTPException(status_code=400, detail=f"该触发条件已存在规则")

    # 创建规则
    rule_id = await db.insert(
        "position_adjust_rules",
        {
            "account_id": account_id,
            "trigger_expression": trigger_expression,
            "trigger_description": trigger_description,
            "target_max_total_pct": target_max_total_pct or 0.5,
            "target_max_single_pct": target_max_single_pct or 0,
            "priority": priority or 0,
            "is_active": is_active or 1,
            "created_at": get_china_time().isoformat()
        }
    )

    return {
        "success": True,
        "message": "持仓调整规则创建成功",
        "rule_id": rule_id
    }


@router.post("/api/v1/ui/{account_id}/position-rules/create-from-natural")
async def create_rule_from_natural_language(
    account_id: str = PathParam(..., description="账户 ID"),
    description: str = Body(..., description="自然语言触发条件描述"),
    target_max_total_pct: Optional[float] = Body(None, description="目标总仓位比例"),
    target_max_single_pct: Optional[float] = Body(None, description="目标单股仓位比例"),
    priority: Optional[int] = Body(0, description="优先级")
):
    """从自然语言描述创建规则（自动LLM翻译）"""
    db = get_db_manager()

    # 验证账户
    account = await db.fetchone(
        "SELECT 1 FROM accounts WHERE account_id = ? AND is_active = 1",
        (account_id,)
    )
    if not account:
        raise HTTPException(status_code=404, detail=f"账户不存在或未激活：{account_id}")

    try:
        # 调用LLM翻译
        translated = await translate_with_llm(description)

        trigger_expression = translated.get("expression")
        trigger_description = translated.get("description", description)

        if not trigger_expression:
            raise Exception("LLM翻译失败：未生成表达式")

        # 创建规则
        rule_id = await db.insert(
            "position_adjust_rules",
            {
                "account_id": account_id,
                "trigger_expression": trigger_expression,
                "trigger_description": trigger_description,
                "target_max_total_pct": target_max_total_pct or 0.5,
                "target_max_single_pct": target_max_single_pct or 0,
                "priority": priority or 0,
                "is_active": 1,
                "created_at": get_china_time().isoformat()
            }
        )

        return {
            "success": True,
            "message": "规则创建成功",
            "rule_id": rule_id,
            "translated": translated
        }

    except Exception as e:
        error_msg = str(e)
        # 翻译失败时返回可用参数提示
        return {
            "success": False,
            "error": f"无法理解您的描述，请参考以下可用参数：\n{get_available_params_hint()}",
            "message": f"创建失败：{error_msg}",
            "available_params_hint": get_available_params_hint()
        }


@router.put("/api/v1/ui/{account_id}/position-rules/{rule_id}")
async def update_position_rule(
    account_id: str = PathParam(..., description="账户 ID"),
    rule_id: int = PathParam(..., description="规则 ID"),
    trigger_expression: Optional[str] = Body(None, description="触发条件表达式"),
    trigger_description: Optional[str] = Body(None, description="触发条件描述"),
    target_max_total_pct: Optional[float] = Body(None, description="目标总仓位比例"),
    target_max_single_pct: Optional[float] = Body(None, description="目标单股仓位比例"),
    priority: Optional[int] = Body(None, description="优先级"),
    is_active: Optional[int] = Body(None, description="是否启用")
):
    """更新持仓调整规则"""
    db = get_db_manager()

    # 检查规则是否存在
    existing = await db.fetchone(
        "SELECT id FROM position_adjust_rules WHERE account_id = ? AND id = ?",
        (account_id, rule_id)
    )

    if not existing:
        raise HTTPException(status_code=404, detail="规则不存在")

    # 验证参数范围
    if target_max_total_pct is not None and not (0.0 <= target_max_total_pct <= 1.0):
        raise HTTPException(status_code=400, detail="总仓位比例必须在0-1之间")
    if target_max_single_pct is not None and not (0.0 <= target_max_single_pct <= 1.0):
        raise HTTPException(status_code=400, detail="单股仓位比例必须在0-1之间")

    # 更新
    update_data = {}
    if trigger_expression is not None:
        update_data["trigger_expression"] = trigger_expression
    if trigger_description is not None:
        update_data["trigger_description"] = trigger_description
    if target_max_total_pct is not None:
        update_data["target_max_total_pct"] = target_max_total_pct
    if target_max_single_pct is not None:
        update_data["target_max_single_pct"] = target_max_single_pct
    if priority is not None:
        update_data["priority"] = priority
    if is_active is not None:
        update_data["is_active"] = is_active

    update_data["updated_at"] = get_china_time().isoformat()

    if update_data:
        await db.update("position_adjust_rules", update_data, "id = ?", (rule_id,))

    return {
        "success": True,
        "message": "持仓调整规则更新成功"
    }


@router.delete("/api/v1/ui/{account_id}/position-rules/{rule_id}")
async def delete_position_rule(
    account_id: str = PathParam(..., description="账户 ID"),
    rule_id: int = PathParam(..., description="规则 ID")
):
    """删除持仓调整规则"""
    db = get_db_manager()

    # 检查规则是否存在
    existing = await db.fetchone(
        "SELECT id FROM position_adjust_rules WHERE account_id = ? AND id = ?",
        (account_id, rule_id)
    )

    if not existing:
        raise HTTPException(status_code=404, detail="规则不存在")

    await db.execute(
        "DELETE FROM position_adjust_rules WHERE account_id = ? AND id = ?",
        (account_id, rule_id)
    )

    return {
        "success": True,
        "message": "持仓调整规则已删除"
    }


@router.post("/api/v1/ui/{account_id}/position-rules/{rule_id}/toggle")
async def toggle_position_rule(
    account_id: str = PathParam(..., description="账户 ID"),
    rule_id: int = PathParam(..., description="规则 ID")
):
    """启用/停用持仓调整规则"""
    db = get_db_manager()

    # 检查规则是否存在
    rule = await db.fetchone(
        "SELECT id, is_active FROM position_adjust_rules WHERE account_id = ? AND id = ?",
        (account_id, rule_id)
    )

    if not rule:
        raise HTTPException(status_code=404, detail="规则不存在")

    new_status = 0 if rule["is_active"] else 1
    await db.update(
        "position_adjust_rules",
        {"is_active": new_status, "updated_at": get_china_time().isoformat()},
        "id = ?",
        (rule_id,)
    )

    return {
        "success": True,
        "message": f"规则已{'停用' if new_status == 0 else '启用'}",
        "is_active": new_status
    }