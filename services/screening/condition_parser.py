"""
条件解析器 - 支持嵌套AND/OR逻辑

解析策略配置中的条件表达式，支持：
1. 简单字符串条件：如 "RSI_14 < 30"
2. 字段-值条件：如 {"field": "roe_min", "value": 15}
3. 嵌套逻辑组：如 {"logic": "OR", "conditions": ["A", "B"]}
"""

from typing import Dict, Any, Union, List
from services.common.indicators import TechnicalIndicators


class ConditionParser:
    """解析嵌套逻辑条件"""

    # stock_filters字段显示名称映射
    FILTER_FIELD_NAMES = {
        "total_market_cap_max": "总市值<",
        "total_market_cap_min": "总市值>",
        "circ_market_cap_max": "流通市值<",
        "circ_market_cap_min": "流通市值>",
        "pe_ttm_max": "PE<",
        "pe_ttm_min": "PE>",
        "pb_max": "PB<",
        "pb_min": "PB>",
        "roe_min": "ROE>",
        "roe_max": "ROE<",
        "gross_margin_min": "毛利率>",
        "net_margin_min": "净利率>",
        "revenue_growth_yoy_min": "营收增长>",
        "sw_level1": "行业",
    }

    def evaluate(self, condition_node: Any, indicators: Dict) -> bool:
        """
        递归评估条件节点

        Args:
            condition_node: 条件节点，可以是：
                - 字符串："RSI_14 < 30"
                - 字段-值字典：{"field": "roe_min", "value": 15}
                - 逻辑组：{"logic": "AND/OR", "conditions": [...]}
                - 列表（旧格式）：["cond1", "cond2"]
            indicators: 指标值字典

        Returns:
            条件是否满足
        """
        # 1. 字符串条件（buy_conditions基本单元）
        if isinstance(condition_node, str):
            return self._evaluate_simple(condition_node, indicators)

        # 2. 字典类型
        if isinstance(condition_node, dict):
            # 2a. 字段-值条件（stock_filters基本单元）
            if "field" in condition_node:
                return self._evaluate_filter_field(condition_node, indicators)

            # 2b. 逻辑组
            logic = condition_node.get("logic", "AND")
            conditions = condition_node.get("conditions", [])

            if not conditions:
                return True  # 空条件默认满足

            results = [self.evaluate(c, indicators) for c in conditions]

            if logic.upper() == "AND":
                return all(results)
            else:  # OR
                return any(results)

        # 3. 列表（旧格式，隐式AND）
        if isinstance(condition_node, list):
            if not condition_node:
                return True
            return all(self.evaluate(c, indicators) for c in condition_node)

        # 4. 无法识别的类型
        return True

    def _evaluate_simple(self, condition: str, indicators: Dict) -> bool:
        """
        评估简单字符串条件

        复用 TechnicalIndicators.check_condition
        """
        return TechnicalIndicators.check_condition(condition, indicators)

    def _evaluate_filter_field(self, condition: Dict, indicators: Dict) -> bool:
        """
        评估stock_filters字段条件

        Args:
            condition: {"field": "roe_min", "value": 15}
            indicators: 指标值字典

        Returns:
            条件是否满足
        """
        field = condition.get("field", "")
        value = condition.get("value")

        if not field or value is None:
            return True

        # 从indicators获取字段值
        # 字段名可能是小写（数据库格式）或大写（代码格式）
        indicator_value = indicators.get(field) or indicators.get(field.lower())

        if indicator_value is None:
            # 指标值不存在，无法判断，默认返回True（不过滤）
            return True

        # 根据字段名确定比较方向
        # _max 结尾：指标值 <= 目标值
        # _min 结尾：指标值 >= 目标值
        # 其他：精确匹配
        if field.endswith("_max") or field.endswith("Max"):
            return indicator_value <= value
        elif field.endswith("_min") or field.endswith("Min"):
            return indicator_value >= value
        elif field == "sw_level1":
            # 行业匹配
            return str(indicator_value) == str(value)
        else:
            # 精确匹配
            return indicator_value == value

    def format_condition(self, condition_node: Any) -> str:
        """
        格式化条件节点为可读字符串

        Args:
            condition_node: 条件节点

        Returns:
            可读字符串，如 "RSI_14 < 30" 或 "(A OR B)"
        """
        # 字符串条件
        if isinstance(condition_node, str):
            return condition_node

        # 字段-值条件
        if isinstance(condition_node, dict) and "field" in condition_node:
            return self._format_filter_field(condition_node)

        # 逻辑组
        if isinstance(condition_node, dict) and "logic" in condition_node:
            logic = condition_node.get("logic", "AND")
            conditions = condition_node.get("conditions", [])
            formatted_parts = [self.format_condition(c) for c in conditions]

            if logic.upper() == "OR":
                return "(" + " OR ".join(formatted_parts) + ")"
            else:
                return " AND ".join(formatted_parts)

        # 列表（旧格式）
        if isinstance(condition_node, list):
            return " AND ".join([self.format_condition(c) for c in condition_node])

        return str(condition_node)

    def _format_filter_field(self, condition: Dict) -> str:
        """格式化stock_filters字段条件"""
        field = condition.get("field", "")
        value = condition.get("value")

        display_name = self.FILTER_FIELD_NAMES.get(field, field)

        # 根据字段类型格式化
        if field.endswith("_max") or field.endswith("Max"):
            return f"{display_name}{value}"
        elif field.endswith("_min") or field.endswith("Min"):
            return f"{display_name}{value}"
        elif field == "sw_level1":
            return f"{display_name}={value}"
        else:
            return f"{display_name}={value}"

    def get_all_conditions(self, condition_node: Any) -> List[str]:
        """
        提取所有基本条件（用于匹配度计算）

        Args:
            condition_node: 条件节点

        Returns:
            基本条件字符串列表
        """
        conditions = []

        if isinstance(condition_node, str):
            conditions.append(condition_node)
        elif isinstance(condition_node, dict):
            if "field" in condition_node:
                conditions.append(self._format_filter_field(condition_node))
            elif "conditions" in condition_node:
                for c in condition_node["conditions"]:
                    conditions.extend(self.get_all_conditions(c))
        elif isinstance(condition_node, list):
            for c in condition_node:
                conditions.extend(self.get_all_conditions(c))

        return conditions


def normalize_conditions(conditions: Any) -> Dict:
    """
    将旧格式转换为新格式

    Args:
        conditions: 可能是旧格式（列表、字段字典）或新格式

    Returns:
        新格式：{"logic": "AND/OR", "conditions": [...]}
    """
    parser = ConditionParser()

    # 新格式（已有logic字段）
    if isinstance(conditions, dict) and "logic" in conditions:
        return conditions

    # 旧格式：列表 ["cond1", "cond2"]
    if isinstance(conditions, list):
        return {"logic": "AND", "conditions": conditions}

    # 旧格式：stock_filters字典 {"field": value}
    if isinstance(conditions, dict):
        converted_conditions = []
        for field, value in conditions.items():
            converted_conditions.append({"field": field, "value": value})
        return {"logic": "AND", "conditions": converted_conditions}

    # 其他情况，返回空条件
    return {"logic": "AND", "conditions": []}


# 全局单例
_parser: ConditionParser = None


def get_condition_parser() -> ConditionParser:
    """获取条件解析器单例"""
    global _parser
    if _parser is None:
        _parser = ConditionParser()
    return _parser