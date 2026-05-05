"""
策略执行引擎 (Strategy Engine)

加载 Python 代码型策略，注入执行上下文，安全执行并捕获交易信号。

使用方式：
    engine = StrategyEngine()
    signals = engine.execute_strategy(strategy, context)
    # signals = [{action, stock_code, buy_price, target_quantity, ...}]
"""

import asyncio
import types
from typing import Dict, List, Optional, Any

from services.common.database import get_db_manager
from services.common.stock_code import normalize_stock_code


# 白名单：允许在策略代码中使用的模块和函数
ALLOWED_BUILTINS = {
    # 基础类型
    "int": int,
    "float": float,
    "str": str,
    "bool": bool,
    "list": list,
    "dict": dict,
    "tuple": tuple,
    "set": set,
    "len": len,
    "range": range,
    "enumerate": enumerate,
    "zip": zip,
    "map": map,
    "filter": filter,
    "sorted": sorted,
    "reversed": reversed,
    "sum": sum,
    "min": min,
    "max": max,
    "abs": abs,
    "round": round,
    "any": any,
    "all": all,
    "isinstance": isinstance,
    "issubclass": issubclass,
    "type": type,
    "print": print,
    "True": True,
    "False": False,
    "None": None,
    "Exception": Exception,
    "ValueError": ValueError,
    "TypeError": TypeError,
    "KeyError": KeyError,
    "IndexError": IndexError,
}

# 黑名单：禁止使用的函数
FORBIDDEN = {"__import__", "import", "open", "eval", "exec", "compile",
             "getattr", "setattr", "delattr", "globals", "locals",
             "breakpoint", "input"}


class StrategyEngine:
    """策略执行引擎"""

    def execute_strategy(self, strategy: Dict, context: Dict) -> List[Dict]:
        """
        执行一条 Python 代码型策略

        Args:
            strategy: {code, function_name, config, name, ...}
            context: {
                stocks: [{stock_code, stock_name, ...}],
                account_id: str,
                today: str,         # 今日日期 YYYY-MM-DD
                indicators: {...},  # 技术指标工具
                get_kline: fn,      # K 线数据获取函数
                get_market_data: fn,# 实时行情获取函数
            }

        Returns:
            signals: [{
                action: 'buy' | 'sell' | 'watch',
                stock_code, stock_name,
                buy_price, target_quantity,
                stop_loss_pct, take_profit_pct,
                reason, expire_at
            }]
        """
        code = strategy.get("code", "")
        function_name = strategy.get("function_name", "run") or "run"

        if not code or not code.strip():
            raise ValueError("策略代码为空")

        # 构建执行环境
        env = self._build_env(context)

        # 执行代码（定义函数）
        try:
            exec(code, env)
        except SyntaxError as e:
            raise ValueError(f"策略代码语法错误: {e}")

        # 获取入口函数
        func = env.get(function_name)
        if func is None or not callable(func):
            raise ValueError(f"策略中未找到入口函数 '{function_name}'")

        # 调用入口函数
        try:
            result = func(context)
        except Exception as e:
            raise RuntimeError(f"策略执行错误: {e}")

        # 验证返回值
        if result is None:
            return []
        if not isinstance(result, list):
            raise TypeError(f"策略返回值应为列表，实际为 {type(result).__name__}")

        # 标准化信号
        signals = []
        for item in result:
            if not isinstance(item, dict):
                continue
            signal = self._normalize_signal(item)
            if signal:
                signals.append(signal)

        return signals

    def validate_code(self, code: str) -> Dict:
        """
        验证策略代码语法

        Returns:
            {"valid": bool, "error": str}
        """
        try:
            compile(code, "<strategy>", "exec")
            return {"valid": True, "error": None}
        except SyntaxError as e:
            return {"valid": False, "error": str(e)}

    def _build_env(self, context: Dict) -> Dict:
        """构建安全的执行环境"""
        # 基础环境
        env = {"__builtins__": {}}

        # 注入白名单内置函数
        for name, obj in ALLOWED_BUILTINS.items():
            env["__builtins__"][name] = obj

        # 注入工具函数
        env["normalize_stock_code"] = normalize_stock_code
        env["get_db_manager"] = get_db_manager

        # 注入技术指标工具
        env["indicators"] = context.get("indicators", {})

        # 注入数据获取函数
        env["get_kline"] = context.get("get_kline", lambda *a, **k: None)
        env["get_market_data"] = context.get("get_market_data", lambda *a, **k: None)

        return env

    def _normalize_signal(self, item: Dict) -> Optional[Dict]:
        """标准化交易信号"""
        stock_code = item.get("stock_code", "").strip()
        if not stock_code:
            return None

        # 规范化代码
        stock_code = normalize_stock_code(stock_code)

        action = item.get("action", "buy")
        if action not in ("buy", "sell", "watch"):
            action = "watch"

        return {
            "action": action,
            "stock_code": stock_code,
            "stock_name": item.get("stock_name", stock_code),
            "buy_price": item.get("buy_price"),
            "target_quantity": item.get("target_quantity", 100),
            "stop_loss_pct": item.get("stop_loss_pct", 0.05),
            "take_profit_pct": item.get("take_profit_pct", 0.15),
            "reason": item.get("reason", ""),
            "expire_at": item.get("expire_at"),
        }

    async def write_signals_to_watchlist(
        self,
        signals: List[Dict],
        account_id: str,
        strategy_id: int,
        group_id: int
    ) -> Dict:
        """
        将策略信号写入 watchlist

        Returns:
            {"added": N, "skipped": M, "total": N}
        """
        db = get_db_manager()

        # 获取当前组内已有股票代码
        existing_rows = await db.fetchall(
            "SELECT stock_code FROM watchlist WHERE account_id = ? AND group_id = ? AND status IN ('pending', 'watching')",
            (account_id, group_id)
        )
        existing_codes = {row["stock_code"] for row in existing_rows}

        added = 0
        skipped = 0
        now = __import__("services.common.timezone", fromlist=["get_china_time"]).get_china_time().isoformat()

        for signal in signals:
            code = signal["stock_code"]
            if code in existing_codes:
                skipped += 1
                continue

            buy_price = signal.get("buy_price")
            sl_pct = signal.get("stop_loss_pct", 0.05)
            tp_pct = signal.get("take_profit_pct", 0.15)

            await db.insert("watchlist", {
                "account_id": account_id,
                "strategy_id": strategy_id,
                "group_id": group_id,
                "source_type": "strategy",
                "stock_code": code,
                "stock_name": signal.get("stock_name", code),
                "reason": signal.get("reason", "策略信号"),
                "buy_price": buy_price,
                "stop_loss_price": round(buy_price * (1 - sl_pct), 2) if buy_price else None,
                "take_profit_price": round(buy_price * (1 + tp_pct), 2) if buy_price else None,
                "target_quantity": signal.get("target_quantity", 100),
                "status": "pending",
                "created_at": now,
                "updated_at": now,
            })
            existing_codes.add(code)
            added += 1

        return {"added": added, "skipped": skipped, "total": len(signals)}


# 全局单例
_engine: Optional[StrategyEngine] = None


def get_strategy_engine() -> StrategyEngine:
    global _engine
    if _engine is None:
        _engine = StrategyEngine()
    return _engine
