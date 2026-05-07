"""
策略执行引擎 (Strategy Engine)

加载 Python 代码型策略，注入执行上下文，安全执行并捕获交易信号。

使用方式：
    engine = StrategyEngine()
    signals = engine.execute_strategy(strategy, context)
    # signals = [{action, stock_code, buy_price, target_quantity, ...}]
"""

import ast
import asyncio
import types
from typing import Dict, List, Optional, Any

from services.common.database import get_db_manager
from services.common.stock_code import normalize_stock_code
from services.common.kronos_service import get_kronos_service


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

# 允许 import 的模块白名单
ALLOWED_MODULES = {
    "pandas", "numpy", "datetime", "statistics", "json", "math", "re",
    "collections", "itertools", "functools", "dataclasses", "typing",
    "time", "calendar", "decimal", "copy", "string",
}

# 黑名单：禁止使用的函数
FORBIDDEN = {"__import__", "import", "open", "eval", "exec", "compile",
             "getattr", "setattr", "delattr", "globals", "locals",
             "breakpoint", "input"}


def _restricted_import(name, globals=None, locals=None, fromlist=(), level=0):
    """受限的 import：只允许白名单模块"""
    base = name.split(".")[0]
    if base not in ALLOWED_MODULES:
        raise ImportError(f"模块 '{name}' 不在允许列表中（可用: {', '.join(sorted(ALLOWED_MODULES))}）")
    return __import__(name, globals, locals, fromlist, level)


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
        验证策略代码语法和调用逻辑

        检查项：
        1. Python 语法正确性
        2. 存在入口函数 def run(context)
        3. 禁止的调用模式（SDK 直调、危险函数）
        4. 推荐的调用模式提示

        Returns:
            {"valid": bool, "error": str, "warnings": list, "info": list}
        """
        warnings = []
        errors = []
        info = []

        # 1. 语法检查 + AST 构建
        try:
            tree = ast.parse(code, filename="<strategy>")
        except SyntaxError as e:
            return {"valid": False, "error": str(e), "warnings": [], "info": []}

        # 2. 检查入口函数
        if isinstance(tree, ast.Module):
            has_run_func = False
            for node in ast.walk(tree):
                if isinstance(node, ast.AsyncFunctionDef):
                    errors.append(f"入口函数不能是 async def: '{node.name}'，请使用 def")
                    return {"valid": False, "error": errors[0], "warnings": warnings, "info": info}
                if isinstance(node, ast.FunctionDef) and node.name == "run":
                    has_run_func = True
                    if len(node.args.args) < 1:
                        errors.append("run() 需要至少一个参数 (context)")
                    elif node.args.args[0].arg != "context":
                        warnings.append(f"建议第一个参数命名为 'context'，当前为 '{node.args.args[0].arg}'")
            if not has_run_func:
                errors.append("未找到入口函数 def run(context)")

        # 3. 禁止的调用模式 — AST 遍历
        forbidden_patterns = {
            "get_sdk_manager": "禁止直接调用 get_sdk_manager()，请使用 context 中注入的数据函数",
            "get_sdk": "禁止直接获取 SDK，请使用 context 中注入的数据函数",
            "query_kline": "禁止直接调用 SDK.query_kline()，请使用 get_kline_smart() 或 get_batch_kline()",
            "get_market_data": "禁止直接调用 SDK.get_market_data()，请使用 context 中的数据函数",
            "__import__": "禁止使用 __import__",
            "import_module": "禁止使用 importlib.import_module",
        }

        for node in ast.walk(tree):
            # 检查函数调用
            if isinstance(node, ast.Call):
                # 检查 func 名称
                if isinstance(node.func, ast.Name):
                    func_name = node.func.id
                    if func_name in forbidden_patterns:
                        errors.append(f"第 {node.lineno} 行: {forbidden_patterns[func_name]}")
                # 检查属性调用 (如 xxx.query_kline)
                elif isinstance(node.func, ast.Attribute):
                    if node.func.attr in forbidden_patterns:
                        errors.append(f"第 {node.lineno} 行: {forbidden_patterns[node.func.attr]}")

            # 检查 import 语句（只允许白名单模块）
            if isinstance(node, ast.Import):
                for alias in node.names:
                    base = alias.name.split(".")[0]
                    if base not in ALLOWED_MODULES:
                        errors.append(f"第 {node.lineno} 行: 模块 '{alias.name}' 不在允许列表中")
            if isinstance(node, ast.ImportFrom) and node.module:
                base = node.module.split(".")[0]
                if base not in ALLOWED_MODULES:
                    errors.append(f"第 {node.lineno} 行: 模块 '{node.module}' 不在允许列表中")

            # 检查 open() 调用
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "open":
                errors.append(f"第 {node.lineno} 行: 禁止使用 open() 文件操作")

        if errors:
            return {"valid": False, "error": "；".join(errors[:3]), "warnings": warnings, "info": info}

        # 4. 推荐检查（不阻塞，只警告）
        recommended = {
            "get_kline_smart": "智能K线获取",
            "get_batch_kline": "批量K线",
            "get_factors": "日频因子",
            "kronos_predict": "Kronos预测（无需 import 模型模块）",
        }
        has_recommended = False
        found_recommended = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name) and node.func.id in recommended:
                    has_recommended = True
                    found_recommended.append(recommended[node.func.id])

        if not has_recommended:
            warnings.append("未检测到推荐的数据获取函数调用（get_kline_smart / get_batch_kline / get_factors），请确保不使用 SDK 直调")
        elif found_recommended:
            info.append(f"检测到推荐函数: {', '.join(found_recommended)}")

        # 5. Kronos 专用提示：如果检测到旧的模型导入模式
        old_kronos_patterns = {"safetensors", "torch", "Kronos", "KronosTokenizer", "KronosPredictor"}
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module:
                base = node.module.split(".")[0]
                if base in old_kronos_patterns:
                    errors.append(f"第 {node.lineno} 行: 禁止直接导入模型模块 '{node.module}'，请使用沙盒注入的 kronos_predict()")
            if isinstance(node, ast.Import):
                for alias in node.names:
                    base = alias.name.split(".")[0]
                    if base in old_kronos_patterns:
                        errors.append(f"第 {node.lineno} 行: 禁止直接导入模型模块 '{alias.name}'，请使用沙盒注入的 kronos_predict()")

        return {"valid": True, "error": None, "warnings": warnings, "info": info}

    def _build_env(self, context: Dict) -> Dict:
        """构建安全的执行环境"""
        # 基础环境
        env = {"__builtins__": {"__import__": _restricted_import}}

        # 注入白名单内置函数
        for name, obj in ALLOWED_BUILTINS.items():
            env["__builtins__"][name] = obj

        # 注入工具函数
        env["normalize_stock_code"] = normalize_stock_code
        env["get_db_manager"] = get_db_manager

        # 注入技术指标工具
        env["indicators"] = context.get("indicators", {})

        # 注入数据获取函数 — 本地数据（同步，不经过 TGW）
        env["get_kline"] = context.get("get_kline", lambda *a, **k: None)
        env["get_batch_kline"] = context.get("get_batch_kline", lambda *a, **k: {})
        env["get_factors"] = context.get("get_factors", lambda *a, **k: None)
        env["get_factors_batch"] = context.get("get_factors_batch", lambda *a, **k: {})

        # 注入智能数据获取函数（自动判断盘中/盘后）
        env["get_kline_smart"] = context.get("get_kline_smart", lambda *a, **k: {})
        env["get_kline_spliced"] = context.get("get_kline_spliced", lambda *a, **k: {})

        # 注入数据获取函数 — 实时/TGW（走 gateway → sdk_connection_manager 排队）
        # 注意：_get_kline 和 _get_market_data 是异步函数，策略代码中直接调用会返回协程对象
        env["get_market_data"] = context.get("get_market_data", lambda *a, **k: None)
        env["get_realtime_quote"] = context.get("get_realtime_quote", lambda *a, **k: None)

        # 注入 Kronos 预测函数（同步，封装了模型加载/路径/环境变量等细节）
        kronos_service = get_kronos_service()
        def _kronos_predict(df_hist, pred_len=5, future_dates=None, **kwargs):
            """Kronos 时间序列预测（封装版，策略无需 import 任何模型相关模块）"""
            return kronos_service.predict(df_hist, pred_len=pred_len, future_dates=future_dates, **kwargs)
        env["kronos_predict"] = _kronos_predict
        env["kronos_available"] = kronos_service.is_available

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
            "target_quantity": item.get("target_quantity", 0),
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
        group_id: int,
        strategy_name: str = "",
    ) -> Dict:
        """
        将策略信号写入 watchlist

        去重规则：
        1. 当日该账户已买入过同一只股票 → 跳过
        2. 已有信号（pending/watching），新价格更低 → 更新价格/止损止盈
        3. 已有信号，新价格更高 → 保留原信号，跳过

        Args:
            strategy_name: 策略名称（用于通知）

        Returns:
            {"added": N, "updated": M, "skipped": K, "total": N}
        """
        db = get_db_manager()
        today = __import__("services.common.timezone", fromlist=["get_china_time"]).get_china_time().strftime("%Y-%m-%d")
        now = __import__("services.common.timezone", fromlist=["get_china_time"]).get_china_time().isoformat()

        # 1. 查询当日已买入的股票（去重：当日不重复买入）
        bought_today = await db.fetchall(
            "SELECT DISTINCT stock_code FROM trade_records WHERE account_id = ? AND DATE(trade_time) = ? AND trade_type = 'buy'",
            (account_id, today)
        )
        bought_codes = {row["stock_code"] for row in bought_today}

        # 2. 查询账户下所有待交易/监控中的股票（不限组，用于跨组去重）
        existing_rows = await db.fetchall(
            "SELECT stock_code, buy_price, stop_loss_price, take_profit_price FROM watchlist WHERE account_id = ? AND status IN ('pending', 'watching')",
            (account_id,)
        )
        existing = {row["stock_code"]: row for row in existing_rows}

        added = 0
        updated = 0
        skipped = 0

        for signal in signals:
            code = signal["stock_code"]
            new_price = signal.get("buy_price")

            # 当日已买入 → 跳过
            if code in bought_codes:
                skipped += 1
                continue

            sl_pct = signal.get("stop_loss_pct", 0.05)
            tp_pct = signal.get("take_profit_pct", 0.15)
            new_sl = round(new_price * (1 - sl_pct), 2) if new_price else None
            new_tp = round(new_price * (1 + tp_pct), 2) if new_price else None
            should_update = False

            if code in existing:
                old = existing[code]
                old_price = old.get("buy_price")

                # 新价格更优的判断：
                # 1. 原信号无价格但新信号有 → 更新
                # 2. 新旧都有价格，新价格更低 → 更新
                # 3. 否则保留原信号
                if new_price and not old_price:
                    should_update = True
                elif new_price and old_price and new_price < old_price:
                    should_update = True

                if should_update:
                    await db.execute(
                        "UPDATE watchlist SET buy_price = ?, stop_loss_price = ?, take_profit_price = ?, "
                        "reason = ?, strategy_id = ?, status = 'pending', created_at = ?, updated_at = ? WHERE account_id = ? AND stock_code = ?",
                        (new_price, new_sl, new_tp, signal.get("reason", "价格更优"), strategy_id, now, now, account_id, code)
                    )
                    updated += 1
                else:
                    # 价格更高或无新价格 → 保留原信号
                    skipped += 1
            else:
                # 全新信号 → 插入
                await db.insert("watchlist", {
                    "account_id": account_id,
                    "strategy_id": strategy_id,
                    "group_id": group_id,
                    "source_type": "strategy",
                    "stock_code": code,
                    "stock_name": signal.get("stock_name", code),
                    "reason": signal.get("reason", "策略信号"),
                    "buy_price": new_price,
                    "stop_loss_price": new_sl,
                    "take_profit_price": new_tp,
                    "target_quantity": signal.get("target_quantity", 0),
                    "status": "pending",
                    "created_at": now,
                    "updated_at": now,
                })
                added += 1

                # 发送信号触发通知
                try:
                    from services.notifications import get_notification_service
                    notification = get_notification_service()
                    # 查询候选组名称
                    group_name = "-"
                    if group_id:
                        group_row = await db.fetchone(
                            "SELECT name FROM candidate_groups WHERE id = ?", (group_id,)
                        )
                        if group_row:
                            group_name = group_row["name"]
                    await notification.emit(
                        event_type="signal_triggered",
                        account_id=account_id,
                        payload={
                            "stock_code": code,
                            "stock_name": signal.get("stock_name", code),
                            "price": f"{new_price:.2f}" if new_price else "-",
                            "buy_price": f"{new_price:.2f}" if new_price else "-",
                            "stop_loss_price": f"{new_sl:.2f}" if new_sl else "-",
                            "take_profit_price": f"{new_tp:.2f}" if new_tp else "-",
                            "reason": signal.get("reason", "策略信号"),
                            "strategy_name": strategy_name or "策略信号",
                            "group_name": group_name,
                            "condition": f"新增信号 | 建议买入价 {new_price:.2f}" if new_price else "新增信号",
                        },
                    )
                except Exception as e:
                    print(f"[StrategyEngine] 发送信号通知失败: {e}")

            # 已有信号但价格更优 → 也发送通知（价格更新）
            if code in existing and should_update:
                try:
                    from services.notifications import get_notification_service
                    notification = get_notification_service()
                    # 查询候选组名称
                    group_name = "-"
                    if group_id:
                        group_row = await db.fetchone(
                            "SELECT name FROM candidate_groups WHERE id = ?", (group_id,)
                        )
                        if group_row:
                            group_name = group_row["name"]
                    await notification.emit(
                        event_type="signal_triggered",
                        account_id=account_id,
                        payload={
                            "stock_code": code,
                            "stock_name": signal.get("stock_name", code),
                            "price": f"{new_price:.2f}" if new_price else "-",
                            "buy_price": f"{new_price:.2f}" if new_price else "-",
                            "stop_loss_price": f"{new_sl:.2f}" if new_sl else "-",
                            "take_profit_price": f"{new_tp:.2f}" if new_tp else "-",
                            "reason": signal.get("reason", "价格更优"),
                            "strategy_name": strategy_name or "策略信号",
                            "group_name": group_name,
                            "condition": f"价格更优 | 新买入价 {new_price:.2f}" if new_price else "价格更优",
                        },
                    )
                except Exception as e:
                    print(f"[StrategyEngine] 发送价格更新通知失败: {e}")

        return {"added": added, "updated": updated, "skipped": skipped, "total": len(signals)}


# 全局单例
_engine: Optional[StrategyEngine] = None


def get_strategy_engine() -> StrategyEngine:
    global _engine
    if _engine is None:
        _engine = StrategyEngine()
    return _engine
