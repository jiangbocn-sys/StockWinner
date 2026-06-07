"""
条件评估器 — 三种数据源的选股条件评估 + 代码型策略执行。
"""
import asyncio
import re
from datetime import datetime
from typing import Dict, List, Optional, Any

from services.common.database import get_db_manager, get_sync_connection
from services.common.timezone import get_china_time, CHINA_TZ
from services.common.technical_indicators import calculate_indicators_for_screening, calculate_ma, calculate_rsi
from services.common.indicators import TechnicalIndicators
from services.data.local_data_service import get_local_data_service
from services.common.structured_logger import get_logger
from .factor_registry import get_factor_registry, FactorRegistry
from .condition_parser import get_condition_parser, normalize_conditions


def _load_stock_names() -> Dict[str, str]:
    """从 kline.db 的 stock_base_info 表批量加载股票名称"""
    try:
        conn = get_sync_connection("kline")
        cursor = conn.cursor()
        cursor.execute("SELECT stock_code, stock_name FROM stock_base_info")
        return {row[0]: row[1].strip() for row in cursor.fetchall()}
    except Exception:
        return {}


class ConditionEvaluator:
    """选股条件评估：优化 DB 模式 / 本地 DB 模式 / SDK 实时模式 + 代码型策略"""

    def __init__(self, progress: Dict, progress_callback: Optional[callable] = None):
        self._progress = progress
        self._progress_callback = progress_callback  # 进度更新回调函数

    def _notify_progress(self):
        """通知进度更新（通过回调）"""
        if self._progress_callback:
            try:
                self._progress_callback(self._progress)
            except Exception:
                pass

    async def evaluate_optimized(
        self, config: Dict, match_score_threshold: float = 0.5, trade_date: Optional[str] = None
    ) -> List[Dict]:
        """优化版本：优先使用数据库因子，缺失因子动态计算"""
        candidates = []

        # 获取买入条件（支持三种格式）
        buy_conditions = config.get("buy_conditions", [])
        if not buy_conditions:
            buy_conditions = config.get("buy", [])
            if not buy_conditions:
                conditions = config.get("conditions", {})
                buy_conditions = conditions.get("buy", [])

        if not buy_conditions:
            print("[Screening] 警告：策略配置中没有找到买入条件")
            return []

        # 提取需要的指标
        registry = get_factor_registry()
        required_factors = registry.extract_required_factors(buy_conditions)
        print(f"[Screening] 需要的指标：{required_factors}")

        # 分类：数据库因子 vs 需要计算的因子
        db_factors, calc_factors = registry.classify_factors(required_factors)
        print(f"[Screening] 数据库因子：{db_factors}, 需计算：{calc_factors}")

        # 检测可能缺失的 DB 因子（如 MA120, MA250）
        potentially_missing = set()
        for f in db_factors:
            if re.match(r'^MA\d+$', f):
                period = int(re.match(r'^MA(\d+)$', f).group(1))
                if period > 60:
                    potentially_missing.add(f)

        if potentially_missing:
            print(f"[Screening] 可能缺失的 DB 因子（需要 K 线回退）：{potentially_missing}")
            calc_factors |= potentially_missing

        # 获取最新交易日期
        if not trade_date:
            conn = get_sync_connection("kline")
            cursor = conn.cursor()
            cursor.execute("SELECT MAX(trade_date) FROM stock_daily_factors")
            result = cursor.fetchone()
            if result and result[0]:
                trade_date = result[0]
            else:
                local_service = get_local_data_service()
                trade_date = local_service.get_earliest_date("")
                if not trade_date:
                    raise Exception("无法获取最新交易日期")

        print(f"[Screening] 使用交易日期：{trade_date}")

        # 批量获取数据库因子
        factor_df = registry.fetch_db_factors(db_factors, trade_date)
        if factor_df.empty:
            print("[Screening] 数据库因子查询失败，回退到传统模式")
            return await self.evaluate_local(config, match_score_threshold)

        stock_codes = factor_df.index.tolist()
        print(f"[Screening] 从数据库获取到 {len(stock_codes)} 只股票的因子数据")

        # 市场过滤
        markets = config.get("markets")
        if markets:
            stock_codes = [c for c in stock_codes if c.split(".")[-1] in markets]
            print(f"[Screening] 市场过滤后剩余 {len(stock_codes)} 只股票（允许市场: {markets}）")

        # 市值过滤
        stock_filters = config.get("stock_filters", {})
        stock_filters_normalized = normalize_conditions(stock_filters)
        cap_values = self._extract_cap_conditions(stock_filters_normalized)
        stock_codes, factor_df = self._apply_cap_filter(
            stock_codes, factor_df, trade_date, cap_values
        )

        # 批量预加载 K 线数据
        kline_cache: Dict[str, List[Dict]] = {}
        if calc_factors:
            max_period = 60
            for f in calc_factors:
                m = re.match(r'^MA(\d+)$', f)
                if m:
                    max_period = max(max_period, int(m.group(1)))
            print(f"[Screening] 批量预加载 {len(stock_codes)} 只股票的 K 线数据（需要 {max_period} 根）...")
            local_service = get_local_data_service()
            kline_cache = local_service.get_batch_kline(stock_codes, limit=max_period)
            print(f"[Screening] K 线预加载完成，共 {len(kline_cache)} 只股票有数据")

        # 遍历评估
        self._progress["total_stocks"] = len(stock_codes)
        self._progress["processed"] = 0
        self._progress["matched"] = 0
        self._progress["current_phase"] = "scanning"

        matched_count = 0
        stock_name_map = _load_stock_names()
        parser = get_condition_parser()
        buy_conditions_normalized = normalize_conditions(buy_conditions)

        for stock_code in stock_codes:
            self._progress["processed"] += 1
            self._progress["current_stock"] = stock_code

            indicators = factor_df.loc[stock_code].to_dict()

            # 动态计算缺失因子
            if calc_factors:
                kline_data = kline_cache.get(stock_code, [])
                if kline_data and len(kline_data) >= 26:
                    closes = [k['close'] for k in kline_data]
                    highs = [k.get('high', k['close']) for k in kline_data]
                    lows = [k.get('low', k['close']) for k in kline_data]

                    for factor in calc_factors:
                        factor_config = registry.get_factor_config(factor)
                        if factor_config and factor_config.get('source') == 'calc':
                            calculator_name = factor_config.get('calculator')
                            calculator = registry.get_calculator(calculator_name)
                            params = factor_config.get('params', {})
                            if calculator:
                                value = self._call_calculator(calculator_name, calculator, closes, highs, lows, params)
                                if value is not None:
                                    indicators[factor.lower()] = value
                                    indicators[factor] = value
                        elif factor_config is None:
                            ma_match = re.match(r'^MA(\d+)$', factor)
                            if ma_match:
                                period = int(ma_match.group(1))
                                if len(closes) >= period:
                                    value = calculate_ma(closes, period)
                                    if value is not None:
                                        indicators[factor.lower()] = value
                                        indicators[factor] = value
                        elif factor_config.get('source') == 'db':
                            db_value = indicators.get(factor.lower()) or indicators.get(factor)
                            if db_value is None or (isinstance(db_value, float) and str(db_value) == 'nan'):
                                ma_match = re.match(r'^MA(\d+)$', factor)
                                if ma_match:
                                    period = int(ma_match.group(1))
                                    if len(closes) >= period:
                                        value = calculate_ma(closes, period)
                                        if value is not None:
                                            indicators[factor.lower()] = value
                                            indicators[factor] = value
                                            print(f"[Screening] K 线计算 {factor}: {value:.2f}")

            # 评估条件
            is_matched = parser.evaluate(buy_conditions_normalized, indicators)
            matched_condition_names = parser.get_all_conditions(buy_conditions_normalized)
            total_conditions = len(matched_condition_names) if matched_condition_names else 0
            match_score = 1.0 if is_matched and total_conditions > 0 else (0.5 if total_conditions == 0 else 0)

            if is_matched or match_score >= match_score_threshold or not buy_conditions:
                current_price = indicators.get('price', indicators.get('PRICE', 0))
                candidates.append({
                    "stock_code": stock_code,
                    "stock_name": stock_name_map.get(stock_code, stock_code),
                    "reason": parser.format_condition(buy_conditions_normalized) if is_matched and total_conditions > 0 else "基础筛选",
                    "current_price": current_price,
                    "match_score": match_score
                })
                if is_matched:
                    matched_count += 1

            # 预估剩余时间
            if self._progress["processed"] % 100 == 0:
                self._update_eta()
                self._notify_progress()  # 每100只股票通知一次进度

        print(f"[Screening] 优化模式筛选完成，共匹配 {matched_count}/{len(stock_codes)} 只股票 (匹配度阈值：{match_score_threshold*100:.0f}%)")
        return candidates

    async def evaluate_local(self, config: Dict, match_score_threshold: float = 0.5) -> List[Dict]:
        """传统本地 K 线数据库模式"""
        candidates = []
        local_service = get_local_data_service()
        stock_codes = local_service.get_all_stocks()
        if not stock_codes:
            raise Exception("本地数据库为空，请先下载 K 线数据")

        markets = config.get("markets")
        if markets:
            stock_codes = [c for c in stock_codes if c.split(".")[-1] in markets]
            print(f"[Screening] 市场过滤后剩余 {len(stock_codes)} 只股票（允许市场: {markets}）")

        print(f"[Screening] 从本地数据库获取到 {len(stock_codes)} 只股票")
        self._progress["total_stocks"] = len(stock_codes)
        self._progress["processed"] = 0
        self._progress["matched"] = 0

        buy_conditions = config.get("buy_conditions", [])
        if not buy_conditions:
            buy_conditions = config.get("buy", [])
            if not buy_conditions:
                conditions = config.get("conditions", {})
                buy_conditions = conditions.get("buy", [])

        if not buy_conditions:
            print("[Screening] 警告：策略配置中没有找到买入条件")

        matched_count = 0
        stock_name_map = _load_stock_names()
        parser = get_condition_parser()
        buy_conditions_normalized = normalize_conditions(buy_conditions)

        for stock_code in stock_codes:
            self._progress["processed"] += 1
            self._progress["current_stock"] = stock_code

            if self._progress["processed"] % 100 == 0:
                self._update_eta()

            kline_data = local_service.get_kline_data(stock_code, limit=60)
            if not kline_data or len(kline_data) < 26:
                continue

            closes = [k['close'] for k in kline_data]
            highs = [k.get('high', k['close']) for k in kline_data]
            lows = [k.get('low', k['close']) for k in kline_data]
            volumes = [k.get('volume', 0) for k in kline_data]
            indicators = calculate_indicators_for_screening(closes, highs, lows, volumes)

            if not indicators.get('ma5') or not indicators.get('ma20') or not indicators.get('rsi'):
                continue

            current_price = indicators.get('price', 0)
            try:
                from services.common.price_cache import get_price_cache
                cached_price = get_price_cache().get(stock_code)
                if cached_price and cached_price > 0:
                    current_price = cached_price
                    indicators['price'] = current_price
                else:
                    from services.trading.gateway import get_gateway
                    gateway = await get_gateway()
                    if gateway and gateway.connected:
                        market_data = await gateway.get_market_data(stock_code)
                        if market_data and market_data.current_price:
                            current_price = market_data.current_price
                            indicators['price'] = current_price
            except Exception:
                pass

            is_matched = parser.evaluate(buy_conditions_normalized, indicators)
            match_score = 1.0 if is_matched else 0

            if is_matched or match_score >= match_score_threshold or not buy_conditions:
                candidates.append({
                    "stock_code": stock_code,
                    "stock_name": stock_name_map.get(stock_code, stock_code),
                    "reason": parser.format_condition(buy_conditions_normalized) if is_matched else "基础筛选",
                    "current_price": current_price,
                    "match_score": match_score
                })
                if is_matched:
                    matched_count += 1

        print(f"[Screening] 本地筛选完成，共匹配 {matched_count}/{len(stock_codes)} 只股票 (匹配度阈值：{match_score_threshold*100:.0f}%)")
        get_logger("screening").log_event("screening_complete",
            f"选股筛选完成，匹配 {matched_count}/{len(stock_codes)} 只股票",
            matched_count=matched_count, total_stocks=len(stock_codes),
            match_score_threshold=match_score_threshold)
        return candidates

    async def evaluate_sdk(self, config: Dict, match_score_threshold: float = 0.5) -> List[Dict]:
        """SDK 实时数据源模式"""
        candidates = []

        try:
            from services.trading.gateway import get_gateway
            gateway = await get_gateway()
            if not gateway or not gateway.connected:
                raise Exception("交易网关未连接")
        except Exception as e:
            raise Exception(f"创建交易网关失败：{str(e)}")

        try:
            stock_list = await gateway.get_stock_list()
            if not stock_list or len(stock_list) == 0:
                raise Exception("交易网关返回空股票列表，请检查 SDK 连接状态和券商 credentials 配置")
            print(f"[Screening] 获取到 {len(stock_list)} 只股票")
            get_logger("screening").log_event("screening_stock_list",
                f"获取到 {len(stock_list)} 只股票", stock_count=len(stock_list))
            self._progress["total_stocks"] = len(stock_list)
            self._progress["processed"] = 0
            self._progress["matched"] = 0

            markets = config.get("markets")
            if markets:
                stock_list = [s for s in stock_list if s.get("code", "").split(".")[-1] in markets]
                print(f"[Screening] 市场过滤后剩余 {len(stock_list)} 只股票（允许市场: {markets}）")
                self._progress["total_stocks"] = len(stock_list)
        except Exception as e:
            raise Exception(f"获取股票列表失败：{str(e)} - SDK 可能未安装或连接失败")

        buy_conditions = config.get("buy_conditions", [])
        if not buy_conditions:
            buy_conditions = config.get("buy", [])
            if not buy_conditions:
                conditions = config.get("conditions", {})
                buy_conditions = conditions.get("buy", [])

        if not buy_conditions:
            print("[Screening] 警告：策略配置中没有找到买入条件")

        success_count = 0
        fail_count = 0
        parser = get_condition_parser()
        buy_conditions_normalized = normalize_conditions(buy_conditions)

        for stock in stock_list:
            stock_code = stock.get("code", "")
            stock_name = stock.get("name", "")

            self._progress["processed"] += 1
            self._progress["current_stock"] = stock_code

            if self._progress["processed"] % 100 == 0:
                self._update_eta()

            # 获取行情数据
            try:
                from services.common.price_cache import get_price_cache
                cached_ohlcv = get_price_cache().get_ohlcv(stock_code)
                if cached_ohlcv and cached_ohlcv.get('close', 0) > 0:
                    market_data = cached_ohlcv
                    success_count += 1
                else:
                    market_data = await gateway.get_market_data(stock_code)
                    if not market_data:
                        fail_count += 1
                        continue
                    success_count += 1
            except Exception:
                fail_count += 1
                continue

            # 获取 K 线历史数据
            try:
                kline_data = await gateway.get_kline_data(stock_code, period="day", limit=60)
                if not kline_data or len(kline_data) == 0:
                    fail_count += 1
                    continue
                closes = [k['close'] for k in kline_data]
                highs = [k.get('high', k['close']) for k in kline_data]
                lows = [k.get('low', k['close']) for k in kline_data]
                volumes = [k.get('volume', 0) for k in kline_data]
            except Exception:
                fail_count += 1
                continue

            indicators = calculate_indicators_for_screening(closes, highs, lows, volumes)

            if not indicators.get('ma5') or not indicators.get('ma20') or not indicators.get('rsi'):
                fail_count += 1
                continue

            if isinstance(market_data, dict):
                indicators['price'] = market_data.get('close', 0)
                indicators['volume'] = market_data.get('volume', 0)
            else:
                indicators['price'] = market_data.current_price
                indicators['volume'] = market_data.volume

            is_matched = parser.evaluate(buy_conditions_normalized, indicators)
            match_score = 1.0 if is_matched else 0
            condition_str = parser.format_condition(buy_conditions_normalized)
            print(f"[Screening] {stock_code}: {is_matched} - {condition_str}")

            if is_matched or match_score >= match_score_threshold or not buy_conditions:
                candidates.append({
                    "stock_code": f"{stock_code}",
                    "stock_name": stock_name,
                    "reason": condition_str if is_matched else "基础筛选",
                    "current_price": market_data.current_price,
                    "match_score": match_score,
                    "condition_results": is_matched
                })
                self._progress["matched"] += 1

        print(f"[Screening] 成功获取 {success_count} 只股票行情，失败 {fail_count} 只 (匹配度阈值：{match_score_threshold*100:.0f}%)")
        return candidates

    async def execute_python_strategy(
        self, account_id: str, strategy: Dict, stock_scope: str, pending_to_temp: bool,
        candidate_manager=None
    ):
        """执行代码型策略"""
        from services.strategy.engine import get_strategy_engine

        db = get_db_manager()

        # 构建股票列表
        group_id = None
        if stock_scope == 'market':
            stocks = await self._get_market_stock_list()
            print(f"[Screening] 代码策略选股范围：全市场，共 {len(stocks)} 只股票")
        elif stock_scope == 'group':
            group_id = await candidate_manager.ensure_candidate_group(
                account_id, strategy.get('id'), strategy.get('name')
            )
            stocks = await db.fetchall(
                "SELECT * FROM watchlist WHERE account_id = ? AND group_id = ? AND status IN ('pending', 'watching')",
                (account_id, group_id)
            )
            print(f"[Screening] 代码策略选股范围：候选组 #{group_id}，共 {len(stocks)} 只股票")
        else:
            print(f"[Screening] 不支持的选股范围: {stock_scope}")
            return

        if not stocks:
            print(f"[Screening] 股票列表为空 ({stock_scope})，跳过策略执行")
            return

        from services.strategy.engine import build_strategy_context

        context = build_strategy_context(
            stocks, account_id,
            include_realtime=True,
            strategy=strategy,
        )

        engine = get_strategy_engine()
        signals = engine.execute_strategy(strategy, context)
        print(f"[Screening] 策略 '{strategy['name']}' 返回 {len(signals)} 个信号")

        if not group_id:
            group_id = await candidate_manager.ensure_candidate_group(
                account_id, strategy['id'], strategy.get('name', '')
            )

        if pending_to_temp:
            for signal in signals:
                await candidate_manager.add_to_temp_candidates(
                    account_id, strategy['id'], {
                        'stock_code': signal['stock_code'],
                        'stock_name': signal['stock_name'],
                        'reason': signal.get('reason', ''),
                        'current_price': signal.get('trigger_price') or 0,
                        'match_score': 1.0,
                    },
                    strategy_config={},
                    group_id=group_id,
                )
        else:
            await engine.write_signals_to_watchlist(
                signals, account_id, strategy['id'], group_id,
                strategy_name=strategy.get('name', ''),
            )

    async def _get_market_stock_list(self) -> list:
        """获取全市场股票列表（从 stock_daily_factors 表）"""
        conn = get_sync_connection("kline")
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(trade_date) FROM stock_daily_factors")
        latest_date = cursor.fetchone()[0]
        if not latest_date:
            return []
        cursor.execute(
            "SELECT DISTINCT stock_code FROM stock_daily_factors WHERE trade_date = ?",
            (latest_date,)
        )
        codes = [row[0] for row in cursor.fetchall()]
        stock_name_map = _load_stock_names()
        return [{"stock_code": c, "stock_name": stock_name_map.get(c, c)} for c in codes]

    @staticmethod
    def _extract_cap_conditions(condition_node) -> Dict:
        """递归提取市值条件"""
        cap_fields = {
            'total_market_cap_max', 'total_market_cap_min',
            'circ_market_cap_max', 'circ_market_cap_min'
        }
        result = {}
        if isinstance(condition_node, dict):
            if 'field' in condition_node:
                field = condition_node.get('field', '')
                if field in cap_fields:
                    result[field] = condition_node.get('value')
            elif 'conditions' in condition_node:
                for c in condition_node['conditions']:
                    result.update(ConditionEvaluator._extract_cap_conditions(c))
        elif isinstance(condition_node, list):
            for c in condition_node:
                result.update(ConditionEvaluator._extract_cap_conditions(c))
        return result

    @staticmethod
    def _apply_cap_filter(stock_codes, factor_df, trade_date, cap_values):
        """应用市值过滤"""
        circ_cap_max = cap_values.get('circ_market_cap_max')
        circ_cap_min = cap_values.get('circ_market_cap_min')
        total_cap_max = cap_values.get('total_market_cap_max')
        total_cap_min = cap_values.get('total_market_cap_min')

        if not (circ_cap_max or circ_cap_min or total_cap_max or total_cap_min):
            return stock_codes, factor_df

        conn = get_sync_connection("kline")
        cursor = conn.cursor()

        cap_conditions = []
        cap_params = []
        if circ_cap_max:
            cap_conditions.append("circ_market_cap <= ?")
            cap_params.append(circ_cap_max)
        if circ_cap_min:
            cap_conditions.append("circ_market_cap >= ?")
            cap_params.append(circ_cap_min)
        if total_cap_max:
            cap_conditions.append("total_market_cap <= ?")
            cap_params.append(total_cap_max)
        if total_cap_min:
            cap_conditions.append("total_market_cap >= ?")
            cap_params.append(total_cap_min)

        if cap_conditions:
            cap_sql = f"""
                SELECT stock_code FROM stock_daily_factors
                WHERE trade_date = ? AND ({' AND '.join(cap_conditions)})
            """
            cap_params.insert(0, trade_date)
            cursor.execute(cap_sql, cap_params)
            cap_filtered_codes = set(row[0] for row in cursor.fetchall())
            original_count = len(stock_codes)
            stock_codes = [c for c in stock_codes if c in cap_filtered_codes]
            factor_df = factor_df.loc[stock_codes]
            print(f"[Screening] 市值过滤：{original_count} → {len(stock_codes)} 只股票")

        return stock_codes, factor_df

    @staticmethod
    def _call_calculator(calculator_name, calculator, closes, highs, lows, params):
        """调用因子计算器"""
        if calculator_name in ('calculate_rsi', 'calculate_ma', 'calculate_ema'):
            return calculator(closes, **params) if params else calculator(closes)
        elif calculator_name in ('calculate_kdj_k', 'calculate_kdj_d', 'calculate_kdj_j'):
            return calculator(closes, highs=highs, lows=lows)
        elif calculator_name == 'calculate_kdj':
            return calculator(highs, lows, closes)
        else:
            return calculator(closes, **params) if params else calculator(closes)

    def _update_eta(self):
        """更新预估剩余时间"""
        if self._progress["start_time"]:
            elapsed = (get_china_time() - datetime.fromisoformat(self._progress["start_time"])).total_seconds()
            avg_time_per_stock = elapsed / self._progress["processed"]
            remaining_stocks = self._progress["total_stocks"] - self._progress["processed"]
            self._progress["estimated_remaining"] = int(remaining_stocks * avg_time_per_stock)
