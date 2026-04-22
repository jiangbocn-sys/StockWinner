"""
选股模块 (Stock Screening)
根据策略条件扫描股票市场，筛选符合条件的股票

优化版本 (v2):
- 优先使用 stock_daily_factors 表中的预计算因子
- 只对缺失因子进行动态计算
- 支持可扩展的因子注册表
"""

import asyncio
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any
from pathlib import Path
from services.common.database import get_db_manager
from services.common.account_manager import get_account_manager
from services.common.timezone import get_china_time, CHINA_TZ
from services.common.technical_indicators import calculate_indicators_for_screening, calculate_rsi
from services.common.indicators import TechnicalIndicators
from services.data.local_data_service import get_local_data_service
from .factor_registry import get_factor_registry, FactorRegistry

# 数据库路径
KLINE_DB = Path(__file__).parent.parent.parent / "data" / "kline.db"


class ScreeningService:
    """选股服务"""

    def __init__(self):
        self._running = False
        self._task = None
        # 进度追踪
        self._progress = {
            "total_stocks": 0,
            "processed": 0,
            "matched": 0,
            "current_phase": "idle",  # idle, fetching_list, scanning, done
            "current_stock": None,
            "start_time": None,
            "estimated_remaining": None
        }

    async def start_screening(
        self,
        account_id: str,
        strategy_id: Optional[int] = None,
        interval: int = 60
    ):
        """
        启动选股服务

        Args:
            account_id: 账户 ID
            strategy_id: 策略 ID（可选，不传则使用所有激活的策略）
            interval: 扫描间隔（秒）
        """
        if self._running:
            return {"success": False, "message": "选股服务已在运行"}

        self._running = True
        self._task = asyncio.create_task(
            self._run_screening_loop(account_id, strategy_id, interval)
        )
        return {"success": True, "message": "选股服务已启动"}

    async def stop_screening(self):
        """停止选股服务"""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        return {"success": True, "message": "选股服务已停止"}

    async def _run_screening_loop(
        self,
        account_id: str,
        strategy_id: Optional[int],
        interval: int
    ):
        """选股扫描循环"""
        print(f"[Screening] 启动选股服务 - 账户：{account_id}, 间隔：{interval}s")

        while self._running:
            try:
                await self._run_screening(account_id, strategy_id)
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"[Screening] 错误：{e}")
                await asyncio.sleep(interval)

        print(f"[Screening] 选股服务已停止")

    async def _run_screening(
        self,
        account_id: str,
        strategy_id: Optional[int],
        use_local: bool = True,
        pending_to_temp: bool = False,  # 新参数：是否暂存到临时表
        require_active: bool = True     # 是否要求策略必须是 active 状态
    ):
        """执行一次选股扫描

        Args:
            account_id: 账户 ID
            strategy_id: 策略 ID
            use_local: 是否使用本地数据源（默认 True，速度更快）
            pending_to_temp: 是否暂存到临时表待确认（默认 False）
            require_active: 是否要求策略必须是 active 状态（默认 True，手动执行时可为 False）
        """
        db = get_db_manager()
        account_manager = get_account_manager()

        if not await account_manager.validate_account(account_id):
            return

        # 清除旧的临时候选数据（避免与上次选股结果混淆）
        if pending_to_temp:
            await db.execute("DELETE FROM temp_candidates WHERE account_id = ?", (account_id,))

        # 重置进度
        self.reset_progress()
        self._progress["start_time"] = get_china_time().isoformat()
        self._progress["current_phase"] = "fetching_list"

        # 获取策略列表
        if strategy_id:
            if require_active:
                # 只获取 active 状态的策略
                strategies = [await db.fetchone(
                    "SELECT * FROM strategies WHERE id = ? AND account_id = ? AND status = 'active'",
                    (strategy_id, account_id)
                )]
            else:
                # 获取任意状态的策略（允许测试 draft/inactive 策略）
                strategies = [await db.fetchone(
                    "SELECT * FROM strategies WHERE id = ? AND account_id = ?",
                    (strategy_id, account_id)
                )]
        else:
            # 不指定 strategy_id 时，只扫描 active 状态的策略
            strategies = await db.fetchall(
                "SELECT * FROM strategies WHERE account_id = ? AND status = 'active'",
                (account_id,)
            )

        # 问题修复：多策略场景下，每个策略独立处理，progress 在每次选股开始时重置
        total_strategies = len([s for s in strategies if s])

        for strategy_idx, strategy in enumerate(strategies):
            if not strategy:
                continue

            print(f"[Screening] 执行策略 {strategy_idx + 1}/{total_strategies}: {strategy.get('name')}")

            # 解析策略配置
            config = self._parse_config(strategy.get('config'))
            if not config:
                continue

            # 获取匹配度阈值（从数据库字段或 config 中读取，默认 50%）
            match_score_threshold = strategy.get('match_score_threshold', 0.5)
            if match_score_threshold is None:
                match_score_threshold = config.get('match_score_threshold', 0.5)

            # 执行选股条件（优先使用优化版本）
            self._progress["current_phase"] = "scanning"

            # 根据参数选择数据源
            if use_local:
                print(f"[Screening] 使用本地数据源进行筛选... (匹配度阈值：{match_score_threshold*100:.0f}%)")
                # 尝试使用优化版本
                try:
                    candidates = await self._evaluate_conditions_from_local_optimized(
                        config, match_score_threshold
                    )
                except Exception as e:
                    print(f"[Screening] 优化模式失败，回退到传统模式：{e}")
                    candidates = await self._evaluate_conditions_from_local(config, match_score_threshold)
            else:
                print(f"[Screening] 使用 SDK 实时数据源进行筛选... (匹配度阈值：{match_score_threshold*100:.0f}%)")
                candidates = await self._evaluate_conditions(config, match_score_threshold)

            # 将候选股票加入 watchlist 或临时表
            for candidate in candidates:
                if pending_to_temp:
                    # 暂存到临时表待确认
                    await self._add_to_temp_candidates(
                        account_id,
                        strategy.get('id'),
                        candidate,
                        config
                    )
                else:
                    # 直接加入 watchlist
                    await self._add_to_watchlist(
                        account_id,
                        strategy.get('id'),
                        candidate,
                        config
                    )

        self._progress["current_phase"] = "done"

    def _parse_config(self, config) -> Dict:
        """解析策略配置"""
        import json
        if not config:
            return {}
        if isinstance(config, str):
            try:
                return json.loads(config)
            except:
                return {}
        return config

    async def _evaluate_conditions_from_local_optimized(
        self,
        config: Dict,
        match_score_threshold: float = 0.5,
        trade_date: Optional[str] = None
    ) -> List[Dict]:
        """
        评估选股条件 - 优化版本（优先使用数据库因子）

        优化点：
        1. 优先从 stock_daily_factors 表读取预计算因子
        2. 只对缺失因子进行动态计算
        3. 批量查询，减少循环次数

        Args:
            config: 策略配置
            match_score_threshold: 匹配度阈值
            trade_date: 交易日期（默认使用最新可用日期）

        Returns:
            候选股票列表
        """
        candidates = []

        # 1. 获取买入条件
        buy_conditions = config.get("buy", [])
        if not buy_conditions:
            conditions = config.get("conditions", {})
            buy_conditions = conditions.get("buy", [])

        if not buy_conditions:
            print("[Screening] 警告：策略配置中没有找到买入条件")
            return []

        # 2. 从条件中提取需要的指标
        registry = get_factor_registry()
        required_factors = registry.extract_required_factors(buy_conditions)
        print(f"[Screening] 需要的指标：{required_factors}")

        # 3. 分类：数据库因子 vs 需要计算的因子
        db_factors, calc_factors = registry.classify_factors(required_factors)
        print(f"[Screening] 数据库因子：{db_factors}, 需计算：{calc_factors}")

        # 4. 获取最新交易日期
        if not trade_date:
            # 从 stock_daily_factors 表获取最新日期
            import sqlite3
            conn = sqlite3.connect(str(KLINE_DB), timeout=60)
            cursor = conn.cursor()
            cursor.execute("PRAGMA busy_timeout=60000")
            cursor.execute("SELECT MAX(trade_date) FROM stock_daily_factors")
            result = cursor.fetchone()
            conn.close()
            if result and result[0]:
                trade_date = result[0]
            else:
                # 回退到从 kline_data 获取
                local_service = get_local_data_service()
                trade_date = local_service.get_earliest_date("")
                if not trade_date:
                    raise Exception("无法获取最新交易日期")

        print(f"[Screening] 使用交易日期：{trade_date}")

        # 5. 批量获取数据库因子
        factor_df = registry.fetch_db_factors(db_factors, trade_date)
        if factor_df.empty:
            print("[Screening] 数据库因子查询失败，回退到传统模式")
            return await self._evaluate_conditions_from_local(config, match_score_threshold)

        stock_codes = factor_df.index.tolist()
        print(f"[Screening] 从数据库获取到 {len(stock_codes)} 只股票的因子数据")

        # 5.1 市值过滤（在因子筛选之前）
        stock_filters = config.get("stock_filters", {})
        circ_cap_max = stock_filters.get("circ_market_cap_max")
        circ_cap_min = stock_filters.get("circ_market_cap_min")
        total_cap_max = stock_filters.get("total_market_cap_max")
        total_cap_min = stock_filters.get("total_market_cap_min")

        if circ_cap_max or circ_cap_min or total_cap_max or total_cap_min:
            import sqlite3
            conn = sqlite3.connect(str(KLINE_DB), timeout=60)
            cursor = conn.cursor()
            cursor.execute("PRAGMA busy_timeout=60000")

            # 构建市值过滤SQL
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
                cap_filtered_codes = [row[0] for row in cursor.fetchall()]

                # 只保留市值符合条件的股票
                original_count = len(stock_codes)
                stock_codes = [c for c in stock_codes if c in cap_filtered_codes]
                factor_df = factor_df.loc[stock_codes]
                print(f"[Screening] 市值过滤：{original_count} → {len(stock_codes)} 只股票")

            conn.close()

        # 6. 获取 K 线数据（用于动态计算缺失因子）
        local_service = get_local_data_service()
        kline_cache = {}  # 缓存 K 线数据

        if calc_factors:
            # 只需要为需要计算的因子获取 K 线数据
            print(f"[Screening] 需要计算缺失因子：{calc_factors}")

        # 7. 遍历股票，评估条件
        self._progress["total_stocks"] = len(stock_codes)
        self._progress["processed"] = 0
        self._progress["matched"] = 0
        self._progress["current_phase"] = "scanning"

        matched_count = 0
        for stock_code in stock_codes:
            # 更新进度
            self._progress["processed"] += 1
            self._progress["current_stock"] = stock_code

            # 获取数据库因子值
            indicators = factor_df.loc[stock_code].to_dict()

            # 动态计算缺失因子
            if calc_factors:
                # 获取 K 线数据（缓存）
                if stock_code not in kline_cache:
                    kline_data = local_service.get_kline_data(stock_code, limit=60)
                    kline_cache[stock_code] = kline_data
                else:
                    kline_data = kline_cache[stock_code]

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
                                # 根据计算器类型调用
                                if calculator_name in ('calculate_rsi', 'calculate_ma', 'calculate_ema'):
                                    value = calculator(closes, **params) if params else calculator(closes)
                                elif calculator_name in ('calculate_kdj_k', 'calculate_kdj_d', 'calculate_kdj_j'):
                                    # KDJ 需要 high/low 数据
                                    value = calculator(closes, highs=highs, lows=lows)
                                elif calculator_name == 'calculate_kdj':
                                    value = calculator(highs, lows, closes)
                                else:
                                    value = calculator(closes, **params) if params else calculator(closes)

                                if value is not None:
                                    # 因子值映射到正确的键名
                                    indicators[factor.lower()] = value
                                    # 同时添加大写键名以兼容条件检查
                                    indicators[factor] = value

            # 检查条件
            matched_conditions = []
            for condition in buy_conditions:
                if TechnicalIndicators.check_condition(condition, indicators):
                    matched_conditions.append(condition)

            # 计算匹配度
            match_score = len(matched_conditions) / max(len(buy_conditions), 1) if buy_conditions else 0.5

            # 如果匹配度达到阈值或以上，加入候选列表
            if match_score >= match_score_threshold or not buy_conditions:
                current_price = indicators.get('price', indicators.get('PRICE', 0))
                candidates.append({
                    "stock_code": stock_code,
                    "stock_name": stock_code,
                    "reason": ", ".join(matched_conditions) if matched_conditions else "基础筛选",
                    "current_price": current_price,
                    "match_score": match_score
                })
                if matched_conditions:
                    matched_count += 1

            # 每 100 只股票计算预估剩余时间
            if self._progress["processed"] % 100 == 0:
                if self._progress["start_time"]:
                    elapsed = (get_china_time() - datetime.fromisoformat(self._progress["start_time"])).total_seconds()
                    avg_time_per_stock = elapsed / self._progress["processed"]
                    remaining_stocks = self._progress["total_stocks"] - self._progress["processed"]
                    self._progress["estimated_remaining"] = int(remaining_stocks * avg_time_per_stock)

        print(f"[Screening] 优化模式筛选完成，共匹配 {matched_count}/{len(stock_codes)} 只股票 (匹配度阈值：{match_score_threshold*100:.0f}%)")
        return candidates

    async def _evaluate_conditions_from_local(self, config: Dict, match_score_threshold: float = 0.5) -> List[Dict]:
        """
        评估选股条件 - 使用本地 K 线数据库
        
        优势：
        - 无需调用 SDK API，速度快
        - 无并发限制
        - 数据可重复使用
        """
        candidates = []
        
        # 获取本地数据服务
        local_service = get_local_data_service()
        
        # 获取股票列表
        stock_codes = local_service.get_all_stocks()
        if not stock_codes:
            raise Exception("本地数据库为空，请先下载 K 线数据")
        
        print(f"[Screening] 从本地数据库获取到 {len(stock_codes)} 只股票")
        self._progress["total_stocks"] = len(stock_codes)
        self._progress["processed"] = 0
        self._progress["matched"] = 0
        
        # 获取买入条件
        buy_conditions = config.get("buy", [])
        if not buy_conditions:
            conditions = config.get("conditions", {})
            buy_conditions = conditions.get("buy", [])
        
        if not buy_conditions:
            print("[Screening] 警告：策略配置中没有找到买入条件")
        
        # 遍历股票列表
        matched_count = 0
        for stock_code in stock_codes:
            # 更新进度
            self._progress["processed"] += 1
            self._progress["current_stock"] = stock_code
            
            # 每处理 100 只股票，计算预估剩余时间
            if self._progress["processed"] % 100 == 0:
                elapsed = (get_china_time() - datetime.fromisoformat(self._progress["start_time"])).total_seconds() if self._progress["start_time"] else 0
                avg_time_per_stock = elapsed / self._progress["processed"] if self._progress["processed"] > 0 else 0
                remaining_stocks = self._progress["total_stocks"] - self._progress["processed"]
                self._progress["estimated_remaining"] = int(remaining_stocks * avg_time_per_stock)
            
            # 从本地获取 K 线数据（60 条）
            kline_data = local_service.get_kline_data(stock_code, limit=60)
            if not kline_data or len(kline_data) < 26:
                continue

            # 构建指标字典 - 使用统一技术指标模块
            closes = [k['close'] for k in kline_data]
            highs = [k.get('high', k['close']) for k in kline_data]
            lows = [k.get('low', k['close']) for k in kline_data]
            volumes = [k.get('volume', 0) for k in kline_data]

            # 使用统一模块计算技术指标
            indicators = calculate_indicators_for_screening(closes, highs, lows, volumes)

            if not indicators.get('ma5') or not indicators.get('ma20') or not indicators.get('rsi'):
                continue

            # 获取最新价格 - 优先使用网关实时价格
            current_price = indicators.get('price', 0)
            gateway = None
            try:
                from services.trading.gateway import get_gateway
                gateway = await get_gateway()
            except Exception:
                pass

            if gateway and gateway.connected:
                try:
                    market_data = await gateway.get_market_data(stock_code)
                    if market_data and market_data.current_price:
                        current_price = market_data.current_price
                        indicators['price'] = current_price  # 更新指标中的价格
                except Exception:
                    pass  # 获取实时价格失败，使用本地收盘价

            # 检查条件
            matched_conditions = []
            for condition in buy_conditions:
                if TechnicalIndicators.check_condition(condition, indicators):
                    matched_conditions.append(condition)
            
            # 计算匹配度
            match_score = len(matched_conditions) / max(len(buy_conditions), 1) if buy_conditions else 0.5

            # 如果匹配度达到阈值或以上，加入候选列表
            if match_score >= match_score_threshold or not buy_conditions:
                candidates.append({
                    "stock_code": stock_code,
                    "stock_name": stock_code,  # 本地数据暂无名称
                    "reason": ", ".join(matched_conditions) if matched_conditions else "基础筛选",
                    "current_price": current_price,
                    "match_score": match_score
                })
                if matched_conditions:
                    matched_count += 1

        print(f"[Screening] 本地筛选完成，共匹配 {matched_count}/{len(stock_codes)} 只股票 (匹配度阈值：{match_score_threshold*100:.0f}%)")
        return candidates

    async def _evaluate_conditions(self, config: Dict, match_score_threshold: float = 0.5) -> List[Dict]:
        """
        评估选股条件 - 使用 SDK 实时数据源
        """
        candidates = []
        errors = []

        # 获取交易网关 - 强制使用真实数据
        try:
            from services.trading.gateway import get_gateway
            gateway = await get_gateway()
            if not gateway or not gateway.connected:
                raise Exception("交易网关未连接")
        except Exception as e:
            raise Exception(f"创建交易网关失败：{str(e)}")

        # 获取股票列表 - 必须成功
        try:
            stock_list = await gateway.get_stock_list()
            if not stock_list or len(stock_list) == 0:
                raise Exception("交易网关返回空股票列表，请检查 SDK 连接状态和券商 credentials 配置")
            print(f"[Screening] 获取到 {len(stock_list)} 只股票")
            # 设置进度
            self._progress["total_stocks"] = len(stock_list)
            self._progress["processed"] = 0
            self._progress["matched"] = 0
        except Exception as e:
            raise Exception(f"获取股票列表失败：{str(e)} - SDK 可能未安装或连接失败")

        # 获取买入条件 - 支持两种配置格式
        # 格式 1: {"buy": [...]} (旧格式)
        # 格式 2: {"conditions": {"buy": [...]}} (新格式)
        buy_conditions = config.get("buy", [])
        if not buy_conditions:
            conditions = config.get("conditions", {})
            buy_conditions = conditions.get("buy", [])

        if not buy_conditions:
            print("[Screening] 警告：策略配置中没有找到买入条件")

        # 遍历股票列表
        success_count = 0
        fail_count = 0
        for stock in stock_list:
            stock_code = stock.get("code", "")
            stock_name = stock.get("name", "")

            # 更新进度
            self._progress["processed"] += 1
            self._progress["current_stock"] = stock_code

            # 每处理 100 只股票，计算预估剩余时间
            if self._progress["processed"] % 100 == 0:
                elapsed = (get_china_time() - datetime.fromisoformat(self._progress["start_time"])).total_seconds() if self._progress["start_time"] else 0
                avg_time_per_stock = elapsed / self._progress["processed"] if self._progress["processed"] > 0 else 0
                remaining_stocks = self._progress["total_stocks"] - self._progress["processed"]
                self._progress["estimated_remaining"] = int(remaining_stocks * avg_time_per_stock)  # 秒

            # 获取行情数据
            try:
                market_data = await gateway.get_market_data(stock_code)
                if not market_data:
                    fail_count += 1
                    continue
                success_count += 1
            except Exception as e:
                errors.append(f"{stock_code}: {str(e)}")
                fail_count += 1
                continue

            # 从网关获取 K 线历史数据（至少 60 条，以满足 MACD 等指标计算需求）
            try:
                kline_data = await gateway.get_kline_data(stock_code, period="day", limit=60)
                if not kline_data or len(kline_data) == 0:
                    errors.append(f"{stock_code}: 无 K 线历史数据")
                    fail_count += 1
                    continue
                closes = [k['close'] for k in kline_data]
                highs = [k.get('high', k['close']) for k in kline_data]
                lows = [k.get('low', k['close']) for k in kline_data]
                volumes = [k.get('volume', 0) for k in kline_data]
            except Exception as kline_err:
                errors.append(f"{stock_code}: 获取 K 线失败 - {str(kline_err)}")
                fail_count += 1
                continue

            # 使用统一模块计算技术指标
            indicators = calculate_indicators_for_screening(closes, highs, lows, volumes)

            if not indicators.get('ma5') or not indicators.get('ma20') or not indicators.get('rsi'):
                errors.append(f"{stock_code}: 技术指标计算失败")
                fail_count += 1
                continue

            # 更新价格和成交量
            indicators['price'] = market_data.current_price
            indicators['volume'] = market_data.volume

            # 检查条件 - 记录每个条件的匹配情况
            matched_conditions = []
            unmatched_conditions = []
            condition_results = []

            for condition in buy_conditions:
                is_matched = TechnicalIndicators.check_condition(condition, indicators)
                condition_results.append({
                    "condition": condition,
                    "matched": is_matched
                })
                if is_matched:
                    matched_conditions.append(condition)
                else:
                    unmatched_conditions.append(condition)

            # 计算匹配度
            match_score = len(matched_conditions) / max(len(buy_conditions), 1) if buy_conditions else 0.5

            # 记录每只股票的条件匹配详情
            print(f"[Screening] {stock_code}: 匹配 {len(matched_conditions)}/{len(buy_conditions)} "
                  f"({match_score*100:.0f}%) - 已匹配：{matched_conditions if matched_conditions else '无'}")
            if unmatched_conditions:
                print(f"  未匹配：{unmatched_conditions}")

            # 如果匹配度达到阈值或以上，加入候选列表
            if match_score >= match_score_threshold or not buy_conditions:
                candidates.append({
                    "stock_code": f"{stock_code}",
                    "stock_name": stock_name,
                    "reason": ", ".join(matched_conditions) if matched_conditions else "基础筛选",
                    "current_price": market_data.current_price,
                    "match_score": match_score,
                    "condition_results": condition_results  # 保存详细的条件匹配结果
                })
                self._progress["matched"] += 1

        # 记录统计信息
        print(f"[Screening] 成功获取 {success_count} 只股票行情，失败 {fail_count} 只 (匹配度阈值：{match_score_threshold*100:.0f}%)")
        if errors:
            print(f"[Screening] 错误列表：{errors[:5]}...")

        return candidates

    async def _add_to_watchlist(
        self,
        account_id: str,
        strategy_id: int,
        candidate: Dict,
        strategy_config: Optional[Dict] = None
    ):
        """
        将候选股票添加到 watchlist

        根据策略配置计算：
        - 买入价格（当前价）
        - 止损价（默认 -5%）
        - 止盈价（默认 +15%）
        - 目标数量（根据策略配置和账户资金）
        """
        db = get_db_manager()

        # 检查是否已存在
        existing = await db.fetchone(
            "SELECT id FROM watchlist WHERE account_id = ? AND stock_code = ? AND status = 'pending'",
            (account_id, candidate['stock_code'])
        )

        if existing:
            return  # 已存在，跳过

        # 获取当前价格
        current_price = candidate.get('current_price', 0)

        # 从策略配置读取止损止盈比例，或使用默认值
        stop_loss_pct = strategy_config.get('stop_loss_pct', 0.05) if strategy_config else 0.05
        take_profit_pct = strategy_config.get('take_profit_pct', 0.15) if strategy_config else 0.15

        stop_loss = current_price * (1 - stop_loss_pct)
        take_profit = current_price * (1 + take_profit_pct)

        # 计算目标买入数量
        # 1. 从策略配置读取固定数量
        # 2. 或从策略配置读取买入比例（占总资金的百分比）
        # 3. 默认 100 股
        target_quantity = 100  # 默认值

        if strategy_config:
            if strategy_config.get('quantity'):
                target_quantity = int(strategy_config.get('quantity', 100))
            elif strategy_config.get('position_pct'):
                # 根据账户可用资金计算
                position_pct = float(strategy_config.get('position_pct', 0.1))
                # 查询账户可用资金
                account = await db.fetchone(
                    "SELECT available_cash FROM accounts WHERE account_id = ?",
                    (account_id,)
                )
                if account:
                    available_cash = account.get('available_cash', 0)
                    # 计算可买金额
                    buy_amount = available_cash * position_pct
                    # 计算可买数量（100 的整数倍）
                    if current_price > 0:
                        target_quantity = int((buy_amount / current_price) // 100) * 100
                        target_quantity = max(target_quantity, 100)  # 至少 100 股

        watchlist_data = {
            "account_id": account_id,
            "strategy_id": strategy_id,
            "stock_code": candidate['stock_code'],
            "stock_name": candidate.get('stock_name', ''),
            "reason": candidate.get('reason', ''),
            "buy_price": current_price,
            "stop_loss_price": stop_loss,
            "take_profit_price": take_profit,
            "target_quantity": target_quantity,
            "status": "pending",
            "created_at": get_china_time().isoformat(),
            "updated_at": get_china_time().isoformat()
        }

        await db.insert("watchlist", watchlist_data)
        print(f"[Screening] 添加至 watchlist: {candidate['stock_code']} - {candidate.get('stock_name')} (目标数量：{target_quantity}股)")

    async def _add_to_temp_candidates(
        self,
        account_id: str,
        strategy_id: int,
        candidate: Dict,
        strategy_config: Optional[Dict] = None
    ):
        """
        将候选股票暂存到临时表（待用户确认）

        根据策略配置计算：
        - 买入价格（当前价）
        - 止损价（默认 -5%）
        - 止盈价（默认 +15%）
        - 目标数量（根据策略配置和账户资金）
        """
        db = get_db_manager()

        # 检查是否已存在
        existing = await db.fetchone(
            "SELECT id FROM temp_candidates WHERE account_id = ? AND stock_code = ?",
            (account_id, candidate['stock_code'])
        )

        if existing:
            return  # 已存在，跳过

        # 获取当前价格
        current_price = candidate.get('current_price', 0)

        # 从策略配置读取止损止盈比例，或使用默认值
        stop_loss_pct = strategy_config.get('stop_loss_pct', 0.05) if strategy_config else 0.05
        take_profit_pct = strategy_config.get('take_profit_pct', 0.15) if strategy_config else 0.15

        stop_loss = current_price * (1 - stop_loss_pct)
        take_profit = current_price * (1 + take_profit_pct)

        # 计算目标买入数量
        target_quantity = 100  # 默认值

        if strategy_config:
            if strategy_config.get('quantity'):
                target_quantity = int(strategy_config.get('quantity', 100))
            elif strategy_config.get('position_pct'):
                position_pct = float(strategy_config.get('position_pct', 0.1))
                account = await db.fetchone(
                    "SELECT available_cash FROM accounts WHERE account_id = ?",
                    (account_id,)
                )
                if account:
                    available_cash = account.get('available_cash', 0)
                    buy_amount = available_cash * position_pct
                    if current_price > 0:
                        target_quantity = int((buy_amount / current_price) // 100) * 100
                        target_quantity = max(target_quantity, 100)

        temp_data = {
            "account_id": account_id,
            "strategy_id": strategy_id,
            "stock_code": candidate['stock_code'],
            "stock_name": candidate.get('stock_name', ''),
            "reason": candidate.get('reason', ''),
            "buy_price": current_price,
            "stop_loss_price": stop_loss,
            "take_profit_price": take_profit,
            "target_quantity": target_quantity,
            "match_score": candidate.get('match_score', 0),
            "created_at": get_china_time().isoformat()
        }

        await db.insert("temp_candidates", temp_data)
        print(f"[Screening] 暂存候选：{candidate['stock_code']} - {candidate.get('stock_name')} (匹配度：{candidate.get('match_score', 0)*100:.0f}%)")

    async def confirm_candidates(
        self,
        account_id: str,
        stock_codes: Optional[List[str]] = None,
        confirm: bool = True
    ) -> Dict:
        """
        确认或拒绝临时候选股票

        Args:
            account_id: 账户 ID
            stock_codes: 要确认的股票代码列表，None 表示全部
            confirm: True=确认加入 watchlist，False=拒绝

        Returns:
            {"success": True, "confirmed": 10, "rejected": 5}
        """
        db = get_db_manager()
        result = {"success": True, "confirmed": 0, "rejected": 0}

        if stock_codes:
            # 确认指定的股票
            for stock_code in stock_codes:
                candidate = await db.fetchone(
                    "SELECT * FROM temp_candidates WHERE account_id = ? AND stock_code = ?",
                    (account_id, stock_code)
                )
                if not candidate:
                    continue

                if confirm:
                    # 加入 watchlist
                    await db.insert("watchlist", {
                        "account_id": account_id,
                        "strategy_id": candidate['strategy_id'],
                        "stock_code": stock_code,
                        "stock_name": candidate['stock_name'],
                        "reason": candidate['reason'],
                        "buy_price": candidate['buy_price'],
                        "stop_loss_price": candidate['stop_loss_price'],
                        "take_profit_price": candidate['take_profit_price'],
                        "target_quantity": candidate['target_quantity'],
                        "status": "pending",
                        "created_at": get_china_time().isoformat(),
                        "updated_at": get_china_time().isoformat()
                    })
                    result["confirmed"] += 1
                else:
                    result["rejected"] += 1

                # 从临时表删除
                await db.delete("temp_candidates", "account_id = ? AND stock_code = ?", (account_id, stock_code))
        else:
            # 确认或拒绝全部
            candidates = await db.fetchall(
                "SELECT * FROM temp_candidates WHERE account_id = ?",
                (account_id,)
            )

            for candidate in candidates:
                if confirm:
                    await db.insert("watchlist", {
                        "account_id": account_id,
                        "strategy_id": candidate['strategy_id'],
                        "stock_code": candidate['stock_code'],
                        "stock_name": candidate['stock_name'],
                        "reason": candidate['reason'],
                        "buy_price": candidate['buy_price'],
                        "stop_loss_price": candidate['stop_loss_price'],
                        "take_profit_price": candidate['take_profit_price'],
                        "target_quantity": candidate['target_quantity'],
                        "status": "pending",
                        "created_at": get_china_time().isoformat(),
                        "updated_at": get_china_time().isoformat()
                    })
                    result["confirmed"] += 1
                else:
                    result["rejected"] += 1

            # 清空临时表
            await db.delete("temp_candidates", "account_id = ?", (account_id,))

        return result

    def get_status(self) -> Dict:
        """获取服务状态"""
        return {
            "running": self._running,
            "task": "active" if self._task else None
        }

    def get_progress(self) -> Dict:
        """获取选股进度"""
        return self._progress.copy()

    def reset_progress(self):
        """重置进度"""
        self._progress = {
            "total_stocks": 0,
            "processed": 0,
            "matched": 0,
            "current_phase": "idle",
            "current_stock": None,
            "start_time": None,
            "estimated_remaining": None
        }


# 全局单例
_screening_service: Optional[ScreeningService] = None


def get_screening_service() -> ScreeningService:
    """获取选股服务单例"""
    global _screening_service
    if _screening_service is None:
        _screening_service = ScreeningService()
    return _screening_service


def reset_screening_service():
    """重置选股服务（用于测试）"""
    global _screening_service
    _screening_service = None
