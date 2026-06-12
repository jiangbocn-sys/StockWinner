"""
交易网关 — 薄门面，委托给子服务模块。

子模块：
- models.py: OrderResult, MarketData, TradingGatewayInterface
- market_data_service.py: 行情数据（单只/批量/缓存/并发）
- kline_service.py: K 线数据（单只/批量）
- trading_executor.py: 交易执行（买入/卖出/撤单）
"""
import asyncio
import logging
import math
from typing import Optional, Dict, Any, List, Set

import numpy as np

from services.trading.models import OrderResult, MarketData, TradingGatewayInterface
from services.trading.market_data_service import MarketDataService
from services.trading.kline_service import KlineDataService
from services.trading.trading_executor import TradingExecutorService

logger = logging.getLogger(__name__)


class TradingGateway(TradingGatewayInterface):
    """交易网关（使用 AmazingData SDK） — 薄门面，委托给子服务"""

    def __init__(self, app_id: str = "", password: str = ""):
        self.app_id = app_id
        self.password = password
        self.server_ip = "140.206.44.234"
        self.server_port = 8600

        try:
            from AmazingData import constant
            self.constant = constant
            self.sdk_available = True
            logger.info("AmazingData SDK 加载成功（通过 SDKManager 管理）")
        except ImportError as e:
            logger.warning(f"AmazingData SDK 不可用：{e}")
            self.sdk_available = False

        # 子服务
        self._market_data = MarketDataService()
        self._kline_data = KlineDataService(self.sdk_available, getattr(self, 'constant', None))
        self._trading = TradingExecutorService()

    @property
    def connected(self) -> bool:
        """检查 SDK 连接状态（使用 SDKProxyClient）"""
        if not self.sdk_available:
            return False
        try:
            from services.common.sdk_proxy_client import SDKProxyClient
            client = SDKProxyClient.get_instance()
            # 如果 IPC 连接断开，尝试重连
            if not client._connected:
                client.connect_to_subprocess(timeout=5.0)
            return client._connected
        except Exception:
            return False

    async def connect(self) -> bool:
        """连接交易服务器 — 委托给 SDKManager"""
        if not self.sdk_available:
            logger.error("AmazingData SDK 未安装，无法连接")
            return False
        try:
            from services.common.sdk_manager import get_sdk_manager
            return get_sdk_manager().connect()
        except Exception as e:
            logger.error(f"交易网关连接失败：{e}")
            return False

    async def disconnect(self):
        """断开连接 — 委托给 SDKManager"""
        from services.common.sdk_manager import get_sdk_manager
        get_sdk_manager().disconnect()

    # ── 行情数据 ──

    async def get_market_data(self, stock_code: str) -> Optional[MarketData]:
        return await self._market_data.get_market_data(stock_code, self.connected)

    async def get_batch_market_data(self, stock_codes: List[str]) -> Dict[str, Optional[MarketData]]:
        return await self._market_data.get_batch_market_data(stock_codes, self.connected)

    # ── K 线数据 ──

    async def get_kline_data(
        self, stock_code: str, period: str = "day",
        start_date: Optional[str] = None, end_date: Optional[str] = None,
        limit: int = 100, priority: int = 1, adjust: str = "none"
    ) -> List[Dict[str, Any]]:
        """获取 K 线数据（用户查询，默认 high priority）

        Args:
            adjust: 复权方式
                - "none": 不复权（原始价格）
                - "forward": 前复权（历史价格调整，以当前价格为基准）
        """
        klines = await self._kline_data.get_kline_data(
            stock_code, period, start_date, end_date, limit,
            task_type="query", priority=priority, connected=self.connected
        )

        # 应用前复权
        if adjust == "forward" and klines:
            klines = await self._apply_forward_adjustment(klines, stock_code)

        return klines

    async def _apply_forward_adjustment(self, klines: List[Dict], stock_code: str) -> List[Dict]:
        """应用前复权（历史价格 × 当日累计因子 / 最新累计因子）"""
        try:
            adj_factors = await self.get_adj_factor([stock_code])
            if not adj_factors:
                return klines

            # 构建日期 → 累计因子映射
            cumulative_map = {}
            latest_cumulative = 1.0
            for f in adj_factors:
                td_raw = str(f.get('trade_date', ''))
                if '-' in td_raw:
                    td = td_raw.replace('-', '')[:8]
                else:
                    td = td_raw[:8]
                cum_factor = float(f.get('cumulative_factor', 1.0))
                cumulative_map[td] = cum_factor
                latest_cumulative = cum_factor  # 最后一个就是最新的

            # 应用前复权公式
            for k in klines:
                td_raw = k.get('trade_date', '')
                if '-' in td_raw:
                    td = td_raw.replace('-', '')[:8]
                else:
                    td = td_raw[:8]

                if td in cumulative_map:
                    factor = cumulative_map[td]
                else:
                    # 找该日期之前最近的累计因子
                    earlier_dates = [d for d in cumulative_map.keys() if d <= td]
                    # 如果没有更早的除权日，使用 1.0（无除权）
                    # 如果有，使用最近的除权日累计因子
                    factor = cumulative_map[max(earlier_dates)] if earlier_dates else 1.0

                adj_ratio = factor / latest_cumulative
                if 'open' in k:
                    k['open'] = round(float(k['open']) * adj_ratio, 2)
                if 'high' in k:
                    k['high'] = round(float(k['high']) * adj_ratio, 2)
                if 'low' in k:
                    k['low'] = round(float(k['low']) * adj_ratio, 2)
                if 'close' in k:
                    k['close'] = round(float(k['close']) * adj_ratio, 2)

            return klines
        except Exception:
            # 复权失败，返回原始数据
            return klines

    async def get_batch_kline_data(
        self, stock_codes: List[str], period: str = "day",
        start_date: Optional[str] = None, end_date: Optional[str] = None,
        limit: int = 100, task_type: str = "query", priority: int = 1
    ) -> Dict[str, Any]:
        """批量获取 K 线数据

        Args:
            priority: 默认 high (1)，后台下载用 low (3)
        """
        return await self._kline_data.get_batch_kline_data(
            stock_codes, period, start_date, end_date, limit,
            task_type=task_type, priority=priority, connected=self.connected
        )

    # ── 外部数据 ──

    @staticmethod
    def _sanitize_nan(obj):
        """将 NaN/Inf 转换为 None，避免 JSON 序列化失败"""
        if obj is None:
            return None
        if isinstance(obj, (np.floating, np.integer)):
            if np.isnan(obj) or np.isinf(obj):
                return None
            return obj.item()
        if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
            return None
        try:
            import pandas as pd
            if obj is pd.NA or obj is pd.NaT:
                return None
            if isinstance(obj, pd.Timestamp):
                return obj.strftime("%Y-%m-%d")
        except Exception:
            pass
        return obj

    @staticmethod
    def _records_from_raw_df(df) -> List[Dict[str, Any]]:
        """将 DataFrame 转为 JSON records，清理 NaN"""
        if df.empty:
            return []
        df.columns = df.columns.str.lower()
        records = df.to_dict(orient="records")
        for record in records:
            for key, value in record.items():
                record[key] = TradingGateway._sanitize_nan(value)
        return records

    @staticmethod
    def _records_from_df(df, stock_code: str) -> List[Dict[str, Any]]:
        """将 DataFrame 按 stock_code 过滤并转为 JSON records，清理 NaN"""
        if df.empty:
            return []
        df.columns = df.columns.str.lower()
        records = df[df["market_code"] == stock_code].to_dict(orient="records")
        for record in records:
            for key, value in record.items():
                record[key] = TradingGateway._sanitize_nan(value)
        return records

    async def get_index_list(self, priority: int = 2) -> List[Dict[str, Any]]:
        """获取指数列表（用户查询，默认 medium priority）"""
        if not self.connected:
            raise Exception("网关未连接")
        try:
            from services.common.sdk_manager import get_sdk_manager
            return await asyncio.to_thread(lambda: get_sdk_manager().get_index_list(priority=priority))
        except Exception as e:
            logger.error(f"获取指数列表失败: {e}")
            return []

    # ── 代码信息 / 行业 ──

    async def get_code_info(self, security_type: str = 'EXTRA_STOCK_A', priority: int = 2) -> List[Dict[str, Any]]:
        """获取代码信息（用户查询，默认 medium priority）"""
        if not self.connected:
            raise Exception("网关未连接")
        try:
            return await asyncio.to_thread(self._get_code_info_sync, security_type, priority)
        except Exception as e:
            raise Exception(f"获取代码信息失败：{str(e)}")

    def _get_code_info_sync(self, security_type: str = 'EXTRA_STOCK_A', priority: int = 2) -> List[Dict[str, Any]]:
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        df = sdk_mgr.get_code_info(security_type=security_type, priority=priority)
        if df.empty:
            return []
        return [{
            "code": str(idx) if idx else '',
            "name": row.get('symbol', '') or str(idx) if idx else '',
            "market": str(idx).split('.')[-1] if '.' in str(idx) else ('SH' if str(idx).startswith('6') else 'SZ'),
        } for idx, row in df.iterrows()]

    async def get_industry_list(self, level: int = 1, priority: int = 2) -> List[Dict[str, Any]]:
        """获取行业列表（用户查询，默认 medium priority）"""
        if not self.connected:
            raise Exception("网关未连接")
        try:
            return await asyncio.to_thread(self._get_industry_list_sync, level, priority)
        except Exception as e:
            raise Exception(f"获取行业列表失败：{str(e)}")

    def _get_industry_list_sync(self, level: int = 1, priority: int = 2) -> List[Dict[str, Any]]:
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        df = sdk_mgr.get_industry_base_info(priority=priority)
        if df.empty:
            return []
        filtered = df[df["LEVEL_TYPE"] == level]
        records = filtered.to_dict(orient="records")
        for record in records:
            for key, value in record.items():
                record[key] = self._sanitize_nan(value)
        return records

    async def get_industry_kline(self, index_code: str, priority: int = 2) -> List[Dict[str, Any]]:
        """获取行业行情（用户查询，默认 medium priority）"""
        if not self.connected:
            raise Exception("网关未连接")
        try:
            return await asyncio.to_thread(self._get_industry_kline_sync, index_code, priority)
        except Exception as e:
            raise Exception(f"获取行业行情失败：{str(e)}")

    def _get_industry_kline_sync(self, index_code: str, priority: int = 2) -> List[Dict[str, Any]]:
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        result = sdk_mgr.get_industry_daily(code_list=[index_code], priority=priority)
        if not result or index_code not in result:
            return []
        df = result[index_code].reset_index()
        df.columns = df.columns.str.lower()
        if "trade_date" in df.columns:
            df["trade_date"] = df["trade_date"].apply(
                lambda x: x.strftime("%Y-%m-%d") if hasattr(x, "strftime") else str(x)[:10]
            )
        records = df.to_dict(orient="records")
        for record in records:
            for key, value in record.items():
                record[key] = self._sanitize_nan(value)
        return records

    async def get_industry_constituent(self, index_code: str, priority: int = 2) -> List[Dict[str, Any]]:
        """获取行业成分股（用户查询，默认 medium priority）"""
        if not self.connected:
            raise Exception("网关未连接")
        try:
            return await asyncio.to_thread(self._get_industry_constituent_sync, index_code, priority)
        except Exception as e:
            raise Exception(f"获取行业成分股失败：{str(e)}")

    def _get_industry_constituent_sync(self, index_code: str, priority: int = 2) -> List[Dict[str, Any]]:
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        df = sdk_mgr.get_industry_constituent(index_codes=[index_code], priority=priority)
        return self._records_from_raw_df(df)

    async def get_index_constituent(self, index_code: str, priority: int = 2) -> List[Dict[str, Any]]:
        """获取指数成分股（用户查询，默认 medium priority）"""
        if not self.connected:
            raise Exception("网关未连接")
        try:
            return await asyncio.to_thread(self._get_index_constituent_sync, index_code, priority)
        except Exception as e:
            raise Exception(f"获取指数成分股失败：{str(e)}")

    def _get_index_constituent_sync(self, index_code: str, priority: int = 2) -> List[Dict[str, Any]]:
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        df = sdk_mgr.get_index_constituent(index_codes=[index_code], priority=priority)
        return self._records_from_raw_df(df)

    # ── 财务报表 ──

    async def get_income_statement(self, stock_code: str, priority: int = 2) -> List[Dict[str, Any]]:
        """获取利润表（用户查询，默认 medium priority）"""
        if not self.connected:
            raise Exception("网关未连接")
        try:
            return await asyncio.to_thread(self._get_income_statement_sync, stock_code, priority)
        except Exception as e:
            raise Exception(f"获取利润表失败：{str(e)}")

    def _get_income_statement_sync(self, stock_code: str, priority: int = 2) -> List[Dict[str, Any]]:
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        df = sdk_mgr.get_income_statement(stock_codes=[stock_code], priority=priority)
        return self._records_from_df(df, stock_code)

    async def get_balance_sheet(self, stock_code: str, priority: int = 2) -> List[Dict[str, Any]]:
        """获取资产负债表（用户查询，默认 medium priority）"""
        if not self.connected:
            raise Exception("网关未连接")
        try:
            return await asyncio.to_thread(self._get_balance_sheet_sync, stock_code, priority)
        except Exception as e:
            raise Exception(f"获取资产负债表失败：{str(e)}")

    def _get_balance_sheet_sync(self, stock_code: str, priority: int = 2) -> List[Dict[str, Any]]:
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        df = sdk_mgr.get_balance_sheet(stock_codes=[stock_code], priority=priority)
        return self._records_from_df(df, stock_code)

    async def get_cash_flow_statement(self, stock_code: str, priority: int = 2) -> List[Dict[str, Any]]:
        """获取现金流量表（用户查询，默认 medium priority）"""
        if not self.connected:
            raise Exception("网关未连接")
        try:
            return await asyncio.to_thread(self._get_cash_flow_statement_sync, stock_code, priority)
        except Exception as e:
            raise Exception(f"获取现金流量表失败：{str(e)}")

    def _get_cash_flow_statement_sync(self, stock_code: str, priority: int = 2) -> List[Dict[str, Any]]:
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        df = sdk_mgr.get_cash_flow_statement(stock_codes=[stock_code], priority=priority)
        return self._records_from_df(df, stock_code)

    async def get_profit_notice(self, stock_code: str, priority: int = 2) -> List[Dict[str, Any]]:
        """获取业绩预告（用户查询，默认 medium priority）"""
        if not self.connected:
            raise Exception("网关未连接")
        try:
            return await asyncio.to_thread(self._get_profit_notice_sync, stock_code, priority)
        except Exception as e:
            raise Exception(f"获取业绩预告失败：{str(e)}")

    def _get_profit_notice_sync(self, stock_code: str, priority: int = 2) -> List[Dict[str, Any]]:
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        df = sdk_mgr.get_profit_notice(stock_codes=[stock_code], priority=priority)
        return self._records_from_df(df, stock_code)

    async def get_profit_express(self, stock_code: str, priority: int = 2) -> List[Dict[str, Any]]:
        """获取业绩快报（用户查询，默认 medium priority）"""
        if not self.connected:
            raise Exception("网关未连接")
        try:
            return await asyncio.to_thread(self._get_profit_express_sync, stock_code, priority)
        except Exception as e:
            raise Exception(f"获取业绩快报失败：{str(e)}")

    def _get_profit_express_sync(self, stock_code: str, priority: int = 2) -> List[Dict[str, Any]]:
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        df = sdk_mgr.get_profit_express(stock_codes=[stock_code], priority=priority)
        return self._records_from_df(df, stock_code)

    # ── 特殊行情 ──

    async def get_long_hu_bang(self, stock_code: str, begin_date: int, end_date: int, priority: int = 2) -> List[Dict[str, Any]]:
        """获取龙虎榜（用户查询，默认 medium priority）"""
        if not self.connected:
            raise Exception("网关未连接")
        try:
            return await asyncio.to_thread(self._get_long_hu_bang_sync, stock_code, begin_date, end_date, priority)
        except Exception as e:
            raise Exception(f"获取龙虎榜失败：{str(e)}")

    def _get_long_hu_bang_sync(self, stock_code: str, begin_date: int, end_date: int, priority: int = 2) -> List[Dict[str, Any]]:
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        df = sdk_mgr.get_long_hu_bang(stock_codes=[stock_code], begin_date=begin_date, end_date=end_date, priority=priority)
        return self._records_from_raw_df(df)

    async def get_margin_summary(self, begin_date: int, end_date: int, priority: int = 2) -> List[Dict[str, Any]]:
        """获取融资融券汇总（用户查询，默认 medium priority）"""
        if not self.connected:
            raise Exception("网关未连接")
        try:
            return await asyncio.to_thread(self._get_margin_summary_sync, begin_date, end_date, priority)
        except Exception as e:
            raise Exception(f"获取两融汇总失败：{str(e)}")

    def _get_margin_summary_sync(self, begin_date: int, end_date: int, priority: int = 2) -> List[Dict[str, Any]]:
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        df = sdk_mgr.get_margin_summary(begin_date=begin_date, end_date=end_date, priority=priority)
        return self._records_from_raw_df(df)

    async def get_margin_detail(self, stock_code: str, begin_date: int, end_date: int, priority: int = 2) -> List[Dict[str, Any]]:
        """获取融资融券明细（用户查询，默认 medium priority）"""
        if not self.connected:
            raise Exception("网关未连接")
        try:
            return await asyncio.to_thread(self._get_margin_detail_sync, stock_code, begin_date, end_date, priority)
        except Exception as e:
            raise Exception(f"获取两融明细失败：{str(e)}")

    def _get_margin_detail_sync(self, stock_code: str, begin_date: int, end_date: int, priority: int = 2) -> List[Dict[str, Any]]:
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        df = sdk_mgr.get_margin_detail(stock_codes=[stock_code], begin_date=begin_date, end_date=end_date, priority=priority)
        return self._records_from_raw_df(df)

    async def get_block_trading(self, stock_code: str, begin_date: int, end_date: int, priority: int = 2) -> List[Dict[str, Any]]:
        """获取大宗交易（用户查询，默认 medium priority）"""
        if not self.connected:
            raise Exception("网关未连接")
        try:
            return await asyncio.to_thread(self._get_block_trading_sync, stock_code, begin_date, end_date, priority)
        except Exception as e:
            raise Exception(f"获取大宗交易失败：{str(e)}")

    def _get_block_trading_sync(self, stock_code: str, begin_date: int, end_date: int, priority: int = 2) -> List[Dict[str, Any]]:
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        df = sdk_mgr.get_block_trading(stock_codes=[stock_code], begin_date=begin_date, end_date=end_date, priority=priority)
        return self._records_from_raw_df(df)

    async def get_treasury_yield(self, priority: int = 2) -> List[Dict[str, Any]]:
        """获取国债收益率（用户查询，默认 medium priority）"""
        if not self.connected:
            raise Exception("网关未连接")
        try:
            return await asyncio.to_thread(self._get_treasury_yield_sync, priority)
        except Exception as e:
            raise Exception(f"获取国债收益率失败：{str(e)}")

    def _get_treasury_yield_sync(self, priority: int = 2) -> List[Dict[str, Any]]:
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        df = sdk_mgr.get_treasury_yield(priority=priority)
        return self._records_from_raw_df(df)

    # ── 代码列表/日历/快照 ──

    async def get_code_list(self, security_type: str = 'EXTRA_STOCK_A', priority: int = 2) -> List[str]:
        """获取证券代码列表"""
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        import asyncio
        code_list = await asyncio.to_thread(sdk_mgr.get_code_list, security_type, priority)
        return code_list

    async def get_calendar(self, market: str = 'SH', priority: int = 2) -> List[int]:
        """获取交易日历"""
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        import asyncio
        calendar = await asyncio.to_thread(sdk_mgr.get_calendar, priority)
        return calendar

    async def query_snapshot(self, code_list: List[str], begin_date: int, end_date: int, priority: int = 2) -> Dict:
        """查询历史快照数据"""
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        import asyncio
        snapshot = await asyncio.to_thread(sdk_mgr.query_snapshot, code_list, begin_date, end_date, priority)
        return snapshot

    # ── 复权因子/基础数据/股东/股权/分红 ──

    async def get_backward_factor(self, stock_codes: List[str], priority: int = 2) -> List[Dict]:
        """获取后复权因子"""
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        import asyncio
        df = await asyncio.to_thread(sdk_mgr.get_backward_factor, stock_codes, priority)
        return df.to_dict('records') if df is not None and not df.empty else []

    async def get_adj_factor(self, stock_codes: List[str], priority: int = 2) -> List[Dict]:
        """获取复权因子（纯数据库查询，不触发 SDK 调用）

        前复权公式：前复权价格 = 原价 × 当日累计因子 / 最新复权因子

        改造说明：
        - 移除按需 SDK 调用逻辑（get_adj_factor 超时问题）
        - 改为盘前预热任务更新（使用 get_dividend，0.3秒）
        - 交易时段零 SDK 开销

        返回格式：[{trade_date, adj_factor, cumulative_factor}, ...]
        """
        from services.data.adj_factor_service import get_adj_factor_for_stock

        if not stock_codes:
            return []

        stock_code = stock_codes[0]

        # 纯数据库读取（不触发 SDK 调用）
        factors = get_adj_factor_for_stock(stock_code)

        # 缺失时记录日志，但不阻塞查询（使用默认值 1.0）
        if not factors:
            logger.warning(f"复权因子缺失: {stock_code}，等待盘前预热任务更新")

        # 转换为原有格式（trade_date YYYYMMDD）
        result = []
        for f in factors:
            trade_date = f['trade_date'].replace('-', '')[:8]
            result.append({
                'trade_date': trade_date,
                'adj_factor': float(f.get('adj_factor', 1.0)),
                'cumulative_factor': float(f.get('cumulative_factor', 1.0))
            })

        return result

    async def get_stock_basic(self, stock_codes: List[str] = None, priority: int = 2) -> List[Dict]:
        """获取股票基础信息（上市日期、退市日期、板块）"""
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        import asyncio
        df = await asyncio.to_thread(sdk_mgr.get_stock_basic, stock_codes or [], priority)
        return df.to_dict('records') if df is not None and not df.empty else []

    async def get_history_code_list(self, date: int, priority: int = 2) -> List[str]:
        """获取历史代码列表"""
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        import asyncio
        codes = await asyncio.to_thread(sdk_mgr.get_history_code_list, date, priority)
        return codes

    async def get_bj_code_mapping(self, priority: int = 2) -> List[Dict]:
        """获取北交所代码对照表"""
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        import asyncio
        df = await asyncio.to_thread(sdk_mgr.get_bj_code_mapping, priority)
        return df.to_dict('records') if df is not None and not df.empty else []

    async def get_shareholder(self, stock_codes: List[str], priority: int = 2) -> List[Dict]:
        """获取十大股东"""
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        import asyncio
        df = await asyncio.to_thread(sdk_mgr.get_shareholder, stock_codes, priority)
        return df.to_dict('records') if df is not None and not df.empty else []

    async def get_holder_num(self, stock_codes: List[str], priority: int = 2) -> List[Dict]:
        """获取股东户数"""
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        import asyncio
        df = await asyncio.to_thread(sdk_mgr.get_holder_num, stock_codes, priority)
        return df.to_dict('records') if df is not None and not df.empty else []

    async def get_equity_structure(self, stock_codes: List[str], priority: int = 2) -> List[Dict]:
        """获取股本结构"""
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        import asyncio
        df = await asyncio.to_thread(sdk_mgr.get_equity_structure, stock_codes, priority)
        return df.to_dict('records') if df is not None and not df.empty else []

    async def get_equity_pledge_freeze(self, stock_codes: List[str], priority: int = 2) -> List[Dict]:
        """获取股权质押冻结"""
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        import asyncio
        df = await asyncio.to_thread(sdk_mgr.get_equity_pledge_freeze, stock_codes, priority)
        return df.to_dict('records') if df is not None and not df.empty else []

    async def get_equity_restricted(self, stock_codes: List[str], priority: int = 2) -> List[Dict]:
        """获取限售股解禁"""
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        import asyncio
        df = await asyncio.to_thread(sdk_mgr.get_equity_restricted, stock_codes, priority)
        return df.to_dict('records') if df is not None and not df.empty else []

    async def get_dividend(self, stock_codes: List[str], priority: int = 2) -> List[Dict]:
        """获取分红数据"""
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        import asyncio
        df = await asyncio.to_thread(sdk_mgr.get_dividend, stock_codes, priority)
        return df.to_dict('records') if df is not None and not df.empty else []

    async def get_right_issue(self, stock_codes: List[str], priority: int = 2) -> List[Dict]:
        """获取配股数据"""
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        import asyncio
        df = await asyncio.to_thread(sdk_mgr.get_right_issue, stock_codes, priority)
        return df.to_dict('records') if df is not None and not df.empty else []

    async def get_index_weight(self, index_code: str, priority: int = 2) -> List[Dict]:
        """获取指数成分权重"""
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        import asyncio
        df = await asyncio.to_thread(sdk_mgr.get_index_weight, index_code, priority)
        return df.to_dict('records') if df is not None and not df.empty else []

    async def get_industry_weight(self, index_code: str, priority: int = 2) -> List[Dict]:
        """获取行业成分权重"""
        from services.common.sdk_manager import get_sdk_manager
        sdk_mgr = get_sdk_manager()
        import asyncio
        df = await asyncio.to_thread(sdk_mgr.get_industry_weight, index_code, priority)
        return df.to_dict('records') if df is not None and not df.empty else []

    # ── 交易执行 ──

    async def buy(self, stock_code: str, price: float, quantity: int, account_id: str = None) -> OrderResult:
        return await self._trading.buy(stock_code, price, quantity, account_id, connected=self.connected)

    async def sell(self, stock_code: str, price: float, quantity: int, account_id: str = None) -> OrderResult:
        return await self._trading.sell(stock_code, price, quantity, account_id, connected=self.connected)

    async def cancel_order(self, order_no: str, account_id: str = None) -> dict:
        return await self._trading.cancel_order(order_no, account_id)

    async def query_order_status(self, order_no: str, account_id: str = None) -> dict:
        return await self._trading.query_order_status(order_no, account_id)

    async def cancel_all_pending_orders(self, account_id: str = None) -> dict:
        return await self._trading.cancel_all_pending_orders(account_id)

    # ── 订阅/调度器委托 ──

    def subscribe(self, subscriber_id: str, stock_codes: Set[str],
                  refresh_interval: int = 30, priority: int = 2):
        from services.trading.gateway_dispatcher import get_gateway_dispatcher
        get_gateway_dispatcher().subscribe(subscriber_id, stock_codes, refresh_interval, priority)

    def unsubscribe(self, subscriber_id: str):
        from services.trading.gateway_dispatcher import get_gateway_dispatcher
        get_gateway_dispatcher().unsubscribe(subscriber_id)

    async def refresh_now(self, subscriber_id: str) -> Dict[str, Any]:
        from services.trading.gateway_dispatcher import get_gateway_dispatcher
        return await get_gateway_dispatcher().refresh_now(subscriber_id)

    def get_dispatch_status(self) -> Dict[str, Any]:
        from services.trading.gateway_dispatcher import get_gateway_dispatcher
        return get_gateway_dispatcher().get_status()

    # ── 列表查询 ──

    async def get_stock_list(self, security_type: str = 'EXTRA_STOCK_A') -> List[Dict[str, str]]:
        """获取证券列表

        Args:
            security_type: 证券类型，可选值：
                - EXTRA_STOCK_A: 沪深北A股（默认）
                - EXTRA_ETF: 沪深ETF
                - EXTRA_INDEX_A: 沪深北指数
                - EXTRA_KZZ: 沪深可转债
        """
        if not self.connected:
            raise Exception("网关未连接")
        try:
            import asyncio
            stock_list = await asyncio.to_thread(self._query_stock_list_sync, security_type)
            return stock_list
        except Exception as e:
            logger.error(f"获取证券列表失败：{e}")
            raise Exception(f"获取证券列表失败 - {str(e)}")

    def _query_stock_list_sync(self, security_type: str = 'EXTRA_STOCK_A') -> List[Dict[str, str]]:
        from services.common.sdk_manager import get_sdk_manager
        from services.common.database import get_sync_connection
        from services.common.security_type import get_security_type

        stock_list = []

        # 优先 SDK（获取最新完整列表，包含新上市股票/ETF）
        try:
            sdk_mgr = get_sdk_manager()
            code_info = sdk_mgr.get_code_info(security_type=security_type)
            if code_info is not None and len(code_info) > 0:
                for idx, row in code_info.iterrows():
                    code = str(idx)
                    symbol = row.get('symbol', '')
                    security_status = row.get('security_status', '')
                    code_without_suffix = code.split('.')[0] if '.' in code else code
                    market = code.split('.')[1] if '.' in code else ("SH" if code.startswith('6') else "SZ")
                    stock_list.append({
                        "code": code_without_suffix, "name": symbol or code_without_suffix,
                        "market": market, "status": security_status
                    })
                logger.info(f"从 SDK 获取到 {len(stock_list)} 只证券（类型: {security_type})")
                return stock_list
        except Exception as e:
            logger.warning(f"SDK获取证券列表失败：{e}，尝试本地数据库兜底")

        # SDK 失败时，从本地数据库兜底
        if security_type in ('EXTRA_STOCK_A', 'EXTRA_ETF'):
            try:
                conn = get_sync_connection("kline")
                cursor = conn.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='kline_data'")
                if cursor.fetchone():
                    cursor.execute("SELECT DISTINCT stock_code FROM kline_data")
                    stock_codes = [row[0] for row in cursor.fetchall()]
                    if stock_codes:
                        for code in stock_codes:
                            code_type = get_security_type(code)
                            if security_type == 'EXTRA_STOCK_A' and code_type == 'stock':
                                parts = code.split('.')
                                if len(parts) == 2:
                                    stock_list.append({"code": parts[0], "name": "", "market": parts[1]})
                            elif security_type == 'EXTRA_ETF' and code_type == 'etf':
                                parts = code.split('.')
                                if len(parts) == 2:
                                    stock_list.append({"code": parts[0], "name": "", "market": parts[1]})
                        if stock_list:
                            logger.info(f"从本地数据库兜底获取到 {len(stock_list)} 只证券（类型: {security_type})")
                            return stock_list
            except Exception as e:
                logger.warning(f"本地数据库兜底失败：{e}")

        raise Exception("无法获取证券列表，请先下载基础数据或检查SDK连接")

    async def get_index_list(self) -> List[Dict[str, str]]:
        if not self.connected:
            raise Exception("网关未连接")
        try:
            import asyncio
            return await asyncio.to_thread(self._query_index_list_sync)
        except Exception as e:
            logger.error(f"获取指数列表失败：{e}")
            raise Exception(f"获取指数列表失败 - {str(e)}")

    def _query_index_list_sync(self) -> List[Dict[str, str]]:
        from services.common.sdk_manager import get_sdk_manager
        major_indices = [
            {'code': '000001', 'name': '上证指数', 'market': 'SH'},
            {'code': '000016', 'name': '上证50', 'market': 'SH'},
            {'code': '000300', 'name': '沪深300', 'market': 'SH'},
            {'code': '000905', 'name': '中证500', 'market': 'SH'},
            {'code': '000852', 'name': '中证1000', 'market': 'SH'},
            {'code': '399001', 'name': '深证成指', 'market': 'SZ'},
            {'code': '399006', 'name': '创业板指', 'market': 'SZ'},
            {'code': '399005', 'name': '中小板指', 'market': 'SZ'},
        ]
        try:
            sdk_mgr = get_sdk_manager()
            code_list = sdk_mgr.get_code_list(security_type='EXTRA_INDEX_A_SH_SZ')
            if code_list:
                code_info = None
                try:
                    code_info = sdk_mgr.get_code_info(security_type='EXTRA_INDEX_A_SH_SZ')
                except Exception as e:
                    logger.warning(f"获取指数名称信息失败：{e}")
                index_list = []
                for code in code_list:
                    code_str = str(code)
                    code_without_suffix = code_str.split('.')[0] if '.' in code_str else code_str
                    market = code_str.split('.')[1] if '.' in code_str else ("SH" if code_str.startswith('0') else "SZ")
                    name = code_without_suffix
                    if code_info is not None:
                        for idx, row in code_info.iterrows():
                            if str(idx) == code_str:
                                name = row.get('symbol', code_without_suffix)
                                break
                    index_list.append({"code": code_without_suffix, "name": name, "market": market})
                return index_list
        except Exception as e:
            logger.warning(f"SDK获取指数列表失败：{e}, 使用预设指数列表")
        return major_indices

    async def get_industry_index_list(self) -> List[Dict[str, str]]:
        if not self.connected:
            raise Exception("网关未连接")
        try:
            import asyncio
            return await asyncio.to_thread(self._query_industry_index_list_sync)
        except Exception as e:
            logger.error(f"获取行业指数列表失败：{e}")
            return []

    def _query_industry_index_list_sync(self) -> List[Dict[str, str]]:
        from services.common.sdk_manager import get_sdk_manager
        try:
            sdk_mgr = get_sdk_manager()
            industry_info = sdk_mgr.get_industry_base_info()
            if industry_info is not None and len(industry_info) > 0:
                level1 = industry_info[industry_info['LEVEL_TYPE'] == 1]
                index_list = []
                for idx, row in level1.iterrows():
                    index_code = row.get('INDEX_CODE', '')
                    level1_name = row.get('LEVEL1_NAME', '')
                    code_without_suffix = str(index_code).split('.')[0] if '.' in str(index_code) else str(index_code)
                    market = str(index_code).split('.')[1] if '.' in str(index_code) else 'SI'
                    index_list.append({
                        "code": code_without_suffix, "name": level1_name,
                        "market": market, "level": 1, "type": "industry"
                    })
                return index_list
        except Exception as e:
            logger.warning(f"SDK获取行业指数列表失败：{e}")
        return []

    # ── 持仓/委托（SDK 暂不支持） ──

    async def get_positions(self) -> List[Dict[str, Any]]:
        if not self.connected:
            return []
        return []

    async def get_orders(self) -> List[Dict[str, Any]]:
        if not self.connected:
            return []
        return []


class ErrorReportingGateway(TradingGatewayInterface):
    """错误报告网关 - 当真实数据不可用时返回具体错误原因"""

    def __init__(self, error_message: str):
        self.error_message = error_message
        self.connected = True

    async def connect(self) -> bool:
        self.connected = True
        return True

    async def disconnect(self):
        self.connected = False

    async def get_market_data(self, stock_code: str) -> Optional[MarketData]:
        if not self.connected:
            return None
        raise Exception(self.error_message)

    async def get_stock_list(self) -> List[Dict[str, str]]:
        if not self.connected:
            return []
        raise Exception(self.error_message)

    async def buy(self, stock_code: str, price: float, quantity: int) -> OrderResult:
        return OrderResult(False, message=self.error_message)

    async def sell(self, stock_code: str, price: float, quantity: int) -> OrderResult:
        return OrderResult(False, message=self.error_message)

    async def get_positions(self) -> List[Dict[str, Any]]:
        return []

    async def get_orders(self) -> List[Dict[str, Any]]:
        return []

    async def get_index_list(self) -> List[Dict[str, str]]:
        raise Exception(self.error_message)

    async def get_industry_index_list(self) -> List[Dict[str, str]]:
        return []

    async def get_batch_market_data(self, stock_codes: List[str]) -> Dict[str, Optional[MarketData]]:
        return {}

    async def get_kline_data(self, stock_code: str, period: str = "day", start_date: str = None, end_date: str = None, limit: int = 100) -> List[Dict[str, Any]]:
        raise Exception(self.error_message)

    async def get_code_info(self, security_type: str = 'EXTRA_STOCK_A') -> List[Dict[str, Any]]:
        return []

    async def get_industry_list(self, level: int = 1) -> List[Dict[str, Any]]:
        return []

    async def get_industry_kline(self, index_code: str) -> List[Dict[str, Any]]:
        return []

    async def get_industry_constituent(self, index_code: str) -> List[Dict[str, Any]]:
        return []

    async def get_index_constituent(self, index_code: str) -> List[Dict[str, Any]]:
        return []

    async def get_income_statement(self, stock_code: str) -> List[Dict[str, Any]]:
        return []

    async def get_balance_sheet(self, stock_code: str) -> List[Dict[str, Any]]:
        return []

    async def get_cash_flow_statement(self, stock_code: str) -> List[Dict[str, Any]]:
        return []

    async def get_profit_notice(self, stock_code: str) -> List[Dict[str, Any]]:
        return []

    async def get_profit_express(self, stock_code: str) -> List[Dict[str, Any]]:
        return []

    async def get_long_hu_bang(self, stock_code: str, begin_date: int, end_date: int) -> List[Dict[str, Any]]:
        return []

    async def get_margin_summary(self, begin_date: int, end_date: int) -> List[Dict[str, Any]]:
        return []

    async def get_margin_detail(self, stock_code: str, begin_date: int, end_date: int) -> List[Dict[str, Any]]:
        return []

    async def get_block_trading(self, stock_code: str, begin_date: int, end_date: int) -> List[Dict[str, Any]]:
        return []

    async def get_treasury_yield(self) -> List[Dict[str, Any]]:
        return []


# ── 工厂函数 ──

def create_gateway(galaxy_app_id: str = "", galaxy_password: str = "") -> TradingGatewayInterface:
    gateway = TradingGateway(app_id=galaxy_app_id, password=galaxy_password)
    if not gateway.sdk_available:
        raise Exception("AmazingData SDK 不可用，无法创建交易网关")
    return gateway


_gateway: Optional[TradingGatewayInterface] = None
_gateway_cache: Dict[str, TradingGatewayInterface] = {}


def clear_gateway_cache():
    global _gateway
    _gateway_cache.clear()
    _gateway = None


async def get_gateway_for_account(broker_credentials: Optional[Dict[str, str]] = None) -> TradingGatewayInterface:
    if not broker_credentials or not broker_credentials.get("broker_account"):
        return await get_gateway()

    account_id = broker_credentials.get("broker_account")
    if account_id in _gateway_cache:
        return _gateway_cache[account_id]

    gateway = create_gateway(
        galaxy_app_id=broker_credentials.get("broker_account", ""),
        galaxy_password=broker_credentials.get("broker_password", "")
    )
    await gateway.connect()
    _gateway_cache[account_id] = gateway
    logger.info(f"为账户 {account_id} 创建了独立的交易网关")
    return gateway


async def get_gateway() -> TradingGatewayInterface:
    global _gateway
    if _gateway is None:
        _gateway = create_gateway()
        await _gateway.connect()
    return _gateway


async def get_gateway_with_credentials(auth_token: Optional[str] = None) -> TradingGatewayInterface:
    if not auth_token:
        return await get_gateway()
    try:
        from services.auth.service import get_auth_service
        auth_service = get_auth_service()
        account = auth_service.validate_token(auth_token)
        if not account:
            logger.warning("无效的认证 token，使用默认网关")
            return await get_gateway()

        broker_creds = {
            "broker_account": account.get("broker_account", ""),
            "broker_password": account.get("broker_password", ""),
        }
        if not broker_creds["broker_account"]:
            logger.warning(f"用户 {account.get('username')} 未配置券商账号，使用默认网关")
            return await get_gateway()

        return await get_gateway_for_account(broker_creds)
    except Exception as e:
        logger.error(f"获取带 credentials 的网关失败：{e}")
        return await get_gateway()


async def init_gateway(galaxy_app_id: str = "", galaxy_password: str = ""):
    global _gateway
    if _gateway:
        await _gateway.disconnect()
    _gateway = create_gateway(galaxy_app_id=galaxy_app_id, galaxy_password=galaxy_password)
    await _gateway.connect()
    return _gateway
