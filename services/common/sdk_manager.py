"""
SDK 登录管理器

统一管理 AmazingData SDK 的登录状态和实例
避免多处重复登录和实例创建

重要：
1. 所有 SDK 实例（BaseData、MarketData、InfoData）都通过此管理器缓存
2. 所有 SDK 数据调用自动通过 SDKConnectionManager 排队，避免 TGW 连接数超限
3. 其他模块应该通过 get_sdk_manager() 获取实例，禁止自行创建 SDK 连接

架构分层：
- SDKConnectionManager（底层）：TGW 连接生命周期管理（状态检测、自动重连、并发排队、超时控制、错误报告）
- SDKManager（中间层）：SDK 实例缓存 + 数据方法封装，连接需求透明委托给底层
- 调用方：通过 get_sdk_manager() 获取实例，所有数据方法自动 acquire/release token
"""

import time
from typing import Optional, Dict
import pandas as pd

from services.common.structured_logger import get_logger


class SDKManager:
    """SDK 登录管理器（单例模式）

    连接生命周期委托给 SDKConnectionManager，本类只负责：
    - SDK 实例缓存（避免重复创建）
    - 数据方法封装（自动 acquire/release token）
    """

    _instance: Optional['SDKManager'] = None
    _info_instance = None
    _base_data_instance = None
    _market_data_instance = None
    _calendar = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    @classmethod
    def get_instance(cls) -> 'SDKManager':
        """获取 SDKManager 单例"""
        if cls._instance is None:
            cls._instance = SDKManager()
        return cls._instance

    # ================================================================
    # 连接管理 — 委托给 SDKConnectionManager
    # ================================================================

    def _get_conn_mgr(self):
        """获取底层连接管理器"""
        from services.common.sdk_connection_manager import get_connection_manager
        return get_connection_manager()

    def connect(self) -> bool:
        """确保 SDK 连接可用（委托给连接管理器）"""
        return self._get_conn_mgr().ensure_connected()

    def disconnect(self):
        """主动断开连接，重置实例缓存"""
        self._get_conn_mgr().disconnect()
        SDKManager._base_data_instance = None
        SDKManager._market_data_instance = None
        SDKManager._info_instance = None

    def is_connected(self) -> bool:
        """检查 SDK 连接状态"""
        from services.common.sdk_connection_manager import ConnectionState
        return self._get_conn_mgr().get_state() == ConnectionState.CONNECTED

    def _ensure_connected(self) -> bool:
        """确保连接可用（内部方法，外部请调用 connect()）"""
        return self._get_conn_mgr().ensure_connected()

    # ================================================================
    # 连接令牌管理 — 所有 SDK 数据调用自动排队
    # ================================================================

    def _acquire_sync(self, task_type="download"):
        """同步获取连接令牌

        先确保连接可用，再通过连接管理器 acquire 排队。
        acquire 现在是同步方法（threading.Lock），无需事件循环。
        """
        # 先确保连接可用
        self._ensure_connected()

        from services.common.sdk_connection_manager import TaskType, ConnectionState
        conn_mgr = self._get_conn_mgr()

        # 如果连接不可用，返回 None
        if conn_mgr.get_state() != ConnectionState.CONNECTED:
            return None

        ttype = TaskType.DOWNLOAD if task_type == "download" else (
            TaskType.SCREENING if task_type == "screening" else TaskType.QUERY
        )
        return conn_mgr.acquire(task_type=ttype)

    def _release_sync(self, token):
        """同步释放连接令牌"""
        if token:
            token.release()

    # ================================================================
    # 实例获取方法 — 确保连接后返回缓存实例
    # ================================================================

    def get_info(self):
        """获取 InfoData 实例（缓存）"""
        self._ensure_connected()
        if SDKManager._info_instance is None:
            from AmazingData import InfoData
            SDKManager._info_instance = InfoData()
        return SDKManager._info_instance

    def get_base_data(self):
        """获取 BaseData 实例（缓存）"""
        self._ensure_connected()
        if SDKManager._base_data_instance is None:
            from AmazingData import BaseData
            SDKManager._base_data_instance = BaseData()
        return SDKManager._base_data_instance

    def get_calendar(self):
        """获取交易日历（缓存）"""
        if SDKManager._calendar is None:
            bd = self.get_base_data()
            SDKManager._calendar = bd.get_calendar()
        return SDKManager._calendar

    def get_market_data(self):
        """获取 MarketData 实例（缓存）"""
        self._ensure_connected()
        if SDKManager._market_data_instance is None:
            from AmazingData import MarketData
            calendar = self.get_calendar()
            SDKManager._market_data_instance = MarketData(calendar)
        return SDKManager._market_data_instance

    # ================================================================
    # 数据获取方法 — 所有 SDK 数据调用自动 acquire/release token
    # ================================================================

    def get_equity_structure(self, stock_codes: list) -> pd.DataFrame:
        """获取股本结构数据（自动排队）"""
        logger = get_logger("sdk_manager")
        info = self.get_info()
        token = self._acquire_sync("download")
        if token is None:
            return pd.DataFrame()
        start = time.monotonic()
        try:
            result = info.get_equity_structure(stock_codes, is_local=False)
            duration_ms = (time.monotonic() - start) * 1000
            logger.log_sdk_call("get_equity_structure", duration_ms, "download", "success", stock_count=len(stock_codes) if stock_codes else 0)
            if isinstance(result, dict):
                dfs = [df for df in result.values() if isinstance(df, pd.DataFrame)]
                if dfs:
                    return pd.concat(dfs, ignore_index=True)
            elif isinstance(result, pd.DataFrame):
                return result
            return pd.DataFrame()
        except Exception as e:
            duration_ms = (time.monotonic() - start) * 1000
            logger.log_sdk_call("get_equity_structure", duration_ms, "download", "error", error=str(e), stock_count=len(stock_codes) if stock_codes else 0)
            return pd.DataFrame()
        finally:
            self._release_sync(token)

    def get_income_statement(self, stock_codes: list) -> pd.DataFrame:
        """获取利润表数据（自动排队）"""
        logger = get_logger("sdk_manager")
        info = self.get_info()
        token = self._acquire_sync("download")
        if token is None:
            return pd.DataFrame()
        start = time.monotonic()
        try:
            result = info.get_income(stock_codes, is_local=False)
            duration_ms = (time.monotonic() - start) * 1000
            logger.log_sdk_call("get_income_statement", duration_ms, "download", "success", stock_count=len(stock_codes) if stock_codes else 0)
            if isinstance(result, dict):
                dfs = [df for df in result.values() if isinstance(df, pd.DataFrame)]
                if dfs:
                    return pd.concat(dfs, ignore_index=True)
            elif isinstance(result, pd.DataFrame):
                return result
            return pd.DataFrame()
        except Exception as e:
            duration_ms = (time.monotonic() - start) * 1000
            logger.log_sdk_call("get_income_statement", duration_ms, "download", "error", error=str(e), stock_count=len(stock_codes) if stock_codes else 0)
            return pd.DataFrame()
        finally:
            self._release_sync(token)

    def get_balance_sheet(self, stock_codes: list) -> pd.DataFrame:
        """获取资产负债表数据（自动排队）"""
        logger = get_logger("sdk_manager")
        info = self.get_info()
        token = self._acquire_sync("download")
        if token is None:
            return pd.DataFrame()
        start = time.monotonic()
        try:
            result = info.get_balance_sheet(stock_codes, is_local=False)
            duration_ms = (time.monotonic() - start) * 1000
            logger.log_sdk_call("get_balance_sheet", duration_ms, "download", "success", stock_count=len(stock_codes) if stock_codes else 0)
            if isinstance(result, dict):
                dfs = [df for df in result.values() if isinstance(df, pd.DataFrame)]
                if dfs:
                    return pd.concat(dfs, ignore_index=True)
            elif isinstance(result, pd.DataFrame):
                return result
            return pd.DataFrame()
        except Exception as e:
            duration_ms = (time.monotonic() - start) * 1000
            logger.log_sdk_call("get_balance_sheet", duration_ms, "download", "error", error=str(e), stock_count=len(stock_codes) if stock_codes else 0)
            return pd.DataFrame()
        finally:
            self._release_sync(token)

    def get_cash_flow_statement(self, stock_codes: list) -> pd.DataFrame:
        """获取现金流量表数据（自动排队）"""
        logger = get_logger("sdk_manager")
        info = self.get_info()
        token = self._acquire_sync("download")
        if token is None:
            return pd.DataFrame()
        start = time.monotonic()
        try:
            result = info.get_cash_flow(stock_codes, is_local=False)
            duration_ms = (time.monotonic() - start) * 1000
            logger.log_sdk_call("get_cash_flow_statement", duration_ms, "download", "success", stock_count=len(stock_codes) if stock_codes else 0)
            if isinstance(result, dict):
                dfs = [df for df in result.values() if isinstance(df, pd.DataFrame)]
                if dfs:
                    return pd.concat(dfs, ignore_index=True)
            elif isinstance(result, pd.DataFrame):
                return result
            return pd.DataFrame()
        except Exception as e:
            duration_ms = (time.monotonic() - start) * 1000
            logger.log_sdk_call("get_cash_flow_statement", duration_ms, "download", "error", error=str(e), stock_count=len(stock_codes) if stock_codes else 0)
            return pd.DataFrame()
        finally:
            self._release_sync(token)

    def get_industry_base_info(self) -> pd.DataFrame:
        """获取行业分类/行业指数基本信息（申万行业分类）（自动排队）"""
        logger = get_logger("sdk_manager")
        info = self.get_info()
        token = self._acquire_sync("download")
        if token is None:
            return pd.DataFrame()
        start = time.monotonic()
        try:
            result = info.get_industry_base_info(is_local=False)
            duration_ms = (time.monotonic() - start) * 1000
            logger.log_sdk_call("get_industry_base_info", duration_ms, "download", "success")
            if isinstance(result, dict):
                dfs = [df for df in result.values() if isinstance(df, pd.DataFrame)]
                if dfs:
                    return pd.concat(dfs, ignore_index=True)
            elif isinstance(result, pd.DataFrame):
                return result
            return pd.DataFrame()
        except Exception as e:
            duration_ms = (time.monotonic() - start) * 1000
            logger.log_sdk_call("get_industry_base_info", duration_ms, "download", "error", error=str(e))
            return pd.DataFrame()
        finally:
            self._release_sync(token)

    def get_code_info(self, security_type: str = 'EXTRA_STOCK_A') -> pd.DataFrame:
        """获取每日最新证券信息（自动排队）"""
        logger = get_logger("sdk_manager")
        token = self._acquire_sync("query")
        if token is None:
            return pd.DataFrame()
        start = time.monotonic()
        try:
            base_data = self.get_base_data()
            result = base_data.get_code_info(security_type=security_type)
            duration_ms = (time.monotonic() - start) * 1000
            logger.log_sdk_call("get_code_info", duration_ms, "query", "success")
            if isinstance(result, pd.DataFrame):
                return result
            return pd.DataFrame()
        except Exception as e:
            duration_ms = (time.monotonic() - start) * 1000
            logger.log_sdk_call("get_code_info", duration_ms, "query", "error", error=str(e))
            return pd.DataFrame()
        finally:
            self._release_sync(token)

    def get_code_list(self, security_type: str = 'EXTRA_STOCK_A') -> list:
        """获取每日最新代码表（自动排队）"""
        logger = get_logger("sdk_manager")
        token = self._acquire_sync("query")
        if token is None:
            return []
        start = time.monotonic()
        try:
            base_data = self.get_base_data()
            result = base_data.get_code_list(security_type=security_type)
            duration_ms = (time.monotonic() - start) * 1000
            logger.log_sdk_call("get_code_list", duration_ms, "query", "success")
            if isinstance(result, list):
                return result
            return []
        except Exception as e:
            duration_ms = (time.monotonic() - start) * 1000
            logger.log_sdk_call("get_code_list", duration_ms, "query", "error", error=str(e))
            return []
        finally:
            self._release_sync(token)

    def _call_with_timeout(self, func, timeout: float, desc: str = "SDK call", **kwargs):
        """带超时的 SDK 调用包装器，防止单次调用卡死导致锁不释放"""
        import concurrent.futures

        def _run():
            return func(**kwargs)

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(_run)
            try:
                return future.result(timeout=timeout)
            except concurrent.futures.TimeoutError:
                get_logger("sdk_manager").error("sdk_timeout", f"{desc} 超时", context={"timeout": timeout})
                raise

    def query_kline(self, code_list: list, begin_date: int, end_date: int,
                    period: int, task_type: str = "query") -> dict:
        """查询K线数据（自动排队）

        超时保护策略：所有类型都有超时，超后释放锁并重试
        - query 单股行情（1-5 只）：10s
        - query 批量行情（6-20 只）：20s
        - query 大批量（21-100 只）：60s
        - download 下载批次：每只股票 30s，最少 5 分钟
        """
        logger = get_logger("sdk_manager")
        md = self.get_market_data()
        token = self._acquire_sync(task_type)
        if token is None:
            return {}
        stock_count = len(code_list) if isinstance(code_list, list) else 1
        if task_type == "query":
            if stock_count <= 5:
                timeout = 10.0
            elif stock_count <= 20:
                timeout = 20.0
            elif stock_count <= 100:
                timeout = 60.0
            else:
                timeout = 120.0
        else:  # download
            # 500只≈44秒，3000只单天≈264秒
            timeout = min(stock_count * 0.2 + 30, 180.0)  # 上限3分钟
            timeout = max(timeout, 30.0)  # 最小30s
        start = time.monotonic()
        try:
            result = self._call_with_timeout(
                md.query_kline,
                timeout=timeout,
                desc=f"query_kline {task_type} {stock_count} stocks",
                code_list=code_list,
                begin_date=begin_date,
                end_date=end_date,
                period=period
            )
            duration_ms = (time.monotonic() - start) * 1000
            logger.log_sdk_call("query_kline", duration_ms, task_type, "success", stock_count=stock_count, timeout=timeout)
            return result if isinstance(result, dict) else {}
        except Exception as e:
            duration_ms = (time.monotonic() - start) * 1000
            is_timeout = isinstance(e, (TimeoutError,))
            if is_timeout:
                logger.log_sdk_call("query_kline", duration_ms, task_type, "timeout", error=str(e), stock_count=stock_count, timeout=timeout)
                SDKManager._market_data_instance = None
            else:
                logger.log_sdk_call("query_kline", duration_ms, task_type, "error", error=str(e), stock_count=stock_count, timeout=timeout)
            return {}
        finally:
            self._release_sync(token)

    def query_snapshot(self, code_list: list, begin_date: int, end_date: int) -> dict:
        """查询快照数据（自动排队）"""
        logger = get_logger("sdk_manager")
        md = self.get_market_data()
        token = self._acquire_sync("query")
        if token is None:
            return {}
        stock_count = len(code_list) if isinstance(code_list, list) else 1
        start = time.monotonic()
        try:
            result = self._call_with_timeout(
                md.query_snapshot,
                timeout=8.0,
                desc=f"query_snapshot {code_list}",
                code_list=code_list,
                begin_date=begin_date,
                end_date=end_date
            )
            duration_ms = (time.monotonic() - start) * 1000
            logger.log_sdk_call("query_snapshot", duration_ms, "query", "success", stock_count=stock_count, timeout=8.0)
            return result if isinstance(result, dict) else {}
        except Exception as e:
            duration_ms = (time.monotonic() - start) * 1000
            is_timeout = isinstance(e, (TimeoutError,))
            if is_timeout:
                logger.log_sdk_call("query_snapshot", duration_ms, "query", "timeout", error=str(e), stock_count=stock_count, timeout=8.0)
                SDKManager._market_data_instance = None
            else:
                logger.log_sdk_call("query_snapshot", duration_ms, "query", "error", error=str(e), stock_count=stock_count, timeout=8.0)
            return {}
        finally:
            self._release_sync(token)

    def get_industry_daily(self, code_list: list) -> Dict[str, pd.DataFrame]:
        """获取行业指数日行情数据（自动排队）"""
        logger = get_logger("sdk_manager")
        token = self._acquire_sync("download")
        if token is None:
            return {}
        start = time.monotonic()
        try:
            info = self.get_info()
            result = info.get_industry_daily(code_list=code_list, is_local=False)
            duration_ms = (time.monotonic() - start) * 1000
            logger.log_sdk_call("get_industry_daily", duration_ms, "download", "success", stock_count=len(code_list) if code_list else 0)
            if isinstance(result, dict):
                return result
            return {}
        except Exception as e:
            duration_ms = (time.monotonic() - start) * 1000
            logger.log_sdk_call("get_industry_daily", duration_ms, "download", "error", error=str(e), stock_count=len(code_list) if code_list else 0)
            return {}
        finally:
            self._release_sync(token)

    def get_profit_notice(self, stock_codes: list) -> pd.DataFrame:
        """获取业绩预告（自动排队）"""
        logger = get_logger("sdk_manager")
        info = self.get_info()
        token = self._acquire_sync("download")
        if token is None:
            return pd.DataFrame()
        start = time.monotonic()
        try:
            result = info.get_profit_notice(code_list=stock_codes, is_local=False)
            duration_ms = (time.monotonic() - start) * 1000
            logger.log_sdk_call("get_profit_notice", duration_ms, "download", "success", stock_count=len(stock_codes) if stock_codes else 0)
            if isinstance(result, dict):
                dfs = [df for df in result.values() if isinstance(df, pd.DataFrame)]
                if dfs:
                    return pd.concat(dfs, ignore_index=True)
            elif isinstance(result, pd.DataFrame):
                return result
            return pd.DataFrame()
        except Exception as e:
            duration_ms = (time.monotonic() - start) * 1000
            logger.log_sdk_call("get_profit_notice", duration_ms, "download", "error", error=str(e), stock_count=len(stock_codes) if stock_codes else 0)
            return pd.DataFrame()
        finally:
            self._release_sync(token)

    def get_profit_express(self, stock_codes: list) -> pd.DataFrame:
        """获取业绩快报（自动排队）"""
        logger = get_logger("sdk_manager")
        info = self.get_info()
        token = self._acquire_sync("download")
        if token is None:
            return pd.DataFrame()
        start = time.monotonic()
        try:
            result = info.get_profit_express(code_list=stock_codes, is_local=False)
            duration_ms = (time.monotonic() - start) * 1000
            logger.log_sdk_call("get_profit_express", duration_ms, "download", "success", stock_count=len(stock_codes) if stock_codes else 0)
            if isinstance(result, dict):
                dfs = [df for df in result.values() if isinstance(df, pd.DataFrame)]
                if dfs:
                    return pd.concat(dfs, ignore_index=True)
            elif isinstance(result, pd.DataFrame):
                return result
            return pd.DataFrame()
        except Exception as e:
            duration_ms = (time.monotonic() - start) * 1000
            logger.log_sdk_call("get_profit_express", duration_ms, "download", "error", error=str(e), stock_count=len(stock_codes) if stock_codes else 0)
            return pd.DataFrame()
        finally:
            self._release_sync(token)

    def get_long_hu_bang(self, stock_codes: list, begin_date: int, end_date: int) -> pd.DataFrame:
        """获取龙虎榜数据（自动排队）"""
        logger = get_logger("sdk_manager")
        info = self.get_info()
        token = self._acquire_sync("download")
        if token is None:
            return pd.DataFrame()
        start = time.monotonic()
        try:
            result = info.get_long_hu_bang(code_list=stock_codes, begin_date=begin_date, end_date=end_date, is_local=False)
            duration_ms = (time.monotonic() - start) * 1000
            logger.log_sdk_call("get_long_hu_bang", duration_ms, "download", "success", stock_count=len(stock_codes) if stock_codes else 0)
            if isinstance(result, pd.DataFrame):
                return result
            return pd.DataFrame()
        except Exception as e:
            duration_ms = (time.monotonic() - start) * 1000
            logger.log_sdk_call("get_long_hu_bang", duration_ms, "download", "error", error=str(e), stock_count=len(stock_codes) if stock_codes else 0)
            return pd.DataFrame()
        finally:
            self._release_sync(token)

    def get_margin_summary(self, begin_date: int, end_date: int) -> pd.DataFrame:
        """获取融资融券汇总（自动排队，无需 stock_codes）"""
        logger = get_logger("sdk_manager")
        info = self.get_info()
        token = self._acquire_sync("download")
        if token is None:
            return pd.DataFrame()
        start = time.monotonic()
        try:
            result = info.get_margin_summary(begin_date=begin_date, end_date=end_date, is_local=False)
            duration_ms = (time.monotonic() - start) * 1000
            logger.log_sdk_call("get_margin_summary", duration_ms, "download", "success")
            if isinstance(result, pd.DataFrame):
                return result
            return pd.DataFrame()
        except Exception as e:
            duration_ms = (time.monotonic() - start) * 1000
            logger.log_sdk_call("get_margin_summary", duration_ms, "download", "error", error=str(e))
            return pd.DataFrame()
        finally:
            self._release_sync(token)

    def get_margin_detail(self, stock_codes: list, begin_date: int, end_date: int) -> pd.DataFrame:
        """获取融资融券明细（自动排队）"""
        logger = get_logger("sdk_manager")
        info = self.get_info()
        token = self._acquire_sync("download")
        if token is None:
            return pd.DataFrame()
        start = time.monotonic()
        try:
            result = info.get_margin_detail(code_list=stock_codes, begin_date=begin_date, end_date=end_date, is_local=False)
            duration_ms = (time.monotonic() - start) * 1000
            logger.log_sdk_call("get_margin_detail", duration_ms, "download", "success", stock_count=len(stock_codes) if stock_codes else 0)
            if isinstance(result, dict):
                dfs = [df for df in result.values() if isinstance(df, pd.DataFrame)]
                if dfs:
                    return pd.concat(dfs, ignore_index=True)
            elif isinstance(result, pd.DataFrame):
                return result
            return pd.DataFrame()
        except Exception as e:
            duration_ms = (time.monotonic() - start) * 1000
            logger.log_sdk_call("get_margin_detail", duration_ms, "download", "error", error=str(e), stock_count=len(stock_codes) if stock_codes else 0)
            return pd.DataFrame()
        finally:
            self._release_sync(token)

    def get_block_trading(self, stock_codes: list, begin_date: int, end_date: int) -> pd.DataFrame:
        """获取大宗交易数据（自动排队）"""
        logger = get_logger("sdk_manager")
        info = self.get_info()
        token = self._acquire_sync("download")
        if token is None:
            return pd.DataFrame()
        start = time.monotonic()
        try:
            result = info.get_block_trading(code_list=stock_codes, begin_date=begin_date, end_date=end_date, is_local=False)
            duration_ms = (time.monotonic() - start) * 1000
            logger.log_sdk_call("get_block_trading", duration_ms, "download", "success", stock_count=len(stock_codes) if stock_codes else 0)
            if isinstance(result, pd.DataFrame):
                return result
            return pd.DataFrame()
        except Exception as e:
            duration_ms = (time.monotonic() - start) * 1000
            logger.log_sdk_call("get_block_trading", duration_ms, "download", "error", error=str(e), stock_count=len(stock_codes) if stock_codes else 0)
            return pd.DataFrame()
        finally:
            self._release_sync(token)

    def get_treasury_yield(self) -> pd.DataFrame:
        """获取国债收益率（自动排队）"""
        logger = get_logger("sdk_manager")
        info = self.get_info()
        token = self._acquire_sync("download")
        if token is None:
            return pd.DataFrame()
        start = time.monotonic()
        try:
            result = info.get_treasury_yield(is_local=False)
            duration_ms = (time.monotonic() - start) * 1000
            logger.log_sdk_call("get_treasury_yield", duration_ms, "download", "success")
            if isinstance(result, dict):
                dfs = [df for df in result.values() if isinstance(df, pd.DataFrame)]
                if dfs:
                    return pd.concat(dfs, ignore_index=True)
            elif isinstance(result, pd.DataFrame):
                return result
            return pd.DataFrame()
        except Exception as e:
            duration_ms = (time.monotonic() - start) * 1000
            logger.log_sdk_call("get_treasury_yield", duration_ms, "download", "error", error=str(e))
            return pd.DataFrame()
        finally:
            self._release_sync(token)

    def get_industry_constituent(self, index_codes: list) -> pd.DataFrame:
        """获取行业成分股（自动排队）"""
        logger = get_logger("sdk_manager")
        info = self.get_info()
        token = self._acquire_sync("download")
        if token is None:
            return pd.DataFrame()
        start = time.monotonic()
        try:
            result = info.get_industry_constituent(code_list=index_codes, is_local=False)
            duration_ms = (time.monotonic() - start) * 1000
            logger.log_sdk_call("get_industry_constituent", duration_ms, "download", "success", stock_count=len(index_codes) if index_codes else 0)
            if isinstance(result, dict):
                dfs = [df for df in result.values() if isinstance(df, pd.DataFrame)]
                if dfs:
                    return pd.concat(dfs, ignore_index=True)
            elif isinstance(result, pd.DataFrame):
                return result
            return pd.DataFrame()
        except Exception as e:
            duration_ms = (time.monotonic() - start) * 1000
            logger.log_sdk_call("get_industry_constituent", duration_ms, "download", "error", error=str(e), stock_count=len(index_codes) if index_codes else 0)
            return pd.DataFrame()
        finally:
            self._release_sync(token)

    def get_index_constituent(self, index_codes: list) -> pd.DataFrame:
        """获取指数成分股（自动排队）"""
        logger = get_logger("sdk_manager")
        info = self.get_info()
        token = self._acquire_sync("download")
        if token is None:
            return pd.DataFrame()
        start = time.monotonic()
        try:
            result = info.get_index_constituent(code_list=index_codes, is_local=False)
            duration_ms = (time.monotonic() - start) * 1000
            logger.log_sdk_call("get_index_constituent", duration_ms, "download", "success", stock_count=len(index_codes) if index_codes else 0)
            if isinstance(result, dict):
                dfs = [df for df in result.values() if isinstance(df, pd.DataFrame)]
                if dfs:
                    return pd.concat(dfs, ignore_index=True)
            elif isinstance(result, pd.DataFrame):
                return result
            return pd.DataFrame()
        except Exception as e:
            duration_ms = (time.monotonic() - start) * 1000
            logger.log_sdk_call("get_index_constituent", duration_ms, "download", "error", error=str(e), stock_count=len(index_codes) if index_codes else 0)
            return pd.DataFrame()
        finally:
            self._release_sync(token)


# SDK 登录信息 - 从环境变量读取，不再硬编码
import os
SDK_USERNAME = os.environ.get("SDK_USERNAME", "")
SDK_PASSWORD = os.environ.get("SDK_PASSWORD", "")
SDK_HOST = os.environ.get("SDK_HOST", "")
SDK_PORT = int(os.environ.get("SDK_PORT", "8600"))


# 全局单例
_sdk_manager: Optional[SDKManager] = None


def get_sdk_manager() -> SDKManager:
    """获取 SDKManager 单例"""
    global _sdk_manager
    if _sdk_manager is None:
        _sdk_manager = SDKManager()
    return _sdk_manager


def reset_sdk_manager():
    """重置 SDKManager（用于测试）"""
    global _sdk_manager
    _sdk_manager = None
