"""
SDK 登录管理器

统一管理 AmazingData SDK 的登录状态和实例
避免多处重复登录和实例创建

重要：所有 SDK 实例（BaseData、MarketData、InfoData）都通过此管理器缓存
其他模块应该通过 get_sdk_manager() 获取实例，避免创建独立连接导致 TGW 连接数超限
"""

from typing import Optional, Dict
import pandas as pd


class SDKManager:
    """SDK 登录管理器（单例模式）"""

    _instance: Optional['SDKManager'] = None
    _login_cache: bool = False
    _info_instance = None
    _base_data_instance = None  # 缓存 BaseData 实例，避免重复创建导致连接数超限
    _market_data_instance = None  # 缓存 MarketData 实例
    _calendar = None  # 缓存交易日历

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

    def _ensure_login(self) -> bool:
        """确保已登录 SDK"""
        if not SDKManager._login_cache:
            try:
                from AmazingData import login
                result = login(
                    SDK_USERNAME,
                    SDK_PASSWORD,
                    SDK_HOST,
                    SDK_PORT
                )
                if result:
                    SDKManager._login_cache = True
                    print("[SDK] 登录成功")
                else:
                    print("[SDK] 登录失败")
            except Exception as e:
                print(f"[SDK] 登录异常：{e}")
                raise

        return SDKManager._login_cache

    def get_info(self):
        """获取 InfoData 实例（缓存）"""
        self._ensure_login()
        if SDKManager._info_instance is None:
            from AmazingData import InfoData
            SDKManager._info_instance = InfoData()
        return SDKManager._info_instance

    def get_base_data(self):
        """获取 BaseData 实例（缓存，避免重复创建导致TGW连接数超限）"""
        self._ensure_login()
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
        """获取 MarketData 实例（缓存，需要交易日历）"""
        self._ensure_login()
        if SDKManager._market_data_instance is None:
            from AmazingData import MarketData
            calendar = self.get_calendar()
            SDKManager._market_data_instance = MarketData(calendar)
        return SDKManager._market_data_instance

    def get_equity_structure(self, stock_codes: list) -> pd.DataFrame:
        """获取股本结构数据"""
        info = self.get_info()
        try:
            result = info.get_equity_structure(stock_codes, is_local=False)
            if isinstance(result, dict):
                dfs = [df for df in result.values() if isinstance(df, pd.DataFrame)]
                if dfs:
                    return pd.concat(dfs, ignore_index=True)
            elif isinstance(result, pd.DataFrame):
                return result
            return pd.DataFrame()
        except Exception as e:
            print(f"[SDK] 获取股本结构数据失败：{e}")
            return pd.DataFrame()

    def get_income_statement(self, stock_codes: list) -> pd.DataFrame:
        """获取利润表数据"""
        info = self.get_info()
        try:
            result = info.get_income(stock_codes, is_local=False)
            if isinstance(result, dict):
                dfs = [df for df in result.values() if isinstance(df, pd.DataFrame)]
                if dfs:
                    return pd.concat(dfs, ignore_index=True)
            elif isinstance(result, pd.DataFrame):
                return result
            return pd.DataFrame()
        except Exception as e:
            print(f"[SDK] 获取利润表数据失败：{e}")
            return pd.DataFrame()

    def get_balance_sheet(self, stock_codes: list) -> pd.DataFrame:
        """获取资产负债表数据"""
        info = self.get_info()
        try:
            result = info.get_balance_sheet(stock_codes, is_local=False)
            if isinstance(result, dict):
                dfs = [df for df in result.values() if isinstance(df, pd.DataFrame)]
                if dfs:
                    return pd.concat(dfs, ignore_index=True)
            elif isinstance(result, pd.DataFrame):
                return result
            return pd.DataFrame()
        except Exception as e:
            print(f"[SDK] 获取资产负债表数据失败：{e}")
            return pd.DataFrame()

    def get_cash_flow_statement(self, stock_codes: list) -> pd.DataFrame:
        """获取现金流量表数据"""
        info = self.get_info()
        try:
            result = info.get_cash_flow(stock_codes, is_local=False)
            if isinstance(result, dict):
                dfs = [df for df in result.values() if isinstance(df, pd.DataFrame)]
                if dfs:
                    return pd.concat(dfs, ignore_index=True)
            elif isinstance(result, pd.DataFrame):
                return result
            return pd.DataFrame()
        except Exception as e:
            print(f"[SDK] 获取现金流量表数据失败：{e}")
            return pd.DataFrame()

    def get_industry_base_info(self) -> pd.DataFrame:
        """获取行业分类数据"""
        info = self.get_info()
        try:
            result = info.get_industry_base_info(is_local=False)
            if isinstance(result, dict):
                dfs = [df for df in result.values() if isinstance(df, pd.DataFrame)]
                if dfs:
                    return pd.concat(dfs, ignore_index=True)
            elif isinstance(result, pd.DataFrame):
                return result
            return pd.DataFrame()
        except Exception as e:
            print(f"[SDK] 获取行业分类数据失败：{e}")
            return pd.DataFrame()

    def get_code_info(self, security_type: str = 'EXTRA_STOCK_A') -> pd.DataFrame:
        """获取每日最新证券信息（包含股票名称、昨收价、涨跌停价等）"""
        self._ensure_login()
        try:
            base_data = self.get_base_data()  # 使用缓存的实例
            result = base_data.get_code_info(security_type=security_type)
            if isinstance(result, pd.DataFrame):
                return result
            return pd.DataFrame()
        except Exception as e:
            print(f"[SDK] 获取证券信息失败：{e}")
            return pd.DataFrame()

    def get_code_list(self, security_type: str = 'EXTRA_STOCK_A') -> list:
        """获取每日最新代码表"""
        self._ensure_login()
        try:
            base_data = self.get_base_data()  # 使用缓存的实例
            result = base_data.get_code_list(security_type=security_type)
            if isinstance(result, list):
                return result
            return []
        except Exception as e:
            print(f"[SDK] 获取代码列表失败：{e}")
            return []

    def get_industry_base_info(self) -> pd.DataFrame:
        """获取行业指数基本信息"""
        self._ensure_login()
        try:
            info = self.get_info()
            result = info.get_industry_base_info(is_local=False)
            if isinstance(result, pd.DataFrame):
                return result
            return pd.DataFrame()
        except Exception as e:
            print(f"[SDK] 获取行业指数基本信息失败：{e}")
            return pd.DataFrame()

    def get_industry_daily(self, code_list: list) -> Dict[str, pd.DataFrame]:
        """获取行业指数日行情数据"""
        self._ensure_login()
        try:
            info = self.get_info()
            result = info.get_industry_daily(code_list=code_list, is_local=False)
            if isinstance(result, dict):
                return result
            return {}
        except Exception as e:
            print(f"[SDK] 获取行业指数日行情失败：{e}")
            return {}


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


def get_equity_structure(stock_codes: list) -> pd.DataFrame:
    """便捷函数：获取股本结构数据"""
    return get_sdk_manager().get_equity_structure(stock_codes)


def get_income_statement(stock_codes: list) -> pd.DataFrame:
    """便捷函数：获取利润表数据"""
    return get_sdk_manager().get_income_statement(stock_codes)


def get_balance_sheet(stock_codes: list) -> pd.DataFrame:
    """便捷函数：获取资产负债表数据"""
    return get_sdk_manager().get_balance_sheet(stock_codes)


def get_cash_flow_statement(stock_codes: list) -> pd.DataFrame:
    """便捷函数：获取现金流量表数据"""
    return get_sdk_manager().get_cash_flow_statement(stock_codes)
