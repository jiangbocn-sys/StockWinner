"""
财务数据服务 — 批量获取 + 比值计算

解决 N+1 SDK 调用问题：
- fundamental_factor_calculator.py：每个比值方法独立调 SDK（10+ 次/股）
- daily_factor_calculator.py：独立调 SDK（3-4 次/股）
- monthly_factor_calculator.py：独立调 SDK（3 次/股）
- monthly_factor_updater.py：批量获取，但比值计算散落在 calculate_factors() 中

本模块提供统一 API：
1. 批量获取财务数据（一次 SDK 调用覆盖多只股票）
2. 从已获取的数据计算所有比值（PE_TTM/PB/PS/PCF/ROE/ROA 等）
3. 同一批次内数据缓存，不重复调 SDK

使用示例:
    from services.factors.financial_service import FinancialService

    fs = FinancialService()
    # 批量获取（内部只调 3 次 SDK，覆盖所有股票）
    batch = fs.fetch_batch(['600000.SH', '000001.SZ'])

    # 从已获取的数据计算比值（不再调 SDK）
    pe = fs.calc_pe_ttm('600000.SH', batch)
    pb = fs.calc_pb('600000.SH', batch)
    roe = fs.calc_roe('600000.SH', batch)
"""

import pandas as pd
import logging
from typing import Optional, Dict, Any, List

from services.common.sdk_manager import get_sdk_manager

logger = logging.getLogger(__name__)


class FinancialBatch:
    """批量财务数据容器"""

    def __init__(self, income_df: pd.DataFrame, balance_df: pd.DataFrame,
                 cashflow_df: pd.DataFrame):
        self.income_df = income_df
        self.balance_df = balance_df
        self.cashflow_df = cashflow_df

    def get_income(self, stock_code: str, report_period: Optional[str] = None) -> Optional[pd.Series]:
        return self._get_latest(self.income_df, stock_code, report_period)

    def get_balance(self, stock_code: str, report_period: Optional[str] = None) -> Optional[pd.Series]:
        return self._get_latest(self.balance_df, stock_code, report_period)

    def get_cashflow(self, stock_code: str, report_period: Optional[str] = None) -> Optional[pd.Series]:
        return self._get_latest(self.cashflow_df, stock_code, report_period)

    @staticmethod
    def _get_latest(df: pd.DataFrame, stock_code: str,
                    report_period: Optional[str] = None) -> Optional[pd.Series]:
        """获取指定股票的最新财务数据"""
        if df.empty:
            return None
        data = df[df['MARKET_CODE'] == stock_code]
        if data.empty:
            return None
        if report_period:
            data = data[data['REPORTING_PERIOD'].astype(str) == str(report_period)]
        if data.empty:
            return None
        return data.sort_values('REPORTING_PERIOD', ascending=False).iloc[0]


class FinancialService:
    """财务数据服务"""

    def __init__(self):
        self.sdk = get_sdk_manager()
        self._cache: Dict[str, FinancialBatch] = {}

    def fetch_batch(self, stock_codes: List[str], force: bool = False) -> FinancialBatch:
        """
        批量获取财务数据。

        内部只调 3 次 SDK（income/balance/cashflow），覆盖所有传入的股票。
        结果缓存，同一批次内多次调用返回同一对象。

        Args:
            stock_codes: 股票代码列表
            force: 是否强制刷新（忽略缓存）
        """
        key = ','.join(sorted(stock_codes))
        if key in self._cache and not force:
            return self._cache[key]

        income = self.sdk.get_income_statement(stock_codes)
        balance = self.sdk.get_balance_sheet(stock_codes)
        cashflow = self.sdk.get_cash_flow_statement(stock_codes)

        batch = FinancialBatch(income, balance, cashflow)
        self._cache[key] = batch
        return batch

    def clear_cache(self):
        """清除缓存"""
        self._cache.clear()

    # ================================================================
    # 估值比值计算（复用已获取的财务数据，不再调 SDK）
    # ================================================================

    def calc_pe_ttm(self, stock_code: str, batch: FinancialBatch,
                    market_cap: float = 0) -> Optional[float]:
        """PE_TTM = 市值 / 净利润 TTM"""
        income = batch.get_income(stock_code)
        if income is None:
            return None
        net_profit = income.get('NET_PRO_INCL_MIN_INT_INC')
        if pd.isna(net_profit) or net_profit <= 0:
            return None
        market_cap_yi = market_cap  # 亿元
        net_profit_yi = net_profit / 100000000
        if net_profit_yi <= 0:
            return None
        return market_cap_yi / net_profit_yi

    def calc_pb(self, stock_code: str, batch: FinancialBatch,
                market_cap: float = 0) -> Optional[float]:
        """PB = 市值 / 净资产"""
        balance = batch.get_balance(stock_code)
        if balance is None:
            return None
        net_assets = balance.get('TOT_SHARE_EQUITY_EXCL_MIN_INT')
        if pd.isna(net_assets) or net_assets <= 0:
            return None
        net_assets_yi = net_assets / 100000000
        if net_assets_yi <= 0:
            return None
        return market_cap / net_assets_yi

    def calc_ps_ttm(self, stock_code: str, batch: FinancialBatch,
                    market_cap: float = 0) -> Optional[float]:
        """PS_TTM = 市值 / 营收 TTM"""
        income = batch.get_income(stock_code)
        if income is None:
            return None
        revenue = income.get('OPERA_REV')
        if pd.isna(revenue) or revenue <= 0:
            return None
        revenue_yi = revenue / 100000000
        if revenue_yi <= 0:
            return None
        return market_cap / revenue_yi

    def calc_pcf(self, stock_code: str, batch: FinancialBatch,
                 market_cap: float = 0) -> Optional[float]:
        """PCF = 市值 / 经营现金流"""
        cashflow = batch.get_cashflow(stock_code)
        if cashflow is None:
            return None
        operating_cf = cashflow.get('NET_CASH_FLOWS_OPERA_ACT')
        if pd.isna(operating_cf) or operating_cf <= 0:
            return None
        operating_cf_yi = operating_cf / 100000000
        if operating_cf_yi <= 0:
            return None
        return market_cap / operating_cf_yi

    def calc_roe(self, stock_code: str, batch: FinancialBatch) -> Optional[float]:
        """ROE = 净利润 / 净资产 * 100%"""
        income = batch.get_income(stock_code)
        balance = batch.get_balance(stock_code)
        if income is None or balance is None:
            return None
        net_profit = income.get('NET_PRO_INCL_MIN_INT_INC')
        net_assets = balance.get('TOT_SHARE_EQUITY_EXCL_MIN_INT')
        if pd.isna(net_profit) or pd.isna(net_assets) or net_assets <= 0:
            return None
        return net_profit / net_assets * 100

    def calc_roa(self, stock_code: str, batch: FinancialBatch) -> Optional[float]:
        """ROA = 净利润 / 总资产 * 100%"""
        income = batch.get_income(stock_code)
        balance = batch.get_balance(stock_code)
        if income is None or balance is None:
            return None
        net_profit = income.get('NET_PRO_INCL_MIN_INT_INC')
        total_assets = balance.get('TOTAL_ASSETS')
        if pd.isna(net_profit) or pd.isna(total_assets) or total_assets <= 0:
            return None
        return net_profit / total_assets * 100

    def calc_gross_margin(self, stock_code: str, batch: FinancialBatch) -> Optional[float]:
        """毛利率 = (营收 - 营业成本) / 营收 * 100%"""
        income = batch.get_income(stock_code)
        if income is None:
            return None
        revenue = income.get('OPERA_REV')
        cost = income.get('OPERA_COST')
        if pd.isna(revenue) or pd.isna(cost) or revenue <= 0:
            return None
        return (revenue - cost) / revenue * 100

    def calc_net_margin(self, stock_code: str, batch: FinancialBatch) -> Optional[float]:
        """净利率 = 净利润 / 营收 * 100%"""
        income = batch.get_income(stock_code)
        if income is None:
            return None
        net_profit = income.get('NET_PRO_INCL_MIN_INT_INC')
        revenue = income.get('OPERA_REV')
        if pd.isna(net_profit) or pd.isna(revenue) or revenue <= 0:
            return None
        return net_profit / revenue * 100

    def calc_operating_margin(self, stock_code: str, batch: FinancialBatch) -> Optional[float]:
        """营业利润率 = 营业利润 / 营收 * 100%"""
        income = batch.get_income(stock_code)
        if income is None:
            return None
        op_profit = income.get('OPERA_PROFIT')
        revenue = income.get('OPERA_REV')
        if pd.isna(op_profit) or pd.isna(revenue) or revenue <= 0:
            return None
        return op_profit / revenue * 100

    def calc_revenue_growth_yoy(self, stock_code: str, batch: FinancialBatch) -> Optional[float]:
        """营收同比增长率"""
        income = batch.get_income(stock_code)
        if income is None:
            return None
        data = batch.income_df[batch.income_df['MARKET_CODE'] == stock_code]
        if len(data) < 2:
            return None
        data = data.sort_values('REPORTING_PERIOD', ascending=False)
        current = data.iloc[0]
        current_period = str(current['REPORTING_PERIOD'])
        last_year_period = f"{int(current_period[:4]) - 1}{current_period[4:]}"
        last_year = data[data['REPORTING_PERIOD'].astype(str) == last_year_period]
        if last_year.empty:
            return None
        current_rev = current.get('OPERA_REV')
        last_rev = last_year.iloc[0].get('OPERA_REV')
        if pd.isna(current_rev) or pd.isna(last_rev) or last_rev <= 0:
            return None
        return (current_rev - last_rev) / last_rev * 100

    def calc_net_profit_growth_yoy(self, stock_code: str, batch: FinancialBatch) -> Optional[float]:
        """净利润同比增长率"""
        income = batch.get_income(stock_code)
        if income is None:
            return None
        data = batch.income_df[batch.income_df['MARKET_CODE'] == stock_code]
        if len(data) < 2:
            return None
        data = data.sort_values('REPORTING_PERIOD', ascending=False)
        current = data.iloc[0]
        current_period = str(current['REPORTING_PERIOD'])
        last_year_period = f"{int(current_period[:4]) - 1}{current_period[4:]}"
        last_year = data[data['REPORTING_PERIOD'].astype(str) == last_year_period]
        if last_year.empty:
            return None
        current_profit = current.get('NET_PRO_INCL_MIN_INT_INC')
        last_profit = last_year.iloc[0].get('NET_PRO_INCL_MIN_INT_INC')
        if pd.isna(current_profit) or pd.isna(last_profit) or last_profit <= 0:
            return None
        return (current_profit - last_profit) / last_profit * 100
