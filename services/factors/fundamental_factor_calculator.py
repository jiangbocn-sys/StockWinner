"""
基本面因子计算器

基于财务数据计算估值、盈利、成长类因子：
- 估值类：PE_TTM, PB, PS_TTM, PCF
- 盈利类：ROE, ROA, 毛利率，净利率
- 成长类：营收增长率，净利润增长率
"""

import sqlite3
import pandas as pd
import numpy as np
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from datetime import datetime

from services.common.sdk_manager import get_sdk_manager

DB_PATH = Path(__file__).parent.parent.parent / "data" / "kline.db"


class FundamentalFactorCalculator:
    """基本面因子计算器"""

    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path
        self.sdk_manager = get_sdk_manager()

    def _get_connection(self) -> sqlite3.Connection:
        """获取数据库连接"""
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        return conn

    def get_market_cap(self, stock_code: str, trade_date: str) -> Optional[float]:
        """获取指定日期的市值（亿元）"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT total_market_cap FROM stock_daily_factors
            WHERE stock_code = ? AND trade_date = ?
        """, (stock_code, trade_date))
        row = cursor.fetchone()
        conn.close()
        return row['total_market_cap'] if row and row['total_market_cap'] else None

    def get_latest_stock_code_list(self, limit: int = 100) -> List[str]:
        """获取最新的股票列表"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT stock_code FROM kline_data
            ORDER BY stock_code
            LIMIT ?
        """, (limit,))
        stocks = [row[0] for row in cursor.fetchall()]
        conn.close()
        return stocks

    def calculate_pe_ttm(self, stock_code: str, trade_date: str) -> Optional[float]:
        """
        计算 PE_TTM (滚动市盈率)

        PE_TTM = 市值 / 净利润 TTM
        净利润 TTM = 最近 4 个季度的净利润之和
        """
        try:
            # 获取财务数据
            income_df = self.sdk_manager.get_income_statement([stock_code])
            if income_df.empty:
                return None

            # 筛选年报数据 (REPORT_TYPE=1 或 4)
            annual_df = income_df[income_df['REPORT_TYPE'].isin(['1', '4', 1, 4])]
            if annual_df.empty:
                # 尝试使用最近 4 个季度的数据
                income_df = income_df.sort_values('REPORTING_PERIOD', ascending=False)
                recent_4 = income_df.head(4)
                net_profit_ttm = recent_4['NET_PRO_INCL_MIN_INT_INC'].sum()
            else:
                # 使用最新年报
                latest = annual_df.sort_values('REPORTING_PERIOD', ascending=False).iloc[0]
                net_profit_ttm = latest['NET_PRO_INCL_MIN_INT_INC']

            if pd.isna(net_profit_ttm) or net_profit_ttm <= 0:
                return None

            # 获取市值
            market_cap = self.get_market_cap(stock_code, trade_date)
            if not market_cap or market_cap <= 0:
                return None

            # 转换单位：SDK 返回的净利润是元，转换为亿元
            net_profit_yi = net_profit_ttm / 100000000

            pe_ttm = market_cap / net_profit_yi
            return pe_ttm

        except Exception as e:
            print(f"[Fundamental] PE_TTM 计算失败 {stock_code}: {e}")
            return None

    def calculate_pb(self, stock_code: str, trade_date: str) -> Optional[float]:
        """
        计算 PB (市净率)

        PB = 市值 / 净资产
        """
        try:
            # 获取资产负债表
            balance_df = self.sdk_manager.get_balance_sheet([stock_code])
            if balance_df.empty:
                return None

            # 获取最新净资产
            balance_df = balance_df.sort_values('REPORTING_PERIOD', ascending=False)
            latest = balance_df.iloc[0]
            net_assets = latest.get('TOT_SHARE_EQUITY_EXCL_MIN_INT')

            if pd.isna(net_assets) or net_assets <= 0:
                return None

            # 获取市值
            market_cap = self.get_market_cap(stock_code, trade_date)
            if not market_cap or market_cap <= 0:
                return None

            # 转换单位：SDK 返回的净资产是元，转换为亿元
            net_assets_yi = net_assets / 100000000

            pb = market_cap / net_assets_yi
            return pb

        except Exception as e:
            print(f"[Fundamental] PB 计算失败 {stock_code}: {e}")
            return None

    def calculate_ps_ttm(self, stock_code: str, trade_date: str) -> Optional[float]:
        """
        计算 PS_TTM (滚动市销率)

        PS_TTM = 市值 / 营收 TTM
        """
        try:
            # 获取利润表
            income_df = self.sdk_manager.get_income_statement([stock_code])
            if income_df.empty:
                return None

            # 获取最新营收
            income_df = income_df.sort_values('REPORTING_PERIOD', ascending=False)
            latest = income_df.iloc[0]
            revenue_ttm = latest.get('OPERA_REV')

            if pd.isna(revenue_ttm) or revenue_ttm <= 0:
                return None

            # 获取市值
            market_cap = self.get_market_cap(stock_code, trade_date)
            if not market_cap or market_cap <= 0:
                return None

            # 转换单位
            revenue_yi = revenue_ttm / 100000000

            ps_ttm = market_cap / revenue_yi
            return ps_ttm

        except Exception as e:
            print(f"[Fundamental] PS_TTM 计算失败 {stock_code}: {e}")
            return None

    def calculate_pcf(self, stock_code: str, trade_date: str) -> Optional[float]:
        """
        计算 PCF (市现率)

        PCF = 市值 / 经营现金流
        """
        try:
            # 获取现金流量表
            cashflow_df = self.sdk_manager.get_cash_flow_statement([stock_code])
            if cashflow_df.empty:
                return None

            # 获取最新经营现金流
            cashflow_df = cashflow_df.sort_values('REPORTING_PERIOD', ascending=False)
            latest = cashflow_df.iloc[0]
            operating_cashflow = latest.get('NET_CASH_FLOWS_OPERA_ACT')

            if pd.isna(operating_cashflow) or operating_cashflow <= 0:
                return None

            # 获取市值
            market_cap = self.get_market_cap(stock_code, trade_date)
            if not market_cap or market_cap <= 0:
                return None

            # 转换单位
            operating_cashflow_yi = operating_cashflow / 100000000

            pcf = market_cap / operating_cashflow_yi
            return pcf

        except Exception as e:
            print(f"[Fundamental] PCF 计算失败 {stock_code}: {e}")
            return None

    def calculate_roe(self, stock_code: str, trade_date: str) -> Optional[float]:
        """
        计算 ROE (净资产收益率)

        ROE = 净利润 / 净资产 * 100
        """
        try:
            # 获取利润表
            income_df = self.sdk_manager.get_income_statement([stock_code])
            # 获取资产负债表
            balance_df = self.sdk_manager.get_balance_sheet([stock_code])

            if income_df.empty or balance_df.empty:
                return None

            # 获取最新净利润
            income_df = income_df.sort_values('REPORTING_PERIOD', ascending=False)
            net_profit = income_df.iloc[0].get('NET_PRO_EXCL_MIN_INT_INC')

            # 获取最新净资产
            balance_df = balance_df.sort_values('REPORTING_PERIOD', ascending=False)
            net_assets = balance_df.iloc[0].get('TOT_SHARE_EQUITY_EXCL_MIN_INT')

            if pd.isna(net_profit) or pd.isna(net_assets) or net_assets <= 0:
                return None

            roe = (net_profit / net_assets) * 100
            return roe

        except Exception as e:
            print(f"[Fundamental] ROE 计算失败 {stock_code}: {e}")
            return None

    def calculate_roa(self, stock_code: str, trade_date: str) -> Optional[float]:
        """
        计算 ROA (总资产收益率)

        ROA = 净利润 / 总资产 * 100
        """
        try:
            # 获取利润表和资产负债表
            income_df = self.sdk_manager.get_income_statement([stock_code])
            balance_df = self.sdk_manager.get_balance_sheet([stock_code])

            if income_df.empty or balance_df.empty:
                return None

            # 获取最新净利润
            income_df = income_df.sort_values('REPORTING_PERIOD', ascending=False)
            net_profit = income_df.iloc[0].get('NET_PRO_INCL_MIN_INT_INC')

            # 获取最新总资产
            balance_df = balance_df.sort_values('REPORTING_PERIOD', ascending=False)
            total_assets = balance_df.iloc[0].get('TOT_ASSETS')

            if pd.isna(net_profit) or pd.isna(total_assets) or total_assets <= 0:
                return None

            roa = (net_profit / total_assets) * 100
            return roa

        except Exception as e:
            print(f"[Fundamental] ROA 计算失败 {stock_code}: {e}")
            return None

    def calculate_gross_margin(self, stock_code: str, trade_date: str) -> Optional[float]:
        """
        计算毛利率

        毛利率 = (营收 - 营业成本) / 营收 * 100
        """
        try:
            income_df = self.sdk_manager.get_income_statement([stock_code])
            if income_df.empty:
                return None

            latest = income_df.sort_values('REPORTING_PERIOD', ascending=False).iloc[0]
            revenue = latest.get('OPERA_REV')
            operating_cost = latest.get('OPERA_COST')

            if pd.isna(revenue) or pd.isna(operating_cost) or revenue <= 0:
                return None

            gross_margin = ((revenue - operating_cost) / revenue) * 100
            return gross_margin

        except Exception as e:
            print(f"[Fundamental] 毛利率计算失败 {stock_code}: {e}")
            return None

    def calculate_net_margin(self, stock_code: str, trade_date: str) -> Optional[float]:
        """
        计算净利率

        净利率 = 净利润 / 营收 * 100
        """
        try:
            income_df = self.sdk_manager.get_income_statement([stock_code])
            if income_df.empty:
                return None

            latest = income_df.sort_values('REPORTING_PERIOD', ascending=False).iloc[0]
            net_profit = latest.get('NET_PRO_INCL_MIN_INT_INC')
            revenue = latest.get('OPERA_REV')

            if pd.isna(net_profit) or pd.isna(revenue) or revenue <= 0:
                return None

            net_margin = (net_profit / revenue) * 100
            return net_margin

        except Exception as e:
            print(f"[Fundamental] 净利率计算失败 {stock_code}: {e}")
            return None

    def calculate_growth_rates(
        self,
        stock_code: str,
        trade_date: str
    ) -> Dict[str, Optional[float]]:
        """
        计算成长类指标

        Returns:
            {
                'revenue_growth_yoy': 营收同比增长率,
                'revenue_growth_qoq': 营收环比增长率,
                'net_profit_growth_yoy': 净利润同比增长率,
                'net_profit_growth_qoq': 净利润环比增长率
            }
        """
        result = {
            'revenue_growth_yoy': None,
            'revenue_growth_qoq': None,
            'net_profit_growth_yoy': None,
            'net_profit_growth_qoq': None
        }

        try:
            income_df = self.sdk_manager.get_income_statement([stock_code])
            if income_df.empty:
                return result

            # 按报告期排序
            income_df = income_df.sort_values('REPORTING_PERIOD', ascending=False)

            # 获取最新 3 期数据
            if len(income_df) < 3:
                return result

            latest = income_df.iloc[0]
            prev_quarter = income_df.iloc[1] if len(income_df) > 1 else None
            prev_year = income_df.iloc[4] if len(income_df) > 4 else None

            # 营收同比增长 (YoY)
            if prev_year is not None:
                revenue_latest = latest.get('OPERA_REV')
                revenue_prev = prev_year.get('OPERA_REV')
                if not pd.isna(revenue_latest) and not pd.isna(revenue_prev) and revenue_prev > 0:
                    result['revenue_growth_yoy'] = ((revenue_latest - revenue_prev) / revenue_prev) * 100

            # 营收环比增长 (QoQ)
            if prev_quarter is not None:
                revenue_latest = latest.get('OPERA_REV')
                revenue_prev = prev_quarter.get('OPERA_REV')
                if not pd.isna(revenue_latest) and not pd.isna(revenue_prev) and revenue_prev > 0:
                    result['revenue_growth_qoq'] = ((revenue_latest - revenue_prev) / revenue_prev) * 100

            # 净利润同比增长 (YoY)
            if prev_year is not None:
                net_profit_latest = latest.get('NET_PRO_INCL_MIN_INT_INC')
                net_profit_prev = prev_year.get('NET_PRO_INCL_MIN_INT_INC')
                if not pd.isna(net_profit_latest) and not pd.isna(net_profit_prev) and net_profit_prev != 0:
                    result['net_profit_growth_yoy'] = ((net_profit_latest - net_profit_prev) / abs(net_profit_prev)) * 100

            # 净利润环比增长 (QoQ)
            if prev_quarter is not None:
                net_profit_latest = latest.get('NET_PRO_INCL_MIN_INT_INC')
                net_profit_prev = prev_quarter.get('NET_PRO_INCL_MIN_INT_INC')
                if not pd.isna(net_profit_latest) and not pd.isna(net_profit_prev) and net_profit_prev != 0:
                    result['net_profit_growth_qoq'] = ((net_profit_latest - net_profit_prev) / abs(net_profit_prev)) * 100

        except Exception as e:
            print(f"[Fundamental] 成长率计算失败 {stock_code}: {e}")

        return result

    def calculate_all_fundamental_factors(
        self,
        stock_code: str,
        trade_date: str
    ) -> Dict[str, Optional[float]]:
        """
        计算所有基本面因子

        Returns:
            包含所有基本面因子的字典
        """
        result = {}

        # 估值类
        result['pe_ttm'] = self.calculate_pe_ttm(stock_code, trade_date)
        result['pb'] = self.calculate_pb(stock_code, trade_date)
        result['ps_ttm'] = self.calculate_ps_ttm(stock_code, trade_date)
        result['pcf'] = self.calculate_pcf(stock_code, trade_date)

        # 盈利类
        result['roe'] = self.calculate_roe(stock_code, trade_date)
        result['roa'] = self.calculate_roa(stock_code, trade_date)
        result['gross_margin'] = self.calculate_gross_margin(stock_code, trade_date)
        result['net_margin'] = self.calculate_net_margin(stock_code, trade_date)

        # 成长类
        growth = self.calculate_growth_rates(stock_code, trade_date)
        result.update(growth)

        return result


if __name__ == "__main__":
    # 测试代码
    calculator = FundamentalFactorCalculator()

    test_stock = "000001.SZ"
    test_date = "2026-04-03"

    print(f"测试股票：{test_stock}, 日期：{test_date}")

    factors = calculator.calculate_all_fundamental_factors(test_stock, test_date)

    print("\n基本面因子结果:")
    for key, value in factors.items():
        print(f"  {key}: {value}")
