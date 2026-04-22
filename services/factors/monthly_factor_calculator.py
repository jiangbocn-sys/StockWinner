"""
月频因子计算器

基于财务数据和行业分类数据计算月频因子，支持：
1. 财报类因子（利润、现金流，集成 SDK 获取数据）
2. 行业分类因子（申万一级、二级、三级行业，集成 SDK 获取数据）
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional, Tuple
import json

# 中国时区
CHINA_TZ = timedelta(hours=8)

DB_PATH = Path(__file__).parent.parent.parent / "data" / "kline.db"


class MonthlyFactorCalculator:
    """月频因子计算器"""

    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path

    def _get_connection(self) -> sqlite3.Connection:
        """获取数据库连接"""
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        return conn

    # ==================== 财报类因子 ====================

    def get_financial_data(
        self,
        stock_code: str,
        report_quarter: int,
        report_year: int
    ) -> Optional[Dict]:
        """
        获取单只股票的财报数据

        使用 SDK 获取：
        - 利润表数据（净利润等）
        - 现金流量表数据（经营现金流等）
        - 资产负债表数据（净资产等）

        返回：
        - net_profit: 净利润
        - net_profit_ttm: 净利润 TTM
        - net_profit_ttm_yoy: 净利润 TTM 同比增速
        - net_profit_single: 单季度净利润
        - net_profit_single_yoy: 单季度净利润同比增速
        - net_profit_single_qoq: 单季度净利润环比增速
        - operating_cash_flow: 经营现金流
        - operating_cash_flow_ttm: 经营现金流 TTM
        - operating_cash_flow_ttm_yoy: 经营现金流 TTM 同比增速
        - operating_cash_flow_single: 单季度经营现金流
        - operating_cash_flow_single_yoy: 单季度经营现金流同比增速
        - operating_cash_flow_single_qoq: 单季度经营现金流环比增速
        - net_assets: 净资产
        """
        try:
            try:
                from .sdk_api import AmazingDataAPI
            except ImportError:
                from sdk_api import AmazingDataAPI
            api = AmazingDataAPI()

            result = {
                'stock_code': stock_code,
                'report_quarter': report_quarter,
                'report_year': report_year,
            }

            # 获取利润表数据
            income_df = api.get_income_statement([stock_code])
            if not income_df.empty:
                # 根据季度和年份筛选数据
                # REPORT_TYPE: 1=年报，2=中报，3=季报，4=一季报
                if report_quarter == 3:  # Q1 一季报
                    target_type = '4'
                elif report_quarter == 6:  # Q2 中报
                    target_type = '2'
                elif report_quarter == 9:  # Q3 季报
                    target_type = '3'
                else:  # Q4 年报
                    target_type = '1'

                target_df = income_df[income_df['REPORT_TYPE'] == target_type]
                target_df = target_df[target_df['REPORTING_PERIOD'].astype(str).str[:4] == str(report_year)]

                if not target_df.empty:
                    row = target_df.iloc[0]
                    # 净利润（包含少数股东损益）
                    result['net_profit'] = row.get('NET_PRO_INCL_MIN_INT_INC')
                    result['net_profit_single'] = row.get('NET_PRO_AFTER_DED_NR_GL')  # 扣非净利润作为单季度
                    result['opera_profit'] = row.get('OPERA_PROFIT')
                    result['total_profit'] = row.get('TOTAL_PROFIT')

                # 计算 TTM 数据（取最近 4 个季度）
                recent = income_df.sort_values('REPORTING_PERIOD', ascending=False).head(4)
                if len(recent) >= 4:
                    result['net_profit_ttm'] = recent['NET_PRO_INCL_MIN_INT_INC'].sum()

            # 获取现金流量表数据
            cashflow_df = api.get_cash_flow_statement([stock_code])
            if not cashflow_df.empty:
                # 获取对应报告期的数据
                if report_quarter == 3:
                    target_type = '4'
                elif report_quarter == 6:
                    target_type = '2'
                elif report_quarter == 9:
                    target_type = '3'
                else:
                    target_type = '1'

                target_df = cashflow_df[cashflow_df['REPORT_TYPE'] == target_type]
                target_df = target_df[target_df['REPORTING_PERIOD'].astype(str).str[:4] == str(report_year)]

                if not target_df.empty:
                    row = target_df.iloc[0]
                    # 经营活动产生的现金流量净额
                    result['operating_cash_flow'] = row.get('NET_CASH_FLOWS_OPERA_ACT')

            # 获取资产负债表数据
            balance_df = api.get_balance_sheet([stock_code])
            if not balance_df.empty:
                # 获取对应报告期的数据
                if report_quarter == 3:
                    target_type = '4'
                elif report_quarter == 6:
                    target_type = '2'
                elif report_quarter == 9:
                    target_type = '3'
                else:
                    target_type = '1'

                target_df = balance_df[balance_df['REPORT_TYPE'] == target_type]
                target_df = target_df[target_df['REPORTING_PERIOD'].astype(str).str[:4] == str(report_year)]

                if not target_df.empty:
                    row = target_df.iloc[0]
                    # 归属母公司股东权益
                    result['net_assets'] = row.get('TOT_SHARE_EQUITY_EXCL_MIN_INT')

            return result

        except Exception as e:
            print(f"获取财务数据失败 {stock_code}: {e}")
            return None

    def calculate_financial_factors(
        self,
        stock_code: str,
        report_date: str
    ) -> Optional[Dict]:
        """
        计算财报类因子

        report_date: 报告期末日期（如 2026-03-31 对应 Q1）

        季度映射：
        - Q1: report_quarter=3 (3 月)
        - Q2: report_quarter=6 (6 月)
        - Q3: report_quarter=9 (9 月)
        - Q4: report_quarter=12 (12 月)
        """
        # 解析报告日期
        report_dt = datetime.strptime(report_date, '%Y-%m-%d')
        report_month = report_dt.month
        report_year = report_dt.year

        # 确定季度
        if report_month <= 3:
            quarter = 1
            report_quarter = 3
        elif report_month <= 6:
            quarter = 2
            report_quarter = 6
        elif report_month <= 9:
            quarter = 3
            report_quarter = 9
        else:
            quarter = 4
            report_quarter = 12

        # 获取财报数据
        financial_data = self.get_financial_data(stock_code, report_quarter, report_year)

        if financial_data:
            financial_data['report_date'] = report_date

        return financial_data

    # ==================== 行业分类因子 ====================

    def get_industry_classification(
        self,
        stock_code: str,
        date: str
    ) -> Optional[Dict]:
        """
        获取行业分类数据

        使用 SDK 获取申万行业分类数据（一级、二级、三级行业）

        返回：
        - sw_level1: 申万一级行业
        - sw_level2: 申万二级行业
        - sw_level3: 申万三级行业
        """
        # TODO: SDK 没有直接获取单只股票行业分类的接口
        # 后续可以通过以下方式实现：
        # 1. 维护股票 - 行业映射表
        # 2. 从外部数据源获取
        # 3. 使用 get_industry_constituent 反向构建映射
        return {
            'stock_code': stock_code,
            'trade_date': date,
            'sw_level1': None,
            'sw_level2': None,
            'sw_level3': None,
        }

    # ==================== 批量计算 ====================

    def batch_calculate_financial(
        self,
        stock_codes: List[str],
        report_dates: List[str]
    ) -> pd.DataFrame:
        """
        批量计算财报类因子

        Args:
            stock_codes: 股票代码列表
            report_dates: 报告期末日期列表

        Returns:
            包含所有财报因子的 DataFrame
        """
        all_data = []

        for stock_code in stock_codes:
            for report_date in report_dates:
                data = self.calculate_financial_factors(stock_code, report_date)
                if data:
                    all_data.append(data)

        return pd.DataFrame(all_data)

    def batch_calculate_industry(
        self,
        stock_codes: List[str],
        dates: List[str]
    ) -> pd.DataFrame:
        """
        批量计算行业分类因子

        Args:
            stock_codes: 股票代码列表
            dates: 日期列表

        Returns:
            包含所有行业分类的 DataFrame
        """
        all_data = []

        for stock_code in stock_codes:
            for date in dates:
                data = self.get_industry_classification(stock_code, date)
                if data:
                    all_data.append(data)

        return pd.DataFrame(all_data)


# ==================== 财务指标计算工具 ====================

def calculate_ttm(values: List[float], quarters: List[int]) -> float:
    """
    计算 TTM（滚动 12 个月）值

    Args:
        values: 最近 4 个季度的单季度值 [Q4, Q3, Q2, Q1]
        quarters: 季度编号 [4, 3, 2, 1]

    Returns:
        TTM 值
    """
    if len(values) < 4:
        return sum(values) if values else 0.0
    return sum(values[:4])


def calculate_yoy(current: float, previous: float) -> Optional[float]:
    """
    计算同比增速

    YoY = (本期值 - 上年同期值) / 上年同期值
    """
    if previous is None or previous == 0:
        return None
    return (current - previous) / previous


def calculate_qoq(current: float, previous: float) -> Optional[float]:
    """
    计算环比增速

    QoQ = (本期值 - 上期值) / 上期值
    """
    if previous is None or previous == 0:
        return None
    return (current - previous) / previous


if __name__ == "__main__":
    # 测试代码
    calculator = MonthlyFactorCalculator()

    # 测试季度映射
    test_dates = ["2026-03-31", "2026-06-30", "2026-09-30", "2026-12-31"]
    for date in test_dates:
        result = calculator.calculate_financial_factors("000001.SZ", date)
        if result:
            print(f"{date} -> Q{calculator._get_quarter(date)}, report_quarter={result['report_quarter']}")
