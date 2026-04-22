"""
AmazingData SDK API 封装

提供股本数据、财务数据的获取能力，支持：
1. 股本结构数据（总股本、流通股本）
2. 利润表数据（净利润、营业收入等）
3. 现金流量表数据（经营现金流等）
4. 资产负债表数据（净资产等）

注意：此模块保留用于向后兼容。
新代码应该使用 services/common/sdk_manager.py 中的 SDKManager。
"""

import sqlite3
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from datetime import datetime
import pandas as pd

# 导入统一的 SDK 管理器
from services.common.sdk_manager import get_sdk_manager, SDK_USERNAME, SDK_PASSWORD, SDK_HOST, SDK_PORT

DB_PATH = Path(__file__).parent.parent.parent / "data" / "stockwinner.db"


class AmazingDataAPI:
    """AmazingData SDK API 封装类（向后兼容）"""

    def __init__(self):
        self._sdk_manager = get_sdk_manager()

    def get_equity_structure(self, stock_codes: List[str]) -> pd.DataFrame:
        """获取股本结构数据"""
        return self._sdk_manager.get_equity_structure(stock_codes)

    def get_income_statement(self, stock_codes: List[str]) -> pd.DataFrame:
        """获取利润表数据"""
        return self._sdk_manager.get_income_statement(stock_codes)

    def get_cash_flow_statement(self, stock_codes: List[str]) -> pd.DataFrame:
        """获取现金流量表数据"""
        return self._sdk_manager.get_cash_flow_statement(stock_codes)

    def get_balance_sheet(self, stock_codes: List[str]) -> pd.DataFrame:
        """获取资产负债表数据"""
        return self._sdk_manager.get_balance_sheet(stock_codes)

    def get_industry_base_info(self) -> pd.DataFrame:
        """获取行业分类数据"""
        return self._sdk_manager.get_industry_base_info()

    def get_market_data(self, stock_codes: List[str], start_date: str, end_date: str) -> pd.DataFrame:
        """获取行情数据（使用 SDKManager 缓存，避免重复连接）"""
        try:
            # 使用 SDKManager 获取 MarketData 实例
            market = self._sdk_manager.get_market_data()
            result_dict = market.get_kline_data(
                stock_codes,
                start_date=start_date,
                end_date=end_date,
                is_local=False
            )
            dfs = []
            for code, df in result_dict.items():
                dfs.append(df)
            if dfs:
                return pd.concat(dfs, ignore_index=True)
            return pd.DataFrame()
        except Exception as e:
            print(f"获取行情数据失败：{e}")
            return pd.DataFrame()


def get_stock_list_from_db(db_path: Path = Path(__file__).parent.parent.parent / "data" / "kline.db") -> List[str]:
    """从数据库获取股票列表"""
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT stock_code FROM kline_data ORDER BY stock_code")
    stocks = [row[0] for row in cursor.fetchall()]
    conn.close()
    return stocks


if __name__ == "__main__":
    # 测试代码
    api = AmazingDataAPI()

    # 测试获取股本数据
    print("测试获取股本结构数据...")
    equity = api.get_equity_structure(["689009.SH"])
    print(f"股本数据条数：{len(equity)}")
    if len(equity) > 0:
        print(equity[['MARKET_CODE', 'ANN_DATE', 'TOT_SHARE', 'FLOAT_SHARE']].head(5))

    # 测试获取利润表
    print("\n测试获取利润表数据...")
    income = api.get_income_statement(["689009.SH"])
    print(f"利润表数据条数：{len(income)}")

    # 测试获取现金流
    print("\n测试获取现金流数据...")
    cashflow = api.get_cash_flow_statement(["689009.SH"])
    print(f"现金流数据条数：{len(cashflow)}")

    # 测试获取资产负债表
    print("\n测试获取资产负债表数据...")
    balance = api.get_balance_sheet(["689009.SH"])
    print(f"资产负债表数据条数：{len(balance)}")
