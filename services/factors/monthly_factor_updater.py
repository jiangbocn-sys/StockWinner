"""
月频因子更新服务 - 优化版本

从SDK获取财务报表数据，计算估值因子和盈利因子，更新到 stock_monthly_factors 表

优化点：
1. 跳过北交所股票（BJ市场无财务数据）
2. 支持更新近3年数据（约12个报告期）
3. 记录无数据股票到日志文件供复查
4. 增加批次大小到200只
5. 批量更新数据库减少IO
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional, Tuple, Set
import json

# 使用统一的SDK管理器
from services.common.sdk_manager import get_sdk_manager
from services.common.timezone import get_china_time

# 数据库路径
DB_PATH = Path(__file__).parent.parent.parent / "data" / "kline.db"
WINNER_DB_PATH = Path(__file__).parent.parent.parent / "data" / "stockwinner.db"
LOG_PATH = Path(__file__).parent.parent.parent / "logs" / "missing_financial_data.log"


class MonthlyFactorUpdater:
    """月频因子更新器 - 优化版本"""

    # 无财务数据的股票集合（缓存，避免重复尝试）
    _missing_data_stocks: Set[str] = set()

    # 全部可写字段（41个，排除 id/stock_code/stock_name/report_date/source/created_at/updated_at）
    ALL_WRITABLE_FIELDS = [
        'net_profit', 'net_profit_ttm', 'net_profit_ttm_yoy',
        'net_profit_single', 'net_profit_single_yoy', 'net_profit_single_qoq',
        'operating_cash_flow', 'operating_cash_flow_ttm', 'operating_cash_flow_ttm_yoy',
        'operating_cash_flow_single', 'operating_cash_flow_single_yoy', 'operating_cash_flow_single_qoq',
        'total_revenue', 'operating_profit', 'operating_cashflow',
        'total_assets', 'net_assets',
        'pe_ttm', 'pb', 'ps_ttm', 'pcf', 'ev_ebitda',
        'pe_inverse', 'pb_inverse',
        'roe', 'roa', 'gross_margin', 'net_margin', 'operating_margin',
        'revenue_growth_yoy', 'revenue_growth_qoq',
        'net_profit_growth_yoy', 'net_profit_growth_qoq',
        'operating_profit_growth_yoy',
        'report_quarter', 'report_year',
        'sw_level1', 'sw_level2', 'sw_level3',
        'total_market_cap', 'circ_market_cap',
    ]

    # 空值检查字段（用于判断是否需要更新）
    NULL_CHECK_FIELDS = [
        'pe_ttm', 'pb', 'ps_ttm', 'pcf', 'ev_ebitda',
        'net_profit_ttm', 'net_profit_ttm_yoy',
        'net_profit_single', 'net_profit_single_yoy', 'net_profit_single_qoq',
        'operating_cash_flow_ttm', 'operating_cash_flow_ttm_yoy',
        'revenue_growth_yoy', 'revenue_growth_qoq',
        'net_profit_growth_yoy', 'net_profit_growth_qoq',
        'operating_profit_growth_yoy',
        'report_quarter', 'report_year',
        'sw_level1', 'sw_level2', 'sw_level3',
    ]

    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path
        self.winner_db_path = WINNER_DB_PATH
        self.sdk = get_sdk_manager()
        self._load_missing_stocks_cache()

    @staticmethod
    def _safe_is_zero(value) -> bool:
        """None/0/NaN/Inf 都视为零"""
        if value is None:
            return True
        if isinstance(value, (int, float)):
            if np.isnan(value) or np.isinf(value):
                return True
            return abs(value) < 1e-10
        return True

    @staticmethod
    def is_quarter_end(report_date: str) -> bool:
        """是否季末日期（03-31, 06-30, 09-30, 12-31）"""
        return report_date[5:] in ('03-31', '06-30', '09-30', '12-31')

    @staticmethod
    def calculate_report_quarter(report_date: str) -> int:
        """从 report_date 提取季度号：01-03月→1, 04-06月→2, 07-09月→3, 10-12月→4"""
        month = int(report_date[5:7])
        return (month - 1) // 3 + 1

    @staticmethod
    def calculate_report_year(report_date: str) -> int:
        """从 report_date 提取年份"""
        return int(report_date[:4])

    def _get_connection(self) -> sqlite3.Connection:
        """获取数据库连接"""
        from services.common.database import configure_kline_connection
        conn = sqlite3.connect(str(self.db_path), timeout=60)
        configure_kline_connection(conn)
        conn.row_factory = sqlite3.Row
        return conn

    def _load_missing_stocks_cache(self):
        """加载无数据股票缓存"""
        if LOG_PATH.exists():
            try:
                with open(LOG_PATH, 'r') as f:
                    for line in f:
                        if line.strip() and ':' in line:
                            stock_code = line.split(':')[0].strip()
                            self._missing_data_stocks.add(stock_code)
                print(f"[MonthlyFactor] 已加载 {len(self._missing_data_stocks)} 只无数据股票缓存")
            except:
                pass

    def _log_missing_data(self, stock_code: str, stock_name: str, report_period: str, reason: str):
        """记录无财务数据的股票到日志"""
        self._missing_data_stocks.add(stock_code)
        with open(LOG_PATH, 'a') as f:
            f.write(f"{stock_code}: {stock_name}, {report_period}, {reason}\n")

    def get_stocks_need_update(self, skip_bj: bool = True) -> List[str]:
        """获取需要更新月频因子的股票列表（任意关键字段为空即需要更新）"""
        conn = self._get_connection()
        cursor = conn.cursor()

        # 检查所有关键字段是否有 NULL
        null_conditions = ' OR '.join([f"{f} IS NULL" for f in self.NULL_CHECK_FIELDS])
        sql = f"SELECT DISTINCT stock_code FROM stock_monthly_factors WHERE ({null_conditions})"
        # 只处理季度记录
        sql += " AND strftime('%m-%d', report_date) IN ('03-31', '06-30', '09-30', '12-31')"
        if skip_bj:
            sql += " AND stock_code NOT LIKE '%BJ'"
        cursor.execute(sql)
        stocks = [row[0] for row in cursor.fetchall()]

        # 排除已知无数据的股票
        stocks = [s for s in stocks if s not in self._missing_data_stocks]

        conn.close()
        return stocks

    def get_all_stocks(self, skip_bj: bool = True) -> List[str]:
        """获取所有股票列表（仅季度记录，用于补充缺失的季度记录）"""
        conn = self._get_connection()
        cursor = conn.cursor()

        sql = "SELECT DISTINCT stock_code FROM stock_monthly_factors WHERE strftime('%m-%d', report_date) IN ('03-31', '06-30', '09-30', '12-31')"
        if skip_bj:
            sql += " AND stock_code NOT LIKE '%BJ'"
        cursor.execute(sql)
        stocks = [row[0] for row in cursor.fetchall()]

        # 排除已知无数据的股票
        stocks = [s for s in stocks if s not in self._missing_data_stocks]

        conn.close()
        return stocks

    def get_stock_names(self) -> Dict[str, str]:
        """获取股票名称映射"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT stock_code, stock_name FROM stock_monthly_factors")
        names = {row[0]: row[1] for row in cursor.fetchall()}
        conn.close()
        return names

    def get_latest_report_period(self) -> str:
        """获取最新报告期"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(report_date) FROM stock_monthly_factors")
        latest = cursor.fetchone()[0]
        conn.close()
        # 转换为SDK格式（如 2026-03-31 -> 20260331）
        return latest.replace('-', '')

    def get_report_periods_last_3_years(self) -> List[str]:
        """获取近3年的报告期列表（共12个季度）"""
        latest = self.get_latest_report_period()
        latest_year = int(latest[:4])
        latest_month = int(latest[4:6])  # 03, 06, 09, 12

        periods = []
        for year in range(latest_year, latest_year - 3, -1):
            for month in [12, 9, 6, 3]:  # Q4, Q3, Q2, Q1
                if year == latest_year and month > latest_month:
                    continue  # 跳过未到期的季度
                day = 31 if month in [3, 12] else 30
                periods.append(f"{year}{month:02d}{day}")

        return periods

    def get_market_cap_batch(self, stock_codes: List[str], trade_date: str, lookback_days: int = 20) -> Dict[str, Dict]:
        """批量获取市值数据（如果报告日期非交易日，回溯查找最近交易日）"""
        conn = self._get_connection()
        cursor = conn.cursor()

        # 先尝试直接匹配报告日期
        cursor.execute("""
            SELECT stock_code, total_market_cap, circ_market_cap
            FROM stock_daily_factors
            WHERE trade_date = ? AND total_market_cap > 0
            AND stock_code IN ({})
        """.format(','.join(['?'] * len(stock_codes))), [trade_date] + stock_codes)

        result = {}
        for row in cursor.fetchall():
            result[row[0]] = {
                'total_market_cap': row[1],
                'circ_market_cap': row[2]
            }

        # 如果直接匹配无数据（报告日期可能是非交易日），回溯查找
        missing_stocks = [s for s in stock_codes if s not in result]
        if missing_stocks and lookback_days > 0:
            # 找到最近有数据的交易日
            for days_back in range(1, lookback_days + 1):
                from datetime import datetime, timedelta
                try:
                    dt = datetime.strptime(trade_date, '%Y-%m-%d') - timedelta(days=days_back)
                    prev_date = dt.strftime('%Y-%m-%d')
                except:
                    continue

                cursor.execute("""
                    SELECT stock_code, total_market_cap, circ_market_cap
                    FROM stock_daily_factors
                    WHERE trade_date = ? AND total_market_cap > 0
                    AND stock_code IN ({})
                """.format(','.join(['?'] * len(missing_stocks))), [prev_date] + missing_stocks)

                found_in_batch = []
                for row in cursor.fetchall():
                    if row[0] not in result:
                        result[row[0]] = {
                            'total_market_cap': row[1],
                            'circ_market_cap': row[2],
                            'market_cap_date': prev_date  # 标记市值来源日期
                        }
                        found_in_batch.append(row[0])

                # 更新缺失列表
                missing_stocks = [s for s in missing_stocks if s not in found_in_batch]
                if not missing_stocks:
                    break

        conn.close()
        return result

    def fetch_financial_data_from_sdk(self, stock_codes: List[str]) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """从SDK获取财务报表数据"""
        print(f"[MonthlyFactor] 从SDK获取 {len(stock_codes)} 只股票的财务数据...")

        income_df = self.sdk.get_income_statement(stock_codes)
        balance_df = self.sdk.get_balance_sheet(stock_codes)
        cashflow_df = self.sdk.get_cash_flow_statement(stock_codes)

        return income_df, balance_df, cashflow_df

    def _load_sw_classification(self, stock_codes: List[str]) -> Dict[str, Dict[str, str]]:
        """从 stock_base_info 表加载申万行业分类"""
        try:
            from services.common.database import get_sync_connection
            conn = get_sync_connection()
            placeholders = ','.join(['?'] * len(stock_codes))
            cursor = conn.execute(f"""
                SELECT stock_code, sw_level1, sw_level2, sw_level3
                FROM stock_base_info
                WHERE stock_code IN ({placeholders})
            """, stock_codes)
            result = {}
            for row in cursor.fetchall():
                result[row['stock_code']] = {
                    'sw_level1': row['sw_level1'],
                    'sw_level2': row['sw_level2'],
                    'sw_level3': row['sw_level3'],
                }
            return result
        except Exception:
            return {}

    def calculate_factors(
        self,
        stock_code: str,
        report_period: str,
        income_df: pd.DataFrame,
        balance_df: pd.DataFrame,
        cashflow_df: pd.DataFrame,
        market_cap: Optional[Dict] = None
    ) -> Optional[Dict]:
        """计算单只股票的月频因子"""

        # 筛选该股票该报告期的数据
        income_data = income_df[income_df['MARKET_CODE'] == stock_code]
        income_data = income_data[income_data['REPORTING_PERIOD'].astype(str) == str(report_period)]

        balance_data = balance_df[balance_df['MARKET_CODE'] == stock_code]
        balance_data = balance_data[balance_data['REPORTING_PERIOD'].astype(str) == str(report_period)]

        cashflow_data = cashflow_df[cashflow_df['MARKET_CODE'] == stock_code]
        cashflow_data = cashflow_data[cashflow_data['REPORTING_PERIOD'].astype(str) == str(report_period)]

        if income_data.empty and balance_data.empty:
            return None

        factors = {}

        # 从利润表提取数据
        if not income_data.empty:
            row = income_data.iloc[0]
            factors['net_profit'] = row.get('NET_PRO_INCL_MIN_INT_INC')
            factors['total_revenue'] = row.get('TOT_OPERA_REV')
            factors['operating_profit'] = row.get('OPERA_PROFIT')
            operating_cost = row.get('TOT_OPERA_COST')

            if factors.get('total_revenue') and not self._safe_is_zero(operating_cost):
                factors['gross_margin'] = (factors['total_revenue'] - operating_cost) / factors['total_revenue'] * 100

            if factors.get('net_profit') and factors.get('total_revenue') and not self._safe_is_zero(factors['total_revenue']):
                factors['net_margin'] = factors['net_profit'] / factors['total_revenue'] * 100

            if factors.get('operating_profit') and factors.get('total_revenue') and not self._safe_is_zero(factors['total_revenue']):
                factors['operating_margin'] = factors['operating_profit'] / factors['total_revenue'] * 100

        # 从资产负债表提取数据
        if not balance_data.empty:
            row = balance_data.iloc[0]
            factors['total_assets'] = row.get('TOTAL_ASSETS')
            factors['net_assets'] = row.get('TOT_SHARE_EQUITY_EXCL_MIN_INT')

            if factors.get('net_profit') and factors.get('net_assets') and not self._safe_is_zero(factors['net_assets']):
                factors['roe'] = factors['net_profit'] / factors['net_assets'] * 100

            if factors.get('net_profit') and factors.get('total_assets') and not self._safe_is_zero(factors['total_assets']):
                factors['roa'] = factors['net_profit'] / factors['total_assets'] * 100

        # 从现金流量表提取数据
        if not cashflow_data.empty:
            row = cashflow_data.iloc[0]
            factors['operating_cashflow'] = row.get('NET_CASH_FLOWS_OPERA_ACT')

        # 市值数据
        if market_cap:
            total_cap = market_cap.get('total_market_cap')
            if total_cap and total_cap > 0:
                factors['total_market_cap'] = total_cap
                factors['circ_market_cap'] = market_cap.get('circ_market_cap')

                # PE（允许负值，亏损股 PE 为负）
                if factors.get('net_profit') and not self._safe_is_zero(factors['net_profit']):
                    factors['pe_ttm'] = (total_cap * 1e8) / factors['net_profit']
                    factors['pe_inverse'] = factors['net_profit'] / (total_cap * 1e8)

                # PB（允许负值，负净资产 PB 为负）
                if factors.get('net_assets') and not self._safe_is_zero(factors['net_assets']):
                    factors['pb'] = (total_cap * 1e8) / factors['net_assets']
                    factors['pb_inverse'] = factors['net_assets'] / (total_cap * 1e8)

                # PS
                if factors.get('total_revenue') and not self._safe_is_zero(factors['total_revenue']):
                    factors['ps_ttm'] = (total_cap * 1e8) / factors['total_revenue']

                # PCF（允许负值）
                if factors.get('operating_cashflow') and not self._safe_is_zero(factors['operating_cashflow']):
                    factors['pcf'] = (total_cap * 1e8) / factors['operating_cashflow']

        # EV/EBITDA
        if market_cap:
            self._calculate_ev_ebitda(factors, market_cap, income_data, balance_data, cashflow_data)

        return factors if factors else None

    def _calculate_ev_ebitda(self, factors: Dict, market_cap: Dict, income_row, balance_row, cashflow_row):
        """
        计算 EV/EBITDA
        EBITDA = operating_profit + DEPRE_FA_OGA_PBA（折旧摊销）
        EV = mcap(亿)×1e8 + TOTAL_LIAB - CASH_EQUIV
        """
        total_cap = market_cap.get('total_market_cap')
        if not total_cap or total_cap <= 0:
            return

        operating_profit = factors.get('operating_profit')
        if operating_profit is None or self._safe_is_zero(operating_profit):
            return

        # 折旧摊销（现金流量表）
        depre_amort = None
        if not cashflow_row.empty:
            depre_amort = cashflow_row.iloc[0].get('DEPRE_FA_OGA_PBA')
        if depre_amort is None or self._safe_is_zero(depre_amort):
            return

        ebitda = operating_profit + depre_amort
        if self._safe_is_zero(ebitda):
            return

        # 总负债和现金等价物（资产负债表）
        total_liab = None
        cash_equiv = None
        if not balance_row.empty:
            total_liab = balance_row.iloc[0].get('TOTAL_LIAB')
            cash_equiv = balance_row.iloc[0].get('CASH_EQUIV')
        if total_liab is None:
            return

        ev = total_cap * 1e8 + total_liab - (cash_equiv or 0)
        factors['ev_ebitda'] = ev / ebitda

    def calculate_all_growth_factors(self, stock_code: str, income_df: pd.DataFrame, cashflow_df: pd.DataFrame) -> Dict:
        """
        计算所有增长因子：TTM、单季、同比、环比

        返回包含：
        - net_profit_ttm, net_profit_ttm_yoy
        - net_profit_single, net_profit_single_yoy, net_profit_single_qoq
        - revenue_growth_yoy, revenue_growth_qoq
        - net_profit_growth_yoy, net_profit_growth_qoq
        - operating_profit_growth_yoy
        - operating_cash_flow_ttm, operating_cash_flow_ttm_yoy
        - operating_cash_flow_single, operating_cash_flow_single_yoy, operating_cash_flow_single_qoq
        """
        factors = {}

        # --- 利润表增长 ---
        inc = income_df[income_df['MARKET_CODE'] == stock_code].copy()
        if len(inc) >= 2:
            inc = inc.sort_values('REPORTING_PERIOD', ascending=True)

            current = inc.iloc[-1]
            previous = inc.iloc[-2]
            current_period_str = str(int(current['REPORTING_PERIOD']))
            last_year_period = f"{int(current_period_str[:4]) - 1}{current_period_str[4:]}"

            # 净利润 TTM（最近4个季度之和）
            last_4 = inc.tail(4)
            if len(last_4) == 4:
                np_col = 'NET_PRO_INCL_MIN_INT_INC'
                ttm = last_4[np_col].sum()
                if not self._safe_is_zero(ttm):
                    factors['net_profit_ttm'] = ttm

                    # TTM 同比
                    if len(inc) >= 8:
                        prior_4 = inc.iloc[-8:-4]
                        prior_ttm = prior_4[np_col].sum()
                        if not self._safe_is_zero(prior_ttm):
                            factors['net_profit_ttm_yoy'] = (ttm - prior_ttm) / abs(prior_ttm) * 100

            # 单季净利润 = 当期累计 - 上期累计
            curr_np = current.get('NET_PRO_INCL_MIN_INT_INC')
            prev_np = previous.get('NET_PRO_INCL_MIN_INT_INC')
            if curr_np is not None and prev_np is not None:
                single_np = curr_np - prev_np
                factors['net_profit_single'] = single_np

                # 单季环比（需要3期数据）
                if len(inc) >= 3:
                    prior = inc.iloc[-3]
                    prior_np = prior.get('NET_PRO_INCL_MIN_INT_INC')
                    if prior_np is not None:
                        prior_single = prev_np - prior_np
                        if not self._safe_is_zero(prior_single):
                            factors['net_profit_single_qoq'] = (single_np - prior_single) / abs(prior_single) * 100

                # 单季同比（需要上年同期）
                ly = inc[inc['REPORTING_PERIOD'].astype(str) == last_year_period]
                if not ly.empty and len(inc) >= 3:
                    ly_idx = inc.index.get_loc(ly.index[0])
                    if ly_idx > 0:
                        ly_prev = inc.iloc[ly_idx - 1]
                        ly_np = ly.iloc[0].get('NET_PRO_INCL_MIN_INT_INC')
                        ly_prev_np = ly_prev.get('NET_PRO_INCL_MIN_INT_INC')
                        ly_single = ly_np - ly_prev_np
                        if not self._safe_is_zero(ly_single):
                            factors['net_profit_single_yoy'] = (single_np - ly_single) / abs(ly_single) * 100

            # 营收增长：同季度同比（与 operating_profit_growth_yoy 一致）
            curr_rev = current.get('TOT_OPERA_REV')
            ly_rev = inc[inc['REPORTING_PERIOD'].astype(str) == last_year_period]
            if curr_rev is not None and not ly_rev.empty:
                last_rev = ly_rev.iloc[0].get('TOT_OPERA_REV')
                if last_rev is not None and not self._safe_is_zero(last_rev):
                    factors['revenue_growth_yoy'] = (curr_rev - last_rev) / abs(last_rev) * 100

                # 环比（单季）
                if len(inc) >= 3:
                    prior_rev = inc.iloc[-3].get('TOT_OPERA_REV')
                    prev_rev = previous.get('TOT_OPERA_REV')
                    if prior_rev is not None and prev_rev is not None:
                        single_curr = curr_rev - prev_rev
                        single_prev = prev_rev - prior_rev
                        if not self._safe_is_zero(single_prev):
                            factors['revenue_growth_qoq'] = (single_curr - single_prev) / abs(single_prev) * 100

            # 净利润同比：同季度累计值同比（与 operating_profit_growth_yoy 一致）
            ly_np_data = inc[inc['REPORTING_PERIOD'].astype(str) == last_year_period]
            if curr_np is not None and not ly_np_data.empty:
                last_np = ly_np_data.iloc[0].get('NET_PRO_INCL_MIN_INT_INC')
                if last_np is not None and not self._safe_is_zero(last_np):
                    factors['net_profit_growth_yoy'] = (curr_np - last_np) / abs(last_np) * 100

            # 净利润环比（复用 single_qoq）
            if 'net_profit_single_qoq' in factors:
                factors['net_profit_growth_qoq'] = factors['net_profit_single_qoq']

            # 营业利润同比
            curr_op = current.get('OPERA_PROFIT')
            ly_op = inc[inc['REPORTING_PERIOD'].astype(str) == last_year_period]
            if curr_op is not None and not ly_op.empty:
                last_op = ly_op.iloc[0].get('OPERA_PROFIT')
                if last_op is not None and not self._safe_is_zero(last_op):
                    factors['operating_profit_growth_yoy'] = (curr_op - last_op) / abs(last_op) * 100

        # --- 现金流量表增长 ---
        cf = cashflow_df[cashflow_df['MARKET_CODE'] == stock_code].copy()
        if len(cf) >= 4:
            cf = cf.sort_values('REPORTING_PERIOD', ascending=True)
            ocf_col = 'NET_CASH_FLOWS_OPERA_ACT'

            last_4_cf = cf.tail(4)
            ocf_ttm = last_4_cf[ocf_col].sum()
            if not self._safe_is_zero(ocf_ttm):
                factors['operating_cash_flow_ttm'] = ocf_ttm

                if len(cf) >= 8:
                    prior_4_cf = cf.iloc[-8:-4]
                    prior_ocf_ttm = prior_4_cf[ocf_col].sum()
                    if not self._safe_is_zero(prior_ocf_ttm):
                        factors['operating_cash_flow_ttm_yoy'] = (ocf_ttm - prior_ocf_ttm) / abs(prior_ocf_ttm) * 100

            if len(cf) >= 2:
                curr_ocf = cf.iloc[-1].get(ocf_col)
                prev_ocf = cf.iloc[-2].get(ocf_col)
                if curr_ocf is not None and prev_ocf is not None:
                    single_ocf = curr_ocf - prev_ocf
                    factors['operating_cash_flow_single'] = single_ocf

                    if len(cf) >= 3:
                        prior_ocf = cf.iloc[-3].get(ocf_col)
                        if prior_ocf is not None:
                            prior_single_ocf = prev_ocf - prior_ocf
                            if not self._safe_is_zero(prior_single_ocf):
                                factors['operating_cash_flow_single_qoq'] = (single_ocf - prior_single_ocf) / abs(prior_single_ocf) * 100

                    # 单季同比
                    curr_period_str = str(int(cf.iloc[-1]['REPORTING_PERIOD']))
                    ly_period = f"{int(curr_period_str[:4]) - 1}{curr_period_str[4:]}"
                    ly_cf = cf[cf['REPORTING_PERIOD'].astype(str) == ly_period]
                    if not ly_cf.empty and len(cf) >= 3:
                        ly_idx = cf.index.get_loc(ly_cf.index[0])
                        if ly_idx > 0:
                            ly_prev = cf.iloc[ly_idx - 1]
                            ly_ocf = ly_cf.iloc[0].get(ocf_col)
                            ly_prev_ocf = ly_prev.get(ocf_col)
                            ly_single = ly_ocf - ly_prev_ocf
                            if not self._safe_is_zero(ly_single):
                                factors['operating_cash_flow_single_yoy'] = (single_ocf - ly_single) / abs(ly_single) * 100

        return factors

    def batch_update_factors(
        self,
        stock_codes: List[str] = None,
        batch_size: int = 200,
        report_periods: List[str] = None,
        skip_bj: bool = True
    ) -> Dict:
        """批量更新月频因子 - 优化版本"""
        from services.common.task_manager import get_task_manager, TaskType

        task_manager = get_task_manager()

        if stock_codes is None:
            stock_codes = self.get_stocks_need_update(skip_bj=skip_bj)

        if not stock_codes:
            print("[MonthlyFactor] 没有需要更新的股票")
            task_manager.update_progress(TaskType.MONTHLY_FACTOR_UPDATE, 100, "无需更新")
            return {'updated': 0, 'failed': 0, 'skipped': len(self._missing_data_stocks)}

        # 默认更新近3年的报告期
        if report_periods is None:
            report_periods = self.get_report_periods_last_3_years()

        print(f"[MonthlyFactor] 需要更新 {len(stock_codes)} 只股票")
        print(f"[MonthlyFactor] 报告期范围: {report_periods}")

        # 更新任务进度
        task_manager.update_progress(TaskType.MONTHLY_FACTOR_UPDATE, 10,
            f"需要更新 {len(stock_codes)} 只股票，{len(report_periods)} 个报告期")

        # 获取股票名称映射
        stock_names = self.get_stock_names()

        # 预加载申万行业分类（该批次内所有股票）
        sw_data = self._load_sw_classification(stock_codes)

        # 分批获取财务数据并更新
        updated_count = 0
        inserted_count = 0
        failed_count = 0
        missing_count = 0

        total_batches = (len(stock_codes) + batch_size - 1) // batch_size

        for i in range(0, len(stock_codes), batch_size):
            batch = stock_codes[i:i+batch_size]
            batch_num = i // batch_size + 1
            progress_pct = 10 + (batch_num / total_batches) * 85  # 10%-95%

            print(f"[MonthlyFactor] 处理第 {batch_num}/{total_batches} 批 ({len(batch)} 只股票)...")

            # 更新任务进度
            task_manager.update_progress(TaskType.MONTHLY_FACTOR_UPDATE, progress_pct,
                f"处理第 {batch_num}/{total_batches} 批 ({len(batch)} 只股票)",
                batch=batch_num, total_batches=total_batches, updated=updated_count)

            try:
                # 一次性获取该批次所有报告期的财务数据
                income, balance, cashflow = self.fetch_financial_data_from_sdk(batch)

                if income.empty and balance.empty:
                    # 整批无数据
                    for stock_code in batch:
                        self._log_missing_data(
                            stock_code,
                            stock_names.get(stock_code, '未知'),
                            'all',
                            '整批无财务数据'
                        )
                    missing_count += len(batch)
                    continue

                # 逐只股票计算因子
                batch_updates = []
                for stock_code in batch:
                    # 检查是否有该股票数据
                    stock_income = income[income['MARKET_CODE'] == stock_code]

                    if stock_income.empty:
                        self._log_missing_data(
                            stock_code,
                            stock_names.get(stock_code, '未知'),
                            'all',
                            '无该股票财务数据'
                        )
                        missing_count += 1
                        continue

                    # 处理每个报告期
                    for report_period in report_periods:
                        # 获取对应日期的市值
                        trade_date = report_period[:4] + '-' + report_period[4:6] + '-' + report_period[6:8]
                        market_caps = self.get_market_cap_batch([stock_code], trade_date)
                        market_cap = market_caps.get(stock_code)

                        factors = self.calculate_factors(
                            stock_code, report_period,
                            income, balance, cashflow,
                            market_cap
                        )

                        if factors:
                            # 计算增长因子（TTM/单季/同比/环比）
                            growth = self.calculate_all_growth_factors(stock_code, income, cashflow)
                            factors.update(growth)

                            # 添加季度和年份
                            factors['report_quarter'] = self.calculate_report_quarter(trade_date)
                            factors['report_year'] = self.calculate_report_year(trade_date)

                            # 添加申万行业分类
                            if stock_code in sw_data:
                                factors.update(sw_data[stock_code])

                            factors['stock_code'] = stock_code
                            factors['report_period'] = report_period

                            batch_updates.append(factors)

                # 批量更新数据库
                if batch_updates:
                    db_result = self._batch_update_db(batch_updates, stock_names)
                    updated_count += db_result['updated']
                    inserted_count += db_result.get('inserted', 0)
                    if db_result.get('inserted', 0) > 0:
                        print(f"[MonthlyFactor] 本批新插入 {db_result['inserted']} 条季度报告记录")

                # 显示进度
                progress = (batch_num / total_batches) * 100
                print(f"[MonthlyFactor] 进度: {progress:.1f}% (更新{updated_count}, 插入{inserted_count})")

            except Exception as e:
                print(f"[MonthlyFactor] 批次 {batch_num} 处理失败: {e}")
                failed_count += len(batch)

        print(f"[MonthlyFactor] 更新完成：更新 {updated_count}，插入 {inserted_count}，无数据 {missing_count}，失败 {failed_count}")
        print(f"[MonthlyFactor] 无数据股票已记录到: {LOG_PATH}")

        # 更新任务进度为完成
        task_manager.update_progress(TaskType.MONTHLY_FACTOR_UPDATE, 100,
            f"更新完成：更新 {updated_count}，插入 {inserted_count}，无数据 {missing_count}")

        return {
            'updated': updated_count,
            'inserted': inserted_count,
            'failed': failed_count,
            'missing': missing_count,
            'skipped': len(self._missing_data_stocks)
        }

    def _batch_update_db(self, factors_list: List[Dict], stock_names: Dict[str, str] = None, fill_nulls_only: bool = False):
        """批量更新数据库（不存在则插入）

        Args:
            factors_list: 因子数据列表
            stock_names: 股票名称映射
            fill_nulls_only: True=只更新当前为NULL的字段；False=覆盖写入（默认）
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        factor_to_db_mapping = {
            'net_profit': 'net_profit',
            'net_profit_ttm': 'net_profit_ttm',
            'net_profit_ttm_yoy': 'net_profit_ttm_yoy',
            'net_profit_single': 'net_profit_single',
            'net_profit_single_yoy': 'net_profit_single_yoy',
            'net_profit_single_qoq': 'net_profit_single_qoq',
            'operating_cash_flow': 'operating_cash_flow',
            'operating_cash_flow_ttm': 'operating_cash_flow_ttm',
            'operating_cash_flow_ttm_yoy': 'operating_cash_flow_ttm_yoy',
            'operating_cash_flow_single': 'operating_cash_flow_single',
            'operating_cash_flow_single_yoy': 'operating_cash_flow_single_yoy',
            'operating_cash_flow_single_qoq': 'operating_cash_flow_single_qoq',
            'total_revenue': 'total_revenue',
            'operating_profit': 'operating_profit',
            'operating_cashflow': 'operating_cashflow',
            'total_assets': 'total_assets',
            'net_assets': 'net_assets',
            'pe_ttm': 'pe_ttm',
            'pb': 'pb',
            'ps_ttm': 'ps_ttm',
            'pcf': 'pcf',
            'ev_ebitda': 'ev_ebitda',
            'pe_inverse': 'pe_inverse',
            'pb_inverse': 'pb_inverse',
            'roe': 'roe',
            'roa': 'roa',
            'gross_margin': 'gross_margin',
            'net_margin': 'net_margin',
            'operating_margin': 'operating_margin',
            'revenue_growth_yoy': 'revenue_growth_yoy',
            'revenue_growth_qoq': 'revenue_growth_qoq',
            'net_profit_growth_yoy': 'net_profit_growth_yoy',
            'net_profit_growth_qoq': 'net_profit_growth_qoq',
            'operating_profit_growth_yoy': 'operating_profit_growth_yoy',
            'report_quarter': 'report_quarter',
            'report_year': 'report_year',
            'sw_level1': 'sw_level1',
            'sw_level2': 'sw_level2',
            'sw_level3': 'sw_level3',
            'total_market_cap': 'total_market_cap',
            'circ_market_cap': 'circ_market_cap',
        }

        # 数值字段（需要 NaN/Inf 检查）
        NUMERIC_FIELDS = set(self.ALL_WRITABLE_FIELDS) - {'sw_level1', 'sw_level2', 'sw_level3'}

        inserted_count = 0
        updated_count = 0

        for factors in factors_list:
            stock_code = factors['stock_code']
            report_period = factors['report_period']
            report_date = report_period[:4] + '-' + report_period[4:6] + '-' + report_period[6:8]

            # 检查是否存在记录
            cursor.execute("""
                SELECT id FROM stock_monthly_factors
                WHERE stock_code = ? AND report_date = ?
            """, (stock_code, report_date))
            existing = cursor.fetchone()

            # 收集因子字段
            factor_values = {}
            for factor_name, db_field in factor_to_db_mapping.items():
                value = factors.get(factor_name)
                if value is None:
                    continue
                if factor_name in NUMERIC_FIELDS:
                    if isinstance(value, (int, float)) and not (np.isnan(value) or np.isinf(value)):
                        factor_values[db_field] = value
                else:
                    factor_values[db_field] = value

            if not factor_values:
                continue

            if existing:
                if fill_nulls_only:
                    # 只更新当前为 NULL 的字段
                    fields_to_check = list(factor_values.keys())
                    cursor.execute(f"""
                        SELECT {', '.join(fields_to_check)}
                        FROM stock_monthly_factors WHERE id = ?
                    """, (existing[0],))
                    existing_values = cursor.fetchone()
                    if existing_values is None:
                        continue

                    # 只包含现有记录中为 NULL 的字段
                    non_null_updates = {}
                    for i, field in enumerate(fields_to_check):
                        if existing_values[i] is None:
                            non_null_updates[field] = factor_values[field]

                    if not non_null_updates:
                        continue

                    update_fields = [f"{field} = ?" for field in non_null_updates.keys()]
                    update_values = list(non_null_updates.values())
                    update_values.extend([
                        get_china_time().strftime('%Y-%m-%d %H:%M:%S'),
                        existing[0]
                    ])
                    sql = f"UPDATE stock_monthly_factors SET {', '.join(update_fields)}, updated_at = ?, source = 'sdk_update' WHERE id = ?"
                    cursor.execute(sql, update_values)
                else:
                    # 覆盖写入
                    update_fields = [f"{field} = ?" for field in factor_values.keys()]
                    update_values = list(factor_values.values())
                    update_values.extend([
                        get_china_time().strftime('%Y-%m-%d %H:%M:%S'),
                        stock_code, report_date
                    ])
                    sql = f"""
                        UPDATE stock_monthly_factors
                        SET {', '.join(update_fields)}, updated_at = ?, source = 'sdk_update'
                        WHERE stock_code = ? AND report_date = ?
                    """
                    cursor.execute(sql, update_values)
                updated_count += 1
            else:
                # 插入新记录
                stock_name = stock_names.get(stock_code, '') if stock_names else ''
                fields = ['stock_code', 'stock_name', 'report_date', 'source', 'created_at', 'updated_at']
                values = [stock_code, stock_name, report_date, 'sdk_insert',
                         get_china_time().strftime('%Y-%m-%d %H:%M:%S'),
                         get_china_time().strftime('%Y-%m-%d %H:%M:%S')]

                for field, value in factor_values.items():
                    fields.append(field)
                    values.append(value)

                sql = f"""
                    INSERT INTO stock_monthly_factors ({', '.join(fields)})
                    VALUES ({', '.join(['?'] * len(fields))})
                """
                cursor.execute(sql, values)
                inserted_count += 1

        conn.commit()
        conn.close()
        return {'updated': updated_count, 'inserted': inserted_count}


# ==================== API 接口 ====================

def run_monthly_factor_update(mode: str = 'fill_empty', years: int = 3) -> Dict:
    """运行月频因子更新

    Args:
        mode: 更新模式
            - 'fill_empty': 只填充pe_ttm为空的记录
            - 'fill_quarters': 补充缺失的季度记录（如2024Q1/Q2）
            - 'all': 更新所有股票
        years: 更新的年数
    """
    updater = MonthlyFactorUpdater()

    if mode == 'fill_empty':
        return updater.batch_update_factors(skip_bj=True)
    elif mode == 'fill_quarters':
        # 获取所有股票，为缺失的季度报告日期创建记录
        stocks = updater.get_all_stocks(skip_bj=True)
        return updater.batch_update_factors(stock_codes=stocks, skip_bj=True)
    elif mode == 'all':
        stocks = updater.get_stocks_need_update(skip_bj=True)
        return updater.batch_update_factors(stock_codes=stocks)
    else:
        raise ValueError(f"未知的更新模式: {mode}")


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()

    print("测试月频因子更新...")

    updater = MonthlyFactorUpdater()
    stocks = updater.get_stocks_need_update(skip_bj=True)
    periods = updater.get_report_periods_last_3_years()
    print(f"需要更新的股票数量: {len(stocks)}")
    print(f"报告期列表: {periods}")