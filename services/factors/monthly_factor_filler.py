"""
月度因子沿用季度数据填充

月度记录使用最近一期季度的财务数据：
- 1月、2月 → 去年12月(Q4)
- 3月 → 去年12月(Q4)（3月底Q1报告可能未发布）
- 4月、5月 → 3月(Q1)
- 6月 → 3月(Q1)
- 7月、8月 → 6月(Q2)
- 9月 → 6月(Q2)
- 10月、11月 → 9月(Q3)
- 12月 → 9月(Q3)

季度报告日期：03-31, 06-30, 09-30, 12-31
"""

import sqlite3
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple

DB_PATH = Path(__file__).parent.parent.parent / "data" / "kline.db"


class MonthlyFactorFiller:
    """月度因子填充器 - 沿用季度数据"""

    # 季度报告日期
    QUARTER_ENDS = ['03-31', '06-30', '09-30', '12-31']

    # 月度到季度的映射（月份 → 对应季度报告的月份）
    MONTH_TO_QUARTER = {
        1: 12,   # 1月 → 去年Q4 (12月)
        2: 12,   # 2月 → 去年Q4 (12月)
        3: 12,   # 3月 → 去年Q4 (12月)（Q1报告未发布）
        4: 3,    # 4月 → Q1 (3月)
        5: 3,    # 5月 → Q1 (3月)
        6: 3,    # 6月 → Q1 (3月)
        7: 6,    # 7月 → Q2 (6月)
        8: 6,    # 8月 → Q2 (6月)
        9: 6,    # 9月 → Q2 (6月)
        10: 9,   # 10月 → Q3 (9月)
        11: 9,   # 11月 → Q3 (9月)
        12: 9,   # 12月 → Q3 (9月)（Q4报告未发布）
    }

    # 季度报告日期（季度末日期）
    QUARTER_REPORT_DAYS = {
        3: '31',   # 3月31日
        6: '30',   # 6月30日
        9: '30',   # 9月30日
        12: '31',  # 12月31日
    }

    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        return conn

    def get_source_report_date(self, monthly_date: str) -> str:
        """
        根据月度日期计算对应的季度报告日期

        Args:
            monthly_date: 月度日期，如 '2026-02-27'

        Returns:
            季度报告日期，如 '2025-12-31'
        """
        year = int(monthly_date[:4])
        month = int(monthly_date[5:7])

        quarter_month = self.MONTH_TO_QUARTER[month]

        # 如果映射到12月，需要使用去年的数据
        if quarter_month == 12 and month <= 3:
            source_year = year - 1
        else:
            source_year = year

        # 使用正确的季度末日期（3月31日、6月30日、9月30日、12月31日）
        quarter_day = self.QUARTER_REPORT_DAYS[quarter_month]

        return f"{source_year}-{quarter_month:02d}-{quarter_day}"

    def fill_monthly_from_quarterly(self) -> Dict:
        """
        将季度财务数据填充到月度记录

        Returns:
            更新统计信息
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        # 获取所有有财务数据的季度记录
        cursor.execute("""
            SELECT stock_code, report_date,
                   pe_ttm, pb, ps_ttm, pcf, pe_inverse, pb_inverse,
                   roe, roa, gross_margin, net_margin, operating_margin,
                   revenue_growth_yoy, net_profit_growth_yoy,
                   total_market_cap, circ_market_cap,
                   total_revenue, operating_profit, net_profit,
                   total_assets, net_assets, operating_cashflow
            FROM stock_monthly_factors
            WHERE source = 'sdk_update' AND pe_ttm > 0
        """)
        quarterly_records = cursor.fetchall()

        print(f"[MonthlyFiller] 获取到 {len(quarterly_records)} 条季度财务记录")

        # 建立索引：stock_code + report_date → 财务数据
        quarterly_index: Dict[str, Dict] = {}
        for row in quarterly_records:
            key = f"{row['stock_code']}|{row['report_date']}"
            quarterly_index[key] = dict(row)

        # 获取所有需要填充的月度记录（migrated且无财务数据）
        cursor.execute("""
            SELECT id, stock_code, report_date
            FROM stock_monthly_factors
            WHERE source = 'migrated'
            AND (pe_ttm IS NULL OR pe_ttm = 0)
        """)
        monthly_records = cursor.fetchall()

        print(f"[MonthlyFiller] 需要填充 {len(monthly_records)} 条月度记录")

        # 执行填充
        filled_count = 0
        no_source_count = 0
        batch_updates = []

        for row in monthly_records:
            stock_code = row['stock_code']
            monthly_date = row['report_date']

            # 计算对应的季度报告日期
            source_date = self.get_source_report_date(monthly_date)

            # 查找对应的季度数据
            key = f"{stock_code}|{source_date}"
            quarterly_data = quarterly_index.get(key)

            if quarterly_data:
                batch_updates.append({
                    'id': row['id'],
                    'stock_code': stock_code,
                    'report_date': monthly_date,
                    'data': quarterly_data,
                    'source_date': source_date
                })
                filled_count += 1
            else:
                no_source_count += 1

        # 批量更新数据库
        if batch_updates:
            self._batch_update_monthly(conn, batch_updates)

        conn.close()

        print(f"[MonthlyFiller] 填充完成：成功 {filled_count}，无源数据 {no_source_count}")

        return {
            'filled': filled_count,
            'no_source': no_source_count,
            'total_monthly': len(monthly_records),
            'quarterly_sources': len(quarterly_records)
        }

    def _batch_update_monthly(self, conn: sqlite3.Connection, updates: List[Dict]):
        """批量更新月度记录"""
        cursor = conn.cursor()

        factor_fields = [
            'pe_ttm', 'pb', 'ps_ttm', 'pcf', 'pe_inverse', 'pb_inverse',
            'roe', 'roa', 'gross_margin', 'net_margin', 'operating_margin',
            'revenue_growth_yoy', 'net_profit_growth_yoy',
            'total_market_cap', 'circ_market_cap',
            'total_revenue', 'operating_profit', 'net_profit',
            'total_assets', 'net_assets', 'operating_cashflow'
        ]

        for item in updates:
            data = item['data']
            record_id = item['id']

            update_fields = []
            update_values = []

            for field in factor_fields:
                value = data.get(field)
                if value is not None:
                    update_fields.append(f"{field} = ?")
                    update_values.append(value)

            if update_fields:
                update_values.extend([
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    f"inherited_from_{item['source_date']}",
                    record_id
                ])

                sql = f"""
                    UPDATE stock_monthly_factors
                    SET {', '.join(update_fields)},
                        updated_at = ?,
                        source = ?
                    WHERE id = ?
                """
                cursor.execute(sql, update_values)

        conn.commit()
        print(f"[MonthlyFiller] 已批量更新 {len(updates)} 条记录")


def run_monthly_factor_fill():
    """执行月度因子填充"""
    filler = MonthlyFactorFiller()
    return filler.fill_monthly_from_quarterly()


if __name__ == "__main__":
    print("开始执行月度因子沿用季度数据填充...")
    result = run_monthly_factor_fill()
    print(f"结果: {result}")