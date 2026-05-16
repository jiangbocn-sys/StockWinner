"""
数据完整性检查

回测前扫描 kline_data + stock_daily_factors 覆盖度：
- BLOCKING：无法回测的严重缺失
- WARNING：可继续但需告知的轻微问题
"""

import logging
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple

from services.common.database import get_db_manager
from services.factors.kline_manager import get_kline_manager
from services.common.structured_logger import get_logger

logger = get_logger("backtest")


@dataclass
class DataGap:
    """单个数据缺口"""
    stock_code: str
    severity: str  # 'BLOCKING' | 'WARNING'
    reason: str
    missing_dates: int = 0
    total_required: int = 0


@dataclass
class DataGapReport:
    """数据完整性报告"""
    blocking_gaps: List[DataGap] = field(default_factory=list)
    warning_gaps: List[DataGap] = field(default_factory=list)
    total_stocks_checked: int = 0
    stocks_with_full_coverage: int = 0
    coverage_pct: float = 0.0

    @property
    def can_proceed(self) -> bool:
        """
        判断是否可以继续回测。
        BLOCKING 不等于完全阻止：只有当 >50% 的股票都无数据时才阻止。
        个别新上市股票数据不足不应阻止全市场回测。
        """
        if len(self.blocking_gaps) == 0:
            return True
        # 超过一半的股票都无法回测 → 阻止
        return len(self.blocking_gaps) < self.total_stocks_checked * 0.5

    def to_dict(self) -> dict:
        return {
            "can_proceed": self.can_proceed,
            "total_stocks_checked": self.total_stocks_checked,
            "stocks_with_full_coverage": self.stocks_with_full_coverage,
            "coverage_pct": round(self.coverage_pct, 1),
            "blocking_count": len(self.blocking_gaps),
            "warning_count": len(self.warning_gaps),
            "blocking_summary": [f"{g.stock_code}: {g.reason}" for g in self.blocking_gaps[:10]],
            "warning_summary": [f"{g.stock_code}: {g.reason}" for g in self.warning_gaps[:10]],
        }


class DataCompletenessChecker:
    """回测数据完整性检查器"""

    MIN_TRADING_DAYS = 60  # 最少交易日要求

    def __init__(self):
        self.db = get_db_manager()
        self.km = get_kline_manager()

    async def check(
        self,
        stock_codes: List[str],
        start_date: str,
        end_date: str,
        require_factors: Optional[List[str]] = None,
    ) -> DataGapReport:
        """
        检查指定股票池在时间范围内的数据完整性。

        Args:
            stock_codes: 股票代码列表
            start_date: 起始日期 'YYYY-MM-DD'
            end_date: 结束日期 'YYYY-MM-DD'
            require_factors: 需要的因子名称列表（可选）

        Returns:
            DataGapReport
        """
        report = DataGapReport()
        report.total_stocks_checked = len(stock_codes)

        if not stock_codes:
            report.blocking_gaps.append(DataGap(
                stock_code="", severity="BLOCKING", reason="股票池为空"
            ))
            return report

        # 批量查询每只股票的交易日期范围和数据量
        batches = self._split_batches(stock_codes)
        all_coverage = {}

        for batch in batches:
            coverage = self.km.get_stocks_date_ranges_batch(
                batch, start_date, end_date
            )
            all_coverage.update(coverage)

        # 获取目标时间范围内的总交易日数
        trade_dates = self.km.get_all_trade_dates(start_date, end_date)
        total_trading_days = len(trade_dates)

        for code in stock_codes:
            info = all_coverage.get(code)
            if not info or info.get("count", 0) == 0:
                report.blocking_gaps.append(DataGap(
                    stock_code=code,
                    severity="BLOCKING",
                    reason=f"时间范围内无任何日K线数据",
                ))
                continue

            count = info.get("count", 0)
            coverage_ratio = count / total_trading_days if total_trading_days > 0 else 0

            if coverage_ratio < 0.5 or count < self.MIN_TRADING_DAYS:
                report.blocking_gaps.append(DataGap(
                    stock_code=code,
                    severity="BLOCKING",
                    reason=f"日K线覆盖不足 {count}/{total_trading_days} 天 ({coverage_ratio*100:.0f}%)",
                    missing_dates=total_trading_days - count,
                    total_required=total_trading_days,
                ))
            elif coverage_ratio < 0.95:
                report.warning_gaps.append(DataGap(
                    stock_code=code,
                    severity="WARNING",
                    reason=f"日K线覆盖 {count}/{total_trading_days} 天 ({coverage_ratio*100:.0f}%)，部分日期缺失",
                    missing_dates=total_trading_days - count,
                    total_required=total_trading_days,
                ))
            else:
                report.stocks_with_full_coverage += 1

        # 计算整体覆盖率
        total_required = len(stock_codes) * total_trading_days
        total_present = sum(
            all_coverage.get(c, {}).get("count", 0) for c in stock_codes
        )
        report.coverage_pct = (total_present / total_required * 100) if total_required > 0 else 0

        # 检查因子覆盖（如果指定了 require_factors）
        if require_factors:
            await self._check_factors(
                stock_codes, start_date, end_date, require_factors, report
            )

        return report

    async def _check_factors(
        self,
        stock_codes: List[str],
        start_date: str,
        end_date: str,
        require_factors: List[str],
        report: DataGapReport,
    ):
        """检查 stock_daily_factors 表中指定因子的覆盖度"""
        try:
            # 获取已有因子的列名
            columns = await self.db.fetchone(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name='stock_daily_factors'"
            )
            if not columns:
                return

            sql_text = columns.get("sql", "")
            available_factors = self._extract_columns(sql_text)

            missing_factors = [f for f in require_factors if f not in available_factors]
            if missing_factors:
                report.warning_gaps.append(DataGap(
                    stock_code="(全局)",
                    severity="WARNING",
                    reason=f"stock_daily_factors 表缺少因子: {', '.join(missing_factors[:5])}",
                ))
        except Exception as e:
            logger.warning(f"因子覆盖检查失败: {e}")

    @staticmethod
    def _extract_columns(sql: str) -> List[str]:
        """从 CREATE TABLE SQL 中提取列名"""
        import re
        columns = []
        for line in sql.split("\n"):
            line = line.strip().rstrip(",")
            match = re.match(r'^(\w+)\s+', line)
            if match and match.group(1).upper() not in ("PRIMARY", "FOREIGN", "UNIQUE", "INDEX", "CHECK", "CONSTRAINT"):
                columns.append(match.group(1))
        return columns

    @staticmethod
    def _split_batches(stock_codes: List[str], batch_size: int = 500) -> List[List[str]]:
        """分批处理大量股票"""
        return [stock_codes[i:i+batch_size] for i in range(0, len(stock_codes), batch_size)]
