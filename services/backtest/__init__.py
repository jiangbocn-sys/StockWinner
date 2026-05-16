"""
回测系统

撮合模拟盘模式：逐日推进，真实模拟买卖，支持滑点、涨跌停、仓位限制
收益率累积模式：信号快速累积，适合快速筛选策略
"""

from services.backtest.engine import BacktestEngine

_backtest_engine: BacktestEngine | None = None


def get_backtest_engine(account_id: str = "") -> BacktestEngine:
    """获取回测引擎单例（按 account_id 隔离）"""
    global _backtest_engine
    if _backtest_engine is None or _backtest_engine.account_id != account_id:
        _backtest_engine = BacktestEngine(account_id)
    return _backtest_engine
