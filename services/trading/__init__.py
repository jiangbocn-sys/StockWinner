"""
交易模块

包含：
- gateway: 交易网关（行情 + 委托）
- execution_service: 交易执行（资金 + 持仓 + 记账）
- order_service: 订单状态机
- position_manager: 持仓管理（对账 + P&L + T+1）
- risk_service: 交易风控
- trigger_evaluators: 条件评估器注册表
- strategy_executor: 策略执行器
"""

from .gateway import get_gateway, get_gateway_for_account, create_gateway
from .execution_service import get_trade_execution_service
from .order_service import get_order_service
from .position_manager import get_position_manager
from .risk_service import get_risk_service
