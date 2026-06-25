"""
缠论分析 API - 提供缠论 K 线可视化

接收前端 K 线数据，调用 czsc 进行缠论分析，返回 HTML 图表
"""

from fastapi import APIRouter, HTTPException, Body
from typing import List, Dict, Any, Optional
from pydantic import BaseModel
import logging

logger = logging.getLogger(__name__)

router = APIRouter()


class CzscChartRequest(BaseModel):
    """缠论图表请求"""
    stock_code: str
    stock_name: Optional[str] = ""
    kline_data: List[Dict[str, Any]]  # 前端已有的 K 线数据
    period: str = "day"  # 日线/周线/月线


class CzscChartResponse(BaseModel):
    """缠论图表响应"""
    success: bool
    html: Optional[str] = None
    message: Optional[str] = None


# 周期映射：前端 period -> czsc Freq
PERIOD_FREQ_MAP = {
    "day": "日线",
    "week": "周线",
    "month": "月线",
}


@router.post("/api/v1/ui/czsc/chart", response_model=CzscChartResponse)
async def generate_czsc_chart(request: CzscChartRequest = Body(...)):
    """
    生成缠论 K 线 HTML 图表

    接收前端传来的 K 线数据，转换为 czsc 格式，生成缠论分析 HTML
    """
    try:
        # 检查数据
        if not request.kline_data or len(request.kline_data) < 10:
            return CzscChartResponse(
                success=False,
                message="K 线数据不足（至少需要10条）"
            )

        # 导入 czsc (安装版本 0.10.12 的 API)
        try:
            import pandas as pd
            from czsc import CZSC, format_standard_kline
            from czsc.utils.plotting import plot_czsc_chart
        except ImportError as e:
            logger.error(f"czsc 导入失败: {e}")
            return CzscChartResponse(
                success=False,
                message=f"czsc 库导入失败: {e}"
            )

        # 获取周期
        freq_str = PERIOD_FREQ_MAP.get(request.period, "日线")

        # 转换 K 线数据为 DataFrame
        # 前端数据格式：{trade_date, open, close, high, low, volume}
        df_data = []
        for k in request.kline_data:
            # 处理日期格式
            trade_date = k.get("trade_date", "")
            if isinstance(trade_date, str):
                # 可能是 YYYYMMDD 或 YYYY-MM-DD 格式
                if len(trade_date) == 8 and trade_date.isdigit():
                    dt_str = f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:8]}"
                elif "-" in trade_date:
                    dt_str = trade_date[:10]
                else:
                    dt_str = trade_date
            else:
                dt_str = str(trade_date)[:10]

            df_data.append({
                "dt": dt_str,
                "symbol": request.stock_code,
                "open": float(k.get("open", 0)),
                "close": float(k.get("close", 0)),
                "high": float(k.get("high", 0)),
                "low": float(k.get("low", 0)),
                "vol": float(k.get("volume", 0) or 0),
                "amount": float(k.get("amount", 0) or k.get("volume", 0) * float(k.get("close", 0))),
            })

        df = pd.DataFrame(df_data)
        df["dt"] = pd.to_datetime(df["dt"])

        # 按日期排序（从旧到新）
        df = df.sort_values("dt").reset_index(drop=True)

        logger.info(f"[CZSC] 转换数据: {len(df)} 条, 周期: {freq_str}, 股票: {request.stock_code}")

        # 转换为 RawBar 列表
        bars = format_standard_kline(df, freq=freq_str)

        # 创建 CZSC 对象进行缠论分析
        c = CZSC(bars)

        # 生成图表（使用安装版本的 API）
        chart = plot_czsc_chart(c, height=700)
        fig = chart.fig

        # 转换为 HTML（使用 CDN 加载 plotly.js，避免体积过大）
        html = fig.to_html(include_plotlyjs='cdn', full_html=True)

        logger.info(f"[CZSC] 生成 HTML 成功, 长度: {len(html) if html else 0}")

        return CzscChartResponse(
            success=True,
            html=html,
            message="缠论分析完成"
        )

    except Exception as e:
        logger.error(f"[CZSC] 缠论分析异常: {e}", exc_info=True)
        return CzscChartResponse(
            success=False,
            message=f"缠论分析异常: {str(e)}"
        )