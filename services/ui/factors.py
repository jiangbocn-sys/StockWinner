"""
因子计算 API - 手动触发因子数据对齐和计算
"""

from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import Optional, List, Dict
from datetime import datetime
import asyncio
import threading

router = APIRouter()

# 全局状态
_calculation_status = {
    'running': False,
    'progress': 0,
    'message': '',
    'started_at': None,
    'completed_at': None,
    'result': None
}


class FactorCalculationRequest(BaseModel):
    """因子计算请求"""
    delete_orphans: bool = False  # 是否删除孤儿记录
    insert_missing: bool = True  # 是否插入缺失记录
    calculate_factors: bool = True  # 是否计算因子
    recalculate: bool = False  # 是否重新计算已有数据
    stock_codes: Optional[List[str]] = None  # 指定股票代码列表


def _run_factor_calculation(request: FactorCalculationRequest):
    """后台运行因子计算"""
    global _calculation_status

    try:
        _calculation_status['running'] = True
        _calculation_status['started_at'] = datetime.now().isoformat()
        _calculation_status['progress'] = 0
        _calculation_status['message'] = '开始因子数据对齐...'

        from services.factors.factor_alignment import (
            FactorAlignment,
            FactorCalculator,
            run_factor_alignment
        )

        # 步骤 1: 分析数据差距
        _calculation_status['message'] = '分析数据差距...'
        alignment = FactorAlignment()
        gap = alignment.analyze_data_gap()
        _calculation_status['progress'] = 10

        # 步骤 2: 删除孤儿记录
        if request.delete_orphans:
            _calculation_status['message'] = '删除孤儿记录...'
            deleted = alignment.delete_orphan_records(dry_run=False)
            _calculation_status['progress'] = 20
            _calculation_status['message'] = f'已删除 {deleted} 条孤儿记录'

        # 步骤 3: 插入缺失记录
        if request.insert_missing:
            _calculation_status['message'] = '插入缺失记录...'
            stock_codes = request.stock_codes
            inserted = alignment.insert_missing_records(stock_codes)
            _calculation_status['progress'] = 40
            _calculation_status['message'] = f'已插入 {inserted} 条缺失记录'

        # 步骤 4: 计算因子
        if request.calculate_factors:
            _calculation_status['message'] = '计算因子...'
            calculator = FactorCalculator()

            if stock_codes:
                stocks_to_process = stock_codes
            else:
                conn = alignment._get_connection()
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT DISTINCT k.stock_code
                    FROM kline_data k
                    LEFT JOIN stock_daily_factors f
                        ON k.stock_code = f.stock_code AND k.trade_date = f.trade_date
                    WHERE f.trade_date IS NULL OR (
                        f.dif IS NULL AND f.macd IS NULL AND f.ma5 IS NULL
                    )
                    ORDER BY k.stock_code
                """)
                stocks_to_process = [row[0] for row in cursor.fetchall()]
                conn.close()

            total = len(stocks_to_process)
            total_updated = 0
            total_skipped = 0

            for i, stock_code in enumerate(stocks_to_process):
                result = calculator.calculate_factors_for_stock(stock_code, request.recalculate)

                if result['status'] == 'success':
                    total_updated += result.get('updated', 0)
                    total_skipped += result.get('skipped', 0)

                # 更新进度
                progress = 50 + int((i + 1) / total * 50)
                _calculation_status['progress'] = progress
                _calculation_status['message'] = f'计算中... {i + 1}/{total} ({stock_code})'

            _calculation_status['progress'] = 100
            _calculation_status['message'] = f'完成！更新 {total_updated} 条记录，跳过 {total_skipped} 条记录'

        _calculation_status['result'] = {
            'gap_analysis': gap,
            'updated_factors': total_updated if request.calculate_factors else 0,
            'skipped_factors': total_skipped if request.calculate_factors else 0
        }

    except Exception as e:
        _calculation_status['message'] = f'错误：{str(e)}'
        _calculation_status['progress'] = -1
    finally:
        _calculation_status['running'] = False
        _calculation_status['completed_at'] = datetime.now().isoformat()


@router.get("/api/v1/ui/factors/status")
async def get_factor_calculation_status():
    """获取因子计算状态"""
    return {
        'running': _calculation_status['running'],
        'progress': _calculation_status['progress'],
        'message': _calculation_status['message'],
        'started_at': _calculation_status['started_at'],
        'completed_at': _calculation_status['completed_at'],
        'result': _calculation_status['result']
    }


@router.post("/api/v1/ui/factors/calculate")
async def start_factor_calculation(request: FactorCalculationRequest = None):
    """
    启动因子计算任务

    如果任务正在运行，返回当前状态
    """
    global _calculation_status

    if _calculation_status['running']:
        return {
            'status': 'running',
            'message': '因子计算任务正在运行中',
            'progress': _calculation_status['progress']
        }

    # 重置状态
    _calculation_status = {
        'running': True,
        'progress': 0,
        'message': '正在启动因子计算任务...',
        'started_at': datetime.now().isoformat(),
        'completed_at': None,
        'result': None
    }

    # 在后台线程中运行
    if request is None:
        request = FactorCalculationRequest()

    thread = threading.Thread(target=_run_factor_calculation, args=(request,))
    thread.daemon = True
    thread.start()

    return {
        'status': 'started',
        'message': '因子计算任务已启动'
    }


@router.get("/api/v1/ui/factors/gap-analysis")
async def get_factor_gap_analysis():
    """获取因子数据差距分析"""
    from services.factors.factor_alignment import FactorAlignment

    alignment = FactorAlignment()
    gap = alignment.analyze_data_gap()

    return {
        'success': True,
        'data': gap
    }


@router.get("/api/v1/ui/factors/missing-records")
async def get_missing_records(
    stock_code: Optional[str] = None,
    limit: int = 100
):
    """获取缺失的因子记录"""
    from services.factors.factor_alignment import FactorAlignment

    alignment = FactorAlignment()
    records = alignment.get_missing_records(stock_code, limit)

    return {
        'success': True,
        'data': records,
        'count': len(records)
    }


@router.get("/api/v1/ui/factors/orphan-records")
async def get_orphan_records(limit: int = 100):
    """获取孤儿因子记录"""
    from services.factors.factor_alignment import FactorAlignment

    alignment = FactorAlignment()
    records = alignment.get_orphan_records(limit)

    return {
        'success': True,
        'data': records,
        'count': len(records)
    }


@router.delete("/api/v1/ui/factors/orphan-records")
async def delete_orphan_records(dry_run: bool = True):
    """
    删除孤儿因子记录

    dry_run=true 时只预览，dry_run=false 时实际删除
    """
    from services.factors.factor_alignment import FactorAlignment

    alignment = FactorAlignment()
    count = alignment.delete_orphan_records(dry_run=dry_run)

    action = "将删除" if dry_run else "已删除"
    return {
        'success': True,
        'message': f'{action} {count} 条孤儿记录',
        'count': count
    }
