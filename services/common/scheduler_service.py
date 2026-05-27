"""
系统调度服务

每天凌晨1点自动执行：
1. 检查kline_data数据是否最新
2. 如果数据落后，启动K线增量下载
3. K线下载完成后，启动日频因子计算
4. 日频因子完成后，启动月频因子更新

使用 APScheduler 实现定时任务调度
"""

import asyncio
import threading
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, Dict, Tuple

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from services.common.timezone import get_china_time, CHINA_TZ
from services.factors.kline_manager import get_kline_manager
from services.common.async_helper import run_async_safe
from services.common.database import get_sync_connection, DB_PATH as WINNER_DB_PATH, KLINE_DB_PATH as DB_PATH
import sqlite3

# 配置日志 — 只写文件，控制台由 structured_logger 统一管理
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(Path(__file__).parent.parent.parent / 'logs' / 'scheduler.log'),
    ],
)
logger = logging.getLogger(__name__)


# FastAPI 事件循环引用（由 lifespan 设置，供 APScheduler 线程提交协程）
_fastapi_loop: Optional[asyncio.AbstractEventLoop] = None


def _get_fastapi_loop() -> Optional[asyncio.AbstractEventLoop]:
    """获取 FastAPI 事件循环引用"""
    return _fastapi_loop


def _set_fastapi_loop(loop: Optional[asyncio.AbstractEventLoop]):
    """设置 FastAPI 事件循环引用"""
    global _fastapi_loop
    _fastapi_loop = loop

class SchedulerService:
    """系统调度服务"""

    def __init__(self):
        self._running = False
        self._scheduler = None
        self._current_task = None
        self._task_status = {
            'last_check_time': None,
            'last_download_time': None,
            'last_factor_calc_time': None,
            'last_monthly_update_time': None,
            'kline_status': None,
            'factor_status': None
        }

    def start(self):
        """启动调度服务"""
        if self._running:
            logger.warning("调度服务已在运行")
            return

        try:
            # 配置 APScheduler 使用中国时区
            self._scheduler = BackgroundScheduler(timezone=CHINA_TZ)
            self._running = True

            self._scheduler.start()
            logger.info("调度服务已启动")

            # 注册内置功能任务（从 strategy_tasks 表读取，含 cron 配置）
            self._register_strategy_tasks()

            # 注册交易监控自动启动任务（每天 9:15）
            self._register_monitor_auto_start_job()

            # 注册 price_cache 心跳兜底任务（每 5 分钟检查缓存新鲜度，仅在监控挂掉时触发）
            self._register_price_cache_fallback_job()

            # 注册 scheduler 心跳检查（每 30 分钟），用于诊断 daemon 线程是否存活
            self._register_heartbeat_job()

            # 记录 daemon 线程状态，用于诊断 scheduler 静默失效
            import threading
            thread_names = [t.name for t in threading.enumerate()]
            aps_threads = [n for n in thread_names if 'apscheduler' in n.lower() or 'apsched' in n.lower()]
            logger.info(f"Scheduler daemon 线程: {aps_threads}, 总线程数: {len(threading.enumerate())}")

        except ImportError:
            logger.warning("APScheduler 未安装，使用简单的定时检查方案")
            self._start_simple_scheduler()

    def stop(self):
        """停止调度服务"""
        if self._scheduler:
            try:
                # wait=False：不等待正在执行的任务完成，防止 shutdown 卡死
                # close=False：保留 executor 供后续重启
                self._scheduler.shutdown(wait=False)
            except Exception as e:
                logger.error(f"调度服务停止异常: {e}")
            self._scheduler = None
        self._running = False
        logger.info("调度服务已停止")

    def _start_simple_scheduler(self):
        """简单定时器方案（备用）"""
        import time

        def simple_loop():
            while self._running:
                # 计算到下一个凌晨1点(中国时间)的等待时间
                now = get_china_time()
                next_run = now.replace(hour=1, minute=0, second=0, microsecond=0)
                if now.hour >= 1:
                    next_run = next_run + timedelta(days=1)

                wait_seconds = (next_run - now).total_seconds()
                logger.info(f"下次执行时间: {next_run}, 等待 {wait_seconds/3600:.1f} 小时")

                time.sleep(wait_seconds)

                if self._running:
                    logger.info("开始执行每日数据检查")
                    self._daily_kline_check_job()

        self._running = True
        thread = threading.Thread(target=simple_loop, daemon=True)
        thread.start()
        logger.info("简单调度器已启动")

    def _daily_kline_check_job(self):
        """每日K线数据检查任务"""
        logger.info("=" * 60)
        logger.info("开始每日K线数据检查任务")
        logger.info("=" * 60)

        self._task_status['last_check_time'] = get_china_time().isoformat()

        try:
            # Step 1: 检查K线数据是否最新
            kline_check = self._check_kline_data()
            expected_date = kline_check['expected_date']

            if kline_check['need_download']:
                logger.info(f"K线数据落后，最新数据: {kline_check['latest_date']}, 应有数据: {expected_date}")

                # Step 2: K线增量下载（外层 _run_builtin_task_threadsafe 已在独立线程）
                download_result = self._run_kline_download()

                self._task_status['last_download_time'] = get_china_time().isoformat()
                self._task_status['kline_status'] = download_result

                if download_result.get('success'):
                    logger.info(f"K线下载完成: {download_result}")
                else:
                    logger.warning(f"K线下载失败: {download_result}")
            else:
                logger.info(f"K线数据已是最新: {kline_check['latest_date']}")
                self._task_status['kline_status'] = {'status': 'up_to_date', 'latest_date': kline_check['latest_date']}

            # Step 3: 行业指数下载（独立任务，在因子计算之前）
            logger.info("下载申万行业指数数据...")
            industry_result = self._run_industry_indices_download()
            self._task_status['industry_indices_status'] = industry_result

            # Step 4: 检查因子覆盖率并补充缺失因子（无论K线是否下载）
            logger.info("检查因子覆盖率...")
            factor_check = self._check_factor_coverage(expected_date)

            if factor_check['need_calc']:
                logger.info(f"因子覆盖率不足: {factor_check['coverage_pct']:.1f}%, 需补充 {factor_check['missing_count']} 只股票")

                # 因子补充计算（外层 _run_builtin_task_threadsafe 已在独立线程）
                factor_result = self._run_daily_factor_calc(
                    None,  # 让函数自动确定起始日期
                    expected_date,
                    force_full=False  # 仅计算缺失日期，不重算已有数据
                )

                self._task_status['last_factor_calc_time'] = get_china_time().isoformat()
                self._task_status['factor_status'] = factor_result

                if factor_result.get('success'):
                    logger.info(f"因子补充计算完成: {factor_result}")
                else:
                    logger.warning(f"因子补充计算失败: {factor_result}")
            else:
                logger.info(f"因子覆盖率正常: {factor_check['coverage_pct']:.1f}%")
                self._task_status['factor_status'] = {'status': 'up_to_date', 'coverage': factor_check['coverage_pct']}

            logger.info("=" * 60)
            logger.info("每日K线数据检查任务完成")
            logger.info("=" * 60)

        except Exception as e:
            logger.error(f"每日K线数据检查任务失败: {e}", exc_info=True)
            self._task_status['kline_status'] = {'status': 'error', 'message': str(e)}

    def _monthly_factor_check_job(self):
        """每月5日月频因子更新任务"""
        logger.info("=" * 60)
        logger.info("开始月频因子更新任务（每月5日）")
        logger.info("=" * 60)

        self._task_status['last_monthly_check_time'] = get_china_time().isoformat()

        try:
            # 检查月频因子是否需要更新
            monthly_check = self._check_monthly_factors()

            if monthly_check['need_update']:
                logger.info(f"月频因子需要更新，最新报告期: {monthly_check['latest_report_period']}")

                # 执行月频因子更新
                result = self._run_monthly_factor_update()
                self._task_status['last_monthly_update_time'] = get_china_time().isoformat()

                logger.info(f"月频因子更新完成: {result}")
            else:
                logger.info(f"月频因子已是最新: {monthly_check['latest_report_period']}")
                result = {'success': True, 'message': '因子已是最新'}
                self._task_status['monthly_factor_status'] = {'status': 'up_to_date'}

            logger.info("=" * 60)
            logger.info("月频因子更新任务完成")
            logger.info("=" * 60)

            return result

        except Exception as e:
            logger.error(f"月频因子更新任务失败: {e}", exc_info=True)
            self._task_status['monthly_factor_status'] = {'status': 'error', 'message': str(e)}
            return {'success': False, 'message': str(e)}

    def _check_monthly_factors(self) -> Dict:
        """检查月频因子是否需要更新"""
        from services.common.database import get_sync_connection
        conn = get_sync_connection("kline", path=DB_PATH)
        cursor = conn.cursor()

        # 获取最新的报告期
        cursor.execute("SELECT MAX(report_date) FROM stock_monthly_factors")
        latest_report = cursor.fetchone()[0]

        # 获取当前应有的最新报告期（季度报告日期）
        now = get_china_time()
        year = now.year
        month = now.month

        # 判断当前季度报告期（财报披露有延迟，取上上季度）
        # 例：5月 → 期待 Q1（03-31），因为Q1财报4月才陆续披露
        if month <= 3:
            expected_report = f"{year-1}-12-31"
        elif month <= 6:
            expected_report = f"{year}-03-31"
        elif month <= 9:
            expected_report = f"{year}-06-30"
        elif month <= 12:
            expected_report = f"{year}-09-30"
        else:
            expected_report = f"{year}-12-31"

        # 检查最新报告期的 PE 填充率
        cursor.execute("""
            SELECT COUNT(*) as total,
                   COUNT(CASE WHEN pe_ttm > 0 THEN 1 END) as has_pe
            FROM stock_monthly_factors
            WHERE report_date = ?
        """, (expected_report,))
        row = cursor.fetchone()
        total = row[0]
        has_pe = row[1]
        pe_ratio = has_pe / total if total > 0 else 0

        # 判断是否需要更新
        need_update = False
        if latest_report is None:
            need_update = True
            logger.info("月频因子表无数据，需要更新")
        elif latest_report < expected_report:
            need_update = True
            logger.info(f"最新报告期 {latest_report} < 应有报告期 {expected_report}")
        elif pe_ratio < 0.85:  # PE填充率低于85%
            need_update = True
            logger.info(f"报告期 {expected_report} PE填充率仅 {pe_ratio*100:.1f}%（{has_pe}/{total}），需要更新")

        return {
            'latest_report_period': latest_report,
            'expected_report_period': expected_report,
            'pe_fill_rate': has_pe / total * 100 if total > 0 else 0,
            'need_update': need_update
        }

    def _check_kline_data(self) -> Dict:
        """检查K线数据是否最新"""
        km = get_kline_manager()

        # 获取数据库中最新的K线日期
        latest_date = km.get_global_latest_date()

        # 获取应有的最新交易日
        expected_date, status_msg = self._get_expected_trading_day()

        # 判断是否需要下载
        need_download = False
        if latest_date is None:
            need_download = True
            logger.info("数据库无K线数据，需要下载")
        elif latest_date < expected_date:
            need_download = True
            logger.info(f"数据库最新日期 {latest_date} < 应有日期 {expected_date}")
        else:
            # 最新日期已到，检查覆盖度：有多少股票有 expected_date 的数据
            if latest_date == expected_date:
                covered = km.get_stock_count_on_date(expected_date)
                total = km.get_total_stock_count()
                if total > 0 and covered < total * 0.95:
                    need_download = True
                    logger.info(f"日期 {expected_date} 覆盖度不足: {covered}/{total} ({covered/total*100:.1f}%)，需要补下载")

        return {
            'latest_date': latest_date,
            'expected_date': expected_date,
            'need_download': need_download,
            'status_msg': status_msg
        }

    def _check_factor_coverage(self, target_date: str) -> Dict:
        """检查因子覆盖率（检查最近5个交易日）"""
        conn = get_sync_connection("kline", path=DB_PATH)
        cursor = conn.cursor()

        # 获取最近5个交易日
        cursor.execute('''
            SELECT DISTINCT trade_date FROM kline_data
            WHERE trade_date <= ? AND stock_code NOT LIKE '801%.SI'
            ORDER BY trade_date DESC LIMIT 5
        ''', (target_date,))
        recent_dates = [row[0] for row in cursor.fetchall()]

        if not recent_dates:
            return {'need_calc': False, 'coverage_pct': 100}

        # 检查每个日期的覆盖率
        min_coverage = 100
        worst_date = target_date
        total_missing = 0

        for date in recent_dates:
            # K线数量
            cursor.execute('''
                SELECT COUNT(DISTINCT stock_code) FROM kline_data
                WHERE trade_date = ? AND stock_code NOT LIKE '801%.SI'
            ''', (date,))
            kline_count = cursor.fetchone()[0]

            # 因子数量
            cursor.execute('''
                SELECT COUNT(DISTINCT stock_code) FROM stock_daily_factors
                WHERE trade_date = ?
            ''', (date,))
            factor_count = cursor.fetchone()[0]

            coverage_pct = factor_count / kline_count * 100 if kline_count > 0 else 0
            missing_count = kline_count - factor_count

            if coverage_pct < min_coverage:
                min_coverage = coverage_pct
                worst_date = date

            if missing_count > 0:
                total_missing += missing_count
                logger.info(f"  {date}: 因子{factor_count}/{kline_count}, 覆盖率{coverage_pct:.1f}%")

        # 判断是否需要补充（最低覆盖率低于95%）
        need_calc = min_coverage < 95

        if need_calc:
            logger.info(f"因子覆盖率不足: 最低 {min_coverage:.1f}% ({worst_date}), 总缺失 {total_missing} 条")
            return {
                'target_date': worst_date,
                'coverage_pct': min_coverage,
                'missing_count': total_missing,
                'need_calc': need_calc,
                'dates_checked': recent_dates
            }
        else:
            logger.info(f"因子覆盖率正常: 所有日期 >= 95%, 最低 {min_coverage:.1f}%")
            return {
                'coverage_pct': min_coverage,
                'need_calc': False,
                'dates_checked': recent_dates
            }

    def _get_expected_trading_day(self) -> tuple:
        """获取应有的最新交易日"""
        from services.data.local_data_service import get_trading_day_end_date
        return get_trading_day_end_date(use_sdk_calendar=True)

    def _run_kline_download(self) -> Dict:
        """执行K线增量下载（从数据库最新日期到应有交易日）"""
        logger.info("开始K线增量下载...")

        from services.common.task_manager import get_task_manager, TaskType
        task_manager = get_task_manager()

        # 检查是否有正在运行的下载任务
        if task_manager.is_running(TaskType.DATA_DOWNLOAD):
            logger.warning("已有下载任务在运行")
            return {'success': False, 'message': '已有下载任务在运行'}

        # 启动下载任务
        task_manager.start_task(TaskType.DATA_DOWNLOAD)
        task_manager.update_progress(TaskType.DATA_DOWNLOAD, 5, "正在初始化...")

        # 获取数据库最新K线日期，检查完整性
        from services.factors.kline_manager import get_kline_manager
        km = get_kline_manager()
        latest_date = km.get_global_latest_date()
        start_date = None  # None 表示首次下载近6个月
        end_date_offset = 1  # 结束日期 = 今天 + 1天

        if latest_date:
            # 检查最新日期的数据量是否完整
            latest_count = km.get_stock_count_on_date(latest_date)

            # 获取前一天数据量
            prev_dt = datetime.strptime(latest_date, '%Y-%m-%d') - timedelta(days=1)
            prev_date = prev_dt.strftime('%Y-%m-%d')
            prev_count = km.get_stock_count_on_date(prev_date)

            logger.info(f"最新日期 {latest_date}: {latest_count} 只, 前一日 {prev_date}: {prev_count} 只")

            if latest_count < prev_count:
                # 最新日期数据不完整，删除后重新下载
                deleted = km.delete_by_date(latest_date)
                logger.info(f"最新日期数据不完整，已删除 {deleted} 条记录，从 {latest_date} 开始重新下载")
                start_date = latest_date
            else:
                # 最新日期数据完整，从下一天开始增量下载
                next_dt = datetime.strptime(latest_date, '%Y-%m-%d') + timedelta(days=1)
                start_date = next_dt.strftime('%Y-%m-%d')
                logger.info(f"最新日期数据完整（{latest_count} >= {prev_count}），从 {start_date} 开始增量下载")

        # 结束日期 = 今天 + 1天
        end_date = (datetime.now(CHINA_TZ) + timedelta(days=1)).strftime('%Y-%m-%d')

        if start_date is None:
            logger.info("数据库无K线数据，首次下载近6个月数据")

        from services.data.local_data_service import download_incremental_kline_data_sync
        from dotenv import load_dotenv
        load_dotenv()

        try:
            success = download_incremental_kline_data_sync(
                start_date=start_date,
                end_date=end_date,
                calculate_factors=False,  # 因子计算由 _run_daily_factor_calc 单独处理
                download_industry=False   # 行业指数由独立任务 _run_industry_indices_download 处理
            )
        except Exception as e:
            logger.error(f"K线下载失败: {e}", exc_info=True)
            task_manager.fail_task(TaskType.DATA_DOWNLOAD, str(e))
            return {'success': False, 'message': str(e)}

        # 更新任务状态（无论成功失败都会执行）
        result = {'success': success, 'message': '下载完成' if success else '部分下载失败'}
        task_manager.update_progress(TaskType.DATA_DOWNLOAD, 100, "下载完成")
        if result['success']:
            task_manager.complete_task(TaskType.DATA_DOWNLOAD, result)
        else:
            task_manager.fail_task(TaskType.DATA_DOWNLOAD, result.get('message', '下载失败'))

        return result

    def _run_daily_factor_calc(self, start_date: Optional[str], end_date: str, force_full: bool = False) -> Dict:
        """执行日频因子计算"""
        logger.info(f"开始日频因子计算: {start_date or '全部'} 至 {end_date}, 强制全量={force_full}")

        from services.common.task_manager import get_task_manager, TaskType
        task_manager = get_task_manager()

        # 检查是否有正在运行的因子计算任务
        if task_manager.is_running(TaskType.DAILY_FACTOR_CALC):
            logger.warning("已有因子计算任务在运行")
            return {'success': False, 'message': '已有因子计算任务在运行'}

        # 启动因子计算任务
        task_manager.start_task(TaskType.DAILY_FACTOR_CALC)
        task_manager.update_progress(TaskType.DAILY_FACTOR_CALC, 5, "正在初始化...")

        try:
            # 执行因子计算
            from services.data.local_data_service import calculate_and_save_factors_for_dates

            # 计算日期范围
            calc_start = start_date or (get_china_time() - timedelta(days=120)).strftime('%Y-%m-%d')

            # 进度回调：每批次由 factor_service 内部通过 tracker 调用
            class FactorProgressTracker:
                def __init__(self, task_mgr, task_type):
                    self.task_mgr = task_mgr
                    self.task_type = task_type
                    self.last_pct = 0

                def update_sync(self, processed=0, current_stock="", message="", total_tasks=None):
                    # 从消息中提取批次进度
                    import re
                    match = re.search(r'批次\s+(\d+)/(\d+)', message) if message else None
                    if match:
                        batch, total = int(match.group(1)), int(match.group(2))
                        pct = int(batch / total * 90) + 5  # 5%~95%
                        if pct != self.last_pct:
                            self.last_pct = pct
                            self.task_mgr.update_progress(self.task_type, pct, message)
                    elif processed > 0:
                        self.task_mgr.update_progress(self.task_type, processed, message)

                def complete_sync(self):
                    pass

            tracker = FactorProgressTracker(task_manager, TaskType.DAILY_FACTOR_CALC)

            # 当覆盖率不足时，使用only_new_dates=False强制全量计算
            # 这样可以填充中间缺失的日期和没有因子记录的股票
            inserted = calculate_and_save_factors_for_dates(
                start_date=calc_start,
                end_date=end_date,
                only_new_dates=not force_full,
                show_progress=True,
                tracker=tracker
            )

            result = {
                'success': True,
                'inserted': inserted,
                'date_range': f'{calc_start} 至 {end_date}'
            }

            task_manager.update_progress(TaskType.DAILY_FACTOR_CALC, 100, "计算完成")
            task_manager.complete_task(TaskType.DAILY_FACTOR_CALC, result)
            return result

        except Exception as e:
            logger.error(f"日频因子计算失败: {e}", exc_info=True)
            task_manager.fail_task(TaskType.DAILY_FACTOR_CALC, str(e))
            return {'success': False, 'message': str(e)}

    def _run_monthly_factor_update(self) -> Dict:
        """执行月频因子更新"""
        logger.info("开始月频因子更新...")

        from services.common.task_manager import get_task_manager, TaskType
        task_manager = get_task_manager()

        # 检查是否有正在运行的月频因子任务
        if task_manager.is_running(TaskType.MONTHLY_FACTOR_UPDATE):
            logger.warning("已有月频因子更新任务在运行")
            return {'success': False, 'message': '已有月频因子更新任务在运行'}

        # 启动月频因子更新任务
        task_manager.start_task(TaskType.MONTHLY_FACTOR_UPDATE)

        try:
            # 执行月频因子更新
            from services.factors.monthly_factor_updater import run_monthly_factor_update
            from dotenv import load_dotenv
            load_dotenv()

            result = run_monthly_factor_update(mode='fill_empty')

            # 执行月度沿用填充
            if result.get('updated', 0) > 0:
                from services.factors.monthly_factor_filler import run_monthly_factor_fill
                fill_result = run_monthly_factor_fill()
                result['inherited'] = fill_result.get('filled', 0)

            task_manager.complete_task(TaskType.MONTHLY_FACTOR_UPDATE, result)
            return result

        except Exception as e:
            logger.error(f"月频因子更新失败: {e}", exc_info=True)
            task_manager.fail_task(TaskType.MONTHLY_FACTOR_UPDATE, str(e))
            return {'success': False, 'message': str(e)}

    def _run_full_kline_download(self) -> Dict:
        """执行K线全量下载"""
        logger.info("开始K线全量下载...")

        from services.common.task_manager import get_task_manager, TaskType
        task_manager = get_task_manager()

        if task_manager.is_running(TaskType.DATA_DOWNLOAD):
            logger.warning("已有下载任务在运行")
            return {'success': False, 'message': '已有下载任务在运行'}

        task_manager.start_task(TaskType.DATA_DOWNLOAD)
        task_manager.update_progress(TaskType.DATA_DOWNLOAD, 5, "正在初始化...")

        from services.data.local_data_service import download_all_kline_data_sync
        from dotenv import load_dotenv
        load_dotenv()

        try:
            success = download_all_kline_data_sync()
        except Exception as e:
            logger.error(f"K线全量下载失败: {e}", exc_info=True)
            task_manager.fail_task(TaskType.DATA_DOWNLOAD, str(e))
            return {'success': False, 'message': str(e)}

        result = {'success': success, 'message': '下载完成' if success else '部分下载失败'}
        task_manager.update_progress(TaskType.DATA_DOWNLOAD, 100, "下载完成")
        if result['success']:
            task_manager.complete_task(TaskType.DATA_DOWNLOAD, result)
        else:
            task_manager.fail_task(TaskType.DATA_DOWNLOAD, result.get('message', '下载失败'))

        return result

    def _run_weekly_kline_download(self) -> Dict:
        """执行周K线下载"""
        logger.info("开始周K线下载...")

        from services.common.task_manager import get_task_manager, TaskType
        task_manager = get_task_manager()

        if task_manager.is_running(TaskType.WEEKLY_KLINE_DOWNLOAD):
            logger.warning("已有周K线下载任务在运行")
            return {'success': False, 'message': '已有周K线下载任务在运行'}

        task_manager.start_task(TaskType.WEEKLY_KLINE_DOWNLOAD)
        task_manager.update_progress(TaskType.WEEKLY_KLINE_DOWNLOAD, 5, "正在初始化...")

        from services.data.download_weekly_kline import download_weekly_kline_sync
        from dotenv import load_dotenv
        load_dotenv()

        try:
            success = download_weekly_kline_sync(
                years=10,
                batch_size=50
            )
        except Exception as e:
            logger.error(f"周K线下载失败: {e}", exc_info=True)
            task_manager.fail_task(TaskType.WEEKLY_KLINE_DOWNLOAD, str(e))
            return {'success': False, 'message': str(e)}

        result = {'success': success, 'message': '下载完成' if success else '部分下载失败'}
        task_manager.update_progress(TaskType.WEEKLY_KLINE_DOWNLOAD, 100, "下载完成")
        if result['success']:
            task_manager.complete_task(TaskType.WEEKLY_KLINE_DOWNLOAD, result)
        else:
            task_manager.fail_task(TaskType.WEEKLY_KLINE_DOWNLOAD, result.get('message', '下载失败'))
        return result

    def _weekly_kline_check_job(self):
        """每周周K线数据下载任务"""
        logger.info("=" * 60)
        logger.info("开始周K线数据下载任务")
        logger.info("=" * 60)

        try:
            self._task_status['last_weekly_kline_check'] = get_china_time().isoformat()

            # 检查周K线覆盖度
            need_download, msg = self._check_weekly_kline_coverage()

            if need_download:
                logger.info(f"周K线覆盖度不足: {msg}，开始增量下载")
                # 周K线下载（外层 _run_builtin_task_threadsafe 已在独立线程）
                result = self._run_weekly_kline_download()
                self._task_status['last_weekly_kline_download'] = get_china_time().isoformat()
                self._task_status['weekly_kline_status'] = result

                if result.get('success'):
                    logger.info(f"周K线下载完成: {result}")
                else:
                    logger.warning(f"周K线下载失败: {result}")
            else:
                logger.info(f"周K线数据已覆盖: {msg}")
                result = {'success': True, 'message': '数据已覆盖', 'detail': msg}
                self._task_status['weekly_kline_status'] = {'status': 'up_to_date'}

            logger.info("=" * 60)
            logger.info("周K线数据下载任务完成")
            logger.info("=" * 60)

            return result

        except Exception as e:
            logger.error(f"周K线下载任务异常: {e}", exc_info=True)
            self._task_status['weekly_kline_status'] = {'success': False, 'message': str(e)}
            return {'success': False, 'message': str(e)}

    def _check_weekly_kline_coverage(self) -> Tuple[bool, str]:
        """检查周K线覆盖度

        同时检查：
        1. 股票覆盖率是否 >= 95%
        2. 数据是否包含最近一个已完成周（截至上周五）
        防止周中手动下载导致数据不完整
        """
        km = get_kline_manager()

        # 获取最近一个有周K线数据的周末
        latest_week = km.get_weekly_latest_date()

        if not latest_week:
            return True, "无周K线数据"

        # 计算最近一个已完成的周五
        today = km.get_last_completed_week_end().date()
        last_friday_str = today.strftime('%Y-%m-%d')

        # 检查是否已包含最近一个完整周的数据
        if latest_week < last_friday_str:
            return True, f"数据截至 {latest_week}，需更新到 {last_friday_str}"

        # 统计有多少股票有周K线数据
        weekly_stocks = km.get_weekly_stock_count()

        # 统计总股票数
        total_stocks = km.get_total_stock_count()

        coverage_pct = weekly_stocks / total_stocks * 100 if total_stocks > 0 else 0

        if coverage_pct < 95:
            return True, f"{weekly_stocks}/{total_stocks} ({coverage_pct:.1f}%)"

        return False, f"已覆盖 {weekly_stocks}/{total_stocks}，数据截至 {latest_week}"

    def _run_industry_indices_download(self) -> Dict:
        """执行申万行业指数下载（带5分钟超时保护）"""
        logger.info("开始申万行业指数下载...")

        from dotenv import load_dotenv
        load_dotenv()

        result: Dict = {}
        def _do_download():
            try:
                from services.data.local_data_service import download_industry_indices
                result.update(download_industry_indices())
            except Exception as e:
                result.update({'success': False, 'message': str(e)})

        thread = threading.Thread(target=_do_download)
        thread.start()
        thread.join(timeout=300)  # 5分钟超时

        if thread.is_alive():
            logger.error("申万行业指数下载超时（5分钟）")
            return {'success': False, 'message': '下载超时（5分钟）'}

        if result.get('success'):
            logger.info(f"申万行业指数下载完成: {result.get('saved', 0)} 条")
        else:
            logger.warning(f"申万行业指数下载失败: {result.get('message', '未知错误')}")

        return result

    def get_status(self) -> Dict:
        """获取调度服务状态（含健康检查日志）"""
        # 健康检查日志：定期打印 scheduler 状态
        import threading
        thread_names = [t.name for t in threading.enumerate()]
        aps_threads = [n for n in thread_names if 'apscheduler' in n.lower() or 'apsched' in n.lower()]
        scheduler_running = bool(self._scheduler and self._scheduler.running)
        job_count = len(self._scheduler.get_jobs()) if self._scheduler else 0

        if not scheduler_running or not aps_threads:
            logger.warning(
                f"Scheduler 健康检查异常: running={self._running}, "
                f"scheduler.running={scheduler_running}, jobs={job_count}, "
                f"daemon_threads={aps_threads}"
            )

        jobs = []
        if self._scheduler:
            for job in self._scheduler.get_jobs():
                next_run = str(job.next_run_time) if job.next_run_time else None
                if next_run and '+' in next_run:
                    next_run = next_run.split('+')[0]
                jobs.append({
                    'id': job.id,
                    'name': job.name,
                    'next_run_time': next_run,
                    'trigger': str(job.trigger)
                })

        return {
            'running': self._running,
            'scheduler_running': scheduler_running,
            'daemon_threads': aps_threads,
            'scheduler_type': 'APScheduler' if self._scheduler else 'Simple',
            'jobs': jobs,
            'current_task': self._current_task,
            'task_status': self._task_status
        }

    def run_manual_kline_check(self, full: bool = False) -> Dict:
        """手动触发K线数据检查"""
        logger.info(f"手动触发K线数据{'全量下载' if full else '检查'}")
        if full:
            thread = threading.Thread(target=self._run_full_kline_download)
        else:
            thread = threading.Thread(target=self._daily_kline_check_job)
        thread.start()
        return {'success': True, 'message': 'K线数据' + ('全量下载任务' if full else '检查任务') + '已启动'}

    def run_manual_weekly_kline_download(self) -> Dict:
        """手动触发周K线下载"""
        logger.info("手动触发周K线下载")
        thread = threading.Thread(target=self._run_weekly_kline_download)
        thread.start()
        return {'success': True, 'message': '周K线下载任务已启动'}

    def run_manual_monthly_check(self) -> Dict:
        """手动触发月频因子更新"""
        logger.info("手动触发月频因子更新")
        thread = threading.Thread(target=self._monthly_factor_check_job)
        thread.start()
        return {'success': True, 'message': '月频因子更新任务已启动'}

    def run_manual_industry_indices_download(self) -> Dict:
        """手动触发申万行业指数下载"""
        logger.info("手动触发申万行业指数下载")
        thread = threading.Thread(target=self._run_industry_indices_download)
        thread.start()
        return {'success': True, 'message': '申万行业指数下载任务已启动'}

    def _register_strategy_tasks(self):
        """从 strategy_tasks 表读取 enabled 任务，注册到 APScheduler"""
        try:
            from services.tasks import get_task
            db_path = Path(__file__).parent.parent.parent / "data" / "stockwinner.db"
            conn = get_sync_connection(path=db_path)
            conn.row_factory = sqlite3.Row
            rows = conn.execute("SELECT * FROM strategy_tasks WHERE enabled = 1").fetchall()

            count = 0
            for task in rows:
                job_id = f'task_{task["id"]}'
                task_type = task["task_type"] if "task_type" in task.keys() else "strategy"
                module = task["module"] if task["module"] else None

                # 生成可读任务名
                if task_type == "builtin" and module:
                    info = get_task(module)
                    job_name = f"内置任务: {info['name']}" if info else f"内置任务: {module}"
                else:
                    job_name = f'策略任务: {task["account_id"]}/{task["group_id"]}'

                self._scheduler.add_job(
                    self._execute_strategy_task_job,
                    CronTrigger.from_crontab(task["cron_expression"], timezone=CHINA_TZ),
                    id=job_id,
                    name=job_name,
                    args=[task["id"]],
                    replace_existing=True,
                )
                count += 1
                logger.info(f"  注册任务: {job_id} (cron={task['cron_expression']})")

            logger.info(f"共注册 {count} 个任务")

            # 注册交易监控自动启停任务（已禁用）
            # self._register_monitor_cron_jobs()

        except Exception as e:
            logger.error(f"注册任务失败: {e}", exc_info=True)

    def _register_monitor_auto_start_job(self):
        """注册交易监控自动启动任务 — 9:15 + 13:00（周一至周五，仅交易日）"""
        try:
            # 早盘启动：9:15
            self._scheduler.add_job(
                self._monitor_auto_start_job,
                CronTrigger.from_crontab("15 9 * * mon-fri", timezone=CHINA_TZ),
                id="monitor_auto_start",
                name="系统任务: 自动启动交易监控",
                replace_existing=True,
            )
            logger.info("  注册监控任务: monitor_auto_start (cron=15 9 * * mon-fri)")

            # 午盘启动：13:00（覆盖后端在午间重启的场景）
            self._scheduler.add_job(
                self._monitor_auto_start_job,
                CronTrigger.from_crontab("0 13 * * mon-fri", timezone=CHINA_TZ),
                id="monitor_auto_start_afternoon",
                name="系统任务: 自动启动交易监控（午后）",
                replace_existing=True,
            )
            logger.info("  注册监控任务: monitor_auto_start_afternoon (cron=0 13 * * mon-fri)")
        except Exception as e:
            logger.error(f"注册监控自动启动任务失败: {e}")

    def _register_heartbeat_job(self):
        """注册 scheduler 心跳检查任务（每 30 分钟），用于确认 daemon 线程存活"""
        try:
            self._scheduler.add_job(
                self._heartbeat_job,
                CronTrigger.from_crontab("*/30 * * * *", timezone=CHINA_TZ),
                id="scheduler_heartbeat",
                name="系统任务: Scheduler 心跳检查",
                replace_existing=True,
            )
            logger.info("  注册心跳任务: scheduler_heartbeat (cron=*/30 * * * *)")
        except Exception as e:
            logger.error(f"注册心跳任务失败: {e}")

    def _register_price_cache_fallback_job(self):
        """注册 price_cache 心跳兜底任务（每 5 分钟），仅在监控循环挂掉时刷新"""
        try:
            self._scheduler.add_job(
                self._price_cache_fallback_job,
                CronTrigger.from_crontab("*/5 * * * *", timezone=CHINA_TZ),
                id="price_cache_fallback",
                name="系统任务: 行情缓存心跳兜底",
                replace_existing=True,
            )
            logger.info("  注册行情缓存心跳任务: price_cache_fallback (cron=*/5 * * * *)")
        except Exception as e:
            logger.error(f"注册行情缓存心跳任务失败: {e}")

    def _price_cache_fallback_job(self):
        """心跳检查：管理 PriceCache TTL + 持仓/watchlist 股缓存过期时强制刷新"""
        from services.data.local_data_service import is_trading_hours
        from services.common.price_cache import get_price_cache

        # 动态调整 PriceCache TTL：非交易时段 12 小时，交易时段 10 分钟
        cache = get_price_cache()
        if is_trading_hours():
            if cache._ttl != 600:
                cache.set_ttl(600)
        else:
            if cache._ttl != 43200:
                cache.set_ttl(43200)

        try:
            from services.trading.gateway_dispatcher import get_gateway_dispatcher
            dispatcher = get_gateway_dispatcher()
            status = dispatcher.get_status()
            monitor_age = status.get("monitor_age_seconds")

            if is_trading_hours():
                # 交易时段：检查 monitor 订阅是否长时间未刷新
                if monitor_age is not None and monitor_age < 300:
                    return  # 监控循环正常，跳过

                if monitor_age is not None:
                    logger.warning(f"price_cache 心跳: monitor 订阅已 {monitor_age:.0f} 秒未刷新，触发兜底")
                else:
                    logger.warning(f"price_cache 心跳: monitor 订阅不存在，触发兜底")

                if "monitor" in status.get("subscriptions", {}):
                    import asyncio
                    loop = asyncio.new_event_loop()
                    try:
                        loop.run_until_complete(dispatcher.refresh_now("monitor"))
                    finally:
                        loop.close()
            else:
                # 非交易时段：数据过期时触发一次刷新（SDK query_kline 可获取当日收盘价）
                if not status.get("data_stale") and status.get("sdk_healthy"):
                    return  # 数据正常，跳过

                logger.info(f"price_cache 心跳: 非交易时段数据过期，触发 SDK kline 兜底")
                if "monitor" in status.get("subscriptions", {}):
                    import asyncio
                    loop = asyncio.new_event_loop()
                    try:
                        loop.run_until_complete(dispatcher.refresh_now("monitor"))
                    finally:
                        loop.close()

            # --- 持仓 + watchlist 股缓存新鲜度检查（交易/非交易时段均执行） ---
            self._check_and_refresh_user_cache(cache, dispatcher)

        except Exception as e:
            logger.warning(f"price_cache 心跳兜底失败: {e}")

    def _check_and_refresh_user_cache(self, cache, dispatcher):
        """检查持仓股和 watchlist 股的 PriceCache 新鲜度，stale 超过 300 秒时触发 dispatcher 刷新"""
        import asyncio
        import time
        from services.common.database import get_sync_connection
        from services.common.stock_code import normalize_stock_code

        try:
            # 收集持仓股 + watchlist 股
            stock_codes: set = set()
            db_path = Path(__file__).parent.parent.parent / "data" / "stockwinner.db"
            conn = get_sync_connection(path=db_path)

            rows = conn.execute(
                "SELECT DISTINCT stock_code FROM stock_positions WHERE quantity > 0"
            ).fetchall()
            for r in rows:
                stock_codes.add(normalize_stock_code(r[0]))

            rows = conn.execute(
                "SELECT DISTINCT stock_code FROM watchlist WHERE status IN ('pending', 'watching', 'bought') AND is_active = 1"
            ).fetchall()
            for r in rows:
                stock_codes.add(normalize_stock_code(r[0]))

            conn.close()

            if not stock_codes:
                return

            # 检查哪些股票 stale 超过 300 秒
            stale_codes = set()
            for code in stock_codes:
                entry = cache.get_ohlcv_with_ttl(code)
                if not entry or not entry.get('is_fresh') or entry.get('data', {}).get('close', 0) <= 0:
                    last_update = entry.get('updated_at', 0) if entry else 0
                    if last_update == 0 or (time.time() - last_update) > 300:
                        stale_codes.add(code)

            if not stale_codes:
                return

            logger.info(f"price_cache 心跳: {len(stale_codes)}/{len(stock_codes)} 只用户股票缓存过期，触发刷新")

            # 通过 dispatcher 后台异步刷新
            sub_id = "_user_cache_fallback"
            dispatcher.subscribe(sub_id, stale_codes, interval=0, priority=3)

            loop = asyncio.new_event_loop()
            try:
                loop.run_until_complete(dispatcher.refresh_now(sub_id))
            finally:
                loop.close()
            dispatcher.unsubscribe(sub_id)

            refreshed = sum(
                1 for code in stale_codes
                if (e := cache.get_ohlcv_with_ttl(code)) and e.get('data', {}).get('close', 0) > 0
            )
            logger.info(f"price_cache 心跳: 刷新完成 {refreshed}/{len(stale_codes)} 只有效行情")

        except Exception as e:
            logger.debug(f"用户缓存新鲜度检查失败: {e}")

    def _heartbeat_job(self):
        """Scheduler 心跳检查，每 30 分钟执行一次
        同时检查交易监控是否存活（交易日交易时段内）
        """
        import threading
        from services.common.timezone import get_china_time

        thread_names = [t.name for t in threading.enumerate()]
        aps_threads = [n for n in thread_names if 'apscheduler' in n.lower() or 'apsched' in n.lower()]
        scheduler_ok = self._scheduler and self._scheduler.running
        now_str = get_china_time().strftime("%H:%M")

        logger.info(
            f"Scheduler 心跳: running={scheduler_ok}, daemon_threads={aps_threads}, "
            f"总线程数={len(threading.enumerate())}"
        )

        # --- 异常检测：发飞书通知 ---
        if not scheduler_ok:
            try:
                from services.notifications import get_notification_service
                notification = get_notification_service()
                # 尝试给所有有通知配置的账户发警报
                conn = get_sync_connection(path=Path(__file__).parent.parent.parent / "data" / "stockwinner.db")
                account_ids = [r["account_id"] for r in conn.execute(
                    "SELECT DISTINCT account_id FROM accounts WHERE is_active = 1"
                ).fetchall()]
                conn.close()

                for acct_id in account_ids:
                    import asyncio
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    try:
                        loop.run_until_complete(notification.emit(
                            event_type="scheduler_down",
                            account_id=acct_id,
                            payload={
                                "detected_at": now_str,
                                "detail": "Scheduler daemon 线程已死亡，需重启后端",
                            },
                        ))
                    finally:
                        loop.close()
                logger.warning("已发送 Scheduler 异常飞书通知")
            except Exception as e:
                logger.error(f"发送 Scheduler 通知失败: {e}")

        # --- 检查交易监控是否存活（仅交易日交易时段） ---
        if scheduler_ok:
            try:
                from services.trading.trading_hours import is_today_trading_day
                from services.trading.trading_hours import is_trading_time as _is_trading_hours

                if is_today_trading_day() and _is_trading_hours():
                    from services.monitoring.service import get_trading_monitor
                    from services.notifications import get_notification_service

                    monitor = get_trading_monitor()

                    # ① 检查监控是否运行
                    if not monitor._running:
                        # 交易时段内监控未运行，尝试自动重启
                        logger.warning("交易监控在交易时段内未运行，尝试自动重启")

                        # 检查是否有持仓
                        positions_conn = get_sync_connection(
                            path=Path(__file__).parent.parent.parent / "data" / "stockwinner.db"
                        )
                        position_count = positions_conn.execute(
                            "SELECT COUNT(*) FROM stock_positions WHERE quantity > 0"
                        ).fetchone()[0]
                        acct_rows = positions_conn.execute(
                            "SELECT DISTINCT account_id FROM stock_positions WHERE quantity > 0"
                        ).fetchall()
                        positions_conn.close()

                        # 先尝试自动重启（用 run_async_safe 确保数据库连接池正常初始化）
                        import asyncio
                        from services.common.async_helper import run_async_safe

                        result: dict = {}
                        def _restart_monitor():
                            loop = asyncio.new_event_loop()
                            asyncio.set_event_loop(loop)
                            try:
                                result.update(loop.run_until_complete(
                                    monitor.start_monitoring(interval=60)
                                ))
                            finally:
                                loop.close()

                        run_async_safe(_restart_monitor)
                        if result.get("success"):
                            logger.info(f"交易监控已自动重启: {result.get('message')}")
                        else:
                            logger.warning(f"交易监控重启失败: {result.get('message')}")
                            # 重启失败才发通知
                            if position_count > 0:
                                notification = get_notification_service()
                                for acct_id in [r["account_id"] for r in acct_rows]:
                                    import asyncio as _asyncio
                                    nloop = _asyncio.new_event_loop()
                                    _asyncio.set_event_loop(nloop)
                                    try:
                                        nloop.run_until_complete(notification.emit(
                                            event_type="monitor_interrupted",
                                            account_id=acct_id,
                                            payload={
                                                "detected_at": now_str,
                                                "account_id": acct_id,
                                                "position_count": position_count,
                                                "restart_result": result.get("message"),
                                            },
                                        ))
                                    finally:
                                        nloop.close()
                                logger.warning(f"已发送交易监控中断飞书通知（{position_count} 只持仓，重启失败）")
                            else:
                                logger.info("交易监控未运行，但无持仓，不发通知")

                    # ② 检查数据是否过期（监控活着但 SDK 取不到数据）
                    elif monitor._data_stale:
                        logger.warning(f"交易监控运行中但数据已过期，最后成功时间={monitor._last_data_time}")
                        positions_conn = get_sync_connection(
                            path=Path(__file__).parent.parent.parent / "data" / "stockwinner.db"
                        )
                        position_count = positions_conn.execute(
                            "SELECT COUNT(*) FROM stock_positions WHERE quantity > 0"
                        ).fetchone()[0]
                        acct_rows = positions_conn.execute(
                            "SELECT DISTINCT account_id FROM stock_positions WHERE quantity > 0"
                        ).fetchall()
                        positions_conn.close()
                        if position_count > 0:
                            for acct_id in [r["account_id"] for r in acct_rows]:
                                notification = get_notification_service()
                                loop = asyncio.new_event_loop()
                                asyncio.set_event_loop(loop)
                                try:
                                    loop.run_until_complete(notification.emit(
                                        event_type="monitor_data_stale",
                                        account_id=acct_id,
                                        payload={
                                            "detected_at": now_str,
                                            "account_id": acct_id,
                                            "last_data_time": monitor._last_data_time or "无",
                                            "sdk_error_msg": monitor._sdk_error_msg or "",
                                        },
                                    ))
                                finally:
                                    loop.close()
                            logger.warning("已发送行情数据过期飞书通知")
            except Exception as e:
                logger.error(f"检查交易监控状态失败: {e}")

    def _monitor_auto_start_job(self):
        """9:15 定时任务 — 自动启动交易监控"""
        logger.info("执行交易监控自动启动任务")

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self._do_monitor_auto_start())
        finally:
            loop.close()

    async def _do_monitor_auto_start(self):
        """检查并自动启动交易监控"""
        from services.trading.trading_hours import is_today_trading_day, can_trade
        from services.monitoring.service import get_trading_monitor

        if not is_today_trading_day():
            logger.info("今天非交易日，跳过交易监控自动启动")
            return

        monitor = get_trading_monitor()
        if monitor._running:
            logger.info("交易监控已在运行，跳过自动启动")
            return

        # 自动启动所有活跃账户的监控
        result = await monitor.start_monitoring(interval=60)
        logger.info(f"交易监控自动启动结果: {result}")

    def reload_strategy_tasks(self):
        """重新加载策略任务：移除已有 job，重新注册"""
        try:
            from services.tasks import get_task

            # 移除所有 task_ 前缀的 job
            for job in list(self._scheduler.get_jobs()):
                if job.id.startswith('task_'):
                    self._scheduler.remove_job(job.id)

            db_path = Path(__file__).parent.parent.parent / "data" / "stockwinner.db"
            conn = get_sync_connection(path=db_path)
            conn.row_factory = sqlite3.Row
            rows = conn.execute("SELECT * FROM strategy_tasks WHERE enabled = 1").fetchall()

            count = 0
            for task in rows:
                job_id = f'task_{task["id"]}'
                task_type = task["task_type"] if "task_type" in task.keys() else "strategy"
                module = task["module"] if task["module"] else None

                if task_type == "builtin" and module:
                    info = get_task(module)
                    job_name = f"内置任务: {info['name']}" if info else f"内置任务: {module}"
                else:
                    job_name = f'策略任务: {task["account_id"]}/{task["group_id"]}'

                self._scheduler.add_job(
                    self._execute_strategy_task_job,
                    CronTrigger.from_crontab(task["cron_expression"], timezone=CHINA_TZ),
                    id=job_id,
                    name=job_name,
                    args=[task["id"]],
                    replace_existing=True,
                )
                count += 1
                logger.info(f"  重新注册任务: {job_id} (cron={task['cron_expression']})")

            logger.info(f"策略任务重新加载完成，共 {count} 个任务")

            # 重新注册监控自动启动任务（9:15 + 13:00）
            self._register_monitor_auto_start_job()

            # 重新注册心跳任务
            self._register_heartbeat_job()

            return {"success": True, "count": count}

        except Exception as e:
            logger.error(f"重新加载任务失败: {e}", exc_info=True)
            return {"success": False, "error": str(e)}

    def _register_monitor_cron_jobs(self):
        """注册交易监控自动启停任务"""
        try:
            db_path = Path(__file__).parent.parent.parent / "data" / "stockwinner.db"
            conn = get_sync_connection(path=db_path)
            conn.row_factory = sqlite3.Row

            # 获取所有激活账户
            accounts = conn.execute(
                "SELECT account_id FROM accounts WHERE is_active = 1"
            ).fetchall()

            for acct in accounts:
                acct_id = acct["account_id"]

                # 开盘前启动监控（09:20）
                start_job_id = f'monitor_start_{acct_id}'
                self._scheduler.add_job(
                    self._auto_start_monitor_job,
                    CronTrigger.from_crontab("20 9 * * mon-fri", timezone=CHINA_TZ),
                    id=start_job_id,
                    name=f"自动启动监控: {acct_id}",
                    args=[acct_id],
                    replace_existing=True,
                )
                logger.info(f"  注册监控任务: {start_job_id} (cron=20 9 * * mon-fri)")

                # 收盘后停止监控（15:05）
                stop_job_id = f'monitor_stop_{acct_id}'
                self._scheduler.add_job(
                    self._auto_stop_monitor_job,
                    CronTrigger.from_crontab("5 15 * * mon-fri", timezone=CHINA_TZ),
                    id=stop_job_id,
                    name=f"自动停止监控: {acct_id}",
                    args=[acct_id],
                    replace_existing=True,
                )
                logger.info(f"  注册停监控任务: {stop_job_id} (cron=5 15 * * mon-fri)")

            # T+1 解冻持仓 + 重置 watchlist pending 状态（每日收盘后 15:05）
            self._scheduler.add_job(
                self._auto_unfreeze_positions_job,
                CronTrigger.from_crontab("5 15 * * mon-fri", timezone=CHINA_TZ),
                id="t1_unfreeze",
                name="T+1 持仓解冻",
                replace_existing=True,
            )
            logger.info(f"  注册 T+1 解冻任务 (cron=5 15 * * mon-fri)")

            # 收盘后失效当日单（15:05）
            self._scheduler.add_job(
                self._expire_day_orders_job,
                CronTrigger.from_crontab("5 15 * * mon-fri", timezone=CHINA_TZ),
                id="expire_day_orders",
                name="收盘后失效当日单",
                replace_existing=True,
            )
            logger.info(f"  注册收盘失效当日单任务 (cron=5 15 * * mon-fri)")

        except Exception as e:
            logger.error(f"注册监控任务失败: {e}", exc_info=True)

    def is_today_trading_day(self) -> bool:
        """判断今天是否为交易日（委托给 trading_hours.py 统一管理）"""
        from services.trading.trading_hours import is_today_trading_day as _is_trading_day
        return _is_trading_day()

    def _auto_start_monitor_job(self, account_id: str):
        """自动启动交易监控 — 通过 FastAPI 事件循环执行"""
        from services.monitoring.service import get_trading_monitor

        monitor = get_trading_monitor()
        if monitor._running:
            logger.info(f"监控已在运行，跳过启动: {account_id}")
            return

        loop = _get_fastapi_loop()
        if loop and loop.is_running():
            asyncio.run_coroutine_threadsafe(
                monitor.start_monitoring(account_id, interval=60),
                loop
            )
            logger.info(f"已提交监控启动请求: {account_id}")
        else:
            logger.warning(f"FastAPI 事件循环不可用，无法启动监控: {account_id}")

    def _auto_stop_monitor_job(self, account_id: str):
        """自动停止交易监控 — 通过 FastAPI 事件循环执行"""
        from services.monitoring.service import get_trading_monitor

        monitor = get_trading_monitor()
        if not monitor._running:
            return

        loop = _get_fastapi_loop()
        if loop and loop.is_running():
            asyncio.run_coroutine_threadsafe(
                monitor.stop_monitoring(),
                loop
            )
            logger.info(f"已提交监控停止请求: {account_id}")
        else:
            logger.warning(f"FastAPI 事件循环不可用，无法停止监控: {account_id}")

    def _auto_unfreeze_positions_job(self):
        """T+1 持仓解冻"""
        logger.info("开始 T+1 持仓解冻")
        run_async_safe(self._do_unfreeze)

    def _expire_day_orders_job(self):
        """收盘后失效当日单（长期单保持 pending，第二天重新提交委托）"""
        logger.info("开始收盘后失效当日单")
        run_async_safe(self._do_expire_day_orders)

    async def _do_expire_day_orders(self):
        """执行收盘后当日单失效处理

        规则：
        - 当日单（order_type='day'）：pending → expired（券商端已自动失效，无需撤单）
          executed → 重置为 pending（次日重新提交委托）
        - 长期单（order_type='gtc'）：executed → 重置为 pending（次日重新提交券商委托）
          pending → 保持不变（次日扫描执行）
        """
        from services.common.database import get_db_manager
        from services.common.timezone import format_china_time
        from services.common.structured_logger import get_logger

        db = get_db_manager()
        log = get_logger("scheduler")
        now = format_china_time()

        # 1. 失效当日 pending 信号
        result = await db.execute(
            "UPDATE trading_signals SET status = 'expired', executed_at = ? WHERE status = 'pending' AND order_type = 'day'",
            (now,)
        )
        expired_count = getattr(result, 'rowcount', 0) if hasattr(result, 'rowcount') else 0

        # 2. 当日 executed 信号 → 重置为 pending（次日重新提交）
        result = await db.execute(
            "UPDATE trading_signals SET status = 'pending', executed_at = NULL WHERE status = 'executed' AND order_type = 'day'",
        )
        day_reset_count = getattr(result, 'rowcount', 0) if hasattr(result, 'rowcount') else 0

        # 3. 长期单 executed → 重置为 pending（次日重新提交委托）
        result = await db.execute(
            "UPDATE trading_signals SET status = 'pending', executed_at = NULL WHERE status = 'executed' AND order_type = 'gtc'",
        )
        gtc_reset_count = getattr(result, 'rowcount', 0) if hasattr(result, 'rowcount') else 0

        # 4. 统计剩余 pending 长期单（之前未 executed 的）
        gtc_pending = await db.fetchall(
            "SELECT account_id, stock_code, stock_name, signal_type FROM trading_signals WHERE status = 'pending' AND order_type = 'gtc'"
        )

        log.log_event("day_orders_expired",
            f"收盘处理: 失效 {expired_count} 个当日pending单, 重置 {day_reset_count} 个当日已执行单, 重置 {gtc_reset_count} 个长期已执行单",
            expired_count=expired_count, day_reset_count=day_reset_count,
            gtc_reset_count=gtc_reset_count, gtc_pending_count=len(gtc_pending))

        if expired_count > 0:
            logger.info(f"失效当日pending单: {expired_count} 个")
        if day_reset_count > 0:
            logger.info(f"重置当日已执行单: {day_reset_count} 个（次日重新委托）")
        if gtc_reset_count > 0:
            logger.info(f"重置长期已执行单: {gtc_reset_count} 个（次日重新委托）")
        if gtc_pending:
            gtc_info = [f"{g['account_id']}:{g['stock_code']}({g['signal_type']})" for g in gtc_pending]
            logger.info(f"待执行长期单: {', '.join(gtc_info)}")

    async def _do_unfreeze(self):
        """执行 T+1 解冻 + 重置 pending 状态为 watching"""
        import sqlite3
        from services.common.database import get_db_manager
        db = get_db_manager()
        accounts = await db.fetchall("SELECT DISTINCT account_id FROM stock_positions")
        for row in accounts:
            try:
                from services.trading.position_manager import get_position_manager
                pm = get_position_manager(row["account_id"])
                count = await pm.unfreeze_positions()
                if count > 0:
                    logger.info(f"T+1 解冻账户 {row['account_id']}: {count} 只股票")

                # 将该账户 watchlist 中 pending 状态重置为 watching
                # 但排除今天有买入的股票（T+1 规则，次日才能交易）
                # 同时排除手动卖出信号（清仓/减仓），避免监控程序执行前被重置丢失
                from services.common.timezone import get_china_time
                today = get_china_time().strftime("%Y-%m-%d")
                result = await db.execute(
                    """UPDATE watchlist SET status = 'watching'
                       WHERE account_id = ? AND status = 'pending'
                       AND NOT (source_type = 'manual' AND signal_type = 'sell')
                       AND stock_code NOT IN (
                           SELECT stock_code FROM trade_records
                           WHERE account_id = ? AND trade_type = 'buy'
                             AND date(trade_time) = ?
                       )""",
                    (row["account_id"], row["account_id"], today)
                )
                reset_count = getattr(result, 'rowcount', 0) if hasattr(result, 'rowcount') else 0
                if reset_count > 0:
                    logger.info(f"重置 pending→watching: {row['account_id']}: {reset_count} 只股票")
            except Exception as e:
                logger.warning(f"T+1 解冻失败 ({row['account_id']}): {e}")

    def _execute_strategy_task_job(self, task_id: int):
        """执行策略任务（根据任务类型选择执行方式）"""
        logger.info(f"开始执行策略任务 ID={task_id}")

        # 先获取任务类型，决定执行方式
        try:
            from services.common.database import get_sync_connection
            db_path = Path(__file__).parent.parent.parent / "data" / "stockwinner.db"
            conn = get_sync_connection(path=db_path)
            task_row = conn.execute("SELECT task_type, module FROM strategy_tasks WHERE id = ?", (task_id,)).fetchone()
            conn.close()
            task_type = task_row["task_type"] if task_row else "strategy"
            module = task_row["module"] if task_row else None
        except Exception:
            task_type = "strategy"
            module = None

        # 内置任务（K线下载、因子计算等）是阻塞同步函数，在独立线程执行
        # 使用 threading.RLock 的 SDKProxyClient 无跨循环问题
        # 使用 sync sqlite3 连接池（不访问 aiosqlite pool）
        if task_type == "builtin" and module in ("kline_check", "monthly_factors", "weekly_kline", "industry_indices"):
            logger.info(f"内置阻塞任务 {task_id} ({module})，在独立线程执行")
            thread = threading.Thread(
                target=self._run_builtin_task_threadsafe,
                args=(task_id,),
                daemon=True
            )
            thread.start()
            return

        # 策略任务：提交到主循环执行（避免跨循环竞争）
        main_loop = _get_fastapi_loop()
        if main_loop and main_loop.is_running():
            future = asyncio.run_coroutine_threadsafe(
                self._execute_strategy_task(task_id, force=False),
                main_loop
            )
            try:
                result = future.result(timeout=300)
                logger.info(f"策略任务 {task_id} 完成: {result}")
            except TimeoutError:
                logger.error(f"策略任务 {task_id} 超时（300秒）")
                future.cancel()
            except Exception as e:
                logger.error(f"策略任务 {task_id} 执行异常: {e}", exc_info=True)
        else:
            logger.warning(f"FastAPI 主循环不可用，创建临时循环执行任务 {task_id}")
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(self._execute_strategy_task(task_id))
            except Exception as e:
                logger.error(f"策略任务 {task_id} 线程异常: {e}", exc_info=True)
            finally:
                loop.close()

    def _run_builtin_task_threadsafe(self, task_id: int):
        """在独立线程中运行内置阻塞任务"""
        # 创建临时事件循环执行 async 部分
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self._execute_strategy_task(task_id, force=False))
            logger.info(f"内置任务 {task_id} 完成")
        except Exception as e:
            logger.error(f"内置任务 {task_id} 异常: {e}", exc_info=True)
            try:
                from services.common.database import get_sync_connection
                import json as _json
                db_path = Path(__file__).parent.parent.parent / "data" / "stockwinner.db"
                conn = get_sync_connection(path=db_path)
                conn.execute(
                    "UPDATE strategy_tasks SET last_status='error', last_output=? WHERE id=?",
                    (_json.dumps({"error": str(e)}, ensure_ascii=False), task_id)
                )
                conn.commit()
                conn.close()
            except Exception:
                pass
        finally:
            loop.close()

    async def _execute_strategy_task(self, task_id: int, force: bool = False):
        """异步执行任务（支持 builtin 和 strategy 两种类型）

        Args:
            force: True=手动触发，跳过 require_trading_day 检查
        """
        from services.common.database import get_db_manager
        import json

        db = get_db_manager()

        # 获取任务信息
        task = await db.fetchone("SELECT * FROM strategy_tasks WHERE id = ?", (task_id,))
        if not task:
            logger.error(f"任务 {task_id} 不存在")
            return

        # 防止重复启动：任务已在运行时跳过（NULL 视为未执行过）
        if task.get("last_status") == "running":
            logger.info(f"任务 {task_id} 已在运行中，跳过重复执行")
            return

        task_type = task.get("task_type", "strategy")

        # 交易日检查：require_trading_day=1 且今天不是交易日时跳过执行
        # 手动触发（force=True）时跳过此检查
        if not force and task.get("require_trading_day"):
            from services.trading.trading_hours import is_today_trading_day
            if not is_today_trading_day():
                logger.info(f"任务 {task_id} 要求交易日，今天非交易日，跳过执行")
                await db.execute(
                    "UPDATE strategy_tasks SET last_run_at = ?, last_status = 'skipped', last_output = ? WHERE id = ?",
                    (get_china_time().isoformat(), json.dumps({"message": "今天非交易日"}, ensure_ascii=False), task_id)
                )
                return

        try:
            # 原子 CAS：last_status 不是 'running' 才更新，防止并发重复执行
            # 注意：SQL 中 NULL != 'running' 结果是 NULL 不是 True，必须显式处理 NULL
            result = await db.execute(
                "UPDATE strategy_tasks SET last_run_at = ?, last_status = 'running' "
                "WHERE id = ? AND (last_status != 'running' OR last_status IS NULL)",
                (get_china_time().isoformat(), task_id)
            )
            if getattr(result, 'rowcount', 1) == 0:
                logger.info(f"任务 {task_id} 已被其他请求抢占执行，跳过")
                return

            if task_type == "builtin":
                # 内置功能任务
                module = task.get("module")
                if not module:
                    raise ValueError(f"builtin 任务缺少 module 字段")

                from services.tasks import get_task
                task_info = get_task(module)
                if not task_info or task_info.get("handler") is None:
                    raise ValueError(f"任务模块 {module} 未找到或加载失败")

                handler = task_info["handler"]
                result = await handler(
                    account_id=task["account_id"],
                    group_id=task.get("group_id"),
                    task_id=task_id,
                )

            elif task_type == "strategy":
                # 代码型策略任务
                from services.strategy.engine import get_strategy_engine
                from services.common import technical_indicators

                strategy = await db.fetchone("SELECT * FROM strategies WHERE id = ?", (task["strategy_id"],))
                if not strategy:
                    raise ValueError(f"策略 {task['strategy_id']} 不存在")

                # 根据 code_scope 决定数据源
                code_scope = strategy.get("code_scope", "screening")

                if code_scope == "trading":
                    # 交易型策略：获取已买入的股票（watchlist status='bought'）
                    stocks = await db.fetchall(
                        "SELECT * FROM watchlist WHERE account_id = ? AND status = 'bought'",
                        (task["account_id"],)
                    )
                    logger.info(f"交易型策略 '{strategy['name']}'，获取到 {len(stocks)} 只已买入股票")
                elif task.get("full_market"):
                    # 全市场模式：从 kline.db 获取所有股票代码 + 名称
                    from services.common.database import get_sync_connection
                    knn = get_sync_connection("kline")
                    knn.row_factory = __import__('sqlite3').Row
                    # 从 stock_base_info 获取代码和名称（避免用代码作为名称）
                    stocks_rows = knn.execute(
                        "SELECT stock_code, stock_name FROM stock_base_info WHERE stock_code NOT LIKE '801%.SI'"
                    ).fetchall()
                    stocks = [{"stock_code": r["stock_code"], "stock_name": r["stock_name"] or r["stock_code"]} for r in stocks_rows]
                    logger.info(f"全市场策略 '{strategy['name']}'，获取到 {len(stocks)} 只股票")
                else:
                    # 选股型策略：获取候选组股票
                    stocks = await db.fetchall(
                        "SELECT * FROM watchlist WHERE account_id = ? AND group_id = ? AND status IN ('pending', 'watching')",
                        (task["account_id"], task["group_id"])
                    )

                # ── 预取当日实时行情
                _pre_fetched_realtime_quotes: Dict[str, Dict] = {}
                stock_codes = [s["stock_code"] for s in stocks]
                try:
                    from services.data.local_data_service import is_trading_hours

                    if is_trading_hours() and stock_codes:
                        # ① 先从 price_cache 取（全局共享，监控循环和 dispatcher 都在刷新）
                        from services.common.price_cache import get_price_cache
                        cache = get_price_cache()
                        cached_ohlcv = cache.get_all_for_codes(set(stock_codes))
                        for code in stock_codes:
                            if code in cached_ohlcv:
                                ohlcv = cached_ohlcv[code]
                                if ohlcv.get('close', 0) > 0:
                                    _pre_fetched_realtime_quotes[code] = {
                                        'open': ohlcv.get('open', ohlcv['close']),
                                        'high': ohlcv.get('high', ohlcv['close']),
                                        'low': ohlcv.get('low', ohlcv['close']),
                                        'close': ohlcv['close'],
                                        'volume': ohlcv.get('volume', 0),
                                        'amount': ohlcv.get('amount', 0),
                                    }

                        cached_count = len([c for c in stock_codes if c in cached_ohlcv])
                        missing_codes = [c for c in stock_codes if c not in _pre_fetched_realtime_quotes]
                        logger.info(f"预取实时行情: {cached_count}/{len(stock_codes)} 只来自 PriceCache"
                                    + (f"，{len(missing_codes)} 只缺失" if missing_codes else ""))
                    elif not is_trading_hours():
                        logger.info("非交易时段，跳过实时行情预取")
                except Exception as e:
                    import traceback
                    tb = traceback.format_exc()
                    logger.warning(f"实时行情预取失败: {e}")
                    logger.warning(tb)

                # ── 数据获取函数（策略沙盒中注入）──

                # ① 本地 K 线查询（同步，不经过 TGW）
                def _get_kline_local(stock_code: str, limit: int = 100, start_date: str = None):
                    """从本地 kline.db 获取历史 K 线"""
                    from services.data.local_data_service import get_local_data_service
                    lds = get_local_data_service()
                    return lds.get_kline_data(stock_code, start_date=start_date, limit=limit)

                # ② 批量本地 K 线查询（同步）
                def _get_batch_kline(stock_codes: list, limit: int = 100):
                    """从本地 kline.db 批量获取 K 线"""
                    from services.data.local_data_service import get_local_data_service
                    lds = get_local_data_service()
                    return lds.get_batch_kline(stock_codes, limit=limit)

                # ③ 日频因子查询（同步）
                def _get_factors(stock_code: str, date: str = None):
                    """从 stock_daily_factors 获取指定日期因子"""
                    from services.data.local_data_service import get_local_data_service
                    lds = get_local_data_service()
                    target_date = date or get_china_time().strftime("%Y-%m-%d")
                    return lds.get_daily_factors(stock_code, target_date)

                # ④ 批量日频因子查询（同步）
                def _get_factors_batch(stock_codes: list, date: str = None):
                    """批量获取多只股票指定日期因子"""
                    from services.data.local_data_service import get_local_data_service
                    lds = get_local_data_service()
                    target_date = date or get_china_time().strftime("%Y-%m-%d")
                    return lds.get_daily_factors_batch(stock_codes, target_date)

                # ⑤ 拼接 K 线：本地历史 + 当日实时（同步，TGW 部分走 gateway 排队）
                # 注：当日实时行情已在策略执行前预取，直接传入 _pre_fetched_realtime_quotes
                def _get_kline_spliced(stock_codes: list, lookback: int = 100):
                    """本地历史 + 当日实时行情拼接（仅当日走 TGW）"""
                    from services.data.local_data_service import get_local_data_service, is_trading_hours
                    lds = get_local_data_service()
                    realtime_quotes = {}
                    missing_codes = []
                    for code in stock_codes:
                        if code in _pre_fetched_realtime_quotes:
                            realtime_quotes[code] = _pre_fetched_realtime_quotes[code]
                        else:
                            missing_codes.append(code)
                    # 少数股票缺失（停牌等）→ 跳过，不影响整体筛选
                    if is_trading_hours() and missing_codes:
                        import logging
                        logging.getLogger("scheduler").warning(
                            f"策略执行: {len(missing_codes)}/{len(stock_codes)} 只股票无实时行情，跳过: {missing_codes[:5]}")
                    return lds.get_kline_spliced(stock_codes, lookback=lookback, realtime_quotes=realtime_quotes if realtime_quotes else None)

                # ⑤b 智能 K 线获取：根据交易时段自动选择数据源
                def _get_kline_smart(stock_codes: list, lookback: int = 100):
                    """
                    盘中 → 本地历史 + 预取的当日实时 OHLCV 拼接
                    盘后 → 纯本地数据（当日因子已计算完成）
                    返回格式: Dict[str, List[Dict]] 与策略代码兼容
                    """
                    from services.data.local_data_service import get_local_data_service, is_trading_hours

                    lds = get_local_data_service()

                    if not is_trading_hours():
                        # 盘后：直接返回本地数据
                        raw = lds.get_batch_kline(stock_codes, limit=lookback)
                    else:
                        # 盘中：跳过缺失实时数据的股票（停牌等）
                        realtime_quotes = {code: data for code, data in _pre_fetched_realtime_quotes.items() if code in stock_codes}
                        missing = [c for c in stock_codes if c not in realtime_quotes]
                        if missing:
                            import logging
                            logging.getLogger("scheduler").warning(
                                f"策略执行: {len(missing)}/{len(stock_codes)} 只股票无实时行情，跳过: {missing[:5]}")
                        raw = lds.get_kline_spliced(stock_codes, lookback=lookback, realtime_quotes=realtime_quotes)

                    # DataFrame → List[Dict] 转换，兼容策略代码
                    result = {}
                    for code, df in raw.items():
                        if hasattr(df, 'to_dict'):
                            result[code] = df.to_dict('records')
                        else:
                            result[code] = df
                    return result

                # ⑥ 实时行情（使用预取数据）
                def _get_realtime_quote(stock_code: str):
                    """获取预取的当日实时 OHLCV"""
                    return _pre_fetched_realtime_quotes.get(stock_code)

                # ⑦ 单股 K 线（异步，走 gateway → sdk_connection_manager 排队）
                async def _get_kline(stock_code: str, period: str = "day", start_date: str = None):
                    gateway = await get_gateway()
                    return await gateway.get_kline_data(stock_code, period=period, start_date=start_date)

                # ⑧ 单股行情（异步，走 gateway → sdk_connection_manager 排队）
                async def _get_market_data(stock_code: str):
                    gateway = await get_gateway()
                    return await gateway.get_market_data(stock_code)

                context = {
                    "stocks": [dict(s) for s in stocks],
                    "account_id": task["account_id"],
                    "today": get_china_time().strftime("%Y-%m-%d"),
                    "strategy": strategy,
                    "group_id": task["group_id"],
                    "code_scope": code_scope,
                    "indicators": {
                        "calculate_ma": technical_indicators.calculate_ma,
                        "calculate_rsi": technical_indicators.calculate_rsi,
                        "calculate_macd": technical_indicators.calculate_macd,
                        "calculate_kdj": technical_indicators.calculate_kdj,
                        "calculate_bollinger_bands": technical_indicators.calculate_bollinger_bands,
                        "calculate_adx": technical_indicators.calculate_adx,
                        "calculate_atr": technical_indicators.calculate_atr,
                        "calculate_ema": technical_indicators.calculate_ema,
                        "calculate_obv": technical_indicators.calculate_obv,
                        "calculate_historical_volatility": technical_indicators.calculate_historical_volatility,
                    },
                    "get_kline": _get_kline,                          # 异步: 走 gateway TGW
                    "get_market_data": _get_market_data,              # 异步: 走 gateway TGW
                    "get_kline_local": _get_kline_local,              # 同步: 本地 kline.db
                    "get_batch_kline": _get_batch_kline,              # 同步: 批量本地 K 线
                    "get_factors": _get_factors,                      # 同步: stock_daily_factors
                    "get_factors_batch": _get_factors_batch,          # 同步: 批量因子
                    "get_kline_spliced": _get_kline_spliced,          # 同步: 本地历史+预取当日拼接
                    "get_kline_smart": _get_kline_smart,              # 同步: 自动判断盘中/盘后
                    "get_realtime_quote": _get_realtime_quote,        # 同步: 预取当日 OHLCV
                }

                # 交易型策略：注入持仓数据（同步可用）
                if code_scope == "trading":
                    positions = await db.fetchall(
                        "SELECT * FROM watchlist WHERE account_id = ? AND status = 'bought'",
                        (task["account_id"],)
                    )
                    context["positions"] = [dict(p) for p in positions]

                engine = get_strategy_engine()
                signals = engine.execute_strategy(strategy, context)
                logger.info(f"策略 '{strategy['name']}' 返回 {len(signals)} 个信号")

                # 写入 watchlist（signal_action: trade=直接交易, watch=继续观察）
                signal_action = task.get("signal_action", "trade") or "trade"
                output_group_id = task.get("target_group_id") or task["group_id"]
                source_group_id = task.get("group_id")  # 股票池来源分组
                result = await engine.write_signals_to_watchlist(
                    signals, task["account_id"], task["strategy_id"], output_group_id,
                    strategy_name=strategy.get("name", ""),
                    signal_action=signal_action,
                    source_group_id=source_group_id,
                )
            else:
                raise ValueError(f"不支持的任务类型: {task_type}")

            # 更新任务状态为 success
            await db.execute(
                "UPDATE strategy_tasks SET last_status = 'success', last_output = ?, updated_at = ? WHERE id = ?",
                (json.dumps(result, ensure_ascii=False), get_china_time().isoformat(), task_id)
            )
            logger.info(f"策略任务完成: {result}")

            # 发送通知
            try:
                from services.notifications import get_notification_service
                notification = get_notification_service()
                await notification.emit(
                    event_type="task_completed",
                    account_id=task["account_id"],
                    payload={
                        "task_name": f"策略任务 #{task_id}",
                        "task_type": task_type,
                        "duration": "N/A",
                        "output": json.dumps(result, ensure_ascii=False)[:500],
                    },
                )
            except Exception as e:
                logger.warning(f"发送任务完成通知失败: {e}")

        except Exception as e:
            logger.error(f"策略任务执行失败: {e}", exc_info=True)
            await db.execute(
                "UPDATE strategy_tasks SET last_status = 'error', last_output = ?, updated_at = ? WHERE id = ?",
                (json.dumps({"error": str(e)}, ensure_ascii=False), get_china_time().isoformat(), task_id)
            )

            # 发送失败通知
            try:
                from services.notifications import get_notification_service
                notification = get_notification_service()
                await notification.emit(
                    event_type="task_failed",
                    account_id=task["account_id"],
                    payload={
                        "task_name": f"策略任务 #{task_id}",
                        "task_type": task_type,
                        "error": str(e),
                    },
                )
            except Exception as notify_err:
                logger.warning(f"发送任务失败通知失败: {notify_err}")

    def _reload_channel_router(self):
        """重新加载 ChannelRouter（数据源配置变更后调用）"""
        try:
            import asyncio
            from services.data.channel.router import get_channel_router, ChannelType, ChannelConfig
            from services.data.channel.config_manager import load_channel_order

            router = get_channel_router()
            # 重新加载通道优先级配置
            async def _reload():
                for ct in [ChannelType.TRADING, ChannelType.MARKET_DATA, ChannelType.DATA_DOWNLOAD]:
                    provider_order = await load_channel_order(ct.value)
                    if provider_order:
                        router.set_channel_config(ct, ChannelConfig(
                            channel_type=ct,
                            provider_order=provider_order,
                            timeout_seconds=15.0,
                        ))

            from services.common.async_helper import run_async_safe
            result = {}
            def _do_reload():
                loop = asyncio.new_event_loop()
                try:
                    loop.run_until_complete(_reload())
                    result['success'] = True
                except Exception as e:
                    result['success'] = False
                    result['error'] = str(e)
                finally:
                    loop.close()

            run_async_safe(_do_reload)
            if result.get('success'):
                logger.info("ChannelRouter 配置已重新加载")
            else:
                logger.warning(f"ChannelRouter 重新加载失败: {result.get('error')}")
        except Exception as e:
            logger.error(f"_reload_channel_router 失败: {e}")

    def run_manual_strategy_task(self, task_id: int) -> Dict:
        """手动触发策略任务（force=True 跳过 require_trading_day 检查）"""
        logger.info(f"手动触发策略任务 ID={task_id}")
        run_async_safe(self._execute_strategy_task, task_id, True)
        return {'success': True, 'message': f'策略任务 {task_id} 已启动'}


# 全局调度器实例
_scheduler_instance: Optional[SchedulerService] = None


def get_scheduler() -> SchedulerService:
    """获取调度器实例"""
    global _scheduler_instance
    if _scheduler_instance is None:
        _scheduler_instance = SchedulerService()
    return _scheduler_instance


def start_scheduler():
    """启动调度服务"""
    scheduler = get_scheduler()
    scheduler.start()
    return scheduler


def stop_scheduler():
    """停止调度服务"""
    global _scheduler_instance
    if _scheduler_instance:
        _scheduler_instance.stop()


if __name__ == "__main__":
    # 测试运行
    from dotenv import load_dotenv
    load_dotenv()

    print("启动调度服务测试...")
    scheduler = start_scheduler()

    print("手动触发一次检查...")
    scheduler.run_manual_kline_check()

    print("等待任务完成...")
    import time
    time.sleep(300)

    print("停止调度服务...")
    stop_scheduler()