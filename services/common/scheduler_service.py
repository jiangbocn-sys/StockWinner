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
import sqlite3

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from services.common.timezone import get_china_time, CHINA_TZ

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(Path(__file__).parent.parent.parent / 'logs' / 'scheduler.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('Scheduler')

DB_PATH = Path(__file__).parent.parent.parent / "data" / "kline.db"
POSITIONS_DB_PATH = Path(__file__).parent.parent.parent / "data" / "stockwinner.db"


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

                # Step 2: 启动K线增量下载
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

            # Step 3: 检查因子覆盖率并补充缺失因子（无论K线是否下载）
            logger.info("检查因子覆盖率...")
            factor_check = self._check_factor_coverage(expected_date)

            if factor_check['need_calc']:
                logger.info(f"因子覆盖率不足: {factor_check['coverage_pct']:.1f}%, 需补充 {factor_check['missing_count']} 只股票")

                # 启动因子补充计算（强制全量计算以填充中间缺失）
                factor_result = self._run_daily_factor_calc(
                    None,  # 让函数自动确定起始日期
                    expected_date,
                    force_full=True  # 覆盖率不足时强制全量计算
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

            # Step 4: 行业指数已随K线下载自动更新（download_incremental_kline_data_sync 默认包含）
            self._task_status['industry_indices_status'] = {'status': 'included_in_kline_download'}

            # Step 5: 日K线下载完成后，检查周K线是否需要补下载
            logger.info("日K线下载完成，检查周K线覆盖度...")
            need_weekly, weekly_msg = self._check_weekly_kline_coverage()
            if need_weekly:
                logger.info(f"周K线覆盖度不足: {weekly_msg}，开始补下载")
                weekly_result = self._run_weekly_kline_download()
                self._task_status['weekly_kline_status'] = weekly_result

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
                self._task_status['monthly_factor_status'] = {'status': 'up_to_date'}

            logger.info("=" * 60)
            logger.info("月频因子更新任务完成")
            logger.info("=" * 60)

        except Exception as e:
            logger.error(f"月频因子更新任务失败: {e}", exc_info=True)
            self._task_status['monthly_factor_status'] = {'status': 'error', 'message': str(e)}

    def _check_monthly_factors(self) -> Dict:
        """检查月频因子是否需要更新"""
        conn = sqlite3.connect(str(DB_PATH))
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

        conn.close()

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
        elif total > 0 and has_pe < total * 0.5:  # PE填充率低于50%
            need_update = True
            logger.info(f"报告期 {expected_report} PE填充率仅 {has_pe/total*100:.1f}%，需要补充")

        return {
            'latest_report_period': latest_report,
            'expected_report_period': expected_report,
            'pe_fill_rate': has_pe / total * 100 if total > 0 else 0,
            'need_update': need_update
        }

    def _check_kline_data(self) -> Dict:
        """检查K线数据是否最新"""
        conn = sqlite3.connect(str(DB_PATH))
        cursor = conn.cursor()

        # 获取数据库中最新的K线日期
        cursor.execute("SELECT MAX(trade_date) FROM kline_data")
        latest_date = cursor.fetchone()[0]

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
                cursor.execute("SELECT COUNT(DISTINCT stock_code) FROM kline_data WHERE trade_date = ?", (expected_date,))
                covered = cursor.fetchone()[0]
                cursor.execute("SELECT COUNT(DISTINCT stock_code) FROM kline_data")
                total = cursor.fetchone()[0]
                if total > 0 and covered < total * 0.95:
                    need_download = True
                    logger.info(f"日期 {expected_date} 覆盖度不足: {covered}/{total} ({covered/total*100:.1f}%)，需要补下载")

        conn.close()

        return {
            'latest_date': latest_date,
            'expected_date': expected_date,
            'need_download': need_download,
            'status_msg': status_msg
        }

    def _check_factor_coverage(self, target_date: str) -> Dict:
        """检查因子覆盖率（检查最近5个交易日）"""
        conn = sqlite3.connect(str(DB_PATH))
        cursor = conn.cursor()

        # 获取最近5个交易日
        cursor.execute('''
            SELECT DISTINCT trade_date FROM kline_data
            WHERE trade_date <= ? AND stock_code NOT LIKE '801%.SI'
            ORDER BY trade_date DESC LIMIT 5
        ''', (target_date,))
        recent_dates = [row[0] for row in cursor.fetchall()]

        if not recent_dates:
            conn.close()
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

        conn.close()

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
        """执行K线增量下载"""
        logger.info("开始K线增量下载...")

        try:
            from services.common.task_manager import get_task_manager, TaskType

            task_manager = get_task_manager()

            # 检查是否有正在运行的下载任务
            if task_manager.is_running(TaskType.DATA_DOWNLOAD):
                logger.warning("已有下载任务在运行")
                return {'success': False, 'message': '已有下载任务在运行'}

            # 启动下载任务
            task_manager.start_task(TaskType.DATA_DOWNLOAD)
            task_manager.update_progress(TaskType.DATA_DOWNLOAD, 5, "正在初始化...")

            # 在当前线程执行（因为是后台任务）
            from services.data.local_data_service import download_incremental_kline_data_sync
            from dotenv import load_dotenv
            load_dotenv()

            # 执行增量下载（行业指数随K线一起下载）
            success = download_incremental_kline_data_sync()

            # 更新任务状态
            result = {'success': success, 'message': '下载完成' if success else '部分下载失败'}
            task_manager.update_progress(TaskType.DATA_DOWNLOAD, 100, "下载完成")
            if result['success']:
                task_manager.complete_task(TaskType.DATA_DOWNLOAD, result)
            else:
                task_manager.fail_task(TaskType.DATA_DOWNLOAD, result.get('message', '下载失败'))

            return result

        except Exception as e:
            logger.error(f"K线下载失败: {e}", exc_info=True)
            # 重置任务状态，避免前端卡在旧状态
            try:
                task_manager.reset_task(TaskType.DATA_DOWNLOAD)
            except Exception:
                pass
            return {'success': False, 'message': str(e)}

    def _run_daily_factor_calc(self, start_date: Optional[str], end_date: str, force_full: bool = False) -> Dict:
        """执行日频因子计算"""
        logger.info(f"开始日频因子计算: {start_date or '全部'} 至 {end_date}, 强制全量={force_full}")

        try:
            from services.common.task_manager import get_task_manager, TaskType

            task_manager = get_task_manager()

            # 检查是否有正在运行的因子计算任务
            if task_manager.is_running(TaskType.DAILY_FACTOR_CALC):
                logger.warning("已有因子计算任务在运行")
                return {'success': False, 'message': '已有因子计算任务在运行'}

            # 启动因子计算任务
            task_manager.start_task(TaskType.DAILY_FACTOR_CALC)
            task_manager.update_progress(TaskType.DAILY_FACTOR_CALC, 5, "正在初始化...")

            # 执行因子计算
            from services.data.local_data_service import calculate_and_save_factors_for_dates

            # 计算日期范围
            calc_start = start_date or (get_china_time() - timedelta(days=120)).strftime('%Y-%m-%d')

            # 当覆盖率不足时，使用only_new_dates=False强制全量计算
            # 这样可以填充中间缺失的日期和没有因子记录的股票
            inserted = calculate_and_save_factors_for_dates(
                start_date=calc_start,
                end_date=end_date,
                only_new_dates=not force_full,
                show_progress=True
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
            try:
                task_manager.reset_task(TaskType.DAILY_FACTOR_CALC)
            except Exception:
                pass
            return {'success': False, 'message': str(e)}

    def _run_monthly_factor_update(self) -> Dict:
        """执行月频因子更新"""
        logger.info("开始月频因子更新...")

        try:
            from services.common.task_manager import get_task_manager, TaskType

            task_manager = get_task_manager()

            # 检查是否有正在运行的月频因子任务
            if task_manager.is_running(TaskType.MONTHLY_FACTOR_UPDATE):
                logger.warning("已有月频因子更新任务在运行")
                return {'success': False, 'message': '已有月频因子更新任务在运行'}

            # 启动月频因子更新任务
            task_manager.start_task(TaskType.MONTHLY_FACTOR_UPDATE)

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
            try:
                task_manager.reset_task(TaskType.MONTHLY_FACTOR_UPDATE)
            except Exception:
                pass
            return {'success': False, 'message': str(e)}

    def _run_full_kline_download(self) -> Dict:
        """执行K线全量下载"""
        logger.info("开始K线全量下载...")

        try:
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

            success = download_all_kline_data_sync()

            result = {'success': success, 'message': '下载完成' if success else '部分下载失败'}
            task_manager.update_progress(TaskType.DATA_DOWNLOAD, 100, "下载完成")
            if result['success']:
                task_manager.complete_task(TaskType.DATA_DOWNLOAD, result)
            else:
                task_manager.fail_task(TaskType.DATA_DOWNLOAD, result.get('message', '下载失败'))

            return result

        except Exception as e:
            logger.error(f"K线全量下载失败: {e}", exc_info=True)
            try:
                task_manager.reset_task(TaskType.DATA_DOWNLOAD)
            except Exception:
                pass
            return {'success': False, 'message': str(e)}

    def _run_weekly_kline_download(self) -> Dict:
        """执行周K线下载"""
        logger.info("开始周K线下载...")

        try:
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

            success = download_weekly_kline_sync(
                years=10,
                batch_size=50
            )

            result = {'success': success, 'message': '下载完成' if success else '部分下载失败'}
            task_manager.update_progress(TaskType.WEEKLY_KLINE_DOWNLOAD, 100, "下载完成")
            task_manager.complete_task(TaskType.WEEKLY_KLINE_DOWNLOAD, result)
            return result

        except Exception as e:
            logger.error(f"周K线下载失败: {e}", exc_info=True)
            try:
                task_manager.reset_task(TaskType.WEEKLY_KLINE_DOWNLOAD)
            except Exception:
                pass
            return {'success': False, 'message': str(e)}

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
                result = self._run_weekly_kline_download()
                self._task_status['last_weekly_kline_download'] = get_china_time().isoformat()
                self._task_status['weekly_kline_status'] = result

                if result.get('success'):
                    logger.info(f"周K线下载完成: {result}")
                else:
                    logger.warning(f"周K线下载失败: {result}")
            else:
                logger.info(f"周K线数据已覆盖: {msg}")
                self._task_status['weekly_kline_status'] = {'status': 'up_to_date'}

            logger.info("=" * 60)
            logger.info("周K线数据下载任务完成")
            logger.info("=" * 60)

        except Exception as e:
            logger.error(f"周K线下载任务异常: {e}", exc_info=True)
            self._task_status['weekly_kline_status'] = {'success': False, 'message': str(e)}

    def _check_weekly_kline_coverage(self) -> Tuple[bool, str]:
        """检查周K线覆盖度

        同时检查：
        1. 股票覆盖率是否 >= 95%
        2. 数据是否包含最近一个已完成周（截至上周五）
        防止周中手动下载导致数据不完整
        """
        conn = sqlite3.connect(str(DB_PATH))
        cursor = conn.cursor()

        # 获取最近一个有周K线数据的周末
        cursor.execute("SELECT MAX(week_end_date) FROM weekly_kline_data")
        latest_week = cursor.fetchone()[0]

        if not latest_week:
            conn.close()
            return True, "无周K线数据"

        # 计算最近一个已完成的周五
        # 如果今天是周五且在交易时间内，本周尚未结束，取上周五
        today = get_china_time().date()
        weekday = today.weekday()  # 0=周一, 4=周五, 5=周六, 6=周日

        if weekday == 4:
            # 今天是周五，检查是否在交易时间内
            from services.data.local_data_service import is_trading_hours
            if is_trading_hours():
                # 盘中：本周未完成，取上周五
                last_friday = today - timedelta(days=7)
            else:
                # 盘后：本周已完成，今天就是最近的周五
                last_friday = today
        elif weekday >= 5:
            # 周六/周日 → 上周五
            last_friday = today - timedelta(days=(weekday - 4))
        else:
            # 周一~周四 → 上周五
            last_friday = today - timedelta(days=(weekday + 3))

        last_friday_str = last_friday.strftime('%Y-%m-%d')

        # 检查是否已包含最近一个完整周的数据
        if latest_week < last_friday_str:
            conn.close()
            return True, f"数据截至 {latest_week}，需更新到 {last_friday_str}"

        # 统计有多少股票有周K线数据
        cursor.execute("SELECT COUNT(DISTINCT stock_code) FROM weekly_kline_data")
        weekly_stocks = cursor.fetchone()[0]

        # 统计总股票数
        cursor.execute("SELECT COUNT(DISTINCT stock_code) FROM kline_data")
        total_stocks = cursor.fetchone()[0]

        conn.close()

        coverage_pct = weekly_stocks / total_stocks * 100 if total_stocks > 0 else 0

        if coverage_pct < 95:
            return True, f"{weekly_stocks}/{total_stocks} ({coverage_pct:.1f}%)"

        return False, f"已覆盖 {weekly_stocks}/{total_stocks}，数据截至 {latest_week}"

    def _run_industry_indices_download(self) -> Dict:
        """执行申万行业指数下载"""
        logger.info("开始申万行业指数下载...")

        try:
            from services.data.local_data_service import download_industry_indices
            from dotenv import load_dotenv
            load_dotenv()

            result = download_industry_indices()
            return result

        except Exception as e:
            logger.error(f"申万行业指数下载失败: {e}", exc_info=True)
            return {'success': False, 'message': str(e)}

    def _post_market_analysis_job(self) -> Dict:
        """盘后分析任务：对每只持仓股调用 DSA 分析并发送飞书通知"""
        logger.info("=" * 60)
        logger.info("开始盘后分析任务")
        logger.info("=" * 60)

        import httpx
        import time
        from services.common.task_manager import get_task_manager, TaskType

        task_manager = get_task_manager()
        task_type = TaskType.POST_MARKET_ANALYSIS

        if task_manager.is_running(task_type):
            return {'success': False, 'message': '分析任务正在运行中'}

        task_manager.start_task(task_type)
        self._task_status['last_post_market_analysis'] = get_china_time().isoformat()

        try:
            conn = sqlite3.connect(str(POSITIONS_DB_PATH))
            cursor = conn.cursor()

            cursor.execute(
                "SELECT DISTINCT account_id, stock_code, stock_name FROM stock_positions WHERE quantity > 0"
            )
            positions = cursor.fetchall()
            conn.close()

            if not positions:
                logger.info("当前无持仓，跳过盘后分析")
                task_manager.update_progress(task_type, 100, "无持仓，跳过分析")
                task_manager.complete_task(task_type, {'success': True, 'message': '无持仓', 'analyzed': 0})
                return {'success': True, 'message': '无持仓，跳过分析', 'analyzed': 0}

            account_positions: Dict[str, list] = {}
            for acct_id, stock_code, stock_name in positions:
                account_positions.setdefault(acct_id, []).append({
                    'stock_code': stock_code,
                    'stock_name': stock_name or stock_code,
                })

            all_stocks = [s for stocks in account_positions.values() for s in stocks]
            total_count = len(all_stocks)

            dsa_base_url = "http://localhost:8000"
            total_analyzed = 0
            total_failed = 0
            max_wait = 300
            interval = 5

            for idx, account_id in enumerate(account_positions.keys()):
                stocks = account_positions[account_id]
                logger.info(f"分析账户 {account_id}: {len(stocks)} 只持仓股")

                for stock_idx, stock in enumerate(stocks):
                    stock_code = stock['stock_code']
                    stock_name = stock['stock_name']

                    # 计算当前是第几只股票（全局序号）
                    current = 0
                    for aid, s_list in account_positions.items():
                        if aid == account_id:
                            current += stock_idx + 1
                            break
                        current += len(s_list)

                    progress_pct = int((current / total_count) * 100)
                    task_manager.update_progress(
                        task_type, progress_pct,
                        f"正在分析: {stock_name}({stock_code}) [{current}/{total_count}]"
                    )

                    try:
                        # 检查当日是否已有分析报告
                        today = get_china_time().strftime('%Y-%m-%d')
                        report_data = None

                        with httpx.Client(timeout=30) as hist_client:
                            hist_resp = hist_client.get(
                                f"{dsa_base_url}/api/v1/history",
                                params={"limit": 100}
                            )
                            if hist_resp.status_code == 200:
                                hist_data = hist_resp.json()
                                for item in hist_data.get("items", []):
                                    if (item.get("stock_code") == stock_code and
                                            item.get("created_at", "").startswith(today)):
                                        # 已有今日报告，直接使用
                                        record_id = item["id"]
                                        report_resp = hist_client.get(
                                            f"{dsa_base_url}/api/v1/history/{record_id}"
                                        )
                                        if report_resp.status_code == 200:
                                            report_data = report_resp.json()
                                            logger.info(f"使用今日已有报告: {stock_code} {stock_name}")
                                        break

                        if report_data is None:
                            # 无今日报告，提交新的分析任务
                            with httpx.Client(timeout=30) as client:
                                resp = client.post(
                                    f"{dsa_base_url}/api/v1/analysis/analyze",
                                    json={
                                        "stock_code": stock_code,
                                        "report_type": "detailed",
                                        "async_mode": True,
                                    }
                                )
                                if resp.status_code not in (200, 202):
                                    logger.warning(f"DSA 提交失败 ({stock_code}): {resp.status_code}")
                                    total_failed += 1
                                    continue

                                task_data = resp.json()
                                task_id_dsa = task_data.get("task_id")
                                if not task_id_dsa:
                                    logger.warning(f"DSA 未返回 task_id ({stock_code})")
                                    total_failed += 1
                                    continue

                            # 轮询等待分析完成（最多 5 分钟）
                            waited = 0

                            with httpx.Client(timeout=30) as client:
                                while waited < max_wait:
                                    time.sleep(interval)
                                    waited += interval

                                    status_resp = client.get(
                                        f"{dsa_base_url}/api/v1/analysis/status/{task_id_dsa}"
                                    )
                                    if status_resp.status_code != 200:
                                        continue

                                    status_data = status_resp.json()
                                    status = status_data.get("status")

                                    if status == "completed":
                                        with httpx.Client(timeout=30) as hist_client:
                                            hist_resp = hist_client.get(
                                                f"{dsa_base_url}/api/v1/history",
                                                params={"query_id": task_id_dsa, "limit": 1}
                                            )
                                            if hist_resp.status_code == 200:
                                                hist_data = hist_resp.json()
                                                items = hist_data.get("items", [])
                                                if items:
                                                    record_id = items[0]["id"]
                                                    report_resp = hist_client.get(
                                                        f"{dsa_base_url}/api/v1/history/{record_id}"
                                                    )
                                                    if report_resp.status_code == 200:
                                                        report_data = report_resp.json()
                                        break
                                    elif status == "failed":
                                        logger.warning(
                                            f"DSA 分析失败 ({stock_code}): {status_data.get('error', 'unknown')}"
                                        )
                                        total_failed += 1
                                        break

                            if report_data is None and waited >= max_wait:
                                logger.warning(f"DSA 分析超时 ({stock_code}), 等待 {max_wait}s")
                                total_failed += 1
                                continue

                        if report_data is None:
                            continue

                        summary = report_data.get("summary", {})
                        strategy = report_data.get("strategy", {})

                        analysis_summary = summary.get("analysis_summary", "暂无分析")
                        operation_advice = summary.get("operation_advice", "暂无建议")
                        sentiment_label = summary.get("sentiment_label", "-")
                        ideal_buy = strategy.get("ideal_buy", "-")
                        stop_loss = strategy.get("stop_loss", "-")
                        take_profit = strategy.get("take_profit", "-")

                        # 发送通知（同步 httpx，避免在已有 event loop 的线程中创建新循环）

                        configs = []
                        try:
                            conn = sqlite3.connect(str(POSITIONS_DB_PATH))
                            conn.row_factory = sqlite3.Row
                            cursor = conn.cursor()
                            cursor.execute(
                                "SELECT * FROM notification_config WHERE account_id = ? AND enabled = 1",
                                (account_id,),
                            )
                            configs = [dict(r) for r in cursor.fetchall()]
                            conn.close()
                        except Exception as qe:
                            logger.warning(f"查询通知配置失败: {qe}")

                        if configs:
                            config = configs[0]
                            if config.get("notify_on_task", 1):
                                content = (
                                    f"**股票代码：** {stock_code}\n"
                                    f"**股票名称：** {stock_name}\n"
                                    f"**市场情绪：** {sentiment_label}\n"
                                    f"**分析摘要：** {analysis_summary}\n"
                                    f"**操作建议：** {operation_advice}\n"
                                    f"**理想买入：** {ideal_buy}\n"
                                    f"**止损价：** {stop_loss}\n"
                                    f"**止盈价：** {take_profit}"
                                )

                                feishu_payload = {
                                    "msg_type": "interactive",
                                    "card": {
                                        "config": {"wide_screen_mode": True},
                                        "header": {
                                            "title": {"tag": "plain_text", "content": "盘后分析"},
                                            "template": "purple",
                                        },
                                        "elements": [
                                            {"tag": "div", "text": {"tag": "lark_md", "content": content}},
                                            {"tag": "hr"},
                                            {"tag": "note", "elements": [
                                                {"tag": "plain_text", "content": f"账户: {account_id} | StockWinner"}
                                            ]},
                                        ],
                                    },
                                }
                                try:
                                    with httpx.Client(timeout=10) as feishu_client:
                                        feishu_resp = feishu_client.post(
                                            config["webhook_url"],
                                            json=feishu_payload,
                                            headers={"Content-Type": "application/json"},
                                        )
                                        logger.info(f"飞书通知发送结果: {feishu_resp.text[:200]}")
                                except Exception as fe:
                                    logger.warning(f"发送飞书通知失败: {fe}")

                        total_analyzed += 1
                        logger.info(f"分析完成: {stock_code} {stock_name}")

                    except Exception as e:
                        logger.error(f"分析异常 ({stock_code}): {e}")
                        total_failed += 1

            logger.info(f"盘后分析完成: 成功 {total_analyzed}, 失败 {total_failed}")
            result = {
                'success': True,
                'analyzed': total_analyzed,
                'failed': total_failed,
                'message': f'分析完成: {total_analyzed} 只成功, {total_failed} 只失败',
            }
            task_manager.update_progress(task_type, 100, result['message'])
            task_manager.complete_task(task_type, result)
            return result

        except Exception as e:
            logger.error(f"盘后分析任务失败: {e}", exc_info=True)
            task_manager.fail_task(task_type, str(e))
            return {'success': False, 'message': str(e)}

    def run_manual_post_market_analysis(self) -> Dict:
        """手动触发盘后分析"""
        logger.info("手动触发盘后分析")
        thread = threading.Thread(target=self._post_market_analysis_job)
        thread.start()
        return {'success': True, 'message': '盘后分析任务已启动'}

    def get_status(self) -> Dict:
        """获取调度服务状态"""
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
            conn = sqlite3.connect(str(db_path))
            conn.row_factory = sqlite3.Row
            rows = conn.execute("SELECT * FROM strategy_tasks WHERE enabled = 1").fetchall()
            conn.close()

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

    def reload_strategy_tasks(self):
        """重新加载策略任务：移除已有 job，重新注册"""
        try:
            from services.tasks import get_task

            # 移除所有 task_ 前缀的 job
            for job in list(self._scheduler.get_jobs()):
                if job.id.startswith('task_'):
                    self._scheduler.remove_job(job.id)

            db_path = Path(__file__).parent.parent.parent / "data" / "stockwinner.db"
            conn = sqlite3.connect(str(db_path))
            conn.row_factory = sqlite3.Row
            rows = conn.execute("SELECT * FROM strategy_tasks WHERE enabled = 1").fetchall()
            conn.close()

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
            return {"success": True, "count": count}

        except Exception as e:
            logger.error(f"重新加载任务失败: {e}", exc_info=True)
            return {"success": False, "error": str(e)}

    def _register_monitor_cron_jobs(self):
        """注册交易监控自动启停任务"""
        try:
            db_path = Path(__file__).parent.parent.parent / "data" / "stockwinner.db"
            conn = sqlite3.connect(str(db_path))
            conn.row_factory = sqlite3.Row

            # 获取所有激活账户
            accounts = conn.execute(
                "SELECT account_id FROM accounts WHERE is_active = 1"
            ).fetchall()
            conn.close()

            for acct in accounts:
                acct_id = acct["account_id"]

                # 开盘前启动监控（09:20）
                start_job_id = f'monitor_start_{acct_id}'
                self._scheduler.add_job(
                    self._auto_start_monitor_job,
                    CronTrigger.from_crontab("20 9 * * 1-5", timezone=CHINA_TZ),
                    id=start_job_id,
                    name=f"自动启动监控: {acct_id}",
                    args=[acct_id],
                    replace_existing=True,
                )
                logger.info(f"  注册监控任务: {start_job_id} (cron=20 9 * * 1-5)")

                # 收盘后停止监控（15:05）
                stop_job_id = f'monitor_stop_{acct_id}'
                self._scheduler.add_job(
                    self._auto_stop_monitor_job,
                    CronTrigger.from_crontab("5 15 * * 1-5", timezone=CHINA_TZ),
                    id=stop_job_id,
                    name=f"自动停止监控: {acct_id}",
                    args=[acct_id],
                    replace_existing=True,
                )
                logger.info(f"  注册停监控任务: {stop_job_id} (cron=5 15 * * 1-5)")

            # T+1 解冻持仓 + 重置 watchlist pending 状态（每日收盘后 15:05）
            self._scheduler.add_job(
                self._auto_unfreeze_positions_job,
                CronTrigger.from_crontab("5 15 * * 1-5", timezone=CHINA_TZ),
                id="t1_unfreeze",
                name="T+1 持仓解冻",
                replace_existing=True,
            )
            logger.info(f"  注册 T+1 解冻任务 (cron=5 15 * * 1-5)")

        except Exception as e:
            logger.error(f"注册监控任务失败: {e}", exc_info=True)

    def is_today_trading_day(self) -> bool:
        """判断今天是否为交易日（使用 SDK 交易日历）"""
        try:
            from services.common.sdk_manager import get_sdk_manager
            sdk_mgr = get_sdk_manager()
            calendar = sdk_mgr.get_calendar()  # int 列表，如 [19901219, 20260507, ...]
            today = int(get_china_time().strftime('%Y%m%d'))
            return today in calendar
        except Exception as e:
            logger.warning(f"获取交易日历失败，降级为工作日判断: {e}")
            return get_china_time().weekday() < 5

    def auto_start_monitoring_if_trading(self):
        """服务启动时检查：如果当前在交易日交易时段，调度延时任务自动启动监控"""
        now = get_china_time()
        hour, minute = now.hour, now.minute
        # 交易时段 09:15 ~ 15:10（覆盖盘前到收盘后）
        in_session = (hour == 9 and minute >= 15) or (10 <= hour <= 14) or (hour == 15 and minute <= 10)

        if not in_session:
            logger.info(f"不在交易时段({hour}:{minute:02d})，跳过自动启动监控")
            return

        if not self.is_today_trading_day():
            logger.info("今天不是交易日，跳过自动启动监控")
            return

        # 调度为 10 秒后执行的一次性任务，避免与主事件循环冲突
        logger.info(f"检测到交易时段，10 秒后自动启动监控...")
        self._scheduler.add_job(
            self._do_auto_start_on_startup,
            'interval',
            seconds=10,
            id='startup_monitor',
            max_instances=1,
            replace_existing=True,
        )

    def _do_auto_start_on_startup(self):
        """延时执行的启动监控任务"""
        try:
            # 移除一次性任务
            try:
                self._scheduler.remove_job('startup_monitor')
            except Exception:
                pass

            conn = sqlite3.connect(str(POSITIONS_DB_PATH))
            conn.row_factory = sqlite3.Row
            accounts = conn.execute("SELECT account_id FROM accounts WHERE is_active = 1").fetchall()
            conn.close()

            for acct in accounts:
                acct_id = acct["account_id"]
                thread = threading.Thread(
                    target=lambda aid=acct_id: asyncio.run(self._do_start_monitor(aid))
                )
                thread.start()
        except Exception as e:
            logger.error(f"自动启动监控失败: {e}")

    def _auto_start_monitor_job(self, account_id: str):
        """自动启动交易监控（先判断是否为交易日）"""
        if not self.is_today_trading_day():
            logger.info(f"今天不是交易日，跳过启动监控: {account_id}")
            return
        """自动启动交易监控"""
        logger.info(f"自动启动交易监控: {account_id}")
        thread = threading.Thread(
            target=lambda: asyncio.run(self._do_start_monitor(account_id))
        )
        thread.start()

    async def _do_start_monitor(self, account_id: str):
        """执行启动监控"""
        from services.monitoring.service import get_trading_monitor
        monitor = get_trading_monitor()
        result = await monitor.start_monitoring(account_id, interval=30)
        logger.info(f"交易监控启动结果: {result}")

    def _auto_stop_monitor_job(self, account_id: str):
        """自动停止交易监控"""
        logger.info(f"自动停止交易监控: {account_id}")
        thread = threading.Thread(
            target=lambda: asyncio.run(self._do_stop_monitor(account_id))
        )
        thread.start()

    async def _do_stop_monitor(self, account_id: str):
        """执行停止监控"""
        from services.monitoring.service import get_trading_monitor
        monitor = get_trading_monitor()
        result = await monitor.stop_monitoring()
        logger.info(f"交易监控停止结果: {result}")

    def _auto_unfreeze_positions_job(self):
        """T+1 持仓解冻"""
        logger.info("开始 T+1 持仓解冻")
        thread = threading.Thread(
            target=lambda: asyncio.run(self._do_unfreeze())
        )
        thread.start()

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

                # 将该账户 watchlist 中所有 pending 状态重置为 watching
                # 防止昨日因信号进入 pending 的股票被重复买入
                result = await db.execute(
                    "UPDATE watchlist SET status = 'watching' WHERE account_id = ? AND status = 'pending'",
                    (row["account_id"],)
                )
                reset_count = getattr(result, 'rowcount', 0) if hasattr(result, 'rowcount') else 0
                if reset_count > 0:
                    logger.info(f"重置 pending→watching: {row['account_id']}: {reset_count} 只股票")
            except Exception as e:
                logger.warning(f"T+1 解冻失败 ({row['account_id']}): {e}")

    def _execute_strategy_task_job(self, task_id: int):
        """执行策略任务（在线程中运行）"""
        logger.info(f"开始执行策略任务 ID={task_id}")

        # 在新事件循环中运行异步代码
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self._execute_strategy_task(task_id))
        finally:
            loop.close()

    async def _execute_strategy_task(self, task_id: int):
        """异步执行任务（支持 builtin 和 strategy 两种类型）"""
        from services.common.database import get_db_manager
        import json

        db = get_db_manager()

        # 获取任务信息
        task = await db.fetchone("SELECT * FROM strategy_tasks WHERE id = ?", (task_id,))
        if not task:
            logger.error(f"任务 {task_id} 不存在")
            return

        task_type = task.get("task_type", "strategy")

        try:
            # 更新任务状态为 running
            await db.execute(
                "UPDATE strategy_tasks SET last_run_at = ?, last_status = 'running' WHERE id = ?",
                (get_china_time().isoformat(), task_id)
            )

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
                from services.trading.gateway import get_gateway

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
                else:
                    # 选股型策略：获取候选组股票
                    stocks = await db.fetchall(
                        "SELECT * FROM watchlist WHERE account_id = ? AND group_id = ? AND status IN ('pending', 'watching')",
                        (task["account_id"], task["group_id"])
                    )

                # 策略执行前：将该组 pending 状态重置为 watching（防重复买入）
                await db.execute(
                    "UPDATE watchlist SET status = 'watching' WHERE account_id = ? AND group_id = ? AND status = 'pending'",
                    (task["account_id"], task["group_id"])
                )

                # ── 预取当日实时行情（聚合当日 tick → OHLCV K 线）──
                _pre_fetched_realtime_quotes: Dict[str, Dict] = {}
                stock_codes = [s["stock_code"] for s in stocks]
                try:
                    from services.common.sdk_manager import get_sdk_manager
                    from services.data.local_data_service import is_trading_hours

                    if is_trading_hours() and stock_codes:
                        sdk_mgr = get_sdk_manager()
                        if sdk_mgr._ensure_login():
                            md = sdk_mgr.get_market_data()
                            today_int = int(get_china_time().strftime('%Y%m%d'))
                            result = md.query_snapshot(
                                code_list=stock_codes,
                                begin_date=today_int,
                                end_date=today_int
                            )
                            if result and isinstance(result, dict):
                                for date_key in result:
                                    inner = result[date_key]
                                    for code, tick_df in inner.items():
                                        if tick_df is None or not hasattr(tick_df, 'empty') or tick_df.empty:
                                            continue
                                        # 过滤有成交的 tick
                                        trade_ticks = tick_df[tick_df['volume'] > 0]
                                        if trade_ticks.empty:
                                            continue
                                        # 聚合为当日 OHLCV
                                        _pre_fetched_realtime_quotes[code] = {
                                            'open': float(trade_ticks.iloc[0]['open']),
                                            'high': float(trade_ticks['high'].max()),
                                            'low': float(trade_ticks['low'].min()),
                                            'close': float(trade_ticks.iloc[-1]['last']),
                                            'volume': float(trade_ticks.iloc[-1]['volume']),
                                            'amount': float(trade_ticks.iloc[-1]['amount']),
                                        }
                                logger.info(f"预取实时行情成功: {len(_pre_fetched_realtime_quotes)}/{len(stock_codes)} 只股票")
                        else:
                            logger.warning("SDK 未登录，跳过实时行情预取")
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
                    from services.data.local_data_service import get_local_data_service
                    lds = get_local_data_service()
                    # 使用预取的实时行情
                    realtime_quotes = {}
                    for code in stock_codes:
                        if code in _pre_fetched_realtime_quotes:
                            realtime_quotes[code] = _pre_fetched_realtime_quotes[code]
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
                        # 盘中：使用预取的实时行情
                        realtime_quotes = {code: data for code, data in _pre_fetched_realtime_quotes.items() if code in stock_codes}
                        raw = lds.get_kline_spliced(stock_codes, lookback=lookback, realtime_quotes=realtime_quotes if realtime_quotes else None)

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

                # 写入 watchlist
                result = await engine.write_signals_to_watchlist(
                    signals, task["account_id"], task["strategy_id"], task["group_id"],
                    strategy_name=strategy.get("name", ""),
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

    def run_manual_strategy_task(self, task_id: int) -> Dict:
        """手动触发策略任务"""
        logger.info(f"手动触发策略任务 ID={task_id}")
        thread = threading.Thread(
            target=lambda: asyncio.run(self._execute_strategy_task(task_id))
        )
        thread.start()
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