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
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict
import sqlite3

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
            from apscheduler.schedulers.background import BackgroundScheduler
            from apscheduler.triggers.cron import CronTrigger

            self._scheduler = BackgroundScheduler()
            self._running = True

            # 每天凌晨1点执行K线数据检查任务
            self._scheduler.add_job(
                self._daily_kline_check_job,
                CronTrigger(hour=1, minute=0),
                id='daily_kline_check',
                name='每日K线数据检查',
                replace_existing=True
            )

            # 每月5日凌晨1点执行月频因子更新任务
            self._scheduler.add_job(
                self._monthly_factor_check_job,
                CronTrigger(day=5, hour=1, minute=0),
                id='monthly_factor_check',
                name='每月因子数据检查',
                replace_existing=True
            )

            self._scheduler.start()
            logger.info("调度服务已启动:")
            logger.info("  - 每天凌晨1点执行K线数据检查")
            logger.info("  - 每月5日凌晨1点执行月频因子更新")

        except ImportError:
            logger.warning("APScheduler 未安装，使用简单的定时检查方案")
            self._start_simple_scheduler()

    def stop(self):
        """停止调度服务"""
        if self._scheduler:
            self._scheduler.shutdown()
            self._scheduler = None
        self._running = False
        logger.info("调度服务已停止")

    def _start_simple_scheduler(self):
        """简单定时器方案（备用）"""
        import time

        def simple_loop():
            while self._running:
                # 计算到下一个凌晨1点的等待时间
                now = datetime.now()
                next_run = now.replace(hour=1, minute=0, second=0, microsecond=0)
                if now.hour >= 1:
                    next_run = next_run + timedelta(days=1)

                wait_seconds = (next_run - now).total_seconds()
                logger.info(f"下次执行时间: {next_run}, 等待 {wait_seconds/3600:.1f} 小时")

                time.sleep(wait_seconds)

                if self._running:
                    logger.info("开始执行每日数据检查")
                    self._daily_check_job()

        self._running = True
        thread = threading.Thread(target=simple_loop, daemon=True)
        thread.start()
        logger.info("简单调度器已启动")

    def _daily_kline_check_job(self):
        """每日K线数据检查任务"""
        logger.info("=" * 60)
        logger.info("开始每日K线数据检查任务")
        logger.info("=" * 60)

        self._task_status['last_check_time'] = datetime.now().isoformat()

        try:
            # Step 1: 检查K线数据是否最新
            kline_check = self._check_kline_data()
            expected_date = kline_check['expected_date']

            if kline_check['need_download']:
                logger.info(f"K线数据落后，最新数据: {kline_check['latest_date']}, 应有数据: {expected_date}")

                # Step 2: 启动K线增量下载
                download_result = self._run_kline_download()
                self._task_status['last_download_time'] = datetime.now().isoformat()
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
                self._task_status['last_factor_calc_time'] = datetime.now().isoformat()
                self._task_status['factor_status'] = factor_result

                if factor_result.get('success'):
                    logger.info(f"因子补充计算完成: {factor_result}")
                else:
                    logger.warning(f"因子补充计算失败: {factor_result}")
            else:
                logger.info(f"因子覆盖率正常: {factor_check['coverage_pct']:.1f}%")
                self._task_status['factor_status'] = {'status': 'up_to_date', 'coverage': factor_check['coverage_pct']}

            # Step 4: 检查并下载申万行业指数数据
            logger.info("检查申万行业指数数据...")
            industry_result = self._run_industry_indices_download()
            self._task_status['industry_indices_status'] = industry_result

            if industry_result.get('success'):
                logger.info(f"行业指数更新完成: {industry_result}")
            else:
                logger.warning(f"行业指数更新失败: {industry_result}")

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

        self._task_status['last_monthly_check_time'] = datetime.now().isoformat()

        try:
            # 检查月频因子是否需要更新
            monthly_check = self._check_monthly_factors()

            if monthly_check['need_update']:
                logger.info(f"月频因子需要更新，最新报告期: {monthly_check['latest_report_period']}")

                # 执行月频因子更新
                result = self._run_monthly_factor_update()
                self._task_status['last_monthly_update_time'] = datetime.now().isoformat()

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
        now = datetime.now()
        year = now.year
        month = now.month

        # 判断当前季度报告期
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

        # 检查PE填充率
        cursor.execute("""
            SELECT COUNT(*) as total,
                   COUNT(CASE WHEN pe_ttm > 0 THEN 1 END) as has_pe
            FROM stock_monthly_factors
            WHERE report_date = ?
        """, (expected_report,))
        row = cursor.fetchone()
        total = row[0]
        has_pe = row[1]

        conn.close()

        # 判断是否需要更新
        need_update = False
        if latest_report is None:
            need_update = True
            logger.info("月频因子表无数据，需要更新")
        elif latest_report < expected_report:
            need_update = True
            logger.info(f"最新报告期 {latest_report} < 应有报告期 {expected_report}")
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

        conn.close()

        # 判断是否需要下载
        need_download = False
        if latest_date is None:
            need_download = True
            logger.info("数据库无K线数据，需要下载")
        elif latest_date < expected_date:
            need_download = True
            logger.info(f"数据库最新日期 {latest_date} < 应有日期 {expected_date}")

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

            # 在当前线程执行（因为是后台任务）
            from services.data.local_data_service import LocalKlineDataService
            from dotenv import load_dotenv
            load_dotenv()

            service = LocalKlineDataService()

            # 执行增量下载
            result = service.download_all_stocks_kline(
                mode='incremental',
                show_progress=True
            )

            # 更新任务状态
            if result.get('success'):
                task_manager.complete_task(TaskType.DATA_DOWNLOAD, result)
            else:
                task_manager.fail_task(TaskType.DATA_DOWNLOAD, result.get('message', '下载失败'))

            return result

        except Exception as e:
            logger.error(f"K线下载失败: {e}", exc_info=True)
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

            # 执行因子计算
            from services.data.local_data_service import calculate_and_save_factors_for_dates

            # 计算日期范围
            calc_start = start_date or (datetime.now() - timedelta(days=120)).strftime('%Y-%m-%d')

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

            task_manager.complete_task(TaskType.DAILY_FACTOR_CALC, result)
            return result

        except Exception as e:
            logger.error(f"日频因子计算失败: {e}", exc_info=True)
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
            return {'success': False, 'message': str(e)}

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

    def get_status(self) -> Dict:
        """获取调度服务状态"""
        jobs = []
        if self._scheduler:
            for job in self._scheduler.get_jobs():
                jobs.append({
                    'id': job.id,
                    'name': job.name,
                    'next_run_time': str(job.next_run_time) if job.next_run_time else None,
                    'trigger': str(job.trigger)
                })

        return {
            'running': self._running,
            'scheduler_type': 'APScheduler' if self._scheduler else 'Simple',
            'jobs': jobs,
            'current_task': self._current_task,
            'task_status': self._task_status
        }

    def run_manual_kline_check(self) -> Dict:
        """手动触发K线数据检查"""
        logger.info("手动触发K线数据检查")
        thread = threading.Thread(target=self._daily_kline_check_job)
        thread.start()
        return {'success': True, 'message': 'K线数据检查任务已启动'}

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
    scheduler.run_manual_check()

    print("等待任务完成...")
    import time
    time.sleep(300)

    print("停止调度服务...")
    stop_scheduler()