"""
数据库连接统一改造 - 回归测试

验证所有 sqlite3.connect() 调用已统一到 database.py，
PRAGMA 配置一致，各模块功能正常。

用法:
    python3 tests/test_all.py
"""

import sys
import os
import unittest
import sqlite3
import threading
from pathlib import Path

# 项目根目录加入 sys.path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


class TestCodeCompliance(unittest.TestCase):
    """代码规范检查：确保没有遗漏的直接 connect 调用"""

    def test_no_direct_connect_in_services(self):
        """services/ 下除 database.py 和 migrate.py 外不应有 sqlite3.connect()"""
        services_dir = PROJECT_ROOT / "services"
        violations = []
        skip_files = {"database.py", "migrate.py"}

        for py_file in services_dir.rglob("*.py"):
            if py_file.name in skip_files:
                continue
            if "__pycache__" in str(py_file):
                continue
            content = py_file.read_text()
            for i, line in enumerate(content.splitlines(), 1):
                stripped = line.strip()
                if stripped.startswith("#"):
                    continue
                if "sqlite3.connect" in stripped:
                    violations.append(f"{py_file.relative_to(PROJECT_ROOT)}:{i}: {stripped}")

        self.assertEqual(
            violations,
            [],
            f"发现 {len(violations)} 处违规 sqlite3.connect() 调用:\n" + "\n".join(violations)
        )

    def test_no_duplicate_db_path_definitions(self):
        """services/ 下不应有重复的 DB_PATH/KLINE_DB_PATH 定义（除 database.py 和 migrate.py）"""
        services_dir = PROJECT_ROOT / "services"
        violations = []
        skip_files = {"database.py", "migrate.py"}
        pattern_candidates = [
            "DB_PATH = Path",
            "KLINE_DB_PATH = Path",
            "KLINE_DB = Path",
            "DB_PATH = os.path",
            "KLINE_DB_PATH = os.path",
        ]

        for py_file in services_dir.rglob("*.py"):
            if py_file.name in skip_files:
                continue
            if "__pycache__" in str(py_file):
                continue
            content = py_file.read_text()
            for i, line in enumerate(content.splitlines(), 1):
                stripped = line.strip()
                for pattern in pattern_candidates:
                    if pattern in stripped and not stripped.startswith("#"):
                        violations.append(f"{py_file.relative_to(PROJECT_ROOT)}:{i}: {stripped}")
                        break

        self.assertEqual(
            violations,
            [],
            f"发现 {len(violations)} 处重复 DB_PATH 定义:\n" + "\n".join(violations)
        )


class TestDatabaseConnection(unittest.TestCase):
    """数据库连接配置验证

    注意：每个测试方法内部直接 import，不用 setUpClass 存函数引用。
    原因：unittest 中 self.xxx = func 后再 self.xxx() 会被当作 bound method，
    自动注入 self 作为第一个参数，导致被存的方法参数错位。
    """

    def test_db_paths_exist(self):
        """数据库文件应存在"""
        from services.common.database import DB_PATH, KLINE_DB_PATH
        self.assertTrue(DB_PATH.exists(), f"stockwinner.db 不存在: {DB_PATH}")
        self.assertTrue(KLINE_DB_PATH.exists(), f"kline.db 不存在: {KLINE_DB_PATH}")

    def test_database_paths_dict(self):
        """DATABASE_PATHS 应包含两个数据库路径"""
        from services.common.database import DB_PATH, KLINE_DB_PATH, DATABASE_PATHS
        self.assertIn("stockwinner", DATABASE_PATHS)
        self.assertIn("kline", DATABASE_PATHS)
        self.assertEqual(DATABASE_PATHS["stockwinner"], DB_PATH)
        self.assertEqual(DATABASE_PATHS["kline"], KLINE_DB_PATH)

    def test_sync_connection_pragma(self):
        """get_sync_connection 应配置正确的 PRAGMA"""
        from services.common.database import get_sync_connection
        conn = get_sync_connection("kline")
        self.assertIsNotNone(conn)

        cursor = conn.execute("PRAGMA journal_mode")
        mode = cursor.fetchone()[0]
        self.assertEqual(mode, "wal", f"WAL mode 未启用，当前: {mode}")

        cursor = conn.execute("PRAGMA busy_timeout")
        timeout = cursor.fetchone()[0]
        self.assertEqual(timeout, 10000, f"busy_timeout 应为 10000，当前: {timeout}")

        cursor = conn.execute("PRAGMA foreign_keys")
        fk = cursor.fetchone()[0]
        self.assertEqual(fk, 1, "foreign_keys 未启用")

    def test_sync_connection_thread_cached(self):
        """同一线程多次调用应返回同一连接"""
        from services.common.database import get_sync_connection
        conn1 = get_sync_connection("kline")
        conn2 = get_sync_connection("kline")
        self.assertIs(conn1, conn2, "同一线程应返回缓存的同一连接实例")

    def test_sync_connection_thread_isolation(self):
        """不同线程应返回不同连接"""
        from services.common.database import get_sync_connection
        results = {}
        barrier = threading.Barrier(2)

        def get_conn(name):
            barrier.wait()
            conn = get_sync_connection("kline")
            results[name] = conn

        t1 = threading.Thread(target=get_conn, args=("t1",))
        t2 = threading.Thread(target=get_conn, args=("t2",))
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        self.assertIsNot(results["t1"], results["t2"], "不同线程应返回不同连接")

    def test_sync_connection_stockwinner(self):
        """stockwinner 数据库连接也应正确配置"""
        from services.common.database import get_sync_connection
        conn = get_sync_connection("stockwinner")
        self.assertIsNotNone(conn)
        cursor = conn.execute("PRAGMA journal_mode")
        self.assertEqual(cursor.fetchone()[0], "wal")

    def test_sync_connection_invalid_name(self):
        """未知数据库名称应抛出 ValueError"""
        from services.common.database import get_sync_connection
        with self.assertRaises(ValueError):
            get_sync_connection("unknown_db")

    def test_db_context_auto_commit(self):
        """get_db_context 应自动 commit"""
        from services.common.database import get_db_context
        with get_db_context("stockwinner") as conn:
            conn.execute("SELECT 1")

    def test_db_context_isolated_closes(self):
        """get_db_context_isolated 使用后应关闭连接"""
        from services.common.database import get_db_context_isolated
        conn_ref = None
        with get_db_context_isolated("stockwinner") as conn:
            conn_ref = conn
            conn.execute("SELECT 1")
        # 关闭后执行 SQL 应抛异常
        with self.assertRaises(sqlite3.ProgrammingError):
            conn_ref.execute("SELECT 1")

    def test_configure_connection_consistency(self):
        """_configure_connection 应统一配置三个 PRAGMA"""
        from services.common.database import _configure_connection
        import tempfile
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            tmp_path = Path(f.name)
        try:
            conn = sqlite3.connect(str(tmp_path))
            _configure_connection(conn)

            cursor = conn.execute("PRAGMA journal_mode")
            self.assertEqual(cursor.fetchone()[0], "wal")
            cursor = conn.execute("PRAGMA busy_timeout")
            self.assertEqual(cursor.fetchone()[0], 10000)
            cursor = conn.execute("PRAGMA foreign_keys")
            self.assertEqual(cursor.fetchone()[0], 1)

            conn.close()
        finally:
            tmp_path.unlink(missing_ok=True)


class TestModuleImports(unittest.TestCase):
    """验证重构后的模块导入正常，无 broken imports"""

    def test_import_factor_registry(self):
        from services.screening.factor_registry import get_factor_registry
        registry = get_factor_registry()
        self.assertIsNotNone(registry)
        self.assertTrue(len(registry.get_all_factors()) > 0)

    def test_import_screening_service(self):
        from services.screening.service import ScreeningService
        self.assertIsNotNone(ScreeningService)

    def test_import_dashboard(self):
        from services.ui.dashboard import get_dashboard
        self.assertIsNotNone(get_dashboard)

    def test_import_monitoring(self):
        from services.ui.monitoring import get_monitoring_status
        self.assertIsNotNone(get_monitoring_status)

    def test_import_scheduler(self):
        from services.ui import scheduler
        self.assertIsNotNone(scheduler.router)

    def test_import_market_data(self):
        from services.ui.market_data import router
        self.assertIsNotNone(router)

    def test_import_factors_ui(self):
        from services.ui.factors import router
        self.assertIsNotNone(router)

    def test_import_screening_ui(self):
        from services.ui.screening import router
        self.assertIsNotNone(router)

    def test_import_gateway(self):
        from services.trading.gateway import create_gateway, get_gateway
        self.assertIsNotNone(create_gateway)
        self.assertIsNotNone(get_gateway)

    def test_import_strategy_engine(self):
        from services.strategy.engine import get_strategy_engine
        self.assertIsNotNone(get_strategy_engine)

    def test_import_daily_factor_calculator(self):
        from services.factors.daily_factor_calculator import DailyFactorCalculator
        self.assertIsNotNone(DailyFactorCalculator)

    def test_import_monthly_factor_calculator(self):
        from services.factors.monthly_factor_calculator import MonthlyFactorCalculator
        self.assertIsNotNone(MonthlyFactorCalculator)

    def test_import_factor_service(self):
        from services.data.factor_service import calculate_and_save_factors_for_dates
        self.assertIsNotNone(calculate_and_save_factors_for_dates)

    def test_import_local_data_service(self):
        from services.data.local_data_service import get_local_data_service
        self.assertIsNotNone(get_local_data_service)

    def test_import_stock_base_info_service(self):
        from services.data.stock_base_info_service import StockBaseInfoService
        self.assertIsNotNone(StockBaseInfoService)

    def test_import_backtest_return_accumulation(self):
        from services.backtest.modes.return_accumulation import ReturnAccumulationEngine
        self.assertIsNotNone(ReturnAccumulationEngine)

    def test_import_backtest_simulated_trading(self):
        from services.backtest.modes.simulated_trading import SimulatedTradingEngine
        self.assertIsNotNone(SimulatedTradingEngine)

    def test_import_agent_handlers(self):
        from services.agent.handlers import _make_idempotency_key
        self.assertIsNotNone(_make_idempotency_key)

    def test_import_data_download(self):
        from services.data.data_download import download_all_kline_data_sync
        self.assertIsNotNone(download_all_kline_data_sync)


class TestFunctionalQueries(unittest.TestCase):
    """核心功能查询测试：验证重构后查询仍然正常"""

    def test_factor_registry_load(self):
        from services.screening.factor_registry import get_factor_registry
        registry = get_factor_registry()
        factors = registry.get_all_factors()
        self.assertGreater(len(factors), 0, "因子注册表应有因子")

    def test_factor_registry_filterable(self):
        from services.screening.factor_registry import get_factor_registry
        registry = get_factor_registry()
        filterable = registry.get_filterable_factors()
        self.assertIsInstance(filterable, list)

    def test_kline_db_query_works(self):
        from services.common.database import get_sync_connection
        conn = get_sync_connection("kline")
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' LIMIT 1")
        row = cursor.fetchone()
        self.assertIsNotNone(row, "kline.db 应有表")

    def test_stockwinner_db_query_works(self):
        from services.common.database import get_sync_connection
        conn = get_sync_connection("stockwinner")
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' LIMIT 1")
        row = cursor.fetchone()
        self.assertIsNotNone(row, "stockwinner.db 应有表")

    def test_stock_positions_table_exists(self):
        from services.common.database import get_sync_connection
        conn = get_sync_connection("stockwinner")
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='stock_positions'"
        )
        self.assertIsNotNone(cursor.fetchone(), "stock_positions 表应存在")

    def test_kline_tables_exist(self):
        from services.common.database import get_sync_connection
        conn = get_sync_connection("kline")
        required_tables = {"kline_data", "stock_daily_factors", "stock_base_info"}
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        existing = {row[0] for row in cursor.fetchall()}
        for table in required_tables:
            self.assertIn(table, existing, f"kline.db 缺少表: {table}")


if __name__ == "__main__":
    print("=" * 60)
    print("数据库连接统一改造 - 回归测试")
    print("=" * 60)
    unittest.main(verbosity=2)
