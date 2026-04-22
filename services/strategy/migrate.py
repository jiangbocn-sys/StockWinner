"""
数据库迁移脚本 - 三维度策略系统

添加选股策略、持仓策略、交易策略相关的数据库表
"""

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent.parent.parent / "stockwinner.db"


def migrate():
    """执行数据库迁移"""
    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()

    print(f"[Migrate] 开始执行策略模块数据库迁移...")
    print(f"[Migrate] 数据库路径：{DB_PATH}")

    # ============== 选股策略相关表 ==============

    # 选股因子配置表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS selection_factors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            factor_id TEXT UNIQUE NOT NULL,
            factor_name TEXT NOT NULL,
            factor_type TEXT NOT NULL,
            weight REAL DEFAULT 0.1,
            threshold REAL DEFAULT 0.5,
            direction TEXT DEFAULT 'higher_better',
            formula TEXT,
            description TEXT,
            is_enabled INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    print("[Migrate] 创建表：selection_factors")

    # 选股结果表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS selection_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_id TEXT NOT NULL,
            strategy_id INTEGER,
            stock_code TEXT NOT NULL,
            stock_name TEXT,
            total_score REAL NOT NULL,
            factor_scores TEXT,
            profile_tags TEXT,
            current_price REAL,
            change_pct REAL,
            volume_ratio REAL,
            match_reasons TEXT,
            selected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(account_id, stock_code, selected_at)
        )
    ''')
    print("[Migrate] 创建表：selection_results")

    # ============== 持仓策略相关表 ==============

    # 持仓策略配置表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS position_configs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_id TEXT NOT NULL,
            strategy_id INTEGER,
            strategy_name TEXT,

            -- 仓位控制
            base_position_pct REAL DEFAULT 0.6,
            max_position_pct REAL DEFAULT 0.8,
            min_position_pct REAL DEFAULT 0.2,

            -- 个股限制
            max_single_stock_pct REAL DEFAULT 0.2,
            min_single_stock_pct REAL DEFAULT 0.05,
            max_holding_count INTEGER DEFAULT 10,

            -- 市场环境调整
            adjust_by_market INTEGER DEFAULT 1,
            bull_position_pct REAL DEFAULT 0.9,
            bear_position_pct REAL DEFAULT 0.1,

            -- 止损止盈
            stop_loss_pct REAL DEFAULT 0.05,
            take_profit_pct REAL DEFAULT 0.15,
            trailing_stop_pct REAL DEFAULT 0.08,

            -- 持仓周期
            default_period TEXT DEFAULT 'short',

            is_active INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    print("[Migrate] 创建表：position_configs")

    # 市场分析记录表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS market_analysis_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_id TEXT NOT NULL,

            -- 指数数据
            index_code TEXT,
            index_name TEXT,
            index_price REAL,
            index_change_pct REAL,

            -- 分析结果
            market_condition TEXT NOT NULL,
            confidence REAL,
            trend_score REAL,
            sentiment_score REAL,
            risk_level TEXT,

            -- 仓位建议
            recommended_position_level TEXT,
            recommended_position_pct REAL,
            max_single_stock_pct REAL,
            max_holding_count INTEGER,
            recommended_period TEXT,

            analysis_details TEXT,
            analyzed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    print("[Migrate] 创建表：market_analysis_records")

    # ============== 交易策略相关表 ==============

    # 交易策略配置表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS trading_configs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_id TEXT NOT NULL,
            strategy_id INTEGER,
            strategy_name TEXT,

            -- 买入配置
            buy_on_breakout INTEGER DEFAULT 1,
            buy_on_golden_cross INTEGER DEFAULT 1,
            buy_on_pattern INTEGER DEFAULT 1,

            -- 仓位限制
            max_position_per_stock REAL DEFAULT 0.2,
            max_total_positions INTEGER DEFAULT 10,

            -- 止损配置
            fixed_stop_loss_pct REAL DEFAULT 0.05,
            trailing_stop_pct REAL DEFAULT 0.08,
            trailing_stop_enabled INTEGER DEFAULT 1,
            ma20_stop_loss INTEGER DEFAULT 1,
            time_stop_days INTEGER DEFAULT 10,
            max_loss_per_stock_pct REAL DEFAULT 0.10,

            -- 止盈配置
            fixed_take_profit_pct REAL DEFAULT 0.15,
            staged_take_profit INTEGER DEFAULT 1,
            trend_tracking INTEGER DEFAULT 1,
            rsi_overbought REAL DEFAULT 80,

            is_active INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    print("[Migrate] 创建表：trading_configs")

    # 交易信号记录表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS trading_signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_id TEXT NOT NULL,
            stock_code TEXT NOT NULL,
            stock_name TEXT,

            -- 信号信息
            signal_type TEXT NOT NULL,
            signal_strength TEXT,
            current_price REAL,
            suggested_price REAL,
            suggested_quantity INTEGER,

            -- 信号依据
            pattern_type TEXT,
            indicators TEXT,
            reasons TEXT,

            -- 风控参数
            stop_loss_price REAL,
            take_profit_price REAL,
            confidence REAL,

            -- 执行状态
            executed INTEGER DEFAULT 0,
            exec_price REAL,
            exec_quantity INTEGER,
            exec_time TIMESTAMP,

            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    print("[Migrate] 创建表：trading_signals")

    # K 线形态检测记录表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS pattern_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            stock_code TEXT NOT NULL,
            pattern_type TEXT NOT NULL,
            pattern_name TEXT,

            detect_price REAL,
            detect_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            confirmed INTEGER DEFAULT 0,

            -- 后续走势追踪
            follow_up_price REAL,
            follow_up_change_pct REAL,

            UNIQUE(stock_code, pattern_type, detect_time)
        )
    ''')
    print("[Migrate] 创建表：pattern_records")

    # ============== 初始化默认数据 ==============

    # 插入默认选股因子
    default_factors = [
        ('ma_uptrend', '均线多头排列', 'technical', 0.10, 0.5, 'higher_better', 'MA5>MA10>MA20', '短期均线在长期均线之上，呈多头排列'),
        ('macd_golden_cross', 'MACD 金叉', 'technical', 0.15, 0.5, 'higher_better', 'MACD>0 AND MACD_HIST>0', 'MACD 快线上穿慢线，形成金叉'),
        ('rsi_strength', 'RSI 强势', 'technical', 0.10, 50, 'higher_better', 'RSI>50', 'RSI 在 50 以上，显示多头强势'),
        ('kdj_golden_cross', 'KDJ 金叉', 'technical', 0.10, 0.5, 'higher_better', 'K>D AND J>50', 'K 线上穿 D 线，J 值在 50 以上'),
        ('volume_ratio', '量比', 'volume', 0.15, 1.5, 'higher_better', 'VOL/MA(VOL,5)>=1.5', '当日成交量是 5 日均量的 1.5 倍以上'),
        ('volume_trend', '成交量趋势', 'volume', 0.10, 0.5, 'higher_better', 'VOL>MA(VOL,5)', '成交量站在 5 日均量之上'),
        ('5day_gain', '5 日涨幅', 'momentum', 0.15, 0.05, 'higher_better', '(CLOSE-REF(CLOSE,5))/REF(CLOSE,5)>=0.05', '近 5 日涨幅超过 5%'),
        ('breakthrough', '突破新高', 'momentum', 0.15, 0.5, 'higher_better', 'CLOSE>=HHV(HIGH,20)', '收盘价突破近 20 日最高点'),
        ('bullish_engulfing', '阳包阴', 'pattern', 0.10, 0.5, 'higher_better', 'CLOSE>OPEN AND CLOSE>REF(CLOSE,1)', '今日阳线完全包裹昨日阴线'),
    ]

    for factor in default_factors:
        cursor.execute('''
            INSERT OR IGNORE INTO selection_factors
            (factor_id, factor_name, factor_type, weight, threshold, direction, formula, description)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', factor)
    print("[Migrate] 插入默认选股因子")

    # 提交事务
    conn.commit()
    conn.close()

    print("[Migrate] 数据库迁移完成!")
    print(f"[Migrate] 数据库路径：{DB_PATH}")


if __name__ == "__main__":
    migrate()
