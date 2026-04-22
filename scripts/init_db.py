#!/usr/bin/env python3
"""
StockWinner 数据库初始化脚本
创建所有表结构和索引
"""

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "stockwinner.db"

def init_database():
    """初始化数据库"""
    # 确保数据目录存在
    DB_PATH.parent.mkdir(exist_ok=True)

    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()

    # 启用外键
    cursor.execute("PRAGMA foreign_keys = ON")

    # 1. 创建账户表
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS accounts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_id TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL,
            username TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            display_name TEXT,
            is_active INTEGER DEFAULT 1,
            created_at DATETIME NOT NULL DEFAULT (datetime('now')),
            updated_at DATETIME NOT NULL DEFAULT (datetime('now')),
            broker_account TEXT,
            broker_password TEXT,
            broker_company TEXT,
            broker_server_ip TEXT,
            broker_server_port INTEGER,
            broker_status TEXT,
            notes TEXT,
            available_cash REAL DEFAULT 0.0
        )
    """)

    # 2. 创建持仓表
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stock_positions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_id TEXT NOT NULL,
            user_id INTEGER NOT NULL,
            stock_code TEXT NOT NULL,
            stock_name TEXT,
            quantity INTEGER NOT NULL DEFAULT 0,
            available_quantity INTEGER NOT NULL DEFAULT 0,
            avg_cost REAL NOT NULL DEFAULT 0.0,
            market_value REAL NOT NULL DEFAULT 0.0,
            current_price REAL NOT NULL DEFAULT 0.0,
            profit_loss REAL NOT NULL DEFAULT 0.0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # 3. 创建交易记录表
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS trade_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_id TEXT NOT NULL,
            user_id INTEGER NOT NULL,
            order_id TEXT NOT NULL,
            stock_code TEXT NOT NULL,
            stock_name TEXT,
            trade_type TEXT NOT NULL,
            quantity INTEGER NOT NULL,
            price REAL NOT NULL,
            amount REAL NOT NULL,
            commission REAL NOT NULL DEFAULT 0.0,
            trade_time TIMESTAMP NOT NULL,
            status TEXT DEFAULT 'pending',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # 4. 创建订单表
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_id TEXT NOT NULL,
            user_id INTEGER NOT NULL,
            stock_code TEXT NOT NULL,
            order_type TEXT,
            quantity INTEGER NOT NULL,
            price REAL,
            status TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # 5. 创建策略表（增强版）
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS strategies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_id TEXT NOT NULL,
            name TEXT NOT NULL,
            description TEXT,
            strategy_type TEXT DEFAULT 'manual',
            config TEXT,
            status TEXT DEFAULT 'draft',
            is_active INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # 6. 创建系统配置表
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS system_config (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            config_key TEXT NOT NULL UNIQUE,
            config_value TEXT,
            description TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # 7. 创建-watchlist 表（候选股票池）
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS watchlist (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_id TEXT NOT NULL,
            strategy_id INTEGER,
            stock_code TEXT NOT NULL,
            stock_name TEXT,
            reason TEXT,
            buy_price REAL,
            stop_loss_price REAL,
            take_profit_price REAL,
            target_quantity INTEGER,
            status TEXT DEFAULT 'pending',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (strategy_id) REFERENCES strategies(id)
        )
    """)

    # 8. 创建交易信号表
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS trading_signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_id TEXT NOT NULL,
            strategy_id INTEGER,
            stock_code TEXT NOT NULL,
            stock_name TEXT,
            signal_type TEXT NOT NULL,
            price REAL,
            target_quantity INTEGER,
            status TEXT DEFAULT 'pending',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            executed_at TIMESTAMP,
            FOREIGN KEY (strategy_id) REFERENCES strategies(id)
        )
    """)

    # 创建索引
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_account_stock ON stock_positions(account_id, stock_code)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_account_positions ON stock_positions(account_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_account_trade ON trade_records(account_id, trade_time)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_account_trades ON trade_records(account_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_account_orders ON orders(account_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_account_strategies ON strategies(account_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_account_watchlist ON watchlist(account_id, status)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_watchlist_account ON watchlist(account_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_signals_account ON trading_signals(account_id, status)")

    conn.commit()
    conn.close()

    print(f"数据库初始化完成：{DB_PATH}")

if __name__ == "__main__":
    init_database()
