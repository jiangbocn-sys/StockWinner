#!/usr/bin/env python3
"""
注册"短线卖出"策略到数据库
用法: python3 scripts/register_morning_seller.py
"""
import sqlite3
import sys
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "stockwinner.db"

def main():
    # 读取策略代码
    code_file = Path(__file__).parent.parent / "services" / "strategy" / "morning_seller.py"
    if not code_file.exists():
        print(f"错误: 找不到策略文件 {code_file}")
        sys.exit(1)

    code = code_file.read_text()

    # 默认参数
    config = {
        "profit_trigger_pct": 0.02,
        "drop_from_high_pct": 0.002,
        "loss_drop_trigger_pct": 0.015,
        "price_drop_speed_pct": 0.008,
        "max_loss_pct": 0.025,
        "exit_hour": 10,
        "exit_minute": 0,
    }

    import json
    config_json = json.dumps(config, ensure_ascii=False)

    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()

    # 检查是否已存在
    existing = cursor.execute(
        "SELECT id FROM strategies WHERE name = '短线卖出' AND strategy_type = 'python'"
    ).fetchone()

    if existing:
        print(f"策略'短线卖出'已存在 (id={existing[0]})，更新代码和参数...")
        cursor.execute(
            "UPDATE strategies SET code = ?, config = ?, code_scope = 'trading', "
            "code_type = 'python', function_name = 'run', updated_at = datetime('now') "
            "WHERE name = '短线卖出' AND strategy_type = 'python'",
            (code, config_json)
        )
    else:
        print("注册策略'短线卖出'...")
        # 对所有账户注册
        accounts = cursor.execute("SELECT DISTINCT account_id FROM accounts WHERE is_active = 1").fetchall()
        if not accounts:
            print("错误: 没有激活的账户")
            sys.exit(1)

        for acct in accounts:
            acct_id = acct[0]
            cursor.execute(
                "INSERT INTO strategies (name, description, strategy_type, code_scope, code_type, "
                "function_name, account_id, code, config, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))",
                (
                    "短线卖出",
                    "早盘跟踪策略：配合尾盘策略使用，对前一交易日买入的股票进行早盘跟踪卖出",
                    "python",
                    "trading",
                    "python",
                    "run",
                    acct_id,
                    code,
                    config_json,
                )
            )
            print(f"  已为账户 {acct_id} 注册策略")

    conn.commit()
    conn.close()
    print("完成")

if __name__ == "__main__":
    main()
