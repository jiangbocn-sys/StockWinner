#!/usr/bin/env python3
"""
根据季度末行业景气度建立股票池

逻辑：
  1. 按申万一级行业聚合，计算平均净利同比增速
  2. 选出当季平均净利同比 >= 1% 的增长行业，按增速降序排名
  3. 根据排名分档取股票：
     前3名 → 全部股票
     第3-7名 → 按净利同比排名取前50%
     第7名之后 → 按净利同比排名取前25%
  4. 剔除问题股：ST（名称含ST）、拟退市（delist_date非空）

存储：stockwinner.db 的 candidate_groups + watchlist
"""
import sqlite3
import math
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

QUARTERS = [
    (2022, 1), (2022, 2), (2022, 3), (2022, 4),
    (2023, 1), (2023, 2), (2023, 3), (2023, 4),
    (2024, 1), (2024, 2), (2024, 3), (2024, 4),
    (2025, 1), (2025, 2), (2025, 3), (2025, 4),
    (2026, 1),
]

ACCOUNT_ID = "8229DE7E"
KLINE_DB = "data/kline.db"
STOCK_DB = "data/stockwinner.db"

# 景气度门槛：行业平均净利同比增速不低于此值
PROSPERITY_THRESHOLD = 1.0  # 1%


def get_growing_industries(kline_cur, year, quarter):
    """选出当季平均净利同比 >= 1% 的行业，按增速降序"""
    kline_cur.execute('''
    SELECT im.sw_level1 AS industry,
           COUNT(*) as cnt,
           ROUND(AVG(f.net_profit_ttm_yoy), 1) as avg_yoy,
           ROUND(100.0 * SUM(CASE WHEN f.net_profit_ttm_yoy > 0 THEN 1 ELSE 0 END) / COUNT(*), 1) as growth_pct
    FROM stock_monthly_factors f
    JOIN (
        SELECT stock_code, sw_level1 FROM stock_base_info WHERE sw_level1 IS NOT NULL
    ) im ON f.stock_code = im.stock_code
    WHERE f.report_year=? AND f.report_quarter=?
      AND f.pe_ttm > 0 AND f.pe_ttm < 100
      AND f.net_profit_ttm_yoy BETWEEN -100 AND 500
    GROUP BY im.sw_level1
    HAVING cnt >= 10
    ORDER BY avg_yoy DESC
    ''', (year, quarter))

    result = []
    for r in kline_cur.fetchall():
        if r[2] >= PROSPERITY_THRESHOLD:
            result.append(r)
    return result


def get_industry_stocks_sorted(kline_cur, year, quarter, industry_name):
    """获取某行业当季所有股票，按净利同比降序排列"""
    kline_cur.execute('''
    SELECT f.stock_code, f.stock_name, im.sw_level2,
           ROUND(f.net_profit_ttm_yoy, 1),
           ROUND(f.pe_ttm, 1),
           ROUND(f.roe, 1)
    FROM stock_monthly_factors f
    JOIN (
        SELECT stock_code, sw_level1, sw_level2 FROM stock_base_info
        WHERE sw_level1 IS NOT NULL
    ) im ON f.stock_code = im.stock_code
    WHERE f.report_year=? AND f.report_quarter=?
      AND im.sw_level1=?
    ORDER BY f.net_profit_ttm_yoy DESC
    ''', (year, quarter, industry_name))

    return kline_cur.fetchall()


def get_problem_stocks_set(kline_cur):
    """获取所有问题股集合：ST、退市"""
    kline_cur.execute('''
    SELECT stock_code FROM stock_base_info
    WHERE stock_name LIKE '%ST%'
       OR (delist_date IS NOT NULL AND delist_date != '')
    ''')
    return {r[0] for r in kline_cur.fetchall()}


def pick_stocks_by_rank(stocks, rank, problem_stocks):
    """按排名分档取股票，剔除问题股和亏损股"""
    seen = set()
    picked = []

    for s in stocks:
        if s[0] in seen:
            continue
        seen.add(s[0])

        if s[0] in problem_stocks:
            continue

        # 剔除亏损股（净利同比 <= 0）
        yoy = s[3]
        if yoy is not None and yoy <= 0:
            continue

        if rank <= 3:
            # 前3名：全部
            picked.append(s)
        elif rank <= 7:
            # 第3-7名：前50%
            cutoff = max(1, int(math.ceil(len(stocks) * 0.50)))
            if len(picked) < cutoff:
                picked.append(s)
        else:
            # 第7名之后：前25%
            cutoff = max(1, int(math.ceil(len(stocks) * 0.25)))
            if len(picked) < cutoff:
                picked.append(s)

    return picked


def save_to_stockwinner_db(stock_cur, quarter_name, stocks, account_id):
    """将股票池存入 stockwinner.db"""
    stock_cur.execute(
        "SELECT id FROM candidate_groups WHERE name = ? AND account_id = ?",
        (quarter_name, account_id)
    )
    existing = stock_cur.fetchone()

    if existing:
        group_id = existing[0]
        stock_cur.execute("DELETE FROM watchlist WHERE group_id = ?", (group_id,))
        print(f"    已存在候选组 ID={group_id}，清空后重新插入")
    else:
        stock_cur.execute(
            "INSERT INTO candidate_groups (account_id, name, group_type, created_at, updated_at) "
            "VALUES (?, ?, 'manual', datetime('now', '+8 hours'), datetime('now', '+8 hours'))",
            (account_id, quarter_name)
        )
        group_id = stock_cur.lastrowid
        print(f"    新建候选组 ID={group_id}")

    now = "2026-05-20T12:00:00"
    for s in stocks:
        yoy = s[3]
        pe = s[4]
        ind2 = s[2] or ""
        if yoy is not None and pe is not None:
            reason = f"{ind2} | 净利同比{yoy:.0f}% PE{pe:.1f}"
        elif yoy is not None:
            reason = f"{ind2} | 净利同比{yoy:.0f}%"
        else:
            reason = ind2
        stock_cur.execute('''
        INSERT INTO watchlist (
            account_id, group_id, source_type, stock_code, stock_name,
            reason, status, target_quantity,
            created_at, updated_at
        ) VALUES (?, ?, 'screening', ?, ?, ?, 'watching', 0, ?, ?)
        ''', (account_id, group_id, s[0], s[1], reason, now, now))

    print(f"    已插入 {len(stocks)} 只股票到 watchlist")
    return group_id


def main():
    print("=" * 60)
    print("行业景气度股票池建立 (2022Q1 - 2026Q1)")
    print(f"景气度门槛: 行业平均净利同比 >= {PROSPERITY_THRESHOLD}%")
    print("=" * 60)

    kline_conn = sqlite3.connect(KLINE_DB)
    kline_cur = kline_conn.cursor()

    stock_conn = sqlite3.connect(STOCK_DB)
    stock_cur = stock_conn.cursor()

    problem_stocks = get_problem_stocks_set(kline_cur)
    print(f"\n问题股(ST/退市): {len(problem_stocks)} 只")

    total_stocks = 0
    quarter_count = 0

    for year, quarter in QUARTERS:
        quarter_name = f"{year}Q{quarter}"
        print(f"\n--- {quarter_name} ---")

        growing = get_growing_industries(kline_cur, year, quarter)
        if not growing:
            print(f"    无满足条件的行业，跳过")
            continue

        print(f"    增长行业({len(growing)}):")
        for i, ind in enumerate(growing):
            print(f"      第{i+1}名 {ind[0]}: 平均净利同比{ind[2]:.1f}%")

        # 分档取股票
        all_picked = []
        for i, ind in enumerate(growing):
            rank = i + 1
            stocks = get_industry_stocks_sorted(kline_cur, year, quarter, ind[0])

            picked = pick_stocks_by_rank(stocks, rank, problem_stocks)

            if picked:
                print(f"    第{rank}名 {ind[0]}: 总{len(stocks)}只, 入选{len(picked)}只")
                all_picked.extend(picked)

        if not all_picked:
            print(f"    剔除问题股后无股票，跳过")
            continue

        print(f"    本季共入选 {len(all_picked)} 只")
        group_id = save_to_stockwinner_db(stock_cur, quarter_name, all_picked, ACCOUNT_ID)
        total_stocks += len(all_picked)
        quarter_count += 1

    stock_conn.commit()

    print("\n" + "=" * 60)
    print(f"完成：共 {total_stocks} 只股票纳入 {quarter_count} 个季度的景气度股票池")
    print("=" * 60)

    kline_conn.close()
    stock_conn.close()


if __name__ == "__main__":
    main()
