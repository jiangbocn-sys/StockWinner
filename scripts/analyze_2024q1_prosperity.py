"""
2024 Q1 行业景气度分析 v2
- 行业分类来自 stock_base_info
- 剔除 PE>100 的极端值（避免平均 PE 被少数股票拉高）
- 景气度 = 盈利增速 + 增长面 + 盈利质量
"""
import sqlite3
conn = sqlite3.connect('data/kline.db')
cur = conn.cursor()

cur.execute('''
CREATE TEMPORARY TABLE industry_map AS
SELECT stock_code, sw_level1, sw_level2, sw_level3
FROM stock_base_info
WHERE sw_level1 IS NOT NULL
''')

# ===== 1. 按行业分析 2024 Q1 景气度 =====
# 过滤条件: PE 0~100, 净利同比 -100%~500%
cur.execute('''
SELECT im.sw_level1 AS industry,
       COUNT(*) as cnt,
       ROUND(AVG(f.roe), 2) as avg_roe,
       ROUND(AVG(f.net_profit_ttm_yoy), 2) as avg_np_yoy,
       ROUND(AVG(f.operating_profit_growth_yoy), 2) as avg_op_yoy,
       ROUND(AVG(f.gross_margin), 2) as avg_gross,
       ROUND(AVG(f.net_margin), 2) as avg_net,
       ROUND(AVG(f.pe_ttm), 2) as avg_pe,
       ROUND(100.0 * SUM(CASE WHEN f.net_profit_ttm_yoy > 0 THEN 1 ELSE 0 END) / COUNT(*), 1) as growth_pct
FROM stock_monthly_factors f
JOIN industry_map im ON f.stock_code = im.stock_code
WHERE f.report_year=2024 AND f.report_quarter=1
  AND f.pe_ttm > 0 AND f.pe_ttm < 100
  AND f.net_profit_ttm_yoy BETWEEN -100 AND 500
GROUP BY im.sw_level1
HAVING cnt >= 10
ORDER BY avg_np_yoy DESC
''')
rows = cur.fetchall()

print('=' * 115)
print('2024 Q1 行业景气度排名 (PE<100, 净利同比 -100%~500%)')
print('=' * 115)
header = f'{"行业":<15} {"样本":>6} {"平均ROE":>8} {"平均净利同比":>12} {"平均营业利润":>12} {"平均毛利率":>10} {"平均PE":>8} {"增长占比":>8}'
print(header)
print('-' * 95)
for r in rows:
    print(f'{r[0]:<15} {r[1]:>6} {r[2]:>7.1f}% {r[3]:>11.1f}% {r[4]:>11.1f}% {r[5]:>9.1f}% {r[6]:>8.1f} {r[7]:>7.1f}%')

# ===== 2. 高景气行业 =====
# 条件: 平均净利同比>10% 且 增长股占比>45%
print('\n' + '=' * 80)
print('高景气行业 (平均净利同比>10% 且 增长股占比>45%)')
print('=' * 80)
high_prosperity = []
for r in rows:
    if r[3] > 10 and r[7] > 45:
        high_prosperity.append(r[0])
        print(f'  [高景气] {r[0]}: 净利同比={r[3]:.1f}%, 增长占比={r[7]:.1f}%, ROE={r[2]:.1f}%, PE={r[6]:.1f}')

# ===== 3. 景气改善行业 =====
print('\n' + '=' * 80)
print('景气改善行业 (增长股占比>50% 但平均净利同比<10%)')
print('=' * 80)
for r in rows:
    if r[0] not in high_prosperity and r[7] > 50:
        print(f'  [改善中] {r[0]}: 净利同比={r[3]:.1f}%, 增长占比={r[7]:.1f}%, ROE={r[2]:.1f}%')

# ===== 4. 高景气行业个股 =====
print('\n' + '=' * 120)
print('高景气行业个股池 (经营正常: ROE>0, PE 0-80, 净利同比>0, 毛利率/净利率合理)')
print('=' * 120)

all_stocks = []
for ind in high_prosperity:
    cur.execute('''
    SELECT f.stock_code, f.stock_name, im.sw_level2,
           ROUND(f.roe, 1), ROUND(f.net_profit_ttm_yoy, 1),
           ROUND(f.pe_ttm, 1),
           ROUND(f.gross_margin, 1),
           ROUND(f.net_margin, 1),
           ROUND(f.total_market_cap, 0)
    FROM stock_monthly_factors f
    JOIN industry_map im ON f.stock_code = im.stock_code
    WHERE f.report_year=2024 AND f.report_quarter=1
      AND im.sw_level1=?
      AND f.roe > 0
      AND f.pe_ttm > 0 AND f.pe_ttm < 80
      AND f.net_profit_ttm_yoy > 0 AND f.net_profit_ttm_yoy < 500
      AND f.gross_margin BETWEEN -10 AND 100
      AND f.net_margin BETWEEN -10 AND 100
    ORDER BY f.net_profit_ttm_yoy DESC
    ''', (ind,))
    stocks = cur.fetchall()
    if stocks:
        all_stocks.extend([(ind, s) for s in stocks])
        print(f'\n  {ind} ({len(stocks)}只)')
        for s in stocks:
            print(f'    {s[0]} {s[1]:<10} {s[2] or "":<12} ROE={s[3]:.1f}% 净利同比={s[4]:.0f}% PE={s[5]:.1f} 毛利率={s[6]:.1f}% 净利率={s[7]:.1f}% 市值={s[8]:.0f}亿')

print(f'\n总计: {len(all_stocks)} 只高景气行业个股')

# ===== 5. 存入结果表 =====
cur.execute('DROP TABLE IF EXISTS prosperity_pool_2024q1')
cur.execute('''
CREATE TABLE prosperity_pool_2024q1 (
    stock_code TEXT,
    stock_name TEXT,
    industry TEXT,
    sub_industry TEXT,
    roe REAL,
    net_profit_ttm_yoy REAL,
    pe_ttm REAL,
    gross_margin REAL,
    net_margin REAL,
    total_market_cap REAL,
    report_year INTEGER,
    report_quarter INTEGER,
    created_at TIMESTAMP DEFAULT (datetime('now', '+8 hours'))
)
''')
for ind, s in all_stocks:
    cur.execute('''
    INSERT INTO prosperity_pool_2024q1
    (stock_code, stock_name, industry, sub_industry, roe, net_profit_ttm_yoy, pe_ttm, gross_margin, net_margin, total_market_cap, report_year, report_quarter)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 2024, 1)
    ''', (s[0], s[1], ind, s[2], s[3], s[4], s[5], s[6], s[7], s[8]))

conn.commit()
print(f'已保存到 prosperity_pool_2024q1 表 ({len(all_stocks)} 条)')

conn.close()
