# Stock Factors 数据可视化工具

## 功能说明

已创建 stock_factors 数据表的可视化管理工具，包括：

### 后端 API (`services/ui/stock_factors_viewer.py`)

| 接口 | 方法 | 功能 |
|------|------|------|
| `/api/v1/ui/stock-factors` | GET | 获取 stock_factors 数据列表（分页、筛选、排序） |
| `/api/v1/ui/stock-factors/{stock_code}` | GET | 获取单只股票的详细数据 |
| `/api/v1/ui/stock-factors/columns` | GET | 获取 stock_factors 表的列信息 |
| `/api/v1/ui/stock-factors/{stock_code}/update` | PUT | 更新 stock_factors 数据 |
| `/api/v1/ui/stock-factors/{stock_code}/delete` | DELETE | 删除 stock_factors 数据 |
| `/api/v1/ui/stock-factors/batch-delete` | POST | 批量删除 stock_factors 数据 |
| `/api/v1/ui/stock-factors/stats/overview` | GET | 获取 stock_factors 数据统计概览 |
| `/api/v1/ui/stock-factors/search` | GET | 搜索 stock_factors 数据 |

### 前端页面 (`frontend/dist/stock-factors.html`)

访问地址：`http://localhost:8080/ui/stock-factors.html`

功能特性：
- 统计数据卡片展示（总记录数、股票数量、最新日期、行业数量）
- 多条件筛选（股票代码、股票名称、交易日期、一级行业）
- 数据表格展示（15 个主要字段）
- 分页导航
- 复选框多选
- 批量删除
- 单条编辑（可编辑行业分类、下期变动等字段）

## 启动服务

```bash
cd /home/bobo/StockWinner
python3 -m uvicorn services.main:app --host 0.0.0.0 --port 8080
```

## 访问页面

浏览器打开：
- Stock Factors 管理：`http://localhost:8080/ui/stock-factors.html`
- API 文档：`http://localhost:8080/docs`

## API 测试示例

```bash
# 获取统计概览
curl http://localhost:8080/api/v1/ui/stock-factors/stats/overview

# 获取数据列表（第 1 页，每页 20 条）
curl "http://localhost:8080/api/v1/ui/stock-factors?page=1&page_size=20"

# 按股票代码筛选
curl "http://localhost:8080/api/v1/ui/stock-factors?stock_code=600000.SH"

# 搜索股票
curl "http://localhost:8080/api/v1/ui/stock-factors/search?keyword=浦发银行"

# 获取单只股票数据
curl "http://localhost:8080/api/v1/ui/stock-factors/600000.SH?trade_date=2026-03-31"
```

## 数据说明

stock_factors 表包含 57 列，约 69 万条记录（数据范围：2006-12-29 至 2026-03-31）：

**数据分组：**
1. 基础信息：id, trade_date, stock_code, stock_name, is_traded
2. 行情数据：open_price, high_price, low_price, close_price, vwap, turnover_value, change_pct
3. 市值数据：circ_market_cap, total_market_cap, days_since_ipo
4. 财报数据：net_profit, net_profit_ttm, operating_cash_flow 等 15 列
5. 市场表现：change_10d, change_20d, bias_5, amplitude_5 等 14 列
6. 技术指标：kdj_k, kdj_d, kdj_j, dif, dea, macd
7. 估值数据：pe_inverse, pb_inverse
8. 行业分类：sw_level1, sw_level2, sw_level3
9. 其他：next_period_changes, created_at

**行业分布 Top 5:**
- 机械设备：72,256 条
- 基础化工：60,590 条
- 医药生物：58,737 条
- 电子：47,888 条
- 计算机：33,774 条

## 注意事项

1. 数据存储在 `data/kline.db` 的 stock_factors 表中
2. 可编辑字段：sw_level1, sw_level2, sw_level3, next_period_changes, is_traded
3. 批量删除操作不可恢复，请谨慎使用
