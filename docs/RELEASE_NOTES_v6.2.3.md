# StockWinner v6.2.3 发布说明

**发布日期**: 2026-04-07  
**版本**: v6.2.3  
**核心主题**: API 数据查询功能增强

---

## 新增功能

### 1. 高级筛选 API
**端点**: `POST /api/v1/ui/databases/{db_name}/tables/{table_name}/query`

**功能特性**:
- 支持 12 种筛选操作符：
  - 比较：`eq`, `ne`, `gt`, `gte`, `lt`, `lte`
  - 集合：`in`, `not_in`
  - 范围：`between`
  - 模糊：`like`
  - 空值：`is_null`, `is_not_null`
- 支持字段选择、多字段排序、分页

**示例请求**:
```json
{
    "filters": [
        {"field": "circ_market_cap", "operator": "lt", "value": 5000000000},
        {"field": "pe_inverse", "operator": "gt", "value": 0.05},
        {"field": "trade_date", "operator": "eq", "value": "2026-04-03"}
    ],
    "fields": ["stock_code", "stock_name", "circ_market_cap"],
    "sort": [{"field": "circ_market_cap", "order": "asc"}],
    "limit": 100,
    "offset": 0
}
```

---

### 2. 聚合统计 API
**端点**: `POST /api/v1/ui/databases/{db_name}/tables/{table_name}/aggregate`

**功能特性**:
- 支持聚合函数：`count`, `sum`, `avg`, `max`, `min`
- 支持 `GROUP BY` 分组
- 支持筛选条件

**示例请求**:
```json
{
    "group_by": ["sw_level1"],
    "aggregations": [
        {"field": "stock_code", "agg": "count", "alias": "stock_count"},
        {"field": "circ_market_cap", "agg": "avg", "alias": "avg_market_cap"}
    ],
    "filters": [
        {"field": "trade_date", "operator": "eq", "value": "2026-04-03"}
    ]
}
```

---

### 3. 筛选模板功能
**配置文件**: `config/screening_templates.json`

**预设模板** (5 个):
| 模板 ID | 名称 | 分类 | 筛选条件 |
|--------|------|------|---------|
| `small_cap_value` | 小市值低估值 | 价值选股 | 市值<50 亿，PE 倒数>0.05，上市天数>=250 |
| `momentum_breakthrough` | 动量突破 | 动量选股 | 10 日涨幅>20%，20 日涨幅>30%，RSI<80 |
| `quality_growth` | 优质成长 | 成长选股 | ROE>15%，净利润增长>20%，毛利率>30% |
| `technical_buy` | 技术面买入 | 技术选股 | KDJ 金叉，RSI<30 |
| `limit_up_potential` | 涨停潜力 | 特色选股 | 10 日内有涨停，5 日波动率>3% |

**API 端点**:
- `GET /api/v1/ui/screening/templates` - 模板列表
- `GET /api/v1/ui/screening/templates/{template_id}` - 模板详情
- `POST /api/v1/ui/screening/template/{template_id}` - 应用模板

---

### 4. 股票基本信息查询
**端点**:
- `GET /api/v1/ui/stocks` - 获取股票列表
- `GET /api/v1/ui/stocks/{stock_code}` - 获取单只股票详情

**功能特性**:
- 支持按行业筛选
- 支持股票代码/名称模糊搜索
- 返回行业分类列表
- 包含最新市值数据

---

### 5. 数据导出功能
**端点**: `POST /api/v1/ui/databases/{db_name}/tables/{table_name}/export`

**支持格式**: CSV, JSON  
**最大导出**: 100,000 条记录

---

### 6. 数据新鲜度检查
**端点**: `GET /api/v1/ui/data/freshness`

**返回信息**:
- 各数据表最新日期
- 总记录数
- 数据表描述

---

## 文件变更清单

### 新增文件
- `config/screening_templates.json` - 筛选模板配置

### 修改文件
- `services/ui/data_explorer.py` (+~500 行)
  - Pydantic 模型定义
  - 高级筛选 API
  - 聚合统计 API
  - 筛选模板 API
  - 股票信息查询 API
  - 数据导出 API
  - 数据新鲜度 API
  
- `services/main.py`
  - 版本号更新为 v6.2.3
  
- `docs/SYSTEM_DESIGN.md`
  - 版本更新至 v6.2.3
  - 新增 API 端点文档
  
- `docs/API_REQUIREMENT_ANALYSIS.md`
  - 版本更新至 v1.1
  - 完成率更新至 90%
  
- `PROGRESS.md`
  - 新增 2026-04-07 工作记录
  
- `README.md`
  - 版本更新至 v6.2.3
  - 新增 API 功能说明
  
- `memory/MEMORY.md`
  - 新增 API 开发完成索引

---

## 测试结果

### API 测试
| 端点 | 状态 |
|------|------|
| 股票列表 | ✅ 通过 |
| 高级筛选 | ✅ 通过 |
| 聚合统计 | ✅ 通过 |
| 模板列表 | ✅ 通过 |
| 数据新鲜度 | ✅ 通过 |
| 数据导出 | ✅ 通过 |

### 服务状态
- 后端服务：✅ 运行正常（端口 8080）
- 版本号：✅ v6.2.3

---

## 需求完成率

| 优先级 | 需求项 | 状态 |
|-------|--------|------|
| P0 | 数据表查询 API | ✅ 100% |
| P0 | 高级筛选功能 | ✅ 100% |
| P0 | 分页功能 | ✅ 100% |
| P0 | 数据一致性修复 | ✅ 100% |
| P1 | 聚合统计 API | ✅ 100% |
| P1 | 预设筛选模板 | ✅ 100% |
| P1 | 股票基本信息查询 | ✅ 100% |
| P1 | 性能优化 | ⚠️ 50% |
| P2 | 数据导出功能 | ✅ 100% |
| P2 | 数据新鲜度检查 | ✅ 100% |
| P2 | 查询性能监控 | ❌ 0% |

**总体完成率**: 约 90%

---

## 升级说明

### 从 v6.2.2 升级
1. 拉取最新代码
2. 重启后端服务：
   ```bash
   lsof -ti:8080 | xargs kill -9
   cd /home/bobo/StockWinner
   source venv/bin/activate
   nohup python3 -m uvicorn services.main:app --host 0.0.0.0 --port 8080 > logs/app.log 2>&1 &
   ```
3. 验证版本：
   ```bash
   curl http://localhost:8080/api/v1/health
   ```

---

## 已知问题

1. **查询性能监控**（P2）暂未实现
   - 计划：在所有查询端点添加执行时间监控
   - 影响：目前无法追踪慢查询

2. **缓存策略**（P1）部分实现
   - 已实现：因子预计算
   - 待实现：Redis 或内存缓存

---

## 下一步计划

### 高优先级
- [ ] 基本面因子计算（PE-TTM, PB, ROE 等）
- [ ] 行业分类数据完善
- [ ] 因子有效性检验（IC 计算、分层回测）

### 中优先级
- [ ] 真实交易执行接口
- [ ] 持仓查询接口
- [ ] 委托查询接口

### 低优先级
- [ ] 查询性能监控
- [ ] 部署测试
- [ ] 性能基准测试

---

**发布人**: AI Assistant  
**审核状态**: ✅ 已完成测试验证
