# 选股程序优化方案

**创建时间**: 2026-04-04
**版本**: v1.0 (已实施第一阶段)
**状态**: 第一阶段已完成，优化版选股功能正常工作

---

## 实施进度

### ✅ 第一阶段：基础设施（已完成）

1. **创建因子注册表** (`services/screening/factor_registry.py`)
   - ✅ 实现因子映射配置
   - ✅ 实现条件解析器
   - ✅ 实现数据库因子批量获取
   - ✅ 注册内置计算器（RSI, MA, EMA, KDJ 分量）

2. **扩展数据库因子** 
   - ✅ `stock_daily_factors` 表已有 30+ 因子
   - ✅ 包括：MACD、KDJ、涨跌幅、乖离率、振幅、波动率、市值、估值等

3. **选股逻辑重构**
   - ✅ 添加 `_evaluate_conditions_from_local_optimized` 方法
   - ✅ 保留旧逻辑作为 fallback
   - ✅ 自动回退机制

### 🔄 第二阶段：测试验证（进行中）

- ✅ 单元测试通过
- ⏳ 性能基准测试
- ⏳ 对比新旧逻辑结果一致性

### ⏳ 第三阶段：因子库扩展（待实施）

5. **丰富指标计算工具库**
   - 添加布林带计算器
   - 添加 ATR 计算器
   - 添加更多技术指标

6. **更新因子计算服务**
   - 将新指标纳入批量计算流程
   - 更新 `stock_daily_factors` 表结构

---

## 问题分析

### 当前实现

```python
# services/screening/service.py:264
indicators = calculate_indicators_for_screening(closes, highs, lows, volumes)
```

**问题**：
1. **重复计算**：每只股票都重新计算所有技术指标（MA、RSI、KDJ、MACD 等）
2. **效率低下**：5194 只股票 × 多个指标 = 数万次计算
3. **数据浪费**：`stock_daily_factors` 表已有 569 万条因子记录未被利用
4. **耦合严重**：选股逻辑与指标计算逻辑绑定

---

## 优化目标

### 核心思路

**"因子优先，按需计算"**

1. **优先使用已有因子**：从 `stock_daily_factors` 表直接读取预计算的因子
2. **按需计算缺失因子**：只对数据库中不包含的指标进行动态计算
3. **可扩展架构**：新增选股条件只需丰富因子库，无需修改选股程序

---

## 优化设计

### 1. 因子映射层

创建因子映射配置，定义选股条件与数据库字段的对应关系：

```python
# services/screening/factor_registry.py

FACTOR_MAPPING = {
    # 均线类
    'MA5': {'table': 'stock_daily_factors', 'column': 'ma5', 'source': 'db'},
    'MA10': {'table': 'stock_daily_factors', 'column': 'ma10', 'source': 'db'},
    'MA20': {'table': 'stock_daily_factors', 'column': 'ma20', 'source': 'db'},
    'MA60': {'table': 'stock_daily_factors', 'column': 'ma60', 'source': 'db'},
    
    # KDJ 类
    'K': {'table': 'stock_daily_factors', 'column': 'kdj_k', 'source': 'db'},
    'D': {'table': 'stock_daily_factors', 'column': 'kdj_d', 'source': 'db'},
    'J': {'table': 'stock_daily_factors', 'column': 'kdj_j', 'source': 'db'},
    
    # MACD 类
    'DIF': {'table': 'stock_daily_factors', 'column': 'dif', 'source': 'db'},
    'DEA': {'table': 'stock_daily_factors', 'column': 'dea', 'source': 'db'},
    'MACD': {'table': 'stock_daily_factors', 'column': 'macd', 'source': 'db'},
    
    # 涨跌幅类
    'CHANGE_10D': {'table': 'stock_daily_factors', 'column': 'change_10d', 'source': 'db'},
    'CHANGE_20D': {'table': 'stock_daily_factors', 'column': 'change_20d', 'source': 'db'},
    
    # 乖离率类
    'BIAS_5': {'table': 'stock_daily_factors', 'column': 'bias_5', 'source': 'db'},
    'BIAS_10': {'table': 'stock_daily_factors', 'column': 'bias_10', 'source': 'db'},
    'BIAS_20': {'table': 'stock_daily_factors', 'column': 'bias_20', 'source': 'db'},
    
    # 振幅类
    'AMPLITUDE_5': {'table': 'stock_daily_factors', 'column': 'amplitude_5', 'source': 'db'},
    'AMPLITUDE_10': {'table': 'stock_daily_factors', 'column': 'amplitude_10', 'source': 'db'},
    'AMPLITUDE_20': {'table': 'stock_daily_factors', 'column': 'amplitude_20', 'source': 'db'},
    
    # 波动率类
    'CHANGE_STD_5': {'table': 'stock_daily_factors', 'column': 'change_std_5', 'source': 'db'},
    'CHANGE_STD_10': {'table': 'stock_daily_factors', 'column': 'change_std_10', 'source': 'db'},
    'CHANGE_STD_20': {'table': 'stock_daily_factors', 'column': 'change_std_20', 'source': 'db'},
    
    # 估值类
    'PE_INV': {'table': 'stock_daily_factors', 'column': 'pe_inverse', 'source': 'db'},
    'PB_INV': {'table': 'stock_daily_factors', 'column': 'pb_inverse', 'source': 'db'},
    
    # 市值类
    'CIRC_MARKET_CAP': {'table': 'stock_daily_factors', 'column': 'circ_market_cap', 'source': 'db'},
    'TOTAL_MARKET_CAP': {'table': 'stock_daily_factors', 'column': 'total_market_cap', 'source': 'db'},
    
    # 需要动态计算的指标
    'RSI': {'calculator': 'calculate_rsi', 'source': 'calc'},
    'EMA12': {'calculator': 'calculate_ema', 'params': {'period': 12}, 'source': 'calc'},
    'EMA26': {'calculator': 'calculate_ema', 'params': {'period': 26}, 'source': 'calc'},
}
```

### 2. 选股流程重构

```
┌─────────────────────────────────────────────────────────┐
│  1. 解析策略条件                                        │
│     buy: ["MA5 > MA20", "RSI < 30", "KDJ_J > 100"]     │
└─────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────┐
│  2. 分析条件中使用的指标                                │
│     需要的指标：MA5, MA20, RSI, KDJ_J                  │
└─────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────┐
│  3. 查询数据库因子 (批量获取)                           │
│     SELECT stock_code, ma5, ma20, kdj_j                │
│     FROM stock_daily_factors                           │
│     WHERE trade_date = '2026-04-02'                    │
└─────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────┐
│  4. 识别缺失指标                                        │
│     数据库有：MA5, MA20, KDJ_J ✓                       │
│     数据库无：RSI → 需要动态计算                        │
└─────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────┐
│  5. 按需计算缺失指标                                    │
│     只对需要的股票计算 RSI                              │
└─────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────┐
│  6. 合并数据并评估条件                                  │
│     检查每个条件，计算匹配度                            │
└─────────────────────────────────────────────────────────┘
```

### 3. 核心类设计

```python
class FactorRegistry:
    """因子注册表 - 管理所有可用因子"""
    
    def __init__(self):
        self._db_factors = {}  # 数据库因子映射
        self._calculators = {}  # 动态计算器注册
        
    def register_db_factor(self, name: str, table: str, column: str):
        """注册数据库因子"""
        self._db_factors[name] = {'table': table, 'column': column}
        
    def register_calculator(self, name: str, calculator_func):
        """注册动态计算器"""
        self._calculators[name] = calculator_func
        
    def get_required_factors(self, conditions: List[str]) -> Set[str]:
        """从条件中提取需要的指标名称"""
        # 解析 "MA5 > MA20" → {'MA5', 'MA20'}
        pass
        
    def fetch_db_factors(
        self,
        factors: Set[str],
        trade_date: str
    ) -> Dict[str, pd.DataFrame]:
        """批量从数据库获取因子数据"""
        # 返回 {factor_name: DataFrame(stock_code, value)}
        pass
        
    def calculate_factors(
        self,
        factors: Set[str],
        stock_codes: List[str],
        kline_data: Dict[str, pd.DataFrame]
    ) -> Dict[str, Dict[str, float]]:
        """按需计算缺失因子"""
        pass
```

### 4. 选股服务重构

```python
class ScreeningService:
    async def _evaluate_conditions_optimized(
        self,
        config: Dict,
        match_score_threshold: float,
        trade_date: str
    ) -> List[Dict]:
        """优化的选股逻辑"""
        
        # 1. 解析条件，提取需要的指标
        buy_conditions = self._parse_buy_conditions(config)
        required_factors = self._extract_required_factors(buy_conditions)
        
        # 2. 分类：数据库因子 vs 需要计算的因子
        db_factors, calc_factors = self._classify_factors(required_factors)
        
        # 3. 批量获取数据库因子
        factor_data = await self._fetch_db_factors(db_factors, trade_date)
        
        # 4. 获取 K 线数据（只针对需要计算的因子）
        kline_data = {}
        if calc_factors:
            kline_data = await self._fetch_kline_data(factor_data.index)
        
        # 5. 计算缺失因子
        if calc_factors:
            calculated = self._calculate_factors(calc_factors, kline_data)
            factor_data = self._merge_factor_data(factor_data, calculated)
        
        # 6. 评估条件
        candidates = []
        for stock_code, indicators in factor_data.iterrows():
            matched = self._evaluate_stock(stock_code, indicators, buy_conditions)
            if matched['score'] >= match_score_threshold:
                candidates.append(matched)
        
        return candidates
```

---

## 实施步骤

### 第一阶段：基础设施

1. **创建因子注册表** (`services/screening/factor_registry.py`)
   - 实现因子映射配置
   - 实现条件解析器
   - 实现数据库因子批量获取

2. **扩展数据库因子** 
   - 检查 `stock_daily_factors` 表，添加缺失因子字段
   - 更新因子计算逻辑，确保新因子被计算

预计工时：4-6 小时

### 第二阶段：选股逻辑重构

3. **修改 `_evaluate_conditions_from_local`**
   - 添加优化版本 `_evaluate_conditions_optimized`
   - 保留旧逻辑作为 fallback

4. **测试验证**
   - 对比新旧逻辑结果一致性
   - 性能基准测试

预计工时：6-8 小时

### 第三阶段：因子库扩展

5. **丰富指标计算工具库**
   - 添加 RSI、布林带、ATR 等计算器
   - 统一接口规范

6. **更新因子计算服务**
   - 将新指标纳入批量计算流程

预计工时：4-6 小时

---

## 性能对比

### 当前实现

| 操作 | 次数 | 说明 |
|------|------|------|
| MA 计算 | 5194 × 4 = 20776 | 每只股票计算 4 个 MA |
| KDJ 计算 | 5194 × 1 = 5194 | 每只股票计算 KDJ |
| MACD 计算 | 5194 × 1 = 5194 | 每只股票计算 MACD |
| RSI 计算 | 5194 × 1 = 5194 | 每只股票计算 RSI |
| **总计** | **~36000 次计算** | 每次选股 |

### 优化后

| 操作 | 次数 | 说明 |
|------|------|------|
| 数据库查询 | 1 次 | 批量获取所有因子 |
| RSI 计算 | 5194 | 只对数据库没有的因子计算 |
| **总计** | **~5200 次计算** | 减少 85% |

**预期性能提升**：
- 计算量减少 85%+
- 选股时间从 2-3 分钟降至 30-60 秒
- 新增选股条件无需修改代码

---

## 扩展示例

假设要添加新的选股条件："布林带下轨突破"

### 传统方式（需要修改选股程序）

1. 修改 `calculate_indicators_for_screening()` 添加布林带计算
2. 修改 `TechnicalIndicators.check_condition()` 添加布林带支持
3. 重新测试整个选股流程

### 优化方式（无需修改选股程序）

1. 在因子库中添加 `calculate_bollinger_bands()` 函数
2. 在因子注册表中注册 `BOLL_UPPER`, `BOLL_MIDDLE`, `BOLL_LOWER`
3. 更新因子计算服务，批量计算布林带因子
4. 用户直接使用条件 `"PRICE < BOLL_LOWER"`

**代码零修改**，只需丰富因子库！

---

## 注意事项

1. **数据一致性**：数据库因子需要定期更新，确保与最新交易日同步
2. **回退机制**：数据库查询失败时，应回退到动态计算模式
3. **因子验证**：新注册的因子需要经过测试验证
4. **文档更新**：维护因子字典文档，说明每个因子的含义和计算方法

---

## 总结

优化后的选股程序具备以下特点：

✅ **高效**：优先使用预计算因子，减少 85%+ 计算量
✅ **可扩展**：新增选股条件只需丰富因子库
✅ **低耦合**：选股逻辑与指标计算解耦
✅ **易维护**：统一的因子注册表，清晰的数据流
