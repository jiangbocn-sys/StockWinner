# 废弃因子计算脚本

这些脚本已废弃，功能已整合到主入口 `services/data/local_data_service.py`。

| 废弃文件 | 原功能 | 替代方案 |
|----------|--------|----------|
| factor_alignment.py | 因子数据对齐补全 | `fill_empty_factor_values()` |
| incremental_update_factors.py | 增量计算因子 | `calculate_and_save_factors_for_dates()` |
| migrate_factors.py | 迁移因子数据 | 数据库重构已完成 |
| correct_daily_factors.py | 修正因子错误 | `fill_empty_factor_values()` |
| correct_market_cap.py | 修正市值错误 | `fill_empty_factor_values()` |

**当前因子计算入口：**
- `local_data_service.calculate_and_save_factors_for_dates()` - 主要计算函数
- `local_data_service.fill_empty_factor_values()` - 填充缺失记录+空值更新

**废弃日期：** 2026-04-17