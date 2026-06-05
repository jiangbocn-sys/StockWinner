---
name: 周K线下载修复
description: 周K线保存使用SDK trade_date直接计算周边界，不再依赖预构建日历
type: project
---

周K线下载两个核心问题的修复（2026-05-16）：

**问题1**: 股票名称显示为代码（如 "600000.SH"）
- 原因：download_weekly_kline.py 从 kline_data 表查名称，部分记录名称字段就是代码本身
- 修复：优先查 stock_base_info 表（每日SDK同步，名称最新），kline_data仅作补充

**问题2**: 新周数据未下载（W19/W20缺失）
- 原因：save_weekly_kline_data() 用个股日K线构建"交易周日历"映射SDK周线bar，日数据覆盖不足的股票日历条目少于SDK返回的bar数，多余bar被丢弃
- 修复：直接使用SDK返回的 trade_date 计算该ISO周的周一（fromisocalendar）和周五（+4天）作为周边界，不再依赖预构建日历
- 增量下载范围：从10年缩至 latest+90天，limit从520降至200

**验证**: W19 (2026-05-08): 5,730条，W20 (2026-05-15): 5,731条，周收盘价与日数据周五收盘价一致