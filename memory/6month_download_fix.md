---
name: 6 个月增量下载修复
description: 修复选股监控 UI 下载 6 个月数据失败的问题
type: project
---

**问题：** 在选股监控 UI 界面选择"增量下载 - 最近 6 个月"时，系统提示下载失败，但选择 5 年可以正常下载。

**根本原因：**
1. 前端发送 `years: 0.5`（浮点数）
2. 后端 `services/ui/screening.py` 参数定义为 `years: int` 且 `ge=1`，拒绝 0.5
3. 参数名称不匹配：前端传 `years`，但 `download_all_kline_data_sync` 使用 `months` 参数

**修复方案：**
1. `services/ui/screening.py:27` - 改为 `years: float`，`ge=0.5`
2. 添加转换逻辑：`months = int(years * 12)`
3. `services/data/local_data_service.py` - `download_all_kline_data` 参数改为 `months`

**修改的文件：**
- `services/ui/screening.py`
- `services/data/local_data_service.py`

**验证：** 2026-04-03 测试通过
- `years=0.3` → 拒绝
- `years=0.5` → 通过（6 个月）
- `years=5` → 通过（5 年）
