---
name: 下载数据 UI 和增量下载优化
description: 修改下载数据 UI 为自定义日期范围选择，优化增量下载策略支持 T-1 高频需求
type: project
---

**日期：** 2026-04-03

## 问题背景

用户提出三个问题：

1. **5 年前数据被删除？** - 最早数据是 2021-04-02，担心系统自动删除旧数据
2. **3 天增量保护过高** - 当前 3 天容差阻碍了 T-1 高频增量下载需求
3. **固定周期不灵活** - 用户希望自定义日期范围而非固定的 6 个月/1 年/2 年/5 年

## 修复内容

### 1. 数据保留策略确认

- `cleanup_old_data()` 函数存在但**未被调用**
- 增量下载逻辑使用 `check_data_integrity()` 只下载缺失部分
- **不会删除已有数据**，用户数据是安全的

### 2. 增量下载容差调整

**修改文件：** `services/data/local_data_service.py`

- 结束日期容差：`3 天` → `1 天`
- 支持 T-1 高频增量下载（下载昨天的数据）
- 仍避免重复下载当天数据（数据可能未更新）

```python
# 修改前
end_date_ok = end_days_diff <= 3
if end_days_diff > 3:  # 超过 3 天才认为是缺失

# 修改后
end_date_ok = end_days_diff <= 1
if end_days_diff > 1:  # 超过 1 天才认为是缺失
```

### 3. 自定义日期范围 UI

**修改文件：** `frontend/src/views/Watchlist.vue`

- 新增模式切换：预设范围 / 自定义范围
- 预设范围：6 个月、1 年、2 年、3 年、5 年、10 年
- 自定义范围：日期选择器选择开始和结束日期
- 前端验证：开始日期不能晚于结束日期

**修改文件：** `services/ui/screening.py`

- API 支持 `start_date` 和 `end_date` 参数
- 保留 `years` 参数向后兼容
- 自动计算月数用于增量下载逻辑

**修改文件：** `services/data/local_data_service.py`

- `download_all_kline_data` 支持 `start_date` 和 `end_date` 参数
- `download_all_kline_data_sync` 传递日期范围参数

## API 使用示例

```bash
# 使用预设年数（向后兼容）
curl -X POST /api/v1/ui/{account_id}/data/download \
  -H "Content-Type: application/json" \
  -d '{"years": 2, "batch_size": 50}'

# 使用自定义日期范围
curl -X POST /api/v1/ui/{account_id}/data/download \
  -H "Content-Type: application/json" \
  -d '{"start_date": "2025-01-01", "end_date": "2026-04-03", "batch_size": 50}'
```

## 验证结果

- 后端服务运行在 8080 端口
- API 接受新的日期范围参数
- 增量下载容差从 3 天改为 1 天
