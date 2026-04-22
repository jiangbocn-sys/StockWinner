<template>
  <div class="layout-container">
    <NavBar />
    <div class="data-explorer-page">
      <div class="page-header">
        <h1>数据浏览器</h1>
        <div class="header-actions">
          <el-button type="primary" @click="loadData">
            <el-icon><Refresh /></el-icon> 刷新
          </el-button>
        </div>
      </div>

      <!-- 数据库和表选择器 -->
      <el-card class="selector-card">
        <el-form :inline="true" class="selector-form">
          <el-form-item label="数据库">
            <el-select v-model="selectedDb" placeholder="选择数据库" @change="onDatabaseChange" style="width: 200px">
              <el-option v-for="db in databases" :key="db.name" :label="db.name" :value="db.name">
                {{ db.name }} {{ db.exists ? '' : '(不存在)' }}
              </el-option>
            </el-select>
          </el-form-item>
          <el-form-item label="数据表">
            <el-select v-model="selectedTable" placeholder="选择数据表" @change="onTableChange" style="width: 200px" :disabled="!selectedDb">
              <el-option v-for="table in tables" :key="table" :label="table" :value="table" />
            </el-select>
          </el-form-item>
          <el-form-item>
            <el-button type="primary" @click="loadData">
              <el-icon><Refresh /></el-icon> 加载数据
            </el-button>
          </el-form-item>
        </el-form>
      </el-card>

      <!-- 统计卡片 -->
      <el-row :gutter="16" class="stats-row" v-if="selectedTable">
        <el-col :span="8">
          <el-card shadow="hover" class="stat-card">
            <div class="stat-label">总记录数</div>
            <div class="stat-value">{{ stats.totalRecords.toLocaleString() }}</div>
            <div class="stat-sub">{{ selectedTable }} 表</div>
          </el-card>
        </el-col>
        <el-col :span="8">
          <el-card shadow="hover" class="stat-card">
            <div class="stat-label">最早日期</div>
            <div class="stat-value">{{ stats.earliestDate }}</div>
            <div class="stat-sub">{{ stats.earliestDateField }}</div>
          </el-card>
        </el-col>
        <el-col :span="8">
          <el-card shadow="hover" class="stat-card">
            <div class="stat-label">最新日期</div>
            <div class="stat-value">{{ stats.latestDate }}</div>
            <div class="stat-sub">{{ stats.latestDateField }}</div>
          </el-card>
        </el-col>
      </el-row>

      <!-- 筛选条件卡片 -->
      <el-card class="filter-card" v-if="selectedTable">
        <template #header>
          <div class="card-header">
            <span>数据筛选</span>
            <el-button type="primary" size="small" @click="applyFilters">
              <el-icon><Search /></el-icon> 应用筛选
            </el-button>
            <el-button size="small" @click="resetFilters">
              <el-icon><RefreshLeft /></el-icon> 重置
            </el-button>
          </div>
        </template>
        <el-form :inline="true" class="filter-form">
          <!-- 股票代码筛选 -->
          <el-form-item label="股票代码">
            <el-input v-model="filters.stock_code" placeholder="多个代码用逗号分隔" style="width: 200px" :disabled="!filterAvailable.stock_code" />
          </el-form-item>
          <!-- 日期范围筛选 -->
          <el-form-item label="开始日期">
            <el-date-picker v-model="filters.start_date" type="date" placeholder="开始日期" value-format="YYYY-MM-DD" style="width: 160px" :disabled="!filterAvailable.date" />
          </el-form-item>
          <el-form-item label="结束日期">
            <el-date-picker v-model="filters.end_date" type="date" placeholder="结束日期" value-format="YYYY-MM-DD" style="width: 160px" :disabled="!filterAvailable.date" />
          </el-form-item>
          <el-form-item label="快捷范围">
            <el-select v-model="filters.date_range" placeholder="快捷选择" clearable style="width: 120px" :disabled="!filterAvailable.date">
              <el-option label="最近 30 天" value="last_30d" />
              <el-option label="最近 90 天" value="last_90d" />
              <el-option label="今年至今" value="ytd" />
            </el-select>
          </el-form-item>
          <!-- 行业筛选 -->
          <el-form-item label="行业">
            <el-input v-model="filters.industry" placeholder="行业名称" style="width: 150px" :disabled="!filterAvailable.industry" />
          </el-form-item>
          <!-- 市值范围筛选 -->
          <el-form-item label="最小市值 (元)">
            <el-input-number v-model="filters.min_market_cap" :min="0" :precision="0" placeholder="最小市值" style="width: 140px" :disabled="!filterAvailable.circ_market_cap" />
          </el-form-item>
          <el-form-item label="最大市值 (元)">
            <el-input-number v-model="filters.max_market_cap" :min="0" :precision="0" placeholder="最大市值" style="width: 140px" :disabled="!filterAvailable.circ_market_cap" />
          </el-form-item>
          <!-- PE 筛选 -->
          <el-form-item label="最小 PE 倒数">
            <el-input-number v-model="filters.min_pe_inverse" :precision="4" :step="0.01" placeholder="最小 PE 倒数" style="width: 120px" :disabled="!filterAvailable.pe_inverse" />
          </el-form-item>
          <el-form-item label="最大 PE 倒数">
            <el-input-number v-model="filters.max_pe_inverse" :precision="4" :step="0.01" placeholder="最大 PE 倒数" style="width: 120px" :disabled="!filterAvailable.pe_inverse" />
          </el-form-item>
          <!-- 禁用提示 -->
          <el-form-item v-if="selectedTable && Object.values(filterAvailable).some(v => !v)">
            <el-text type="info" size="small">部分筛选器因当前表无对应字段已禁用</el-text>
          </el-form-item>
        </el-form>

        <!-- 组合排序设置 -->
        <div class="sort-settings">
          <div class="sort-header">
            <span>组合排序</span>
            <el-button size="small" type="primary" @click="addSortField" :disabled="!tableColumns.length">
              + 添加排序
            </el-button>
            <el-button size="small" @click="clearSortList" v-if="sortList.length > 0">
              清空
            </el-button>
          </div>

          <div class="sort-list" v-if="sortList.length > 0">
            <div class="sort-item" v-for="(sort, index) in sortList" :key="index">
              <span class="sort-priority">{{ index + 1 }}</span>
              <el-select v-model="sort.field" placeholder="选择字段" style="width: 150px">
                <el-option v-for="col in tableColumns" :key="col.field" :label="col.field" :value="col.field" />
              </el-select>
              <el-select v-model="sort.order" style="width: 90px">
                <el-option label="↑ 升序" value="asc" />
                <el-option label="↓ 降序" value="desc" />
              </el-select>
              <div class="sort-actions">
                <el-button size="small" @click="moveSortField(index, 'up')" :disabled="index === 0" circle>
                  ↑
                </el-button>
                <el-button size="small" @click="moveSortField(index, 'down')" :disabled="index === sortList.length - 1" circle>
                  ↓
                </el-button>
                <el-button size="small" type="danger" @click="removeSortField(index)" circle>
                  ×
                </el-button>
              </div>
            </div>
          </div>
          <el-text type="info" size="small" v-else>点击"添加排序"设置多字段排序，按优先级顺序排列</el-text>
        </div>
      </el-card>

      <!-- 数据表格 -->
      <el-card class="table-card" v-if="selectedTable">
        <div class="table-info">
          <span>当前：{{ selectedDb }} / {{ selectedTable }}</span>
          <span class="column-count">共 {{ tableColumns.length }} 列</span>
          <span v-if="sortList.length > 0" class="sort-info">
            排序：{{ sortList.map(s => `${s.field}(${s.order === 'asc' ? '↑' : '↓'})`).join(' → ') }}
          </span>
        </div>

        <el-table
          :data="tableData"
          style="width: 100%"
          v-loading="loading"
          max-height="calc(100vh - 340px)"
          :scrollbar-always-on="true"
          @sort-change="handleSortChange"
        >
          <el-table-column
            v-for="col in visibleColumns"
            :key="col.field"
            :prop="col.field"
            :label="col.field"
            :width="getColumnWidth(col.type)"
            align="right"
            sortable="custom"
            :sort-orders="['ascending', 'descending', null]"
          />
        </el-table>

        <!-- 分页 -->
        <el-pagination
          v-model:current-page="pagination.page"
          v-model:page-size="pagination.pageSize"
          :page-sizes="[20, 50, 100, 200]"
          :total="pagination.total"
          layout="total, sizes, prev, pager, next"
          @size-change="handlePageChange"
          @current-change="handlePageChange"
          style="margin-top: 20px; justify-content: flex-end"
        />
      </el-card>

      <!-- 提示信息 -->
      <el-empty v-if="!selectedDb" description="请选择数据库开始浏览" />
      <el-empty v-else-if="!selectedTable" description="请选择数据表" />
    </div>
  </div>
</template>

<script setup>
import { ref, reactive, onMounted, computed } from 'vue'
import { ElMessage } from 'element-plus'
import { Refresh, Search, RefreshLeft, Download } from '@element-plus/icons-vue'
import NavBar from '../components/NavBar.vue'

const loading = ref(false)
const selectedDb = ref('')
const selectedTable = ref('')
const tableData = ref([])
const tableColumns = ref([])

const databases = ref([])
const tables = ref([])

const stats = reactive({
  totalRecords: 0,
  earliestDate: '-',
  earliestDateField: '',
  latestDate: '-',
  latestDateField: '',
  primaryDateField: '',  // 主日期字段（用于筛选）
  tableType: 'standard'  // 表类型：standard, weekly, monthly
})

const pagination = reactive({
  page: 1,
  pageSize: 50,
  total: 0
})

// 排序状态（支持多字段组合排序）
const sortList = ref([])  // [{field, order}, ...] 按优先级排序

// 添加排序字段
const addSortField = () => {
  if (!tableColumns.value.length) return
  // 默认添加第一个可排序字段
  const availableFields = tableColumns.value.map(col => col.field)
  if (availableFields.length > 0) {
    sortList.value.push({
      field: availableFields[0],
      order: 'desc'
    })
  }
}

// 删除排序字段
const removeSortField = (index) => {
  sortList.value.splice(index, 1)
}

// 移动排序字段优先级
const moveSortField = (index, direction) => {
  const newIndex = direction === 'up' ? index - 1 : index + 1
  if (newIndex >= 0 && newIndex < sortList.value.length) {
    const item = sortList.value.splice(index, 1)[0]
    sortList.value.splice(newIndex, 0, item)
  }
}

// 清空所有排序
const clearSortList = () => {
  sortList.value = []
}

// 筛选条件
const filters = reactive({
  stock_code: '',
  start_date: '',
  end_date: '',
  date_range: '',
  industry: '',
  min_market_cap: null,
  max_market_cap: null,
  min_pe_inverse: null,
  max_pe_inverse: null
})

// 筛选条件可用性检测（使用动态日期字段）
const filterAvailable = computed(() => {
  const columnFields = tableColumns.value.map(col => col.field)
  const dateField = stats.primaryDateField || 'trade_date'
  return {
    stock_code: columnFields.includes('stock_code'),
    date: columnFields.includes(dateField),  // 动态日期字段检测
    industry: columnFields.includes('sw_level1'),
    circ_market_cap: columnFields.includes('circ_market_cap'),
    pe_inverse: columnFields.includes('pe_inverse')
  }
})

// 可见的列（限制显示前 50 列避免页面卡顿）
const visibleColumns = computed(() => {
  return tableColumns.value.slice(0, 50)
})

// 加载数据库列表
const loadDatabases = async () => {
  try {
    const res = await fetch('/api/v1/ui/databases')
    const data = await res.json()
    if (data.success) {
      databases.value = data.databases
      if (data.databases.length > 0 && data.databases[0].exists) {
        selectedDb.value = data.databases[0].name
        loadTables()
      }
    }
  } catch (e) {
    console.error('加载数据库列表失败:', e)
  }
}

// 加载表列表
const loadTables = async () => {
  if (!selectedDb.value) return
  try {
    const res = await fetch(`/api/v1/ui/databases/${selectedDb.value}/tables`)
    const data = await res.json()
    if (data.success) {
      tables.value = data.tables
      if (data.tables.length > 0) {
        selectedTable.value = data.tables[0]
        loadTableStats()
        loadTableColumns()
        loadData()
      }
    }
  } catch (e) {
    console.error('加载表列表失败:', e)
  }
}

// 加载表统计
const loadTableStats = async () => {
  if (!selectedDb.value || !selectedTable.value) return
  try {
    const res = await fetch(`/api/v1/ui/databases/${selectedDb.value}/tables/${selectedTable.value}/stats`)
    const data = await res.json()
    if (data.success) {
      const s = data.stats
      stats.totalRecords = s.total_records
      stats.primaryDateField = s.primary_date_field || ''
      stats.tableType = s.table_type || 'standard'

      // 处理日期统计
      const dateStats = s.date_stats || {}
      const dateFields = Object.keys(dateStats)
      if (dateFields.length > 0) {
        // 使用主日期字段显示
        const primaryField = stats.primaryDateField || dateFields[0]
        if (dateStats[primaryField]) {
          stats.earliestDate = dateStats[primaryField].earliest || '-'
          stats.latestDate = dateStats[primaryField].latest || '-'
          stats.earliestDateField = primaryField
          stats.latestDateField = primaryField
        } else {
          // 如果主字段没有数据，使用第一个有数据的字段
          const firstDateField = dateFields[0]
          stats.earliestDate = dateStats[firstDateField].earliest || '-'
          stats.latestDate = dateStats[firstDateField].latest || '-'
          stats.earliestDateField = firstDateField
          stats.latestDateField = firstDateField
        }
      } else {
        stats.earliestDate = '-'
        stats.latestDate = '-'
        stats.earliestDateField = '无日期字段'
        stats.latestDateField = '无日期字段'
      }
    }
  } catch (e) {
    console.error('加载统计失败:', e)
  }
}

// 加载表结构
const loadTableColumns = async () => {
  if (!selectedDb.value || !selectedTable.value) return
  try {
    const res = await fetch(`/api/v1/ui/databases/${selectedDb.value}/tables/${selectedTable.value}/columns`)
    const data = await res.json()
    if (data.success) {
      tableColumns.value = data.columns
    }
  } catch (e) {
    console.error('加载表结构失败:', e)
  }
}

// 加载数据
const loadData = async () => {
  if (!selectedDb.value || !selectedTable.value) return
  loading.value = true
  try {
    const params = new URLSearchParams({
      page: pagination.page,
      page_size: pagination.pageSize
    })

    // 添加筛选参数
    if (filters.stock_code) params.append('stock_code', filters.stock_code)
    if (filters.start_date) params.append('start_date', filters.start_date)
    if (filters.end_date) params.append('end_date', filters.end_date)
    if (filters.date_range) params.append('date_range', filters.date_range)
    if (filters.industry) params.append('industry', filters.industry)

    // 添加排序参数（组合排序）
    if (sortList.value.length > 0) {
      // 使用 JSON 格式传递排序参数
      params.append('sort_config', JSON.stringify(sortList.value))
    }

    const res = await fetch(`/api/v1/ui/databases/${selectedDb.value}/tables/${selectedTable.value}/data?${params}`)
    const data = await res.json()

    if (data.success) {
      tableData.value = data.data
      pagination.total = data.pagination.total
      pagination.page = data.pagination.page
      tableColumns.value = data.columns.map(col => ({ field: col, type: 'text' }))
    }
  } catch (e) {
    console.error('加载数据失败:', e)
    ElMessage.error('加载数据失败：' + e.message)
  } finally {
    loading.value = false
  }
}

// 应用筛选（使用高级筛选 API）
const applyFilters = async () => {
  if (!selectedDb.value || !selectedTable.value) {
    ElMessage.warning('请先选择数据库和数据表')
    return
  }

  loading.value = true
  try {
    // 构建筛选条件
    const queryFilters = []

    // 使用动态日期字段
    const dateField = stats.primaryDateField || 'trade_date'

    if (filters.stock_code) {
      const codes = filters.stock_code.split(',').map(c => c.trim()).filter(c => c)
      if (codes.length > 0) {
        queryFilters.push({
          field: 'stock_code',
          operator: codes.length === 1 ? 'eq' : 'in',
          value: codes.length === 1 ? codes[0] : codes
        })
      }
    }

    // 日期筛选（根据表类型调整逻辑）
    if (stats.tableType === 'weekly') {
      // 周K线表：特殊处理
      if (filters.start_date) {
        queryFilters.push({ field: 'week_end_date', operator: 'gte', value: filters.start_date })
      }
      if (filters.end_date) {
        queryFilters.push({ field: 'week_start_date', operator: 'lte', value: filters.end_date })
      }
    } else {
      // 其他表：标准日期筛选
      if (filters.start_date) {
        queryFilters.push({ field: dateField, operator: 'gte', value: filters.start_date })
      }
      if (filters.end_date) {
        queryFilters.push({ field: dateField, operator: 'lte', value: filters.end_date })
      }
    }

    if (filters.industry) {
      queryFilters.push({ field: 'sw_level1', operator: 'eq', value: filters.industry })
    }
    if (filters.min_market_cap !== null && filters.min_market_cap !== undefined) {
      queryFilters.push({ field: 'circ_market_cap', operator: 'gte', value: filters.min_market_cap })
    }
    if (filters.max_market_cap !== null && filters.max_market_cap !== undefined) {
      queryFilters.push({ field: 'circ_market_cap', operator: 'lte', value: filters.max_market_cap })
    }
    if (filters.min_pe_inverse !== null && filters.min_pe_inverse !== undefined) {
      queryFilters.push({ field: 'pe_inverse', operator: 'gte', value: filters.min_pe_inverse })
    }
    if (filters.max_pe_inverse !== null && filters.max_pe_inverse !== undefined) {
      queryFilters.push({ field: 'pe_inverse', operator: 'lte', value: filters.max_pe_inverse })
    }

    const payload = {
      filters: queryFilters,
      limit: pagination.pageSize,
      offset: (pagination.page - 1) * pagination.pageSize,
      // 添加排序参数（组合排序）
      sort: sortList.value
    }

    const res = await fetch(`/api/v1/ui/databases/${selectedDb.value}/tables/${selectedTable.value}/query`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    })
    const data = await res.json()

    if (data.success) {
      tableData.value = data.data
      pagination.total = data.pagination.total
      ElMessage.success(`筛选出 ${data.pagination.total} 条记录`)
    }
  } catch (e) {
    console.error('筛选失败:', e)
    ElMessage.error('筛选失败：' + e.message)
  } finally {
    loading.value = false
  }
}

// 重置筛选
const resetFilters = () => {
  filters.stock_code = ''
  filters.start_date = ''
  filters.end_date = ''
  filters.date_range = ''
  filters.industry = ''
  filters.min_market_cap = null
  filters.max_market_cap = null
  filters.min_pe_inverse = null
  filters.max_pe_inverse = null
  // 清空排序
  clearSortList()
  loadData()
  ElMessage.success('已重置筛选条件')
}

// 分页处理
const handlePageChange = () => {
  // 检查是否有筛选条件
  const hasFilters = filters.stock_code || filters.start_date || filters.end_date || filters.industry ||
                     filters.min_market_cap !== null || filters.max_market_cap !== null ||
                     filters.min_pe_inverse !== null || filters.max_pe_inverse !== null

  if (hasFilters) {
    applyFilters()
  } else {
    loadData()
  }
}

// 排序处理（点击表头时作为第一优先级排序）
const handleSortChange = ({ prop, order }) => {
  // order: 'ascending', 'descending', 或 null
  if (order === 'ascending') {
    // 表头排序作为第一优先级，替换现有排序
    sortList.value = [{ field: prop, order: 'asc' }]
  } else if (order === 'descending') {
    sortList.value = [{ field: prop, order: 'desc' }]
  } else {
    // null 表示取消排序，清空所有
    clearSortList()
  }

  // 重新加载数据（保持当前筛选条件）
  const hasFilters = filters.stock_code || filters.start_date || filters.end_date || filters.industry ||
                     filters.min_market_cap !== null || filters.max_market_cap !== null ||
                     filters.min_pe_inverse !== null || filters.max_pe_inverse !== null

  if (hasFilters) {
    applyFilters()
  } else {
    loadData()
  }
}

// 数据库切换
const onDatabaseChange = () => {
  selectedTable.value = ''
  tables.value = []
  stats.totalRecords = 0
  stats.earliestDate = '-'
  stats.latestDate = '-'
  tableData.value = []
  tableColumns.value = []
  // 清空排序
  clearSortList()
  loadTables()
}

// 表切换
const onTableChange = () => {
  stats.totalRecords = 0
  stats.earliestDate = '-'
  stats.latestDate = '-'
  tableData.value = []
  tableColumns.value = []
  // 清空排序
  clearSortList()
  loadTableStats()
  loadTableColumns()
  loadData()
}

// 获取列宽
const getColumnWidth = (type) => {
  if (type === 'INTEGER' || type === 'INT') return 100
  if (type === 'REAL' || type === 'FLOAT') return 120
  if (type === 'TEXT') return 150
  return 120
}

// 获取当前账户 ID
const currentAccountId = computed(() => {
  return localStorage.getItem('current_account_id') || 'default'
})

onMounted(() => {
  loadDatabases()
})
</script>

<style scoped>
.layout-container {
  min-height: 100vh;
  background-color: #f5f7fa;
}

.data-explorer-page {
  padding: 12px;
}

.page-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 12px;
}

.page-header h1 {
  font-size: 20px;
  color: #303133;
}

.header-actions {
  display: flex;
  gap: 8px;
}

.selector-card {
  margin-bottom: 12px;
}

.selector-card :deep(.el-card__body) {
  padding: 12px;
}

.selector-form {
  display: flex;
  flex-wrap: wrap;
}

.stats-row {
  margin-bottom: 12px;
}

.stat-card {
  text-align: center;
}

.stat-card :deep(.el-card__body) {
  padding: 8px 12px;
}

.stat-label {
  font-size: 12px;
  color: #909399;
  margin-bottom: 4px;
}

.stat-value {
  font-size: 20px;
  font-weight: bold;
  color: #409EFF;
}

.stat-sub {
  font-size: 11px;
  color: #C0C4CC;
  margin-top: 2px;
}

.filter-card {
  margin-bottom: 12px;
}

.filter-card :deep(.el-card__body) {
  padding: 12px;
}

.table-card {
  margin-bottom: 12px;
}

.table-card :deep(.el-card__body) {
  padding: 12px;
}

.table-info {
  display: flex;
  justify-content: space-between;
  padding: 6px 0;
  border-bottom: 1px solid #ebeef5;
  margin-bottom: 8px;
  font-size: 12px;
}

.column-count {
  color: #909399;
  font-size: 12px;
}

.sort-info {
  color: #409EFF;
  font-size: 12px;
  font-weight: 500;
}

.table-card :deep(.el-table) {
  overflow-x: auto;
}

.table-card :deep(.el-table .el-table__cell) {
  padding: 4px 8px;
}

.table-card :deep(.el-table .el-table__header .el-table__cell) {
  padding: 6px 8px;
}

.table-card :deep(.el-table__row) {
  height: 28px;
}

/* 组合排序样式 */
.sort-settings {
  margin-top: 16px;
  padding-top: 12px;
  border-top: 1px solid #ebeef5;
}

.sort-header {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-bottom: 12px;
}

.sort-header span {
  font-size: 14px;
  font-weight: 500;
  color: #303133;
}

.sort-list {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.sort-item {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 8px 12px;
  background: #f5f7fa;
  border-radius: 4px;
}

.sort-priority {
  width: 24px;
  height: 24px;
  display: flex;
  align-items: center;
  justify-content: center;
  background: #409EFF;
  color: white;
  font-size: 12px;
  font-weight: bold;
  border-radius: 4px;
}

.sort-actions {
  display: flex;
  gap: 4px;
}

.sort-actions .el-button {
  width: 28px;
  height: 28px;
  font-size: 12px;
}
</style>
