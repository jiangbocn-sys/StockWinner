<template>
  <div class="layout-container">
    <NavBar />
    <el-main class="main-content">
      <div class="page-header">
        <h2>交易信号 - {{ currentAccount?.display_name }}</h2>
        <el-space>
          <el-dropdown @command="(fmt) => handleExportSignals(fmt)">
            <el-button type="success" size="small">
              <el-icon><Download /></el-icon>导出
            </el-button>
            <template #dropdown>
              <el-dropdown-menu>
                <el-dropdown-item command="csv">CSV</el-dropdown-item>
                <el-dropdown-item command="json">JSON</el-dropdown-item>
                <el-dropdown-item command="md">Markdown</el-dropdown-item>
                <el-dropdown-item command="txt">TXT</el-dropdown-item>
                <el-dropdown-item command="excel">Excel</el-dropdown-item>
              </el-dropdown-menu>
            </template>
          </el-dropdown>
          <el-button @click="loadSignals">
            <el-icon><Refresh /></el-icon>
            刷新
          </el-button>
        </el-space>
      </div>

      <!-- 服务状态 -->
      <el-card class="status-card">
        <el-descriptions :column="3" border>
          <el-descriptions-item label="监控服务">
            <el-tag :type="monitoringRunning ? 'success' : 'info'" size="small">
              {{ monitoringRunning ? '运行中（自动管理）' : '未启动' }}
            </el-tag>
          </el-descriptions-item>
          <el-descriptions-item label="监控账户">
            {{ monitoringAccountIds.length > 0 ? monitoringAccountIds.join(', ') : '无' }}
          </el-descriptions-item>
          <el-descriptions-item label="待处理信号">
            {{ signals.filter(s => s.status === 'pending').length }}
          </el-descriptions-item>
        </el-descriptions>
      </el-card>

      <!-- 主内容区：手动下单 + 信号列表 -->
      <div class="split-container" ref="splitContainer">
        <!-- 左侧：手动下单 -->
        <ManualOrderPanel
          :account-id="currentAccountId"
          @order-submitted="loadSignals"
        />

        <!-- 拖拽分割线 -->
        <div class="splitter" @mousedown="startDrag">
          <div class="splitter-handle"></div>
        </div>

        <!-- 右侧：信号列表 -->
        <div class="signals-panel">
          <!-- 信号类型筛选 -->
          <div class="filter-bar">
            <el-radio-group v-model="filterType" size="small" @change="loadSignals">
              <el-radio-button label="">全部</el-radio-button>
              <el-radio-button label="buy">买入</el-radio-button>
              <el-radio-button label="sell_stop_loss">止损卖出</el-radio-button>
              <el-radio-button label="sell_take_profit">止盈卖出</el-radio-button>
            </el-radio-group>
          </div>

          <el-table :data="signals" stripe style="width: 100%" v-loading="loading">
            <el-table-column prop="created_at" label="生成时间" width="170">
              <template #default="{ row }">{{ formatTime(row.created_at) }}</template>
            </el-table-column>
            <el-table-column prop="stock_code" label="股票代码" width="120" />
            <el-table-column prop="stock_name" label="股票名称" width="120" />
            <el-table-column prop="signal_type" label="信号类型" width="120">
              <template #default="{ row }">
                <el-tag :type="getSignalType(row.signal_type)" size="small">
                  {{ getSignalText(row.signal_type) }}
                </el-tag>
              </template>
            </el-table-column>
            <el-table-column prop="price" label="委托价" width="100" align="right">
              <template #default="{ row }">¥{{ row.price?.toFixed(2) }}</template>
            </el-table-column>
            <el-table-column prop="current_price" label="现价" width="100" align="right">
              <template #default="{ row }">
                <span v-if="row.current_price" :class="row.current_price > row.price ? 'price-up' : row.current_price < row.price ? 'price-down' : ''">
                  ¥{{ row.current_price.toFixed(2) }}
                </span>
                <span v-else class="text-muted">-</span>
              </template>
            </el-table-column>
            <el-table-column prop="target_quantity" label="数量" width="80" align="right" />
            <el-table-column prop="status" label="状态" width="90">
              <template #default="{ row }">
                <el-tag :type="getStatusType(row.status)" size="small">
                  {{ getStatusText(row.status) }}
                </el-tag>
              </template>
            </el-table-column>
            <el-table-column prop="executed_at" label="执行时间" width="170">
              <template #default="{ row }">{{ formatTime(row.executed_at) }}</template>
            </el-table-column>
            <el-table-column label="操作" fixed="right" width="180">
              <template #default="{ row }">
                <el-button
                  v-if="row.status === 'pending'"
                  type="success"
                  size="small"
                  @click="executeSignal(row)"
                >
                  执行
                </el-button>
                <el-button
                  v-if="row.status === 'pending'"
                  type="warning"
                  size="small"
                  @click="cancelSignal(row)"
                >
                  取消
                </el-button>
                <el-button
                  v-if="row.status !== 'pending'"
                  type="info"
                  size="small"
                  disabled
                >
                  已完成
                </el-button>
              </template>
            </el-table-column>
          </el-table>

          <el-empty v-if="!loading && signals.length === 0" description="暂无交易信号" />
        </div>
      </div>
    </el-main>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, watch } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { Refresh, Download } from '@element-plus/icons-vue'
import { useAccountStore } from '../stores/account'
import NavBar from '../components/NavBar.vue'
import ManualOrderPanel from '../components/ManualOrderPanel.vue'
import { exportTable as doExport } from '@/utils/exportHelper'

const accountStore = useAccountStore()
const currentAccountId = computed(() => accountStore.currentAccountId)
const currentAccount = computed(() => accountStore.currentAccount)

const loading = ref(false)
const signals = ref([])
const filterType = ref('')
const monitoringRunning = ref(false)
const monitoringAccountIds = ref([])

// ============================================================
// 拖拽分割线
// ============================================================

const leftPanelWidth = ref(300)
const splitContainer = ref(null)

function startDrag(e) {
  document.body.style.cursor = 'col-resize'
  document.body.style.userSelect = 'none'

  const onMove = (e2) => {
    const container = splitContainer.value
    if (!container) return
    const rect = container.getBoundingClientRect()
    let newWidth = e2.clientX - rect.left
    newWidth = Math.max(260, Math.min(newWidth, rect.width - 400))
    leftPanelWidth.value = newWidth
  }

  const onUp = () => {
    document.body.style.cursor = ''
    document.body.style.userSelect = ''
    document.removeEventListener('mousemove', onMove)
    document.removeEventListener('mouseup', onUp)
  }

  document.addEventListener('mousemove', onMove)
  document.addEventListener('mouseup', onUp)
}

// ============================================================
// 信号列表（原有逻辑）
// ============================================================

const signalColumns = [
  { label: '生成时间', prop: 'created_at' },
  { label: '股票代码', prop: 'stock_code' },
  { label: '股票名称', prop: 'stock_name' },
  { label: '信号类型', prop: 'signal_type' },
  { label: '委托价', prop: 'price' },
  { label: '现价', prop: 'current_price' },
  { label: '数量', prop: 'target_quantity' },
  { label: '状态', prop: 'status' },
  { label: '执行时间', prop: 'executed_at' },
]

const handleExportSignals = (format) => {
  const data = signals.value.map(s => ({
    ...s,
    signal_type: getSignalText(s.signal_type),
    status: getStatusText(s.status),
    created_at: formatTime(s.created_at),
    executed_at: formatTime(s.executed_at),
    price: s.price != null ? '¥' + s.price.toFixed(2) : '-',
    current_price: s.current_price != null ? '¥' + s.current_price.toFixed(2) : '-',
  }))
  doExport(signalColumns, data, '交易信号', format)
}

const loadSignals = async () => {
  loading.value = true
  try {
    const url = filterType.value
      ? `/api/v1/ui/${currentAccountId.value}/signals?signal_type=${filterType.value}`
      : `/api/v1/ui/${currentAccountId.value}/signals`

    const response = await fetch(url)
    const data = await response.json()
    signals.value = data.signals || []
  } catch (error) {
    console.error('加载信号失败:', error)
    ElMessage.error('加载失败')
  } finally {
    loading.value = false
  }
}

const checkServiceStatus = async () => {
  try {
    const response = await fetch(`/api/v1/ui/${currentAccountId.value}/monitoring/status`)
    const data = await response.json()
    monitoringRunning.value = data.monitoring?.running || false
    monitoringAccountIds.value = data.monitoring?.account_ids || []
  } catch (error) {
    console.error('检查服务状态失败:', error)
  }
}

const executeSignal = async (row) => {
  try {
    // 确认是否以对手价立即成交
    const cpLabel = row.signal_type === 'buy' ? '卖一价' : '买一价'
    await ElMessageBox.confirm(
      `确认以${cpLabel}立即执行该信号？\n\n股票：${row.stock_name || row.stock_code}\n代码：${row.stock_code}\n委托价：¥${row.price?.toFixed(2)}\n数量：${row.target_quantity} 股`,
      `确认${row.signal_type === 'buy' ? '买入' : '卖出'}`,
      { type: 'warning' }
    )

    // TODO: 接入券商实盘后，此处先撤销原委托单，再以对手价重新提交委托
    // 当前 mock 模式直接以现价成交
    const response = await fetch(`/api/v1/ui/${currentAccountId.value}/signals/${row.id}/execute`, {
      method: 'POST'
    })

    if (response.ok) {
      ElMessage.success('信号已执行（模拟）')
      await loadSignals()
    }
  } catch (error) {
    if (error !== 'cancel') {
      console.error('执行信号失败:', error)
      ElMessage.error('执行失败')
    }
  }
}

const cancelSignal = async (row) => {
  try {
    const response = await fetch(`/api/v1/ui/${currentAccountId.value}/signals/${row.id}/cancel`, {
      method: 'POST'
    })

    if (response.ok) {
      ElMessage.success('信号已取消')
      await loadSignals()
    }
  } catch (error) {
    console.error('取消信号失败:', error)
    ElMessage.error('取消失败')
  }
}

const getSignalType = (type) => {
  const types = {
    'buy': 'danger',
    'sell_stop_loss': 'warning',
    'sell_take_profit': 'success'
  }
  return types[type] || 'info'
}

const getSignalText = (type) => {
  const texts = {
    'buy': '买入',
    'sell_stop_loss': '止损卖出',
    'sell_take_profit': '止盈卖出'
  }
  return texts[type] || type
}

const getStatusType = (status) => {
  const types = {
    'pending': 'warning',
    'executed': 'success',
    'cancelled': 'info'
  }
  return types[status] || 'info'
}

const getStatusText = (status) => {
  const texts = {
    'pending': '待处理',
    'executed': '已执行',
    'cancelled': '已取消'
  }
  return texts[status] || status
}

const formatTime = (t) => {
  if (!t) return '-'
  return t.split('.')[0].replace('T', ' ')
}

onMounted(async () => {
  await checkServiceStatus()
  await loadSignals()
})
</script>

<style scoped>
.layout-container {
  height: 100%;
  display: flex;
  flex-direction: column;
}

.main-content {
  padding: 20px;
}

.page-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 20px;
}

.page-header h2 {
  color: #303133;
  margin: 0;
}

.status-card {
  margin-bottom: 20px;
}

/* Split layout */
.split-container {
  display: flex;
  gap: 0;
  min-height: 400px;
}

.price-up {
  color: #f56c6c;
  font-weight: 500;
}
.price-down {
  color: #67c23a;
  font-weight: 500;
}
.text-muted {
  color: #c0c4cc;
}

/* 拖拽分割线 */
.splitter {
  width: 6px;
  cursor: col-resize;
  background: #ebeef5;
  display: flex;
  align-items: center;
  justify-content: center;
  flex-shrink: 0;
  transition: background 0.2s;
}

.splitter:hover {
  background: #dcdfe6;
}

.splitter-handle {
  width: 2px;
  height: 30px;
  border-left: 1px solid #c0c4cc;
  border-right: 1px solid #c0c4cc;
}

/* 右侧信号列表 */
.signals-panel {
  flex: 1;
  min-width: 400px;
  overflow-x: auto;
}

.filter-bar {
  margin-bottom: 12px;
}
</style>
