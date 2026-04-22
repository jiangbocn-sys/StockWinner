<template>
  <div class="layout-container">
    <NavBar />
    <el-main class="main-content">
      <div class="page-header">
        <h2>交易信号 - {{ currentAccount?.display_name }}</h2>
        <el-space>
          <el-button :type="monitoringRunning ? 'danger' : 'success'" @click="toggleMonitoring">
            <el-icon><VideoPlay /></el-icon>
            {{ monitoringRunning ? '停止监控' : '启动监控' }}
          </el-button>
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
              {{ monitoringRunning ? '运行中' : '已停止' }}
            </el-tag>
          </el-descriptions-item>
          <el-descriptions-item label="待处理信号">
            {{ signals.filter(s => s.status === 'pending').length }}
          </el-descriptions-item>
          <el-descriptions-item label="已执行">
            {{ signals.filter(s => s.status === 'executed').length }}
          </el-descriptions-item>
        </el-descriptions>
      </el-card>

      <!-- 信号类型筛选 -->
      <el-card>
        <template #header>
          <div class="card-header">
            <span>交易信号列表</span>
            <el-radio-group v-model="filterType" size="small" @change="loadSignals">
              <el-radio-button label="">全部</el-radio-button>
              <el-radio-button label="buy">买入</el-radio-button>
              <el-radio-button label="sell_stop_loss">止损卖出</el-radio-button>
              <el-radio-button label="sell_take_profit">止盈卖出</el-radio-button>
            </el-radio-group>
          </div>
        </template>

        <el-table :data="signals" stripe style="width: 100%" v-loading="loading">
          <el-table-column prop="created_at" label="生成时间" width="160" />
          <el-table-column prop="stock_code" label="股票代码" width="120" />
          <el-table-column prop="stock_name" label="股票名称" width="120" />
          <el-table-column prop="signal_type" label="信号类型" width="120">
            <template #default="{ row }">
              <el-tag :type="getSignalType(row.signal_type)" size="small">
                {{ getSignalText(row.signal_type) }}
              </el-tag>
            </template>
          </el-table-column>
          <el-table-column prop="price" label="信号价格" width="100" align="right">
            <template #default="{ row }">¥{{ row.price?.toFixed(2) }}</template>
          </el-table-column>
          <el-table-column prop="target_quantity" label="数量" width="80" align="right" />
          <el-table-column prop="status" label="状态" width="90">
            <template #default="{ row }">
              <el-tag :type="getStatusType(row.status)" size="small">
                {{ getStatusText(row.status) }}
              </el-tag>
            </template>
          </el-table-column>
          <el-table-column prop="executed_at" label="执行时间" width="160" />
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
      </el-card>
    </el-main>
  </div>
</template>

<script setup>
import { ref, onMounted, computed } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { useAccountStore } from '../stores/account'
import NavBar from '../components/NavBar.vue'

const accountStore = useAccountStore()
const currentAccountId = computed(() => accountStore.currentAccountId)
const currentAccount = computed(() => accountStore.currentAccount)

const loading = ref(false)
const signals = ref([])
const filterType = ref('')
const monitoringRunning = ref(false)

// 加载交易信号
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

// 检查监控服务状态
const checkServiceStatus = async () => {
  try {
    const response = await fetch(`/api/v1/ui/${currentAccountId.value}/monitoring/status`)
    const data = await response.json()
    monitoringRunning.value = data.monitoring?.running || false
  } catch (error) {
    console.error('检查服务状态失败:', error)
  }
}

// 切换监控服务
const toggleMonitoring = async () => {
  try {
    const url = `/api/v1/ui/${currentAccountId.value}/monitoring/${monitoringRunning.value ? 'stop' : 'start'}`
    const response = await fetch(url, { method: 'POST' })
    const data = await response.json()

    if (data.success) {
      ElMessage.success(monitoringRunning.value ? '监控服务已停止' : '监控服务已启动')
      monitoringRunning.value = !monitoringRunning.value
    } else {
      ElMessage.error(data.message || '操作失败')
    }
  } catch (error) {
    console.error('切换监控服务失败:', error)
    ElMessage.error('操作失败')
  }
}

// 执行信号
const executeSignal = async (row) => {
  try {
    await ElMessageBox.confirm('确认执行该交易信号？', '确认执行', { type: 'warning' })

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

// 取消信号
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

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
}
</style>
