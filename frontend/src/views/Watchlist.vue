<template>
  <div class="layout-container">
    <NavBar />
    <el-main class="main-content">
      <div class="page-header">
        <h2>选股监控 - {{ currentAccount?.display_name }}</h2>
        <el-space>
          <el-button type="primary" @click="showStrategySelectDialog = true" :loading="screeningProgress.processing">
            <el-icon><Search /></el-icon>
            选股
          </el-button>
          <el-button type="danger" @click="confirmClearWatchlist" :disabled="watchlist.length === 0">
            <el-icon><Delete /></el-icon>
            清空已选
          </el-button>
          <el-button @click="loadWatchlist">
            <el-icon><Refresh /></el-icon>
            刷新
          </el-button>
        </el-space>
      </div>

      <!-- 选股进度 -->
      <el-card v-if="screeningProgress.processing" class="progress-card">
        <el-progress
          :percentage="screeningProgress.percent"
          :status="screeningProgress.status"
          :stroke-width="20"
          :format="formatProgress"
        />
        <div class="progress-details">
          <span>已处理：{{ screeningProgress.processed }}/{{ screeningProgress.total }}</span>
          <span>已匹配：{{ screeningProgress.matched }} 只</span>
          <span v-if="screeningProgress.currentStock">当前：{{ screeningProgress.currentStock }}</span>
          <span v-if="screeningProgress.estimatedRemaining">预计剩余：{{ formatTime(screeningProgress.estimatedRemaining) }}</span>
        </div>
      </el-card>

      <!-- 本地数据状态 -->
      <el-card class="status-card">
        <el-descriptions :column="5" border>
          <el-descriptions-item label="本地股票数">
            {{ dataStats.total_stocks || 0 }}
          </el-descriptions-item>
          <el-descriptions-item label="K 线数据量">
            {{ dataStats.total_records || 0 }}
          </el-descriptions-item>
          <el-descriptions-item label="最新日期">
            {{ dataStats.latest_date || '无数据' }}
          </el-descriptions-item>
          <el-descriptions-item label="最早日期">
            {{ dataStats.earliest_date || '无数据' }}
          </el-descriptions-item>
          <el-descriptions-item label="数据源">
            <el-tag :type="dataStats.total_stocks > 0 ? 'success' : 'warning'" size="small">
              {{ dataStats.total_stocks > 0 ? '本地' : 'SDK' }}
            </el-tag>
          </el-descriptions-item>
        </el-descriptions>
      </el-card>

      <!-- 服务状态 -->
      <el-card class="status-card">
        <el-descriptions :column="3" border>
          <el-descriptions-item label="监控服务">
            <el-tag :type="monitoringRunning ? 'success' : 'info'" size="small">
              {{ monitoringRunning ? '运行中' : '已停止' }}
            </el-tag>
          </el-descriptions-item>
          <el-descriptions-item label="Watchlist 数量">
            {{ watchlist.length }}
          </el-descriptions-item>
          <el-descriptions-item label="待处理">
            {{ watchlist.filter(w => w.status === 'pending').length }}
          </el-descriptions-item>
        </el-descriptions>
      </el-card>

      <!-- Watchlist 列表 -->
      <el-card>
        <template #header>
          <div class="card-header">
            <span>候选股票池</span>
            <el-radio-group v-model="filterStatus" size="small" @change="loadWatchlist">
              <el-radio-button label="">全部</el-radio-button>
              <el-radio-button label="pending">待观察</el-radio-button>
              <el-radio-button label="watching">观察中</el-radio-button>
              <el-radio-button label="bought">已买入</el-radio-button>
              <el-radio-button label="sold">已卖出</el-radio-button>
            </el-radio-group>
          </div>
        </template>

        <el-table :data="watchlist" stripe style="width: 100%" v-loading="loading">
          <el-table-column prop="stock_code" label="股票代码" width="120" />
          <el-table-column prop="stock_name" label="股票名称" width="120" />
          <el-table-column prop="reason" label="入选原因" min-width="180" show-overflow-tooltip />
          <el-table-column prop="buy_price" label="买入价" width="90" align="right">
            <template #default="{ row }">¥{{ row.buy_price?.toFixed(2) }}</template>
          </el-table-column>
          <el-table-column prop="stop_loss_price" label="止损价" width="90" align="right">
            <template #default="{ row }">¥{{ row.stop_loss_price?.toFixed(2) }}</template>
          </el-table-column>
          <el-table-column prop="take_profit_price" label="止盈价" width="90" align="right">
            <template #default="{ row }">¥{{ row.take_profit_price?.toFixed(2) }}</template>
          </el-table-column>
          <el-table-column prop="target_quantity" label="数量" width="80" align="right" />
          <el-table-column prop="status" label="状态" width="90">
            <template #default="{ row }">
              <el-tag :type="getStatusType(row.status)" size="small">
                {{ getStatusText(row.status) }}
              </el-tag>
            </template>
          </el-table-column>
          <el-table-column prop="created_at" label="入选时间" width="160" />
          <el-table-column label="操作" fixed="right" width="200">
            <template #default="{ row }">
              <el-button type="primary" size="small" @click="editStock(row)">
                编辑
              </el-button>
              <el-button type="danger" size="small" @click="removeStock(row)">
                移除
              </el-button>
            </template>
          </el-table-column>
        </el-table>

        <el-empty v-if="!loading && watchlist.length === 0" description="暂无候选股票，运行选股服务添加股票" />
      </el-card>

      <!-- 清空确认对话框 -->
      <el-dialog v-model="showClearDialog" title="确认清空" width="400px">
        <el-alert
          :title="`确定要清空所有已选股票吗？（共 ${watchlist.length} 只）`"
          description="此操作不可恢复，清空后需要重新选股添加"
          type="warning"
          :closable="false"
        />
        <template #footer>
          <el-button @click="showClearDialog = false">取消</el-button>
          <el-button type="danger" @click="clearWatchlist" :loading="clearing">
            {{ clearing ? '清空中...' : '确认清空' }}
          </el-button>
        </template>
      </el-dialog>

      <!-- 策略选择对话框 -->
      <el-dialog v-model="showStrategySelectDialog" title="选择选股策略" width="500px">
        <el-alert
          title="选择策略进行选股"
          description="选股结果将暂存到候选列表，经确认后才会添加到 watchlist"
          type="info"
          :closable="false"
          style="margin-bottom: 20px"
        />

        <el-form :model="strategySelectForm" label-width="120px">
          <el-form-item label="选择策略" required>
            <el-select v-model="strategySelectForm.strategyId" placeholder="请选择策略" style="width: 100%">
              <el-option
                v-for="s in strategies"
                :key="s.id"
                :label="`${s.name} (${s.status === 'active' ? '激活' : '停用'})`"
                :value="s.id"
                :disabled="s.status !== 'active'"
              />
            </el-select>
          </el-form-item>

          <el-form-item label="数据源">
            <el-radio-group v-model="strategySelectForm.useLocal">
              <el-radio :label="true">本地数据（快）</el-radio>
              <el-radio :label="false">SDK 实时（慢）</el-radio>
            </el-radio-group>
          </el-form-item>

          <el-divider />

          <div style="font-size: 13px; color: #606266;">
            <p style="margin: 0 0 8px 0;"><strong>当前激活策略：</strong></p>
            <ul style="margin: 0; padding-left: 20px;">
              <li v-for="s in activeStrategies" :key="s.id">
                <strong>{{ s.name }}</strong> - {{ s.description || '无描述' }}
              </li>
              <li v-if="activeStrategies.length === 0">暂无激活策略，请先在策略管理页面激活策略</li>
            </ul>
          </div>
        </el-form>

        <template #footer>
          <el-button @click="showStrategySelectDialog = false">取消</el-button>
          <el-button type="primary" @click="runScreeningWithStrategy" :loading="screeningProgress.processing">
            开始选股
          </el-button>
        </template>
      </el-dialog>

      <!-- 临时候选确认对话框 -->
      <el-dialog v-model="showCandidatesDialog" title="确认候选股票" width="700px">
        <el-alert
          :title="`发现 ${candidates.length} 只候选股票，请确认是否加入 watchlist`"
          :description="`已选 ${selectedCandidates.length} 只`"
          type="info"
          :closable="false"
          style="margin-bottom: 20px"
        />

        <el-table :data="candidates" stripe style="width: 100%" @selection-change="handleSelectionChange">
          <el-table-column type="selection" width="50" />
          <el-table-column prop="stock_code" label="股票代码" width="120" />
          <el-table-column prop="stock_name" label="股票名称" width="120" />
          <el-table-column prop="reason" label="入选原因" min-width="150" show-overflow-tooltip />
          <el-table-column prop="buy_price" label="买入价" width="80" align="right">
            <template #default="{ row }">¥{{ row.buy_price?.toFixed(2) }}</template>
          </el-table-column>
          <el-table-column prop="match_score" label="匹配度" width="80" align="right">
            <template #default="{ row }">{{ (row.match_score * 100).toFixed(0) }}%</template>
          </el-table-column>
          <el-table-column prop="created_at" label="时间" width="160" />
        </el-table>

        <template #footer>
          <el-button @click="cancelCandidates">取消</el-button>
          <el-button type="danger" @click="rejectCandidates(false)" :loading="confirming">
            拒绝未选
          </el-button>
          <el-button type="warning" @click="rejectCandidates(true)" :loading="confirming">
            全部拒绝
          </el-button>
          <el-button type="primary" @click="confirmCandidates" :loading="confirming">
            确认已选 ({{ selectedCandidates.length }})
          </el-button>
        </template>
      </el-dialog>

      <!-- 编辑对话框 -->
      <el-dialog v-model="showEditDialog" title="编辑股票" width="500px">
        <el-form :model="editingStock" label-width="100px">
          <el-form-item label="股票代码">
            <el-input v-model="editingStock.stock_code" disabled />
          </el-form-item>
          <el-form-item label="股票名称">
            <el-input v-model="editingStock.stock_name" />
          </el-form-item>
          <el-form-item label="买入价格">
            <el-input-number v-model="editingStock.buy_price" :precision="2" :step="0.1" />
          </el-form-item>
          <el-form-item label="止损价格">
            <el-input-number v-model="editingStock.stop_loss_price" :precision="2" :step="0.1" />
          </el-form-item>
          <el-form-item label="止盈价格">
            <el-input-number v-model="editingStock.take_profit_price" :precision="2" :step="0.1" />
          </el-form-item>
          <el-form-item label="目标数量">
            <el-input-number v-model="editingStock.target_quantity" :min="100" :step="100" />
          </el-form-item>
          <el-form-item label="状态">
            <el-select v-model="editingStock.status">
              <el-option label="待观察" value="pending" />
              <el-option label="观察中" value="watching" />
              <el-option label="已买入" value="bought" />
              <el-option label="已卖出" value="sold" />
              <el-option label="已忽略" value="ignored" />
            </el-select>
          </el-form-item>
        </el-form>
        <template #footer>
          <el-button @click="showEditDialog = false">取消</el-button>
          <el-button type="primary" @click="saveStock">保存</el-button>
        </template>
      </el-dialog>
    </el-main>
  </div>
</template>

<script setup>
import { ref, reactive, onMounted, computed } from 'vue'
import { ElMessage } from 'element-plus'
import { Search, Refresh, Delete } from '@element-plus/icons-vue'
import { useAccountStore } from '../stores/account'
import NavBar from '../components/NavBar.vue'

const accountStore = useAccountStore()
const currentAccountId = computed(() => accountStore.currentAccountId)
const currentAccount = computed(() => accountStore.currentAccount)

const loading = ref(false)
const watchlist = ref([])
const filterStatus = ref('')
const monitoringRunning = ref(false)
const showEditDialog = ref(false)
const showClearDialog = ref(false)
const showStrategySelectDialog = ref(false)
const showCandidatesDialog = ref(false)
const clearing = ref(false)
const confirming = ref(false)
const strategySelectForm = reactive({
  strategyId: null,
  useLocal: true
})

const strategies = ref([])
const candidates = ref([])
const selectedCandidates = ref([])
const dataStats = ref({
  total_stocks: 0,
  total_records: 0,
  latest_date: null,
  earliest_date: null
})
const screeningProgress = reactive({
  processing: false,
  total: 0,
  processed: 0,
  matched: 0,
  currentStock: '',
  estimatedRemaining: 0,
  percent: 0,
  status: ''
})

let progressPollingTimer = null

// 格式化进度显示
const formatProgress = (percent) => `${percent}%`

// 格式化时间
const formatTime = (seconds) => {
  if (seconds < 60) return `${seconds}秒`
  const mins = Math.floor(seconds / 60)
  const secs = seconds % 60
  return `${mins}分${secs}秒`
}

// 轮询选股进度
const pollProgress = async () => {
  try {
    const response = await fetch(`/api/v1/ui/${currentAccountId.value}/screening/status`)
    const data = await response.json()

    if (data.progress) {
      const progress = data.progress
      screeningProgress.total = progress.total_stocks || 0
      screeningProgress.processed = progress.processed || 0
      screeningProgress.matched = progress.matched || 0
      screeningProgress.currentStock = progress.current_stock || ''
      screeningProgress.estimatedRemaining = progress.estimated_remaining || 0

      if (screeningProgress.total > 0) {
        screeningProgress.percent = Math.round((screeningProgress.processed / screeningProgress.total) * 100)
      }

      // 选股完成的判断：phase 为 done 或者已处理数量等于总数
      const isCompleted = progress.current_phase === 'done' ||
                          (progress.total_stocks > 0 && progress.processed >= progress.total_stocks)

      if (isCompleted) {
        screeningProgress.processing = false
        screeningProgress.status = 'success'
        if (progressPollingTimer) {
          clearInterval(progressPollingTimer)
          progressPollingTimer = null
        }
        // 选股完成，检查临时候选
        await checkTempCandidates()
        if (candidates.value.length === 0) {
          // 如果没有候选，直接刷新列表
          await loadWatchlist()
          ElMessage.success(`选股完成，共匹配 ${screeningProgress.matched} 只股票`)
        }
      } else if (progress.current_phase === 'scanning') {
        screeningProgress.status = ''
        screeningProgress.processing = true
      }
    }
  } catch (error) {
    console.error('获取进度失败:', error)
  }
}

const editingStock = reactive({
  stock_code: '',
  stock_name: '',
  buy_price: 0,
  stop_loss_price: 0,
  take_profit_price: 0,
  target_quantity: 100,
  status: 'pending'
})

// 加载策略列表
const loadStrategies = async () => {
  try {
    const response = await fetch(`/api/v1/ui/${currentAccountId.value}/strategies`)
    const data = await response.json()
    strategies.value = data.strategies || []
  } catch (error) {
    console.error('加载策略失败:', error)
  }
}

// 计算激活的策略
const activeStrategies = computed(() => {
  return strategies.value.filter(s => s.status === 'active')
})

// 显示策略选择对话框
const showStrategySelect = () => {
  loadStrategies()
  showStrategySelectDialog.value = true
}

// 带策略选择的选股
const runScreeningWithStrategy = async () => {
  if (!strategySelectForm.strategyId) {
    ElMessage.warning('请选择策略')
    return
  }

  screeningProgress.processing = true
  screeningProgress.status = ''
  screeningProgress.percent = 0
  screeningProgress.processed = 0
  screeningProgress.total = 0
  screeningProgress.matched = 0
  showStrategySelectDialog.value = false

  try {
    const response = await fetch(`/api/v1/ui/${currentAccountId.value}/screening/run`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        strategy_id: strategySelectForm.strategyId,
        use_local: strategySelectForm.useLocal,
        pending_to_temp: true  // 暂存到临时表
      })
    })
    const data = await response.json()

    if (data.success) {
      // 开始轮询进度
      progressPollingTimer = setInterval(pollProgress, 2000)
    } else {
      ElMessage.error(data.message || '选股失败')
      screeningProgress.processing = false
    }
  } catch (error) {
    console.error('选股失败:', error)
    ElMessage.error('选股失败')
    screeningProgress.processing = false
  }
}

// 检查临时候选股票
const checkTempCandidates = async () => {
  try {
    const response = await fetch(`/api/v1/ui/${currentAccountId.value}/candidates`)
    const data = await response.json()
    if (data.candidates && data.candidates.length > 0) {
      candidates.value = data.candidates
      showCandidatesDialog.value = true
    }
  } catch (error) {
    console.error('加载候选股票失败:', error)
  }
}

// 处理选择变化
const handleSelectionChange = (selection) => {
  selectedCandidates.value = selection.map(s => s.stock_code)
}

// 确认候选股票
const confirmCandidates = async () => {
  confirming.value = true
  try {
    const response = await fetch(`/api/v1/ui/${currentAccountId.value}/candidates/confirm`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        stock_codes: selectedCandidates.value.length > 0 ? selectedCandidates.value : null,
        confirm: true
      })
    })
    const data = await response.json()

    if (data.success) {
      ElMessage.success(`已确认 ${data.confirmed} 只股票`)
      showCandidatesDialog.value = false
      await loadWatchlist()
    }
  } catch (error) {
    console.error('确认失败:', error)
    ElMessage.error('确认失败')
  } finally {
    confirming.value = false
  }
}

// 拒绝候选股票
const rejectCandidates = async (rejectAll) => {
  confirming.value = true
  try {
    const response = await fetch(`/api/v1/ui/${currentAccountId.value}/candidates/confirm`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        stock_codes: rejectAll ? null : selectedCandidates.value,
        confirm: false
      })
    })
    const data = await response.json()

    if (data.success) {
      ElMessage.success(`已拒绝 ${data.rejected} 只股票`)
      showCandidatesDialog.value = false
    }
  } catch (error) {
    console.error('拒绝失败:', error)
    ElMessage.error('拒绝失败')
  } finally {
    confirming.value = false
  }
}

// 取消候选对话框
const cancelCandidates = () => {
  showCandidatesDialog.value = false
  screeningProgress.processing = false
  screeningProgress.status = ''
}

// 清空 watchlist
const clearWatchlist = async () => {
  clearing.value = true
  try {
    const response = await fetch(`/api/v1/ui/${currentAccountId.value}/watchlist/clear`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' }
    })
    const data = await response.json()

    if (data.success) {
      ElMessage.success('已清空所有已选股票')
      showClearDialog.value = false
      await loadWatchlist()
    } else {
      ElMessage.error(data.message || '清空失败')
    }
  } catch (error) {
    console.error('清空失败:', error)
    ElMessage.error('清空失败')
  } finally {
    clearing.value = false
  }
}

// 加载数据 stats
const loadDataStats = async () => {
  try {
    const response = await fetch(`/api/v1/ui/${currentAccountId.value}/data/stats`)
    const data = await response.json()
    if (data.success) {
      dataStats.value = data.stats
    }
  } catch (error) {
    console.error('加载数据统计失败:', error)
  }
}

// 加载 watchlist
const loadWatchlist = async () => {
  loading.value = true
  try {
    const url = filterStatus.value
      ? `/api/v1/ui/${currentAccountId.value}/watchlist?status=${filterStatus.value}`
      : `/api/v1/ui/${currentAccountId.value}/watchlist`

    const response = await fetch(url)
    const data = await response.json()
    watchlist.value = data.watchlist || []
  } catch (error) {
    console.error('加载 watchlist 失败:', error)
    ElMessage.error('加载失败')
  } finally {
    loading.value = false
  }
}

// 确认清空 watchlist
const confirmClearWatchlist = () => {
  showClearDialog.value = true
}

// 检查服务状态
const checkServiceStatus = async () => {
  try {
    const monitoringRes = await fetch(`/api/v1/ui/${currentAccountId.value}/monitoring/status`)
    const monitoringData = await monitoringRes.json()
    monitoringRunning.value = monitoringData.monitoring?.running || false

    // 如果选股服务正在运行，开始轮询进度
    if (screeningData.screening?.running && !screeningProgress.processing) {
      screeningProgress.processing = true
      progressPollingTimer = setInterval(pollProgress, 2000)
    }
  } catch (error) {
    console.error('检查服务状态失败:', error)
  }
}

// 编辑股票
const editStock = (row) => {
  Object.assign(editingStock, row)
  showEditDialog.value = true
}

// 保存股票
const saveStock = async () => {
  try {
    const response = await fetch(`/api/v1/ui/${currentAccountId.value}/watchlist/${editingStock.stock_code}/prices`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        buy_price: editingStock.buy_price,
        stop_loss_price: editingStock.stop_loss_price,
        take_profit_price: editingStock.take_profit_price,
        target_quantity: editingStock.target_quantity
      })
    })

    if (response.ok) {
      // 更新状态
      await fetch(`/api/v1/ui/${currentAccountId.value}/watchlist/${editingStock.stock_code}/status`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ status: editingStock.status })
      })

      ElMessage.success('保存成功')
      showEditDialog.value = false
      await loadWatchlist()
    }
  } catch (error) {
    console.error('保存失败:', error)
    ElMessage.error('保存失败')
  }
}

// 移除股票
const removeStock = async (row) => {
  try {
    const response = await fetch(`/api/v1/ui/${currentAccountId.value}/watchlist/${row.stock_code}`, {
      method: 'DELETE'
    })

    if (response.ok) {
      ElMessage.success('已移除')
      await loadWatchlist()
    }
  } catch (error) {
    console.error('移除失败:', error)
    ElMessage.error('移除失败')
  }
}

const getStatusType = (status) => {
  const types = {
    'pending': 'info',
    'watching': 'warning',
    'bought': 'success',
    'sold': 'success',
    'ignored': 'info'
  }
  return types[status] || 'info'
}

const getStatusText = (status) => {
  const texts = {
    'pending': '待观察',
    'watching': '观察中',
    'bought': '已买入',
    'sold': '已卖出',
    'ignored': '已忽略'
  }
  return texts[status] || status
}

// 组件卸载时清理定时器
import { onUnmounted } from 'vue'
onUnmounted(() => {
  if (progressPollingTimer) {
    clearInterval(progressPollingTimer)
    progressPollingTimer = null
  }
})

onMounted(async () => {
  await checkServiceStatus()
  await loadDataStats()
  await loadWatchlist()
  await loadStrategies()
  // 检查是否有待确认的候选股票
  await checkTempCandidates()
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

.progress-card {
  margin-bottom: 20px;
}

.progress-details {
  display: flex;
  justify-content: space-between;
  margin-top: 10px;
  font-size: 13px;
  color: #606266;
}
</style>
