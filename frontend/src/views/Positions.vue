<template>
  <div class="layout-container">
    <NavBar />
    <el-main class="main-content">
      <h2>持仓分析 - {{ currentAccount?.display_name }}</h2>

      <!-- 总体概览 -->
      <el-card class="overview-card">
        <el-descriptions :column="4" border>
          <el-descriptions-item label="总资产">¥{{ formatNumber(totalAssets) }}</el-descriptions-item>
          <el-descriptions-item label="可用资金">¥{{ formatNumber(availableCash) }}</el-descriptions-item>
          <el-descriptions-item label="持仓市值">¥{{ formatNumber(marketValue) }}</el-descriptions-item>
          <el-descriptions-item label="总盈亏">
            <span :class="totalPnl >= 0 ? 'profit-positive' : 'profit-negative'">
              {{ totalPnl >= 0 ? '+' : '' }}¥{{ formatNumber(Math.abs(totalPnl)) }} ({{ pnlPercent }}%)
            </span>
          </el-descriptions-item>
        </el-descriptions>
      </el-card>

      <!-- 持仓明细 -->
      <el-card>
        <template #header>
          <div class="card-header">
            <span>持仓明细</span>
            <el-button type="primary" size="small" @click="refreshPrices" :loading="refreshing">
              <el-icon><Refresh /></el-icon>
              刷新行情
            </el-button>
          </div>
        </template>

        <el-table :data="positions" stripe style="width: 100%">
          <el-table-column prop="stock_code" label="股票代码" width="100" />
          <el-table-column prop="stock_name" label="股票名称" width="120" />
          <el-table-column prop="quantity" label="数量" width="100" align="right" />
          <el-table-column prop="avg_cost" label="成本价" width="100" align="right">
            <template #default="{ row }">¥{{ row.avg_cost }}</template>
          </el-table-column>
          <el-table-column prop="current_price" label="当前价" width="100" align="right">
            <template #default="{ row }">¥{{ row.current_price }}</template>
          </el-table-column>
          <el-table-column prop="market_value" label="市值" width="120" align="right">
            <template #default="{ row }">¥{{ formatNumber(row.market_value) }}</template>
          </el-table-column>
          <el-table-column prop="profit_loss" label="盈亏" width="120" align="right">
            <template #default="{ row }">
              <span :class="row.profit_loss >= 0 ? 'profit-positive' : 'profit-negative'">
                {{ row.profit_loss >= 0 ? '+' : '' }}¥{{ formatNumber(Math.abs(row.profit_loss)) }}
              </span>
            </template>
          </el-table-column>
          <el-table-column prop="profit_percent" label="盈亏%" width="100" align="right">
            <template #default="{ row }">
              <span :class="row.profit_loss >= 0 ? 'profit-positive' : 'profit-negative'">
                {{ ((row.profit_loss / (row.avg_cost * row.quantity)) * 100).toFixed(2) }}%
              </span>
            </template>
          </el-table-column>
          <el-table-column label="操作" fixed="right" width="280">
            <template #default="{ row }">
              <el-button type="info" size="small" @click="handleDsaAnalysis(row)" :loading="dsaAnalyzing === row.stock_code">DSA 分析</el-button>
              <el-button type="primary" size="small" @click="handleAction(row, 'add')">加仓</el-button>
              <el-button type="warning" size="small" @click="handleAction(row, 'reduce')">减仓</el-button>
              <el-button type="danger" size="small" @click="handleAction(row, 'clear')">清仓</el-button>
            </template>
          </el-table-column>
        </el-table>
      </el-card>

      <!-- DSA 分析结果弹窗 -->
      <el-dialog
        v-model="dsaDialogVisible"
        :title="`DSA 分析 - ${dsaStock.stock_name}(${dsaStock.stock_code})`"
        width="700px"
        :close-on-click-modal="false"
        :close-on-press-escape="false"
        :show-close="false"
      >
        <div v-if="dsaAnalyzing" class="dsa-loading">
          <el-icon class="is-loading" size="40"><Loading /></el-icon>
          <p>正在分析中，请稍候...</p>
        </div>
        <div v-else-if="dsaResult" class="dsa-content">
          <el-descriptions :column="2" border size="small" class="mb-16">
            <el-descriptions-item label="当前价">¥{{ dsaResult.meta?.current_price || '-' }}</el-descriptions-item>
            <el-descriptions-item label="涨跌幅">{{ dsaResult.meta?.change_pct?.toFixed(2) || '-' }}%</el-descriptions-item>
          </el-descriptions>

          <h4 class="section-title">市场情绪</h4>
          <el-tag :type="dsaResult.summary?.sentiment_label === '看多' ? 'danger' : dsaResult.summary?.sentiment_label === '看空' ? 'success' : 'info'" size="large">
            {{ dsaResult.summary?.sentiment_label || '-' }}
          </el-tag>
          <el-tag v-if="dsaResult.summary?.sentiment_score" size="small" style="margin-left: 8px">
            评分: {{ dsaResult.summary.sentiment_score }}
          </el-tag>

          <h4 class="section-title">分析摘要</h4>
          <div class="analysis-text">{{ dsaResult.summary?.analysis_summary || '暂无分析' }}</div>

          <h4 class="section-title">操作建议</h4>
          <div class="analysis-text">{{ dsaResult.summary?.operation_advice || '暂无建议' }}</div>

          <h4 class="section-title">交易参考</h4>
          <el-descriptions :column="3" border size="small">
            <el-descriptions-item label="理想买入">¥{{ dsaResult.strategy?.ideal_buy || '-' }}</el-descriptions-item>
            <el-descriptions-item label="止损价">¥{{ dsaResult.strategy?.stop_loss || '-' }}</el-descriptions-item>
            <el-descriptions-item label="止盈价">¥{{ dsaResult.strategy?.take_profit || '-' }}</el-descriptions-item>
          </el-descriptions>
        </div>
        <div v-else-if="dsaError" class="dsa-error">
          <el-alert :title="dsaError" type="error" :closable="false" />
        </div>
        <template #footer>
          <el-button type="primary" @click="dsaDialogVisible = false">关闭</el-button>
        </template>
      </el-dialog>
    </el-main>
  </div>
</template>

<script setup>
import { ref, onMounted, computed } from 'vue'
import { ElMessage } from 'element-plus'
import { useAccountStore } from '../stores/account'
import NavBar from '../components/NavBar.vue'

const accountStore = useAccountStore()
const currentAccount = computed(() => accountStore.currentAccount)
const currentAccountId = computed(() => accountStore.currentAccountId)

const positions = ref([])
const totalAssets = ref(0)
const availableCash = ref(0)
const marketValue = ref(0)
const totalPnl = ref(0)
const pnlPercent = ref(0)

// DSA 分析状态
const dsaDialogVisible = ref(false)
const dsaAnalyzing = ref(false)
const dsaStock = ref({ stock_code: '', stock_name: '' })
const dsaResult = ref(null)
const dsaError = ref('')

const refreshing = ref(false)

const loadPositions = async () => {
  // 先从 DB 加载数据（不等待行情刷新）
  try {
    const response = await fetch(`/api/v1/ui/${currentAccountId.value}/positions`)
    const data = await response.json()
    positions.value = data.positions || []
    availableCash.value = data.available_cash || 0
    marketValue.value = positions.value.reduce((sum, p) => sum + (p.market_value || 0), 0)
    totalPnl.value = positions.value.reduce((sum, p) => sum + (p.profit_loss || 0), 0)
    totalAssets.value = marketValue.value + availableCash.value
    pnlPercent.value = (totalPnl.value / (totalAssets.value - availableCash.value)) * 100 || 0
  } catch (error) {
    console.error('加载持仓失败:', error)
  }
}

const refreshPrices = async () => {
  refreshing.value = true
  try {
    const response = await fetch(`/api/v1/ui/${currentAccountId.value}/positions/refresh-prices`, { method: 'POST' })
    const data = await response.json()

    positions.value = data.positions || []
    availableCash.value = data.available_cash || 0

    marketValue.value = positions.value.reduce((sum, p) => sum + (p.market_value || 0), 0)
    totalPnl.value = positions.value.reduce((sum, p) => sum + (p.profit_loss || 0), 0)
    totalAssets.value = marketValue.value + availableCash.value
    pnlPercent.value = (totalPnl.value / (totalAssets.value - availableCash.value)) * 100 || 0

    ElMessage.success('行情已刷新')
  } catch (error) {
    console.error('刷新行情失败:', error)
    ElMessage.error('刷新行情失败')
  } finally {
    refreshing.value = false
  }
}

const formatNumber = (num) => {
  return Number(num || 0).toLocaleString('zh-CN', { minimumFractionDigits: 2 })
}

const handleAction = (row, action) => {
  console.log('操作:', action, row.stock_code)
  // TODO: 实现交易操作
}

const handleDsaAnalysis = async (row) => {
  dsaStock.value = { stock_code: row.stock_code, stock_name: row.stock_name }
  dsaDialogVisible.value = true
  dsaAnalyzing.value = true
  dsaResult.value = null
  dsaError.value = ''

  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/positions/${row.stock_code}/dsa-analyze`, {
      method: 'POST',
    })
    const data = await res.json()

    if (!res.ok) {
      if (data.code === 409) {
        dsaError.value = data.message
        return
      }
      dsaError.value = data.detail || '分析失败'
      return
    }

    dsaResult.value = data
  } catch (e) {
    dsaError.value = '请求失败，请检查网络连接'
  } finally {
    dsaAnalyzing.value = false
  }
}

onMounted(async () => {
  // 先展示 DB 数据
  await loadPositions()
  // 后台静默刷新实时行情
  refreshPrices()
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

h2 {
  margin-bottom: 20px;
  color: #303133;
}

.overview-card {
  margin-bottom: 20px;
}

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.profit-positive {
  color: #f56c6c;
  font-weight: bold;
}

.profit-negative {
  color: #67c23a;
  font-weight: bold;
}

.dsa-loading {
  text-align: center;
  padding: 40px 0;
}

.dsa-loading p {
  margin-top: 16px;
  color: #909399;
}

.dsa-content .section-title {
  margin: 16px 0 8px;
  font-size: 14px;
  color: #303133;
  border-left: 3px solid #409EFF;
  padding-left: 8px;
}

.dsa-content .analysis-text {
  background: #f5f7fa;
  padding: 12px;
  border-radius: 4px;
  font-size: 13px;
  line-height: 1.6;
  color: #606266;
  white-space: pre-wrap;
}

.dsa-content .mb-16 {
  margin-bottom: 16px;
}

.dsa-error {
  padding: 20px 0;
}
</style>
