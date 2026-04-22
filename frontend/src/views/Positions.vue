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
            <el-button type="primary" size="small" @click="loadPositions">
              <el-icon><Refresh /></el-icon>
              刷新
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
          <el-table-column label="操作" fixed="right" width="200">
            <template #default="{ row }">
              <el-button type="primary" size="small" @click="handleAction(row, 'add')">加仓</el-button>
              <el-button type="warning" size="small" @click="handleAction(row, 'reduce')">减仓</el-button>
              <el-button type="danger" size="small" @click="handleAction(row, 'clear')">清仓</el-button>
            </template>
          </el-table-column>
        </el-table>
      </el-card>
    </el-main>
  </div>
</template>

<script setup>
import { ref, onMounted, computed } from 'vue'
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

const loadPositions = async () => {
  try {
    const response = await fetch(`/api/v1/ui/${currentAccountId.value}/positions`)
    const data = await response.json()

    positions.value = data.positions || []
    availableCash.value = data.available_cash || 0

    // 计算汇总数据
    marketValue.value = positions.value.reduce((sum, p) => sum + (p.market_value || 0), 0)
    totalPnl.value = positions.value.reduce((sum, p) => sum + (p.profit_loss || 0), 0)
    totalAssets.value = marketValue.value + availableCash.value
    pnlPercent.value = (totalPnl.value / (totalAssets.value - availableCash.value)) * 100 || 0
  } catch (error) {
    console.error('加载持仓数据失败:', error)
  }
}

const formatNumber = (num) => {
  return Number(num || 0).toLocaleString('zh-CN', { minimumFractionDigits: 2 })
}

const handleAction = (row, action) => {
  console.log('操作:', action, row.stock_code)
  // TODO: 实现交易操作
}

onMounted(async () => {
  await loadPositions()
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
</style>
