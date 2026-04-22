<template>
  <div class="layout-container">
    <NavBar />
    <el-main class="main-content">
      <h2>交易监控 - {{ currentAccount?.display_name }}</h2>

      <!-- 统计卡片 -->
      <el-row :gutter="20" class="stats-row">
        <el-col :span="6">
          <el-statistic title="总交易笔数" :value="stats.totalCount" />
        </el-col>
        <el-col :span="6">
          <el-statistic title="买入笔数" :value="stats.buyCount" />
        </el-col>
        <el-col :span="6">
          <el-statistic title="卖出笔数" :value="stats.sellCount" />
        </el-col>
        <el-col :span="6">
          <el-statistic title="成功率" :value="stats.winRate" suffix="%" />
        </el-col>
      </el-row>

      <!-- 交易明细表 -->
      <el-card>
        <template #header>
          <div class="card-header">
            <span>交易明细</span>
            <el-space>
              <el-date-picker
                v-model="dateRange"
                type="daterange"
                range-separator="至"
                start-placeholder="开始日期"
                end-placeholder="结束日期"
                size="small"
                @change="loadTrades"
              />
              <el-button type="primary" size="small" @click="loadTrades">
                <el-icon><Refresh /></el-icon>
                刷新
              </el-button>
            </el-space>
          </div>
        </template>

        <el-table :data="trades" stripe style="width: 100%">
          <el-table-column prop="trade_time" label="时间" width="160" />
          <el-table-column prop="stock_code" label="股票代码" width="100" />
          <el-table-column prop="stock_name" label="股票名称" width="100" />
          <el-table-column prop="trade_type" label="操作" width="80">
            <template #default="{ row }">
              <el-tag :type="row.trade_type === 'buy' ? 'danger' : 'success'">
                {{ row.trade_type === 'buy' ? '买入' : '卖出' }}
              </el-tag>
            </template>
          </el-table-column>
          <el-table-column prop="quantity" label="数量" width="100" align="right" />
          <el-table-column prop="price" label="价格" width="100" align="right">
            <template #default="{ row }">¥{{ row.price }}</template>
          </el-table-column>
          <el-table-column prop="amount" label="金额" width="120" align="right">
            <template #default="{ row }">¥{{ formatNumber(row.amount) }}</template>
          </el-table-column>
          <el-table-column prop="status" label="状态" width="80">
            <template #default="{ row }">
              <el-tag :type="row.status === 'success' ? 'success' : 'warning'" size="small">
                {{ row.status === 'success' ? '成功' : row.status }}
              </el-tag>
            </template>
          </el-table-column>
        </el-table>
      </el-card>
    </el-main>
  </div>
</template>

<script setup>
import { ref, reactive, onMounted, computed } from 'vue'
import { useAccountStore } from '../stores/account'
import NavBar from '../components/NavBar.vue'

const accountStore = useAccountStore()
const currentAccount = computed(() => accountStore.currentAccount)
const currentAccountId = computed(() => accountStore.currentAccountId)

const dateRange = ref([])
const trades = ref([])
const stats = reactive({
  totalCount: 0,
  buyCount: 0,
  sellCount: 0,
  winRate: 0
})

const loadTrades = async () => {
  try {
    const response = await fetch(`/api/v1/ui/${currentAccountId.value}/trades/today`)
    const data = await response.json()

    trades.value = data.trades || []
    stats.totalCount = data.stats?.total_count || 0
    stats.buyCount = data.stats?.buy_count || 0
    stats.sellCount = data.stats?.sell_count || 0
  } catch (error) {
    console.error('加载交易数据失败:', error)
  }
}

const formatNumber = (num) => {
  return Number(num || 0).toLocaleString('zh-CN', { minimumFractionDigits: 2 })
}

onMounted(async () => {
  await loadTrades()
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

.stats-row {
  margin-bottom: 20px;
}

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
}
</style>
