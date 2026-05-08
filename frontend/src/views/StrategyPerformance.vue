<template>
  <div class="layout-container">
    <NavBar />
    <el-main class="main-content">
      <h2>策略效能评估</h2>

      <!-- 策略选择 -->
      <el-select ref="strategySelectRef" v-model="selectedStrategyId" placeholder="选择策略" style="width: 300px; margin-bottom: 20px" @change="onStrategyChange" :loading="loading">
        <el-option label="全部策略" :value="null" />
        <el-option v-for="s in strategies" :key="s.id" :label="s.name" :value="s.id" />
      </el-select>

      <!-- KPI 卡片 -->
      <el-row :gutter="16" style="margin-bottom: 20px">
        <el-col :span="6">
          <el-card shadow="hover">
            <div class="kpi-card">
              <div class="kpi-label">胜率</div>
              <div class="kpi-value" :class="stats.win_rate >= 50 ? 'profit-positive' : 'profit-negative'">{{ stats.win_rate }}%</div>
              <div class="kpi-sub">{{ stats.win_count }}胜 {{ stats.lose_count }}败 / {{ stats.total_trades }}笔</div>
            </div>
          </el-card>
        </el-col>
        <el-col :span="6">
          <el-card shadow="hover">
            <div class="kpi-card">
              <div class="kpi-label">总盈亏</div>
              <div class="kpi-value" :class="stats.total_pnl >= 0 ? 'profit-positive' : 'profit-negative'">¥{{ formatNumber(Math.abs(stats.total_pnl)) }}</div>
              <div class="kpi-sub">盈利 ¥{{ formatNumber(stats.total_profit) }} / 亏损 ¥{{ formatNumber(stats.total_loss) }}</div>
            </div>
          </el-card>
        </el-col>
        <el-col :span="6">
          <el-card shadow="hover">
            <div class="kpi-card">
              <div class="kpi-label">执行率</div>
              <div class="kpi-value">{{ stats.execution_rate }}%</div>
              <div class="kpi-sub">{{ stats.bought_count }}次买入 / {{ stats.total_selections }}次选出</div>
            </div>
          </el-card>
        </el-col>
        <el-col :span="6">
          <el-card shadow="hover">
            <div class="kpi-card">
              <div class="kpi-label">平均持仓</div>
              <div class="kpi-value">{{ stats.avg_holding_days }}天</div>
              <div class="kpi-sub">盈亏比 {{ stats.profit_factor }}</div>
            </div>
          </el-card>
        </el-col>
      </el-row>

      <!-- 全部策略排行榜（未选择单策略时显示） -->
      <el-card v-if="!selectedStrategyId && allStats.length > 0" style="margin-bottom: 20px">
        <template #header><span>策略排行榜</span></template>
        <el-table :data="allStats" stripe>
          <el-table-column label="策略名" width="200">
            <template #default="{ row }">{{ row.name }}</template>
          </el-table-column>
          <el-table-column label="选出" width="80" align="right">
            <template #default="{ row }">{{ row.total_selections }}</template>
          </el-table-column>
          <el-table-column label="买入" width="80" align="right">
            <template #default="{ row }">{{ row.bought_count }}</template>
          </el-table-column>
          <el-table-column label="交易" width="80" align="right">
            <template #default="{ row }">{{ row.total_trades }}</template>
          </el-table-column>
          <el-table-column label="胜率" width="100" align="right">
            <template #default="{ row }">
              <span :class="row.win_rate >= 50 ? 'profit-positive' : 'profit-negative'">{{ row.win_rate }}%</span>
            </template>
          </el-table-column>
          <el-table-column label="总盈亏" width="120" align="right">
            <template #default="{ row }">
              <span :class="row.total_pnl >= 0 ? 'profit-positive' : 'profit-negative'">¥{{ formatNumber(row.total_pnl) }}</span>
            </template>
          </el-table-column>
          <el-table-column label="执行率" width="100" align="right">
            <template #default="{ row }">{{ row.execution_rate }}%</template>
          </el-table-column>
          <el-table-column label="操作" width="100">
            <template #default="{ row }">
              <el-button type="primary" size="small" @click="selectedStrategyId = row.strategy_id">查看</el-button>
            </template>
          </el-table-column>
        </el-table>
      </el-card>

      <!-- 选股明细 -->
      <el-card style="margin-bottom: 20px">
        <template #header>
          <div class="card-header">
            <span>选股明细</span>
            <el-button type="primary" size="small" @click="loadPerformance"><el-icon><Refresh /></el-icon>刷新</el-button>
          </div>
        </template>
        <el-table :data="selections" stripe>
          <el-table-column prop="stock_code" label="代码" width="110" />
          <el-table-column prop="stock_name" label="名称" width="120" />
          <el-table-column label="选出日期" width="180">
            <template #default="{ row }">{{ (row.selected_at || '').split('T')[0] }}</template>
          </el-table-column>
          <el-table-column label="选出价" width="100" align="right">
            <template #default="{ row }">¥{{ row.buy_price || '-' }}</template>
          </el-table-column>
          <el-table-column label="买入价" width="100" align="right">
            <template #default="{ row }">¥{{ row.buy_price_actual || '-' }}</template>
          </el-table-column>
          <el-table-column label="卖出价" width="100" align="right">
            <template #default="{ row }">¥{{ row.sell_price || '-' }}</template>
          </el-table-column>
          <el-table-column label="盈亏" width="120" align="right">
            <template #default="{ row }">
              <span v-if="row.profit_loss !== null" :class="row.profit_loss >= 0 ? 'profit-positive' : 'profit-negative'">
                {{ row.profit_loss >= 0 ? '+' : '' }}¥{{ formatNumber(Math.abs(row.profit_loss)) }}
              </span>
              <span v-else>-</span>
            </template>
          </el-table-column>
          <el-table-column label="状态" width="80" align="center">
            <template #default="{ row }">
              <el-tag :type="row.bought ? 'success' : 'info'" size="small">{{ row.bought ? '已买' : '未买' }}</el-tag>
            </template>
          </el-table-column>
        </el-table>
      </el-card>

      <!-- 权益曲线 -->
      <el-card>
        <template #header><span>权益曲线</span></template>
        <div ref="equityChartRef" style="width: 100%; height: 400px"></div>
      </el-card>
    </el-main>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import { Refresh } from '@element-plus/icons-vue'
import { useAccountStore } from '../stores/account'
import NavBar from '../components/NavBar.vue'
import * as echarts from 'echarts'

const accountStore = useAccountStore()
const currentAccountId = ref(accountStore.currentAccountId)

const strategies = ref([])
const allStats = ref([])
const selections = ref([])
const selectedStrategyId = ref(null)
const loading = ref(false)
const strategySelectRef = ref(null)

const stats = ref({
  win_rate: 0, win_count: 0, lose_count: 0, total_trades: 0,
  total_pnl: 0, total_profit: 0, total_loss: 0,
  execution_rate: 0, bought_count: 0, total_selections: 0,
  avg_holding_days: 0, profit_factor: 0
})

const equityChartRef = ref(null)
let equityChart = null

const formatNumber = (num) => {
  return Number(num || 0).toLocaleString('zh-CN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })
}

const onStrategyChange = () => {
  loadPerformance()
}

const loadPerformance = async () => {
  loading.value = true
  try {
    const url = selectedStrategyId.value
      ? `/api/v1/ui/${currentAccountId.value}/performance/summary?strategy_id=${selectedStrategyId.value}`
      : `/api/v1/ui/${currentAccountId.value}/performance/summary`

    const res = await fetch(url)
    const data = await res.json()

    strategies.value = data.strategies || []
    allStats.value = data.all_stats || []

    if (data.stats) {
      stats.value = data.stats
    } else {
      stats.value = {
        win_rate: 0, win_count: 0, lose_count: 0, total_trades: 0,
        total_pnl: 0, total_profit: 0, total_loss: 0,
        execution_rate: 0, bought_count: 0, total_selections: 0,
        avg_holding_days: 0, profit_factor: 0
      }
    }

    // 加载选股明细
    if (selectedStrategyId.value) {
      const selRes = await fetch(`/api/v1/ui/${currentAccountId.value}/performance/${selectedStrategyId.value}/selections`)
      const selData = await selRes.json()
      selections.value = selData.selections || []
    } else {
      selections.value = []
    }

    // 加载权益曲线
    loadEquityCurve()
  } catch (e) {
    console.error('加载效能数据失败:', e)
  } finally {
    loading.value = false
  }
}

const loadEquityCurve = async () => {
  try {
    const url = selectedStrategyId.value
      ? `/api/v1/ui/${currentAccountId.value}/performance/equity-curve?strategy_id=${selectedStrategyId.value}`
      : `/api/v1/ui/${currentAccountId.value}/performance/equity-curve`

    const res = await fetch(url)
    const data = await res.json()
    renderEquityChart(data.curve || [])
  } catch (e) {
    console.error('加载权益曲线失败:', e)
  }
}

const renderEquityChart = (curve) => {
  if (!equityChartRef.value) return
  if (!equityChart) {
    equityChart = echarts.init(equityChartRef.value)
  }

  const dates = curve.map(c => c.date)
  const pnlData = curve.map(c => c.cumulative_pnl)
  const lastPnl = pnlData.length > 0 ? pnlData[pnlData.length - 1] : 0

  equityChart.setOption({
    tooltip: { trigger: 'axis' },
    grid: { left: '3%', right: '4%', bottom: '3%', containLabel: true },
    xAxis: { type: 'category', data: dates, axisLabel: { rotate: 45 } },
    yAxis: { type: 'value', name: '累计盈亏 (¥)' },
    series: [{
      name: '累计盈亏',
      type: 'line',
      data: pnlData,
      smooth: true,
      lineStyle: { color: lastPnl >= 0 ? '#f56c6c' : '#67c23a', width: 2 },
      areaStyle: {
        color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
          { offset: 0, color: lastPnl >= 0 ? 'rgba(245,108,108,0.3)' : 'rgba(103,194,58,0.3)' },
          { offset: 1, color: 'rgba(255,255,255,0)' }
        ])
      }
    }]
  })
}

onMounted(() => {
  loadPerformance()
  window.addEventListener('resize', () => equityChart?.resize())
})
</script>

<style scoped>
.layout-container { height: 100%; display: flex; flex-direction: column; }
.main-content { padding: 20px; }
h2 { margin-bottom: 20px; color: #303133; }
.kpi-card { text-align: center; padding: 10px 0; }
.kpi-label { font-size: 13px; color: #909399; margin-bottom: 8px; }
.kpi-value { font-size: 28px; font-weight: bold; }
.kpi-sub { font-size: 12px; color: #909399; margin-top: 4px; }
.profit-positive { color: #f56c6c; }
.profit-negative { color: #67c23a; }
.card-header { display: flex; justify-content: space-between; align-items: center; }
</style>
