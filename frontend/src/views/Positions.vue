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
            <el-tabs v-model="activeTab" class="detail-tabs">
              <el-tab-pane label="当前持仓" name="holding" />
              <el-tab-pane :label="`已清仓 (${closedCount})`" name="closed" />
            </el-tabs>
            <el-button v-if="activeTab === 'holding'" type="primary" size="small" @click="refreshPrices" :loading="refreshing">
              <el-icon><Refresh /></el-icon>
              刷新行情
            </el-button>
          </div>
        </template>

        <!-- 当前持仓表格 -->
        <el-table v-show="activeTab === 'holding'" :data="paginatedPositions" stripe style="width: 100%" @row-dblclick="showKline">
          <el-table-column type="index" label="序号" width="60" align="center" :index="indexMethod" />
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

        <div class="pagination-bar" v-if="activeTab === 'holding' && positions.length > posPageSize">
          <el-pagination
            v-model:current-page="posCurrentPage"
            v-model:page-size="posPageSize"
            :total="positions.length"
            :page-sizes="[10, 20, 50, 100]"
            layout="sizes, prev, pager, next, total"
            small
          />
        </div>

        <!-- 已清仓明细表格 -->
        <el-table v-show="activeTab === 'closed'" :data="paginatedClosed" stripe style="width: 100%">
          <el-table-column type="index" label="序号" width="60" align="center" :index="closedIndexMethod" />
          <el-table-column prop="stock_code" label="股票代码" width="100" />
          <el-table-column prop="stock_name" label="股票名称" width="120" />
          <el-table-column prop="buy_quantity" label="数量" width="80" align="right" />
          <el-table-column label="买入价" width="100" align="right">
            <template #default="{ row }">¥{{ row.avg_buy_price }}</template>
          </el-table-column>
          <el-table-column label="卖出价" width="100" align="right">
            <template #default="{ row }">¥{{ row.avg_sell_price }}</template>
          </el-table-column>
          <el-table-column label="买入时间" width="110">
            <template #default="{ row }">{{ row.first_buy_time }}</template>
          </el-table-column>
          <el-table-column label="卖出时间" width="110">
            <template #default="{ row }">{{ row.last_sell_time }}</template>
          </el-table-column>
          <el-table-column prop="holding_days" label="持有天数" width="80" align="right" />
          <el-table-column label="交易成本" width="110" align="right">
            <template #default="{ row }">¥{{ formatNumber(row.total_commission) }}</template>
          </el-table-column>
          <el-table-column label="清仓收益" width="120" align="right">
            <template #default="{ row }">
              <span :class="row.net_profit >= 0 ? 'profit-positive' : 'profit-negative'">
                {{ row.net_profit >= 0 ? '+' : '' }}¥{{ formatNumber(Math.abs(row.net_profit)) }}
              </span>
            </template>
          </el-table-column>
          <el-table-column label="收益率" width="100" align="right">
            <template #default="{ row }">
              <span :class="row.profit_pct >= 0 ? 'profit-positive' : 'profit-negative'">
                {{ row.profit_pct >= 0 ? '+' : '' }}{{ row.profit_pct }}%
              </span>
            </template>
          </el-table-column>
          <el-table-column label="年化收益" width="100" align="right">
            <template #default="{ row }">
              <span :class="row.annualized_pct >= 0 ? 'profit-positive' : 'profit-negative'">
                {{ row.annualized_pct >= 0 ? '+' : '' }}{{ formatPct(row.annualized_pct) }}%
              </span>
            </template>
          </el-table-column>
        </el-table>

        <div class="pagination-bar" v-if="activeTab === 'closed' && closedPositions.length > closedPageSize">
          <el-pagination
            v-model:current-page="closedCurrentPage"
            v-model:page-size="closedPageSize"
            :total="closedPositions.length"
            :page-sizes="[10, 20, 50, 100]"
            layout="sizes, prev, pager, next, total"
            small
          />
        </div>
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

      <!-- K 线图弹窗 -->
      <el-dialog v-model="klineVisible" :title="`${klineStockInfo.name} (${klineStockInfo.code}) K线走势`" width="85%" top="5vh" @close="destroyKlineChart">
        <div class="kline-nav">
          <el-button size="small" @click="prevStock" :disabled="!hasPrevStock">
            <el-icon><ArrowLeft /></el-icon> 上一只
          </el-button>
          <span class="kline-nav-text">{{ klineNavText }}</span>
          <el-button size="small" @click="nextStock" :disabled="!hasNextStock">
            下一只 <el-icon><ArrowRight /></el-icon>
          </el-button>
        </div>
        <div ref="klineChartRef" style="width: 100%; height: 550px"></div>
      </el-dialog>
    </el-main>
  </div>
</template>

<script setup>
import { ref, onMounted, computed, nextTick } from 'vue'
import { ElMessage } from 'element-plus'
import { ArrowLeft, ArrowRight } from '@element-plus/icons-vue'
import { useAccountStore } from '../stores/account'
import NavBar from '../components/NavBar.vue'
import * as echarts from 'echarts'

const accountStore = useAccountStore()
const currentAccount = computed(() => accountStore.currentAccount)
const currentAccountId = computed(() => accountStore.currentAccountId)

const positions = ref([])
const totalAssets = ref(0)
const availableCash = ref(0)
const marketValue = ref(0)
const totalPnl = ref(0)
const pnlPercent = ref(0)

// 分页
const posCurrentPage = ref(1)
const posPageSize = ref(20)
const paginatedPositions = computed(() => {
  const start = (posCurrentPage.value - 1) * posPageSize.value
  return positions.value.slice(start, start + posPageSize.value)
})
const indexMethod = (index) => (posCurrentPage.value - 1) * posPageSize.value + index + 1

// DSA 分析状态
const dsaDialogVisible = ref(false)
const dsaAnalyzing = ref(false)
const dsaStock = ref({ stock_code: '', stock_name: '' })
const dsaResult = ref(null)
const dsaError = ref('')

const refreshing = ref(false)

// 已清仓明细
const activeTab = ref('holding')
const closedPositions = ref([])
const closedCount = ref(0)
const closedCurrentPage = ref(1)
const closedPageSize = ref(20)
const paginatedClosed = computed(() => {
  const start = (closedCurrentPage.value - 1) * closedPageSize.value
  return closedPositions.value.slice(start, start + closedPageSize.value)
})
const closedIndexMethod = (index) => (closedCurrentPage.value - 1) * closedPageSize.value + index + 1

const loadClosedPositions = async () => {
  try {
    const response = await fetch(`/api/v1/ui/${currentAccountId.value}/closed-positions?limit=500`)
    const data = await response.json()
    closedPositions.value = data.closed_positions || []
    closedCount.value = data.total || 0
    closedCurrentPage.value = 1
  } catch (error) {
    console.error('加载已清仓明细失败:', error)
  }
}

// K 线图
const klineVisible = ref(false)
const klineChartRef = ref(null)
const klineStockInfo = ref({ code: '', name: '' })
const klineStockIndex = ref(-1)
let klineChart = null

const hasPrevStock = computed(() => klineStockIndex.value > 0)
const hasNextStock = computed(() => klineStockIndex.value >= 0 && klineStockIndex.value < positions.value.length - 1)
const klineNavText = computed(() => {
  const total = positions.value.length
  const idx = klineStockIndex.value
  if (idx < 0 || total === 0) return ''
  return `${idx + 1} / ${total}`
})

const loadPositions = async () => {
  // 先从 DB 加载数据（不等待行情刷新）
  try {
    const response = await fetch(`/api/v1/ui/${currentAccountId.value}/positions`)
    const data = await response.json()
    positions.value = data.positions || []
    posCurrentPage.value = 1
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
    posCurrentPage.value = 1
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

const formatPct = (num) => {
  const n = Number(num || 0)
  if (n >= 1000) return '1000+'
  if (n <= -1000) return '-1000+'
  return n.toFixed(2)
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

// ========== K 线图 ==========

const showKline = async (row) => {
  const idx = positions.value.findIndex(s => s.stock_code === row.stock_code)
  klineStockIndex.value = idx
  klineVisible.value = true
  await loadKlineData(row.stock_code, row.stock_name)
}

const loadKlineData = async (code, name) => {
  klineStockInfo.value = { code, name }

  // 优先从本地 kline.db 读取
  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/stocks/${code}/kline-local?months=6`)
    if (res.ok) {
      const data = await res.json()
      await nextTick()
      if (data.success && data.kline && data.kline.length > 0) {
        renderKlineChart(data.kline)
        return
      }
    }
  } catch (e) {
    console.warn('本地 K 线数据读取失败，回退 SDK:', e.message)
  }

  // 回退：SDK 查询
  const endDt = new Date()
  const startDt = new Date()
  startDt.setMonth(startDt.getMonth() - 6)
  const start = startDt.toISOString().slice(0, 10).replace(/-/g, '')
  const end = endDt.toISOString().slice(0, 10).replace(/-/g, '')

  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/market/kline?stock_code=${code}&period=day&start_date=${start}&end_date=${end}`)
    const data = await res.json()
    await nextTick()
    renderKlineChart(data.data?.kline || [])
  } catch (e) {
    console.error('加载 K 线数据失败:', e)
  }
}

const prevStock = async () => {
  if (!hasPrevStock.value) return
  const idx = klineStockIndex.value - 1
  const row = positions.value[idx]
  if (!row) return
  klineStockIndex.value = idx
  await loadKlineData(row.stock_code, row.stock_name)
}

const nextStock = async () => {
  if (!hasNextStock.value) return
  const idx = klineStockIndex.value + 1
  const row = positions.value[idx]
  if (!row) return
  klineStockIndex.value = idx
  await loadKlineData(row.stock_code, row.stock_name)
}

const renderKlineChart = (klineData) => {
  if (!klineChartRef.value) return
  if (!klineChart) {
    klineChart = echarts.init(klineChartRef.value)
  }

  const dates = klineData.map(d => String(d.trade_date))
  const ohlcValues = klineData.map(d => [d.open, d.close, d.low, d.high])
  const volumes = klineData.map(d => d.volume || 0)

  klineChart.setOption({
    tooltip: {
      trigger: 'axis',
      axisPointer: { type: 'cross' },
      formatter: (params) => {
        const p = params[0]
        if (!p || !p.value) return ''
        const v = p.value
        const idx = dates.indexOf(p.name)
        const vol = idx >= 0 ? volumes[idx] : '-'
        return `${p.name}<br/>开: ${v[1]}  收: ${v[2]}  低: ${v[3]}  高: ${v[4]}<br/>量: ${typeof vol === 'number' ? vol.toLocaleString() : vol}`
      },
    },
    grid: [
      { left: '8%', right: '4%', top: '8%', height: '55%' },
      { left: '8%', right: '4%', top: '68%', height: '22%' },
    ],
    xAxis: [
      { type: 'category', data: dates, gridIndex: 0, axisLabel: { show: false } },
      { type: 'category', data: dates, gridIndex: 1 },
    ],
    yAxis: [
      { type: 'value', scale: true, gridIndex: 0 },
      { type: 'value', scale: true, gridIndex: 1, splitNumber: 2 },
    ],
    series: [
      {
        name: 'K线',
        type: 'candlestick',
        data: ohlcValues,
        xAxisIndex: 0,
        yAxisIndex: 0,
        itemStyle: { color: '#ef232a', color0: '#14b143', borderColor: '#ef232a', borderColor0: '#14b143' },
      },
      {
        name: '成交量',
        type: 'bar',
        data: volumes,
        xAxisIndex: 1,
        yAxisIndex: 1,
        itemStyle: {
          color: (param) => {
            const idx = param.dataIndex
            if (idx < klineData.length) {
              return klineData[idx].close >= klineData[idx].open ? '#ef232a' : '#14b143'
            }
            return '#999'
          },
        },
      },
    ],
    dataZoom: [{ type: 'inside' }, { type: 'slider', xAxisIndex: [0, 1] }],
  }, true)
  if (!klineChart._resizeBound) {
    const handler = () => klineChart && klineChart.resize()
    window.addEventListener('resize', handler)
    klineChart._resizeBound = true
  }
}

const destroyKlineChart = () => {
  if (klineChart) { klineChart.dispose(); klineChart = null }
}

import { onUnmounted } from 'vue'
onUnmounted(() => {
  destroyKlineChart()
})

onMounted(async () => {
  // 先展示 DB 数据
  await loadPositions()
  await loadClosedPositions()
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

.detail-tabs {
  flex: 1;
}

.detail-tabs :deep(.el-tabs__header) {
  margin-bottom: 0;
}

.detail-tabs :deep(.el-tabs__nav-wrap::after) {
  display: none;
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

.pagination-bar {
  display: flex; justify-content: center; padding: 12px 0;
}

.kline-nav {
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 16px;
  margin-bottom: 12px;
}

.kline-nav-text {
  font-size: 14px;
  color: #606266;
  min-width: 80px;
  text-align: center;
  font-family: monospace;
}
</style>
