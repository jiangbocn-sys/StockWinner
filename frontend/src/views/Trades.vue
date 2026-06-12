<template>
  <div class="layout-container">
    <NavBar />
    <el-main class="main-content">
      <h2>交易监控 - {{ currentAccount?.display_name }}</h2>

      <!-- 主内容区：左侧手动下单 + 右侧交易数据 -->
      <div class="split-container" ref="splitContainer">
        <!-- 左侧：手动下单 -->
        <ManualOrderPanel
          :account-id="currentAccountId"
          @order-submitted="loadTrades"
        />

        <!-- 拖拽分割线 -->
        <div class="splitter" @mousedown="startDrag">
          <div class="splitter-handle"></div>
        </div>

        <!-- 右侧：统计数据 + 标签页 -->
        <div class="right-panel">
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

          <!-- 标签页 -->
          <el-tabs v-model="activeTab" type="card">
            <!-- 标签页 1：交易明细 -->
            <el-tab-pane label="交易明细" name="trades">
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
                      <el-button type="primary" size="small" @click="loadTrades" :loading="tradesLoading">
                        <el-icon><Refresh /></el-icon>
                        刷新
                      </el-button>
                      <el-dropdown @command="handleExportTrades">
                        <el-button type="success" size="small"><el-icon><Download /></el-icon>导出</el-button>
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
                    </el-space>
                  </div>
                </template>

                <el-table :data="trades" stripe style="width: 100%" @row-dblclick="showKline">
                  <el-table-column prop="trade_time" label="时间" min-width="160" max-width="180" />
                  <el-table-column prop="stock_code" label="代码" min-width="90" max-width="110" />
                  <el-table-column prop="stock_name" label="名称" min-width="100" max-width="150" show-overflow-tooltip />
                  <el-table-column prop="trade_type" label="操作" width="75">
                    <template #default="{ row }">
                      <el-tag :type="row.trade_type === 'buy' ? 'danger' : 'success'">
                        {{ row.trade_type === 'buy' ? '买入' : '卖出' }}
                      </el-tag>
                    </template>
                  </el-table-column>
                  <el-table-column prop="quantity" label="数量" min-width="80" max-width="100" align="right" />
                  <el-table-column prop="price" label="价格" min-width="90" max-width="110" align="right">
                    <template #default="{ row }">¥{{ formatNumber(row.price) }}</template>
                  </el-table-column>
                  <el-table-column prop="amount" label="金额" min-width="110" max-width="140" align="right">
                    <template #default="{ row }">¥{{ formatNumber(row.amount) }}</template>
                  </el-table-column>
                  <el-table-column prop="commission" label="手续费" min-width="90" max-width="120" align="right">
                    <template #default="{ row }">¥{{ formatNumber(row.commission) }}</template>
                  </el-table-column>
                  <el-table-column prop="trigger_source" label="触发来源" min-width="90" max-width="130" show-overflow-tooltip />
                  <el-table-column prop="status" label="状态" width="75">
                    <template #default="{ row }">
                      <el-tag :type="row.status === 'completed' ? 'success' : 'warning'" size="small">
                        {{ row.status === 'completed' ? '成功' : row.status }}
                      </el-tag>
                    </template>
                  </el-table-column>
                </el-table>
              </el-card>
            </el-tab-pane>

            <!-- 标签页 2：通知记录 -->
            <el-tab-pane label="通知记录" name="notifications">
          <el-card>
            <template #header>
              <div class="card-header">
                <span>通知记录</span>
                <el-space>
                  <el-select v-model="notificationFilter" size="small" @change="loadNotifications">
                    <el-option label="全部" value="" />
                    <el-option label="成交通知" value="trade_executed" />
                    <el-option label="信号触发" value="signal_triggered" />
                    <el-option label="任务完成" value="task_completed" />
                    <el-option label="任务失败" value="task_failed" />
                  </el-select>
                  <el-button size="small" @click="loadNotifications">
                    <el-icon><Refresh /></el-icon>
                    刷新
                  </el-button>
                </el-space>
              </div>
            </template>

            <el-table :data="notifications" stripe style="width: 100%">
              <el-table-column prop="created_at" label="时间" width="180">
                <template #default="{ row }">
                  {{ formatTime(row.created_at) }}
                </template>
              </el-table-column>
              <el-table-column prop="event_type" label="事件类型" width="120">
                <template #default="{ row }">
                  <el-tag :type="getEventTypeColor(row.event_type)" size="small">
                    {{ formatEventType(row.event_type) }}
                  </el-tag>
                </template>
              </el-table-column>
              <el-table-column prop="title" label="标题" width="120" />
              <el-table-column prop="channel" label="渠道" width="80" />
              <el-table-column prop="content" label="内容" min-width="300" show-overflow-tooltip />
              <el-table-column prop="status" label="状态" width="80">
                <template #default="{ row }">
                  <el-tag :type="row.status === 'sent' ? 'success' : 'danger'" size="small">
                    {{ row.status === 'sent' ? '已发' : '失败' }}
                  </el-tag>
                </template>
              </el-table-column>
            </el-table>
          </el-card>
        </el-tab-pane>
      </el-tabs>
        </div><!-- right-panel -->
      </div><!-- split-container -->
    </el-main>

    <!-- K 线图弹窗 -->
    <el-dialog v-model="klineVisible" :title="klineDialogTitle" width="85%" top="5vh">
      <div class="kline-nav">
        <el-button size="small" @click="prevStock" :disabled="!hasPrevStock">
          <el-icon><ArrowLeft /></el-icon> 上一只
        </el-button>
        <span class="kline-nav-text">{{ klineNavText }}</span>
        <el-button size="small" @click="nextStock" :disabled="!hasNextStock">
          下一只 <el-icon><ArrowRight /></el-icon>
        </el-button>
      </div>
      <div class="kline-controls" style="margin-bottom: 12px; display: flex; align-items: center; gap: 16px; flex-wrap: wrap">
        <el-radio-group v-model="klinePeriod" size="small" @change="reloadKline">
          <el-radio-button label="day">日线</el-radio-button>
          <el-radio-button label="week">周线</el-radio-button>
          <el-radio-button label="month">月线</el-radio-button>
        </el-radio-group>
        <el-radio-group v-model="klineAdjust" size="small" @change="reloadKline">
          <el-radio-button label="none">不复权</el-radio-button>
          <el-radio-button label="forward">前复权</el-radio-button>
        </el-radio-group>
        <!-- 技术指标选择器 -->
        <el-dropdown trigger="click" @command="toggleIndicator" style="margin-left: 8px">
          <el-button size="small">
            <el-icon><Setting /></el-icon> 技术指标
            <el-icon class="el-icon--right"><ArrowDown /></el-icon>
          </el-button>
          <template #dropdown>
            <el-dropdown-menu>
              <el-dropdown-item :class="{ 'is-active': selectedIndicators.includes('ma5') }" command="ma5">
                MA5 均线 <el-tag v-if="selectedIndicators.includes('ma5')" size="small" type="success">已选</el-tag>
              </el-dropdown-item>
              <el-dropdown-item :class="{ 'is-active': selectedIndicators.includes('ma10') }" command="ma10">
                MA10 均线 <el-tag v-if="selectedIndicators.includes('ma10')" size="small" type="success">已选</el-tag>
              </el-dropdown-item>
              <el-dropdown-item :class="{ 'is-active': selectedIndicators.includes('ma20') }" command="ma20">
                MA20 均线 <el-tag v-if="selectedIndicators.includes('ma20')" size="small" type="success">已选</el-tag>
              </el-dropdown-item>
              <el-dropdown-item :class="{ 'is-active': selectedIndicators.includes('ma60') }" command="ma60">
                MA60 均线 <el-tag v-if="selectedIndicators.includes('ma60')" size="small" type="success">已选</el-tag>
              </el-dropdown-item>
              <el-dropdown-item divided :class="{ 'is-active': selectedIndicators.includes('boll') }" command="boll">
                布林带 (BOLL) <el-tag v-if="selectedIndicators.includes('boll')" size="small" type="success">已选</el-tag>
              </el-dropdown-item>
              <el-dropdown-item :class="{ 'is-active': selectedIndicators.includes('ema12') }" command="ema12">
                EMA12 <el-tag v-if="selectedIndicators.includes('ema12')" size="small" type="success">已选</el-tag>
              </el-dropdown-item>
              <el-dropdown-item :class="{ 'is-active': selectedIndicators.includes('ema26') }" command="ema26">
                EMA26 <el-tag v-if="selectedIndicators.includes('ema26')" size="small" type="success">已选</el-tag>
              </el-dropdown-item>
            </el-dropdown-menu>
          </template>
        </el-dropdown>
        <el-button size="small" @click="loadMoreKline" :disabled="klinePeriod === 'month' || klineLoadingMore" v-if="klinePeriod !== 'month'">
          <el-icon><Download /></el-icon> 加载更多
        </el-button>
        <span v-if="klinePeriod === 'month'" style="color: #909399; font-size: 12px">月线已显示全部数据</span>
      </div>
      <KlineChart ref="klineChartRef" :data="klineData" height="550px"
        :stockCode="klineStockInfo.code"
        :accountId="currentAccountId"
        :enableDrillDown="true"
        :indicators="klineIndicators"
        :indicatorConfig="klineIndicatorConfig" />
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, reactive, onMounted, computed, watch } from 'vue'
import { useAccountStore } from '../stores/account'
import { ElMessage } from 'element-plus'
import { ArrowLeft, ArrowRight, Refresh, Download, Setting, ArrowDown } from '@element-plus/icons-vue'
import { exportTable as doExport } from '@/utils/exportHelper'
import NavBar from '../components/NavBar.vue'
import ManualOrderPanel from '../components/ManualOrderPanel.vue'
import KlineChart from '../components/KlineChart.vue'

const accountStore = useAccountStore()
const currentAccount = computed(() => accountStore.currentAccount)
const currentAccountId = computed(() => accountStore.currentAccountId)

const activeTab = ref('trades')

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
// 交易明细
// ============================================================
const dateRange = ref([])
const trades = ref([])
const tradesLoading = ref(false)
const stats = reactive({
  totalCount: 0,
  buyCount: 0,
  sellCount: 0,
  winRate: 0
})

const tradeColumns = [
  { label: '时间', prop: 'trade_time' },
  { label: '代码', prop: 'stock_code' },
  { label: '名称', prop: 'stock_name' },
  { label: '操作', prop: 'trade_type' },
  { label: '数量', prop: 'quantity' },
  { label: '价格', prop: 'price' },
  { label: '金额', prop: 'amount' },
  { label: '手续费', prop: 'commission' },
  { label: '触发来源', prop: 'trigger_source' },
  { label: '状态', prop: 'status' },
]

const handleExportTrades = (format) => {
  doExport(tradeColumns, trades.value, '交易记录', format)
}

const loadTrades = async () => {
  tradesLoading.value = true
  try {
    // 添加时间戳防止浏览器缓存
    const ts = Date.now()
    let response
    if (dateRange.value && dateRange.value.length === 2) {
      // 有日期范围 → 调用历史交易接口
      const startDate = dateRange.value[0].toISOString().slice(0, 10)
      const endDate = dateRange.value[1].toISOString().slice(0, 10)
      response = await fetch(
        `/api/v1/ui/${currentAccountId.value}/trades?start_date=${startDate}&end_date=${endDate}&limit=200&_t=${ts}`
      )
    } else {
      // 无日期范围 → 默认今日
      response = await fetch(`/api/v1/ui/${currentAccountId.value}/trades/today?_t=${ts}`)
    }
    const data = await response.json()
    trades.value = data.trades || []
    stats.totalCount = data.stats?.total_count || 0
    stats.buyCount = data.stats?.buy_count || 0
    stats.sellCount = data.stats?.sell_count || 0
  } catch (error) {
    console.error('加载交易数据失败:', error)
    ElMessage.error('加载交易数据失败')
  } finally {
    tradesLoading.value = false
  }
}

// === 通知记录 ===
const notifications = ref([])
const notificationFilter = ref('')

const loadNotifications = async () => {
  try {
    const params = new URLSearchParams({ limit: 100 })
    if (notificationFilter.value) {
      params.set('event_type', notificationFilter.value)
    }
    const response = await fetch(`/api/v1/ui/${currentAccountId.value}/notifications/history?${params}`)
    const data = await response.json()
    notifications.value = data.data || []
  } catch (error) {
    console.error('加载通知历史失败:', error)
  }
}

const formatEventType = (type) => {
  const map = {
    'trade_executed': '成交',
    'signal_triggered': '信号',
    'task_completed': '任务完成',
    'task_failed': '任务失败',
    'trade_failed': '交易失败',
  }
  return map[type] || type
}

const getEventTypeColor = (type) => {
  if (type === 'trade_executed') return 'success'
  if (type === 'signal_triggered') return ''
  if (type === 'task_completed') return 'success'
  if (type === 'task_failed') return 'danger'
  return 'danger'
}

const formatNumber = (num) => {
  return Number(num || 0).toLocaleString('zh-CN', { minimumFractionDigits: 2 })
}

const formatTime = (time) => {
  if (!time) return '-'
  // naive string 默认中国时间，显式附加 +08:00
  const str = time.includes('+') || time.endsWith('Z') ? time : time + '+08:00'
  const date = new Date(str)
  if (isNaN(date.getTime())) return time
  const pad = (n) => n.toString().padStart(2, '0')
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())} ${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`
}

// ============================================================
// K 线图弹窗
// ============================================================
const klineVisible = ref(false)
const klineChartRef = ref(null)
const klineStockInfo = ref({ code: '', name: '', sw_level1: '', sw_level2: '', sw_level3: '' })
const klineStockIndex = ref(-1)
const klineData = ref([])
const klinePeriod = ref('day')
const klineAdjust = ref('forward')
const klineMonths = ref(12)
const klineLoadingMore = ref(false)

// 技术指标叠加功能
const selectedIndicators = ref([])
const klineIndicators = ref({})
const klineIndicatorConfig = computed(() => {
  const config = []
  const indicatorColors = {
    ma5: '#FF6B6B',
    ma10: '#4ECDC4',
    ma20: '#FFD93D',
    ma60: '#96CEB4',
    boll_upper: '#FF8C00',
    boll_middle: '#FF1493',
    boll_lower: '#9370DB',
    ema12: '#00CED1',
    ema26: '#8B4513',
  }
  for (const key of selectedIndicators.value) {
    if (key === 'boll') {
      config.push({ key: 'boll_upper', name: 'BOLL上轨', color: indicatorColors.boll_upper, width: 1 })
      config.push({ key: 'boll_middle', name: 'BOLL中轨', color: indicatorColors.boll_middle, width: 1 })
      config.push({ key: 'boll_lower', name: 'BOLL下轨', color: indicatorColors.boll_lower, width: 1 })
    } else {
      const name = key.toUpperCase()
      config.push({ key, name, color: indicatorColors[key] || '#999', width: 1 })
    }
  }
  return config
})

const klineDialogTitle = computed(() => {
  const { name, code, sw_level1, sw_level2, sw_level3 } = klineStockInfo.value
  const industryParts = [sw_level1, sw_level2, sw_level3].filter(Boolean)
  const industryStr = industryParts.length > 0 ? ` [${industryParts.join(' - ')}]` : ''
  return `${name} (${code})${industryStr} K线走势`
})

const hasPrevStock = computed(() => klineStockIndex.value > 0)
const hasNextStock = computed(() => klineStockIndex.value >= 0 && klineStockIndex.value < trades.value.length - 1)
const klineNavText = computed(() => {
  const total = trades.value.length
  const idx = klineStockIndex.value
  if (idx < 0 || total === 0) return ''
  return `${idx + 1} / ${total}`
})

const showKline = async (row) => {
  const idx = trades.value.findIndex(s => s.stock_code === row.stock_code)
  klineStockIndex.value = idx >= 0 ? idx : -1
  klineVisible.value = true
  if (klinePeriod.value === 'day') {
    klineMonths.value = 12
  } else if (klinePeriod.value === 'week') {
    klineMonths.value = 60
  }
  await loadKlineData(row.stock_code, row.stock_name)
}

const reloadKline = () => {
  if (klineStockInfo.value.code) {
    klineIndicators.value = {}
    if (klinePeriod.value === 'day') {
      klineMonths.value = 12
    } else if (klinePeriod.value === 'week') {
      klineMonths.value = 60
    }
    const { code, name } = klineStockInfo.value
    loadKlineData(code, name)
  }
}

const loadMoreKline = async () => {
  if (klinePeriod.value === 'month') return
  klineLoadingMore.value = true
  klineMonths.value += 12
  try {
    const { code, name } = klineStockInfo.value
    await loadKlineData(code, name)
  } finally {
    klineLoadingMore.value = false
  }
}

const loadKlineData = async (code, name, sw_level1 = '', sw_level2 = '', sw_level3 = '') => {
  klineStockInfo.value = { code, name, sw_level1, sw_level2, sw_level3 }
  klineData.value = []

  const monthsParam = klinePeriod.value === 'month' ? 0 : klineMonths.value
  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/stocks/${code}/kline-local?months=${monthsParam}&period=${klinePeriod.value}&adjust=${klineAdjust.value}`)
    if (res.ok) {
      const data = await res.json()
      if (data.success && data.kline && data.kline.length > 0) {
        klineData.value = data.kline
        if (klinePeriod.value === 'day' && selectedIndicators.value.length > 0) {
          await loadIndicatorData()
        }
        return
      }
    }
  } catch (e) {
    console.warn('本地 K 线数据读取失败:', e.message)
  }

  // 回退：SDK 查询（仅日线）
  if (klinePeriod.value !== 'day') {
    return
  }

  const endDt = new Date()
  const startDt = new Date()
  startDt.setMonth(startDt.getMonth() - klineMonths.value)
  const start = startDt.toISOString().slice(0, 10).replace(/-/g, '')
  const end = endDt.toISOString().slice(0, 10).replace(/-/g, '')

  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/market/kline?stock_code=${code}&period=day&start_date=${start}&end_date=${end}&adjust=${klineAdjust.value}`)
    const data = await res.json()
    klineData.value = data.data?.kline || []
    if (selectedIndicators.value.length > 0) {
      await loadIndicatorData()
    }
  } catch (e) {
    console.error('加载 K 线数据失败:', e)
  }
}

const prevStock = async () => {
  if (!hasPrevStock.value) return
  const idx = klineStockIndex.value - 1
  const row = trades.value[idx]
  if (!row) return
  klineStockIndex.value = idx
  klineIndicators.value = {}
  await loadKlineData(row.stock_code, row.stock_name)
}

const nextStock = async () => {
  if (!hasNextStock.value) return
  const idx = klineStockIndex.value + 1
  const row = trades.value[idx]
  if (!row) return
  klineStockIndex.value = idx
  klineIndicators.value = {}
  await loadKlineData(row.stock_code, row.stock_name)
}

const toggleIndicator = (key) => {
  const idx = selectedIndicators.value.indexOf(key)
  if (idx >= 0) {
    selectedIndicators.value.splice(idx, 1)
  } else {
    selectedIndicators.value.push(key)
  }
  loadIndicatorData()
}

const loadIndicatorData = async () => {
  if (!klineStockInfo.value.code || selectedIndicators.value.length === 0) {
    klineIndicators.value = {}
    return
  }
  if (klinePeriod.value !== 'day') {
    ElMessage.warning('技术指标仅支持日线模式')
    return
  }

  const code = klineStockInfo.value.code
  const dates = klineData.value.map(d => d.trade_date)
  if (dates.length === 0) return

  const startDate = dates[0]
  const endDate = dates[dates.length - 1]

  const fields = []
  for (const key of selectedIndicators.value) {
    if (key === 'boll') {
      fields.push('boll_upper', 'boll_middle', 'boll_lower')
    } else {
      fields.push(key)
    }
  }

  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/factors/${code}?start_date=${startDate}&end_date=${endDate}&fields=${fields.join(',')}`)
    const data = await res.json()
    if (data.success && data.factors) {
      const indicators = {}
      for (const field of fields) {
        indicators[field] = data.factors.map(f => ({
          trade_date: f.trade_date,
          value: f[field]
        }))
      }
      klineIndicators.value = indicators
    }
  } catch (e) {
    console.error('[指标] 加载失败:', e)
  }
}

onMounted(async () => {
  await loadTrades()
  await loadNotifications()
})

// 切换账户时刷新数据
watch(currentAccountId, async () => {
  await loadTrades()
  await loadNotifications()
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

/* 分栏布局 */
.split-container {
  display: flex;
  gap: 0;
  min-height: 400px;
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

.splitter:hover { background: #dcdfe6; }

.splitter-handle {
  width: 2px;
  height: 30px;
  border-left: 1px solid #c0c4cc;
  border-right: 1px solid #c0c4cc;
}

.right-panel {
  flex: 1;
  min-width: 400px;
  overflow-x: auto;
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
