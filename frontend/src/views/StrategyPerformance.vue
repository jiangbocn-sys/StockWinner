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
        <template #header>
          <div class="card-header">
            <span>策略排行榜</span>
            <el-dropdown @command="(fmt) => handleExportAllStats(fmt)">
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
          </div>
        </template>
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
            <el-space>
              <el-dropdown @command="(fmt) => handleExportSelections(fmt)">
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
              <el-button type="primary" size="small" @click="loadPerformance"><el-icon><Refresh /></el-icon>刷新</el-button>
            </el-space>
          </div>
        </template>
        <el-table :data="sortedSelections" stripe @row-dblclick="showKline" @sort-change="onSelectionSortChange">
          <el-table-column prop="stock_code" label="代码" width="110" sortable="custom" />
          <el-table-column prop="stock_name" label="名称" width="120" sortable="custom" />
          <el-table-column prop="selected_at" label="选出日期" width="180" sortable="custom">
            <template #default="{ row }">{{ (row.selected_at || '').split('T')[0] }}</template>
          </el-table-column>
          <el-table-column prop="trigger_price" label="选出价" width="100" align="right" sortable="custom">
            <template #default="{ row }">¥{{ row.trigger_price || '-' }}</template>
          </el-table-column>
          <el-table-column prop="buy_price_actual" label="买入价" width="100" align="right" sortable="custom">
            <template #default="{ row }">¥{{ row.buy_price_actual || '-' }}</template>
          </el-table-column>
          <el-table-column prop="sell_price" label="卖出价" width="100" align="right" sortable="custom">
            <template #default="{ row }">¥{{ row.sell_price || '-' }}</template>
          </el-table-column>
          <el-table-column prop="profit_loss" label="盈亏" width="120" align="right" sortable="custom">
            <template #default="{ row }">
              <span v-if="row.profit_loss !== null" :class="row.profit_loss >= 0 ? 'profit-positive' : 'profit-negative'">
                {{ row.profit_loss >= 0 ? '+' : '' }}¥{{ formatNumber(Math.abs(row.profit_loss)) }}
              </span>
              <span v-else>-</span>
            </template>
          </el-table-column>
          <el-table-column prop="bought" label="状态" width="80" align="center" sortable="custom">
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
import { ref, onMounted, computed } from 'vue'
import { Refresh, Download, ArrowLeft, ArrowRight, Setting, ArrowDown } from '@element-plus/icons-vue'
import { useAccountStore } from '../stores/account'
import NavBar from '../components/NavBar.vue'
import KlineChart from '../components/KlineChart.vue'
import { exportTable as doExport } from '@/utils/exportHelper'
import * as echarts from 'echarts'

const accountStore = useAccountStore()
const currentAccountId = ref(accountStore.currentAccountId)

const strategies = ref([])
const allStats = ref([])
const selections = ref([])
const selectedStrategyId = ref(null)
const loading = ref(false)
const strategySelectRef = ref(null)

// 选股明细排序
const selSortProp = ref('selected_at')
const selSortOrder = ref('descending')
const onSelectionSortChange = ({ prop, order }) => {
  selSortProp.value = prop || 'selected_at'
  selSortOrder.value = order || 'descending'
}
const sortedSelections = computed(() => {
  const arr = [...selections.value]
  const prop = selSortProp.value
  const desc = selSortOrder.value === 'descending'
  arr.sort((a, b) => {
    const av = a[prop]; const bv = b[prop]
    if (av == null && bv == null) return 0
    if (av == null) return desc ? 1 : -1
    if (bv == null) return desc ? -1 : 1
    const cmp = typeof av === 'number' ? av - bv : String(av).localeCompare(String(bv))
    return desc ? -cmp : cmp
  })
  return arr
})

const stats = ref({
  win_rate: 0, win_count: 0, lose_count: 0, total_trades: 0,
  total_pnl: 0, total_profit: 0, total_loss: 0,
  execution_rate: 0, bought_count: 0, total_selections: 0,
  avg_holding_days: 0, profit_factor: 0
})

const equityChartRef = ref(null)
let equityChart = null

const allStatsColumns = [
  { label: '策略名', prop: 'name' },
  { label: '选出', prop: 'total_selections' },
  { label: '买入', prop: 'bought_count' },
  { label: '交易', prop: 'total_trades' },
  { label: '胜率', prop: 'win_rate' },
  { label: '总盈亏', prop: 'total_pnl' },
  { label: '执行率', prop: 'execution_rate' },
]

const selectionColumns = [
  { label: '代码', prop: 'stock_code' },
  { label: '名称', prop: 'stock_name' },
  { label: '选出日期', prop: 'selected_at' },
  { label: '选出价', prop: 'trigger_price' },
  { label: '买入价', prop: 'buy_price_actual' },
  { label: '卖出价', prop: 'sell_price' },
  { label: '盈亏', prop: 'profit_loss' },
  { label: '状态', prop: 'bought' },
]

const handleExportAllStats = (format) => {
  const data = allStats.value.map(s => ({
    ...s,
    win_rate: s.win_rate + '%',
    total_pnl: '¥' + formatNumber(s.total_pnl),
    execution_rate: s.execution_rate + '%',
  }))
  doExport(allStatsColumns, data, '策略排行榜', format)
}

const handleExportSelections = (format) => {
  const data = selections.value.map(s => ({
    ...s,
    selected_at: (s.selected_at || '').split('T')[0],
    trigger_price: s.trigger_price != null ? '¥' + s.trigger_price : '-',
    buy_price_actual: s.buy_price_actual != null ? '¥' + s.buy_price_actual : '-',
    sell_price: s.sell_price != null ? '¥' + s.sell_price : '-',
    profit_loss: s.profit_loss != null ? (s.profit_loss >= 0 ? '+' : '') + '¥' + formatNumber(Math.abs(s.profit_loss)) : '-',
    bought: s.bought ? '已买' : '未买',
  }))
  doExport(selectionColumns, data, '选股明细', format)
}

import { formatNumber } from '../utils/format'

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
const hasNextStock = computed(() => klineStockIndex.value >= 0 && klineStockIndex.value < selections.value.length - 1)
const klineNavText = computed(() => {
  const total = selections.value.length
  const idx = klineStockIndex.value
  if (idx < 0 || total === 0) return ''
  return `${idx + 1} / ${total}`
})

const showKline = async (row) => {
  const idx = selections.value.findIndex(s => s.stock_code === row.stock_code)
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
  const row = selections.value[idx]
  if (!row) return
  klineStockIndex.value = idx
  klineIndicators.value = {}
  await loadKlineData(row.stock_code, row.stock_name)
}

const nextStock = async () => {
  if (!hasNextStock.value) return
  const idx = klineStockIndex.value + 1
  const row = selections.value[idx]
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
