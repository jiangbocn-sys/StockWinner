<template>
  <div class="kline-wrapper">
    <!-- 钻取模式下的工具栏 -->
    <div v-if="drillDownMode" class="drill-toolbar">
      <el-button size="small" @click="exitDrillDown">
        <el-icon><ArrowLeft /></el-icon> 返回日线
      </el-button>
      <span class="drill-date">{{ drillDownDate }}</span>
      <el-radio-group v-model="minutePeriod" size="small" @change="loadMinuteData">
        <el-radio-button value="1m">1分</el-radio-button>
        <el-radio-button value="5m">5分</el-radio-button>
        <el-radio-button value="15m">15分</el-radio-button>
        <el-radio-button value="30m">30分</el-radio-button>
        <el-radio-button value="60m">60分</el-radio-button>
      </el-radio-group>
    </div>

    <!-- 图表容器 -->
    <div ref="chartRef" :style="{ width: '100%', height: actualHeight }"></div>

    <!-- 加载状态 -->
    <div v-if="minuteLoading" class="loading-overlay">
      <el-icon class="is-loading"><Loading /></el-icon>
      <span>加载分钟数据...</span>
    </div>
  </div>
</template>

<script setup>
import { ref, watch, computed, onBeforeUnmount, nextTick } from 'vue'
import * as echarts from 'echarts'
import { ArrowLeft, Loading } from '@element-plus/icons-vue'
import { ElMessage } from 'element-plus'

const props = defineProps({
  data: { type: Array, required: true },
  tradeMarks: { type: Object, default: null },
  showVolume: { type: Boolean, default: true },
  height: { type: String, default: '550px' },
  title: { type: String, default: '' },
  indicators: { type: Object, default: null },
  indicatorConfig: { type: Array, default: () => [] },
  // 新增：钻取功能所需 props
  stockCode: { type: String, default: '' },
  accountId: { type: String, default: '' },
  enableDrillDown: { type: Boolean, default: true },
  // 复权设置（钻取分钟数据时使用）
  adjust: { type: String, default: 'forward' },  // none/forward
})

const chartRef = ref(null)
let chart = null

// 钻取状态
const drillDownMode = ref(false)
const drillDownDate = ref('')
const minutePeriod = ref('5m')
const minuteData = ref([])
const minuteLoading = ref(false)

// 实际高度（钻取模式下减去工具栏高度）
const actualHeight = computed(() => {
  return drillDownMode.value ? '510px' : props.height
})

// 分钟周期配置
// 每日条数：1m≈240, 5m≈48, 15m≈16, 30m≈8, 60m≈4
const dailyBarsMap = { '1m': 240, '5m': 48, '15m': 16, '30m': 8, '60m': 4 }

// 显示窗口条数：约500条K线
const windowBars = 500

// 总共需要获取的天数 = ceil(500条 / 每日条数)
// 例如：5分钟需要 ceil(500/48) = 11天，取前5天+后5天
const totalDaysMap = {
  '1m': 3,    // 3天 ≈ 720条（取前1天+后1天）
  '5m': 11,   // 11天 ≈ 528条（取前5天+后5天）
  '15m': 32,  // 32天 ≈ 512条（取前16天+后16天）
  '30m': 63,  // 63天 ≈ 504条（取前31天+后31天）
  '60m': 126  // 126天 ≈ 504条（取前63天+后63天）
}

const renderChart = () => {
  if (!chartRef.value) return
  if (!chart) {
    chart = echarts.init(chartRef.value)
  }

  const dates = props.data.map(d => String(d.trade_date))
  const ohlcValues = props.data.map(d => [d.open, d.close, d.low, d.high])
  const volumes = props.data.map(d => d.volume || 0)

  const series = [
    {
      name: 'K线',
      type: 'candlestick',
      data: ohlcValues,
      xAxisIndex: 0,
      yAxisIndex: 0,
      itemStyle: { color: '#ef232a', color0: '#14b143', borderColor: '#ef232a', borderColor0: '#14b143' },
      markPoint: props.tradeMarks ? buildMarkPoints() : { data: [] },
      markLine: props.tradeMarks ? buildMarkLines() : { data: [] },
    },
  ]

  // 添加技术指标线
  if (props.indicators && props.indicatorConfig.length > 0) {
    for (const cfg of props.indicatorConfig) {
      const indicatorData = props.indicators[cfg.key]
      if (indicatorData && indicatorData.length > 0) {
        const matchedData = matchIndicatorData(dates, indicatorData)
        if (matchedData.length > 0) {
          series.push({
            name: cfg.name,
            type: 'line',
            data: matchedData,
            xAxisIndex: 0,
            yAxisIndex: 0,
            smooth: cfg.smooth || false,
            lineStyle: { width: cfg.width || 1, color: cfg.color || '#999' },
            symbol: 'none',
          })
        }
      }
    }
  }

  const xAxis = [
    { type: 'category', data: dates, gridIndex: 0, axisLabel: { show: true, rotate: 30, fontSize: 10 } },
    { type: 'category', data: dates, gridIndex: 1, axisLabel: { show: true, fontSize: 10 } },
  ]
  const yAxis = [
    { type: 'value', scale: true, gridIndex: 0 },
    { type: 'value', scale: true, gridIndex: 1, splitNumber: 2 },
  ]
  const grid = [
    { left: '8%', right: '4%', top: props.title ? '14%' : '8%', height: props.showVolume ? '55%' : '70%' },
  ]
  const dataZoomXAxisIndices = [0, 1]

  if (props.showVolume) {
    series.push({
      name: '成交量',
      type: 'bar',
      data: volumes,
      xAxisIndex: 1,
      yAxisIndex: 1,
      itemStyle: {
        color: (param) => {
          const idx = param.dataIndex
          if (idx < props.data.length) {
            return props.data[idx].close >= props.data[idx].open ? '#ef232a' : '#14b143'
          }
          return '#999'
        },
      },
    })
    grid.push({ left: '8%', right: '4%', top: '68%', height: '22%' })
  } else {
    dataZoomXAxisIndices[0] = 0
  }

  const option = {
    title: props.title ? { text: props.title, left: 'center', textStyle: { fontSize: 12 } } : undefined,
    tooltip: {
      trigger: 'axis',
      axisPointer: { type: 'cross' },
      formatter: (params) => {
        const p = params[0]
        if (!p || !p.value) return ''
        const v = p.value
        const idx = dates.indexOf(p.name)
        const vol = idx >= 0 ? volumes[idx] : '-'
        let tip = `${p.name}<br/>开: ${v[1]}  收: ${v[2]}  低: ${v[3]}  高: ${v[4]}<br/>量: ${typeof vol === 'number' ? vol.toLocaleString() : vol}`
        if (params.length > 1) {
          for (let i = 1; i < params.length; i++) {
            const indicatorParam = params[i]
            if (indicatorParam.seriesName && indicatorParam.value != null) {
              tip += `<br/>${indicatorParam.seriesName}: ${Number(indicatorParam.value).toFixed(2)}`
            }
          }
        }
        return tip
      },
    },
    legend: props.indicatorConfig.length > 0 ? {
      data: props.indicatorConfig.map(c => c.name),
      top: props.title ? '4%' : '2%',
      left: 'center',
      itemWidth: 20,
      itemHeight: 10,
    } : undefined,
    grid,
    xAxis,
    yAxis,
    series,
    dataZoom: [{ type: 'inside', start: 0, end: 100 }, { type: 'slider', xAxisIndex: dataZoomXAxisIndices, start: 0, end: 100 }],
  }

  chart.setOption(option, true)

  // 添加双击事件（钻取功能）
  if (props.enableDrillDown && !chart._dblclickBound) {
    chart.getZr().on('dblclick', handleDblClick)
    chart._dblclickBound = true
  }

  if (!chart._resizeBound) {
    const handler = () => chart && chart.resize()
    window.addEventListener('resize', handler)
    chart._resizeBound = true
  }
}

// 双击事件处理
const handleDblClick = (params) => {
  if (!props.enableDrillDown || drillDownMode.value) return
  if (!props.stockCode || !props.accountId) {
    console.warn('[KlineChart] 缺少 stockCode 或 accountId，无法进入钻取')
    return
  }

  try {
    const pointInPixel = [params.offsetX, params.offsetY]
    const pointInGrid = chart.convertFromPixel({ gridIndex: 0 }, pointInPixel)
    const xIndex = Math.round(pointInGrid[0])

    if (xIndex >= 0 && xIndex < props.data.length) {
      const clickedDate = props.data[xIndex].trade_date
      enterDrillDown(clickedDate)
    }
  } catch (e) {
    console.warn('[KlineChart] 双击位置解析失败:', e)
  }
}

// 进入钻取模式
const enterDrillDown = (date, period = '5m') => {
  if (!props.stockCode || !props.accountId) return
  drillDownMode.value = true
  drillDownDate.value = String(date).slice(0, 10)
  minutePeriod.value = period
  loadMinuteData()
}

// 退出钻取模式
const exitDrillDown = () => {
  drillDownMode.value = false
  minuteData.value = []
  // 完全销毁图表，强制重新初始化
  if (chart) {
    chart.getZr().off('dblclick')
    chart.dispose()
    chart = null
  }
  nextTick(renderChart)
}

// 加载分钟数据
const loadMinuteData = async () => {
  if (!props.stockCode || !props.accountId) {
    minuteLoading.value = false
    return
  }

  minuteLoading.value = true
  console.log('[KlineChart] 加载分钟数据:', drillDownDate.value, minutePeriod.value)

  // 根据周期计算数据获取范围（前后各取一半天数，填满500条窗格）
  const totalDays = totalDaysMap[minutePeriod.value] || 10
  const halfDays = Math.ceil(totalDays / 2)

  // 计算日期范围：指定日期前 halfDays 天 到 指定日期后 halfDays 天
  const targetDate = new Date(drillDownDate.value)
  const startDate = new Date(targetDate)
  startDate.setDate(startDate.getDate() - halfDays)  // 往前取 halfDays 天

  const endDate = new Date(targetDate)
  endDate.setDate(endDate.getDate() + halfDays + 1)  // 往后取 halfDays 天 + SDK半开区间加1天

  const startDateStr = startDate.toISOString().slice(0, 10).replace(/-/g, '')
  const endDateStr = endDate.toISOString().slice(0, 10).replace(/-/g, '')

  console.log('[KlineChart] 分钟数据请求:', startDateStr, '-', endDateStr, '周期:', minutePeriod.value, '前后各:', halfDays, '天')

  try {
    const res = await fetch(
      `/api/v1/ui/${props.accountId}/market/kline` +
      `?stock_code=${props.stockCode}` +
      `&period=${minutePeriod.value}` +
      `&start_date=${startDateStr}` +
      `&end_date=${endDateStr}` +
      `&limit=1000` +
      `&adjust=${props.adjust}`
    )
    const data = await res.json()

    console.log('[KlineChart] 分钟数据响应:', data.success, '条数:', data.data?.kline?.length || 0)

    if (data.success && data.data?.kline && data.data.kline.length > 0) {
      // 检查数据排序：第一条和最后一条的日期，确保从旧到新
      const klineData = data.data.kline
      const firstDate = klineData[0]?.trade_date || ''
      const lastDate = klineData[klineData.length - 1]?.trade_date || ''
      console.log('[KlineChart] 数据范围:', firstDate, '-', lastDate)

      // 如果第一条日期比最后一条日期新，说明数据是从新到旧排序，需要反转
      if (firstDate > lastDate) {
        console.log('[KlineChart] 数据从新到旧，正在反转')
        minuteData.value = klineData.reverse()
      } else {
        minuteData.value = klineData
      }
      nextTick(renderMinuteChart)
    } else {
      ElMessage.warning(data.message || '分钟数据获取失败')
      minuteLoading.value = false
    }
  } catch (e) {
    console.error('[KlineChart] 分钟数据请求异常:', e)
    ElMessage.error('分钟数据请求异常')
    minuteLoading.value = false
  }
}

// 渲染分钟 K 线（带高亮区域）
const renderMinuteChart = () => {
  // 确保 chart 已初始化
  if (!chartRef.value) {
    minuteLoading.value = false
    return
  }
  if (!chart) {
    chart = echarts.init(chartRef.value)
  }

  if (minuteData.value.length === 0) {
    minuteLoading.value = false
    return
  }

  minuteLoading.value = false

  const dates = minuteData.value.map(d => {
    const t = d.trade_date
    if (typeof t === 'string') return t
    if (t instanceof Date) return t.toLocaleString('zh-CN', { hour12: false })
    return String(t)
  })
  const ohlc = minuteData.value.map(d => [d.open, d.close, d.low, d.high])
  const volumes = minuteData.value.map(d => d.volume || 0)

  // 找到目标日期的分钟 K 线索引范围
  const targetDatePrefix = drillDownDate.value  // YYYY-MM-DD
  const targetIndices = []
  for (let i = 0; i < dates.length; i++) {
    const d = dates[i]
    if (d.startsWith(targetDatePrefix) || d.includes(targetDatePrefix)) {
      targetIndices.push(i)
    }
  }

  console.log('[KlineChart] 目标日期:', targetDatePrefix, '匹配索引:', targetIndices.length, '总数据:', dates.length)
  if (targetIndices.length > 0) {
    console.log('[KlineChart] 匹配范围:', targetIndices[0], '-', targetIndices[targetIndices.length - 1])
  }

  // 高亮区域配置（ECharts markArea 格式：每个区域是 [{起点}, {终点}]）
  const markAreaData = targetIndices.length > 0 ? [
    [{ xAxis: targetIndices[0] }, { xAxis: targetIndices[targetIndices.length - 1] }]
  ] : []

  // 计算显示窗口位置：让指定日期居中
  const totalBars = minuteData.value.length
  const dailyBars = dailyBarsMap[minutePeriod.value] || 48
  const targetDayBars = targetIndices.length > 0 ? targetIndices.length : dailyBars

  // 居中计算：(500条 - 当日条数) / 2 / 每日条数 = 需要往前显示的天数
  const offsetDays = Math.round((windowBars - targetDayBars) / 2 / dailyBars)
  const offsetBars = offsetDays * dailyBars

  let startPercent, endPercent
  if (targetIndices.length > 0) {
    // 指定日期的中间位置
    const targetCenter = Math.floor((targetIndices[0] + targetIndices[targetIndices.length - 1]) / 2)

    // 理想窗口：从指定日期往前 offsetBars 条开始，显示 windowBars 条
    const idealStart = targetCenter - offsetBars
    const idealEnd = idealStart + windowBars

    // 边界处理
    if (idealStart < 0) {
      // 左侧不足，从 0 开始
      startPercent = 0
      endPercent = Math.min(100, windowBars / totalBars * 100)
    } else if (idealEnd > totalBars) {
      // 右侧不足，显示最后 windowBars 条（指定日期偏右是正常的）
      const actualStart = Math.max(0, totalBars - windowBars)
      startPercent = actualStart / totalBars * 100
      endPercent = 100
    } else {
      // 正常情况：指定日期居中
      startPercent = idealStart / totalBars * 100
      endPercent = idealEnd / totalBars * 100
    }

    console.log('[KlineChart] 居中: targetCenter=', targetCenter, 'offset=', offsetBars, 'window=', windowBars, 'start=', startPercent.toFixed(1), 'end=', endPercent.toFixed(1))
  } else {
    // 无匹配：显示最新数据
    startPercent = Math.max(0, 100 - windowBars / totalBars * 100)
    endPercent = 100
    console.log('[KlineChart] 无匹配，显示右侧')
  }

  const option = {
    tooltip: {
      trigger: 'axis',
      axisPointer: { type: 'cross' },
      formatter: (params) => {
        if (!params[0]) return ''
        const p = params[0]
        const v = p.value
        const idx = p.dataIndex
        const vol = volumes[idx] || 0
        return `${dates[idx]}<br/>开: ${v[1]}  收: ${v[2]}  低: ${v[3]}  高: ${v[4]}<br/>量: ${vol.toLocaleString()}`
      }
    },
    axisPointer: { link: [{ xAxisIndex: [0, 1] }] },
    grid: [
      { left: '8%', right: '4%', top: '8%', height: '60%' },
      { left: '8%', right: '4%', top: '72%', height: '18%' }
    ],
    xAxis: [
      { type: 'category', data: dates, gridIndex: 0, axisLabel: { show: true, rotate: 45, fontSize: 10 } },
      { type: 'category', data: dates, gridIndex: 1, axisLabel: { show: true, fontSize: 10 } }
    ],
    yAxis: [
      { type: 'value', scale: true, gridIndex: 0 },
      { type: 'value', scale: true, gridIndex: 1, splitNumber: 2 }
    ],
    dataZoom: [
      { type: 'inside', xAxisIndex: [0, 1], start: startPercent, end: endPercent },
      { type: 'slider', xAxisIndex: [0, 1], start: startPercent, end: endPercent }
    ],
    series: [
      {
        name: 'K线',
        type: 'candlestick',
        data: ohlc,
        xAxisIndex: 0,
        yAxisIndex: 0,
        itemStyle: { color: '#ef232a', color0: '#14b143', borderColor: '#ef232a', borderColor0: '#14b143' },
        markArea: {
          data: markAreaData,
          silent: true,
          itemStyle: { color: 'rgba(255, 182, 193, 0.3)' }
        }
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
            if (idx < ohlc.length) {
              return ohlc[idx][1] >= ohlc[idx][0] ? '#ef232a' : '#14b143'
            }
            return '#999'
          }
        }
      }
    ]
  }

  chart.setOption(option, true)
}

// 匹配指标数据日期与 K 线日期
const matchIndicatorData = (klineDates, indicatorData) => {
  if (!indicatorData || indicatorData.length === 0) return []

  const validData = indicatorData.filter(d => d != null)
  if (validData.length === 0) return []

  const normalizeDate = (d) => {
    const s = String(d).slice(0, 10)
    if (s.length === 8 && /^\d{8}$/.test(s)) {
      return `${s.slice(0,4)}-${s.slice(4,6)}-${s.slice(6,8)}`
    }
    return s
  }

  const firstItem = validData[0]
  if (typeof firstItem === 'object' && firstItem.trade_date != null) {
    const indicatorMap = {}
    for (const d of validData) {
      if (d && d.trade_date != null && d.value != null) {
        const dateKey = normalizeDate(d.trade_date)
        indicatorMap[dateKey] = d.value
      }
    }
    return klineDates.map(date => {
      const dateKey = normalizeDate(date)
      return indicatorMap[dateKey] ?? null
    })
  }

  if (typeof firstItem === 'number') {
    if (validData.length === klineDates.length) {
      return validData
    }
    const offset = klineDates.length - validData.length
    return klineDates.map((_, i) => i >= offset ? validData[i - offset] : null)
  }

  return []
}

const buildMarkPoints = () => {
  const m = props.tradeMarks
  if (!m) return { data: [] }
  const data = []
  const dates = props.data.map(d => String(d.trade_date))

  if (m.buyDate) {
    const buyIdx = dates.findIndex(d => d.slice(0, 8) === m.buyDate.slice(0, 8) || d.startsWith(m.buyDate.slice(0, 10)))
    if (buyIdx >= 0) {
      data.push({
        name: '买入',
        coord: [buyIdx, props.data[buyIdx].low],
        value: '买入',
        itemStyle: { color: '#409EFF' },
      })
    }
  }
  if (m.sellDate) {
    const sellIdx = dates.findIndex(d => d.slice(0, 8) === m.sellDate.slice(0, 8) || d.startsWith(m.sellDate.slice(0, 10)))
    if (sellIdx >= 0) {
      data.push({
        name: '卖出',
        coord: [sellIdx, props.data[sellIdx].high],
        value: '卖出',
        itemStyle: { color: '#67C23A' },
      })
    }
  }
  return { data, symbolSize: 50 }
}

const buildMarkLines = () => {
  const m = props.tradeMarks
  if (!m) return { data: [] }
  const data = []
  if (m.buyPrice) {
    data.push({ name: '买入价', yAxis: m.buyPrice, lineStyle: { color: '#409EFF', type: 'dashed' }, label: { formatter: '买入 {c}' } })
  }
  if (m.sellPrice) {
    data.push({ name: '卖出价', yAxis: m.sellPrice, lineStyle: { color: '#67C23A', type: 'dashed' }, label: { formatter: '卖出 {c}' } })
  }
  return { data, silent: true }
}

watch(() => props.data, () => {
  if (!drillDownMode.value) {
    // 切换股票时完全重置图表，避免状态污染
    if (chart) {
      chart.dispose()
      chart = null
    }
    nextTick(renderChart)
  } else {
    // 钻取模式下切换股票，重新加载分钟数据
    if (props.stockCode && props.accountId && drillDownDate.value) {
      console.log('[KlineChart] 钻取模式切换股票，重新加载分钟数据:', props.stockCode)
      minuteData.value = []
      if (chart) {
        chart.dispose()
        chart = null
      }
      nextTick(loadMinuteData)
    }
  }
}, { deep: true })

watch(() => props.tradeMarks, () => {
  if (!drillDownMode.value) {
    nextTick(renderChart)
  }
}, { deep: true })

watch(() => props.indicators, () => {
  if (!drillDownMode.value) {
    nextTick(renderChart)
  }
}, { deep: true })

watch(() => props.indicatorConfig, () => {
  if (!drillDownMode.value) {
    nextTick(renderChart)
  }
}, { deep: true })

onBeforeUnmount(() => {
  if (chart) {
    chart.getZr().off('dblclick')
    chart.dispose()
    chart = null
  }
})

defineExpose({
  resize: () => chart?.resize(),
  // 钻取状态
  drillDownMode,
  drillDownDate,
  minutePeriod,
  // 方法
  enterDrillDown,
  exitDrillDown,
})
</script>

<style scoped>
.kline-wrapper {
  position: relative;
}

.drill-toolbar {
  display: flex;
  align-items: center;
  gap: 12px;
  padding: 8px 16px;
  background: #f5f7fa;
  border-radius: 4px;
  margin-bottom: 8px;
}

.drill-date {
  font-weight: 500;
  color: #303133;
}

.loading-overlay {
  position: absolute;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  background: rgba(255, 255, 255, 0.8);
  z-index: 10;
}

.loading-overlay .el-icon {
  font-size: 32px;
  margin-bottom: 8px;
}
</style>