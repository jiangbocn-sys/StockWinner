<template>
  <div ref="chartRef" :style="{ width: '100%', height }"></div>
</template>

<script setup>
import { ref, watch, onBeforeUnmount, nextTick } from 'vue'
import * as echarts from 'echarts'

const props = defineProps({
  data: { type: Array, required: true },
  tradeMarks: { type: Object, default: null }, // { buyDate, buyPrice, sellDate, sellPrice }
  showVolume: { type: Boolean, default: true },
  height: { type: String, default: '550px' },
  title: { type: String, default: '' },
})

const chartRef = ref(null)
let chart = null

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

  const xAxis = [
    { type: 'category', data: dates, gridIndex: 0, axisLabel: { show: !props.showVolume } },
    { type: 'category', data: dates, gridIndex: 1 },
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
        return `${p.name}<br/>开: ${v[1]}  收: ${v[2]}  低: ${v[3]}  高: ${v[4]}<br/>量: ${typeof vol === 'number' ? vol.toLocaleString() : vol}`
      },
    },
    grid,
    xAxis,
    yAxis,
    series,
    dataZoom: [{ type: 'inside' }, { type: 'slider', xAxisIndex: dataZoomXAxisIndices }],
  }

  chart.setOption(option, true)

  if (!chart._resizeBound) {
    const handler = () => chart && chart.resize()
    window.addEventListener('resize', handler)
    chart._resizeBound = true
  }
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

watch(() => props.data, () => { nextTick(renderChart) }, { deep: true })
watch(() => props.tradeMarks, () => { nextTick(renderChart) }, { deep: true })

onBeforeUnmount(() => {
  if (chart) { chart.dispose(); chart = null }
})

defineExpose({ resize: () => chart?.resize() })
</script>
