<template>
  <!-- K 线图弹窗 -->
  <el-dialog v-model="dialogVisible" :title="dialogTitle" width="85%" top="5vh" @close="handleClose">
    <!-- 导航栏（可选） -->
    <div v-if="showNavigation" class="kline-nav">
      <el-button size="small" @click="$emit('prev-stock')" :disabled="!hasPrevStock">
        <el-icon><ArrowLeft /></el-icon> 上一只
      </el-button>
      <span class="kline-nav-text">{{ navText }}</span>
      <el-button size="small" @click="$emit('next-stock')" :disabled="!hasNextStock">
        下一只 <el-icon><ArrowRight /></el-icon>
      </el-button>
    </div>

    <!-- 控制栏 -->
    <div class="kline-controls" style="margin-bottom: 12px; display: flex; align-items: center; gap: 16px; flex-wrap: wrap">
      <!-- 周期选择 -->
      <el-radio-group v-model="period" size="small" @change="handlePeriodChange">
        <el-radio-button label="day">日线</el-radio-button>
        <el-radio-button label="week">周线</el-radio-button>
        <el-radio-button label="month">月线</el-radio-button>
      </el-radio-group>

      <!-- 复权选择（可选） -->
      <el-radio-group v-if="showAdjust" v-model="adjust" size="small" @change="handleAdjustChange">
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

      <!-- 加载更多按钮 -->
      <el-button size="small" @click="$emit('load-more')" :disabled="period === 'month' || loadingMore" v-if="period !== 'month'">
        <el-icon><Download /></el-icon> 加载更多
      </el-button>
      <span v-if="period === 'month'" style="color: #909399; font-size: 12px">月线已显示全部数据</span>

      <!-- 缠论分析按钮 -->
      <el-button size="small" type="warning" @click="showCzscChart" :loading="czscLoading">
        <el-icon><TrendCharts /></el-icon> 缠论分析
      </el-button>
    </div>

    <!-- K 线图表 -->
    <KlineChart ref="klineChartRef" :data="klineData" height="550px"
      :stockCode="stockInfo.code"
      :accountId="accountId"
      :enableDrillDown="true"
      :adjust="adjust"
      :indicators="indicators"
      :indicatorConfig="indicatorConfig" />
  </el-dialog>

  <!-- 缠论分析弹窗 -->
  <el-dialog v-model="czscVisible" :title="czscDialogTitle" width="90%" top="3vh">
    <div v-if="czscLoading" style="text-align: center; padding: 50px">
      <el-icon class="is-loading" :size="40"><Loading /></el-icon>
      <p style="margin-top: 16px; color: #909399">正在生成缠论分析图表...</p>
    </div>
    <iframe v-else-if="czscHtml" :srcdoc="czscHtml" style="width: 100%; height: 75vh; border: none;"></iframe>
    <div v-else style="text-align: center; padding: 50px; color: #909399">
      {{ czscError || '暂无数据' }}
    </div>
  </el-dialog>
</template>

<script setup>
import { ref, computed, watch } from 'vue'
import { ElMessage } from 'element-plus'
import { ArrowLeft, ArrowRight, Download, Setting, ArrowDown, TrendCharts, Loading } from '@element-plus/icons-vue'
import KlineChart from './KlineChart.vue'

// Props
const props = defineProps({
  visible: { type: Boolean, default: false },
  stockInfo: { type: Object, default: () => ({ code: '', name: '', sw_level1: '', sw_level2: '', sw_level3: '' }) },
  klineData: { type: Array, default: () => [] },
  accountId: { type: String, default: '' },
  showNavigation: { type: Boolean, default: true },
  showAdjust: { type: Boolean, default: true },
  hasPrevStock: { type: Boolean, default: false },
  hasNextStock: { type: Boolean, default: false },
  navText: { type: String, default: '' },
  loadingMore: { type: Boolean, default: false },
  indicators: { type: Object, default: () => {} },
})

// Events
const emit = defineEmits(['update:visible', 'prev-stock', 'next-stock', 'reload-kline', 'load-more', 'indicator-change'])

// 内部状态
const klineChartRef = ref(null)
const period = ref('day')
const adjust = ref('forward')
const selectedIndicators = ref([])
const czscVisible = ref(false)
const czscLoading = ref(false)
const czscHtml = ref('')
const czscError = ref('')

// 对话框状态
const dialogVisible = computed({
  get: () => props.visible,
  set: (val) => emit('update:visible', val)
})

// 对话框标题
const dialogTitle = computed(() => {
  const { name, code, sw_level1, sw_level2, sw_level3 } = props.stockInfo
  const industryParts = [sw_level1, sw_level2, sw_level3].filter(Boolean)
  const industryStr = industryParts.length > 0 ? ` [${industryParts.join(' - ')}]` : ''
  return `${name} (${code})${industryStr} K线走势`
})

// 缠论对话框标题
const czscDialogTitle = computed(() => {
  const { name, code } = props.stockInfo
  return `${name} (${code}) 缠论结构分析`
})

// 指标配置
const indicatorConfig = computed(() => {
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

// 周期变化
const handlePeriodChange = () => {
  emit('reload-kline', { period: period.value, adjust: adjust.value })
}

// 复权变化
const handleAdjustChange = () => {
  emit('reload-kline', { period: period.value, adjust: adjust.value })
}

// 切换指标
const toggleIndicator = (indicator) => {
  const idx = selectedIndicators.value.indexOf(indicator)
  if (idx >= 0) {
    selectedIndicators.value.splice(idx, 1)
  } else {
    selectedIndicators.value.push(indicator)
  }
  emit('indicator-change', selectedIndicators.value)
}

// 缠论分析
const showCzscChart = async () => {
  if (!props.klineData || props.klineData.length < 10) {
    ElMessage.warning('K 线数据不足，无法进行缠论分析')
    return
  }

  czscVisible.value = true
  czscLoading.value = true
  czscHtml.value = ''
  czscError.value = ''

  try {
    const token = localStorage.getItem('auth_token')
    const res = await fetch('/api/v1/ui/czsc/chart', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${token}`
      },
      body: JSON.stringify({
        stock_code: props.stockInfo.code,
        stock_name: props.stockInfo.name,
        kline_data: props.klineData,
        period: period.value
      })
    })

    const data = await res.json()

    if (data.success && data.html) {
      czscHtml.value = data.html
    } else {
      czscError.value = data.message || '缠论分析失败'
    }
  } catch (e) {
    console.error('缠论分析请求失败:', e)
    czscError.value = '请求失败: ' + e.message
  } finally {
    czscLoading.value = false
  }
}

// 关闭处理
const handleClose = () => {
  selectedIndicators.value = []
  czscHtml.value = ''
  czscError.value = ''
}

// 重置周期（供外部调用）
const resetPeriod = (newPeriod) => {
  period.value = newPeriod || 'day'
}

// 暴露方法
defineExpose({
  klineChartRef,
  resetPeriod,
  showCzscChart,
})
</script>

<style scoped>
.kline-nav {
  display: flex;
  align-items: center;
  gap: 12px;
  padding: 8px 0;
}

.kline-nav-text {
  font-weight: 500;
  color: #303133;
}
</style>