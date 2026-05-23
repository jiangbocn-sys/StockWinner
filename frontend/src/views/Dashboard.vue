<template>
  <div class="layout-container">
    <NavBar />
    <el-main class="main-content">
      <el-container>
        <!-- 侧边栏 -->
        <el-aside width="200px" class="sidebar">
          <el-menu
            :default-active="currentAccountId"
            background-color="#f5f7fa"
          >
            <el-sub-menu index="accounts">
              <template #title>
                <el-icon><User /></el-icon>
                <span>账户列表</span>
              </template>
              <el-menu-item
                v-for="acc in accounts"
                :key="acc.account_id"
                :index="acc.account_id"
                :class="{ 'active-account': acc.account_id === currentAccountId, 'other-account': acc.account_id !== currentAccountId }"
                :disabled="acc.account_id !== currentAccountId"
              >
                {{ acc.display_name }}
              </el-menu-item>
            </el-sub-menu>
          </el-menu>
        </el-aside>

        <!-- 主内容区 -->
        <el-main class="dashboard-main">
          <h2>仪表盘 - {{ currentAccount?.display_name || currentAccountId }}</h2>

          <!-- 系统健康度 -->
          <el-card class="health-card" :class="{ 'health-card--unhealthy': healthStatus === 'unhealthy' }">
            <template #header>
              <div class="card-header">
                <span>系统健康度</span>
                <el-tag :type="healthStatus === 'healthy' ? 'success' : 'danger'">
                  {{ healthStatus === 'healthy' ? '正常' : '异常' }}
                </el-tag>
              </div>
            </template>
            <el-descriptions :column="5" border>
              <el-descriptions-item label="运行时长">{{ uptimeText }}</el-descriptions-item>
              <el-descriptions-item label="版本">{{ appVersion }}</el-descriptions-item>
              <el-descriptions-item label="CPU">{{ cpuPercent }}%</el-descriptions-item>
              <el-descriptions-item label="内存">{{ memoryMb }} MB</el-descriptions-item>
              <el-descriptions-item label="硬盘">{{ diskPercent }}%</el-descriptions-item>
            </el-descriptions>
            <el-alert v-if="sdkIssues.length > 0" type="error" :closable="false" style="margin-top: 15px">
              <template #title>健康异常</template>
              <div v-for="(issue, idx) in sdkIssues" :key="idx" style="margin-top: 5px">
                <el-icon><WarningFilled /></el-icon> {{ issue }}
              </div>
              <div v-if="sdkErrorTime" style="margin-top: 5px; color: #909399; font-size: 12px">
                最近一次异常: {{ sdkErrorTime }}
              </div>
            </el-alert>
          </el-card>

          <!-- 数据通道状态 -->
          <el-card class="data-sources-card" v-if="dataSources.length > 0">
            <template #header>
              <div class="card-header">
                <span>数据通道状态</span>
                <div>
                  <el-tag size="small" :type="allSourcesConnected ? 'success' : 'warning'">
                    {{ connectedCount }}/{{ dataSources.length }} 已连接
                  </el-tag>
                  <el-button size="small" @click="refreshDataSources" :loading="checkingHealth" style="margin-left: 8px">
                    <el-icon><Refresh /></el-icon> 检测
                  </el-button>
                </div>
              </div>
            </template>
            <el-descriptions :column="3" border>
              <el-descriptions-item
                v-for="ds in dataSources"
                :key="ds.provider_id"
                :label="ds.display_name"
              >
                <el-tag :type="statusTagType(ds.status)" size="small">
                  {{ statusText(ds.status) }}
                  <el-icon v-if="ds._checking" class="is-loading" style="margin-left: 4px"><Loading /></el-icon>
                </el-tag>
                <span v-if="ds.latency_ms >= 0" style="color: #909399; font-size: 12px; margin-left: 6px">
                  {{ ds.latency_ms }}ms
                </span>
                <div v-if="ds._checking" style="margin-top: 4px; font-size: 12px; color: #409eff">
                  正在检测...
                </div>
                <div v-else-if="ds.error_message" class="error-text" style="margin-top: 4px; font-size: 12px">
                  {{ ds.error_message }}
                </div>
              </el-descriptions-item>
            </el-descriptions>
          </el-card>

          <!-- 系统实时指标 -->
          <el-row :gutter="20" class="stats-row">
            <el-col :span="12">
              <el-card class="stat-card">
                <template #header><span>数据库吞吐</span></template>
                <el-descriptions :column="2" border>
                  <el-descriptions-item label="查询/秒">{{ dbThroughput.queries_per_sec }}</el-descriptions-item>
                  <el-descriptions-item label="读取/秒">{{ dbThroughput.reads_per_sec }}</el-descriptions-item>
                  <el-descriptions-item label="写入/秒">{{ dbThroughput.writes_per_sec }}</el-descriptions-item>
                  <el-descriptions-item label="读取行/秒">{{ dbThroughput.rows_read_per_sec }}</el-descriptions-item>
                  <el-descriptions-item label="累计查询" :span="2">{{ formatNumber(dbThroughput.total_queries) }}</el-descriptions-item>
                </el-descriptions>
              </el-card>
            </el-col>
            <el-col :span="12">
              <el-card class="stat-card">
                <template #header><span>SDK 调用统计</span></template>
                <el-descriptions :column="2" border>
                  <el-descriptions-item label="近60秒调用">{{ sdkMetrics.recent_60s.calls }} 次</el-descriptions-item>
                  <el-descriptions-item label="近60秒返回">{{ sdkMetrics.recent_60s.rows }} 行</el-descriptions-item>
                  <el-descriptions-item label="成功率">{{ sdkMetrics.recent_60s.success_rate }}%</el-descriptions-item>
                  <el-descriptions-item label="活跃方法">
                    <el-tag v-for="m in sdkMetrics.recent_60s.active_methods" :key="m" size="small" style="margin: 1px">{{ m }}</el-tag>
                  </el-descriptions-item>
                  <el-descriptions-item label="累计调用" :span="2">{{ formatNumber(sdkMetrics.session.total_calls) }} 次</el-descriptions-item>
                </el-descriptions>
              </el-card>
            </el-col>
          </el-row>

          <!-- 仓位信息 -->
          <el-card class="position-card">
            <template #header>
              <span>仓位信息</span>
            </template>
            <el-descriptions :column="5" border>
              <el-descriptions-item label="可用资金">¥{{ formatNumber(availableCash) }}</el-descriptions-item>
              <el-descriptions-item label="持仓数量">{{ positionCount }} 只</el-descriptions-item>
              <el-descriptions-item label="当前市值">¥{{ formatNumber(totalMarketValue) }}</el-descriptions-item>
              <el-descriptions-item label="当前盈亏">
                <span :class="totalPnl >= 0 ? 'profit-positive' : 'profit-negative'">
                  {{ totalPnl >= 0 ? '+' : '' }}¥{{ formatNumber(Math.abs(totalPnl)) }}
                </span>
              </el-descriptions-item>
              <el-descriptions-item label="当日盈亏">
                <span :class="dailyPnl >= 0 ? 'profit-positive' : 'profit-negative'">
                  {{ dailyPnl >= 0 ? '+' : '' }}¥{{ formatNumber(Math.abs(dailyPnl)) }}
                </span>
              </el-descriptions-item>
            </el-descriptions>
          </el-card>

          <!-- 今日交易统计 -->
          <el-row :gutter="20" class="stats-row">
            <el-col :span="12">
              <el-card class="stat-card">
                <template #header>
                  <span>今日交易统计</span>
                </template>
                <el-descriptions :column="2" border>
                  <el-descriptions-item label="交易笔数">{{ tradeCount }}</el-descriptions-item>
                  <el-descriptions-item label="买入">{{ buyCount }}</el-descriptions-item>
                  <el-descriptions-item label="卖出">{{ sellCount }}</el-descriptions-item>
                  <el-descriptions-item label="总金额">¥{{ formatNumber(totalAmount) }}</el-descriptions-item>
                </el-descriptions>
              </el-card>
            </el-col>

            <el-col :span="12">
              <el-card class="stat-card">
                <template #header>
                  <span>今日任务执行</span>
                </template>
                <el-descriptions :column="2" border>
                  <el-descriptions-item label="执行次数">{{ taskCount }}</el-descriptions-item>
                  <el-descriptions-item label="成功">{{ taskSuccess }}</el-descriptions-item>
                  <el-descriptions-item label="失败" :span="2">
                    <span :class="taskFail > 0 ? 'profit-negative' : ''">{{ taskFail }}</span>
                  </el-descriptions-item>
                </el-descriptions>
              </el-card>
            </el-col>
          </el-row>

          <!-- 数据库状态 -->
          <el-row :gutter="20" class="stats-row">
            <el-col :span="12">
              <el-card class="db-card">
                <template #header><span>日K线数据</span></template>
                <el-descriptions :column="2" border>
                  <el-descriptions-item label="最新日期">{{ dbStats.klineLatestDate || '-' }}</el-descriptions-item>
                  <el-descriptions-item label="当日条数">{{ dbStats.klineLatestCount || 0 }}</el-descriptions-item>
                  <el-descriptions-item label="总条数" :span="2">{{ dbStats.klineTotalCount || 0 }}</el-descriptions-item>
                </el-descriptions>
              </el-card>
            </el-col>
            <el-col :span="12">
              <el-card class="db-card">
                <template #header><span>日频因子数据</span></template>
                <el-descriptions :column="2" border>
                  <el-descriptions-item label="最新日期">{{ dbStats.factorLatestDate || '-' }}</el-descriptions-item>
                  <el-descriptions-item label="当日因子数">{{ dbStats.factorLatestCount || 0 }}</el-descriptions-item>
                  <el-descriptions-item label="因子总数" :span="2">{{ dbStats.factorTotalCount || 0 }}</el-descriptions-item>
                </el-descriptions>
              </el-card>
            </el-col>
          </el-row>
          <el-row :gutter="20" class="stats-row">
            <el-col :span="12">
              <el-card class="db-card">
                <template #header><span>周K线数据</span></template>
                <el-descriptions :column="2" border>
                  <el-descriptions-item label="最新日期">{{ dbStats.weeklyLatestDate || '-' }}</el-descriptions-item>
                  <el-descriptions-item label="当日条数">{{ dbStats.weeklyLatestCount || 0 }}</el-descriptions-item>
                  <el-descriptions-item label="总条数" :span="2">{{ dbStats.weeklyTotalCount || 0 }}</el-descriptions-item>
                </el-descriptions>
              </el-card>
            </el-col>
            <el-col :span="12">
              <el-card class="db-card">
                <template #header><span>股票基本信息</span></template>
                <el-descriptions :column="2" border>
                  <el-descriptions-item label="总数">{{ dbStats.baseInfoCount || 0 }} 只</el-descriptions-item>
                  <el-descriptions-item label="沪市">{{ dbStats.baseInfoSh || 0 }} 只</el-descriptions-item>
                  <el-descriptions-item label="深市">{{ dbStats.baseInfoSz || 0 }} 只</el-descriptions-item>
                  <el-descriptions-item label="北交所">{{ dbStats.baseInfoBj || 0 }} 只</el-descriptions-item>
                  <el-descriptions-item label="新三板">{{ dbStats.baseInfoNeeq || 0 }} 只</el-descriptions-item>
                </el-descriptions>
              </el-card>
            </el-col>
          </el-row>
        </el-main>
      </el-container>
    </el-main>
  </div>
</template>

<script setup>
import { ref, onMounted, onUnmounted, computed } from 'vue'
import { useAccountStore } from '../stores/account'
import { WarningFilled, Refresh, Loading } from '@element-plus/icons-vue'
import NavBar from '../components/NavBar.vue'

const accountStore = useAccountStore()
const currentAccountId = computed(() => accountStore.currentAccountId)
const accounts = computed(() => accountStore.accounts)
const currentAccount = computed(() => accountStore.currentAccount)

const healthStatus = ref('healthy')
const uptimeText = ref('0天0小时0分0秒')
const appVersion = ref('v7.0.0')
const cpuPercent = ref(0)
const memoryMb = ref(0)
const diskPercent = ref(0)
const sdkIssues = ref([])
const sdkErrorTime = ref('')
const taskCount = ref(0)
const taskSuccess = ref(0)
const taskFail = ref(0)
const tradeCount = ref(0)
const buyCount = ref(0)
const sellCount = ref(0)
const totalAmount = ref(0)
const availableCash = ref(0)
const positionCount = ref(0)
const totalMarketValue = ref(0)
const totalPnl = ref(0)
const dailyPnl = ref(0)

// 数据库状态
const dbStats = ref({
  klineLatestDate: '',
  klineLatestCount: 0,
  klineTotalCount: 0,
  factorLatestDate: '',
  factorLatestCount: 0,
  factorTotalCount: 0,
  weeklyLatestDate: '',
  weeklyLatestCount: 0,
  weeklyTotalCount: 0,
  baseInfoCount: 0,
  baseInfoSh: 0,
  baseInfoSz: 0,
  baseInfoBj: 0,
  baseInfoNeeq: 0,
})

// 数据库吞吐量
const dbThroughput = ref({
  total_queries: 0,
  total_reads: 0,
  total_writes: 0,
  total_rows_read: 0,
  total_rows_written: 0,
  queries_per_sec: 0,
  reads_per_sec: 0,
  writes_per_sec: 0,
  rows_read_per_sec: 0,
  rows_written_per_sec: 0,
})

// SDK 调用统计
const sdkMetrics = ref({
  recent_60s: { calls: 0, rows: 0, success_rate: 0, active_methods: [] },
  session: { total_calls: 0, success_calls: 0, total_rows: 0 },
})

// 数据通道状态
const dataSources = ref([])
const statusTagType = (status) => {
  const map = { connected: 'success', disconnected: 'warning', error: 'danger', not_configured: 'info' }
  return map[status] || 'info'
}
const statusText = (status) => {
  const map = { connected: '已连接', disconnected: '未连接', error: '连接失败', not_configured: '未配置' }
  return map[status] || '未知'
}
const connectedCount = computed(() => dataSources.value.filter(ds => ds.status === 'connected').length)
const allSourcesConnected = computed(() => dataSources.value.length > 0 && dataSources.value.every(ds => ds.status === 'connected'))

// 手动检测数据源健康（SSE 流式）
const checkingHealth = ref(false)
const refreshDataSources = async () => {
  checkingHealth.value = true
  // 重置所有数据源的检测状态
  for (const ds of dataSources.value) {
    ds._checking = false
    ds.error_message = ds.error_message && ds.status !== 'connected' ? ds.error_message : null
  }

  try {
    const response = await fetch('/api/v1/ui/data-sources/health/stream')
    if (!response.ok) throw new Error('SSE 请求失败')

    const reader = response.body.getReader()
    const decoder = new TextDecoder()
    let buffer = ''

    while (true) {
      const { done, value } = await reader.read()
      if (done) break

      buffer += decoder.decode(value, { stream: true })
      const lines = buffer.split('\n')
      buffer = lines.pop() || '' // 保留最后一个不完整的行

      for (const line of lines) {
        if (!line.startsWith('data: ')) continue
        try {
          const msg = JSON.parse(line.slice(6))

          if (msg.provider_id === '__meta__' || msg.provider_id === '__done__' || msg.provider_id === '__error__') {
            if (msg.provider_id === '__error__') {
              console.error('健康检查错误:', msg.message)
            }
            continue
          }

          if (msg.status === 'checking') {
            // 标记该 provider 正在检测
            const ds = dataSources.value.find(d => d.provider_id === msg.provider_id)
            if (ds) ds._checking = true
          } else if (msg.status === 'done') {
            // 更新检测结果
            const ds = dataSources.value.find(d => d.provider_id === msg.provider_id)
            if (ds) {
              ds._checking = false
              if (msg.ok) {
                ds.status = 'connected'
                ds.error_message = null
              } else {
                ds.status = 'error'
                ds.error_message = msg.message || '健康检查失败'
              }
              ds.latency_ms = msg.latency_ms >= 0 ? msg.latency_ms : ds.latency_ms
            }
          }
        } catch (e) {
          console.warn('SSE 解析失败:', e)
        }
      }
    }
  } catch (e) {
    console.error('健康检查失败:', e)
    // 回退到旧接口
    try {
      const res = await fetch('/api/v1/ui/data-sources/health')
      const data = await res.json()
      if (data.success && data.data) {
        for (const ds of dataSources.value) {
          const health = data.data[ds.provider_id]
          if (health) {
            if (health.ok) {
              ds.status = 'connected'
              ds.error_message = null
            } else {
              ds.status = 'error'
              ds.error_message = health.message || '健康检查失败'
            }
            ds.latency_ms = health.latency_ms ?? ds.latency_ms
          }
        }
      }
    } catch (e2) {
      console.error('回退健康检查也失败:', e2)
    }
  } finally {
    checkingHealth.value = false
    // 清除所有 _checking 标记
    for (const ds of dataSources.value) {
      delete ds._checking
    }
  }
}

// 本地时钟自动更新运行时长
let uptimeTimer = null
let serverStartTimestamp = null  // 服务器启动时间的时间戳（毫秒）

const formatUptime = (elapsedMs) => {
  const totalSeconds = Math.max(0, Math.floor(elapsedMs / 1000))
  const days = Math.floor(totalSeconds / 86400)
  const hours = Math.floor((totalSeconds % 86400) / 3600)
  const minutes = Math.floor((totalSeconds % 3600) / 60)
  const seconds = totalSeconds % 60
  return `${days}天${hours}小时${minutes}分${seconds}秒`
}

const startUptimeTimer = () => {
  if (uptimeTimer) clearInterval(uptimeTimer)
  if (serverStartTimestamp) {
    uptimeTimer = setInterval(() => {
      uptimeText.value = formatUptime(Date.now() - serverStartTimestamp)
    }, 1000)
  }
}

// 加载仪表盘数据（silent=true 时静默失败，不输出日志）
const loadDashboard = async (silent = false) => {
  try {
    const response = await fetch(`/api/v1/ui/${currentAccountId.value}/dashboard`)
    if (!response.ok) {
      if (!silent) console.warn('仪表盘数据加载失败:', response.status)
      return
    }
    const data = await response.json()

    healthStatus.value = data.system_health?.status || 'unknown'
    sdkIssues.value = data.system_health?.issues || []
    sdkErrorTime.value = data.system_health?.monitor_sdk_error_time || ''

    // 服务器启动时间戳，用于本地时钟计算运行时长
    if (data.system_health?.server_start) {
      serverStartTimestamp = new Date(data.system_health.server_start + '+08:00').getTime()
      uptimeText.value = formatUptime(Date.now() - serverStartTimestamp)
      startUptimeTimer()
    } else {
      uptimeText.value = data.system_health?.uptime_text || '0天0小时0分0秒'
    }

    appVersion.value = `v${data.system_health?.version || '7.0.0'}`
    cpuPercent.value = data.system_health?.cpu_percent || 0
    memoryMb.value = data.system_health?.memory_mb || 0
    diskPercent.value = data.system_health?.disk_percent || 0
    tradeCount.value = data.today_trading?.trade_count || 0
    buyCount.value = data.today_trading?.buy_count || 0
    sellCount.value = data.today_trading?.sell_count || 0
    totalAmount.value = data.today_trading?.total_amount || 0
    taskCount.value = data.today_tasks?.task_count || 0
    taskSuccess.value = data.today_tasks?.success_count || 0
    taskFail.value = data.today_tasks?.fail_count || 0
    availableCash.value = data.positions_summary?.available_cash || 0
    positionCount.value = data.positions_summary?.position_count || 0
    totalMarketValue.value = data.positions_summary?.total_market_value || 0
    totalPnl.value = data.positions_summary?.total_pnl || 0
    dailyPnl.value = data.positions_summary?.daily_pnl || 0

    // 数据库状态
    const ds = data.db_stats || {}
    dbStats.value = {
      klineLatestDate: ds.kline_latest_date || '',
      klineLatestCount: ds.kline_latest_count || 0,
      klineTotalCount: ds.kline_total_count || 0,
      factorLatestDate: ds.factor_latest_date || '',
      factorLatestCount: ds.factor_latest_count || 0,
      factorTotalCount: ds.factor_total_count || 0,
      weeklyLatestDate: ds.weekly_latest_date || '',
      weeklyLatestCount: ds.weekly_latest_count || 0,
      weeklyTotalCount: ds.weekly_total_count || 0,
      baseInfoCount: ds.base_info_count || 0,
      baseInfoSh: ds.base_info_sh || 0,
      baseInfoSz: ds.base_info_sz || 0,
      baseInfoBj: ds.base_info_bj || 0,
      baseInfoNeeq: ds.base_info_neeq || 0,
    }

    // 数据库吞吐量
    const dt = data.db_throughput || {}
    dbThroughput.value = {
      total_queries: dt.total_queries || 0,
      total_reads: dt.total_reads || 0,
      total_writes: dt.total_writes || 0,
      total_rows_read: dt.total_rows_read || 0,
      total_rows_written: dt.total_rows_written || 0,
      queries_per_sec: dt.queries_per_sec || 0,
      reads_per_sec: dt.reads_per_sec || 0,
      writes_per_sec: dt.writes_per_sec || 0,
      rows_read_per_sec: dt.rows_read_per_sec || 0,
      rows_written_per_sec: dt.rows_written_per_sec || 0,
    }

    // SDK 调用统计
    const sm = data.sdk_metrics || {}
    sdkMetrics.value = {
      recent_60s: sm.recent_60s || { calls: 0, rows: 0, success_rate: 0, active_methods: [] },
      session: sm.session || { total_calls: 0, success_calls: 0, total_rows: 0 },
    }

    // 数据通道状态
    if (data.data_sources_status) {
      dataSources.value = data.data_sources_status.map(ds => ({ ...ds, _checking: false }))
    }
  } catch (error) {
    if (!silent) console.error('加载仪表盘数据失败:', error)
  }
}

const refreshData = () => {
  loadDashboard()
}

const formatNumber = (num) => {
  return Number(num || 0).toLocaleString('zh-CN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })
}

// 自动刷新定时器
let refreshTimer = null
const REFRESH_INTERVAL = 60000 // 1 分钟

onMounted(async () => {
  await accountStore.loadAccounts()
  await loadDashboard()
  // 每分钟自动刷新
  refreshTimer = setInterval(() => loadDashboard(true), REFRESH_INTERVAL)
})

onUnmounted(() => {
  if (uptimeTimer) clearInterval(uptimeTimer)
  if (refreshTimer) clearInterval(refreshTimer)
})
</script>

<style scoped>
.layout-container {
  height: 100%;
  display: flex;
  flex-direction: column;
}

.main-content {
  padding: 0;
  overflow-y: auto; /* 添加垂直滚动 */
  height: 100%; /* 确保高度占满容器 */
}

.dashboard-main {
  padding: 20px;
  overflow-y: auto; /* 主内容区滚动 */
  max-height: calc(100vh - 60px); /* 最大高度为视口高度减去导航栏 */
  height: auto; /* 内容少时自动调整 */
}

.dashboard-main h2 {
  margin-bottom: 20px;
  color: #303133;
}

.sidebar {
  background-color: #f5f7fa;
  border-right: 1px solid #e4e7ed;
  overflow-y: auto; /* 侧边栏滚动 */
  max-height: calc(100vh - 60px); /* 侧边栏最大高度 */
}

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.health-card,
.position-card,
.stat-card {
  margin-bottom: 20px;
}

.health-card--unhealthy :deep(.el-card__header) {
  background-color: #fef0f0;
  border-color: #fbc4c4;
}

.health-card--unhealthy :deep(.el-card__header .card-header span:first-child) {
  color: #f56c6c;
}

.stats-row {
  margin-bottom: 20px;
}

.db-card {
  margin-bottom: 20px;
}

.data-sources-card {
  margin-bottom: 20px;
}

.error-text {
  color: #f56c6c;
}

.stat-card {
  height: 100%;
}

.profit-positive {
  color: #f56c6c;
  font-weight: bold;
}

.profit-negative {
  color: #67c23a;
  font-weight: bold;
}

.active-account {
  background-color: #ecf5ff !important;
  color: #409EFF !important;
}

.other-account {
  color: #c0c4cc !important;
}
</style>
