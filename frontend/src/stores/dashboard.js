import { defineStore } from 'pinia'
import { ref } from 'vue'

export const useDashboardStore = defineStore('dashboard', () => {
  const healthStatus = ref('unknown')
  const sdkIssues = ref([])
  const sdkErrorTime = ref('')
  const appVersion = ref('')
  const cpuPercent = ref(0)
  const memoryMb = ref(0)
  const diskPercent = ref(0)
  const uptimeText = ref('0天0小时0分0秒')
  const serverStartTimestamp = ref(null)

  const tradeCount = ref(0)
  const buyCount = ref(0)
  const sellCount = ref(0)
  const totalAmount = ref(0)
  const taskCount = ref(0)
  const taskSuccess = ref(0)
  const taskFail = ref(0)

  const klineStatusText = ref('')
  const klineNeedDownload = ref(false)
  const factorCoverage = ref(0)
  const backtestRunning = ref(0)
  const monitorRunning = ref(false)
  const notificationConfigured = ref(false)
  const dsaRunning = ref(false)
  const dbSizeMb = ref(0)

  const dataSourceProviders = ref([])
  const loaded = ref(false)

  const loadDashboard = async (accountId) => {
    const response = await fetch(`/api/v1/ui/${accountId}/dashboard`)
    const data = await response.json()

    healthStatus.value = data.system_health?.status || 'unknown'
    sdkIssues.value = data.system_health?.issues || []
    sdkErrorTime.value = data.system_health?.monitor_sdk_error_time || ''
    if (data.system_health?.server_start) {
      serverStartTimestamp.value = new Date(data.system_health.server_start + '+08:00').getTime()
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
    klineStatusText.value = data.data_status?.kline_status_text || ''
    klineNeedDownload.value = data.data_status?.kline_need_download || false
    factorCoverage.value = data.data_status?.factor_coverage || 0
    backtestRunning.value = data.system_status?.backtest_running || 0
    monitorRunning.value = data.system_status?.monitor_running || false
    notificationConfigured.value = data.system_status?.notification_configured || false
    dsaRunning.value = data.system_status?.dsa_running || false
    dbSizeMb.value = data.system_status?.db_size_mb || 0

    dataSourceProviders.value = data.data_sources_health?.providers || []
    loaded.value = true
  }

  return {
    healthStatus, sdkIssues, sdkErrorTime, appVersion,
    cpuPercent, memoryMb, diskPercent, uptimeText,
    serverStartTimestamp, tradeCount, buyCount, sellCount,
    totalAmount, taskCount, taskSuccess, taskFail,
    klineStatusText, klineNeedDownload, factorCoverage,
    backtestRunning, monitorRunning, notificationConfigured,
    dsaRunning, dbSizeMb, dataSourceProviders,
    loaded, loadDashboard
  }
})
