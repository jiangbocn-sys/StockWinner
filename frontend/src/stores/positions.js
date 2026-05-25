import { defineStore } from 'pinia'
import { ref, computed } from 'vue'

export const usePositionsStore = defineStore('positions', () => {
  // 核心数据
  const positions = ref([])
  const availableCash = ref(0)
  const closedPositions = ref([])
  const closedCount = ref(0)
  const strategyStats = ref([])
  const loaded = ref(false)

  // 计算属性
  const marketValue = computed(() =>
    positions.value.reduce((sum, p) => sum + (p.market_value || 0), 0)
  )
  const totalPnl = computed(() =>
    positions.value.reduce((sum, p) => sum + (p.profit_loss || 0), 0)
  )
  const totalAssets = computed(() => marketValue.value + availableCash.value)
  const pnlPercent = computed(() => {
    const cost = totalAssets.value - availableCash.value
    return cost > 0 ? (totalPnl.value / cost) * 100 : 0
  })

  // 加载持仓
  const loadPositions = async (accountId) => {
    const response = await fetch(`/api/v1/ui/${accountId}/positions`)
    const data = await response.json()
    positions.value = data.positions || []
    availableCash.value = data.available_cash || 0
    loaded.value = true
  }

  // 加载已清仓
  const loadClosedPositions = async (accountId) => {
    const response = await fetch(`/api/v1/ui/${accountId}/closed-positions?limit=500`)
    const data = await response.json()
    closedPositions.value = data.closed_positions || []
    closedCount.value = data.total || 0
  }

  // 加载策略统计
  const loadStrategyStats = async (accountId) => {
    const res = await fetch(`/api/v1/ui/${accountId}/positions/strategy-stats`)
    const data = await res.json()
    strategyStats.value = data.strategy_stats || []
  }

  // 刷新实时行情
  const refreshPrices = async (accountId) => {
    const response = await fetch(`/api/v1/ui/${accountId}/positions/refresh-prices`, { method: 'POST' })
    const data = await response.json()
    positions.value = data.positions || []
    availableCash.value = data.available_cash || 0
    // 如果后台仍在刷新，等 3 秒后重新拉取
    if (data.price_cache_status?.refreshing) {
      await new Promise(resolve => setTimeout(resolve, 3000))
      await loadPositions(accountId)
    }
  }

  // 加载全部基础数据
  const loadAll = async (accountId) => {
    await Promise.all([
      loadPositions(accountId),
      loadClosedPositions(accountId),
      loadStrategyStats(accountId)
    ])
  }

  const reset = () => {
    positions.value = []
    availableCash.value = 0
    closedPositions.value = []
    closedCount.value = 0
    strategyStats.value = []
    loaded.value = false
  }

  return {
    positions, availableCash, closedPositions, closedCount, strategyStats,
    loaded, marketValue, totalPnl, totalAssets, pnlPercent,
    loadPositions, loadClosedPositions, loadStrategyStats, loadAll, refreshPrices, reset
  }
})
