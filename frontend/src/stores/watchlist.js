import { defineStore } from 'pinia'
import { ref } from 'vue'

export const useWatchlistStore = defineStore('watchlist', () => {
  // 核心数据
  const strategies = ref([])
  const candidateGroups = ref([])
  const loaded = ref(false)

  const loadStrategies = async (accountId) => {
    // 添加时间戳防止浏览器缓存
    const ts = Date.now()
    const res = await fetch(`/api/v1/ui/${accountId}/strategies?_t=${ts}`)
    const data = await res.json()
    strategies.value = data.strategies || data.data || []
  }

  const loadGroups = async (accountId) => {
    // 添加时间戳防止浏览器缓存
    const ts = Date.now()
    const res = await fetch(`/api/v1/ui/${accountId}/candidate-groups?_t=${ts}`)
    const data = await res.json()
    candidateGroups.value = data.groups || data.data || []
  }

  const loadAll = async (accountId) => {
    loaded.value = false  // 开始加载时设置为 false，显示 loading 状态
    await Promise.all([loadStrategies(accountId), loadGroups(accountId)])
    loaded.value = true
  }

  const reset = () => {
    strategies.value = []
    candidateGroups.value = []
    loaded.value = false
  }

  return { strategies, candidateGroups, loaded, loadStrategies, loadGroups, loadAll, reset }
})