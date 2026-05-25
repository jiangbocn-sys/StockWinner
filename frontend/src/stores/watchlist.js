import { defineStore } from 'pinia'
import { ref } from 'vue'

export const useWatchlistStore = defineStore('watchlist', () => {
  const strategies = ref([])
  const candidateGroups = ref([])
  const loaded = ref(false)

  const loadStrategies = async (accountId) => {
    const res = await fetch(`/api/v1/ui/${accountId}/strategies`)
    const data = await res.json()
    strategies.value = data.strategies || data.data || []
  }

  const loadGroups = async (accountId) => {
    const res = await fetch(`/api/v1/ui/${accountId}/candidate-groups`)
    const data = await res.json()
    candidateGroups.value = data.groups || data.data || []
  }

  const loadAll = async (accountId) => {
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
