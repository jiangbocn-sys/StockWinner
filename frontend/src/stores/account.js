import { defineStore } from 'pinia'
import { ref, computed } from 'vue'

export const useAccountStore = defineStore('account', () => {
  // 当前账户 ID
  const currentAccountId = ref('8229DE7E')

  // 账户列表
  const accounts = ref([])

  // 当前账户信息
  const currentAccount = computed(() => {
    return accounts.value.find(acc => acc.account_id === currentAccountId.value)
  })

  // 设置当前账户
  const setCurrentAccount = (accountId) => {
    currentAccountId.value = accountId
    // 切换到新账户时刷新页面数据
    window.location.reload()
  }

  // 加载账户列表
  const loadAccounts = async () => {
    try {
      const response = await fetch('/api/v1/ui/accounts')
      const data = await response.json()
      accounts.value = data.data || []

      // 如果当前账户不在列表中，选择第一个激活的账户
      if (!accounts.value.find(acc => acc.account_id === currentAccountId.value)) {
        const activeAccount = accounts.value.find(acc => acc.is_active)
        if (activeAccount) {
          currentAccountId.value = activeAccount.account_id
        }
      }
    } catch (error) {
      console.error('加载账户列表失败:', error)
    }
  }

  return {
    currentAccountId,
    accounts,
    currentAccount,
    setCurrentAccount,
    loadAccounts
  }
})
