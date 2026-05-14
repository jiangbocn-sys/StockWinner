import { defineStore } from 'pinia'
import { ref, computed } from 'vue'
import { ElMessage } from 'element-plus'
import { useRouter } from 'vue-router'

export const useAccountStore = defineStore('account', () => {
  // 当前账户 ID — 从 localStorage 的 current_user 获取（登录时写入）
  const savedUser = typeof localStorage !== 'undefined' ? localStorage.getItem('current_user') : null
  const userAccount = savedUser ? JSON.parse(savedUser) : null
  const currentAccountId = ref(userAccount?.account_id || '')

  // 账户列表
  const accounts = ref([])

  // 当前账户信息
  const currentAccount = computed(() => {
    return accounts.value.find(acc => acc.account_id === currentAccountId.value)
  })

  // 设置当前账户 — 不允许切换，始终锁定为登录账户
  // eslint-disable-next-line no-unused-vars
  const setCurrentAccount = (accountId) => {
    return
  }

  // 加载账户列表
  const loadAccounts = async () => {
    try {
      const response = await fetch('/api/v1/ui/accounts')
      const data = await response.json()
      accounts.value = data.data || []

      // 如果登录账户不在列表中，提示并退出
      if (userAccount?.account_id && !accounts.value.find(acc => acc.account_id === userAccount.account_id)) {
        ElMessage.error('您的账户已被禁用或不存在，请联系管理员')
        localStorage.removeItem('auth_token')
        localStorage.removeItem('current_user')
        window.location.href = '/ui/login'
        return
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
