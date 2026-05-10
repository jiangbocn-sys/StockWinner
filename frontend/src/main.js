import { createApp } from 'vue'
import { createPinia } from 'pinia'
import ElementPlus from 'element-plus'
import 'element-plus/dist/index.css'
import * as ElementPlusIconsVue from '@element-plus/icons-vue'
import zhCn from 'element-plus/dist/locale/zh-cn.mjs'
import router from './router'
import App from './App.vue'

// ============================================================
// 全局 fetch 拦截：
// 1. 对 /api/v1/ui/ 请求自动注入 X-Auth-Token
// 2. 后端重启后内存 session 失效（401）自动跳登录页
// ============================================================
const _originalFetch = window.fetch
window.fetch = async (...args) => {
  const url = typeof args[0] === 'string' ? args[0] : args[0]?.url || ''
  const token = localStorage.getItem('auth_token')

  // 自动注入 token 到 UI API 请求
  if ((url.includes('/api/v1/ui/') || url.includes('/api/accounts/')) && token) {
    const existingHeaders = (typeof args[1]?.headers === 'object') ? args[1].headers : {}
    const headers = { ...existingHeaders, 'X-Auth-Token': token }
    if (args[1]) {
      args[1] = { ...args[1], headers }
    } else {
      args[1] = { headers }
    }
  }

  try {
    const response = await _originalFetch(...args)
    if (response.status === 401) {
      // 仅拦截 auth 相关端点，避免误伤
      if (url.includes('/api/auth/') || url.includes('/api/v1/ui/') || url.includes('/api/v1/agent/') || url.includes('/api/accounts/')) {
        localStorage.removeItem('auth_token')
        localStorage.removeItem('current_user')
        // 避免重复跳转
        if (window.location.pathname !== '/login') {
          window.location.href = '/login'
        }
      }
    }
    return response
  } catch (error) {
    throw error
  }
}

const app = createApp(App)
const pinia = createPinia()

// 注册所有图标
for (const [key, component] of Object.entries(ElementPlusIconsVue)) {
  app.component(key, component)
}

app.use(pinia)
app.use(router)
app.use(ElementPlus, { locale: zhCn })

app.mount('#app')
