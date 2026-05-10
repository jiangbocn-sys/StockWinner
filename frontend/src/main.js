import { createApp } from 'vue'
import { createPinia } from 'pinia'
import ElementPlus from 'element-plus'
import 'element-plus/dist/index.css'
import * as ElementPlusIconsVue from '@element-plus/icons-vue'
import zhCn from 'element-plus/dist/locale/zh-cn.mjs'
import router from './router'
import App from './App.vue'

// ============================================================
// 全局 fetch 拦截：后端重启后内存 session 失效（401）自动跳登录页
// ============================================================
const _originalFetch = window.fetch
window.fetch = async (...args) => {
  try {
    const response = await _originalFetch(...args)
    if (response.status === 401) {
      // 仅拦截 auth 相关端点，避免误伤
      const url = typeof args[0] === 'string' ? args[0] : args[0]?.url || ''
      if (url.includes('/api/auth/') || url.includes('/api/v1/ui/') || url.includes('/api/v1/agent/')) {
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
