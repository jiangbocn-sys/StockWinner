import { createRouter, createWebHistory } from 'vue-router'

const routes = [
  {
    path: '/login',
    name: 'Login',
    component: () => import('../views/Login.vue'),
    meta: { title: '登录', requiresAuth: false }
  },
  {
    path: '/',
    redirect: '/dashboard'
  },
  {
    path: '/dashboard',
    name: 'Dashboard',
    component: () => import('../views/Dashboard.vue'),
    meta: { title: '仪表盘' }
  },
  {
    path: '/trades',
    name: 'Trades',
    component: () => import('../views/Trades.vue'),
    meta: { title: '交易监控' }
  },
  {
    path: '/positions',
    name: 'Positions',
    component: () => import('../views/Positions.vue'),
    meta: { title: '持仓分析' }
  },
  {
    path: '/strategies',
    name: 'Strategies',
    component: () => import('../views/Strategies.vue'),
    meta: { title: '策略管理' }
  },
  {
    path: '/data-explorer',
    name: 'DataExplorer',
    component: () => import('../views/DataExplorer.vue'),
    meta: { title: '数据浏览器' }
  },
  {
    path: '/watchlist',
    name: 'Watchlist',
    component: () => import('../views/Watchlist.vue'),
    meta: { title: '选股监控' }
  },
  {
    path: '/signals',
    name: 'Signals',
    component: () => import('../views/Signals.vue'),
    meta: { title: '交易信号' }
  },
  {
    path: '/accounts',
    name: 'Accounts',
    component: () => import('../views/AccountManagement.vue'),
    meta: { title: '账户管理' }
  },
  {
    path: '/settings',
    name: 'Settings',
    component: () => import('../views/Settings.vue'),
    meta: { title: '系统设置' }
  },
  {
    path: '/change-password',
    name: 'ChangePassword',
    component: () => import('../views/ChangePassword.vue'),
    meta: { title: '修改密码' }
  }
]

const router = createRouter({
  history: createWebHistory(),
  routes
})

router.beforeEach((to, from, next) => {
  if (to.meta.title) {
    document.title = `${to.meta.title} - StockWinner`
  }

  // 检查认证状态
  const token = localStorage.getItem('auth_token')
  const requiresAuth = to.meta.requiresAuth !== false

  if (requiresAuth && !token) {
    // 需要认证但没有 token，重定向到登录页
    next('/login')
  } else if (to.path === '/login' && token) {
    // 已登录但尝试访问登录页，重定向到首页
    next('/dashboard')
  } else {
    next()
  }
})

export default router
