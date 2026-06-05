<template>
  <el-header class="nav-header">
    <div class="nav-content">
      <!-- Logo -->
      <div class="logo">
        <el-icon :size="24"><TrendCharts /></el-icon>
        <span>StockWinner</span>
      </div>

      <!-- 导航菜单 -->
      <el-menu
        mode="horizontal"
        :default-active="activeMenu"
        background-color="#304156"
        text-color="#bfcbd9"
        active-text-color="#409EFF"
        @select="handleMenuSelect"
      >
        <el-menu-item index="/dashboard" :class="{ 'nav-item--unhealthy': systemUnhealthy }">
          <el-icon><DataAnalysis /></el-icon>
          <span>仪表盘</span>
          <el-badge v-if="systemUnhealthy" is-dot type="danger" style="margin-left: 4px" />
        </el-menu-item>
        <el-menu-item index="/watchlist">
          <el-icon><Search /></el-icon>
          <span>选股监控</span>
        </el-menu-item>
        <el-menu-item index="/trades">
          <el-icon><Money /></el-icon>
          <span>交易监控</span>
        </el-menu-item>
        <el-menu-item index="/positions">
          <el-icon><Wallet /></el-icon>
          <span>持仓分析</span>
        </el-menu-item>
        <el-menu-item index="/strategies">
          <el-icon><Setting /></el-icon>
          <span>策略管理</span>
        </el-menu-item>
        <el-menu-item index="/backtest">
          <el-icon><TrendCharts /></el-icon>
          <span>策略回测</span>
        </el-menu-item>
        <el-menu-item index="/performance">
          <el-icon><DataBoard /></el-icon>
          <span>策略效能</span>
        </el-menu-item>
        <el-menu-item index="/data-explorer">
          <el-icon><DataLine /></el-icon>
          <span>数据浏览器</span>
        </el-menu-item>
        <el-menu-item index="/task-management">
          <el-icon><Files /></el-icon>
          <span>任务管理</span>
        </el-menu-item>
        <el-menu-item index="/settings">
          <el-icon><Tools /></el-icon>
          <span>系统设置</span>
        </el-menu-item>
      </el-menu>

      <!-- 右侧：用户信息和登出 -->
      <div class="nav-right">
        <div class="user-info">
          <el-icon><User /></el-icon>
          <span class="username">{{ currentUser?.display_name || currentUser?.name }}</span>
          <el-tag :type="currentUser?.role === 'admin' ? 'danger' : 'info'" size="small" style="margin-left: 8px;">
            {{ currentUser?.role === 'admin' ? '管理员' : '用户' }}
          </el-tag>
        </div>
        <el-dropdown style="margin-left: 15px">
          <el-button size="small">
            {{ currentUser?.display_name || currentUser?.name }}<el-icon class="el-icon--right"><ArrowDown /></el-icon>
          </el-button>
          <template #dropdown>
            <el-dropdown-menu>
              <el-dropdown-item @click="router.push('/settings')">
                <el-icon><Tools /></el-icon>
                系统设置
              </el-dropdown-item>
              <el-dropdown-item divided @click="handleLogout">
                <el-icon><SwitchButton /></el-icon>
                退出登录
              </el-dropdown-item>
            </el-dropdown-menu>
          </template>
        </el-dropdown>
      </div>
    </div>
  </el-header>
</template>

<script setup>
import { computed, ref, onMounted } from 'vue'
import { useRouter, useRoute } from 'vue-router'
import { ElMessage } from 'element-plus'
import { Lock, SwitchButton, ArrowDown, DataLine, Files, DataBoard } from '@element-plus/icons-vue'

const router = useRouter()
const route = useRoute()
const currentUser = ref(null)
const systemUnhealthy = ref(false)

const activeMenu = computed(() => route.path)

// 加载当前用户信息（优先从后端获取，兜底 localStorage）
onMounted(async () => {
  const token = localStorage.getItem('auth_token')
  if (token) {
    try {
      const res = await fetch('/api/me', { headers: { 'X-Auth-Token': token } })
      const data = await res.json()
      if (data.success) {
        currentUser.value = data.data
        localStorage.setItem('current_user', JSON.stringify(data.data))
      }
    } catch (e) { /* fallback */ }
  }
  const userStr = localStorage.getItem('current_user')
  if (userStr) {
    currentUser.value = JSON.parse(userStr)
  }

  // 检查系统健康状态
  checkHealth()
  // 每30秒轮询一次健康状态
  setInterval(checkHealth, 30000)
})

const checkHealth = async () => {
  const userStr = localStorage.getItem('current_user')
  const account = userStr ? JSON.parse(userStr) : null
  if (!account?.account_id) return
  try {
    const res = await fetch(`/api/v1/ui/${account.account_id}/dashboard`)
    const data = await res.json()
    systemUnhealthy.value = data.system_health?.status === 'unhealthy'
  } catch (e) { /* ignore */ }
}

const handleMenuSelect = (index) => {
  router.push(index)
}

const handleLogout = async () => {
  try {
    const token = localStorage.getItem('auth_token')
    if (token) {
      await fetch('/api/auth/logout', {
        method: 'POST',
        headers: { 'X-Auth-Token': token }
      })
    }
  } catch (error) {
    console.error('登出失败:', error)
  } finally {
    // 清除本地存储并刷新页面
    localStorage.removeItem('auth_token')
    localStorage.removeItem('current_user')
    ElMessage.success('已退出登录')
    window.location.href = '/ui/login'
  }
}
</script>

<style scoped>
.nav-header {
  background-color: #304156;
  padding: 0;
  display: flex;
  align-items: center;
}

.nav-content {
  display: flex;
  align-items: center;
  width: 100%;
  padding: 0 20px;
}

.logo {
  display: flex;
  align-items: center;
  gap: 10px;
  color: #fff;
  font-size: 18px;
  font-weight: bold;
  margin-right: 40px;
}

.el-menu {
  flex: 1;
  border: none;
  height: 60px;
}

.nav-right {
  display: flex;
  align-items: center;
}

.account-selector {
  display: flex;
  align-items: center;
  gap: 8px;
  color: #fff;
  cursor: pointer;
  padding: 8px 16px;
  border-radius: 4px;
  transition: background-color 0.3s;
}

.account-selector:hover {
  background-color: #3a4a5f;
}

.account-name {
  font-size: 14px;
}

.nav-item--unhealthy :deep(.el-menu-item__content span) {
  color: #f56c6c !important;
}
</style>
