<template>
  <el-header class="nav-header">
    <div class="nav-content">
      <!-- Logo -->
      <div class="logo">
        <el-icon :size="24"><TrendCharts /></el-icon>
        <span>StockWinner v6.2.3</span>
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
        <el-menu-item index="/dashboard">
          <el-icon><DataAnalysis /></el-icon>
          <span>仪表盘</span>
        </el-menu-item>
        <el-menu-item index="/watchlist">
          <el-icon><Search /></el-icon>
          <span>选股监控</span>
        </el-menu-item>
        <el-menu-item index="/signals">
          <el-icon><Bell /></el-icon>
          <span>交易信号</span>
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
        <el-menu-item index="/data-explorer">
          <el-icon><DataLine /></el-icon>
          <span>数据浏览器</span>
        </el-menu-item>
        <el-menu-item index="/accounts">
          <el-icon><User /></el-icon>
          <span>账户管理</span>
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
          <span class="username">{{ currentUser?.name }}</span>
        </div>
        <el-dropdown style="margin-left: 15px">
          <el-button size="small">
            {{ currentUser?.name }}<el-icon class="el-icon--right"><ArrowDown /></el-icon>
          </el-button>
          <template #dropdown>
            <el-dropdown-menu>
              <el-dropdown-item @click="router.push('/change-password')">
                <el-icon><Lock /></el-icon>
                修改密码
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
import { Lock, SwitchButton, ArrowDown, DataLine } from '@element-plus/icons-vue'

const router = useRouter()
const route = useRoute()
const currentUser = ref(null)

const activeMenu = computed(() => route.path)

// 加载当前用户信息
onMounted(() => {
  const userStr = localStorage.getItem('current_user')
  if (userStr) {
    currentUser.value = JSON.parse(userStr)
  }
})

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
    // 清除本地存储
    localStorage.removeItem('auth_token')
    localStorage.removeItem('current_user')
    ElMessage.success('已退出登录')
    router.push('/login')
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
</style>
