<template>
  <div class="layout-container">
    <NavBar />
    <el-main class="main-content">
      <el-container>
        <!-- 侧边栏 -->
        <el-aside width="200px" class="sidebar">
          <el-menu
            :default-active="currentAccountId"
            background-color="#f5f7fa"
            @select="handleAccountSelect"
          >
            <el-sub-menu index="accounts">
              <template #title>
                <el-icon><User /></el-icon>
                <span>账户列表</span>
              </template>
              <el-menu-item
                v-for="acc in accounts"
                :key="acc.account_id"
                :index="acc.account_id"
                :class="{ 'active-account': acc.account_id === currentAccountId }"
              >
                {{ acc.display_name }}
              </el-menu-item>
            </el-sub-menu>
          </el-menu>
        </el-aside>

        <!-- 主内容区 -->
        <el-main class="dashboard-main">
          <h2>仪表盘 - {{ currentAccount?.display_name || currentAccountId }}</h2>

          <!-- 系统健康度 -->
          <el-card class="health-card">
            <template #header>
              <div class="card-header">
                <span>系统健康度</span>
                <el-tag :type="healthStatus === 'healthy' ? 'success' : 'danger'">
                  {{ healthStatus === 'healthy' ? '正常' : '异常' }}
                </el-tag>
              </div>
            </template>
            <el-descriptions :column="5" border>
              <el-descriptions-item label="运行时长">{{ uptimeText }}</el-descriptions-item>
              <el-descriptions-item label="版本">{{ appVersion }}</el-descriptions-item>
              <el-descriptions-item label="CPU">{{ cpuPercent }}%</el-descriptions-item>
              <el-descriptions-item label="内存">{{ memoryMb }} MB</el-descriptions-item>
              <el-descriptions-item label="硬盘">{{ diskPercent }}%</el-descriptions-item>
            </el-descriptions>
          </el-card>

          <!-- 仓位信息 -->
          <el-card class="position-card">
            <template #header>
              <span>仓位信息</span>
            </template>
            <el-descriptions :column="5" border>
              <el-descriptions-item label="可用资金">¥{{ formatNumber(availableCash) }}</el-descriptions-item>
              <el-descriptions-item label="持仓数量">{{ positionCount }} 只</el-descriptions-item>
              <el-descriptions-item label="当前市值">¥{{ formatNumber(totalMarketValue) }}</el-descriptions-item>
              <el-descriptions-item label="当前盈亏">
                <span :class="totalPnl >= 0 ? 'profit-positive' : 'profit-negative'">
                  {{ totalPnl >= 0 ? '+' : '' }}¥{{ formatNumber(Math.abs(totalPnl)) }}
                </span>
              </el-descriptions-item>
              <el-descriptions-item label="当日盈亏">
                <span :class="dailyPnl >= 0 ? 'profit-positive' : 'profit-negative'">
                  {{ dailyPnl >= 0 ? '+' : '' }}¥{{ formatNumber(Math.abs(dailyPnl)) }}
                </span>
              </el-descriptions-item>
            </el-descriptions>
          </el-card>

          <!-- 今日交易统计 -->
          <el-row :gutter="20" class="stats-row">
            <el-col :span="12">
              <el-card class="stat-card">
                <template #header>
                  <span>今日交易统计</span>
                </template>
                <el-descriptions :column="2" border>
                  <el-descriptions-item label="交易笔数">{{ tradeCount }}</el-descriptions-item>
                  <el-descriptions-item label="买入">{{ buyCount }}</el-descriptions-item>
                  <el-descriptions-item label="卖出">{{ sellCount }}</el-descriptions-item>
                  <el-descriptions-item label="总金额">¥{{ formatNumber(totalAmount) }}</el-descriptions-item>
                </el-descriptions>
              </el-card>
            </el-col>

            <el-col :span="12">
              <el-card class="stat-card">
                <template #header>
                  <span>今日任务执行</span>
                </template>
                <el-descriptions :column="2" border>
                  <el-descriptions-item label="执行次数">{{ taskCount }}</el-descriptions-item>
                  <el-descriptions-item label="成功">{{ taskSuccess }}</el-descriptions-item>
                  <el-descriptions-item label="失败" :span="2">
                    <span :class="taskFail > 0 ? 'profit-negative' : ''">{{ taskFail }}</span>
                  </el-descriptions-item>
                </el-descriptions>
              </el-card>
            </el-col>
          </el-row>
        </el-main>
      </el-container>
    </el-main>
  </div>
</template>

<script setup>
import { ref, onMounted, computed } from 'vue'
import { useAccountStore } from '../stores/account'
import NavBar from '../components/NavBar.vue'

const accountStore = useAccountStore()
const currentAccountId = computed(() => accountStore.currentAccountId)
const accounts = computed(() => accountStore.accounts)
const currentAccount = computed(() => accountStore.currentAccount)

const healthStatus = ref('healthy')
const uptimeText = ref('0 小时')
const appVersion = ref('v7.0.0')
const cpuPercent = ref(0)
const memoryMb = ref(0)
const diskPercent = ref(0)
const taskCount = ref(0)
const taskSuccess = ref(0)
const taskFail = ref(0)
const tradeCount = ref(0)
const buyCount = ref(0)
const sellCount = ref(0)
const totalAmount = ref(0)
const availableCash = ref(0)
const positionCount = ref(0)
const totalMarketValue = ref(0)
const totalPnl = ref(0)
const dailyPnl = ref(0)

// 加载仪表盘数据
const loadDashboard = async () => {
  try {
    const response = await fetch(`/api/v1/ui/${currentAccountId.value}/dashboard`)
    const data = await response.json()

    healthStatus.value = data.system_health?.status || 'unknown'
    uptimeText.value = `${data.system_health?.uptime_hours || 0} 小时`
    appVersion.value = `v${data.system_health?.version || '7.0.0'}`
    cpuPercent.value = data.system_health?.cpu_percent || 0
    memoryMb.value = data.system_health?.memory_mb || 0
    diskPercent.value = data.system_health?.disk_percent || 0
    tradeCount.value = data.today_trading?.trade_count || 0
    buyCount.value = data.today_trading?.buy_count || 0
    sellCount.value = data.today_trading?.sell_count || 0
    totalAmount.value = data.today_trading?.total_amount || 0
    taskCount.value = data.today_tasks?.task_count || 0
    taskSuccess.value = data.today_tasks?.success_count || 0
    taskFail.value = data.today_tasks?.fail_count || 0
    availableCash.value = data.positions_summary?.available_cash || 0
    positionCount.value = data.positions_summary?.position_count || 0
    totalMarketValue.value = data.positions_summary?.total_market_value || 0
    totalPnl.value = data.positions_summary?.total_pnl || 0
    dailyPnl.value = data.positions_summary?.daily_pnl || 0
  } catch (error) {
    console.error('加载仪表盘数据失败:', error)
  }
}

const refreshData = () => {
  loadDashboard()
}

const handleAccountSelect = (accountId) => {
  accountStore.setCurrentAccount(accountId)
}

const formatNumber = (num) => {
  return Number(num || 0).toLocaleString('zh-CN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })
}

onMounted(async () => {
  await accountStore.loadAccounts()
  await loadDashboard()
})
</script>

<style scoped>
.layout-container {
  height: 100%;
  display: flex;
  flex-direction: column;
}

.main-content {
  padding: 0;
  overflow-y: auto; /* 添加垂直滚动 */
  height: 100%; /* 确保高度占满容器 */
}

.dashboard-main {
  padding: 20px;
  overflow-y: auto; /* 主内容区滚动 */
  max-height: calc(100vh - 60px); /* 最大高度为视口高度减去导航栏 */
  height: auto; /* 内容少时自动调整 */
}

.dashboard-main h2 {
  margin-bottom: 20px;
  color: #303133;
}

.sidebar {
  background-color: #f5f7fa;
  border-right: 1px solid #e4e7ed;
  overflow-y: auto; /* 侧边栏滚动 */
  max-height: calc(100vh - 60px); /* 侧边栏最大高度 */
}

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.health-card,
.position-card,
.stat-card {
  margin-bottom: 20px;
}

.stats-row {
  margin-bottom: 20px;
}

.stat-card {
  height: 100%;
}

.profit-positive {
  color: #f56c6c;
  font-weight: bold;
}

.profit-negative {
  color: #67c23a;
  font-weight: bold;
}

.active-account {
  background-color: #ecf5ff !important;
  color: #409EFF !important;
}
</style>
