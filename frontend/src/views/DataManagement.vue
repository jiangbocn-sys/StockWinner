<template>
  <div class="layout-container">
    <NavBar />
    <div class="content">
      <el-row :gutter="20" class="cards-row">
        <!-- K线数据 -->
        <el-col :span="12">
          <el-card shadow="hover">
            <template #header>
              <div class="card-header">
                <span>K线数据</span>
                <span class="data-date">最新: {{ dataStatus.kline_latest || '未知' }}</span>
              </div>
            </template>
            <div class="card-actions">
              <el-button type="primary" @click="incrementalKlineCheck" :loading="klineChecking" :disabled="hasRunningTask">
                增量检查
              </el-button>
              <el-button type="warning" @click="fullKlineDownload" :loading="klineDownloading" :disabled="hasRunningTask">
                全量下载
              </el-button>
            </div>
            <div class="task-progress" v-if="klineTask">
              <el-progress :percentage="klineTask.progress?.percent || 0" :status="klineProgressStatus" />
              <div class="task-message">{{ klineTask.progress?.message || '' }}</div>
            </div>
          </el-card>
        </el-col>

        <!-- 日频因子 -->
        <el-col :span="12">
          <el-card shadow="hover">
            <template #header>
              <div class="card-header">
                <span>日频因子</span>
                <span class="data-date">最新: {{ dataStatus.daily_factors_latest || '未知' }}</span>
              </div>
            </template>
            <div class="card-actions">
              <el-button type="primary" @click="calculateDailyFactors" :loading="dailyCalcLoading" :disabled="hasRunningTask">
                智能补算
              </el-button>
              <el-button type="warning" @click="fillDailyEmpty" :loading="dailyFillLoading" :disabled="hasRunningTask">
                补算空值
              </el-button>
            </div>
            <div class="task-progress" v-if="dailyFactorTask">
              <el-progress :percentage="dailyFactorTask.progress?.percent || 0" :status="dailyFactorProgressStatus" />
              <div class="task-message">{{ dailyFactorTask.progress?.message || '' }}</div>
            </div>
          </el-card>
        </el-col>

        <!-- 周K线数据 -->
        <el-col :span="12">
          <el-card shadow="hover">
            <template #header>
              <div class="card-header">
                <span>周K线数据</span>
                <span class="data-date">最新: {{ dataStatus.weekly_kline_latest || '未知' }}</span>
              </div>
            </template>
            <div class="card-actions">
              <el-button type="primary" @click="downloadWeeklyKline" :loading="weeklyKlineLoading" :disabled="hasRunningTask">
                下载周K线
              </el-button>
            </div>
            <div class="task-progress" v-if="weeklyKlineTask">
              <el-progress :percentage="weeklyKlineTask.progress?.percent || 0" :status="weeklyKlineProgressStatus" />
              <div class="task-message">{{ weeklyKlineTask.progress?.message || '' }}</div>
            </div>
          </el-card>
        </el-col>

        <!-- 月频因子 -->
        <el-col :span="12">
          <el-card shadow="hover">
            <template #header>
              <div class="card-header">
                <span>月频因子</span>
                <span class="data-date">最新: {{ dataStatus.monthly_factors_latest || '未知' }}</span>
              </div>
            </template>
            <div class="card-actions">
              <el-button type="primary" @click="updateMonthlyFactors" :loading="monthlyUpdateLoading" :disabled="hasRunningTask">
                更新因子
              </el-button>
              <el-button type="warning" @click="fillMonthlyInherit" :loading="monthlyFillLoading" :disabled="hasRunningTask">
                填充继承
              </el-button>
            </div>
            <div class="task-progress" v-if="monthlyTaskStatus">
              <el-progress :percentage="monthlyTaskStatus.progress?.percent || 0" :status="monthlyFactorProgressStatus" />
              <div class="task-message">{{ monthlyTaskStatus.progress?.message || '' }}</div>
            </div>
          </el-card>
        </el-col>

        <!-- 调度服务 -->
        <el-col :span="24">
          <el-card shadow="hover">
            <template #header>
              <div class="card-header">
                <span>调度服务</span>
                <el-tag :type="schedulerStatus.running ? 'success' : 'info'">
                  {{ schedulerStatus.running ? '运行中' : '已停止' }}
                </el-tag>
              </div>
            </template>
            <div class="scheduler-content">
              <div class="scheduler-controls">
                <el-button type="success" @click="startScheduler" :disabled="schedulerStatus.running">启动</el-button>
                <el-button type="danger" @click="stopScheduler" :disabled="!schedulerStatus.running">停止</el-button>
                <el-divider direction="vertical" />
                <el-button @click="manualKlineCheck" :disabled="hasRunningTask">立即K线检查</el-button>
                <el-button @click="manualWeeklyKline" :disabled="hasRunningTask">立即周K线下载</el-button>
                <el-button @click="manualMonthlyCheck" :disabled="hasRunningTask">立即月频检查</el-button>
                <el-button @click="manualIndustryDownload" :disabled="hasRunningTask">立即下载行业指数</el-button>
              </div>
              <div class="scheduler-jobs" v-if="schedulerStatus.jobs?.length">
                <h4>定时任务</h4>
                <el-table :data="schedulerStatus.jobs" size="small" border>
                  <el-table-column prop="id" label="任务ID" width="200" />
                  <el-table-column prop="name" label="名称" width="200" />
                  <el-table-column prop="trigger" label="触发器" width="150" />
                  <el-table-column prop="next_run_time" label="下次执行时间" />
                </el-table>
              </div>
            </div>
          </el-card>
        </el-col>
      </el-row>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, onUnmounted } from 'vue'
import { ElMessage } from 'element-plus'
import NavBar from '../components/NavBar.vue'

// 数据状态
const dataStatus = ref({
  kline_latest: null,
  weekly_kline_latest: null,
  daily_factors_latest: null,
  monthly_factors_latest: null
})

// 调度状态
const schedulerStatus = ref({ running: false, jobs: [] })

// 任务状态（从 /api/v1/ui/tasks/status 获取）
const allTasksStatus = ref({})

// 加载状态
const klineChecking = ref(false)
const klineDownloading = ref(false)
const weeklyKlineLoading = ref(false)
const dailyCalcLoading = ref(false)
const dailyFillLoading = ref(false)
const monthlyUpdateLoading = ref(false)
const monthlyFillLoading = ref(false)

// 轮询定时器
let statusTimer = null

const hasRunningTask = computed(() => {
  return Object.values(allTasksStatus.value).some(t => t.status === 'running')
})

// K线任务状态
const klineTask = computed(() => allTasksStatus.value['data_download'])

// 日频因子任务状态
const dailyFactorTask = computed(() => allTasksStatus.value['daily_factor_calc'] || allTasksStatus.value['daily_factor_fill'])

// 周K线任务状态
const weeklyKlineTask = computed(() => allTasksStatus.value['weekly_kline_download'])

// 月频因子任务状态
const monthlyTaskStatus = computed(() => allTasksStatus.value['monthly_factor_update'])

// 进度状态
const klineProgressStatus = computed(() => {
  const t = klineTask.value
  if (!t) return ''
  return t.status === 'completed' ? 'success' : t.status === 'failed' ? 'exception' : ''
})

const dailyFactorProgressStatus = computed(() => {
  const t = dailyFactorTask.value
  if (!t) return ''
  return t.status === 'completed' ? 'success' : t.status === 'failed' ? 'exception' : ''
})

const weeklyKlineProgressStatus = computed(() => {
  const t = weeklyKlineTask.value
  if (!t) return ''
  return t.status === 'completed' ? 'success' : t.status === 'failed' ? 'exception' : ''
})

const monthlyFactorProgressStatus = computed(() => {
  const t = monthlyTaskStatus.value
  if (!t) return ''
  return t.status === 'completed' ? 'success' : t.status === 'failed' ? 'exception' : ''
})

// 加载数据状态
async function loadDataStatus() {
  try {
    const res = await fetch('/api/v1/ui/data/status')
    dataStatus.value = await res.json()
  } catch (e) {
    console.error('Failed to load data status:', e)
  }
}

// 加载调度状态
async function loadSchedulerStatus() {
  try {
    const res = await fetch('/api/v1/ui/scheduler/status')
    schedulerStatus.value = await res.json()
  } catch (e) {
    console.error('Failed to load scheduler status:', e)
  }
}

// 加载所有任务状态
async function loadTasksStatus() {
  try {
    const res = await fetch('/api/v1/ui/tasks/status')
    allTasksStatus.value = await res.json()
  } catch (e) {
    console.error('Failed to load tasks status:', e)
  }
}

// 刷新所有状态
function refreshAll() {
  loadDataStatus()
  loadSchedulerStatus()
  loadTasksStatus()
}

// K线增量检查
async function incrementalKlineCheck() {
  klineChecking.value = true
  try {
    const res = await fetch('/api/v1/ui/scheduler/kline/check', { method: 'POST' })
    const data = await res.json()
    ElMessage.success(data.message || 'K线检查任务已启动')
    loadTasksStatus()
  } catch (e) {
    ElMessage.error('请求失败')
  } finally {
    klineChecking.value = false
  }
}

// K线全量下载
async function fullKlineDownload() {
  klineDownloading.value = true
  try {
    const res = await fetch('/api/v1/ui/scheduler/kline/check?full=true', { method: 'POST' })
    const data = await res.json()
    ElMessage.success(data.message || 'K线全量下载任务已启动')
    loadTasksStatus()
  } catch (e) {
    ElMessage.error('请求失败')
  } finally {
    klineDownloading.value = false
  }
}

// 下载周K线
async function downloadWeeklyKline() {
  weeklyKlineLoading.value = true
  try {
    const res = await fetch('/api/v1/ui/scheduler/weekly/kline', { method: 'POST' })
    const data = await res.json()
    ElMessage.success(data.message || '周K线下载任务已启动')
    loadTasksStatus()
  } catch (e) {
    ElMessage.error('请求失败')
  } finally {
    weeklyKlineLoading.value = false
  }
}

// 智能补算日频因子
async function calculateDailyFactors() {
  dailyCalcLoading.value = true
  try {
    const res = await fetch('/api/v1/ui/factors/daily/calculate', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ lookback_days: 5 })
    })
    const data = await res.json()
    ElMessage.success(data.message || '日频因子计算任务已启动')
    loadTasksStatus()
  } catch (e) {
    ElMessage.error('请求失败')
  } finally {
    dailyCalcLoading.value = false
  }
}

// 补算日频因子空值
async function fillDailyEmpty() {
  dailyFillLoading.value = true
  try {
    const res = await fetch('/api/v1/ui/factors/daily/fill-empty', { method: 'POST' })
    const data = await res.json()
    ElMessage.success(data.message || '日频因子补算空值任务已启动')
    loadTasksStatus()
  } catch (e) {
    ElMessage.error('请求失败')
  } finally {
    dailyFillLoading.value = false
  }
}

// 更新月频因子
async function updateMonthlyFactors() {
  monthlyUpdateLoading.value = true
  try {
    const res = await fetch('/api/v1/ui/factors/monthly/update', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ mode: 'fill_empty' })
    })
    const data = await res.json()
    ElMessage.success(data.message || '月频因子更新任务已启动')
    loadTasksStatus()
  } catch (e) {
    ElMessage.error('请求失败')
  } finally {
    monthlyUpdateLoading.value = false
  }
}

// 填充继承月频因子
async function fillMonthlyInherit() {
  monthlyFillLoading.value = true
  try {
    const res = await fetch('/api/v1/ui/factors/monthly/fill-inherit', { method: 'POST' })
    const data = await res.json()
    ElMessage.success(data.message || '月频因子填充继承任务已启动')
    loadTasksStatus()
  } catch (e) {
    ElMessage.error('请求失败')
  } finally {
    monthlyFillLoading.value = false
  }
}

// 调度服务操作
async function startScheduler() {
  try {
    const res = await fetch('/api/v1/ui/scheduler/start', { method: 'POST' })
    const data = await res.json()
    ElMessage.success(data.message)
    loadSchedulerStatus()
  } catch (e) {
    ElMessage.error('请求失败')
  }
}

async function stopScheduler() {
  try {
    const res = await fetch('/api/v1/ui/scheduler/stop', { method: 'POST' })
    const data = await res.json()
    ElMessage.success(data.message)
    loadSchedulerStatus()
  } catch (e) {
    ElMessage.error('请求失败')
  }
}

async function manualKlineCheck() {
  try {
    const res = await fetch('/api/v1/ui/scheduler/kline/check', { method: 'POST' })
    const data = await res.json()
    ElMessage.success(data.message)
    loadTasksStatus()
  } catch (e) {
    ElMessage.error('请求失败')
  }
}

async function manualWeeklyKline() {
  try {
    const res = await fetch('/api/v1/ui/scheduler/weekly/kline', { method: 'POST' })
    const data = await res.json()
    ElMessage.success(data.message)
    loadTasksStatus()
  } catch (e) {
    ElMessage.error('请求失败')
  }
}

async function manualMonthlyCheck() {
  try {
    const res = await fetch('/api/v1/ui/scheduler/monthly/check', { method: 'POST' })
    const data = await res.json()
    ElMessage.success(data.message)
    loadTasksStatus()
  } catch (e) {
    ElMessage.error('请求失败')
  }
}

async function manualIndustryDownload() {
  try {
    const res = await fetch('/api/v1/ui/scheduler/industry/download', { method: 'POST' })
    const data = await res.json()
    ElMessage.success(data.message)
    loadTasksStatus()
  } catch (e) {
    ElMessage.error('请求失败')
  }
}

onMounted(() => {
  refreshAll()
  statusTimer = setInterval(loadTasksStatus, 3000)
})

onUnmounted(() => {
  if (statusTimer) clearInterval(statusTimer)
})
</script>

<style scoped>
.layout-container {
  height: 100%;
  display: flex;
  flex-direction: column;
}

.content {
  flex: 1;
  padding: 20px;
  background: #f5f7fa;
  overflow-y: auto;
}

.cards-row {
  margin-bottom: 20px;
}

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.data-date {
  font-size: 12px;
  color: #909399;
}

.card-actions {
  display: flex;
  gap: 10px;
  margin-bottom: 10px;
}

.task-progress {
  margin-top: 10px;
}

.task-message {
  font-size: 12px;
  color: #606266;
  margin-top: 5px;
}

.scheduler-content {
  padding: 0;
}

.scheduler-controls {
  display: flex;
  align-items: center;
  margin-bottom: 15px;
}

.scheduler-jobs h4 {
  margin: 0 0 10px 0;
  font-size: 14px;
  color: #303133;
}
</style>
