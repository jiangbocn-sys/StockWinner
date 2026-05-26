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

        <!-- 盘后分析 -->
        <el-col :span="24">
          <el-card shadow="hover">
            <template #header>
              <div class="card-header">
                <span>盘后分析</span>
                <span class="task-status" v-if="postMarketTask">
                  <el-tag :type="postMarketTask.status === 'running' ? 'warning' : postMarketTask.status === 'completed' ? 'success' : 'info'" size="small">
                    {{ postMarketTask.status === 'running' ? '分析中' : postMarketTask.status === 'completed' ? '已完成' : postMarketTask.status === 'failed' ? '失败' : '空闲' }}
                  </el-tag>
                </span>
              </div>
            </template>
            <div class="card-actions">
              <el-button type="primary" @click="triggerPostMarketAnalysis" :loading="postMarketLoading" :disabled="hasRunningTask">
                立即分析
              </el-button>
              <span class="task-hint" v-if="postMarketTask?.result?.message">
                {{ postMarketTask.result.message }}
              </span>
            </div>
            <div class="task-progress" v-if="postMarketTask && postMarketTask.status === 'running'">
              <el-progress :percentage="postMarketTask.progress?.percent || 0" :status="postMarketProgressStatus" />
              <div class="task-message">{{ postMarketTask.progress?.message || '' }}</div>
            </div>
          </el-card>
        </el-col>

        <!-- 策略任务管理 -->
        <el-col :span="24">
          <el-card shadow="hover">
            <template #header>
              <div class="card-header">
                <span>策略任务</span>
                <el-space>
                  <el-tag :type="schedulerStatus.running ? 'success' : 'info'" size="small" style="cursor: pointer" @click="toggleScheduler" :title="schedulerStatus.running ? '点击停止' : '点击启动'">
                    调度: {{ schedulerStatus.running ? '运行中' : '已停止' }}
                  </el-tag>
                  <el-select v-model="taskFilterGroup" placeholder="全部分组" clearable style="width: 180px" size="small" @change="loadStrategyTasks">
                    <el-option v-for="g in candidateGroups" :key="g.id" :label="g.name" :value="g.id" />
                  </el-select>
                  <el-button size="small" @click="scanTasks" :loading="scanningTasks">
                    <el-icon><Refresh /></el-icon>
                    扫描插件
                  </el-button>
                  <el-button type="primary" size="small" @click="showCreateTaskForm = !showCreateTaskForm">
                    <el-icon><Plus /></el-icon>
                    {{ showCreateTaskForm ? '收起表单' : '添加任务' }}
                  </el-button>
                </el-space>
              </div>
            </template>

            <!-- 添加/编辑任务表单 -->
            <el-collapse v-model="activeTaskForm" style="margin-bottom: 16px" v-if="showCreateTaskForm">
              <el-collapse-item title="添加/编辑任务" name="form">
                <el-form :model="newTaskForm" label-width="100px">
                  <el-form-item label="任务类型">
                    <el-radio-group v-model="newTaskForm.taskType">
                      <el-radio value="builtin">内置功能</el-radio>
                      <el-radio value="strategy">策略任务</el-radio>
                    </el-radio-group>
                  </el-form-item>
                  <el-form-item v-if="newTaskForm.taskType === 'builtin'" label="选择功能">
                    <el-select v-model="newTaskForm.module" placeholder="选择功能" style="width: 100%">
                      <el-option-group
                        v-for="group in taskCategoryGroups"
                        :key="group.category"
                        :label="group.category"
                      >
                        <el-option
                          v-for="t in group.tasks"
                          :key="t.module"
                          :label="t.name + (t.source === 'user' ? ' ★' : '')"
                          :value="t.module"
                        >
                          <span>{{ t.name }}</span>
                          <span v-if="t.source === 'user'" style="color: #E6A23C; margin-left: 4px">★</span>
                          <span v-if="!t.available" style="color: #F56C6C; margin-left: 4px">(加载失败)</span>
                        </el-option>
                      </el-option-group>
                    </el-select>
                  </el-form-item>
                  <el-form-item v-if="newTaskForm.taskType === 'strategy'" label="关联策略">
                    <el-select v-model="newTaskForm.strategyId" placeholder="选择策略" style="width: 100%">
                      <el-option
                        v-for="s in taskStrategies"
                        :key="s.id"
                        :label="s.name + (s.code_type === 'python' ? ' [代码]' : '')"
                        :value="s.id"
                      />
                    </el-select>
                  </el-form-item>
                  <el-form-item v-if="newTaskForm.taskType === 'strategy'" label="候选分组" required>
                    <el-select v-model="newTaskForm.groupId" placeholder="选择分组" style="width: 100%">
                      <el-option v-for="g in candidateGroups" :key="g.id" :label="g.name" :value="g.id" />
                    </el-select>
                  </el-form-item>
                  <el-form-item label="执行频率">
                    <el-input v-model="newTaskForm.cronText" placeholder="自然语言描述，如：每个交易日14:30执行" clearable style="flex: 1" @clear="newTaskForm.cron = ''">
                      <template #append>
                        <el-button @click="translateCron" :loading="translatingCron">LLM 转译</el-button>
                      </template>
                    </el-input>
                  </el-form-item>
                  <el-form-item label="Cron 表达式">
                    <el-input v-model="newTaskForm.cron" placeholder="如: 0 14 * * 1-5" />
                    <div v-if="cronDescription" class="hint" style="color: #67C23A">
                      已转译：{{ cronDescription }}
                    </div>
                    <div class="hint">
                      快捷模板：<br/>
                      <span class="cron-quick-btn" @click="newTaskForm.cron = '0 14 * * 1-5'; newTaskForm.cronText = '每个交易日14:00'" style="cursor: pointer; color: #409EFF; text-decoration: underline">每个交易日14:00</span> |
                      <span class="cron-quick-btn" @click="newTaskForm.cron = '0 14 * * *'; newTaskForm.cronText = '每天14:00'" style="cursor: pointer; color: #409EFF; text-decoration: underline">每天14:00</span> |
                      <span class="cron-quick-btn" @click="newTaskForm.cron = '0 * * * *'; newTaskForm.cronText = '每小时'" style="cursor: pointer; color: #409EFF; text-decoration: underline">每小时</span>
                    </div>
                  </el-form-item>
                  <el-form-item label="启用">
                    <el-switch v-model="newTaskForm.enabled" :active-value="1" :inactive-value="0" />
                  </el-form-item>
                  <el-form-item label="仅交易日">
                    <el-switch v-model="newTaskForm.requireTradingDay" :active-value="1" :inactive-value="0" />
                    <span class="hint" style="margin-left: 8px; color: #909399">开启后仅在交易日执行</span>
                  </el-form-item>
                  <el-form-item label="全市场">
                    <el-switch v-model="newTaskForm.fullMarket" :active-value="1" :inactive-value="0" />
                    <span class="hint" style="margin-left: 8px; color: #E6A23C">遍历全部A股（非交易时段使用），无需选分组</span>
                  </el-form-item>
                  <el-form-item>
                    <el-button type="primary" @click="createTask" :loading="creatingTask">
                      {{ editingTaskId ? '更新任务' : '创建任务' }}
                    </el-button>
                    <el-button v-if="editingTaskId" @click="cancelEditTask">取消编辑</el-button>
                  </el-form-item>
                </el-form>
              </el-collapse-item>
            </el-collapse>

            <!-- 任务列表 -->
            <div>
              <el-table :data="filteredStrategyTasks" stripe style="width: 100%">
                <el-table-column label="类型" width="80">
                  <template #default="{ row }">
                    <el-tag :type="row.task_type === 'builtin' ? 'warning' : 'primary'" size="small">
                      {{ row.task_type === 'builtin' ? '内置' : '策略' }}
                    </el-tag>
                  </template>
                </el-table-column>
                <el-table-column label="名称" width="160">
                  <template #default="{ row }">
                    {{ row.task_name || row.strategy_name || '-' }}
                  </template>
                </el-table-column>
                <el-table-column label="分组" width="120">
                  <template #default="{ row }">
                    {{ row.group_name || '-' }}
                  </template>
                </el-table-column>
                <el-table-column label="执行时间" width="200">
                  <template #default="{ row }">
                    <div>{{ row.cron_description || row.cron_expression }}</div>
                    <div style="font-size: 11px; color: #909399; font-family: monospace">{{ row.cron_expression }}</div>
                  </template>
                </el-table-column>
                <el-table-column label="状态" width="80">
                  <template #default="{ row }">
                    <el-tag :type="row.enabled ? 'success' : 'info'" size="small">{{ row.enabled ? '启用' : '停用' }}</el-tag>
                  </template>
                </el-table-column>
                <el-table-column label="交易日" width="70">
                  <template #default="{ row }">
                    <el-tooltip :content="row.require_trading_day ? '仅交易日执行' : '按 cron 执行'" placement="top" :show-after="0">
                      <el-tag :type="row.require_trading_day ? 'warning' : 'info'" size="small">{{ row.require_trading_day ? '是' : '-' }}</el-tag>
                    </el-tooltip>
                  </template>
                </el-table-column>
                <el-table-column label="上次执行" width="170">
                  <template #default="{ row }">
                    <span v-if="row.last_run_at">{{ row.last_run_at?.split('.')[0] }}</span>
                    <span v-else style="color: #999">未执行</span>
                  </template>
                </el-table-column>
                <el-table-column label="上次状态" width="110">
                  <template #default="{ row }">
                    <el-tooltip v-if="row.realtime_status === 'running'" :content="row.realtime_progress?.message || '正在执行'" placement="top" :show-after="0">
                      <el-tag type="warning" size="small">运行中</el-tag>
                    </el-tooltip>
                    <el-tag v-else-if="row.last_status" :type="{success:'success',error:'danger',running:'warning'}[row.last_status] || 'info'" size="small">
                      {{ {success:'成功',error:'失败',running:'运行中'}[row.last_status] || row.last_status }}
                    </el-tag>
                    <el-tooltip v-if="row.last_status === 'error' && row.last_output" :content="parseTaskError(row)" placement="top" :show-after="0">
                      <el-icon style="color: #F56C6C; cursor: pointer; margin-left: 4px"><WarningFilled /></el-icon>
                    </el-tooltip>
                  </template>
                </el-table-column>
                <el-table-column label="操作" width="320">
                  <template #default="{ row }">
                    <el-button type="primary" size="small" @click="editTask(row)">编辑</el-button>
                    <el-button :type="row.enabled ? 'warning' : 'success'" size="small" @click="toggleTask(row)">
                      {{ row.enabled ? '停用' : '启用' }}
                    </el-button>
                    <el-button type="success" size="small" @click="runTask(row)" :disabled="row.last_status === 'running'">手动执行</el-button>
                    <el-button type="danger" size="small" @click="deleteTask(row)">删除</el-button>
                  </template>
                </el-table-column>
              </el-table>
              <el-empty v-if="filteredStrategyTasks.length === 0" description="暂无任务" />
            </div>
          </el-card>
        </el-col>
      </el-row>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, onUnmounted, reactive } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { Plus, Refresh, WarningFilled } from '@element-plus/icons-vue'
import NavBar from '../components/NavBar.vue'
import { useAccountStore } from '../stores/account'

const accountStore = useAccountStore()
const currentAccountId = computed(() => accountStore.currentAccountId)

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
const postMarketLoading = ref(false)

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

// 盘后分析任务状态
const postMarketTask = computed(() => allTasksStatus.value['post_market_analysis'])

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

const postMarketProgressStatus = computed(() => {
  const t = postMarketTask.value
  if (!t) return ''
  return t.status === 'completed' ? 'success' : t.status === 'failed' ? 'exception' : ''
})

// 策略任务
const strategyTasks = ref([])
const creatingTask = ref(false)
const scanningTasks = ref(false)
const translatingCron = ref(false)
const editingTaskId = ref(null)
const taskRegistry = ref([])
const taskStrategies = ref([])
const candidateGroups = ref([])
const taskFilterGroup = ref(null)
const showCreateTaskForm = ref(false)
const activeTaskForm = ref('form')
const cronDescription = ref('')

const newTaskForm = reactive({
  taskType: 'builtin',
  module: null,
  strategyId: null,
  groupId: null,
  cron: '',
  cronText: '',
  enabled: 1,
  requireTradingDay: 0,
  fullMarket: 0
})

const filteredStrategyTasks = computed(() => {
  if (!taskFilterGroup.value) return strategyTasks.value
  return strategyTasks.value.filter(t => t.group_id === taskFilterGroup.value)
})

const taskCategoryGroups = computed(() => {
  const groups = {}
  for (const t of taskRegistry.value) {
    if (!groups[t.category]) groups[t.category] = []
    groups[t.category].push(t)
  }
  return Object.entries(groups).map(([category, tasks]) => ({ category, tasks }))
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
    const res = await fetch('/api/v1/ui/factors/daily/fill-empty', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ lookback_days: 365 })
    })
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

// 盘后分析
async function triggerPostMarketAnalysis() {
  postMarketLoading.value = true
  try {
    const res = await fetch('/api/v1/ui/scheduler/post-market-analysis', { method: 'POST' })
    const data = await res.json()
    ElMessage.success(data.message || '盘后分析任务已启动')
    loadTasksStatus()
  } catch (e) {
    ElMessage.error('请求失败')
  } finally {
    postMarketLoading.value = false
  }
}

// 调度服务操作
async function toggleScheduler() {
  const action = schedulerStatus.value.running ? '停止' : '启动'
  try {
    const res = await fetch(`/api/v1/ui/scheduler/${schedulerStatus.value.running ? 'stop' : 'start'}`, { method: 'POST' })
    const data = await res.json()
    ElMessage.success(data.message || `调度服务已${action}`)
    loadSchedulerStatus()
  } catch (e) {
    ElMessage.error('请求失败')
  }
}

// ========== 策略任务管理 ==========

// 后台轮询加载策略任务（静默刷新）
const loadStrategyTasks = async () => {
  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/strategy-tasks`)
    const data = await res.json()
    strategyTasks.value = data.tasks || []
  } catch (e) {
    console.error('加载任务失败:', e)
  }
}

const loadTaskRegistry = async () => {
  try {
    const res = await fetch('/api/v1/ui/scheduler/task-registry')
    const data = await res.json()
    taskRegistry.value = data.tasks || []
    if (taskRegistry.value.length > 0 && !newTaskForm.module) {
      const first = taskRegistry.value.find(t => t.available)
      if (first) newTaskForm.module = first.module
    }
  } catch (e) {
    console.error('加载任务注册表失败:', e)
  }
}

const loadCandidateGroups = async () => {
  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/candidate-groups`)
    const data = await res.json()
    candidateGroups.value = data.groups || []
  } catch (e) {
    console.error('加载候选分组失败:', e)
  }
}

const loadTaskStrategies = async () => {
  try {
    const [screeningRes, codeRes] = await Promise.all([
      fetch(`/api/v1/ui/${currentAccountId.value}/strategies`),
      fetch(`/api/v1/ui/${currentAccountId.value}/code-strategies`)
    ])
    const [screeningData, codeData] = await Promise.all([
      screeningRes.json(),
      codeRes.json()
    ])
    const screening = (screeningData.strategies || []).filter(s => s.strategy_type === 'screening')
    const code = codeData.strategies || []
    taskStrategies.value = [...screening, ...code]
  } catch (e) {
    console.error('加载策略列表失败:', e)
  }
}

const scanTasks = async () => {
  scanningTasks.value = true
  try {
    const res = await fetch('/api/v1/ui/scheduler/scan-tasks', { method: 'POST' })
    const data = await res.json()
    taskRegistry.value = data.tasks || []
    ElMessage.success(data.message || '扫描完成')
  } catch (e) {
    ElMessage.error('扫描失败')
  } finally {
    scanningTasks.value = false
  }
}

const createTask = async () => {
  if (newTaskForm.taskType === 'builtin' && !newTaskForm.module) {
    ElMessage.warning('请选择功能')
    return
  }
  if (newTaskForm.taskType === 'strategy' && !newTaskForm.strategyId) {
    ElMessage.warning('请选择策略')
    return
  }
  if (newTaskForm.taskType === 'strategy' && !newTaskForm.fullMarket && !newTaskForm.groupId) {
    ElMessage.warning('请选择候选分组')
    return
  }
  if (newTaskForm.fullMarket) {
    // 全市场必须非交易时段：cron 小时不在 9-14 范围
    const parts = newTaskForm.cron.trim().split(/\s+/)
    if (parts.length >= 2) {
      const hour = parts[1]
      if (hour !== '*' && !isNaN(hour)) {
        const h = parseInt(hour)
        if (h >= 9 && h <= 14) {
          ElMessage.warning('全市场策略请设置在非交易时段（如 15:00 之后或 9:00 之前）')
          return
        }
      }
    }
  }
  if (!newTaskForm.cron.trim()) {
    if (newTaskForm.cronText.trim()) {
      translatingCron.value = true
      try {
        const res = await fetch('/api/v1/ui/scheduler/translate-cron', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ text: newTaskForm.cronText.trim() })
        })
        const data = await res.json()
        if (data.success && data.cron) {
          newTaskForm.cron = data.cron
          cronDescription.value = data.description || '转译成功'
        } else {
          ElMessage.error(data.error || 'Cron 表达式转译失败')
          return
        }
      } catch (e) {
        ElMessage.error('Cron 表达式自动转译失败')
        return
      } finally {
        translatingCron.value = false
      }
    } else {
      ElMessage.warning('请输入 Cron 表达式或自然语言描述')
      return
    }
  }
  creatingTask.value = true
  try {
    const body = {
      task_type: newTaskForm.taskType,
      cron_expression: newTaskForm.cron,
      enabled: newTaskForm.enabled,
      require_trading_day: newTaskForm.requireTradingDay,
      full_market: newTaskForm.fullMarket,
    }
    if (newTaskForm.taskType === 'builtin') {
      body.module = newTaskForm.module
      body.account_id = 'SYSTEM'
    } else {
      body.strategy_id = newTaskForm.strategyId
      body.group_id = newTaskForm.fullMarket ? null : newTaskForm.groupId
    }

    let res
    if (editingTaskId.value) {
      res = await fetch(`/api/v1/ui/${currentAccountId.value}/strategy-tasks/${editingTaskId.value}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body)
      })
    } else {
      res = await fetch(`/api/v1/ui/${currentAccountId.value}/strategy-tasks`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body)
      })
    }
    const data = await res.json()
    if (res.ok) {
      ElMessage.success(editingTaskId.value ? '任务已更新，立即生效' : (data.message || '任务已创建'))
      cancelEditTask()
      await loadStrategyTasks()
    } else {
      ElMessage.error(data.detail || '创建失败')
    }
  } catch (e) {
    ElMessage.error(editingTaskId.value ? '更新失败' : '创建失败')
  } finally {
    creatingTask.value = false
  }
}

const cancelEditTask = () => {
  editingTaskId.value = null
  newTaskForm.taskType = 'builtin'
  newTaskForm.module = null
  newTaskForm.strategyId = null
  newTaskForm.groupId = null
  newTaskForm.cron = ''
  newTaskForm.cronText = ''
  newTaskForm.enabled = 1
  newTaskForm.requireTradingDay = 0
  cronDescription.value = ''
}

const translateCron = async () => {
  if (!newTaskForm.cronText.trim()) {
    ElMessage.warning('请输入自然语言描述')
    return
  }
  translatingCron.value = true
  try {
    const res = await fetch('/api/v1/ui/scheduler/translate-cron', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ text: newTaskForm.cronText.trim() })
    })
    const data = await res.json()
    if (data.success && data.cron) {
      newTaskForm.cron = data.cron
      cronDescription.value = data.description || '转译成功'
      ElMessage.success('Cron 表达式已生成')
    } else {
      ElMessage.error(data.error || '转译失败')
    }
  } catch (e) {
    ElMessage.error('转译请求失败')
  } finally {
    translatingCron.value = false
  }
}

const parseTaskError = (task) => {
  try {
    const output = JSON.parse(task.last_output)
    if (output.error) return output.error
    if (output.warnings && output.warnings.length > 0) return output.warnings.join('; ')
    return '执行失败，无详细错误信息'
  } catch {
    return '错误信息解析失败'
  }
}

const runTask = async (task) => {
  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/strategy-tasks/${task.id}/run`, { method: 'POST' })
    const data = await res.json()
    if (res.ok) {
      ElMessage.success('任务已启动')
      setTimeout(() => loadStrategyTasks(), 2000)
    } else {
      ElMessage.error(data.detail || '执行失败')
    }
  } catch (e) {
    ElMessage.error('执行失败')
  }
}

const deleteTask = async (task) => {
  try {
    await ElMessageBox.confirm('确定删除该任务？', '确认删除', { type: 'warning' })
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/strategy-tasks/${task.id}`, { method: 'DELETE' })
    if (res.ok) {
      ElMessage.success('任务已删除')
      await loadStrategyTasks()
    }
  } catch (e) {
    if (e !== 'cancel') ElMessage.error('删除失败')
  }
}

const editTask = (task) => {
  editingTaskId.value = task.id
  newTaskForm.taskType = task.task_type || 'strategy'
  newTaskForm.module = task.module || null
  newTaskForm.strategyId = task.strategy_id || null
  newTaskForm.groupId = task.group_id || null
  newTaskForm.cron = task.cron_expression || ''
  newTaskForm.cronText = ''
  newTaskForm.enabled = task.enabled
  newTaskForm.requireTradingDay = task.require_trading_day || 0
  cronDescription.value = ''
  showCreateTaskForm.value = true
  activeTaskForm.value = 'form'
}

const toggleTask = async (task) => {
  try {
    const newEnabled = task.enabled ? 0 : 1
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/strategy-tasks/${task.id}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ enabled: newEnabled })
    })
    if (res.ok) {
      ElMessage.success(newEnabled ? '任务已启用' : '任务已停用')
      await loadStrategyTasks()
    } else {
      const data = await res.json()
      ElMessage.error(data.detail || '操作失败')
    }
  } catch (e) {
    ElMessage.error('操作失败')
  }
}

onMounted(() => {
  refreshAll()
  loadStrategyTasks()
  loadTaskRegistry()
  loadCandidateGroups()
  loadTaskStrategies()
  statusTimer = setInterval(() => {
    loadTasksStatus()
    loadStrategyTasks()
  }, 5000)
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

/* 策略任务 */
.hint { font-size: 12px; color: #909399; margin-top: 8px; line-height: 1.6; }
.hint code { background: #f5f7fa; padding: 1px 4px; border-radius: 3px; font-family: monospace; color: #606266; }
.cron-quick-btn { transition: color 0.2s; }
.cron-quick-btn:hover { color: #66b1ff !important; }
</style>
