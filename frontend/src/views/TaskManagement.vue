<template>
  <div class="layout-container">
    <NavBar />
    <div class="content">
      <el-row :gutter="20" class="cards-row">
        <!-- 任务管理 -->
        <el-col :span="24">
          <el-card shadow="hover">
            <template #header>
              <div class="card-header">
                <span>任务列表</span>
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

            <!-- 双标签页 -->
            <el-tabs v-model="activeTab">
              <!-- 系统任务（仅 admin） -->
              <el-tab-pane v-if="accountStore.isAdmin" label="系统任务" name="system">
                <el-table :data="systemTasks" stripe style="width: 100%">
                  <el-table-column label="名称" width="200">
                    <template #default="{ row }">{{ row.task_name || row.module }}</template>
                  </el-table-column>
                  <el-table-column label="执行时间" width="220">
                    <template #default="{ row }">
                      <span v-if="row.cron_expression">{{ row.cron_description || row.cron_expression }}</span>
                      <el-tag v-else type="info" size="small">手动启动</el-tag>
                    </template>
                  </el-table-column>
                  <el-table-column label="状态" width="100">
                    <template #default="{ row }">
                      <el-tag :type="row.enabled ? 'success' : 'info'" size="small">{{ row.enabled ? '启用' : '停用' }}</el-tag>
                    </template>
                  </el-table-column>
                  <el-table-column label="上次执行" width="170">
                    <template #default="{ row }">
                      <span v-if="row.last_run_at">{{ row.last_run_at?.split('.')[0] }}</span>
                      <span v-else style="color: #999">未执行</span>
                    </template>
                  </el-table-column>
                  <el-table-column label="上次状态" width="100">
                    <template #default="{ row }">
                      <el-tag v-if="row.last_status" :type="{success:'success',error:'danger',running:'warning'}[row.last_status] || 'info'" size="small">
                        {{ {success:'成功',error:'失败',running:'运行中'}[row.last_status] || row.last_status }}
                      </el-tag>
                      <span v-else style="color: #999">-</span>
                    </template>
                  </el-table-column>
                  <el-table-column label="操作" width="200">
                    <template #default="{ row }">
                      <el-button :type="row.enabled ? 'warning' : 'success'" size="small" @click="toggleSystemTask(row)">
                        {{ row.enabled ? '停用' : '启用' }}
                      </el-button>
                      <el-button type="success" size="small" @click="runSystemTask(row)" :disabled="row.last_status === 'running'">手动执行</el-button>
                    </template>
                  </el-table-column>
                </el-table>
                <el-empty v-if="systemTasks.length === 0" description="暂无系统任务" />
              </el-tab-pane>

              <!-- 策略任务 -->
              <el-tab-pane label="策略任务" name="strategy">
                <!-- 添加/编辑任务表单 -->
                <el-collapse v-model="activeTaskForm" style="margin-bottom: 16px" v-if="showCreateTaskForm">
                  <el-collapse-item title="添加/编辑任务" name="form">
                    <el-form :model="newTaskForm" label-width="100px">
                      <el-form-item label="关联策略">
                        <el-select v-model="newTaskForm.strategyId" placeholder="选择策略" style="width: 100%">
                          <el-option
                            v-for="s in taskStrategies"
                            :key="s.id"
                            :label="s.name + (s.code_type === 'python' ? ' [代码]' : '')"
                            :value="s.id"
                          />
                        </el-select>
                      </el-form-item>
                      <el-form-item label="候选分组" required>
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
                      <el-form-item label="信号处理">
                        <el-radio-group v-model="newTaskForm.signalAction">
                          <el-radio value="trade">直接交易</el-radio>
                          <el-radio value="watch">继续观察</el-radio>
                        </el-radio-group>
                        <div class="hint" style="color: #909399">"直接交易"写入pending待监控执行，"继续观察"写入watching仅跟踪</div>
                      </el-form-item>
                      <el-form-item v-if="newTaskForm.signalAction === 'watch'" label="目标分组">
                        <el-select v-model="newTaskForm.targetGroupId" placeholder="选择信号输出分组（不选则写入源分组）" style="width: 100%" clearable>
                          <el-option v-for="g in targetGroupOptions" :key="g.id" :label="g.name" :value="g.id" :disabled="g.id === newTaskForm.groupId" />
                        </el-select>
                        <div class="hint" style="color: #E6A23C">二次筛选结果写入此分组，不可与源分组相同</div>
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
                <el-table :data="filteredStrategyTasks" stripe style="width: 100%">
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
                <el-empty v-if="filteredStrategyTasks.length === 0" description="暂无策略任务" />
              </el-tab-pane>
            </el-tabs>
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

// 调度状态
const schedulerStatus = ref({ running: false, jobs: [] })

// 轮询定时器
let statusTimer = null

// 标签页状态
const activeTab = ref('strategy')

// 系统任务列表（仅 admin）
const systemTasks = ref([])

// 策略任务列表
const strategyTasks = ref([])
const creatingTask = ref(false)
const scanningTasks = ref(false)
const translatingCron = ref(false)
const editingTaskId = ref(null)
const taskStrategies = ref([])
const candidateGroups = ref([])
const taskFilterGroup = ref(null)
const showCreateTaskForm = ref(false)
const activeTaskForm = ref('form')
const cronDescription = ref('')

const newTaskForm = reactive({
  taskType: 'strategy',  // 策略任务标签页只允许创建 strategy 类型
  strategyId: null,
  groupId: null,
  cron: '',
  cronText: '',
  enabled: 1,
  requireTradingDay: 0,
  fullMarket: 0,
  signalAction: 'trade',
  targetGroupId: null
})

const filteredStrategyTasks = computed(() => {
  if (!taskFilterGroup.value) return strategyTasks.value
  return strategyTasks.value.filter(t => t.group_id === taskFilterGroup.value)
})

const targetGroupOptions = computed(() => {
  return candidateGroups.value.filter(g => g.id !== newTaskForm.groupId)
})

// 加载调度状态
async function loadSchedulerStatus() {
  try {
    const res = await fetch('/api/v1/ui/scheduler/status')
    schedulerStatus.value = await res.json()
  } catch (e) {
    console.error('Failed to load scheduler status:', e)
  }
}

// 加载系统任务（仅 admin）
async function loadSystemTasks() {
  if (!accountStore.isAdmin) return
  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/system-tasks`)
    const data = await res.json()
    systemTasks.value = data.tasks || []
  } catch (e) {
    console.error('加载系统任务失败:', e)
  }
}

// 加载策略任务（不含系统任务）
async function loadStrategyTasks() {
  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/strategy-tasks-only`)
    const data = await res.json()
    strategyTasks.value = data.tasks || []
  } catch (e) {
    console.error('加载任务失败:', e)
  }
}

// 加载候选分组
async function loadCandidateGroups() {
  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/candidate-groups`)
    const data = await res.json()
    candidateGroups.value = data.groups || []
  } catch (e) {
    console.error('加载候选分组失败:', e)
  }
}

// 加载策略列表
async function loadTaskStrategies() {
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

// 扫描任务插件
async function scanTasks() {
  scanningTasks.value = true
  try {
    const res = await fetch('/api/v1/ui/scheduler/scan-tasks', { method: 'POST' })
    const data = await res.json()
    ElMessage.success(data.message || '扫描完成')
  } catch (e) {
    ElMessage.error('扫描失败')
  } finally {
    scanningTasks.value = false
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

// 系统任务操作
async function toggleSystemTask(task) {
  try {
    const newEnabled = task.enabled ? 0 : 1
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/strategy-tasks/${task.id}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ enabled: newEnabled })
    })
    if (res.ok) {
      ElMessage.success(newEnabled ? '任务已启用' : '任务已停用')
      loadSystemTasks()
    } else {
      const data = await res.json()
      ElMessage.error(data.detail || '操作失败')
    }
  } catch (e) {
    ElMessage.error('操作失败')
  }
}

async function runSystemTask(task) {
  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/strategy-tasks/${task.id}/run`, { method: 'POST' })
    const data = await res.json()
    if (res.ok) {
      ElMessage.success('任务已启动')
      setTimeout(() => loadSystemTasks(), 2000)
    } else {
      ElMessage.error(data.detail || '执行失败')
    }
  } catch (e) {
    ElMessage.error('执行失败')
  }
}

// 创建任务
async function createTask() {
  if (!newTaskForm.strategyId) {
    ElMessage.warning('请选择策略')
    return
  }
  if (!newTaskForm.fullMarket && !newTaskForm.groupId) {
    ElMessage.warning('请选择候选分组')
    return
  }
  if (newTaskForm.fullMarket) {
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
      task_type: 'strategy',
      cron_expression: newTaskForm.cron,
      enabled: newTaskForm.enabled,
      require_trading_day: newTaskForm.requireTradingDay,
      full_market: newTaskForm.fullMarket,
      signal_action: newTaskForm.signalAction,
      strategy_id: newTaskForm.strategyId,
      group_id: newTaskForm.fullMarket ? null : newTaskForm.groupId,
    }
    if (newTaskForm.signalAction === 'watch' && newTaskForm.targetGroupId) {
      body.target_group_id = newTaskForm.targetGroupId
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

function cancelEditTask() {
  editingTaskId.value = null
  newTaskForm.strategyId = null
  newTaskForm.groupId = null
  newTaskForm.cron = ''
  newTaskForm.cronText = ''
  newTaskForm.enabled = 1
  newTaskForm.requireTradingDay = 0
  newTaskForm.fullMarket = 0
  newTaskForm.signalAction = 'trade'
  newTaskForm.targetGroupId = null
  cronDescription.value = ''
}

async function translateCron() {
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

function parseTaskError(task) {
  try {
    const output = JSON.parse(task.last_output)
    if (output.error) return output.error
    if (output.warnings && output.warnings.length > 0) return output.warnings.join('; ')
    return '执行失败，无详细错误信息'
  } catch {
    return '错误信息解析失败'
  }
}

async function runTask(task) {
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

async function deleteTask(task) {
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

function editTask(task) {
  editingTaskId.value = task.id
  newTaskForm.strategyId = task.strategy_id || null
  newTaskForm.groupId = task.group_id || null
  newTaskForm.cron = task.cron_expression || ''
  newTaskForm.cronText = ''
  newTaskForm.enabled = task.enabled
  newTaskForm.requireTradingDay = task.require_trading_day || 0
  newTaskForm.fullMarket = task.full_market || 0
  newTaskForm.signalAction = task.signal_action || 'trade'
  newTaskForm.targetGroupId = task.target_group_id || null
  cronDescription.value = ''
  showCreateTaskForm.value = true
  activeTaskForm.value = 'form'
}

async function toggleTask(task) {
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
  loadSchedulerStatus()
  loadSystemTasks()
  loadStrategyTasks()
  loadCandidateGroups()
  loadTaskStrategies()
  statusTimer = setInterval(() => {
    loadSchedulerStatus()
    loadSystemTasks()
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

.hint { font-size: 12px; color: #909399; margin-top: 8px; line-height: 1.6; }
.hint code { background: #f5f7fa; padding: 1px 4px; border-radius: 3px; font-family: monospace; color: #606266; }
.cron-quick-btn { transition: color 0.2s; }
.cron-quick-btn:hover { color: #66b1ff !important; }
</style>