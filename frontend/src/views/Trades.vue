<template>
  <div class="layout-container">
    <NavBar />
    <el-main class="main-content">
      <h2>交易监控 - {{ currentAccount?.display_name }}</h2>

      <!-- 统计卡片 -->
      <el-row :gutter="20" class="stats-row">
        <el-col :span="6">
          <el-statistic title="总交易笔数" :value="stats.totalCount" />
        </el-col>
        <el-col :span="6">
          <el-statistic title="买入笔数" :value="stats.buyCount" />
        </el-col>
        <el-col :span="6">
          <el-statistic title="卖出笔数" :value="stats.sellCount" />
        </el-col>
        <el-col :span="6">
          <el-statistic title="成功率" :value="stats.winRate" suffix="%" />
        </el-col>
      </el-row>

      <!-- 三标签页 -->
      <el-tabs v-model="activeTab" type="card">
        <!-- 标签页 1：交易明细 -->
        <el-tab-pane label="交易明细" name="trades">
          <el-card>
            <template #header>
              <div class="card-header">
                <span>交易明细</span>
                <el-space>
                  <el-date-picker
                    v-model="dateRange"
                    type="daterange"
                    range-separator="至"
                    start-placeholder="开始日期"
                    end-placeholder="结束日期"
                    size="small"
                    @change="loadTrades"
                  />
                  <el-button type="primary" size="small" @click="loadTrades">
                    <el-icon><Refresh /></el-icon>
                    刷新
                  </el-button>
                </el-space>
              </div>
            </template>

            <el-table :data="trades" stripe style="width: 100%">
              <el-table-column prop="trade_time" label="时间" width="160" />
              <el-table-column prop="stock_code" label="股票代码" width="100" />
              <el-table-column prop="stock_name" label="股票名称" width="100" />
              <el-table-column prop="trade_type" label="操作" width="80">
                <template #default="{ row }">
                  <el-tag :type="row.trade_type === 'buy' ? 'danger' : 'success'">
                    {{ row.trade_type === 'buy' ? '买入' : '卖出' }}
                  </el-tag>
                </template>
              </el-table-column>
              <el-table-column prop="quantity" label="数量" width="100" align="right" />
              <el-table-column prop="price" label="价格" width="100" align="right">
                <template #default="{ row }">¥{{ formatNumber(row.price) }}</template>
              </el-table-column>
              <el-table-column prop="amount" label="金额" width="120" align="right">
                <template #default="{ row }">¥{{ formatNumber(row.amount) }}</template>
              </el-table-column>
              <el-table-column prop="commission" label="手续费" width="100" align="right">
                <template #default="{ row }">¥{{ formatNumber(row.commission) }}</template>
              </el-table-column>
              <el-table-column prop="trigger_source" label="触发来源" width="120" />
              <el-table-column prop="status" label="状态" width="80">
                <template #default="{ row }">
                  <el-tag :type="row.status === 'completed' ? 'success' : 'warning'" size="small">
                    {{ row.status === 'completed' ? '成功' : row.status }}
                  </el-tag>
                </template>
              </el-table-column>
            </el-table>
          </el-card>
        </el-tab-pane>

        <!-- 标签页 2：交易信号 -->
        <el-tab-pane label="交易信号" name="signals">
          <el-card>
            <template #header>
              <div class="card-header">
                <span>交易信号</span>
                <el-space>
                  <el-button type="primary" size="small" @click="showStrategyDialog">
                    <el-icon><Plus /></el-icon>
                    新建交易策略
                  </el-button>
                  <el-button size="small" @click="loadSignals">
                    <el-icon><Refresh /></el-icon>
                    刷新
                  </el-button>
                </el-space>
              </div>
            </template>

            <el-table :data="signals" stripe style="width: 100%">
              <el-table-column prop="created_at" label="时间" width="160" />
              <el-table-column prop="stock_code" label="股票代码" width="100" />
              <el-table-column prop="stock_name" label="股票名称" width="100" />
              <el-table-column prop="signal_type" label="信号类型" width="140">
                <template #default="{ row }">
                  <el-tag :type="getSignalTypeColor(row.signal_type)" size="small">
                    {{ formatSignalType(row.signal_type) }}
                  </el-tag>
                </template>
              </el-table-column>
              <el-table-column prop="price" label="触发价格" width="100" align="right">
                <template #default="{ row }">¥{{ formatNumber(row.price) }}</template>
              </el-table-column>
              <el-table-column prop="target_quantity" label="目标数量" width="100" align="right" />
              <el-table-column prop="status" label="状态" width="80">
                <template #default="{ row }">
                  <el-tag :type="row.status === 'executed' || row.status === 'completed' ? 'success' : row.status === 'pending' ? 'warning' : 'info'" size="small">
                    {{ row.status === 'executed' || row.status === 'completed' ? '已执行' : row.status === 'pending' ? '待执行' : row.status === 'cancelled' ? '已取消' : row.status }}
                  </el-tag>
                </template>
              </el-table-column>
            </el-table>
          </el-card>
        </el-tab-pane>

        <!-- 标签页 3：通知记录 -->
        <el-tab-pane label="通知记录" name="notifications">
          <el-card>
            <template #header>
              <div class="card-header">
                <span>通知记录</span>
                <el-space>
                  <el-select v-model="notificationFilter" size="small" @change="loadNotifications">
                    <el-option label="全部" value="" />
                    <el-option label="成交通知" value="trade_executed" />
                    <el-option label="信号触发" value="signal_triggered" />
                    <el-option label="任务完成" value="task_completed" />
                    <el-option label="任务失败" value="task_failed" />
                  </el-select>
                  <el-button size="small" @click="loadNotifications">
                    <el-icon><Refresh /></el-icon>
                    刷新
                  </el-button>
                </el-space>
              </div>
            </template>

            <el-table :data="notifications" stripe style="width: 100%">
              <el-table-column prop="created_at" label="时间" width="180">
                <template #default="{ row }">
                  {{ formatTime(row.created_at) }}
                </template>
              </el-table-column>
              <el-table-column prop="event_type" label="事件类型" width="120">
                <template #default="{ row }">
                  <el-tag :type="getEventTypeColor(row.event_type)" size="small">
                    {{ formatEventType(row.event_type) }}
                  </el-tag>
                </template>
              </el-table-column>
              <el-table-column prop="title" label="标题" width="120" />
              <el-table-column prop="channel" label="渠道" width="80" />
              <el-table-column prop="content" label="内容" min-width="300" show-overflow-tooltip />
              <el-table-column prop="status" label="状态" width="80">
                <template #default="{ row }">
                  <el-tag :type="row.status === 'sent' ? 'success' : 'danger'" size="small">
                    {{ row.status === 'sent' ? '已发' : '失败' }}
                  </el-tag>
                </template>
              </el-table-column>
            </el-table>
          </el-card>
        </el-tab-pane>
      </el-tabs>
    </el-main>

    <!-- 交易策略创建/编辑对话框 -->
    <el-dialog v-model="strategyDialogVisible" :title="strategyDialogTitle" width="900px">
      <!-- 已有策略列表 -->
      <el-table :data="strategyList" stripe style="width: 100%" max-height="250">
        <el-table-column prop="name" label="策略名称" width="120" />
        <el-table-column label="策略类型" width="110">
          <template #default="{ row }">
            {{ formatStrategyType(row.strategy_type) }}
          </template>
        </el-table-column>
        <el-table-column label="方向" width="70">
          <template #default="{ row }">
            <el-tag :type="row.action === 'buy' ? 'danger' : 'success'" size="small">
              {{ row.action === 'buy' ? '买入' : '卖出' }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column label="状态" width="70">
          <template #default="{ row }">
            <el-tag :type="row.enabled ? 'success' : 'info'" size="small">
              {{ row.enabled ? '启用' : '停用' }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="cooldown_seconds" label="冷却(秒)" width="80" />
        <el-table-column label="操作" width="120">
          <template #default="{ row }">
            <el-button size="small" @click="editStrategy(row)">编辑</el-button>
            <el-button size="small" type="danger" @click="deleteStrategy(row.id)">删除</el-button>
          </template>
        </el-table-column>
      </el-table>

      <el-divider />
      <el-form :model="strategyForm" label-width="100px">
        <el-form-item label="策略名称">
          <el-input v-model="strategyForm.name" placeholder="如：突破买入监测" />
        </el-form-item>
        <el-form-item label="策略类型">
          <el-select v-model="strategyForm.strategy_type" placeholder="选择策略类型">
            <el-option label="价格监测" value="price_monitor" />
            <el-option label="涨跌幅监测" value="change_pct_monitor" />
            <el-option label="交易量监测" value="volume_monitor" />
          </el-select>
        </el-form-item>
        <el-form-item label="操作方向">
          <el-radio-group v-model="strategyForm.action">
            <el-radio label="buy">买入</el-radio>
            <el-radio label="sell">卖出</el-radio>
          </el-radio-group>
        </el-form-item>
        <el-form-item label="目标股票">
          <el-radio-group v-model="strategyForm.target_mode">
            <el-radio label="all">全部 watchlist</el-radio>
            <el-radio label="specific">指定代码</el-radio>
          </el-radio-group>
        </el-form-item>
        <el-form-item v-if="strategyForm.target_mode === 'specific'" label="股票代码">
          <el-input v-model="strategyForm.target_stocks_str" placeholder="多个代码用逗号分隔，如 600000.SH,000001.SZ" />
        </el-form-item>
        <el-form-item label="冷却时间">
          <el-input-number v-model="strategyForm.cooldown_seconds" :min="60" :max="3600" /> 秒
        </el-form-item>

        <!-- 价格监测条件 -->
        <template v-if="strategyForm.strategy_type === 'price_monitor'">
          <el-form-item label="价格方向">
            <el-select v-model="strategyForm.condition_direction">
              <el-option label="突破（高于）" value="above" />
              <el-option label="跌破（低于）" value="below" />
            </el-select>
          </el-form-item>
          <el-form-item label="目标价格">
            <el-input-number v-model="strategyForm.condition_target_price" :precision="2" :min="0" />
          </el-form-item>
        </template>

        <!-- 涨跌幅监测条件 -->
        <template v-if="strategyForm.strategy_type === 'change_pct_monitor'">
          <el-form-item label="涨跌方向">
            <el-select v-model="strategyForm.condition_direction">
              <el-option label="上涨超过" value="up" />
              <el-option label="下跌超过" value="down" />
            </el-select>
          </el-form-item>
          <el-form-item label="阈值(%)">
            <el-input-number v-model="strategyForm.condition_threshold" :precision="2" :min="0" :max="20" />
          </el-form-item>
        </template>

        <!-- 交易量监测条件 -->
        <template v-if="strategyForm.strategy_type === 'volume_monitor'">
          <el-form-item label="监测模式">
            <el-select v-model="strategyForm.condition_mode">
              <el-option label="量比模式" value="ratio" />
              <el-option label="绝对量模式" value="absolute" />
            </el-select>
          </el-form-item>
          <el-form-item label="阈值">
            <el-input-number v-model="strategyForm.condition_threshold" :precision="2" :min="0" />
            <span v-if="strategyForm.condition_mode === 'ratio'" style="margin-left: 8px; color: #999;">（倍于均量）</span>
          </el-form-item>
        </template>

        <el-form-item label="启用">
          <el-switch v-model="strategyForm.enabled" :active-value="1" :inactive-value="0" />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="strategyDialogVisible = false">取消</el-button>
        <el-button type="primary" @click="saveStrategy">保存</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, reactive, onMounted, computed, watch } from 'vue'
import { useAccountStore } from '../stores/account'
import { ElMessage } from 'element-plus'
import NavBar from '../components/NavBar.vue'

const accountStore = useAccountStore()
const currentAccount = computed(() => accountStore.currentAccount)
const currentAccountId = computed(() => accountStore.currentAccountId)

const activeTab = ref('trades')

// === 交易明细 ===
const dateRange = ref([])
const trades = ref([])
const stats = reactive({
  totalCount: 0,
  buyCount: 0,
  sellCount: 0,
  winRate: 0
})

const loadTrades = async () => {
  try {
    const response = await fetch(`/api/v1/ui/${currentAccountId.value}/trades/today`)
    const data = await response.json()
    trades.value = data.trades || []
    stats.totalCount = data.stats?.total_count || 0
    stats.buyCount = data.stats?.buy_count || 0
    stats.sellCount = data.stats?.sell_count || 0
  } catch (error) {
    console.error('加载交易数据失败:', error)
  }
}

// === 交易信号 ===
const signals = ref([])

const loadSignals = async () => {
  try {
    const response = await fetch(`/api/v1/ui/${currentAccountId.value}/trading-signals?limit=100`)
    const data = await response.json()
    signals.value = data.signals || []
  } catch (error) {
    console.error('加载信号数据失败:', error)
  }
}

const formatSignalType = (type) => {
  const map = {
    'buy_executed': '买入执行',
    'buy_failed': '买入失败',
    'sell_stop_loss': '止损卖出',
    'sell_take_profit': '止盈卖出',
  }
  return map[type] || type
}

const getSignalTypeColor = (type) => {
  if (type.includes('buy') || type.includes('executed')) return 'success'
  if (type.includes('stop_loss')) return 'warning'
  if (type.includes('take_profit')) return 'info'
  return 'danger'
}

// === 通知记录 ===
const notifications = ref([])
const notificationFilter = ref('')

const loadNotifications = async () => {
  try {
    const params = new URLSearchParams({ limit: 100 })
    if (notificationFilter.value) {
      params.set('event_type', notificationFilter.value)
    }
    const response = await fetch(`/api/v1/ui/${currentAccountId.value}/notifications/history?${params}`)
    const data = await response.json()
    notifications.value = data.data || []
  } catch (error) {
    console.error('加载通知历史失败:', error)
  }
}

const formatEventType = (type) => {
  const map = {
    'trade_executed': '成交',
    'signal_triggered': '信号',
    'task_completed': '任务完成',
    'task_failed': '任务失败',
    'trade_failed': '交易失败',
  }
  return map[type] || type
}

const getEventTypeColor = (type) => {
  if (type === 'trade_executed') return 'success'
  if (type === 'signal_triggered') return ''
  if (type === 'task_completed') return 'success'
  if (type === 'task_failed') return 'danger'
  return 'danger'
}

const formatNumber = (num) => {
  return Number(num || 0).toLocaleString('zh-CN', { minimumFractionDigits: 2 })
}

const formatTime = (time) => {
  if (!time) return '-'
  // naive string 默认中国时间，显式附加 +08:00
  const str = time.includes('+') || time.endsWith('Z') ? time : time + '+08:00'
  const date = new Date(str)
  if (isNaN(date.getTime())) return time
  const pad = (n) => n.toString().padStart(2, '0')
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())} ${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`
}

// === 交易策略 CRUD ===
const strategyDialogVisible = ref(false)
const strategyList = ref([])
const strategyForm = reactive({
  id: null,
  name: '',
  strategy_type: 'price_monitor',
  action: 'buy',
  target_mode: 'all',
  target_stocks_str: '',
  cooldown_seconds: 300,
  condition_direction: 'above',
  condition_target_price: 0,
  condition_threshold: 0,
  condition_mode: 'ratio',
  enabled: 1,
})

const strategyDialogTitle = computed(() => strategyForm.id ? '编辑交易策略' : '新建交易策略')

const showStrategyDialog = async () => {
  strategyForm.id = null
  strategyForm.name = ''
  strategyForm.strategy_type = 'price_monitor'
  strategyForm.action = 'buy'
  strategyForm.target_mode = 'all'
  strategyForm.target_stocks_str = ''
  strategyForm.cooldown_seconds = 300
  strategyForm.condition_direction = 'above'
  strategyForm.condition_target_price = 0
  strategyForm.condition_threshold = 0
  strategyForm.condition_mode = 'ratio'
  strategyForm.enabled = 1
  await loadStrategies()
  strategyDialogVisible.value = true
}

const saveStrategy = async () => {
  if (!strategyForm.name) {
    ElMessage.warning('请输入策略名称')
    return
  }

  // 构建条件 JSON
  let conditions = []
  if (strategyForm.strategy_type === 'price_monitor') {
    conditions = [{
      type: 'price',
      direction: strategyForm.condition_direction,
      target_price: strategyForm.condition_target_price,
    }]
  } else if (strategyForm.strategy_type === 'change_pct_monitor') {
    conditions = [{
      type: 'change_pct',
      direction: strategyForm.condition_direction,
      threshold: strategyForm.condition_threshold,
    }]
  } else if (strategyForm.strategy_type === 'volume_monitor') {
    conditions = [{
      type: 'volume',
      mode: strategyForm.condition_mode,
      threshold: strategyForm.condition_threshold,
    }]
  }

  const target_stocks = strategyForm.target_mode === 'specific'
    ? strategyForm.target_stocks_str.split(',').map(s => s.trim()).filter(s => s)
    : null

  const payload = {
    name: strategyForm.name,
    strategy_type: strategyForm.strategy_type,
    conditions: JSON.stringify(conditions),
    action: strategyForm.action,
    target_stocks: target_stocks ? JSON.stringify(target_stocks) : null,
    cooldown_seconds: strategyForm.cooldown_seconds,
    enabled: strategyForm.enabled,
  }

  try {
    const url = strategyForm.id
      ? `/api/v1/ui/${currentAccountId.value}/trading-strategies/${strategyForm.id}`
      : `/api/v1/ui/${currentAccountId.value}/trading-strategies`
    const method = strategyForm.id ? 'PUT' : 'POST'
    const resp = await fetch(url, {
      method,
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    })
    const data = await resp.json()
    if (data.success) {
      ElMessage.success(strategyForm.id ? '策略已更新' : '策略已创建')
      strategyDialogVisible.value = false
      loadStrategies()
    } else {
      ElMessage.error(data.message || '保存失败')
    }
  } catch (error) {
    ElMessage.error('保存失败: ' + error.message)
  }
}

const loadStrategies = async () => {
  try {
    const resp = await fetch(`/api/v1/ui/${currentAccountId.value}/trading-strategies`)
    const data = await resp.json()
    strategyList.value = data.strategies || []
  } catch (error) {
    console.error('加载策略列表失败:', error)
  }
}

const formatStrategyType = (type) => {
  const map = { price_monitor: '价格监测', change_pct_monitor: '涨跌幅监测', volume_monitor: '交易量监测' }
  return map[type] || type
}

const editStrategy = (row) => {
  strategyForm.id = row.id
  strategyForm.name = row.name
  strategyForm.strategy_type = row.strategy_type
  strategyForm.action = row.action
  strategyForm.target_mode = row.target_stocks ? 'specific' : 'all'
  try {
    const stocks = JSON.parse(row.target_stocks || 'null')
    strategyForm.target_stocks_str = Array.isArray(stocks) ? stocks.join(',') : ''
  } catch {
    strategyForm.target_stocks_str = ''
  }
  strategyForm.cooldown_seconds = row.cooldown_seconds || 300
  strategyForm.enabled = row.enabled
  // 解析 conditions
  try {
    const conditions = JSON.parse(row.conditions || '[]')
    const cond = conditions[0] || {}
    if (cond.type === 'price') {
      strategyForm.condition_direction = cond.direction || 'above'
      strategyForm.condition_target_price = cond.target_price || 0
    } else if (cond.type === 'change_pct') {
      strategyForm.condition_direction = cond.direction || 'up'
      strategyForm.condition_threshold = cond.threshold || 0
    } else if (cond.type === 'volume') {
      strategyForm.condition_mode = cond.mode || 'ratio'
      strategyForm.condition_threshold = cond.threshold || 0
    }
  } catch {
    // 默认值
  }
  strategyDialogVisible.value = true
}

const deleteStrategy = async (id) => {
  try {
    const resp = await fetch(`/api/v1/ui/${currentAccountId.value}/auto-trading-strategies/${id}`, { method: 'DELETE' })
    const data = await resp.json()
    if (data.success) {
      ElMessage.success('策略已删除')
      loadStrategies()
    } else {
      ElMessage.error(data.message || '删除失败')
    }
  } catch (error) {
    ElMessage.error('删除失败: ' + error.message)
  }
}

onMounted(async () => {
  await loadTrades()
  await loadSignals()
  await loadNotifications()
})

// 切换账户时刷新数据
watch(currentAccountId, async () => {
  await loadTrades()
  await loadSignals()
  await loadNotifications()
})
</script>

<style scoped>
.layout-container {
  height: 100%;
  display: flex;
  flex-direction: column;
}

.main-content {
  padding: 20px;
}

h2 {
  margin-bottom: 20px;
  color: #303133;
}

.stats-row {
  margin-bottom: 20px;
}

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
}
</style>
