<template>
  <div class="layout-container">
    <NavBar />
    <el-main class="main-content">
      <h2>交易监控 - {{ currentAccount?.display_name }}</h2>

      <!-- 主内容区：左侧手动下单 + 右侧交易数据 -->
      <div class="split-container" ref="splitContainer">
        <!-- 左侧：手动下单 -->
        <div class="order-panel" :style="{ width: leftPanelWidth + 'px' }">
          <el-card class="order-card" shadow="hover">
            <template #header>
              <div class="card-header">
                <span class="order-title">手动下单</span>
                <el-tag size="small" :type="orderForm.order_type === 'gtc' ? 'warning' : 'info'">
                  {{ orderForm.order_type === 'gtc' ? 'GTC 长期有效' : '当日有效' }}
                </el-tag>
              </div>
            </template>

            <el-form :model="orderForm" label-width="80px" label-position="top">
              <el-form-item label="证券代码">
                <el-autocomplete
                  v-model="orderForm.stock_code"
                  :fetch-suggestions="searchStocks"
                  placeholder="输入代码/名称/拼音"
                  :debounce="300"
                  value-key="stock_code"
                  @select="onStockSelect"
                  @clear="onCodeClear"
                  clearable
                >
                  <template #default="{ item }">
                    <span style="font-weight: 500; min-width: 90px">{{ item.stock_code }}</span>
                    <span style="color: #909399; margin-left: 8px">{{ item.stock_name }}</span>
                  </template>
                </el-autocomplete>
              </el-form-item>

              <el-form-item label="股票名称">
                <el-input v-model="orderForm.stock_name" placeholder="自动填充或手动输入" />
              </el-form-item>

              <div v-if="quoteInfo" class="quote-info">
                <el-row :gutter="8">
                  <el-col :span="8">
                    <span class="label">现价</span>
                    <span class="val">¥{{ quoteInfo.current_price?.toFixed(2) || '-' }}</span>
                  </el-col>
                  <el-col :span="8">
                    <span class="label">买一</span>
                    <span class="val buy">¥{{ quoteInfo.bid1?.toFixed(2) || '-' }}</span>
                  </el-col>
                  <el-col :span="8">
                    <span class="label">卖一</span>
                    <span class="val sell">¥{{ quoteInfo.ask1?.toFixed(2) || '-' }}</span>
                  </el-col>
                </el-row>
              </div>

              <el-form-item label="交易方向">
                <el-radio-group v-model="orderForm.trade_type" class="trade-type-group">
                  <el-radio-button value="buy" class="buy-btn">买入</el-radio-button>
                  <el-radio-button value="sell" class="sell-btn">卖出</el-radio-button>
                </el-radio-group>
              </el-form-item>

              <el-form-item label="委托价格">
                <el-input-number
                  v-model="orderForm.price"
                  :precision="2"
                  :step="0.01"
                  :min="0.01"
                  controls-position="right"
                  style="width: 100%"
                />
              </el-form-item>

              <el-form-item label="委托数量">
                <div style="display: flex; align-items: center; gap: 8px; width: 100%">
                  <el-input-number
                    v-model="orderForm.quantity"
                    :step="100"
                    :min="0"
                    controls-position="right"
                    style="flex: 1; min-width: 0"
                  />
                  <el-tooltip
                    :content="orderForm.trade_type === 'buy' ? `最大可买 ${maxBuyQty} 股` : `可卖 ${availableQty} 股`"
                    placement="top"
                  >
                    <el-button size="small" @click="useMaxQuantity" :disabled="!canUseMax" style="white-space: nowrap">
                      {{ orderForm.trade_type === 'buy' ? (maxBuyQty > 0 ? maxBuyQty + ' 股' : '--') : (availableQty > 0 ? availableQty + ' 股' : '--') }}
                    </el-button>
                  </el-tooltip>
                </div>
                <div v-if="orderForm.trade_type === 'sell' && positionQty !== availableQty" class="t1-hint">
                  持仓 {{ positionQty }} 股，T+1 冻结 {{ positionQty - availableQty }} 股
                </div>
              </el-form-item>

              <el-form-item label="委托类型">
                <el-radio-group v-model="orderForm.order_type" size="small">
                  <el-radio-button value="day">当日有效</el-radio-button>
                  <el-radio-button value="gtc">长期有效</el-radio-button>
                </el-radio-group>
              </el-form-item>

              <el-button
                type="primary"
                :disabled="!canSubmit"
                :loading="submitting"
                style="width: 100%; margin-top: 12px"
                @click="submitOrder"
              >
                {{ submitBtnText }}
              </el-button>
            </el-form>
          </el-card>
        </div>

        <!-- 拖拽分割线 -->
        <div class="splitter" @mousedown="startDrag">
          <div class="splitter-handle"></div>
        </div>

        <!-- 右侧：统计数据 + 标签页 -->
        <div class="right-panel">
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

          <!-- 标签页 -->
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
                      <el-dropdown @command="handleExportTrades">
                        <el-button type="success" size="small"><el-icon><Download /></el-icon>导出</el-button>
                        <template #dropdown>
                          <el-dropdown-menu>
                            <el-dropdown-item command="csv">CSV</el-dropdown-item>
                            <el-dropdown-item command="json">JSON</el-dropdown-item>
                            <el-dropdown-item command="md">Markdown</el-dropdown-item>
                            <el-dropdown-item command="txt">TXT</el-dropdown-item>
                            <el-dropdown-item command="excel">Excel</el-dropdown-item>
                          </el-dropdown-menu>
                        </template>
                      </el-dropdown>
                    </el-space>
                  </div>
                </template>

                <el-table :data="trades" stripe style="width: 100%">
                  <el-table-column prop="trade_time" label="时间" min-width="160" max-width="180" />
                  <el-table-column prop="stock_code" label="代码" min-width="90" max-width="110" />
                  <el-table-column prop="stock_name" label="名称" min-width="100" max-width="150" show-overflow-tooltip />
                  <el-table-column prop="trade_type" label="操作" width="75">
                    <template #default="{ row }">
                      <el-tag :type="row.trade_type === 'buy' ? 'danger' : 'success'">
                        {{ row.trade_type === 'buy' ? '买入' : '卖出' }}
                      </el-tag>
                    </template>
                  </el-table-column>
                  <el-table-column prop="quantity" label="数量" min-width="80" max-width="100" align="right" />
                  <el-table-column prop="price" label="价格" min-width="90" max-width="110" align="right">
                    <template #default="{ row }">¥{{ formatNumber(row.price) }}</template>
                  </el-table-column>
                  <el-table-column prop="amount" label="金额" min-width="110" max-width="140" align="right">
                    <template #default="{ row }">¥{{ formatNumber(row.amount) }}</template>
                  </el-table-column>
                  <el-table-column prop="commission" label="手续费" min-width="90" max-width="120" align="right">
                    <template #default="{ row }">¥{{ formatNumber(row.commission) }}</template>
                  </el-table-column>
                  <el-table-column prop="trigger_source" label="触发来源" min-width="90" max-width="130" show-overflow-tooltip />
                  <el-table-column prop="status" label="状态" width="75">
                    <template #default="{ row }">
                      <el-tag :type="row.status === 'completed' ? 'success' : 'warning'" size="small">
                        {{ row.status === 'completed' ? '成功' : row.status }}
                      </el-tag>
                    </template>
                  </el-table-column>
                </el-table>
              </el-card>
            </el-tab-pane>

            <!-- 标签页 2：通知记录 -->
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
        </div><!-- right-panel -->
      </div><!-- split-container -->
    </el-main>

  </div>
</template>

<script setup>
import { ref, reactive, onMounted, computed, watch } from 'vue'
import { useAccountStore } from '../stores/account'
import { ElMessage, ElMessageBox } from 'element-plus'
import { Refresh, Download } from '@element-plus/icons-vue'
import { exportTable as doExport } from '@/utils/exportHelper'
import NavBar from '../components/NavBar.vue'

const accountStore = useAccountStore()
const currentAccount = computed(() => accountStore.currentAccount)
const currentAccountId = computed(() => accountStore.currentAccountId)

const activeTab = ref('trades')

// ============================================================
// 手动下单
// ============================================================

const orderForm = ref({
  stock_code: '',
  stock_name: '',
  trade_type: 'buy',
  price: 0,
  quantity: 100,
  order_type: 'day',
})

const quoteInfo = ref(null)
const submitting = ref(false)
const leftPanelWidth = ref(300)
const splitContainer = ref(null)

const maxBuyQty = ref(0)
const fundLimitQty = ref(0)
const positionQty = ref(0)
const availableQty = ref(0)

function normalizeStockCode(code) {
  if (!code) return code
  code = code.trim().toUpperCase()
  if (code.includes('.')) return code
  const prefix = code.length >= 2 ? code.slice(0, 2) : ''
  const shPrefixes = ['60', '68', '65', '50', '51', '52', '53', '54', '55', '56', '57', '58', '69']
  const szPrefixes = ['00', '20', '30']
  if (shPrefixes.includes(prefix) || code.startsWith('9')) return `${code}.SH`
  if (szPrefixes.includes(prefix)) return `${code}.SZ`
  return `${code}.BJ`
}

function resetQuoteInfo() {
  quoteInfo.value = null
  orderForm.value.stock_name = ''
  orderForm.value.price = 0
  maxBuyQty.value = 0
  fundLimitQty.value = 0
  positionQty.value = 0
  availableQty.value = 0
}

async function searchStocks(query, cb) {
  if (!query || query.trim().length === 0) { cb([]); return }
  try {
    const resp = await fetch(`/api/v1/ui/stocks/search?q=${encodeURIComponent(query.trim())}&limit=15`)
    const data = await resp.json()
    if (data.success && data.stocks) {
      cb(data.stocks.map(s => ({ stock_code: s.stock_code, stock_name: s.stock_name, value: s.stock_code })))
    } else { cb([]) }
  } catch { cb([]) }
}

async function onStockSelect(item) {
  orderForm.value.stock_code = normalizeStockCode(item.stock_code)
  await lookupAndCalculate()
}

function onCodeClear() {
  orderForm.value.stock_code = ''
  resetQuoteInfo()
}

async function lookupAndCalculate() {
  const code = orderForm.value.stock_code
  if (!code || !code.includes('.')) return
  resetQuoteInfo()
  try {
    const resp = await fetch(`/api/v1/ui/${currentAccountId.value}/manual-order/quote`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ stock_code: code }),
    })
    const data = await resp.json()
    if (data.success) {
      quoteInfo.value = { current_price: data.current_price, bid1: data.bid1, ask1: data.ask1 }
      orderForm.value.stock_name = data.stock_name || '-'
      if (data.current_price && data.current_price > 0) orderForm.value.price = data.current_price
      maxBuyQty.value = data.max_buy_quantity || 0
      fundLimitQty.value = data.fund_limit_quantity || 0
      positionQty.value = data.position_quantity || 0
      availableQty.value = data.available_quantity || 0
    } else {
      quoteInfo.value = null
      ElMessage.error(data.message || '券商服务器连接失败')
    }
  } catch (e) {
    quoteInfo.value = null
    ElMessage.error('获取行情失败：' + e.message)
  }
}

function useMaxQuantity() {
  if (orderForm.value.trade_type === 'buy' && fundLimitQty.value > 0) {
    orderForm.value.quantity = fundLimitQty.value
  } else if (orderForm.value.trade_type === 'sell' && availableQty.value > 0) {
    orderForm.value.quantity = availableQty.value
  }
}

const canSubmit = computed(() => {
  return orderForm.value.stock_code && orderForm.value.price > 0 && orderForm.value.quantity > 0 && orderForm.value.quantity % 100 === 0 && !submitting.value
})

const canUseMax = computed(() => {
  if (orderForm.value.trade_type === 'buy') return maxBuyQty.value > 0
  return availableQty.value > 0
})

const submitBtnText = computed(() => {
  const dir = orderForm.value.trade_type === 'buy' ? '买入' : '卖出'
  const type = orderForm.value.order_type === 'gtc' ? 'GTC' : '当日'
  return `${dir}委托（${type}）`
})

async function submitOrder() {
  const { stock_code, stock_name, trade_type, price, quantity, order_type } = orderForm.value
  let confirmed = false
  if (trade_type === 'buy' && maxBuyQty.value > 0 && quantity > maxBuyQty.value) {
    try {
      await ElMessageBox.confirm(
        `当前数量 ${quantity} 股 超出策略单只持仓上限 ${maxBuyQty.value} 股。\n\n超出部分将在成交后由监控程序自动标记为超限，但系统不会阻止成交。是否继续？`,
        '超出持仓限制',
        { type: 'warning', confirmButtonText: '继续提交', cancelButtonText: '取消' }
      )
      confirmed = true
    } catch { return }
  }
  const direction = trade_type === 'buy' ? '买入' : '卖出'
  const typeLabel = order_type === 'gtc' ? '长期有效' : '当日有效'
  if (!confirmed) {
    try {
      await ElMessageBox.confirm(
        `确认${direction}委托（${typeLabel}）？\n\n股票：${stock_name || stock_code}\n代码：${stock_code}\n价格：¥${price.toFixed(2)}\n数量：${quantity} 股`,
        `确认${direction}`,
        { type: 'warning' }
      )
    } catch { return }
  }
  submitting.value = true
  try {
    const response = await fetch(`/api/v1/ui/${currentAccountId.value}/manual-order/submit`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ stock_code, stock_name, trade_type, price, quantity, order_type }),
    })
    const data = await response.json()
    if (data.success) {
      const msg = data.warning ? `${data.message || '委托成功'}\n${data.warning}` : (data.message || '委托成功')
      ElMessage.success(msg)
      orderForm.value = { stock_code: '', stock_name: '', trade_type: 'buy', price: 0, quantity: 100, order_type: 'day' }
      quoteInfo.value = null
      maxBuyQty.value = 0
      fundLimitQty.value = 0
      positionQty.value = 0
      availableQty.value = 0
      await loadTrades()
    } else {
      ElMessage.error(data.message || '委托失败')
    }
  } catch (e) {
    ElMessage.error('提交失败：' + e.message)
  } finally {
    submitting.value = false
  }
}

watch(() => orderForm.value.trade_type, () => {
  if (orderForm.value.trade_type === 'buy' && fundLimitQty.value > 0) {
    orderForm.value.quantity = fundLimitQty.value
  } else if (orderForm.value.trade_type === 'sell' && availableQty.value > 0) {
    orderForm.value.quantity = availableQty.value
  } else {
    orderForm.value.quantity = 100
  }
})

function startDrag(e) {
  document.body.style.cursor = 'col-resize'
  document.body.style.userSelect = 'none'
  const onMove = (e2) => {
    const container = splitContainer.value
    if (!container) return
    const rect = container.getBoundingClientRect()
    let newWidth = e2.clientX - rect.left
    newWidth = Math.max(260, Math.min(newWidth, rect.width - 400))
    leftPanelWidth.value = newWidth
  }
  const onUp = () => {
    document.body.style.cursor = ''
    document.body.style.userSelect = ''
    document.removeEventListener('mousemove', onMove)
    document.removeEventListener('mouseup', onUp)
  }
  document.addEventListener('mousemove', onMove)
  document.addEventListener('mouseup', onUp)
}

// ============================================================
// 交易明细
// ============================================================
const dateRange = ref([])
const trades = ref([])
const stats = reactive({
  totalCount: 0,
  buyCount: 0,
  sellCount: 0,
  winRate: 0
})

const tradeColumns = [
  { label: '时间', prop: 'trade_time' },
  { label: '代码', prop: 'stock_code' },
  { label: '名称', prop: 'stock_name' },
  { label: '操作', prop: 'trade_type' },
  { label: '数量', prop: 'quantity' },
  { label: '价格', prop: 'price' },
  { label: '金额', prop: 'amount' },
  { label: '手续费', prop: 'commission' },
  { label: '触发来源', prop: 'trigger_source' },
  { label: '状态', prop: 'status' },
]

const handleExportTrades = (format) => {
  doExport(tradeColumns, trades.value, '交易记录', format)
}

const loadTrades = async () => {
  try {
    let response
    if (dateRange.value && dateRange.value.length === 2) {
      // 有日期范围 → 调用历史交易接口
      const startDate = dateRange.value[0].toISOString().slice(0, 10)
      const endDate = dateRange.value[1].toISOString().slice(0, 10)
      response = await fetch(
        `/api/v1/ui/${currentAccountId.value}/trades?start_date=${startDate}&end_date=${endDate}&limit=200`
      )
    } else {
      // 无日期范围 → 默认今日
      response = await fetch(`/api/v1/ui/${currentAccountId.value}/trades/today`)
    }
    const data = await response.json()
    trades.value = data.trades || []
    stats.totalCount = data.stats?.total_count || 0
    stats.buyCount = data.stats?.buy_count || 0
    stats.sellCount = data.stats?.sell_count || 0
  } catch (error) {
    console.error('加载交易数据失败:', error)
  }
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

import { formatNumber, formatTime } from '../utils/format'

onMounted(async () => {
  await loadTrades()
  await loadNotifications()
})

// 切换账户时刷新数据
watch(currentAccountId, async () => {
  await loadTrades()
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

/* 分栏布局 */
.split-container {
  display: flex;
  gap: 0;
  min-height: 400px;
}

.order-panel {
  flex-shrink: 0;
  overflow-y: auto;
}

.order-card {
  border-radius: 8px;
}

.order-card :deep(.el-card__header) {
  padding: 12px 20px;
  background-color: #f5f7fa;
}

.card-header .order-title {
  font-weight: 600;
  font-size: 15px;
  color: #303133;
}

.order-card :deep(.el-card__body) {
  padding: 20px;
}

/* 行情信息 */
.quote-info {
  margin-bottom: 12px;
  padding: 8px 12px;
  background: #f5f7fa;
  border-radius: 6px;
  font-size: 13px;
}

.quote-info .label {
  color: #909399;
  font-size: 12px;
}

.quote-info .val {
  display: block;
  font-weight: 500;
  color: #303133;
}

.quote-info .val.buy { color: #f56c6c; }
.quote-info .val.sell { color: #67c23a; }

.trade-type-group { width: 100%; }
.trade-type-group :deep(.buy-btn .el-radio-button__inner) { color: #f56c6c; }
.trade-type-group :deep(.sell-btn .el-radio-button__inner) { color: #67c23a; }

.t1-hint {
  font-size: 12px;
  color: #909399;
  margin-top: 4px;
}

/* 拖拽分割线 */
.splitter {
  width: 6px;
  cursor: col-resize;
  background: #ebeef5;
  display: flex;
  align-items: center;
  justify-content: center;
  flex-shrink: 0;
  transition: background 0.2s;
}

.splitter:hover { background: #dcdfe6; }

.splitter-handle {
  width: 2px;
  height: 30px;
  border-left: 1px solid #c0c4cc;
  border-right: 1px solid #c0c4cc;
}

.right-panel {
  flex: 1;
  min-width: 400px;
  overflow-x: auto;
}
</style>
