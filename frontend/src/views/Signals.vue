<template>
  <div class="layout-container">
    <NavBar />
    <el-main class="main-content">
      <div class="page-header">
        <h2>交易信号 - {{ currentAccount?.display_name }}</h2>
        <el-space>
          <el-button @click="loadSignals">
            <el-icon><Refresh /></el-icon>
            刷新
          </el-button>
        </el-space>
      </div>

      <!-- 服务状态 -->
      <el-card class="status-card">
        <el-descriptions :column="3" border>
          <el-descriptions-item label="监控服务">
            <el-tag :type="monitoringRunning ? 'success' : 'info'" size="small">
              {{ monitoringRunning ? '运行中（自动管理）' : '未启动' }}
            </el-tag>
          </el-descriptions-item>
          <el-descriptions-item label="监控账户">
            {{ monitoringAccountIds.length > 0 ? monitoringAccountIds.join(', ') : '无' }}
          </el-descriptions-item>
          <el-descriptions-item label="待处理信号">
            {{ signals.filter(s => s.status === 'pending').length }}
          </el-descriptions-item>
        </el-descriptions>
      </el-card>

      <!-- 主内容区：手动下单 + 信号列表 -->
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
              <!-- 证券代码 -->
              <el-form-item label="证券代码">
                <el-autocomplete
                  v-model="orderForm.stock_code"
                  :fetch-suggestions="searchStocks"
                  placeholder="输入代码/名称/拼音，如 600000、NJJL、浦发"
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

              <!-- 股票名称 -->
              <el-form-item label="股票名称">
                <el-input
                  v-model="orderForm.stock_name"
                  placeholder="自动填充或手动输入"
                />
              </el-form-item>

              <!-- 行情信息 -->
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

              <!-- 买入/卖出切换 -->
              <el-form-item label="交易方向">
                <el-radio-group v-model="orderForm.trade_type" class="trade-type-group">
                  <el-radio-button value="buy" class="buy-btn">买入</el-radio-button>
                  <el-radio-button value="sell" class="sell-btn">卖出</el-radio-button>
                </el-radio-group>
              </el-form-item>

              <!-- 委托价格 -->
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

              <!-- 委托数量 -->
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

              <!-- 有效类型 -->
              <el-form-item label="委托类型">
                <el-radio-group v-model="orderForm.order_type" size="small">
                  <el-radio-button value="day">当日有效</el-radio-button>
                  <el-radio-button value="gtc">长期有效</el-radio-button>
                </el-radio-group>
              </el-form-item>

              <!-- 提交按钮 -->
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

        <!-- 右侧：信号列表 -->
        <div class="signals-panel">
          <!-- 信号类型筛选 -->
          <div class="filter-bar">
            <el-radio-group v-model="filterType" size="small" @change="loadSignals">
              <el-radio-button label="">全部</el-radio-button>
              <el-radio-button label="buy">买入</el-radio-button>
              <el-radio-button label="sell_stop_loss">止损卖出</el-radio-button>
              <el-radio-button label="sell_take_profit">止盈卖出</el-radio-button>
            </el-radio-group>
          </div>

          <el-table :data="signals" stripe style="width: 100%" v-loading="loading">
            <el-table-column prop="created_at" label="生成时间" width="170">
              <template #default="{ row }">{{ formatTime(row.created_at) }}</template>
            </el-table-column>
            <el-table-column prop="stock_code" label="股票代码" width="120" />
            <el-table-column prop="stock_name" label="股票名称" width="120" />
            <el-table-column prop="signal_type" label="信号类型" width="120">
              <template #default="{ row }">
                <el-tag :type="getSignalType(row.signal_type)" size="small">
                  {{ getSignalText(row.signal_type) }}
                </el-tag>
              </template>
            </el-table-column>
            <el-table-column prop="price" label="委托价" width="100" align="right">
              <template #default="{ row }">¥{{ row.price?.toFixed(2) }}</template>
            </el-table-column>
            <el-table-column prop="current_price" label="现价" width="100" align="right">
              <template #default="{ row }">
                <span v-if="row.current_price" :class="row.current_price > row.price ? 'price-up' : row.current_price < row.price ? 'price-down' : ''">
                  ¥{{ row.current_price.toFixed(2) }}
                </span>
                <span v-else class="text-muted">-</span>
              </template>
            </el-table-column>
            <el-table-column prop="target_quantity" label="数量" width="80" align="right" />
            <el-table-column prop="status" label="状态" width="90">
              <template #default="{ row }">
                <el-tag :type="getStatusType(row.status)" size="small">
                  {{ getStatusText(row.status) }}
                </el-tag>
              </template>
            </el-table-column>
            <el-table-column prop="executed_at" label="执行时间" width="170">
              <template #default="{ row }">{{ formatTime(row.executed_at) }}</template>
            </el-table-column>
            <el-table-column label="操作" fixed="right" width="180">
              <template #default="{ row }">
                <el-button
                  v-if="row.status === 'pending'"
                  type="success"
                  size="small"
                  @click="executeSignal(row)"
                >
                  执行
                </el-button>
                <el-button
                  v-if="row.status === 'pending'"
                  type="warning"
                  size="small"
                  @click="cancelSignal(row)"
                >
                  取消
                </el-button>
                <el-button
                  v-if="row.status !== 'pending'"
                  type="info"
                  size="small"
                  disabled
                >
                  已完成
                </el-button>
              </template>
            </el-table-column>
          </el-table>

          <el-empty v-if="!loading && signals.length === 0" description="暂无交易信号" />
        </div>
      </div>
    </el-main>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, watch } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { useAccountStore } from '../stores/account'
import NavBar from '../components/NavBar.vue'

const accountStore = useAccountStore()
const currentAccountId = computed(() => accountStore.currentAccountId)
const currentAccount = computed(() => accountStore.currentAccount)

const loading = ref(false)
const signals = ref([])
const filterType = ref('')
const monitoringRunning = ref(false)
const monitoringAccountIds = ref([])

// ============================================================
// 手动下单
// ============================================================

const orderForm = ref({
  stock_code: '',
  stock_name: '',
  trade_type: 'buy',
  price: 0,
  quantity: 100,
  order_type: 'day', // day=当日有效, gtc=长期有效
})

const quoteInfo = ref(null)
const submitting = ref(false)
const leftPanelWidth = ref(300)

// 买入时最大可买数量（策略仓位上限）
const maxBuyQty = ref(0)
// 资金允许的最大可买（不含策略限制，可能超限）
const fundLimitQty = ref(0)
// 卖出时持仓/可卖数量
const positionQty = ref(0)
const availableQty = ref(0)

// 股票代码规范化（与后端 normalize_stock_code 一致）
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

// 重置行情和名称信息
function resetQuoteInfo() {
  quoteInfo.value = null
  orderForm.value.stock_name = ''
  orderForm.value.price = 0
  maxBuyQty.value = 0
  fundLimitQty.value = 0
  positionQty.value = 0
  availableQty.value = 0
}

// 智能搜索股票（支持代码/名称/拼音首字母）
async function searchStocks(query, cb) {
  if (!query || query.trim().length === 0) {
    cb([])
    return
  }
  try {
    const resp = await fetch(`/api/v1/ui/stocks/search?q=${encodeURIComponent(query.trim())}&limit=15`)
    const data = await resp.json()
    if (data.success && data.stocks) {
      cb(data.stocks.map(s => ({
        stock_code: s.stock_code,
        stock_name: s.stock_name,
        value: s.stock_code, // el-autocomplete 显示值
      })))
    } else {
      cb([])
    }
  } catch {
    cb([])
  }
}

// 用户从搜索结果中选中一只股票
async function onStockSelect(item) {
  orderForm.value.stock_code = normalizeStockCode(item.stock_code)
  await lookupAndCalculate()
}

// 清空代码输入
function onCodeClear() {
  orderForm.value.stock_code = ''
  resetQuoteInfo()
}

function onCodeBlur() {
  const raw = orderForm.value.stock_code
  const normalized = normalizeStockCode(raw)
  orderForm.value.stock_code = normalized
  // 只有规范化后是合法代码格式（含 .）才重新查询
  if (normalized && normalized.includes('.')) {
    lookupAndCalculate()
  }
}

async function onCodeEnter() {
  const raw = orderForm.value.stock_code
  const normalized = normalizeStockCode(raw)
  orderForm.value.stock_code = normalized
  if (normalized && normalized.includes('.')) {
    await lookupAndCalculate()
  }
}

async function lookupAndCalculate() {
  const code = orderForm.value.stock_code
  if (!code || !code.includes('.')) return

  // 先清除之前的行情信息，防止旧数据残留
  resetQuoteInfo()

  try {
    const resp = await fetch(`/api/v1/ui/${currentAccountId.value}/manual-order/quote`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ stock_code: code }),
    })
    const data = await resp.json()
    if (data.success) {
      quoteInfo.value = {
        current_price: data.current_price,
        bid1: data.bid1,
        ask1: data.ask1,
      }
      // 直接覆盖股票名称
      if (data.stock_name) {
        orderForm.value.stock_name = data.stock_name
      }
      // 每次获取行情后自动用现价填充委托价格
      if (data.current_price && data.current_price > 0) {
        orderForm.value.price = data.current_price
      }
      // 买入参考：最大可买数量（策略上限）
      maxBuyQty.value = data.max_buy_quantity || 0
      // 资金允许的最大可买（不含策略限制）
      fundLimitQty.value = data.fund_limit_quantity || 0
      // 卖出参考：持仓/可卖
      positionQty.value = data.position_quantity || 0
      availableQty.value = data.available_quantity || 0
    } else {
      quoteInfo.value = null
      maxBuyQty.value = 0
      positionQty.value = 0
      availableQty.value = 0
      ElMessage.error(data.message || '券商服务器连接失败，请检查网络后重试')
    }
  } catch (e) {
    quoteInfo.value = null
    maxBuyQty.value = 0
    positionQty.value = 0
    availableQty.value = 0
    ElMessage.error('获取行情失败：' + e.message)
  }
}

// 使用资金允许的最大可买/可卖数量
function useMaxQuantity() {
  if (orderForm.value.trade_type === 'buy' && fundLimitQty.value > 0) {
    orderForm.value.quantity = fundLimitQty.value
  } else if (orderForm.value.trade_type === 'sell' && availableQty.value > 0) {
    orderForm.value.quantity = availableQty.value
  }
}

const canSubmit = computed(() => {
  return (
    orderForm.value.stock_code &&
    orderForm.value.price > 0 &&
    orderForm.value.quantity > 0 &&
    orderForm.value.quantity % 100 === 0 &&
    !submitting.value
  )
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

  // 买入时检查是否超出策略单只上限
  let confirmed = false
  if (trade_type === 'buy' && maxBuyQty.value > 0 && quantity > maxBuyQty.value) {
    try {
      await ElMessageBox.confirm(
        `当前数量 ${quantity} 股 超出策略单只持仓上限 ${maxBuyQty.value} 股。\n\n超出部分将在成交后由监控程序自动标记为超限，但系统不会阻止成交。是否继续？`,
        '超出持仓限制',
        { type: 'warning', confirmButtonText: '继续提交', cancelButtonText: '取消' }
      )
      confirmed = true
    } catch {
      return
    }
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
    } catch {
      return
    }
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
      // 如果是买入单且不在 watchlist 中，后端会自动创建「手动下单」分组并加入

      const msg = data.warning ? `${data.message || '委托成功'}\n${data.warning}` : (data.message || '委托成功')
      ElMessage.success(msg)
      // 重置表单
      orderForm.value = { stock_code: '', stock_name: '', trade_type: 'buy', price: 0, quantity: 100, order_type: 'day' }
      quoteInfo.value = null
      maxBuyQty.value = 0
      fundLimitQty.value = 0
      positionQty.value = 0
      availableQty.value = 0
      await loadSignals()
    } else {
      ElMessage.error(data.message || '委托失败')
    }
  } catch (e) {
    ElMessage.error('提交失败：' + e.message)
  } finally {
    submitting.value = false
  }
}

// 监听交易方向变化，重置数量
watch(() => orderForm.value.trade_type, () => {
  if (orderForm.value.trade_type === 'buy' && fundLimitQty.value > 0) {
    orderForm.value.quantity = fundLimitQty.value
  } else if (orderForm.value.trade_type === 'sell' && availableQty.value > 0) {
    orderForm.value.quantity = availableQty.value
  } else {
    orderForm.value.quantity = 100
  }
})

// ============================================================
// 拖拽分割线
// ============================================================

const splitContainer = ref(null)

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
// 信号列表（原有逻辑）
// ============================================================

const loadSignals = async () => {
  loading.value = true
  try {
    const url = filterType.value
      ? `/api/v1/ui/${currentAccountId.value}/signals?signal_type=${filterType.value}`
      : `/api/v1/ui/${currentAccountId.value}/signals`

    const response = await fetch(url)
    const data = await response.json()
    signals.value = data.signals || []
  } catch (error) {
    console.error('加载信号失败:', error)
    ElMessage.error('加载失败')
  } finally {
    loading.value = false
  }
}

const checkServiceStatus = async () => {
  try {
    const response = await fetch(`/api/v1/ui/${currentAccountId.value}/monitoring/status`)
    const data = await response.json()
    monitoringRunning.value = data.monitoring?.running || false
    monitoringAccountIds.value = data.monitoring?.account_ids || []
  } catch (error) {
    console.error('检查服务状态失败:', error)
  }
}

const executeSignal = async (row) => {
  try {
    // 确认是否以对手价立即成交
    const cpLabel = row.signal_type === 'buy' ? '卖一价' : '买一价'
    await ElMessageBox.confirm(
      `确认以${cpLabel}立即执行该信号？\n\n股票：${row.stock_name || row.stock_code}\n代码：${row.stock_code}\n委托价：¥${row.price?.toFixed(2)}\n数量：${row.target_quantity} 股`,
      `确认${row.signal_type === 'buy' ? '买入' : '卖出'}`,
      { type: 'warning' }
    )

    // TODO: 接入券商实盘后，此处先撤销原委托单，再以对手价重新提交委托
    // 当前 mock 模式直接以现价成交
    const response = await fetch(`/api/v1/ui/${currentAccountId.value}/signals/${row.id}/execute`, {
      method: 'POST'
    })

    if (response.ok) {
      ElMessage.success('信号已执行（模拟）')
      await loadSignals()
    }
  } catch (error) {
    if (error !== 'cancel') {
      console.error('执行信号失败:', error)
      ElMessage.error('执行失败')
    }
  }
}

const cancelSignal = async (row) => {
  try {
    const response = await fetch(`/api/v1/ui/${currentAccountId.value}/signals/${row.id}/cancel`, {
      method: 'POST'
    })

    if (response.ok) {
      ElMessage.success('信号已取消')
      await loadSignals()
    }
  } catch (error) {
    console.error('取消信号失败:', error)
    ElMessage.error('取消失败')
  }
}

const getSignalType = (type) => {
  const types = {
    'buy': 'danger',
    'sell_stop_loss': 'warning',
    'sell_take_profit': 'success'
  }
  return types[type] || 'info'
}

const getSignalText = (type) => {
  const texts = {
    'buy': '买入',
    'sell_stop_loss': '止损卖出',
    'sell_take_profit': '止盈卖出'
  }
  return texts[type] || type
}

const getStatusType = (status) => {
  const types = {
    'pending': 'warning',
    'executed': 'success',
    'cancelled': 'info'
  }
  return types[status] || 'info'
}

const getStatusText = (status) => {
  const texts = {
    'pending': '待处理',
    'executed': '已执行',
    'cancelled': '已取消'
  }
  return texts[status] || status
}

const formatTime = (t) => {
  if (!t) return '-'
  return t.split('.')[0].replace('T', ' ')
}

onMounted(async () => {
  await checkServiceStatus()
  await loadSignals()
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

.page-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 20px;
}

.page-header h2 {
  color: #303133;
  margin: 0;
}

.status-card {
  margin-bottom: 20px;
}

/* Split layout */
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

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.order-title {
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

.quote-info .val.buy {
  color: #f56c6c;
}

.quote-info .val.sell {
  color: #67c23a;
}

/* 买入/卖出按钮 */
.trade-type-group {
  width: 100%;
}

.trade-type-group :deep(.buy-btn .el-radio-button__inner) {
  color: #f56c6c;
}

.trade-type-group :deep(.sell-btn .el-radio-button__inner) {
  color: #67c23a;
}

/* 委托数量 T+1 提示 */
.t1-hint {
  font-size: 12px;
  color: #909399;
  margin-top: 4px;
}

.price-up {
  color: #f56c6c;
  font-weight: 500;
}
.price-down {
  color: #67c23a;
  font-weight: 500;
}
.text-muted {
  color: #c0c4cc;
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

.splitter:hover {
  background: #dcdfe6;
}

.splitter-handle {
  width: 2px;
  height: 30px;
  border-left: 1px solid #c0c4cc;
  border-right: 1px solid #c0c4cc;
}

/* 右侧信号列表 */
.signals-panel {
  flex: 1;
  min-width: 400px;
  overflow-x: auto;
}

.filter-bar {
  margin-bottom: 12px;
}
</style>
