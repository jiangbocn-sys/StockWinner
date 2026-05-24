<template>
  <div class="order-panel" :style="{ width: panelWidth + 'px' }">
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
</template>

<script setup>
import { ref, computed, watch } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'

const props = defineProps({
  accountId: {
    type: String,
    required: true,
  },
  searchLimit: {
    type: Number,
    default: 15,
  },
})

const emit = defineEmits(['order-submitted'])

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
    const resp = await fetch(`/api/v1/ui/stocks/search?q=${encodeURIComponent(query.trim())}&limit=${props.searchLimit}`)
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
    const resp = await fetch(`/api/v1/ui/${props.accountId}/manual-order/quote`, {
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
    const response = await fetch(`/api/v1/ui/${props.accountId}/manual-order/submit`, {
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
      emit('order-submitted')
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
</script>

<style scoped>
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
</style>
