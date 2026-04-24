<template>
  <div class="layout-container">
    <NavBar />
    <el-main class="main-content">
      <div class="page-header">
        <h2>策略管理 - {{ currentAccount?.display_name }}</h2>
        <el-button @click="loadAllData">
          <el-icon><Refresh /></el-icon>
          刷新
        </el-button>
      </div>

      <!-- 三种策略类型标签页 -->
      <el-tabs v-model="activeTab" type="border-card">
        <!-- 持仓策略 -->
        <el-tab-pane label="持仓策略" name="position">
          <el-card>
            <template #header>
              <span>仓位参数设置</span>
            </template>
            <el-form :model="positionStrategy" label-width="180px">
              <el-form-item label="总仓位上限">
                <el-slider
                  v-model="positionStrategy.max_total_position_pct"
                  :min="0.1"
                  :max="1.0"
                  :step="0.05"
                  :marks="{0.1: '10%', 0.5: '50%', 0.8: '80%', 1.0: '100%'}"
                  show-input
                />
                <div class="hint">股票资产占总资产的最大比例</div>
              </el-form-item>
              <el-form-item label="单股仓位上限">
                <el-slider
                  v-model="positionStrategy.max_single_position_pct"
                  :min="0.01"
                  :max="0.3"
                  :step="0.01"
                  :marks="{0.01: '1%', 0.05: '5%', 0.15: '15%', 0.3: '30%'}"
                  show-input
                />
                <div class="hint">单只股票占总资产的最大比例</div>
              </el-form-item>
              <el-form-item label="现金保留比例">
                <el-slider
                  v-model="positionStrategy.cash_reserve_pct"
                  :min="0.05"
                  :max="0.5"
                  :step="0.05"
                  :marks="{0.05: '5%', 0.2: '20%', 0.3: '30%', 0.5: '50%'}"
                  show-input
                />
                <div class="hint">必须保留的现金比例，用于应对突发情况</div>
              </el-form-item>
              <el-form-item>
                <el-button type="primary" @click="savePositionStrategy" :loading="savingPosition">
                  保存持仓策略
                </el-button>
              </el-form-item>
            </el-form>
          </el-card>

          <!-- 持仓调整规则 -->
          <el-card class="margin-top-20">
            <template #header>
              <div class="card-header">
                <span>动态调整规则</span>
                <el-button type="primary" size="small" @click="showRuleDialog = true">
                  <el-icon><Plus /></el-icon>
                  新建规则
                </el-button>
              </div>
            </template>
            <el-alert type="info" :closable="false" class="margin-bottom-15">
              根据市场条件自动调整仓位参数，系统会在条件触发时向用户发送调整建议
            </el-alert>
            <el-table :data="positionRules" stripe v-loading="loadingRules">
              <el-table-column prop="trigger_condition" label="触发条件" width="200" />
              <el-table-column prop="trigger_description" label="说明" min-width="150" />
              <el-table-column prop="target_max_total_pct" label="总仓位上限" width="100">
                <template #default="{ row }">
                  {{ (row.target_max_total_pct * 100).toFixed(0) }}%
                </template>
              </el-table-column>
              <el-table-column prop="target_max_single_pct" label="单股上限" width="100">
                <template #default="{ row }">
                  {{ row.target_max_single_pct > 0 ? (row.target_max_single_pct * 100).toFixed(0) + '%' : '-' }}
                </template>
              </el-table-column>
              <el-table-column prop="priority" label="优先级" width="80" />
              <el-table-column prop="is_active" label="状态" width="80">
                <template #default="{ row }">
                  <el-tag :type="row.is_active ? 'success' : 'info'" size="small">
                    {{ row.is_active ? '启用' : '停用' }}
                  </el-tag>
                </template>
              </el-table-column>
              <el-table-column label="操作" width="150">
                <template #default="{ row }">
                  <el-button type="warning" size="small" @click="toggleRule(row)">
                    {{ row.is_active ? '停用' : '启用' }}
                  </el-button>
                  <el-button type="danger" size="small" @click="deleteRule(row)">
                    删除
                  </el-button>
                </template>
              </el-table-column>
            </el-table>
            <el-empty v-if="!loadingRules && positionRules.length === 0" description="暂无调整规则" />
          </el-card>
        </el-tab-pane>

        <!-- 选股策略 -->
        <el-tab-pane label="选股策略" name="screening">
          <el-card>
            <template #header>
              <div class="card-header">
                <span>选股策略列表</span>
                <el-space>
                  <el-button type="primary" size="small" @click="showCreateScreeningDialog = true">
                    <el-icon><Plus /></el-icon>
                    新建策略
                  </el-button>
                  <el-button type="success" size="small" @click="showLLMDialog = true" :disabled="generating">
                    <el-icon><MagicStick /></el-icon>
                    LLM生成
                  </el-button>
                </el-space>
              </div>
            </template>

            <el-table :data="screeningStrategies" stripe v-loading="loadingScreening">
              <el-table-column prop="name" label="策略名称" width="180" />
              <el-table-column prop="description" label="描述" min-width="200" show-overflow-tooltip />
              <el-table-column prop="status" label="状态" width="80">
                <template #default="{ row }">
                  <el-tag :type="getStatusType(row.status)" size="small">
                    {{ getStatusText(row.status) }}
                  </el-tag>
                </template>
              </el-table-column>
              <el-table-column prop="created_at" label="创建时间" width="160" />
              <el-table-column label="操作" width="200">
                <template #default="{ row }">
                  <el-button type="primary" size="small" @click="viewScreeningStrategy(row)">
                    详情
                  </el-button>
                  <el-button type="success" size="small" @click="runScreening(row)">
                    执行筛选
                  </el-button>
                  <el-button type="danger" size="small" @click="deleteScreeningStrategy(row)">
                    删除
                  </el-button>
                </template>
              </el-table-column>
            </el-table>
            <el-empty v-if="!loadingScreening && screeningStrategies.length === 0" description="暂无选股策略" />
          </el-card>
        </el-tab-pane>

        <!-- 交易策略 -->
        <el-tab-pane label="交易策略" name="trading">
          <el-card>
            <template #header>
              <div class="card-header">
                <span>股票交易策略</span>
                <el-space>
                  <el-input
                    v-model="searchStockCode"
                    placeholder="搜索股票代码"
                    size="small"
                    style="width: 150px;"
                    clearable
                  />
                  <el-button type="primary" size="small" @click="showTradingDialog = true">
                    <el-icon><Plus /></el-icon>
                    新建策略
                  </el-button>
                </el-space>
              </div>
            </template>

            <el-alert type="info" :closable="false" class="margin-bottom-15">
              为持仓股或候选股设置具体的交易参数：建仓价、止损止盈、单次买卖数量
            </el-alert>

            <el-table :data="filteredTradingStrategies" stripe v-loading="loadingTrading">
              <el-table-column prop="stock_code" label="股票代码" width="120" />
              <el-table-column prop="stock_name" label="股票名称" width="120" />
              <el-table-column prop="entry_price" label="建仓价" width="100">
                <template #default="{ row }">
                  {{ row.entry_price > 0 ? row.entry_price.toFixed(2) : '-' }}
                </template>
              </el-table-column>
              <el-table-column prop="stop_loss_price" label="止损价" width="100">
                <template #default="{ row }">
                  {{ row.stop_loss_price > 0 ? row.stop_loss_price.toFixed(2) : '-' }}
                </template>
              </el-table-column>
              <el-table-column prop="stop_loss_pct" label="止损比例" width="100">
                <template #default="{ row }">
                  {{ row.stop_loss_pct > 0 ? (row.stop_loss_pct * 100).toFixed(1) + '%' : '-' }}
                </template>
              </el-table-column>
              <el-table-column prop="take_profit_price" label="止盈价" width="100">
                <template #default="{ row }">
                  {{ row.take_profit_price > 0 ? row.take_profit_price.toFixed(2) : '-' }}
                </template>
              </el-table-column>
              <el-table-column prop="take_profit_pct" label="止盈比例" width="100">
                <template #default="{ row }">
                  {{ row.take_profit_pct > 0 ? (row.take_profit_pct * 100).toFixed(1) + '%' : '-' }}
                </template>
              </el-table-column>
              <el-table-column prop="max_trade_quantity" label="单次买卖数量" width="120">
                <template #default="{ row }">
                  {{ row.max_trade_quantity > 0 ? row.max_trade_quantity + '股' : '-' }}
                </template>
              </el-table-column>
              <el-table-column label="操作" width="150">
                <template #default="{ row }">
                  <el-button type="primary" size="small" @click="editTradingStrategy(row)">
                    编辑
                  </el-button>
                  <el-button type="danger" size="small" @click="deleteTradingStrategy(row)">
                    删除
                  </el-button>
                </template>
              </el-table-column>
            </el-table>
            <el-empty v-if="!loadingTrading && tradingStrategies.length === 0" description="暂无交易策略" />
          </el-card>
        </el-tab-pane>
      </el-tabs>

      <!-- 持仓调整规则对话框 -->
      <el-dialog v-model="showRuleDialog" title="新建调整规则" width="550px">
        <el-form :model="newRule" label-width="120px">
          <el-form-item label="触发条件描述" required>
            <el-input
              v-model="newRule.natural_description"
              type="textarea"
              :rows="2"
              placeholder="用自然语言描述，如：'上证指数RSI跌破30时降低仓位'"
              :disabled="translating"
            />
            <div class="hint">可用：上证/深证/创业板指数，MACD金叉/死叉，RSI超买/超卖，成交量放大等</div>
          </el-form-item>
          <el-form-item>
            <el-button type="warning" @click="translateCondition" :loading="translating" :disabled="!newRule.natural_description">
              <el-icon v-if="!translating"><MagicStick /></el-icon>
              {{ translating ? '翻译中...' : 'LLM翻译' }}
            </el-button>
          </el-form-item>
          <el-form-item label="翻译结果" v-if="newRule.trigger_expression">
            <el-alert type="success" :closable="false">
              <div><strong>表达式：</strong>{{ newRule.trigger_expression }}</div>
              <div><strong>说明：</strong>{{ newRule.trigger_description }}</div>
            </el-alert>
          </el-form-item>
          <el-form-item label="目标总仓位(占总资产)" v-if="newRule.trigger_expression">
            <el-slider
              v-model="newRule.target_max_total_pct"
              :min="0.1"
              :max="1.0"
              :step="0.05"
              :marks="{0.1: '10%', 0.5: '50%', 0.8: '80%', 1.0: '100%'}"
              show-input
            />
          </el-form-item>
          <el-form-item label="单股上限(占总资产)" v-if="newRule.trigger_expression">
            <el-slider
              v-model="newRule.target_max_single_pct"
              :min="0"
              :max="0.3"
              :step="0.01"
              show-input
            />
            <div class="hint">0 表示不限制单股仓位；总资产 = 现金 + 持仓市值</div>
          </el-form-item>
          <el-form-item label="优先级" v-if="newRule.trigger_expression">
            <el-input-number v-model="newRule.priority" :min="0" :max="100" />
          </el-form-item>
        </el-form>
        <template #footer>
          <el-button @click="showRuleDialog = false">取消</el-button>
          <el-button type="primary" @click="createRule" :loading="creatingRule" :disabled="!newRule.trigger_expression">
            创建规则
          </el-button>
        </template>
      </el-dialog>

      <!-- 新建交易策略对话框 -->
      <el-dialog v-model="showTradingDialog" title="新建交易策略" width="500px">
        <el-form :model="newTrading" label-width="120px">
          <el-form-item label="股票代码" required>
            <el-input v-model="newTrading.stock_code" placeholder="如：600000.SH" />
          </el-form-item>
          <el-form-item label="建仓价">
            <el-input-number v-model="newTrading.entry_price" :min="0" :precision="2" />
          </el-form-item>
          <el-form-item label="止损价（固定）">
            <el-input-number v-model="newTrading.stop_loss_price" :min="0" :precision="2" />
          </el-form-item>
          <el-form-item label="止损比例">
            <el-slider
              v-model="newTrading.stop_loss_pct"
              :min="0"
              :max="0.3"
              :step="0.01"
              show-input
            />
            <div class="hint">固定止损价优先，为空时用比例计算</div>
          </el-form-item>
          <el-form-item label="止盈价（固定）">
            <el-input-number v-model="newTrading.take_profit_price" :min="0" :precision="2" />
          </el-form-item>
          <el-form-item label="止盈比例">
            <el-slider
              v-model="newTrading.take_profit_pct"
              :min="0"
              :max="0.5"
              :step="0.01"
              show-input
            />
          </el-form-item>
          <el-form-item label="单次买卖数量">
            <el-input-number v-model="newTrading.max_trade_quantity" :min="0" :step="100" />
            <div class="hint">一次交易允许买卖的最大股数</div>
          </el-form-item>
        </el-form>
        <template #footer>
          <el-button @click="showTradingDialog = false">取消</el-button>
          <el-button type="primary" @click="saveTradingStrategy" :loading="savingTrading">保存</el-button>
        </template>
      </el-dialog>

      <!-- 编辑交易策略对话框 -->
      <el-dialog v-model="showEditTradingDialog" title="编辑交易策略" width="500px">
        <el-form :model="editingTrading" label-width="120px">
          <el-form-item label="股票代码">
            <el-input :value="editingTrading.stock_code" disabled />
          </el-form-item>
          <el-form-item label="建仓价">
            <el-input-number v-model="editingTrading.entry_price" :min="0" :precision="2" />
          </el-form-item>
          <el-form-item label="止损价">
            <el-input-number v-model="editingTrading.stop_loss_price" :min="0" :precision="2" />
          </el-form-item>
          <el-form-item label="止损比例">
            <el-slider
              v-model="editingTrading.stop_loss_pct"
              :min="0"
              :max="0.3"
              :step="0.01"
              show-input
            />
          </el-form-item>
          <el-form-item label="止盈价">
            <el-input-number v-model="editingTrading.take_profit_price" :min="0" :precision="2" />
          </el-form-item>
          <el-form-item label="止盈比例">
            <el-slider
              v-model="editingTrading.take_profit_pct"
              :min="0"
              :max="0.5"
              :step="0.01"
              show-input
            />
          </el-form-item>
          <el-form-item label="单次买卖数量">
            <el-input-number v-model="editingTrading.max_trade_quantity" :min="0" :step="100" />
          </el-form-item>
        </el-form>
        <template #footer>
          <el-button @click="showEditTradingDialog = false">取消</el-button>
          <el-button type="primary" @click="updateTradingStrategy" :loading="savingTrading">保存</el-button>
        </template>
      </el-dialog>

      <!-- LLM生成选股策略对话框 -->
      <el-dialog v-model="showLLMDialog" title="LLM 生成选股策略" width="550px">
        <el-form :model="llmStrategy" label-width="120px">
          <el-form-item label="策略描述" required>
            <el-input
              v-model="llmStrategy.description"
              type="textarea"
              :rows="4"
              placeholder="描述你想要的策略，例如：'总市值小于50亿，MACD金叉，成交量放大'"
              :disabled="generating"
            />
          </el-form-item>
        </el-form>
        <template #footer>
          <el-button @click="showLLMDialog = false" :disabled="generating">取消</el-button>
          <el-button type="warning" @click="generateLLMStrategy" :loading="generating">
            <el-icon v-if="!generating"><MagicStick /></el-icon>
            {{ generating ? '正在生成...' : '生成策略' }}
          </el-button>
        </template>
      </el-dialog>

      <!-- 选股策略详情对话框 -->
      <el-dialog v-model="showScreeningDetailDialog" :title="selectedScreening?.name" width="650px">
        <el-descriptions :column="2" border v-if="selectedScreening">
          <el-descriptions-item label="策略名称">{{ selectedScreening.name }}</el-descriptions-item>
          <el-descriptions-item label="状态">
            <el-tag :type="getStatusType(selectedScreening.status)" size="small">
              {{ getStatusText(selectedScreening.status) }}
            </el-tag>
          </el-descriptions-item>
          <el-descriptions-item label="描述" :span="2">{{ selectedScreening.description }}</el-descriptions-item>
          <el-descriptions-item label="筛选条件" :span="2">
            <el-tag v-for="(cond, idx) in parseConfig(selectedScreening.config)?.stock_filters || {}" :key="idx" size="small" style="margin: 2px;">
              {{ formatFilter(cond) }}
            </el-tag>
          </el-descriptions-item>
          <el-descriptions-item label="买入条件" :span="2">
            <el-tag v-for="(cond, idx) in parseConfig(selectedScreening.config)?.buy_conditions || []" :key="idx" size="small" style="margin: 2px;">
              {{ cond }}
            </el-tag>
            <span v-if="!(parseConfig(selectedScreening.config)?.buy_conditions || []).length" style="color: #999;">无</span>
          </el-descriptions-item>
          <el-descriptions-item label="创建时间">{{ selectedScreening.created_at }}</el-descriptions-item>
          <el-descriptions-item label="更新时间">{{ selectedScreening.updated_at }}</el-descriptions-item>
        </el-descriptions>
      </el-dialog>
    </el-main>
  </div>
</template>

<script setup>
import { ref, reactive, computed, onMounted } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { Plus, Refresh, MagicStick } from '@element-plus/icons-vue'
import { useAccountStore } from '../stores/account'
import NavBar from '../components/NavBar.vue'

const accountStore = useAccountStore()
const currentAccountId = computed(() => accountStore.currentAccountId)
const currentAccount = computed(() => accountStore.currentAccount)

const activeTab = ref('position')

// 持仓策略
const positionStrategy = reactive({
  max_total_position_pct: 0.80,
  max_single_position_pct: 0.15,
  cash_reserve_pct: 0.20
})
const savingPosition = ref(false)

// 持仓调整规则
const positionRules = ref([])
const loadingRules = ref(false)
const showRuleDialog = ref(false)
const creatingRule = ref(false)
const translating = ref(false)  // LLM翻译中状态
const newRule = reactive({
  natural_description: '',       // 用户自然语言描述
  trigger_expression: '',         // LLM翻译后的表达式
  trigger_description: '',        // LLM翻译后的描述
  target_max_total_pct: 0.5,
  target_max_single_pct: 0,
  priority: 0
})

// 选股策略
const screeningStrategies = ref([])
const loadingScreening = ref(false)
const showCreateScreeningDialog = ref(false)
const showScreeningDetailDialog = ref(false)
const selectedScreening = ref(null)

// LLM生成
const showLLMDialog = ref(false)
const generating = ref(false)
const llmStrategy = reactive({
  description: ''
})

// 交易策略
const tradingStrategies = ref([])
const loadingTrading = ref(false)
const searchStockCode = ref('')
const showTradingDialog = ref(false)
const showEditTradingDialog = ref(false)
const savingTrading = ref(false)
const newTrading = reactive({
  stock_code: '',
  entry_price: 0,
  stop_loss_price: 0,
  stop_loss_pct: 0.05,
  take_profit_price: 0,
  take_profit_pct: 0.15,
  max_trade_quantity: 0
})
const editingTrading = reactive({
  stock_code: '',
  entry_price: 0,
  stop_loss_price: 0,
  stop_loss_pct: 0,
  take_profit_price: 0,
  take_profit_pct: 0,
  max_trade_quantity: 0
})

const filteredTradingStrategies = computed(() => {
  if (!searchStockCode.value) return tradingStrategies.value
  return tradingStrategies.value.filter(s =>
    s.stock_code.toLowerCase().includes(searchStockCode.value.toLowerCase())
  )
})

// 加载所有数据
const loadAllData = async () => {
  await loadPositionStrategy()
  await loadPositionRules()
  await loadScreeningStrategies()
  await loadTradingStrategies()
}

// 加载持仓策略
const loadPositionStrategy = async () => {
  try {
    const res = await fetch(`/api/v1/ui/accounts/${currentAccountId.value}/position-strategy`)
    const data = await res.json()
    if (data.success) {
      positionStrategy.max_total_position_pct = data.data.max_total_position_pct
      positionStrategy.max_single_position_pct = data.data.max_single_position_pct
      positionStrategy.cash_reserve_pct = data.data.cash_reserve_pct
    }
  } catch (error) {
    console.error('加载持仓策略失败:', error)
  }
}

// 保存持仓策略
const savePositionStrategy = async () => {
  savingPosition.value = true
  try {
    const res = await fetch(`/api/v1/ui/accounts/${currentAccountId.value}/position-strategy`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        max_total_position_pct: positionStrategy.max_total_position_pct,
        max_single_position_pct: positionStrategy.max_single_position_pct,
        cash_reserve_pct: positionStrategy.cash_reserve_pct
      })
    })
    const data = await res.json()
    if (data.success) {
      ElMessage.success('持仓策略保存成功')
    } else {
      ElMessage.error(data.message || '保存失败')
    }
  } catch (error) {
    ElMessage.error('保存失败：' + error.message)
  } finally {
    savingPosition.value = false
  }
}

// 加载持仓调整规则
const loadPositionRules = async () => {
  loadingRules.value = true
  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/position-rules`)
    const data = await res.json()
    if (data.success) {
      positionRules.value = data.rules
    }
  } catch (error) {
    console.error('加载调整规则失败:', error)
  } finally {
    loadingRules.value = false
  }
}

// LLM翻译触发条件
const translateCondition = async () => {
  if (!newRule.natural_description) {
    ElMessage.warning('请输入触发条件描述')
    return
  }

  translating.value = true
  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/position-rules/translate`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ description: newRule.natural_description })
    })
    const data = await res.json()
    if (data.success) {
      newRule.trigger_expression = data.translated.expression
      newRule.trigger_description = data.translated.description
      ElMessage.success('翻译成功')
    } else {
      // 显示可用参数提示
      if (data.available_params_hint) {
        ElMessageBox.alert(data.available_params_hint, '可用参数参考', {
          confirmButtonText: '知道了',
          customClass: 'params-hint-dialog'
        })
      } else {
        ElMessage.error(data.error || '翻译失败')
      }
    }
  } catch (error) {
    ElMessage.error('翻译失败：' + error.message)
  } finally {
    translating.value = false
  }
}

// 创建规则
const createRule = async () => {
  if (!newRule.trigger_expression) {
    ElMessage.warning('请先翻译触发条件')
    return
  }

  creatingRule.value = true
  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/position-rules`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        trigger_expression: newRule.trigger_expression,
        trigger_description: newRule.trigger_description,
        target_max_total_pct: newRule.target_max_total_pct,
        target_max_single_pct: newRule.target_max_single_pct,
        priority: newRule.priority
      })
    })
    const data = await res.json()
    if (data.success) {
      ElMessage.success('规则创建成功')
      showRuleDialog.value = false
      // 重置表单
      newRule.natural_description = ''
      newRule.trigger_expression = ''
      newRule.trigger_description = ''
      newRule.target_max_total_pct = 0.5
      newRule.target_max_single_pct = 0
      newRule.priority = 0
      await loadPositionRules()
    } else {
      ElMessage.error(data.detail || '创建失败')
    }
  } catch (error) {
    ElMessage.error('创建失败：' + error.message)
  } finally {
    creatingRule.value = false
  }
}

// 启用/停用规则
const toggleRule = async (rule) => {
  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/position-rules/${rule.id}/toggle`, {
      method: 'POST'
    })
    const data = await res.json()
    if (data.success) {
      ElMessage.success(data.message)
      await loadPositionRules()
    }
  } catch (error) {
    ElMessage.error('操作失败')
  }
}

// 删除规则
const deleteRule = async (rule) => {
  try {
    await ElMessageBox.confirm('确定删除该规则？', '确认删除', { type: 'warning' })
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/position-rules/${rule.id}`, {
      method: 'DELETE'
    })
    const data = await res.json()
    if (data.success) {
      ElMessage.success('规则已删除')
      await loadPositionRules()
    }
  } catch (error) {
    if (error !== 'cancel') {
      ElMessage.error('删除失败')
    }
  }
}

// 加载选股策略
const loadScreeningStrategies = async () => {
  loadingScreening.value = true
  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/strategies`)
    const data = await res.json()
    if (data.strategies) {
      screeningStrategies.value = data.strategies.filter(s => s.strategy_type === 'screening')
    }
  } catch (error) {
    console.error('加载选股策略失败:', error)
  } finally {
    loadingScreening.value = false
  }
}

// 查看选股策略详情
const viewScreeningStrategy = (row) => {
  selectedScreening.value = row
  showScreeningDetailDialog.value = true
}

// 执行筛选
const runScreening = async (row) => {
  ElMessage.info('筛选功能开发中')
}

// 删除选股策略
const deleteScreeningStrategy = async (row) => {
  try {
    await ElMessageBox.confirm('确定删除该策略？', '确认删除', { type: 'warning' })
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/strategies/${row.id}`, {
      method: 'DELETE'
    })
    const data = await res.json()
    if (data.success) {
      ElMessage.success('策略已删除')
      await loadScreeningStrategies()
    }
  } catch (error) {
    if (error !== 'cancel') {
      ElMessage.error('删除失败')
    }
  }
}

// LLM生成策略
const generateLLMStrategy = async () => {
  if (!llmStrategy.description) {
    ElMessage.warning('请输入策略描述')
    return
  }

  generating.value = true
  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/strategies/generate`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ description: llmStrategy.description })
    })
    const data = await res.json()
    if (data.success) {
      // 创建策略
      const createRes = await fetch(`/api/v1/ui/${currentAccountId.value}/strategies`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name: data.strategy.name,
          description: llmStrategy.description,
          strategy_type: 'screening',
          config: data.strategy.config
        })
      })
      const createData = await createRes.json()
      if (createData.success) {
        ElMessage.success('策略生成并创建成功')
        showLLMDialog.value = false
        llmStrategy.description = ''
        await loadScreeningStrategies()
      } else {
        ElMessage.error('创建失败：' + createData.message)
      }
    } else {
      ElMessage.error(data.message || '生成失败')
    }
  } catch (error) {
    ElMessage.error('生成失败：' + error.message)
  } finally {
    generating.value = false
  }
}

// 加载交易策略
const loadTradingStrategies = async () => {
  loadingTrading.value = true
  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/trading-strategies`)
    const data = await res.json()
    if (data.success) {
      tradingStrategies.value = data.strategies
    }
  } catch (error) {
    console.error('加载交易策略失败:', error)
  } finally {
    loadingTrading.value = false
  }
}

// 保存交易策略
const saveTradingStrategy = async () => {
  if (!newTrading.stock_code) {
    ElMessage.warning('请输入股票代码')
    return
  }

  savingTrading.value = true
  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/trading-strategies`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(newTrading)
    })
    const data = await res.json()
    if (data.success) {
      ElMessage.success(data.message)
      showTradingDialog.value = false
      // 重置表单
      newTrading.stock_code = ''
      newTrading.entry_price = 0
      newTrading.stop_loss_price = 0
      newTrading.stop_loss_pct = 0.05
      newTrading.take_profit_price = 0
      newTrading.take_profit_pct = 0.15
      newTrading.max_trade_quantity = 0
      await loadTradingStrategies()
    } else {
      ElMessage.error(data.message || '保存失败')
    }
  } catch (error) {
    ElMessage.error('保存失败：' + error.message)
  } finally {
    savingTrading.value = false
  }
}

// 编辑交易策略
const editTradingStrategy = (row) => {
  editingTrading.stock_code = row.stock_code
  editingTrading.entry_price = row.entry_price
  editingTrading.stop_loss_price = row.stop_loss_price
  editingTrading.stop_loss_pct = row.stop_loss_pct
  editingTrading.take_profit_price = row.take_profit_price
  editingTrading.take_profit_pct = row.take_profit_pct
  editingTrading.max_trade_quantity = row.max_trade_quantity
  showEditTradingDialog.value = true
}

// 更新交易策略
const updateTradingStrategy = async () => {
  savingTrading.value = true
  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/trading-strategies/${editingTrading.stock_code}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(editingTrading)
    })
    const data = await res.json()
    if (data.success) {
      ElMessage.success('策略更新成功')
      showEditTradingDialog.value = false
      await loadTradingStrategies()
    } else {
      ElMessage.error(data.message || '更新失败')
    }
  } catch (error) {
    ElMessage.error('更新失败：' + error.message)
  } finally {
    savingTrading.value = false
  }
}

// 删除交易策略
const deleteTradingStrategy = async (row) => {
  try {
    await ElMessageBox.confirm('确定删除该交易策略？', '确认删除', { type: 'warning' })
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/trading-strategies/${row.stock_code}`, {
      method: 'DELETE'
    })
    const data = await res.json()
    if (data.success) {
      ElMessage.success('策略已删除')
      await loadTradingStrategies()
    }
  } catch (error) {
    if (error !== 'cancel') {
      ElMessage.error('删除失败')
    }
  }
}

// 辅助函数
const getStatusType = (status) => {
  const types = { 'draft': 'info', 'active': 'success', 'inactive': 'warning' }
  return types[status] || 'info'
}

const getStatusText = (status) => {
  const texts = { 'draft': '草稿', 'active': '激活', 'inactive': '停用' }
  return texts[status] || status
}

const parseConfig = (config) => {
  if (!config) return {}
  if (typeof config === 'string') {
    try { return JSON.parse(config) } catch { return {} }
  }
  return config
}

const formatFilter = (value) => {
  if (typeof value === 'object') {
    return JSON.stringify(value)
  }
  return value
}

onMounted(() => {
  loadAllData()
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
  background-color: #f5f7fa;
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

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.el-tabs--border-card {
  background: #fff;
}

.hint {
  color: #909399;
  font-size: 12px;
  margin-top: 5px;
}

.margin-top-20 {
  margin-top: 20px;
}

.margin-bottom-15 {
  margin-bottom: 15px;
}
</style>