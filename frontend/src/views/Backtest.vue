<template>
  <div class="backtest-container">
    <NavBar />
    <div class="backtest-content">
      <h2>策略回测</h2>

      <!-- 回测配置 -->
      <el-card class="config-card">
        <template #header>
          <div class="card-header">
            <span>回测配置</span>
          </div>
        </template>

        <el-form :model="form" label-width="120px" label-position="right">
          <el-row :gutter="20">
            <el-col :span="8">
              <el-form-item label="回测名称">
                <el-input v-model="form.name" placeholder="请输入回测名称" />
              </el-form-item>
            </el-col>
            <el-col :span="8">
              <el-form-item label="回测模式">
                <el-tooltip content="撮合模拟盘：逐日推进，模拟真实交易（考虑仓位、现金、T+1）。收益率累积：快速信号配对，不考虑资金限制" placement="top">
                  <el-select v-model="form.mode" style="width: 100%">
                    <el-option label="撮合模拟盘" value="simulated" />
                    <el-option label="收益率累积" value="return_accumulation" />
                  </el-select>
                </el-tooltip>
              </el-form-item>
            </el-col>
            <el-col :span="8">
              <el-form-item label="选择策略">
                <el-tooltip content="可选。选择后使用该策略的买入条件和止盈止损参数；不选则使用下方手动配置" placement="top">
                  <el-select v-model="form.strategy_id" placeholder="可选" clearable style="width: 100%">
                    <el-option v-for="s in strategies" :key="s.id" :label="s.name" :value="s.id" />
                  </el-select>
                </el-tooltip>
              </el-form-item>
            </el-col>
          </el-row>

          <el-row :gutter="20">
            <el-col :span="8">
              <el-form-item label="起始日期">
                <el-date-picker v-model="form.start_date" type="date" value-format="YYYY-MM-DD" style="width: 100%" />
              </el-form-item>
            </el-col>
            <el-col :span="8">
              <el-form-item label="结束日期">
                <el-date-picker v-model="form.end_date" type="date" value-format="YYYY-MM-DD" style="width: 100%" />
              </el-form-item>
            </el-col>
            <el-col :span="8">
              <el-form-item label="初始资金">
                <el-input-number v-model="form.initial_capital" :min="10000" :step="100000" style="width: 100%" />
              </el-form-item>
            </el-col>
          </el-row>

          <el-row :gutter="20">
            <el-col :span="8">
              <el-form-item label="市场选择">
                <el-select v-model="form.markets" multiple placeholder="全市场" style="width: 100%" clearable>
                  <el-option label="上海 (SH)" value="SH" />
                  <el-option label="深圳 (SZ)" value="SZ" />
                </el-select>
              </el-form-item>
            </el-col>
            <el-col :span="8">
              <el-form-item label="股票池">
                <el-tooltip content="选择候选组作为回测股票池，优先级高于市场选择" placement="top">
                  <el-select v-model="form.group_ids" multiple placeholder="使用市场选择" style="width: 100%" clearable filterable>
                    <el-option v-for="g in candidateGroups" :key="g.id" :label="`${g.name} (${g.stock_count}只)`" :value="g.id" />
                  </el-select>
                </el-tooltip>
              </el-form-item>
            </el-col>
            <el-col :span="8">
              <el-form-item label="止损比例 (%)">
                <el-tooltip content="买入价下跌超过该比例时触发止损。例如填 5 表示亏损 5% 时卖出" placement="top">
                  <el-input-number v-model="form.stop_loss_pct" :min="0" :max="50" :step="1" :precision="1" style="width: 100%" />
                </el-tooltip>
              </el-form-item>
            </el-col>
            <el-col :span="8">
              <el-form-item label="止盈比例 (%)">
                <el-tooltip content="买入价上涨超过该比例时触发止盈。例如填 15 表示盈利 15% 时卖出" placement="top">
                  <el-input-number v-model="form.take_profit_pct" :min="0" :max="100" :step="1" :precision="1" style="width: 100%" />
                </el-tooltip>
              </el-form-item>
            </el-col>
            <el-col :span="8">
              <el-form-item label="移动止盈 (%)">
                <el-tooltip content="持仓期间，从最高点回撤超过该比例时触发卖出。例如填 3 表示从最高点回撤 3% 时止盈" placement="top">
                  <el-input-number v-model="form.trailing_stop_pct" :min="0" :max="50" :step="0.5" :precision="1" style="width: 100%" placeholder="可选" />
                </el-tooltip>
              </el-form-item>
            </el-col>
          </el-row>

          <el-row :gutter="20">
            <el-col :span="8">
              <el-form-item label="止盈止损成交价">
                <el-tooltip content="收盘价模式：触发后按当日收盘价成交；触发价模式：当日价格覆盖止盈/止损位即按触发价成交（更贴近实盘）" placement="top">
                  <el-select v-model="form.stop_execution_price" style="width: 100%">
                    <el-option label="收盘价" value="close" />
                    <el-option label="触发价" value="trigger" />
                  </el-select>
                </el-tooltip>
              </el-form-item>
            </el-col>
            <el-col :span="8">
              <el-form-item label="手续费率">
                <el-tooltip content="券商佣金费率，按成交金额计算。例如万分之0.86填 0.000086，万分之三填 0.0003。最低佣金 5 元（不足按 5 元收取）" placement="top">
                  <el-input-number v-model="form.commission_rate" :min="0" :max="0.01" :step="0.000001" :precision="6" style="width: 100%" />
                </el-tooltip>
              </el-form-item>
            </el-col>
            <el-col :span="8">
              <el-form-item label="滑点 (%)">
                <el-tooltip content="模拟成交价格的偏移，买入时成交价上浮、卖出时成交价下浮。例如填 0.1 表示买入按报价×1.001、卖出按报价×0.999 成交" placement="top">
                  <el-input-number v-model="form.slippage_pct" :min="0" :max="1" :step="0.01" :precision="2" style="width: 100%" />
                </el-tooltip>
              </el-form-item>
            </el-col>
          </el-row>

          <el-form-item label="回测说明">
            <el-input v-model="form.description" type="textarea" :rows="2" placeholder="简要记录回测目的、注意事项等（可选）" style="width: 100%" />
          </el-form-item>

          <el-form-item>
            <el-button type="primary" @click="handleStartBacktest" :loading="running">
              开始回测
            </el-button>
            <el-button @click="handleCheckData">检查数据完整性</el-button>
          </el-form-item>
        </el-form>
      </el-card>

      <!-- 回测历史 -->
      <el-card class="history-card">
        <template #header>
          <div class="card-header">
            <span>回测历史</span>
            <div>
              <el-button v-if="selectedRuns.length >= 2" size="small" type="success" @click="handleCompare">
                对比 ({{ selectedRuns.length }})
              </el-button>
              <el-button size="small" @click="loadHistory">
                <el-icon><Refresh /></el-icon>
                刷新
              </el-button>
            </div>
          </div>
        </template>

        <el-table :data="history" v-loading="loadingHistory" stripe @selection-change="handleSelectionChange">
          <el-table-column type="selection" width="40" :selectable="(row) => row.status === 'completed'" />
          <el-table-column label="回测名称" min-width="100">
            <template #default="{ row }">
              <el-tooltip v-if="row.description" :content="row.description" placement="top">
                <span>{{ row.name }}</span>
              </el-tooltip>
              <span v-else>{{ row.name }}</span>
            </template>
          </el-table-column>
          <el-table-column prop="strategy_name" label="策略" width="100" show-overflow-tooltip>
            <template #default="{ row }">
              <span v-if="row.strategy_name">{{ row.strategy_name }}</span>
              <span v-else class="text-muted">手动</span>
            </template>
          </el-table-column>
          <el-table-column prop="mode" label="模式" width="120">
            <template #default="{ row }">
              {{ row.mode === 'simulated' ? '撮合模拟盘' : '收益率累积' }}
            </template>
          </el-table-column>
          <el-table-column prop="start_date" label="起始日" width="110" />
          <el-table-column prop="end_date" label="结束日" width="110" />
          <el-table-column prop="initial_capital" label="初始资金" width="120">
            <template #default="{ row }">{{ formatMoney(row.initial_capital) }}</template>
          </el-table-column>
          <el-table-column label="总收益" width="100">
            <template #default="{ row }">
              <span v-if="row.result_summary" :class="row.result_summary.total_return >= 0 ? 'text-green' : 'text-red'">
                {{ row.result_summary.total_return }}%
              </span>
              <span v-else-if="row.status === 'running'" class="text-muted">计算中...</span>
              <span v-else>-</span>
            </template>
          </el-table-column>
          <el-table-column label="年化" width="90">
            <template #default="{ row }">
              <span v-if="row.result_summary">{{ row.result_summary.annualized_return }}%</span>
              <span v-else-if="row.status === 'running'" class="text-muted">计算中...</span>
              <span v-else>-</span>
            </template>
          </el-table-column>
          <el-table-column label="最大回撤" width="100">
            <template #default="{ row }">
              <span v-if="row.result_summary" class="text-red">{{ row.result_summary.max_drawdown }}%</span>
              <span v-else-if="row.status === 'running'" class="text-muted">计算中...</span>
              <span v-else>-</span>
            </template>
          </el-table-column>
          <el-table-column label="夏普" width="80">
            <template #default="{ row }">
              <span v-if="row.result_summary">{{ row.result_summary.sharpe_ratio }}</span>
              <span v-else-if="row.status === 'running'" class="text-muted">计算中...</span>
              <span v-else>-</span>
            </template>
          </el-table-column>
          <el-table-column label="胜率" width="80">
            <template #default="{ row }">
              <span v-if="row.result_summary">{{ row.result_summary.win_rate }}%</span>
              <span v-else-if="row.status === 'running'" class="text-muted">计算中...</span>
              <span v-else>-</span>
            </template>
          </el-table-column>
          <el-table-column label="状态" width="160">
            <template #default="{ row }">
              <el-tag v-if="row.status === 'completed'" type="success" size="small">完成</el-tag>
              <el-tag v-else-if="row.status === 'running'" type="warning" size="small">运行中 {{ row.progress }}%</el-tag>
              <el-tooltip v-else-if="row.status === 'failed' && row.error_message" :content="row.error_message" placement="top">
                <el-tag type="danger" size="small">失败</el-tag>
              </el-tooltip>
              <el-tag v-else-if="row.status === 'failed'" type="danger" size="small">失败</el-tag>
              <el-tag v-else type="info" size="small">{{ row.status }}</el-tag>
              <div v-if="row.status === 'running' && row.current_trade_date" class="backtest-date">回测至 {{ row.current_trade_date }}</div>
            </template>
          </el-table-column>
          <el-table-column label="操作" width="220" fixed="right">
            <template #default="{ row }">
              <div class="action-buttons">
                <el-button size="small" type="primary" @click="viewDetail(row)" :disabled="row.status !== 'completed'">详情</el-button>
                <el-button v-if="row.status === 'failed'" size="small" type="warning" @click="rerunBacktest(row)">重试</el-button>
                <el-button size="small" type="danger" @click="deleteRun(row.id)">删除</el-button>
              </div>
            </template>
          </el-table-column>
        </el-table>
      </el-card>

      <!-- 回测详情对话框 -->
      <el-dialog v-model="detailVisible" :title="`回测详情 - ${currentRun?.name}`" width="90%" top="5vh">
        <!-- 回测参数 -->
        <el-card style="margin-bottom: 16px">
          <template #header><span>回测参数</span></template>
          <el-descriptions :column="4" border size="small">
            <el-descriptions-item label="回测名称" :span="3">{{ currentRun.name }}</el-descriptions-item>
            <el-descriptions-item label="模式">
              {{ currentRun.mode === 'simulated' ? '撮合模拟盘' : '收益率累积' }}
            </el-descriptions-item>
            <el-descriptions-item label="策略">
              <span v-if="currentRun.strategy_name">{{ currentRun.strategy_name }}</span>
              <span v-else class="text-muted">手动配置</span>
            </el-descriptions-item>
            <el-descriptions-item label="日期范围">{{ currentRun.start_date }} ~ {{ currentRun.end_date }}</el-descriptions-item>
            <el-descriptions-item label="初始资金">{{ formatMoney(currentRun.initial_capital) }}</el-descriptions-item>
            <el-descriptions-item label="股票池">
              <span v-if="currentRun.group_ids && currentRun.group_ids.length > 0">候选组 {{ currentRun.group_ids.join(', ') }}</span>
              <span v-else-if="currentRun.markets && currentRun.markets.length > 0">{{ currentRun.markets.join(', ') }}</span>
              <span v-else-if="currentRun.stock_pool && currentRun.stock_pool.length > 0">{{ currentRun.stock_pool.length }} 只</span>
              <span v-else class="text-muted">全市场</span>
            </el-descriptions-item>
            <el-descriptions-item v-if="currentRun.description" label="回测说明" :span="4">{{ currentRun.description }}</el-descriptions-item>
            <el-descriptions-item label="止损 / 止盈">
              {{ formatPct(currentRun.stop_loss_pct) }} / {{ formatPct(currentRun.take_profit_pct) }}
            </el-descriptions-item>
            <el-descriptions-item label="移动止盈">
              <span v-if="currentRun.trailing_stop_pct">{{ formatPct(currentRun.trailing_stop_pct) }}</span>
              <span v-else class="text-muted">未设置</span>
            </el-descriptions-item>
            <el-descriptions-item label="止盈止损成交价">
              {{ currentRun.stop_execution_price === 'trigger' ? '触发价' : '收盘价' }}
            </el-descriptions-item>
            <el-descriptions-item label="滑点">{{ formatPct(currentRun.slippage_pct) }}</el-descriptions-item>
            <el-descriptions-item label="手续费率">{{ formatPct(currentRun.commission_rate) }}</el-descriptions-item>
            <el-descriptions-item label="最低佣金">¥{{ currentRun.min_commission || 5.0 }}</el-descriptions-item>
            <el-descriptions-item label="印花税">{{ formatPct(currentRun.stamp_tax) }}</el-descriptions-item>
            <el-descriptions-item label="过户费">{{ formatPct(currentRun.transfer_fee) }}</el-descriptions-item>
            <el-descriptions-item label="最大总仓位">{{ formatPct(currentRun.max_total_position_pct) }}</el-descriptions-item>
            <el-descriptions-item label="单股最大仓位">{{ formatPct(currentRun.max_single_position_pct) }}</el-descriptions-item>
            <el-descriptions-item label="现金预留">{{ formatPct(currentRun.cash_reserve_pct) }}</el-descriptions-item>
          </el-descriptions>
        </el-card>

        <!-- 绩效指标 -->
        <el-row :gutter="16" style="margin-bottom: 20px">
          <el-col :span="4" v-for="item in metricCards" :key="item.label">
            <el-card shadow="hover" class="metric-card">
              <div class="metric-label">{{ item.label }}</div>
              <div class="metric-value" :class="item.class">{{ item.value }}</div>
            </el-card>
          </el-col>
        </el-row>

        <!-- 净值曲线 -->
        <el-card>
          <template #header><span>净值曲线</span></template>
          <div ref="navChartRef" style="width: 100%; height: 400px"></div>
        </el-card>

        <!-- 交易记录 -->
        <el-card style="margin-top: 16px">
          <template #header><span>交易记录 ({{ trades.length }} 笔)</span></template>
          <el-table :data="trades" stripe max-height="400">
            <el-table-column prop="stock_code" label="股票代码" width="110" />
            <el-table-column prop="stock_name" label="股票名称" width="100" />
            <el-table-column label="买入日期" width="110">
              <template #default="{ row }">{{ row.buy_date }}</template>
            </el-table-column>
            <el-table-column label="买入价格" width="90">
              <template #default="{ row }">{{ row.buy_price?.toFixed(2) }}</template>
            </el-table-column>
            <el-table-column label="卖出日期" width="110">
              <template #default="{ row }">{{ row.sell_date || '-' }}</template>
            </el-table-column>
            <el-table-column label="卖出价格" width="90">
              <template #default="{ row }">{{ row.sell_price?.toFixed(2) || '-' }}</template>
            </el-table-column>
            <el-table-column label="买入佣金" width="90">
              <template #default="{ row }">{{ row.buy_commission?.toFixed(2) || '-' }}</template>
            </el-table-column>
            <el-table-column label="卖出费用" width="90">
              <template #default="{ row }">{{ row.sell_commission?.toFixed(2) || '-' }}</template>
            </el-table-column>
            <el-table-column label="盈亏(%)" width="100">
              <template #default="{ row }">
                <span :class="row.pnl_pct >= 0 ? 'text-green' : 'text-red'">
                  {{ row.pnl_pct?.toFixed(2) || '0.00' }}%
                </span>
              </template>
            </el-table-column>
            <el-table-column label="持仓天数" width="90">
              <template #default="{ row }">{{ row.holding_days || '-' }}</template>
            </el-table-column>
            <el-table-column label="卖出原因" width="200">
              <template #default="{ row }">{{ row.sell_reason || '-' }}</template>
            </el-table-column>
          </el-table>
        </el-card>
      </el-dialog>

      <!-- 数据完整性检查对话框 -->
      <el-dialog v-model="dataCheckVisible" title="数据完整性检查" width="60%">
        <div v-if="dataCheckReport">
          <el-descriptions :column="2" border>
            <el-descriptions-item label="检查股票数">{{ dataCheckReport.total_stocks_checked }}</el-descriptions-item>
            <el-descriptions-item label="完整覆盖">{{ dataCheckReport.stocks_with_full_coverage }}</el-descriptions-item>
            <el-descriptions-item label="覆盖率">{{ dataCheckReport.coverage_pct }}%</el-descriptions-item>
            <el-descriptions-item label="可继续">
              <el-tag :type="dataCheckReport.can_proceed ? 'success' : 'danger'">
                {{ dataCheckReport.can_proceed ? '是' : '否' }}
              </el-tag>
            </el-descriptions-item>
          </el-descriptions>

          <div v-if="dataCheckReport.blocking_count > 0" style="margin-top: 16px">
            <h4 style="color: #f56c6c">严重问题 ({{ dataCheckReport.blocking_count }})</h4>
            <el-alert v-for="(s, i) in dataCheckReport.blocking_summary" :key="i" :title="s" type="error" :closable="false" style="margin-bottom: 8px" />
          </div>

          <div v-if="dataCheckReport.warning_count > 0" style="margin-top: 16px">
            <h4 style="color: #e6a23c">警告 ({{ dataCheckReport.warning_count }})</h4>
            <el-alert v-for="(s, i) in dataCheckReport.warning_summary" :key="i" :title="s" type="warning" :closable="false" style="margin-bottom: 8px" />
          </div>
        </div>
        <template #footer>
          <el-button @click="dataCheckVisible = false">关闭</el-button>
        </template>
      </el-dialog>

      <!-- 回测对比对话框 -->
      <el-dialog v-model="compareVisible" title="回测对比" width="90%" top="5vh" @close="destroyCompareChart">
        <!-- 指标对比表 -->
        <el-table :data="compareMetrics" stripe style="margin-bottom: 20px">
          <el-table-column prop="label" label="指标" width="120" fixed />
          <el-table-column v-for="run in compareRuns" :key="run.id" :label="run.name" min-width="120" show-overflow-tooltip>
            <template #default="{ row }">
              <span :class="row[`best_${run.id}`] ? 'text-green' : ''">{{ row[`run_${run.id}`] }}</span>
            </template>
          </el-table-column>
        </el-table>

        <!-- 净值曲线对比 -->
        <el-card>
          <template #header><span>净值曲线对比</span></template>
          <div ref="compareChartRef" style="width: 100%; height: 450px"></div>
        </el-card>
      </el-dialog>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, nextTick, onBeforeUnmount } from 'vue'
import { ElMessage, ElMessageBox, ElTooltip } from 'element-plus'
import { Refresh } from '@element-plus/icons-vue'
import NavBar from '../components/NavBar.vue'
import { useAccountStore } from '../stores/account'
import * as echarts from 'echarts'

const accountStore = useAccountStore()
const currentAccountId = computed(() => accountStore.currentAccountId)

// 表单数据
const form = ref({
  name: '',
  mode: 'simulated',
  strategy_id: null,
  start_date: '2024-01-01',
  end_date: '2025-12-31',
  initial_capital: 1000000,
  markets: [],
  group_ids: [],
  stop_loss_pct: 5,
  take_profit_pct: 15,
  trailing_stop_pct: null,
  stop_execution_price: 'close',
  commission_rate: 0.0001,
  slippage_pct: 0,
  description: '',
})

const candidateGroups = ref([])

const strategies = ref([])
const history = ref([])
const running = ref(false)
const loadingHistory = ref(false)

// 回测详情
const detailVisible = ref(false)
const currentRun = ref(null)
const trades = ref([])
const navData = ref([])
const navChartRef = ref(null)
let navChart = null

// 数据完整性检查
const dataCheckVisible = ref(false)
const dataCheckReport = ref(null)

// 回测对比
const selectedRuns = ref([])
const compareVisible = ref(false)
const compareRuns = ref([])
const compareNavData = ref({})
const compareMetrics = ref([])
const compareChartRef = ref(null)
let compareChart = null

// 指标卡片
const metricCards = computed(() => {
  const r = currentRun.value?.result_summary || {}
  return [
    { label: '总收益率', value: r.total_return != null ? r.total_return + '%' : '-', class: r.total_return >= 0 ? 'text-green' : 'text-red' },
    { label: '年化收益率', value: r.annualized_return != null ? r.annualized_return + '%' : '-', class: r.annualized_return >= 0 ? 'text-green' : 'text-red' },
    { label: '最大回撤', value: r.max_drawdown != null ? r.max_drawdown + '%' : '-', class: 'text-red' },
    { label: '夏普比率', value: r.sharpe_ratio ?? '-' },
    { label: '卡玛比率', value: r.calmar_ratio ?? '-' },
    { label: '胜率', value: r.win_rate != null ? r.win_rate + '%' : '-' },
    { label: '盈亏比', value: r.profit_factor ?? '-' },
    { label: '总交易次数', value: r.total_trades ?? '-' },
    { label: '平均持仓', value: r.avg_holding_days != null ? r.avg_holding_days + '天' : '-' },
    { label: '最佳交易', value: r.best_trade != null ? r.best_trade + '%' : '-', class: 'text-green' },
    { label: '最差交易', value: r.worst_trade != null ? r.worst_trade + '%' : '-', class: 'text-red' },
    { label: '最终净值', value: r.final_nav ?? '-' },
  ]
})

// 加载策略列表
const loadStrategies = async () => {
  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/strategies`)
    const data = await res.json()
    strategies.value = data.strategies || []
  } catch (e) {
    console.error('加载策略失败:', e)
  }
}

// 加载候选组列表
const loadCandidateGroups = async () => {
  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/candidate-groups`)
    const data = await res.json()
    candidateGroups.value = data.groups || []
  } catch (e) {
    console.error('加载候选组失败:', e)
  }
}

// 加载回测历史
const loadHistory = async (silent = false) => {
  if (!silent) loadingHistory.value = true
  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/backtest/runs?limit=50`)
    const data = await res.json()
    history.value = data.runs || []
  } catch (e) {
    console.error('加载回测历史失败:', e)
  } finally {
    if (!silent) loadingHistory.value = false
  }
}

// 开始回测
const handleStartBacktest = async () => {
  if (!form.value.start_date || !form.value.end_date) {
    ElMessage.warning('请选择起始和结束日期')
    return
  }

  running.value = true
  try {
    const body = {
      name: form.value.name || `回测 ${form.value.start_date} ~ ${form.value.end_date}`,
      mode: form.value.mode,
      strategy_id: form.value.strategy_id,
      start_date: form.value.start_date,
      end_date: form.value.end_date,
      initial_capital: form.value.initial_capital,
      stop_loss_pct: form.value.stop_loss_pct / 100,
      take_profit_pct: form.value.take_profit_pct / 100,
      trailing_stop_pct: form.value.trailing_stop_pct ? form.value.trailing_stop_pct / 100 : null,
      stop_execution_price: form.value.stop_execution_price,
      commission_rate: form.value.commission_rate,
      slippage_pct: form.value.slippage_pct / 100,
      description: form.value.description || null,
      markets: form.value.markets.length > 0 ? form.value.markets : null,
      group_ids: form.value.group_ids.length > 0 ? form.value.group_ids : null,
      config: {},
    }

    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/backtest/runs`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    })
    const data = await res.json()

    if (data.success) {
      ElMessage.success('回测任务已启动，请稍后刷新查看结果')
      loadHistory()
    } else {
      ElMessage.error(data.message || data.error || '回测启动失败')
    }
  } catch (e) {
    ElMessage.error('网络错误: ' + e.message)
  } finally {
    running.value = false
  }
}

// 重试失败的回测
const rerunBacktest = async (row) => {
  try {
    const body = {
      name: row.name,
      mode: row.mode || 'simulated',
      strategy_id: row.strategy_id,
      start_date: row.start_date,
      end_date: row.end_date,
      initial_capital: row.initial_capital || 1000000,
      stop_loss_pct: row.stop_loss_pct || 0.05,
      take_profit_pct: row.take_profit_pct || 0.15,
      trailing_stop_pct: row.trailing_stop_pct,
      stop_execution_price: row.config?.stop_execution_price || 'close',
      commission_rate: row.commission_rate || 0.0001,
      slippage_pct: row.slippage_pct || 0,
      description: row.description || null,
      markets: row.markets || null,
      group_ids: row.config?.group_ids || null,
      config: row.config || {},
    }

    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/backtest/runs`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    })
    const data = await res.json()

    if (data.success) {
      ElMessage.success('回测任务已重新启动')
      loadHistory()
    } else {
      ElMessage.error(data.message || data.error || '回测启动失败')
    }
  } catch (e) {
    ElMessage.error('网络错误: ' + e.message)
  }
}

// 检查数据完整性
const handleCheckData = async () => {
  try {
    const body = {
      start_date: form.value.start_date,
      end_date: form.value.end_date,
      markets: form.value.markets.length > 0 ? form.value.markets : null,
      group_ids: form.value.group_ids.length > 0 ? form.value.group_ids : null,
    }

    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/backtest/check-data`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    })
    const data = await res.json()

    if (data.success) {
      dataCheckReport.value = data.report
      dataCheckVisible.value = true
    } else {
      ElMessage.error('数据检查失败')
    }
  } catch (e) {
    ElMessage.error('网络错误: ' + e.message)
  }
}

// 查看回测详情
const viewDetail = async (run) => {
  currentRun.value = run
  detailVisible.value = true

  // 加载回测详情（获取完整参数）
  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/backtest/runs/${run.id}`)
    const data = await res.json()
    currentRun.value = data.run
  } catch (e) {
    console.error('加载回测详情失败:', e)
  }

  // 加载交易记录
  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/backtest/runs/${run.id}/trades`)
    const data = await res.json()
    trades.value = data.trades || []
  } catch (e) {
    console.error('加载交易记录失败:', e)
  }

  // 加载净值数据并渲染图表
  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/backtest/runs/${run.id}/nav`)
    const data = await res.json()
    navData.value = data.nav || []
    await nextTick()
    renderNavChart()
  } catch (e) {
    console.error('加载净值数据失败:', e)
  }
}

// 渲染净值曲线
const renderNavChart = () => {
  if (!navChartRef.value) return

  if (!navChart) {
    navChart = echarts.init(navChartRef.value)
  }

  const dates = navData.value.map(d => d.trade_date)
  const navs = navData.value.map(d => d.nav)
  const values = navData.value.map(d => d.total_value)
  const drawdowns = navData.value.map(d => d.drawdown * 100)

  navChart.setOption({
    tooltip: { trigger: 'axis' },
    grid: { left: '3%', right: '4%', bottom: '3%', containLabel: true },
    xAxis: { type: 'category', data: dates, axisLabel: { rotate: 45 } },
    yAxis: [
      { type: 'value', name: '净值', position: 'left' },
      { type: 'value', name: '总资产', position: 'right' },
    ],
    series: [
      {
        name: '净值',
        type: 'line',
        data: navs,
        smooth: true,
        yAxisIndex: 0,
        lineStyle: { width: 2 },
        areaStyle: {
          color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
            { offset: 0, color: 'rgba(64, 158, 255, 0.3)' },
            { offset: 1, color: 'rgba(64, 158, 255, 0.05)' },
          ]),
        },
      },
      {
        name: '总资产',
        type: 'line',
        data: values,
        smooth: true,
        yAxisIndex: 1,
        lineStyle: { width: 1, type: 'dashed' },
        showSymbol: false,
      },
    ],
    dataZoom: [{ type: 'inside' }, { type: 'slider' }],
  })

  // 窗口大小变化时重新渲染
  window.addEventListener('resize', () => navChart.resize())
}

// 删除回测任务
const deleteRun = async (runId) => {
  try {
    await ElMessageBox.confirm('确定要删除此回测任务及其所有数据吗？', '确认删除', {
      confirmButtonText: '删除',
      cancelButtonText: '取消',
      type: 'warning',
    })

    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/backtest/runs/${runId}`, {
      method: 'DELETE',
    })
    const data = await res.json()

    if (data.success) {
      ElMessage.success('删除成功')
      loadHistory()
    } else {
      ElMessage.error('删除失败')
    }
  } catch (e) {
    // 用户取消
  }
}

// 格式化金额
const formatMoney = (val) => {
  if (!val) return '-'
  return Number(val).toLocaleString('zh-CN', { minimumFractionDigits: 0, maximumFractionDigits: 0 })
}

// 格式化百分比
const formatPct = (val) => {
  if (val == null) return '-'
  const n = Number(val)
  if (n === 0) return '0'
  return (n * 100).toFixed(2) + '%'
}

// 选中变化
const handleSelectionChange = (selection) => {
  selectedRuns.value = selection
}

// 对比回测
const handleCompare = async () => {
  compareRuns.value = [...selectedRuns.value]
  compareVisible.value = true
  compareNavData.value = {}

  // 并发加载各回测净值
  const promises = compareRuns.value.map(async (run) => {
    try {
      const res = await fetch(`/api/v1/ui/${currentAccountId.value}/backtest/runs/${run.id}/nav`)
      const data = await res.json()
      compareNavData.value[run.id] = data.nav || []
    } catch (e) {
      console.error('加载净值失败:', e)
      compareNavData.value[run.id] = []
    }
  })
  await Promise.all(promises)

  // 构建指标对比表
  buildCompareMetrics()

  // 渲染对比图
  await nextTick()
  renderCompareChart()
}

const buildCompareMetrics = () => {
  const fields = [
    { key: 'total_return', label: '总收益率', fmt: (v) => (v != null ? v.toFixed(2) + '%' : '-'), lower: false },
    { key: 'annualized_return', label: '年化收益率', fmt: (v) => (v != null ? v.toFixed(2) + '%' : '-'), lower: false },
    { key: 'max_drawdown', label: '最大回撤', fmt: (v) => (v != null ? v.toFixed(2) + '%' : '-'), lower: true },
    { key: 'sharpe_ratio', label: '夏普比率', fmt: (v) => (v != null ? v.toFixed(2) : '-'), lower: false },
    { key: 'calmar_ratio', label: '卡玛比率', fmt: (v) => (v != null ? v.toFixed(2) : '-'), lower: false },
    { key: 'win_rate', label: '胜率', fmt: (v) => (v != null ? v.toFixed(2) + '%' : '-'), lower: false },
    { key: 'profit_factor', label: '盈亏比', fmt: (v) => (v != null ? v.toFixed(2) : '-'), lower: false },
    { key: 'total_trades', label: '交易次数', fmt: (v) => (v != null ? v : '-'), lower: false },
    { key: 'avg_holding_days', label: '平均持仓', fmt: (v) => (v != null ? v.toFixed(1) + '天' : '-'), lower: false },
    { key: 'best_trade', label: '最佳交易', fmt: (v) => (v != null ? v.toFixed(2) + '%' : '-'), lower: false },
    { key: 'worst_trade', label: '最差交易', fmt: (v) => (v != null ? v.toFixed(2) + '%' : '-'), lower: false },
    { key: 'final_nav', label: '最终净值', fmt: (v) => (v != null ? v.toFixed(4) : '-'), lower: false },
  ]

  const runs = compareRuns.value
  compareMetrics.value = fields.map((f) => {
    const row = { label: f.label }
    const values = []
    runs.forEach((run) => {
      const r = run.result_summary || {}
      const v = r[f.key]
      row[`run_${run.id}`] = f.fmt(v)
      values.push({ id: run.id, v })
    })
    // 标记最优值
    const validValues = values.filter((x) => x.v != null && typeof x.v === 'number')
    if (validValues.length > 0) {
      const best = f.lower
        ? Math.min(...validValues.map((x) => x.v))
        : Math.max(...validValues.map((x) => x.v))
      validValues.forEach((x) => {
        if (x.v === best) row[`best_${x.id}`] = true
      })
    }
    return row
  })
}

const renderCompareChart = () => {
  if (!compareChartRef.value) return

  if (compareChart) {
    compareChart.dispose()
  }
  compareChart = echarts.init(compareChartRef.value)

  const colors = ['#409EFF', '#67C23A', '#E6A23C', '#F56C6C', '#909399', '#722ED1', '#13C2C2', '#EB2F96']
  const series = compareRuns.value.map((run, idx) => {
    const nav = compareNavData.value[run.id] || []
    return {
      name: run.name,
      type: 'line',
      data: nav.map((d) => d.nav),
      smooth: true,
      lineStyle: { width: 2 },
      itemStyle: { color: colors[idx % colors.length] },
      showSymbol: false,
    }
  })

  const dates = compareRuns.value.length > 0
    ? (compareNavData.value[compareRuns.value[0].id] || []).map((d) => d.trade_date)
    : []

  compareChart.setOption({
    tooltip: { trigger: 'axis' },
    legend: { data: compareRuns.value.map((r) => r.name), top: 0 },
    grid: { left: '3%', right: '4%', bottom: '3%', containLabel: true },
    xAxis: { type: 'category', data: dates, axisLabel: { rotate: 45 } },
    yAxis: { type: 'value', name: '净值' },
    series,
    dataZoom: [{ type: 'inside' }, { type: 'slider' }],
  })
}

const destroyCompareChart = () => {
  if (compareChart) {
    compareChart.dispose()
    compareChart = null
  }
}

let statusTimer = null

onMounted(() => {
  loadStrategies()
  loadCandidateGroups()
  loadHistory()
  // 自动轮询运行中的回测任务状态（静默刷新）
  statusTimer = setInterval(async () => {
    const hasRunning = history.value.some(r => r.status === 'running' || r.status === 'pending')
    if (hasRunning) {
      await loadHistory(true)
    }
  }, 3000)
})

onBeforeUnmount(() => {
  if (statusTimer) clearInterval(statusTimer)
})
</script>

<style scoped>
.backtest-container {
  min-height: 100vh;
  background-color: #f5f7fa;
}

.backtest-content {
  padding: 20px;
  max-width: 1400px;
  margin: 0 auto;
}

.backtest-content h2 {
  margin-bottom: 20px;
  color: #303133;
}

.config-card {
  margin-bottom: 20px;
}

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.metric-card {
  text-align: center;
  padding: 8px;
}

.metric-label {
  font-size: 12px;
  color: #909399;
  margin-bottom: 4px;
}

.metric-value {
  font-size: 18px;
  font-weight: bold;
  color: #303133;
}

.text-green {
  color: #67c23a;
}

.text-red {
  color: #f56c6c;
}

.text-muted {
  color: #909399;
  font-size: 12px;
}

.backtest-date {
  font-size: 11px;
  color: #e6a23c;
  margin-top: 2px;
}

.action-buttons {
  display: flex;
  gap: 4px;
  flex-wrap: nowrap;
}

:deep(.el-table) {
  font-size: 13px;
}
</style>
