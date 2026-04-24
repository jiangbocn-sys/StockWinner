<template>
  <div class="layout-container">
    <NavBar />
    <el-main class="main-content">
      <div class="page-header">
        <h2>选股监控 - {{ currentAccount?.display_name }}</h2>
        <el-space>
          <el-button type="primary" @click="showStrategySelectDialog = true" :loading="screeningProgress.processing">
            <el-icon><Search /></el-icon>
            选股
          </el-button>
          <el-button :type="screeningRunning ? 'danger' : 'success'" @click="toggleScreening">
            <el-icon><VideoPlay /></el-icon>
            {{ screeningRunning ? '停止选股' : '启动选股' }}
          </el-button>
          <el-button type="primary" @click="showDownloadDialog = true" :loading="downloading">
            <el-icon><Download /></el-icon>
            下载数据
          </el-button>
          <el-button type="success" @click="showFactorCalcDialog = true" :loading="calculatingFactors">
            <el-icon><DataAnalysis /></el-icon>
            因子计算
          </el-button>
          <el-button type="danger" @click="confirmClearWatchlist" :disabled="watchlist.length === 0">
            <el-icon><Delete /></el-icon>
            清空已选
          </el-button>
          <el-button @click="loadWatchlist">
            <el-icon><Refresh /></el-icon>
            刷新
          </el-button>
        </el-space>
      </div>

      <!-- 选股进度 -->
      <el-card v-if="screeningProgress.processing" class="progress-card">
        <el-progress
          :percentage="screeningProgress.percent"
          :status="screeningProgress.status"
          :stroke-width="20"
          :format="formatProgress"
        />
        <div class="progress-details">
          <span>已处理：{{ screeningProgress.processed }}/{{ screeningProgress.total }}</span>
          <span>已匹配：{{ screeningProgress.matched }} 只</span>
          <span v-if="screeningProgress.currentStock">当前：{{ screeningProgress.currentStock }}</span>
          <span v-if="screeningProgress.estimatedRemaining">预计剩余：{{ formatTime(screeningProgress.estimatedRemaining) }}</span>
        </div>
      </el-card>

      <!-- 本地数据状态 -->
      <el-card class="status-card">
        <el-descriptions :column="5" border>
          <el-descriptions-item label="本地股票数">
            {{ dataStats.total_stocks || 0 }}
          </el-descriptions-item>
          <el-descriptions-item label="K 线数据量">
            {{ dataStats.total_records || 0 }}
          </el-descriptions-item>
          <el-descriptions-item label="最新日期">
            {{ dataStats.latest_date || '无数据' }}
          </el-descriptions-item>
          <el-descriptions-item label="最早日期">
            {{ dataStats.earliest_date || '无数据' }}
          </el-descriptions-item>
          <el-descriptions-item label="数据源">
            <el-tag :type="dataStats.total_stocks > 0 ? 'success' : 'warning'" size="small">
              {{ dataStats.total_stocks > 0 ? '本地' : 'SDK' }}
            </el-tag>
          </el-descriptions-item>
        </el-descriptions>
      </el-card>

      <!-- 服务状态 -->
      <el-card class="status-card">
        <el-descriptions :column="4" border>
          <el-descriptions-item label="选股服务">
            <el-tag :type="screeningRunning ? 'success' : 'info'" size="small">
              {{ screeningRunning ? '运行中' : '已停止' }}
            </el-tag>
          </el-descriptions-item>
          <el-descriptions-item label="监控服务">
            <el-tag :type="monitoringRunning ? 'success' : 'info'" size="small">
              {{ monitoringRunning ? '运行中' : '已停止' }}
            </el-tag>
          </el-descriptions-item>
          <el-descriptions-item label="Watchlist 数量">
            {{ watchlist.length }}
          </el-descriptions-item>
          <el-descriptions-item label="待处理">
            {{ watchlist.filter(w => w.status === 'pending').length }}
          </el-descriptions-item>
        </el-descriptions>
      </el-card>

      <!-- Watchlist 列表 -->
      <el-card>
        <template #header>
          <div class="card-header">
            <span>候选股票池</span>
            <el-radio-group v-model="filterStatus" size="small" @change="loadWatchlist">
              <el-radio-button label="">全部</el-radio-button>
              <el-radio-button label="pending">待观察</el-radio-button>
              <el-radio-button label="watching">观察中</el-radio-button>
              <el-radio-button label="bought">已买入</el-radio-button>
              <el-radio-button label="sold">已卖出</el-radio-button>
            </el-radio-group>
          </div>
        </template>

        <el-table :data="watchlist" stripe style="width: 100%" v-loading="loading">
          <el-table-column prop="stock_code" label="股票代码" width="120" />
          <el-table-column prop="stock_name" label="股票名称" width="120" />
          <el-table-column prop="reason" label="入选原因" min-width="180" show-overflow-tooltip />
          <el-table-column prop="buy_price" label="买入价" width="90" align="right">
            <template #default="{ row }">¥{{ row.buy_price?.toFixed(2) }}</template>
          </el-table-column>
          <el-table-column prop="stop_loss_price" label="止损价" width="90" align="right">
            <template #default="{ row }">¥{{ row.stop_loss_price?.toFixed(2) }}</template>
          </el-table-column>
          <el-table-column prop="take_profit_price" label="止盈价" width="90" align="right">
            <template #default="{ row }">¥{{ row.take_profit_price?.toFixed(2) }}</template>
          </el-table-column>
          <el-table-column prop="target_quantity" label="数量" width="80" align="right" />
          <el-table-column prop="status" label="状态" width="90">
            <template #default="{ row }">
              <el-tag :type="getStatusType(row.status)" size="small">
                {{ getStatusText(row.status) }}
              </el-tag>
            </template>
          </el-table-column>
          <el-table-column prop="created_at" label="入选时间" width="160" />
          <el-table-column label="操作" fixed="right" width="200">
            <template #default="{ row }">
              <el-button type="primary" size="small" @click="editStock(row)">
                编辑
              </el-button>
              <el-button type="danger" size="small" @click="removeStock(row)">
                移除
              </el-button>
            </template>
          </el-table-column>
        </el-table>

        <el-empty v-if="!loading && watchlist.length === 0" description="暂无候选股票，运行选股服务添加股票" />
      </el-card>

      <!-- 下载数据对话框 -->
      <el-dialog v-model="showDownloadDialog" title="下载 K 线数据" width="550px">
        <el-alert
          title="下载数据到本地数据库"
          description="下载全市场股票的 K 线数据到本地，选股速度将大幅提升（从 30 分钟降至 2-3 分钟）"
          type="info"
          :closable="false"
          style="margin-bottom: 20px"
        />

        <el-form :model="downloadForm" label-width="120px">
          <el-form-item label="下载模式">
            <el-radio-group v-model="downloadForm.mode">
              <el-radio label="incremental">增量更新（推荐）</el-radio>
              <el-radio label="full">重新下载</el-radio>
            </el-radio-group>
          </el-form-item>

          <el-form-item label="时间范围">
            <el-radio-group v-model="downloadForm.dateMode" style="margin-bottom: 15px;">
              <el-radio label="preset">预设范围</el-radio>
              <el-radio label="custom">自定义范围</el-radio>
            </el-radio-group>

            <!-- 预设范围 -->
            <el-select v-if="downloadForm.dateMode === 'preset'" v-model="downloadForm.years" style="width: 200px;">
              <el-option label="最近 6 个月" :value="0.5" />
              <el-option label="最近 1 年" :value="1" />
              <el-option label="最近 2 年（推荐）" :value="2" />
              <el-option label="最近 3 年" :value="3" />
              <el-option label="最近 5 年" :value="5" />
              <el-option label="最近 10 年" :value="10" />
            </el-select>

            <!-- 自定义范围 -->
            <div v-else style="display: flex; align-items: center; gap: 10px;">
              <el-date-picker
                v-model="downloadForm.startDate"
                type="date"
                placeholder="开始日期"
                :disabled-date="(date) => date > new Date()"
                style="width: 160px;"
              />
              <span>至</span>
              <el-date-picker
                v-model="downloadForm.endDate"
                type="date"
                placeholder="结束日期"
                :disabled-date="(date) => date > new Date()"
                style="width: 160px;"
              />
            </div>

            <span style="margin-left: 10px; color: #909399; font-size: 13px;">
              (当前最早：{{ dataStats.earliest_date || '无数据' }}，最新：{{ dataStats.latest_date || '无数据' }})
            </span>
          </el-form-item>

          <el-form-item label="每批次数量">
            <el-select v-model="downloadForm.batchSize">
              <el-option label="10 只/批" :value="10" />
              <el-option label="20 只/批（推荐）" :value="20" />
              <el-option label="50 只/批" :value="50" />
            </el-select>
          </el-form-item>

          <el-form-item label="市场筛选">
            <el-checkbox-group v-model="downloadForm.marketFilter">
              <el-checkbox label="SH">沪市 A 股</el-checkbox>
              <el-checkbox label="SZ">深市 A 股</el-checkbox>
              <el-checkbox label="BJ">北交所</el-checkbox>
            </el-checkbox-group>
            <div style="color: #909399; font-size: 12px; margin-top: 5px;">
              提示：北交所股票数据可能不完整，建议仅下载沪深 A 股
            </div>
          </el-form-item>

          <el-divider />

          <div style="background: #f5f7fa; padding: 15px; border-radius: 4px; margin-top: 10px;">
            <div style="font-size: 13px; color: #606266;">
              <p style="margin: 0 0 8px 0;"><strong>当前数据状态：</strong></p>
              <ul style="margin: 0; padding-left: 20px;">
                <li>股票数量：{{ dataStats.total_stocks || 0 }}</li>
                <li>K 线记录：{{ dataStats.total_records || 0 }}</li>
                <li>最新日期：{{ dataStats.latest_date || '无数据' }}</li>
              </ul>
            </div>
          </div>
        </el-form>

        <template #footer>
          <el-button @click="showDownloadDialog = false">取消</el-button>
          <el-button type="primary" @click="startDownload" :loading="downloading">
            {{ downloading ? '下载中...' : '开始下载' }}
          </el-button>
        </template>
      </el-dialog>

      <!-- 下载进度对话框 -->
      <el-dialog v-model="showDownloadProgress" title="下载进度" width="600px" :close-on-click-modal="false" :close-on-press-escape="false">
        <el-progress
          :percentage="downloadProgress.percent"
          :status="downloadProgress.status === 'error' ? 'exception' : downloadProgress.status === 'completed' ? 'success' : undefined"
          :stroke-width="20"
          :format="formatDownloadProgress"
        />
        <div class="progress-details" style="margin-top: 20px;">
          <p><strong>状态：</strong>{{ getDownloadStatusText(downloadProgress.status) }}</p>
          <p v-if="downloadProgress.current_stock"><strong>当前股票：</strong>{{ downloadProgress.current_stock }}</p>
          <p><strong>已处理：</strong>{{ downloadProgress.processed_tasks }} / {{ downloadProgress.total_tasks }}</p>
          <p><strong>已下载记录：</strong>{{ downloadProgress.downloaded_records }}</p>
          <p v-if="downloadProgress.elapsed_seconds"><strong>已用时间：</strong>{{ formatTime(downloadProgress.elapsed_seconds) }}</p>
          <p v-if="downloadProgress.estimated_remaining"><strong>预计剩余：</strong>{{ formatTime(downloadProgress.estimated_remaining) }}</p>
          <p v-if="downloadProgress.message"><strong>消息：</strong>{{ downloadProgress.message }}</p>
          <p v-if="downloadProgress.error" style="color: #f56c6c;"><strong>错误：</strong>{{ downloadProgress.error }}</p>
        </div>
        <template #footer>
          <el-button @click="showDownloadProgress = false" :disabled="downloadProgress.status === 'downloading' || downloadProgress.status === 'preparing' || downloadProgress.status === 'calculating_factors'">
            关闭
          </el-button>
          <el-button type="primary" @click="loadDataStats" :disabled="downloadProgress.status === 'downloading' || downloadProgress.status === 'preparing' || downloadProgress.status === 'calculating_factors'">
            刷新数据
          </el-button>
        </template>
      </el-dialog>

      <!-- 因子计算对话框 -->
      <el-dialog v-model="showFactorCalcDialog" title="计算日频因子" width="500px">
        <el-alert
          title="计算并更新 stock_daily_factors 表"
          description="对 kline_data 表中的数据进行因子计算，补充缺失的因子数据"
          type="info"
          :closable="false"
          style="margin-bottom: 20px"
        />

        <!-- 计算进度显示 -->
        <div v-if="factorCalcProgress.status === 'calculating'" style="margin-bottom: 20px;">
          <el-progress
            :percentage="factorCalcProgress.percent"
            :stroke-width="20"
            :format="formatProgress"
          />
          <div class="progress-details" style="margin-top: 15px;">
            <p><strong>当前批次：</strong>{{ factorCalcProgress.current_batch }}/{{ factorCalcProgress.total_batches }}</p>
            <p><strong>当前股票：</strong>{{ factorCalcProgress.current_stock }}</p>
            <p><strong>已插入：</strong>{{ factorCalcProgress.inserted_count }} 条</p>
            <p><strong>已更新：</strong>{{ factorCalcProgress.updated_count }} 条</p>
            <p><strong>耗时：</strong>{{ formatTime(factorCalcProgress.elapsed_seconds) }}</p>
          </div>
        </div>

        <el-form :model="factorCalcForm" label-width="120px" v-if="factorCalcProgress.status !== 'calculating'">
          <el-form-item label="计算模式">
            <el-radio-group v-model="factorCalcForm.mode">
              <el-radio label="smart">智能更新（推荐）</el-radio>
              <el-radio label="full">全量重算</el-radio>
            </el-radio-group>
            <div style="color: #909399; font-size: 12px; margin-top: 5px;">
              智能更新：自动检测并计算缺失记录和空值字段；全量：重新计算所有数据
            </div>
          </el-form-item>

          <el-form-item label="日期范围">
            <div style="display: flex; align-items: center; gap: 10px;">
              <el-date-picker
                v-model="factorCalcForm.startDate"
                type="date"
                placeholder="开始日期"
                value-format="YYYY-MM-DD"
                style="width: 160px;"
              />
              <span>至</span>
              <el-date-picker
                v-model="factorCalcForm.endDate"
                type="date"
                placeholder="结束日期"
                value-format="YYYY-MM-DD"
                style="width: 160px;"
              />
            </div>
            <div style="color: #909399; font-size: 12px; margin-top: 5px;">
              不指定日期时，智能更新自动检测最新日期
            </div>
          </el-form-item>

          <el-form-item label="因子范围">
            <div style="color: #606266; font-size: 13px;">
              自动计算全部因子（技术指标 + 扩展指标）
            </div>
            <div style="color: #909399; font-size: 12px; margin-top: 5px;">
              包括：MA、EMA、KDJ、MACD、RSI、布林带、ATR、CCI、ADX、涨跌幅、乖离率、振幅等
            </div>
          </el-form-item>

          <el-divider />

          <div style="background: #f5f7fa; padding: 15px; border-radius: 4px;">
            <div style="font-size: 13px; color: #606266;">
              <p style="margin: 0 0 8px 0;"><strong>数据状态：</strong></p>
              <ul style="margin: 0; padding-left: 20px;">
                <li>kline_data 最新：{{ dataStats.latest_date || '无' }}</li>
                <li>因子数据最新：{{ factorStats.latest_date || '未知' }}</li>
                <li>待计算股票：{{ factorStats.pending_count || '未知' }}</li>
              </ul>
            </div>
          </div>
        </el-form>

        <template #footer>
          <el-button @click="showFactorCalcDialog = false" :disabled="factorCalcProgress.status === 'calculating'">取消</el-button>
          <el-button type="success" @click="startFactorCalc" :loading="calculatingFactors" v-if="factorCalcProgress.status !== 'calculating'">
            开始计算
          </el-button>
          <el-button type="primary" @click="loadFactorStats" v-if="factorCalcProgress.status === 'completed'">
            刷新状态
          </el-button>
        </template>
      </el-dialog>

      <!-- 清空确认对话框 -->
      <el-dialog v-model="showClearDialog" title="确认清空" width="400px">
        <el-alert
          :title="`确定要清空所有已选股票吗？（共 ${watchlist.length} 只）`"
          description="此操作不可恢复，清空后需要重新选股添加"
          type="warning"
          :closable="false"
        />
        <template #footer>
          <el-button @click="showClearDialog = false">取消</el-button>
          <el-button type="danger" @click="clearWatchlist" :loading="clearing">
            {{ clearing ? '清空中...' : '确认清空' }}
          </el-button>
        </template>
      </el-dialog>

      <!-- 策略选择对话框 -->
      <el-dialog v-model="showStrategySelectDialog" title="选择选股策略" width="500px">
        <el-alert
          title="选择策略进行选股"
          description="选股结果将暂存到候选列表，经确认后才会添加到 watchlist"
          type="info"
          :closable="false"
          style="margin-bottom: 20px"
        />

        <el-form :model="strategySelectForm" label-width="120px">
          <el-form-item label="选择策略" required>
            <el-select v-model="strategySelectForm.strategyId" placeholder="请选择策略" style="width: 100%">
              <el-option
                v-for="s in strategies"
                :key="s.id"
                :label="`${s.name} (${s.status === 'active' ? '激活' : '停用'})`"
                :value="s.id"
                :disabled="s.status !== 'active'"
              />
            </el-select>
          </el-form-item>

          <el-form-item label="数据源">
            <el-radio-group v-model="strategySelectForm.useLocal">
              <el-radio :label="true">本地数据（快）</el-radio>
              <el-radio :label="false">SDK 实时（慢）</el-radio>
            </el-radio-group>
          </el-form-item>

          <el-divider />

          <div style="font-size: 13px; color: #606266;">
            <p style="margin: 0 0 8px 0;"><strong>当前激活策略：</strong></p>
            <ul style="margin: 0; padding-left: 20px;">
              <li v-for="s in activeStrategies" :key="s.id">
                <strong>{{ s.name }}</strong> - {{ s.description || '无描述' }}
              </li>
              <li v-if="activeStrategies.length === 0">暂无激活策略，请先在策略管理页面激活策略</li>
            </ul>
          </div>
        </el-form>

        <template #footer>
          <el-button @click="showStrategySelectDialog = false">取消</el-button>
          <el-button type="primary" @click="runScreeningWithStrategy" :loading="screeningProgress.processing">
            开始选股
          </el-button>
        </template>
      </el-dialog>

      <!-- 临时候选确认对话框 -->
      <el-dialog v-model="showCandidatesDialog" title="确认候选股票" width="700px">
        <el-alert
          :title="`发现 ${candidates.length} 只候选股票，请确认是否加入 watchlist`"
          :description="`已选 ${selectedCandidates.length} 只`"
          type="info"
          :closable="false"
          style="margin-bottom: 20px"
        />

        <el-table :data="candidates" stripe style="width: 100%" @selection-change="handleSelectionChange">
          <el-table-column type="selection" width="50" />
          <el-table-column prop="stock_code" label="股票代码" width="120" />
          <el-table-column prop="stock_name" label="股票名称" width="120" />
          <el-table-column prop="reason" label="入选原因" min-width="150" show-overflow-tooltip />
          <el-table-column prop="buy_price" label="买入价" width="80" align="right">
            <template #default="{ row }">¥{{ row.buy_price?.toFixed(2) }}</template>
          </el-table-column>
          <el-table-column prop="match_score" label="匹配度" width="80" align="right">
            <template #default="{ row }">{{ (row.match_score * 100).toFixed(0) }}%</template>
          </el-table-column>
          <el-table-column prop="created_at" label="时间" width="160" />
        </el-table>

        <template #footer>
          <el-button @click="cancelCandidates">取消</el-button>
          <el-button type="danger" @click="rejectCandidates(false)" :loading="confirming">
            拒绝未选
          </el-button>
          <el-button type="warning" @click="rejectCandidates(true)" :loading="confirming">
            全部拒绝
          </el-button>
          <el-button type="primary" @click="confirmCandidates" :loading="confirming">
            确认已选 ({{ selectedCandidates.length }})
          </el-button>
        </template>
      </el-dialog>

      <!-- 编辑对话框 -->
      <el-dialog v-model="showEditDialog" title="编辑股票" width="500px">
        <el-form :model="editingStock" label-width="100px">
          <el-form-item label="股票代码">
            <el-input v-model="editingStock.stock_code" disabled />
          </el-form-item>
          <el-form-item label="股票名称">
            <el-input v-model="editingStock.stock_name" />
          </el-form-item>
          <el-form-item label="买入价格">
            <el-input-number v-model="editingStock.buy_price" :precision="2" :step="0.1" />
          </el-form-item>
          <el-form-item label="止损价格">
            <el-input-number v-model="editingStock.stop_loss_price" :precision="2" :step="0.1" />
          </el-form-item>
          <el-form-item label="止盈价格">
            <el-input-number v-model="editingStock.take_profit_price" :precision="2" :step="0.1" />
          </el-form-item>
          <el-form-item label="目标数量">
            <el-input-number v-model="editingStock.target_quantity" :min="100" :step="100" />
          </el-form-item>
          <el-form-item label="状态">
            <el-select v-model="editingStock.status">
              <el-option label="待观察" value="pending" />
              <el-option label="观察中" value="watching" />
              <el-option label="已买入" value="bought" />
              <el-option label="已卖出" value="sold" />
              <el-option label="已忽略" value="ignored" />
            </el-select>
          </el-form-item>
        </el-form>
        <template #footer>
          <el-button @click="showEditDialog = false">取消</el-button>
          <el-button type="primary" @click="saveStock">保存</el-button>
        </template>
      </el-dialog>
    </el-main>
  </div>
</template>

<script setup>
import { ref, reactive, onMounted, computed } from 'vue'
import { ElMessage } from 'element-plus'
import { Search, VideoPlay, Download, Refresh, Delete, VideoPause, DataAnalysis } from '@element-plus/icons-vue'
import { useAccountStore } from '../stores/account'
import NavBar from '../components/NavBar.vue'

const accountStore = useAccountStore()
const currentAccountId = computed(() => accountStore.currentAccountId)
const currentAccount = computed(() => accountStore.currentAccount)

const loading = ref(false)
const watchlist = ref([])
const filterStatus = ref('')
const screeningRunning = ref(false)
const monitoringRunning = ref(false)
const showEditDialog = ref(false)
const showDownloadDialog = ref(false)
const showDownloadProgress = ref(false)
const showClearDialog = ref(false)
const showStrategySelectDialog = ref(false)
const showCandidatesDialog = ref(false)
const showFactorCalcDialog = ref(false)  // 因子计算对话框
const downloading = ref(false)
const clearing = ref(false)
const confirming = ref(false)
const calculatingFactors = ref(false)  // 因子计算进行中
const downloadForm = reactive({
  mode: 'incremental',  // incremental: 增量更新，full: 重新下载
  dateMode: 'preset',   // preset: 预设范围，custom: 自定义范围
  years: 2,
  startDate: null,
  endDate: null,
  batchSize: 20,
  marketFilter: ['SH', 'SZ']  // 市场筛选：默认只下载沪深 A 股，跳过北交所
})
const strategySelectForm = reactive({
  strategyId: null,
  useLocal: true
})

// 因子计算表单
const factorCalcForm = reactive({
  mode: 'fill_empty',
  startDate: null,
  endDate: null
})

// 因子数据统计
const factorStats = ref({
  latest_date: null,
  pending_count: 0
})

// 因子计算进度
const factorCalcProgress = ref({
  status: 'idle',
  total_stocks: 0,
  processed_stocks: 0,
  inserted_count: 0,
  updated_count: 0,
  percent: 0,
  current_stock: '',
  current_batch: 0,
  total_batches: 0,
  message: '',
  error: '',
  elapsed_seconds: 0,
  estimated_remaining: 0
})

let factorCalcProgressTimer = null

const strategies = ref([])
const candidates = ref([])
const selectedCandidates = ref([])
const dataStats = ref({
  total_stocks: 0,
  total_records: 0,
  latest_date: null,
  earliest_date: null
})

// 下载进度
const downloadProgress = ref({
  status: 'idle',
  total_stocks: 0,
  total_tasks: 0,
  processed_tasks: 0,
  downloaded_records: 0,
  percent: 0,
  current_stock: '',
  current_batch: 0,
  total_batches: 0,
  message: '',
  error: '',
  elapsed_seconds: 0,
  estimated_remaining: 0
})

let downloadProgressTimer = null

// 格式化下载进度显示
const formatDownloadProgress = (percent) => `${percent}%`

// 获取下载状态文本
const getDownloadStatusText = (status) => {
  const statusMap = {
    'idle': '空闲',
    'preparing': '准备中',
    'downloading': '下载中',
    'calculating_factors': '计算因子中',
    'completed': '已完成',
    'error': '错误'
  }
  return statusMap[status] || status
}
const screeningProgress = reactive({
  processing: false,
  total: 0,
  processed: 0,
  matched: 0,
  currentStock: '',
  estimatedRemaining: 0,
  percent: 0,
  status: ''
})

let progressPollingTimer = null

// 格式化进度显示
const formatProgress = (percent) => `${percent}%`

// 格式化时间
const formatTime = (seconds) => {
  if (seconds < 60) return `${seconds}秒`
  const mins = Math.floor(seconds / 60)
  const secs = seconds % 60
  return `${mins}分${secs}秒`
}

// 轮询选股进度
const pollProgress = async () => {
  try {
    const response = await fetch(`/api/v1/ui/${currentAccountId.value}/screening/status`)
    const data = await response.json()

    if (data.progress) {
      const progress = data.progress
      screeningProgress.total = progress.total_stocks || 0
      screeningProgress.processed = progress.processed || 0
      screeningProgress.matched = progress.matched || 0
      screeningProgress.currentStock = progress.current_stock || ''
      screeningProgress.estimatedRemaining = progress.estimated_remaining || 0

      if (screeningProgress.total > 0) {
        screeningProgress.percent = Math.round((screeningProgress.processed / screeningProgress.total) * 100)
      }

      // 选股完成的判断：phase 为 done 或者已处理数量等于总数
      const isCompleted = progress.current_phase === 'done' ||
                          (progress.total_stocks > 0 && progress.processed >= progress.total_stocks)

      if (isCompleted) {
        screeningProgress.processing = false
        screeningProgress.status = 'success'
        if (progressPollingTimer) {
          clearInterval(progressPollingTimer)
          progressPollingTimer = null
        }
        // 选股完成，检查临时候选
        await checkTempCandidates()
        if (candidates.value.length === 0) {
          // 如果没有候选，直接刷新列表
          await loadWatchlist()
          ElMessage.success(`选股完成，共匹配 ${screeningProgress.matched} 只股票`)
        }
      } else if (progress.current_phase === 'scanning') {
        screeningProgress.status = ''
        screeningProgress.processing = true
      }
    }
  } catch (error) {
    console.error('获取进度失败:', error)
  }
}

const editingStock = reactive({
  stock_code: '',
  stock_name: '',
  buy_price: 0,
  stop_loss_price: 0,
  take_profit_price: 0,
  target_quantity: 100,
  status: 'pending'
})

// 开始下载
const startDownload = async () => {
  downloading.value = true
  try {
    // 验证市场筛选
    if (downloadForm.marketFilter.length === 0) {
      ElMessage.error('请至少选择一个市场')
      downloading.value = false
      return
    }

    // 根据模式构建请求参数
    let requestData = {
      mode: downloadForm.mode,
      batch_size: downloadForm.batchSize,
      market_filter: downloadForm.marketFilter  // 添加市场筛选参数
    }

    if (downloadForm.dateMode === 'preset') {
      requestData.years = downloadForm.years
    } else {
      // 自定义日期范围
      if (!downloadForm.startDate || !downloadForm.endDate) {
        ElMessage.error('请选择开始和结束日期')
        downloading.value = false
        return
      }
      const startDate = new Date(downloadForm.startDate)
      const endDate = new Date(downloadForm.endDate)

      if (startDate > endDate) {
        ElMessage.error('开始日期不能晚于结束日期')
        downloading.value = false
        return
      }

      requestData.start_date = startDate.toISOString().split('T')[0]
      requestData.end_date = endDate.toISOString().split('T')[0]
    }

    const response = await fetch(`/api/v1/ui/${currentAccountId.value}/data/download`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(requestData)
    })
    const data = await response.json()

    if (data.success) {
      ElMessage.success('开始下载 K 线数据，请在进度窗口查看实时进度')
      showDownloadDialog.value = false
      // 显示进度窗口并启动轮询
      showDownloadProgress.value = true
      pollDownloadProgress()
    } else {
      ElMessage.error(data.message || '下载失败')
    }
  } catch (error) {
    console.error('下载失败:', error)
    ElMessage.error('下载失败')
  } finally {
    downloading.value = false
  }
}

// 轮询下载进度
const pollDownloadProgress = async () => {
  try {
    const response = await fetch(`/api/v1/ui/${currentAccountId.value}/data/download/progress`)
    const data = await response.json()

    if (data.success && data.progress) {
      const progress = data.progress
      downloadProgress.value = {
        status: progress.status,
        total_stocks: progress.total_stocks,
        total_tasks: progress.total_tasks,
        processed_tasks: progress.processed_tasks,
        downloaded_records: progress.downloaded_records,
        percent: progress.percent,
        current_stock: progress.current_stock,
        current_batch: progress.current_batch,
        total_batches: progress.total_batches,
        message: progress.message,
        error: progress.error,
        elapsed_seconds: progress.elapsed_seconds,
        estimated_remaining: progress.estimated_remaining
      }

      // 下载完成的判断
      const isCompleted = progress.status === 'completed' || progress.status === 'error'

      if (isCompleted) {
        if (downloadProgressTimer) {
          clearInterval(downloadProgressTimer)
          downloadProgressTimer = null
        }
        if (progress.status === 'completed') {
          ElMessage.success('下载完成')
          // 延迟加载统计数据
          setTimeout(loadDataStats, 2000)
        } else if (progress.error) {
          ElMessage.error(`下载失败：${progress.error}`)
        }
      } else {
        // 继续轮询
        downloadProgressTimer = setTimeout(pollDownloadProgress, 2000)
      }
    }
  } catch (error) {
    console.error('获取下载进度失败:', error)
  }
}

// 加载策略列表
const loadStrategies = async () => {
  try {
    const response = await fetch(`/api/v1/ui/${currentAccountId.value}/strategies`)
    const data = await response.json()
    strategies.value = data.strategies || []
  } catch (error) {
    console.error('加载策略失败:', error)
  }
}

// 计算激活的策略
const activeStrategies = computed(() => {
  return strategies.value.filter(s => s.status === 'active')
})

// 显示策略选择对话框
const showStrategySelect = () => {
  loadStrategies()
  showStrategySelectDialog.value = true
}

// 带策略选择的选股
const runScreeningWithStrategy = async () => {
  if (!strategySelectForm.strategyId) {
    ElMessage.warning('请选择策略')
    return
  }

  screeningProgress.processing = true
  screeningProgress.status = ''
  screeningProgress.percent = 0
  screeningProgress.processed = 0
  screeningProgress.total = 0
  screeningProgress.matched = 0
  showStrategySelectDialog.value = false

  try {
    const response = await fetch(`/api/v1/ui/${currentAccountId.value}/screening/run`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        strategy_id: strategySelectForm.strategyId,
        use_local: strategySelectForm.useLocal,
        pending_to_temp: true  // 暂存到临时表
      })
    })
    const data = await response.json()

    if (data.success) {
      // 开始轮询进度
      progressPollingTimer = setInterval(pollProgress, 2000)
    } else {
      ElMessage.error(data.message || '选股失败')
      screeningProgress.processing = false
    }
  } catch (error) {
    console.error('选股失败:', error)
    ElMessage.error('选股失败')
    screeningProgress.processing = false
  }
}

// 检查临时候选股票
const checkTempCandidates = async () => {
  try {
    const response = await fetch(`/api/v1/ui/${currentAccountId.value}/candidates`)
    const data = await response.json()
    if (data.candidates && data.candidates.length > 0) {
      candidates.value = data.candidates
      showCandidatesDialog.value = true
    }
  } catch (error) {
    console.error('加载候选股票失败:', error)
  }
}

// 处理选择变化
const handleSelectionChange = (selection) => {
  selectedCandidates.value = selection.map(s => s.stock_code)
}

// 确认候选股票
const confirmCandidates = async () => {
  confirming.value = true
  try {
    const response = await fetch(`/api/v1/ui/${currentAccountId.value}/candidates/confirm`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        stock_codes: selectedCandidates.value.length > 0 ? selectedCandidates.value : null,
        confirm: true
      })
    })
    const data = await response.json()

    if (data.success) {
      ElMessage.success(`已确认 ${data.confirmed} 只股票`)
      showCandidatesDialog.value = false
      await loadWatchlist()
    }
  } catch (error) {
    console.error('确认失败:', error)
    ElMessage.error('确认失败')
  } finally {
    confirming.value = false
  }
}

// 拒绝候选股票
const rejectCandidates = async (rejectAll) => {
  confirming.value = true
  try {
    const response = await fetch(`/api/v1/ui/${currentAccountId.value}/candidates/confirm`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        stock_codes: rejectAll ? null : selectedCandidates.value,
        confirm: false
      })
    })
    const data = await response.json()

    if (data.success) {
      ElMessage.success(`已拒绝 ${data.rejected} 只股票`)
      showCandidatesDialog.value = false
    }
  } catch (error) {
    console.error('拒绝失败:', error)
    ElMessage.error('拒绝失败')
  } finally {
    confirming.value = false
  }
}

// 取消候选对话框
const cancelCandidates = () => {
  showCandidatesDialog.value = false
  screeningProgress.processing = false
  screeningProgress.status = ''
}

// 清空 watchlist
const clearWatchlist = async () => {
  clearing.value = true
  try {
    const response = await fetch(`/api/v1/ui/${currentAccountId.value}/watchlist/clear`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' }
    })
    const data = await response.json()

    if (data.success) {
      ElMessage.success('已清空所有已选股票')
      showClearDialog.value = false
      await loadWatchlist()
    } else {
      ElMessage.error(data.message || '清空失败')
    }
  } catch (error) {
    console.error('清空失败:', error)
    ElMessage.error('清空失败')
  } finally {
    clearing.value = false
  }
}

// 加载数据 stats
const loadDataStats = async () => {
  try {
    const response = await fetch(`/api/v1/ui/${currentAccountId.value}/data/stats`)
    const data = await response.json()
    if (data.success) {
      dataStats.value = data.stats
    }
  } catch (error) {
    console.error('加载数据统计失败:', error)
  }
}

// 加载因子数据统计
const loadFactorStats = async () => {
  try {
    const response = await fetch(`/api/v1/ui/${currentAccountId.value}/data/factor-stats`)
    const data = await response.json()
    if (data.success) {
      factorStats.value = data.stats
    }
  } catch (error) {
    console.error('加载因子统计失败:', error)
  }
}

// 开始因子计算
const startFactorCalc = async () => {
  calculatingFactors.value = true
  try {
    const payload = {
      mode: factorCalcForm.mode,
      start_date: factorCalcForm.startDate,
      end_date: factorCalcForm.endDate
    }

    const response = await fetch(`/api/v1/ui/${currentAccountId.value}/data/calculate-factors`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    })

    const data = await response.json()
    if (data.success) {
      if (data.status === 'running') {
        // 已有计算任务在运行，开始轮询进度
        startFactorCalcProgressPolling()
      } else if (data.status === 'started') {
        // 新任务已启动，开始轮询进度
        factorCalcProgress.value = {
          status: 'calculating',
          percent: 0,
          message: '因子计算已启动...'
        }
        startFactorCalcProgressPolling()
      } else {
        // 旧版本同步返回结果
        ElMessage.success(`因子计算完成：插入 ${data.inserted_count || 0} 条记录`)
        showFactorCalcDialog.value = false
        loadFactorStats()
      }
    } else {
      ElMessage.error(data.error || '因子计算失败')
    }
  } catch (error) {
    console.error('因子计算失败:', error)
    ElMessage.error('因子计算失败：' + error.message)
  } finally {
    calculatingFactors.value = false
  }
}

// 轮询因子计算进度
const startFactorCalcProgressPolling = () => {
  if (factorCalcProgressTimer) {
    clearInterval(factorCalcProgressTimer)
  }

  factorCalcProgressTimer = setInterval(async () => {
    try {
      const response = await fetch(`/api/v1/ui/${currentAccountId.value}/data/factor-calc/progress`)
      const data = await response.json()

      if (data.success && data.progress) {
        const progress = data.progress
        factorCalcProgress.value = progress

        // 完成时停止轮询
        if (progress.status === 'completed') {
          clearInterval(factorCalcProgressTimer)
          factorCalcProgressTimer = null
          calculatingFactors.value = false
          ElMessage.success(`因子计算完成：插入 ${progress.inserted_count || 0} 条，更新 ${progress.updated_count || 0} 条`)
          loadFactorStats()
        } else if (progress.status === 'error') {
          clearInterval(factorCalcProgressTimer)
          factorCalcProgressTimer = null
          calculatingFactors.value = false
          ElMessage.error(progress.error || '因子计算出错')
        }
      }
    } catch (error) {
      console.error('轮询因子计算进度失败:', error)
    }
  }, 2000)  // 每2秒轮询一次
}

// 加载 watchlist
const loadWatchlist = async () => {
  loading.value = true
  try {
    const url = filterStatus.value
      ? `/api/v1/ui/${currentAccountId.value}/watchlist?status=${filterStatus.value}`
      : `/api/v1/ui/${currentAccountId.value}/watchlist`

    const response = await fetch(url)
    const data = await response.json()
    watchlist.value = data.watchlist || []
  } catch (error) {
    console.error('加载 watchlist 失败:', error)
    ElMessage.error('加载失败')
  } finally {
    loading.value = false
  }
}

// 确认清空 watchlist
const confirmClearWatchlist = () => {
  showClearDialog.value = true
}

// 切换选股服务
const toggleScreening = async () => {
  try {
    const url = `/api/v1/ui/${currentAccountId.value}/screening/${screeningRunning.value ? 'stop' : 'start'}`
    const response = await fetch(url, { method: 'POST' })
    const data = await response.json()

    if (data.success) {
      ElMessage.success(screeningRunning.value ? '选股服务已停止' : '选股服务已启动')
      screeningRunning.value = !screeningRunning.value
    } else {
      ElMessage.error(data.message || '操作失败')
    }
  } catch (error) {
    console.error('切换选股服务失败:', error)
    ElMessage.error('操作失败')
  }
}

// 检查服务状态
const checkServiceStatus = async () => {
  try {
    const [screeningRes, monitoringRes] = await Promise.all([
      fetch(`/api/v1/ui/${currentAccountId.value}/screening/status`),
      fetch(`/api/v1/ui/${currentAccountId.value}/monitoring/status`)
    ])

    const screeningData = await screeningRes.json()
    const monitoringData = await monitoringRes.json()

    screeningRunning.value = screeningData.screening?.running || false
    monitoringRunning.value = monitoringData.monitoring?.running || false

    // 如果选股服务正在运行，开始轮询进度
    if (screeningData.screening?.running && !screeningProgress.processing) {
      screeningProgress.processing = true
      progressPollingTimer = setInterval(pollProgress, 2000)
    }
  } catch (error) {
    console.error('检查服务状态失败:', error)
  }
}

// 编辑股票
const editStock = (row) => {
  Object.assign(editingStock, row)
  showEditDialog.value = true
}

// 保存股票
const saveStock = async () => {
  try {
    const response = await fetch(`/api/v1/ui/${currentAccountId.value}/watchlist/${editingStock.stock_code}/prices`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        buy_price: editingStock.buy_price,
        stop_loss_price: editingStock.stop_loss_price,
        take_profit_price: editingStock.take_profit_price,
        target_quantity: editingStock.target_quantity
      })
    })

    if (response.ok) {
      // 更新状态
      await fetch(`/api/v1/ui/${currentAccountId.value}/watchlist/${editingStock.stock_code}/status`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ status: editingStock.status })
      })

      ElMessage.success('保存成功')
      showEditDialog.value = false
      await loadWatchlist()
    }
  } catch (error) {
    console.error('保存失败:', error)
    ElMessage.error('保存失败')
  }
}

// 移除股票
const removeStock = async (row) => {
  try {
    const response = await fetch(`/api/v1/ui/${currentAccountId.value}/watchlist/${row.stock_code}`, {
      method: 'DELETE'
    })

    if (response.ok) {
      ElMessage.success('已移除')
      await loadWatchlist()
    }
  } catch (error) {
    console.error('移除失败:', error)
    ElMessage.error('移除失败')
  }
}

const getStatusType = (status) => {
  const types = {
    'pending': 'info',
    'watching': 'warning',
    'bought': 'success',
    'sold': 'success',
    'ignored': 'info'
  }
  return types[status] || 'info'
}

const getStatusText = (status) => {
  const texts = {
    'pending': '待观察',
    'watching': '观察中',
    'bought': '已买入',
    'sold': '已卖出',
    'ignored': '已忽略'
  }
  return texts[status] || status
}

// 组件卸载时清理定时器
import { onUnmounted } from 'vue'
onUnmounted(() => {
  if (progressPollingTimer) {
    clearInterval(progressPollingTimer)
    progressPollingTimer = null
  }
})

onMounted(async () => {
  await checkServiceStatus()
  await loadDataStats()
  await loadWatchlist()
  await loadStrategies()
  // 检查是否有待确认的候选股票
  await checkTempCandidates()
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

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.progress-card {
  margin-bottom: 20px;
}

.progress-details {
  display: flex;
  justify-content: space-between;
  margin-top: 10px;
  font-size: 13px;
  color: #606266;
}
</style>
