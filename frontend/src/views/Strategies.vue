<template>
  <div class="layout-container">
    <NavBar />
    <el-main class="main-content">
      <div class="page-header">
        <h2>策略管理 - {{ currentAccount?.display_name }}</h2>
        <el-space>
          <el-button type="primary" @click="showCreateDialog = true">
            <el-icon><Plus /></el-icon>
            新建策略
          </el-button>
          <el-button type="success" @click="showLLMDialog = true" :disabled="generating">
            <el-icon><MagicStick /></el-icon>
            LLM 生成策略
          </el-button>
          <el-button @click="loadStrategies">
            <el-icon><Refresh /></el-icon>
            刷新
          </el-button>
        </el-space>
      </div>

      <!-- 策略列表 -->
      <el-card>
        <template #header>
          <div class="card-header">
            <span>我的策略</span>
            <el-radio-group v-model="filterStatus" size="small" @change="loadStrategies">
              <el-radio-button label="">全部</el-radio-button>
              <el-radio-button label="draft">草稿</el-radio-button>
              <el-radio-button label="active">激活</el-radio-button>
              <el-radio-button label="inactive">停用</el-radio-button>
            </el-radio-group>
          </div>
        </template>

        <el-table :data="strategies" stripe style="width: 100%" v-loading="loading">
          <el-table-column prop="name" label="策略名称" width="200" />
          <el-table-column prop="description" label="描述" min-width="200" show-overflow-tooltip />
          <el-table-column prop="strategy_type" label="类型" width="80">
            <template #default="{ row }">
              <el-tag :type="row.strategy_type === 'llm' ? 'warning' : 'info'" size="small">
                {{ row.strategy_type === 'llm' ? 'LLM 生成' : '手动' }}
              </el-tag>
            </template>
          </el-table-column>
          <el-table-column prop="status" label="状态" width="80">
            <template #default="{ row }">
              <el-tag :type="getStatusType(row.status)" size="small">
                {{ getStatusText(row.status) }}
              </el-tag>
            </template>
          </el-table-column>
          <el-table-column prop="created_at" label="创建时间" width="160" />
          <el-table-column label="操作" fixed="right" width="340">
            <template #default="{ row }">
              <el-button type="primary" size="small" @click="viewStrategy(row)">
                详情
              </el-button>
              <el-button type="success" size="small" @click="editStrategy(row)">
                编辑
              </el-button>
              <el-button
                v-if="row.status === 'active'"
                type="warning"
                size="small"
                @click="deactivateStrategy(row)"
              >
                停用
              </el-button>
              <el-button
                v-else
                type="success"
                size="small"
                @click="activateStrategy(row)"
              >
                激活
              </el-button>
              <el-button type="info" size="small" @click="viewBacktest(row)">
                回测
              </el-button>
              <el-button type="danger" size="small" @click="deleteStrategy(row)">
                删除
              </el-button>
            </template>
          </el-table-column>
        </el-table>

        <el-empty v-if="!loading && strategies.length === 0" description="暂无策略，创建一个吧！" />
      </el-card>

      <!-- 创建策略对话框 -->
      <el-dialog v-model="showCreateDialog" title="新建策略" width="500px">
        <el-form :model="newStrategy" label-width="100px" @submit.prevent>
          <el-form-item label="策略名称" required>
            <el-input v-model="newStrategy.name" placeholder="输入策略名称" />
          </el-form-item>
          <el-form-item label="策略描述">
            <el-input
              v-model="newStrategy.description"
              type="textarea"
              :rows="3"
              placeholder="描述策略逻辑"
            />
          </el-form-item>
          <el-form-item label="策略类型">
            <el-select v-model="newStrategy.strategy_type">
              <el-option label="手动创建" value="manual" />
              <el-option label="条件策略" value="conditional" />
            </el-select>
          </el-form-item>
        </el-form>
        <template #footer>
          <el-button @click="showCreateDialog = false">取消</el-button>
          <el-button type="primary" @click="handleCreateStrategy">创建</el-button>
        </template>
      </el-dialog>

      <!-- 编辑策略对话框 -->
      <el-dialog v-model="showEditDialog" title="编辑策略" width="500px">
        <el-form :model="editingStrategy" label-width="100px" @submit.prevent>
          <el-form-item label="策略名称" required>
            <el-input v-model="editingStrategy.name" placeholder="输入策略名称" />
          </el-form-item>
          <el-form-item label="策略描述">
            <el-input
              v-model="editingStrategy.description"
              type="textarea"
              :rows="3"
              placeholder="描述策略逻辑"
            />
          </el-form-item>
          <el-form-item label="匹配度阈值">
            <el-slider v-model="editingStrategy.match_score_threshold" :min="0.1" :max="1.0" :step="0.1" :marks="{0.1: '10%', 0.5: '50%', 1.0: '100%'}" />
            <div style="color: #909399; font-size: 12px; margin-top: 5px;">
              入选条件：满足全部条件的股票比例 ≥ 阈值（推荐 50%）
            </div>
          </el-form-item>
          <el-form-item label="止损比例">
            <el-slider v-model="editingStrategy.stop_loss_pct" :min="0.01" :max="0.20" :step="0.01" :marks="{0.01: '1%', 0.05: '5%', 0.10: '10%', 0.20: '20%'}" />
          </el-form-item>
          <el-form-item label="止盈比例">
            <el-slider v-model="editingStrategy.take_profit_pct" :min="0.05" :max="0.50" :step="0.05" :marks="{0.05: '5%', 0.15: '15%', 0.30: '30%', 0.50: '50%'}" />
          </el-form-item>
          <el-form-item label="策略状态">
            <el-select v-model="editingStrategy.status">
              <el-option label="草稿" value="draft" />
              <el-option label="激活" value="active" />
              <el-option label="停用" value="inactive" />
            </el-select>
          </el-form-item>
        </el-form>
        <template #footer>
          <el-button @click="showEditDialog = false">取消</el-button>
          <el-button type="primary" @click="handleUpdateStrategy" :loading="updating">保存</el-button>
        </template>
      </el-dialog>

      <!-- LLM 生成策略对话框 -->
      <el-dialog v-model="showLLMDialog" title="LLM 生成策略" width="550px">
        <el-form :model="llmStrategy" label-width="120px" @submit.prevent>
          <el-form-item label="策略描述" required>
            <el-input
              v-model="llmStrategy.description"
              type="textarea"
              :rows="4"
              placeholder="描述你想要的策略，例如：'低估值蓝筹股，PE<10，股息率>5%'"
              :disabled="generating"
            />
          </el-form-item>
          <el-form-item label="风险等级">
            <el-select v-model="llmStrategy.risk_level" :disabled="generating">
              <el-option label="保守" value="low" />
              <el-option label="稳健" value="medium" />
              <el-option label="激进" value="high" />
            </el-select>
          </el-form-item>
          <el-form-item label="匹配度阈值">
            <el-slider v-model="llmStrategy.match_score_threshold" :min="0.1" :max="1.0" :step="0.1" :marks="{0.1: '10%', 0.5: '50%', 1.0: '100%'}" :disabled="generating" />
            <div style="color: #909399; font-size: 12px; margin-top: 5px;">
              入选条件：满足全部条件的股票比例 ≥ 阈值（推荐 50%）
            </div>
          </el-form-item>
          <el-form-item label="止损比例">
            <el-slider v-model="llmStrategy.stop_loss_pct" :min="0.01" :max="0.20" :step="0.01" :marks="{0.01: '1%', 0.05: '5%', 0.10: '10%', 0.20: '20%'}" :disabled="generating" />
          </el-form-item>
          <el-form-item label="止盈比例">
            <el-slider v-model="llmStrategy.take_profit_pct" :min="0.05" :max="0.50" :step="0.05" :marks="{0.05: '5%', 0.15: '15%', 0.30: '30%', 0.50: '50%'}" :disabled="generating" />
          </el-form-item>
        </el-form>
        <template #footer>
          <el-button @click="showLLMDialog = false" :disabled="generating">取消</el-button>
          <el-button type="warning" @click="handleGenerateLLMStrategy" :loading="generating">
            <el-icon v-if="!generating"><MagicStick /></el-icon>
            {{ generating ? '正在生成策略...' : '生成策略' }}
          </el-button>
        </template>
      </el-dialog>

      <!-- 策略详情对话框 -->
      <el-dialog v-model="showDetailDialog" :title="selectedStrategy?.name" width="650px">
        <el-descriptions :column="2" border v-if="selectedStrategy">
          <el-descriptions-item label="策略 ID">{{ selectedStrategy.id }}</el-descriptions-item>
          <el-descriptions-item label="策略名称">{{ selectedStrategy.name }}</el-descriptions-item>
          <el-descriptions-item label="描述" :span="2">{{ selectedStrategy.description }}</el-descriptions-item>
          <el-descriptions-item label="类型">
            <el-tag :type="selectedStrategy.strategy_type === 'llm' ? 'warning' : 'info'" size="small">
              {{ selectedStrategy.strategy_type === 'llm' ? 'LLM 生成' : '手动' }}
            </el-tag>
          </el-descriptions-item>
          <el-descriptions-item label="状态">
            <el-tag :type="getStatusType(selectedStrategy.status)" size="small">
              {{ getStatusText(selectedStrategy.status) }}
            </el-tag>
          </el-descriptions-item>
          <el-descriptions-item label="匹配度阈值">
            {{ ((selectedStrategy.match_score_threshold || 0.5) * 100).toFixed(0) }}%
          </el-descriptions-item>
          <el-descriptions-item label="止损/止盈">
            {{ ((parseConfig(selectedStrategy.config)?.stop_loss_pct || 0.05) * 100).toFixed(0) }}% / {{ ((parseConfig(selectedStrategy.config)?.take_profit_pct || 0.15) * 100).toFixed(0) }}%
          </el-descriptions-item>
          <el-descriptions-item label="买入条件" :span="2">
            <el-tag v-for="(cond, idx) in parseConfig(selectedStrategy.config)?.conditions?.buy || []" :key="idx" size="small" style="margin: 2px;">
              {{ cond }}
            </el-tag>
            <span v-if="!(parseConfig(selectedStrategy.config)?.conditions?.buy || []).length" style="color: #999;">无</span>
          </el-descriptions-item>
          <el-descriptions-item label="卖出条件" :span="2">
            <el-tag v-for="(cond, idx) in parseConfig(selectedStrategy.config)?.conditions?.sell || []" :key="idx" size="small" style="margin: 2px;" type="danger">
              {{ cond }}
            </el-tag>
            <span v-if="!(parseConfig(selectedStrategy.config)?.conditions?.sell || []).length" style="color: #999;">无</span>
          </el-descriptions-item>
          <el-descriptions-item label="创建时间">{{ selectedStrategy.created_at }}</el-descriptions-item>
          <el-descriptions-item label="更新时间">{{ selectedStrategy.updated_at }}</el-descriptions-item>
        </el-descriptions>
      </el-dialog>

      <!-- 回测结果对话框 -->
      <el-dialog v-model="showBacktestDialog" title="回测结果" width="600px">
        <div v-if="backtestResult">
          <el-alert
            :title="backtestResult.backtest_result?.message"
            type="info"
            :closable="false"
            style="margin-bottom: 20px"
          />
          <el-row :gutter="20">
            <el-col :span="12">
              <el-statistic title="总收益率" :value="(backtestResult.backtest_result?.data?.total_return || 0) * 100" suffix="%" />
            </el-col>
            <el-col :span="12">
              <el-statistic title="年化收益" :value="(backtestResult.backtest_result?.data?.annual_return || 0) * 100" suffix="%" />
            </el-col>
          </el-row>
          <el-row :gutter="20" style="margin-top: 20px">
            <el-col :span="12">
              <el-statistic title="夏普比率" :value="backtestResult.backtest_result?.data?.sharpe_ratio || 0" :precision="2" />
            </el-col>
            <el-col :span="12">
              <el-statistic title="最大回撤" :value="(backtestResult.backtest_result?.data?.max_drawdown || 0) * 100" suffix="%" />
            </el-col>
          </el-row>
          <el-row :gutter="20" style="margin-top: 20px">
            <el-col :span="12">
              <el-statistic title="胜率" :value="(backtestResult.backtest_result?.data?.win_rate || 0) * 100" suffix="%" />
            </el-col>
            <el-col :span="12">
              <el-statistic title="交易次数" :value="backtestResult.backtest_result?.data?.total_trades || 0" />
            </el-col>
          </el-row>
        </div>
        <template #footer>
          <el-button @click="showBacktestDialog = false">关闭</el-button>
        </template>
      </el-dialog>
    </el-main>
  </div>
</template>

<script setup>
import { ref, reactive, onMounted, computed } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { useAccountStore } from '../stores/account'
import NavBar from '../components/NavBar.vue'

const accountStore = useAccountStore()
const currentAccountId = computed(() => accountStore.currentAccountId)
const currentAccount = computed(() => accountStore.currentAccount)

const loading = ref(false)
const generating = ref(false)  // LLM 生成中状态
const strategies = ref([])
const filterStatus = ref('')

// 对话框
const showCreateDialog = ref(false)
const showLLMDialog = ref(false)
const showDetailDialog = ref(false)
const showBacktestDialog = ref(false)
const showEditDialog = ref(false)
const updating = ref(false)  // 更新中状态

// 新建策略表单
const newStrategy = reactive({
  name: '',
  description: '',
  strategy_type: 'manual'
})

// LLM 生成策略表单
const llmStrategy = reactive({
  description: '',
  risk_level: 'medium',
  match_score_threshold: 0.5,  // 50% 匹配度阈值
  stop_loss_pct: 0.05,  // 5% 止损
  take_profit_pct: 0.15  // 15% 止盈
})

// 选中的策略
const selectedStrategy = ref(null)
const backtestResult = ref(null)

// 编辑中的策略
const editingStrategy = reactive({
  id: null,
  name: '',
  description: '',
  match_score_threshold: 0.5,
  stop_loss_pct: 0.05,
  take_profit_pct: 0.15,
  status: 'draft'
})

// 加载策略列表
const loadStrategies = async () => {
  loading.value = true
  try {
    const url = filterStatus.value
      ? `/api/v1/ui/${currentAccountId.value}/strategies?status=${filterStatus.value}`
      : `/api/v1/ui/${currentAccountId.value}/strategies`

    const response = await fetch(url)
    const data = await response.json()
    strategies.value = data.strategies || []
  } catch (error) {
    console.error('加载策略失败:', error)
    ElMessage.error('加载策略失败')
  } finally {
    loading.value = false
  }
}

// 创建策略
const createStrategy = async () => {
  if (!newStrategy.name) {
    ElMessage.warning('请输入策略名称')
    return
  }

  try {
    const payload = {
      name: newStrategy.name,
      description: newStrategy.description,
      strategy_type: newStrategy.strategy_type
    }
    console.log('创建策略，发送数据:', payload)

    const response = await fetch(`/api/v1/ui/${currentAccountId.value}/strategies`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    })

    console.log('响应状态:', response.status)
    const data = await response.json()
    console.log('响应数据:', data)

    if (data.success) {
      ElMessage.success('策略创建成功')
      // 重置表单
      newStrategy.name = ''
      newStrategy.description = ''
      newStrategy.strategy_type = 'manual'
      await loadStrategies()
    } else {
      ElMessage.error(data.message || '创建失败')
    }
  } catch (error) {
    console.error('创建策略失败:', error)
    ElMessage.error('创建失败：' + error.message)
  } finally {
    showCreateDialog.value = false
  }
}

// 处理创建策略（包装函数）
const handleCreateStrategy = () => {
  createStrategy()
}

// LLM 生成策略
const generateLLMStrategy = async () => {
  if (!llmStrategy.description) {
    ElMessage.warning('请输入策略描述')
    return
  }

  // 设置生成中状态
  generating.value = true

  try {
    const payload = {
      description: llmStrategy.description,
      risk_level: llmStrategy.risk_level,
      match_score_threshold: llmStrategy.match_score_threshold,
      stop_loss_pct: llmStrategy.stop_loss_pct,
      take_profit_pct: llmStrategy.take_profit_pct
    }
    console.log('LLM 生成策略，发送数据:', payload)

    const response = await fetch(`/api/v1/ui/${currentAccountId.value}/strategies/generate`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    })

    console.log('响应状态:', response.status)
    const data = await response.json()
    console.log('响应数据:', data)

    if (data.success) {
      // 使用 LLM 返回的策略数据创建策略
      const createPayload = {
        name: data.strategy.name,
        description: data.strategy.description,
        strategy_type: data.strategy.strategy_type,
        config: data.strategy.config,
        match_score_threshold: llmStrategy.match_score_threshold
      }

      const createResponse = await fetch(`/api/v1/ui/${currentAccountId.value}/strategies`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(createPayload)
      })

      const createData = await createResponse.json()
      if (createData.success) {
        ElMessage.success('策略生成并创建成功')
        llmStrategy.description = ''
        llmStrategy.risk_level = 'medium'
        llmStrategy.match_score_threshold = 0.5
        llmStrategy.stop_loss_pct = 0.05
        llmStrategy.take_profit_pct = 0.15
        showLLMDialog.value = false
        await loadStrategies()
      } else {
        ElMessage.error('策略创建失败：' + createData.message)
      }
    } else {
      ElMessage.error(data.message || '生成失败')
    }
  } catch (error) {
    console.error('生成策略失败:', error)
    ElMessage.error('生成失败：' + error.message)
  } finally {
    // 重置生成中状态
    generating.value = false
  }
}

// 处理生成 LLM 策略（包装函数）
const handleGenerateLLMStrategy = () => {
  generateLLMStrategy()
}

// 查看策略详情
const viewStrategy = (row) => {
  selectedStrategy.value = row
  showDetailDialog.value = true
}

// 编辑策略
const editStrategy = (row) => {
  editingStrategy.id = row.id
  editingStrategy.name = row.name
  editingStrategy.description = row.description || ''
  editingStrategy.match_score_threshold = row.match_score_threshold || 0.5
  editingStrategy.status = row.status || 'draft'

  // 解析 config 获取止损止盈比例
  const config = parseConfig(row.config)
  editingStrategy.stop_loss_pct = config.stop_loss_pct || 0.05
  editingStrategy.take_profit_pct = config.take_profit_pct || 0.15

  showEditDialog.value = true
}

// 更新策略
const updateStrategy = async () => {
  if (!editingStrategy.name) {
    ElMessage.warning('请输入策略名称')
    return
  }

  try {
    const payload = {
      name: editingStrategy.name,
      description: editingStrategy.description,
      status: editingStrategy.status,
      config: {
        stop_loss_pct: editingStrategy.stop_loss_pct,
        take_profit_pct: editingStrategy.take_profit_pct
      }
    }

    const response = await fetch(`/api/v1/ui/${currentAccountId.value}/strategies/${editingStrategy.id}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    })

    const data = await response.json()
    if (data.success) {
      ElMessage.success('策略更新成功')
      showEditDialog.value = false
      await loadStrategies()
    } else {
      ElMessage.error(data.message || '更新失败')
    }
  } catch (error) {
    console.error('更新策略失败:', error)
    ElMessage.error('更新失败：' + error.message)
  } finally {
    updating.value = false
  }
}

// 处理更新策略（包装函数）
const handleUpdateStrategy = () => {
  updateStrategy()
}

// 激活策略
const activateStrategy = async (row) => {
  try {
    const response = await fetch(`/api/v1/ui/${currentAccountId.value}/strategies/${row.id}/activate`, {
      method: 'POST'
    })
    const data = await response.json()

    if (data.success) {
      ElMessage.success('策略已激活')
      await loadStrategies()
    }
  } catch (error) {
    ElMessage.error('激活失败')
  }
}

// 停用策略
const deactivateStrategy = async (row) => {
  try {
    const response = await fetch(`/api/v1/ui/${currentAccountId.value}/strategies/${row.id}/deactivate`, {
      method: 'POST'
    })
    const data = await response.json()

    if (data.success) {
      ElMessage.success('策略已停用')
      await loadStrategies()
    }
  } catch (error) {
    ElMessage.error('停用失败')
  }
}

// 删除策略
const deleteStrategy = async (row) => {
  try {
    await ElMessageBox.confirm('确定要删除这个策略吗？', '确认删除', { type: 'warning' })

    const response = await fetch(`/api/v1/ui/${currentAccountId.value}/strategies/${row.id}`, {
      method: 'DELETE'
    })
    const data = await response.json()

    if (data.success) {
      ElMessage.success('删除成功')
      await loadStrategies()
    }
  } catch (error) {
    if (error !== 'cancel') {
      ElMessage.error('删除失败')
    }
  }
}

// 查看回测结果
const viewBacktest = async (row) => {
  try {
    const response = await fetch(`/api/v1/ui/${currentAccountId.value}/strategies/${row.id}/backtest`)
    const data = await response.json()
    backtestResult.value = data
    showBacktestDialog.value = true
  } catch (error) {
    ElMessage.error('加载回测结果失败')
  }
}

// 辅助函数
const getStatusType = (status) => {
  const types = {
    'draft': 'info',
    'active': 'success',
    'inactive': 'warning'
  }
  return types[status] || 'info'
}

const getStatusText = (status) => {
  const texts = {
    'draft': '草稿',
    'active': '激活',
    'inactive': '停用'
  }
  return texts[status] || status
}

const parseConfig = (config) => {
  if (!config) return {}
  if (typeof config === 'string') {
    try {
      return JSON.parse(config)
    } catch {
      return config
    }
  }
  return config
}

onMounted(async () => {
  await loadStrategies()
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

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

pre {
  background-color: #f5f7fa;
  padding: 10px;
  border-radius: 4px;
  max-height: 300px;
  overflow: auto;
}
</style>
