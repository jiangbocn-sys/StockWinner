<template>
  <div class="layout-container">
    <NavBar />
    <el-main class="main-content">
      <h2>系统设置 - {{ currentAccount?.display_name }}</h2>

      <!-- API 配置 -->
      <el-card>
        <template #header>
          <span>API 配置</span>
        </template>
        <el-form label-width="150px">
          <el-form-item label="后端地址">
            <el-input value="http://localhost:8080" disabled />
          </el-form-item>
          <el-form-item label="服务状态">
            <el-tag type="success">运行中</el-tag>
          </el-form-item>
        </el-form>
      </el-card>

      <!-- 数据源管理 -->
      <el-card style="margin-top: 20px">
        <template #header>
          <div class="card-header">
            <span>数据源管理</span>
            <div>
              <el-button size="small" @click="openPriorityDialog">
                调整优先级
              </el-button>
              <el-button size="small" @click="loadDataSources" :loading="dsLoading">
                刷新
              </el-button>
            </div>
          </div>
        </template>

        <el-table :data="dataSources" border>
          <el-table-column prop="display_name" label="数据源" width="130" />
          <el-table-column label="启用" width="80">
            <template #default="{ row }">
              <el-switch
                v-model="row.is_enabled"
                @change="toggleDataSource(row)"
                :loading="row._toggling"
              />
            </template>
          </el-table-column>
          <el-table-column label="状态" width="100">
            <template #default="{ row }">
              <el-tag :type="dsStatusTag(row.status)" size="small">
                {{ dsStatusText(row.status) }}
              </el-tag>
            </template>
          </el-table-column>
          <el-table-column label="通道优先级" min-width="200">
            <template #default="{ row }">
              <template v-if="row.channel_priority_json">
                <el-tag size="small" style="margin-right: 4px" v-if="row.channel_priority_json.trading">交易 {{ row.channel_priority_json.trading }}</el-tag>
                <el-tag size="small" style="margin-right: 4px" type="success" v-if="row.channel_priority_json.market_data">行情 {{ row.channel_priority_json.market_data }}</el-tag>
                <el-tag size="small" type="warning" v-if="row.channel_priority_json.download">下载 {{ row.channel_priority_json.download }}</el-tag>
              </template>
            </template>
          </el-table-column>
          <el-table-column label="操作" width="100">
            <template #default="{ row }">
              <el-button size="small" v-if="row.requires_config" @click="openConfigDialog(row)">配置</el-button>
              <span v-else class="text-muted" style="color: #c0c4cc">无需配置</span>
            </template>
          </el-table-column>
        </el-table>
      </el-card>

      <!-- 通知配置 -->
      <el-card style="margin-top: 20px">
        <template #header>
          <div class="card-header">
            <span>飞书 Webhook 通知配置</span>
            <el-tag :type="notificationConfigured ? 'success' : 'warning'">
              {{ notificationConfigured ? '已配置' : '未配置' }}
            </el-tag>
          </div>
        </template>

        <el-alert
          title="如何配置飞书 Webhook"
          type="info"
          :closable="false"
          style="margin-bottom: 20px;"
        >
          <ol style="padding-left: 20px; margin: 10px 0;">
            <li>打开飞书群聊，点击右上角"设置"</li>
            <li>找到"群机器人"，点击"添加机器人"</li>
            <li>选择"自定义机器人（通过 Webhook 接入）"</li>
            <li>复制生成的 Webhook URL，粘贴到下方输入框</li>
          </ol>
        </el-alert>

        <el-form label-width="120px">
          <el-form-item label="Webhook URL">
            <el-input
              v-model="notifForm.webhook_url"
              placeholder="https://open.feishu.cn/open-apis/bot/v2/hook/..."
              style="width: 500px;"
            />
          </el-form-item>

          <el-form-item label="通知开关">
            <el-switch v-model="notifForm.enabled" :active-value="1" :inactive-value="0" />
          </el-form-item>

          <el-form-item label="事件类型">
            <el-checkbox-group v-model="notifForm.events">
              <el-checkbox label="notify_on_trade">成交通知</el-checkbox>
              <el-checkbox label="notify_on_signal">信号触发</el-checkbox>
              <el-checkbox label="notify_on_task">任务状态</el-checkbox>
            </el-checkbox-group>
          </el-form-item>

          <el-form-item>
            <el-button type="primary" @click="saveNotification" :loading="notifSaving">
              保存配置
            </el-button>
            <el-button @click="testNotification" :loading="notifTesting" :disabled="!notificationConfigured">
              发送测试通知
            </el-button>
          </el-form-item>
        </el-form>
      </el-card>

      <!-- AI Agent 管理 -->
      <el-card style="margin-top: 20px">
        <template #header>
          <div class="card-header">
            <span>AI Agent 管理</span>
            <el-tag type="info">外部 AI 客户端接入</el-tag>
          </div>
        </template>

        <el-alert
          title="通过 API Key 将 OpenClaw / Hermes / Claude Code 等 AI Agent 接入本系统"
          type="info"
          :closable="false"
          style="margin-bottom: 20px;"
        >
          <ol style="padding-left: 20px; margin: 10px 0;">
            <li>点击下方"创建 Agent"按钮，选择 Agent 类型和权限</li>
            <li>复制返回的 API Key 到 AI Agent 的配置中</li>
            <li>Agent 将通过 X-Agent-Key 请求头调用系统 API</li>
          </ol>
        </el-alert>

        <el-table :data="agents" style="width: 100%" size="default">
          <el-table-column prop="agent_id" label="Agent ID" width="120">
            <template #default="{ row }">
              <code style="font-size: 11px; color: #909399;">{{ row.agent_id.substring(0, 8) }}...</code>
            </template>
          </el-table-column>
          <el-table-column prop="name" label="名称" width="150" />
          <el-table-column prop="agent_type" label="类型" width="120" />
          <el-table-column prop="role" label="角色" width="100">
            <template #default="{ row }">
              <el-tag :type="roleTagType(row.role)" size="small">{{ row.role }}</el-tag>
            </template>
          </el-table-column>
          <el-table-column prop="rate_limit_per_min" label="限速" width="80">
            <template #default="{ row }">{{ row.rate_limit_per_min }}/min</template>
          </el-table-column>
          <el-table-column prop="last_used_at" label="最后使用" width="160" />
          <el-table-column label="操作" width="200">
            <template #default="{ row }">
              <el-button size="small" @click="showKeyDialog(row)" :icon="Key">查看 Key</el-button>
              <el-button size="small" type="danger" @click="deleteAgent(row)" :loading="deletingAgentId === row.agent_id">删除</el-button>
            </template>
          </el-table-column>
        </el-table>

        <div style="margin-top: 15px; text-align: center;">
          <el-button type="primary" @click="showCreateDialog" size="default">
            + 创建 Agent
          </el-button>
        </div>
      </el-card>

      <!-- LLM API 配置 -->
      <el-card style="margin-top: 20px">
        <template #header>
          <div class="card-header">
            <span>LLM API 配置</span>
            <el-tag :type="llmConfig.configured ? 'success' : 'danger'">
              {{ llmConfig.configured ? '已配置' : '未配置' }}
            </el-tag>
          </div>
        </template>

        <el-alert
          title="支持任意兼容 OpenAI 格式的 LLM API"
          type="info"
          :closable="false"
          style="margin-bottom: 20px;"
        >
          <ol style="padding-left: 20px; margin: 10px 0;">
            <li>选择 LLM 提供商，或选择"自定义"填写任意兼容地址</li>
            <li>输入 API Key、Base URL 和模型名称</li>
            <li>点击"测试连接"验证配置是否正确</li>
          </ol>
        </el-alert>

        <el-form label-width="120px">
          <el-form-item label="LLM 提供商">
            <el-select v-model="llmForm.provider" style="width: 300px" @change="onProviderChange">
              <el-option label="Anthropic Claude" value="anthropic" />
              <el-option label="OpenAI GPT" value="openai" />
              <el-option label="DeepSeek" value="deepseek" />
              <el-option label="阿里云通义千问" value="aliyun" />
              <el-option label="月之暗面 Kimi" value="moonshot" />
              <el-option label="智谱 AI" value="zhipu" />
              <el-option label="自定义" value="custom" />
            </el-select>
          </el-form-item>
          <el-form-item label="Base URL">
            <el-input
              v-model="llmForm.base_url"
              placeholder="https://api.example.com/v1/chat/completions"
              style="width: 500px;"
            />
          </el-form-item>
          <el-form-item label="API Key">
            <el-input
              v-model="llmForm.api_key"
              type="password"
              placeholder="输入 API Key"
              show-password
              style="width: 400px;"
            />
            <span v-if="llmConfig.api_key_masked" style="margin-left: 10px; color: #999; font-size: 12px;">
              当前配置: {{ llmConfig.api_key_masked }}
            </span>
          </el-form-item>
          <el-form-item label="模型名称">
            <el-input
              v-model="llmForm.model_name"
              placeholder="例如: gpt-4o, qwen-plus"
              style="width: 300px;"
            />
          </el-form-item>
          <el-form-item>
            <el-button type="primary" @click="saveLLMConfig" :loading="saving">
              保存配置
            </el-button>
            <el-button @click="testLLMConnection" :loading="testing">
              测试连接
            </el-button>
            <el-button type="danger" @click="deleteLLMConfig" :loading="deleting" v-if="llmConfig.configured">
              删除配置
            </el-button>
          </el-form-item>
        </el-form>

        <el-divider />

        <div class="info-section">
          <h4>LLM 策略生成说明</h4>
          <ul style="padding-left: 20px; line-height: 2;">
            <li>LLM 会将您的自然语言描述转换为结构化的交易策略配置</li>
            <li>支持描述均线、RSI、MACD、KDJ 等技术指标条件</li>
            <li>支持描述 PE、股息率等基本面筛选条件</li>
            <li>如果 API 不可用，系统会自动生成模拟策略</li>
          </ul>
        </div>
      </el-card>

      <el-card style="margin-top: 20px">
        <template #header>
          <span>策略描述示例</span>
        </template>
        <div class="examples">
          <el-tag size="small" style="margin: 5px;">低估值蓝筹股，PE&lt;10，股息率&gt;5%</el-tag>
          <el-tag size="small" style="margin: 5px;">均线金叉买入，RSI 超卖</el-tag>
          <el-tag size="small" style="margin: 5px;">科技成长股，MACD 金叉，成交量放大</el-tag>
          <el-tag size="small" style="margin: 5px;">保守型策略，小仓位，严格止损</el-tag>
        </div>
      </el-card>

      <el-card style="margin-top: 20px">
        <template #header>
          <span>账户信息</span>
        </template>
        <el-descriptions :column="1" border>
          <el-descriptions-item label="账户 ID">{{ currentAccount?.account_id }}</el-descriptions-item>
          <el-descriptions-item label="显示名称">{{ currentAccount?.display_name }}</el-descriptions-item>
          <el-descriptions-item label="用户名">{{ currentAccount?.name }}</el-descriptions-item>
        </el-descriptions>
      </el-card>
    </el-main>

    <!-- 创建 Agent 对话框 -->
    <el-dialog v-model="createVisible" title="创建 AI Agent" width="450px">
      <el-form label-width="100px">
        <el-form-item label="Agent 名称">
          <el-input v-model="createForm.name" placeholder="如: OpenClaw-Agent" />
        </el-form-item>
        <el-form-item label="Agent 类型">
          <el-select v-model="createForm.agent_type" style="width: 200px">
            <el-option label="OpenClaw" value="openclaw" />
            <el-option label="Hermes" value="hermes" />
            <el-option label="Claude Code" value="claude_code" />
            <el-option label="通用" value="generic" />
          </el-select>
        </el-form-item>
        <el-form-item label="角色">
          <el-select v-model="createForm.role" style="width: 200px">
            <el-option label="只读查询 (viewer)" value="viewer" />
            <el-option label="策略管理 (strategist)" value="strategist" />
            <el-option label="系统操作 (operator)" value="operator" />
            <el-option label="管理员 (admin)" value="admin" />
          </el-select>
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="createVisible = false">取消</el-button>
        <el-button type="primary" @click="createAgent" :loading="creating">创建</el-button>
      </template>
    </el-dialog>

    <!-- 查看 API Key 对话框 -->
    <el-dialog v-model="keyVisible" :title="keyResult ? '重置 API Key' : '查看 API Key'" width="500px">
      <div v-if="newApiKey">
        <el-alert type="warning" :closable="false" style="margin-bottom: 15px;">
          <b>请妥善保存此 API Key，关闭后不会再次显示</b>
        </el-alert>
        <el-input v-model="newApiKey" readonly>
          <template #append>
            <el-button @click="copyKey">复制</el-button>
          </template>
        </el-input>
        <div style="margin-top: 10px; color: #999; font-size: 12px;">
          使用方式：在请求头中添加 <code>X-Agent-Key: {{ newApiKey }}</code>
        </div>
      </div>
      <div v-else>
        <p style="color: #999;">系统不会存储明文 API Key，如需新 Key 请点击"重置"。</p>
      </div>
      <template #footer>
        <el-button v-if="keyResult" @click="resetKey(keyResult)" :loading="rotating">重置 Key</el-button>
        <el-button @click="keyVisible = false">关闭</el-button>
      </template>
    </el-dialog>

    <!-- 数据源配置对话框 -->
    <el-dialog v-model="dsConfigVisible" :title="'配置数据源 - ' + (dsConfigForm.provider_id || '')" width="500px">
      <el-form label-width="120px">
        <el-form-item v-if="dsConfigForm.provider_id === 'tushare'" label="API Token">
          <el-input
            v-model="dsConfigForm.api_token"
            type="password"
            placeholder="输入 Tushare Pro API Token"
            show-password
          />
        </el-form-item>
        <el-form-item v-if="dsConfigForm.provider_id === 'amazingdata'" label="Host">
          <el-input :value="'101.230.159.234'" disabled />
        </el-form-item>
        <el-form-item v-if="dsConfigForm.provider_id === 'amazingdata'" label="Port">
          <el-input :value="'8600'" disabled />
        </el-form-item>
        <el-form-item v-if="!dsConfigForm.requires_config" label="配置">
          <span style="color: #909399;">该数据源无需配置</span>
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="dsConfigVisible = false">取消</el-button>
        <el-button type="primary" @click="saveDsConfig" :loading="dsSaving">保存</el-button>
      </template>
    </el-dialog>

    <!-- 通道优先级调整对话框 -->
    <el-dialog v-model="priorityVisible" title="调整通道优先级" width="550px">
      <el-alert type="info" :closable="false" style="margin-bottom: 20px">
        数字越小越优先。拖动排序可调整 provider 顺序，系统按从上到下的顺序依次尝试。
      </el-alert>

      <el-tabs v-model="priorityTab">
        <el-tab-pane label="交易通道" name="trading">
          <el-table :data="priorityForm.trading" border style="width: 100%">
            <el-table-column type="index" label="顺序" width="60" />
            <el-table-column prop="provider_id" label="数据源" width="130" />
            <el-table-column label="状态" width="100">
              <template #default="{ row }">
                <el-tag :type="dsStatusTag(row.status)" size="small">{{ dsStatusText(row.status) }}</el-tag>
              </template>
            </el-table-column>
            <el-table-column>
              <template #header>
                <span>操作</span>
              </template>
              <template #default="{ $index }">
                <el-button size="small" :disabled="$index === 0" @click="moveUp('trading', $index)">↑</el-button>
                <el-button size="small" :disabled="$index === priorityForm.trading.length - 1" @click="moveDown('trading', $index)">↓</el-button>
              </template>
            </el-table-column>
          </el-table>
        </el-tab-pane>
        <el-tab-pane label="行情通道" name="market_data">
          <el-table :data="priorityForm.market_data" border style="width: 100%">
            <el-table-column type="index" label="顺序" width="60" />
            <el-table-column prop="provider_id" label="数据源" width="130" />
            <el-table-column label="状态" width="100">
              <template #default="{ row }">
                <el-tag :type="dsStatusTag(row.status)" size="small">{{ dsStatusText(row.status) }}</el-tag>
              </template>
            </el-table-column>
            <el-table-column>
              <template #header>
                <span>操作</span>
              </template>
              <template #default="{ $index }">
                <el-button size="small" :disabled="$index === 0" @click="moveUp('market_data', $index)">↑</el-button>
                <el-button size="small" :disabled="$index === priorityForm.market_data.length - 1" @click="moveDown('market_data', $index)">↓</el-button>
              </template>
            </el-table-column>
          </el-table>
        </el-tab-pane>
        <el-tab-pane label="下载通道" name="download">
          <el-table :data="priorityForm.download" border style="width: 100%">
            <el-table-column type="index" label="顺序" width="60" />
            <el-table-column prop="provider_id" label="数据源" width="130" />
            <el-table-column label="状态" width="100">
              <template #default="{ row }">
                <el-tag :type="dsStatusTag(row.status)" size="small">{{ dsStatusText(row.status) }}</el-tag>
              </template>
            </el-table-column>
            <el-table-column>
              <template #header>
                <span>操作</span>
              </template>
              <template #default="{ $index }">
                <el-button size="small" :disabled="$index === 0" @click="moveUp('download', $index)">↑</el-button>
                <el-button size="small" :disabled="$index === priorityForm.download.length - 1" @click="moveDown('download', $index)">↓</el-button>
              </template>
            </el-table-column>
          </el-table>
        </el-tab-pane>
      </el-tabs>

      <template #footer>
        <el-button @click="priorityVisible = false">取消</el-button>
        <el-button type="primary" @click="savePriority" :loading="dsSaving">保存</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, reactive, computed, onMounted } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { useAccountStore } from '../stores/account'
import NavBar from '../components/NavBar.vue'

const accountStore = useAccountStore()
const currentAccount = computed(() => accountStore.currentAccount)
const currentAccountId = computed(() => accountStore.currentAccountId)

const notificationConfigured = ref(false)
const notifForm = reactive({
  webhook_url: '',
  enabled: 1,
  events: [],
})
const notifSaving = ref(false)
const notifTesting = ref(false)

const loadNotificationConfig = async () => {
  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/notifications/config`)
    const data = await res.json()
    if (data.success && data.data) {
      const cfg = data.data
      notifForm.webhook_url = cfg.webhook_url || ''
      notifForm.enabled = cfg.enabled || 1
      notifForm.events = []
      if (cfg.notify_on_trade) notifForm.events.push('notify_on_trade')
      if (cfg.notify_on_signal) notifForm.events.push('notify_on_signal')
      if (cfg.notify_on_task) notifForm.events.push('notify_on_task')
      notificationConfigured.value = true
    } else {
      notifForm.webhook_url = ''
      notifForm.enabled = 1
      notifForm.events = ['notify_on_trade', 'notify_on_signal', 'notify_on_task']
      notificationConfigured.value = false
    }
  } catch (error) {
    console.error('加载通知配置失败:', error)
  }
}

const saveNotification = async () => {
  if (!notifForm.webhook_url) {
    ElMessage.warning('请输入 Webhook URL')
    return
  }

  notifSaving.value = true
  try {
    const resp = await fetch(`/api/v1/ui/${currentAccountId.value}/notifications/config`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        channel: 'feishu',
        webhook_url: notifForm.webhook_url,
        enabled: notifForm.enabled,
        notify_on_trade: notifForm.events.includes('notify_on_trade') ? 1 : 0,
        notify_on_signal: notifForm.events.includes('notify_on_signal') ? 1 : 0,
        notify_on_task: notifForm.events.includes('notify_on_task') ? 1 : 0,
      }),
    })
    const data = await resp.json()
    if (data.success) {
      ElMessage.success('通知配置已保存')
      notificationConfigured.value = true
    } else {
      ElMessage.error(data.detail || '保存失败')
    }
  } catch (error) {
    ElMessage.error('保存失败: ' + error.message)
  } finally {
    notifSaving.value = false
  }
}

const testNotification = async () => {
  notifTesting.value = true
  try {
    const resp = await fetch(`/api/v1/ui/${currentAccountId.value}/notifications/test`, {
      method: 'POST',
    })
    const data = await resp.json()
    if (data.success) {
      ElMessage.success('测试通知已发送，请检查飞书群')
    } else {
      ElMessage.error(data.detail || '发送失败')
    }
  } catch (error) {
    ElMessage.error('发送失败: ' + error.message)
  } finally {
    notifTesting.value = false
  }
}

// === LLM 配置 ===
const llmConfig = reactive({
  configured: false,
  provider: 'custom',
  base_url: '',
  model_name: '',
  api_key_masked: ''
})

const llmForm = reactive({
  provider: 'custom',
  base_url: '',
  api_key: '',
  model_name: ''
})

const PROVIDER_DEFAULTS = {
  anthropic: { base_url: 'https://api.anthropic.com/v1/messages', model: 'claude-sonnet-4-20250514' },
  openai: { base_url: 'https://api.openai.com/v1/chat/completions', model: 'gpt-4o' },
  deepseek: { base_url: 'https://api.deepseek.com/v1/chat/completions', model: 'deepseek-chat' },
  aliyun: { base_url: 'https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions', model: 'qwen-plus' },
  moonshot: { base_url: 'https://api.moonshot.cn/v1/chat/completions', model: 'moonshot-v1-8k' },
  zhipu: { base_url: 'https://open.bigmodel.cn/api/paas/v4/chat/completions', model: 'glm-4' },
  custom: { base_url: '', model: '' },
}

const onProviderChange = () => {
  const defaults = PROVIDER_DEFAULTS[llmForm.provider] || { base_url: '', model: '' }
  llmForm.base_url = defaults.base_url
  llmForm.model_name = defaults.model
}

const saving = ref(false)
const testing = ref(false)
const deleting = ref(false)

const loadLLMConfig = async () => {
  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/llm/config`)
    const data = await res.json()
    if (data.success) {
      llmConfig.configured = data.data.configured
      llmConfig.provider = data.data.provider
      llmConfig.base_url = data.data.base_url
      llmConfig.model_name = data.data.model_name
      llmConfig.api_key_masked = data.data.api_key_masked

      // 填充表单
      llmForm.provider = data.data.provider
      llmForm.base_url = data.data.base_url
      llmForm.model_name = data.data.model_name
      llmForm.api_key = ''
    }
  } catch (error) {
    console.error('加载 LLM 配置失败:', error)
  }
}

const saveLLMConfig = async () => {
  if (!llmForm.api_key && !llmConfig.api_key_masked) {
    ElMessage.warning('请输入 API Key')
    return
  }
  if (!llmForm.base_url) {
    ElMessage.warning('请输入 Base URL')
    return
  }
  if (!llmForm.model_name) {
    ElMessage.warning('请输入模型名称')
    return
  }

  saving.value = true
  try {
    const apiKey = llmForm.api_key || ''
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/llm/config`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        provider: llmForm.provider,
        base_url: llmForm.base_url,
        api_key: apiKey,
        model_name: llmForm.model_name
      })
    })
    const data = await res.json()
    if (data.success) {
      ElMessage.success(data.message)
      llmConfig.configured = true
      await loadLLMConfig()
    } else {
      ElMessage.error(data.error || '保存失败')
    }
  } catch (error) {
    ElMessage.error('保存失败：' + error.message)
  } finally {
    saving.value = false
  }
}

const testLLMConnection = async () => {
  if (!llmForm.api_key && !llmConfig.api_key_masked) {
    ElMessage.warning('请输入 API Key')
    return
  }
  saving.value = true
  testing.value = true
  try {
    const apiKey = llmForm.api_key || ''
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/llm/config`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        provider: llmForm.provider,
        base_url: llmForm.base_url,
        api_key: apiKey,
        model_name: llmForm.model_name
      })
    })
    const data = await res.json()
    if (data.success) {
      if (data.api_valid) {
        ElMessage.success('API 连接成功！')
      } else {
        ElMessage.warning(data.message || 'API 可能无效')
      }
    } else {
      ElMessage.error(data.error || '测试失败')
    }
  } catch (error) {
    ElMessage.error('测试失败：' + error.message)
  } finally {
    saving.value = false
    testing.value = false
  }
}

const deleteLLMConfig = async () => {
  deleting.value = true
  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/llm/config`, {
      method: 'DELETE'
    })
    const data = await res.json()
    if (data.success) {
      ElMessage.success('配置已删除')
      llmConfig.configured = false
      llmForm.provider = 'custom'
      llmForm.base_url = ''
      llmForm.api_key = ''
      llmForm.model_name = ''
      await loadLLMConfig()
    } else {
      ElMessage.error(data.error || '删除失败')
    }
  } catch (error) {
    ElMessage.error('删除失败：' + error.message)
  } finally {
    deleting.value = false
  }
}

onMounted(() => {
  loadLLMConfig()
  loadNotificationConfig()
  loadAgents()
  loadDataSources()
})

// === Agent 管理 ===
const agents = ref([])
const createVisible = ref(false)
const keyVisible = ref(false)
const creating = ref(false)
const deletingAgentId = ref(null)
const rotating = ref(false)
const keyResult = ref(null)
const newApiKey = ref('')

// === 数据源管理 ===
const dataSources = ref([])
const dsLoading = ref(false)
const dsConfigVisible = ref(false)
const dsSaving = ref(false)
const dsConfigForm = reactive({ provider_id: '', api_token: '', requires_config: false })
const currentDsRow = ref(null)

const dsStatusTag = (status) => {
  const map = { connected: 'success', disconnected: 'warning', error: 'danger', not_configured: 'info' }
  return map[status] || 'info'
}
const dsStatusText = (status) => {
  const map = { connected: '已连接', disconnected: '未连接', error: '连接失败', not_configured: '未配置' }
  return map[status] || '未知'
}

const getToken = () => localStorage.getItem('auth_token') || ''

const loadDataSources = async () => {
  dsLoading.value = true
  try {
    const res = await fetch('/api/v1/ui/data-sources', {
      headers: { 'X-Auth-Token': getToken() }
    })
    const data = await res.json()
    if (data.success) {
      dataSources.value = data.data
    }
  } catch (e) {
    console.error('加载数据源失败:', e)
  } finally {
    dsLoading.value = false
  }
}

const toggleDataSource = async (row) => {
  row._toggling = true
  try {
    const res = await fetch(`/api/v1/ui/data-sources/${row.provider_id}/toggle`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Auth-Token': getToken() },
      body: JSON.stringify({ is_enabled: row.is_enabled })
    })
    const data = await res.json()
    if (data.success) {
      ElMessage.success(data.message)
    } else {
      ElMessage.error(data.message || '操作失败')
      row.is_enabled = !row.is_enabled  // 回滚
    }
  } catch (e) {
    ElMessage.error('操作失败: ' + e.message)
    row.is_enabled = !row.is_enabled  // 回滚
  } finally {
    row._toggling = false
  }
}

const openConfigDialog = (row) => {
  currentDsRow.value = row
  dsConfigForm.provider_id = row.provider_id
  dsConfigForm.api_token = ''
  dsConfigForm.requires_config = row.requires_config
  dsConfigVisible.value = true
}

const saveDsConfig = async () => {
  dsSaving.value = true
  try {
    const payload = {}
    if (dsConfigForm.provider_id === 'tushare') {
      if (!dsConfigForm.api_token) {
        ElMessage.warning('请输入 API Token')
        dsSaving.value = false
        return
      }
      payload.api_token = dsConfigForm.api_token
    }

    const res = await fetch(`/api/v1/ui/data-sources/${dsConfigForm.provider_id}/config`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Auth-Token': getToken() },
      body: JSON.stringify(payload)
    })
    const data = await res.json()
    if (data.success) {
      ElMessage.success(data.message)
      dsConfigVisible.value = false
      loadDataSources()
    } else {
      ElMessage.error(data.message || '保存失败')
    }
  } catch (e) {
    ElMessage.error('保存失败: ' + e.message)
  } finally {
    dsSaving.value = false
  }
}

// === 通道优先级管理 ===
const priorityVisible = ref(false)
const priorityTab = ref('trading')
const priorityForm = reactive({
  trading: [],
  market_data: [],
  download: [],
})

const openPriorityDialog = () => {
  const enabledProviders = dataSources.value.filter(ds => ds.is_enabled)
  for (const channel of ['trading', 'market_data', 'download']) {
    const sorted = [...enabledProviders].sort((a, b) => {
      const pa = a.channel_priority_json?.[channel] ?? 999
      const pb = b.channel_priority_json?.[channel] ?? 999
      return pa - pb
    })
    priorityForm[channel] = sorted.map(ds => ({
      provider_id: ds.provider_id,
      display_name: ds.display_name,
      status: ds.status,
    }))
  }
  priorityTab.value = 'trading'
  priorityVisible.value = true
}

const moveUp = (channel, index) => {
  if (index === 0) return
  const arr = priorityForm[channel]
  ;[arr[index - 1], arr[index]] = [arr[index], arr[index - 1]]
}

const moveDown = (channel, index) => {
  const arr = priorityForm[channel]
  if (index >= arr.length - 1) return
  ;[arr[index], arr[index + 1]] = [arr[index + 1], arr[index]]
}

const savePriority = async () => {
  dsSaving.value = true
  try {
    for (const channel of ['trading', 'market_data', 'download']) {
      const provider_order = priorityForm[channel].map(p => p.provider_id)
      if (provider_order.length === 0) continue
      const res = await fetch('/api/v1/ui/data-sources/priority', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-Auth-Token': getToken() },
        body: JSON.stringify({ channel_type: channel, provider_order })
      })
      const data = await res.json()
      if (!data.success) {
        ElMessage.error(`保存 ${channel} 优先级失败: ${data.message}`)
        dsSaving.value = false
        return
      }
    }
    ElMessage.success('通道优先级已更新')
    priorityVisible.value = false
    loadDataSources()
  } catch (e) {
    ElMessage.error('保存失败: ' + e.message)
  } finally {
    dsSaving.value = false
  }
}

const createForm = reactive({
  name: '',
  agent_type: 'claude_code',
  role: 'viewer',
})

const roleTagType = (role) => {
  const map = { viewer: 'info', strategist: '', operator: 'warning', admin: 'danger' }
  return map[role] || 'info'
}


const loadAgents = async () => {
  try {
    const res = await fetch('/api/auth/agents', {
      headers: { 'X-Auth-Token': getToken() }
    })
    const data = await res.json()
    if (data.success) {
      agents.value = data.agents
    }
  } catch (e) {
    console.error('加载 Agent 列表失败:', e)
  }
}

const showCreateDialog = () => {
  createForm.name = ''
  createForm.agent_type = 'claude_code'
  createForm.role = 'viewer'
  createVisible.value = true
}

const createAgent = async () => {
  if (!createForm.name) {
    ElMessage.warning('请输入 Agent 名称')
    return
  }
  creating.value = true
  try {
    const res = await fetch('/api/auth/agent/bind', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Auth-Token': getToken() },
      body: JSON.stringify(createForm)
    })
    const data = await res.json()
    if (data.success) {
      ElMessage.success('Agent 创建成功')
      newApiKey.value = data.api_key
      keyResult.value = data
      keyVisible.value = true
      createVisible.value = false
      await loadAgents()
    } else {
      ElMessage.error(data.detail || '创建失败')
    }
  } catch (e) {
    ElMessage.error('创建失败: ' + e.message)
  } finally {
    creating.value = false
  }
}

const showKeyDialog = (agent) => {
  keyResult.value = agent
  newApiKey.value = ''
  keyVisible.value = true
}

const resetKey = async (agent) => {
  rotating.value = true
  try {
    const res = await fetch(`/api/auth/agent/${agent.agent_id}/rotate-key`, {
      method: 'POST',
      headers: { 'X-Auth-Token': getToken() }
    })
    const data = await res.json()
    if (data.success) {
      newApiKey.value = data.api_key
      ElMessage.success('API Key 已重置，旧 Key 已失效')
    } else {
      ElMessage.error(data.detail || '重置失败')
    }
  } catch (e) {
    ElMessage.error('重置失败: ' + e.message)
  } finally {
    rotating.value = false
  }
}

const deleteAgent = async (agent) => {
  try {
    await ElMessageBox.confirm(`确定要删除 Agent "${agent.name}" 吗？此操作不可恢复。`, '确认删除', {
      type: 'warning'
    })
  } catch {
    return
  }
  deletingAgentId.value = agent.agent_id
  try {
    const res = await fetch(`/api/auth/agent/${agent.agent_id}`, {
      method: 'DELETE',
      headers: { 'X-Auth-Token': getToken() }
    })
    const data = await res.json()
    if (data.success) {
      ElMessage.success(`Agent "${agent.name}" 已删除`)
      await loadAgents()
    } else {
      ElMessage.error(data.detail || '删除失败')
    }
  } catch (e) {
    ElMessage.error('删除失败: ' + e.message)
  } finally {
    deletingAgentId.value = null
  }
}

const copyKey = async () => {
  try {
    await navigator.clipboard.writeText(newApiKey.value)
    ElMessage.success('已复制到剪贴板')
  } catch {
    ElMessage.error('复制失败')
  }
}
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

h2 {
  margin-bottom: 20px;
  color: #303133;
}

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.info-section h4 {
  color: #303133;
  margin-bottom: 10px;
}

.examples {
  padding: 10px 0;
}

a {
  color: #409eff;
  text-decoration: none;
}

a:hover {
  text-decoration: underline;
}
</style>
