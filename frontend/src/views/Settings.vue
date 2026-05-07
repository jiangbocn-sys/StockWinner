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
  </div>
</template>

<script setup>
import { ref, reactive, computed, onMounted } from 'vue'
import { ElMessage } from 'element-plus'
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
