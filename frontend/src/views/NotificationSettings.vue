<template>
  <div class="notification-settings">
    <el-card class="settings-card">
      <template #header>
        <div class="card-header">
          <span>通知规则配置</span>
          <el-button type="primary" size="small" @click="saveSettings" :loading="saving">
            保存配置
          </el-button>
        </div>
      </template>

      <!-- 渠道配置 -->
      <el-divider content-position="left">渠道配置</el-divider>
      <el-form-item label="飞书 Webhook URL">
        <el-input v-model="channelConfig.webhook_url" placeholder="输入飞书机器人 Webhook URL" />
      </el-form-item>
      <el-form-item>
        <el-button size="small" @click="sendTestNotification" :loading="testing">
          测试发送
        </el-button>
      </el-form-item>

      <!-- 事件分类开关 -->
      <el-divider content-position="left">事件分类开关</el-divider>
      <el-checkbox-group v-model="enabledCategories">
        <el-checkbox label="trade">交易通知（成交/拒绝）</el-checkbox>
        <el-checkbox label="signal">信号通知（策略触发）</el-checkbox>
        <el-checkbox label="task">任务通知（完成/失败）</el-checkbox>
        <el-checkbox label="system">系统通知（异常告警）</el-checkbox>
        <el-checkbox label="risk">风控通知（仓位/资金）</el-checkbox>
      </el-checkbox-group>

      <!-- 策略任务条件 -->
      <el-divider content-position="left">策略任务条件</el-divider>
      <el-form-item label="signal_action 过滤">
        <el-radio-group v-model="ruleConfig.signal_action_filter">
          <el-radio value="">全部</el-radio>
          <el-radio value="trade">仅 trade（直接交易）</el-radio>
          <el-radio value="watch">仅 watch（继续观察）</el-radio>
        </el-radio-group>
        <div class="hint">选择"仅 trade"时，signal_action=watch 的任务不会发送通知</div>
      </el-form-item>

      <!-- 时间限制 -->
      <el-divider content-position="left">时间限制</el-divider>
      <el-form-item>
        <el-checkbox v-model="ruleConfig.trading_hours_only">仅交易时段发送</el-checkbox>
      </el-form-item>
      <el-form-item label="时间范围">
        <el-time-picker v-model="timeRangeStart" placeholder="开始时间" format="HH:mm" />
        <span style="margin: 0 10px;">至</span>
        <el-time-picker v-model="timeRangeEnd" placeholder="结束时间" format="HH:mm" />
      </el-form-item>
    </el-card>
  </div>
</template>

<script setup>
import { ref, onMounted, computed } from 'vue'
import { useRoute } from 'vue-router'
import { ElMessage } from 'element-plus'

const route = useRoute()
const accountId = computed(() => route.params.account_id || localStorage.getItem('current_account_id'))

// 状态
const saving = ref(false)
const testing = ref(false)

// 渠道配置
const channelConfig = ref({
  webhook_url: '',
  channel_type: 'feishu',
  enabled: 1,
})

// 规则配置
const ruleConfig = ref({
  rule_name: '默认规则',
  event_types: [],
  event_categories: [],
  notify_on_trade: 1,
  notify_on_signal: 1,
  notify_on_task: 1,
  notify_on_system: 1,
  notify_on_risk: 1,
  trading_hours_only: 0,
  signal_action_filter: '',
  time_range_start: null,
  time_range_end: null,
  channels: [],
  priority: 1,
  enabled: 1,
})

// 时间范围
const timeRangeStart = ref(null)
const timeRangeEnd = ref(null)

// 分类开关
const enabledCategories = ref(['trade', 'signal', 'system', 'risk'])

// 加载配置
const loadSettings = async () => {
  try {
    const token = localStorage.getItem('auth_token')

    // 加载渠道配置
    const channelRes = await fetch(`/api/v1/ui/${accountId.value}/notification-channels`, {
      headers: { 'Authorization': 'Bearer ' + token }
    })
    const channelData = await channelRes.json()
    if (channelData.success && channelData.data?.length > 0) {
      channelConfig.value = channelData.data[0]
    } else {
      // 从旧配置加载
      const oldRes = await fetch(`/api/v1/ui/${accountId.value}/notifications/config`, {
        headers: { 'Authorization': 'Bearer ' + token }
      })
      const oldData = await oldRes.json()
      if (oldData.success && oldData.data) {
        channelConfig.value.webhook_url = oldData.data.webhook_url || ''
      }
    }

    // 加载规则配置
    const ruleRes = await fetch(`/api/v1/ui/${accountId.value}/notification-rules`, {
      headers: { 'Authorization': 'Bearer ' + token }
    })
    const ruleData = await ruleRes.json()
    if (ruleData.success && ruleData.data?.length > 0) {
      const rule = ruleData.data[0]
      ruleConfig.value = { ...ruleConfig.value, ...rule }

      // 同步分类开关
      enabledCategories.value = []
      if (rule.notify_on_trade) enabledCategories.value.push('trade')
      if (rule.notify_on_signal) enabledCategories.value.push('signal')
      if (rule.notify_on_task) enabledCategories.value.push('task')
      if (rule.notify_on_system) enabledCategories.value.push('system')
      if (rule.notify_on_risk) enabledCategories.value.push('risk')
    }
  } catch (e) {
    ElMessage.warning('加载配置失败')
  }
}

// 保存配置
const saveSettings = async () => {
  saving.value = true
  try {
    const token = localStorage.getItem('auth_token')

    // 同步分类开关到规则配置
    ruleConfig.value.notify_on_trade = enabledCategories.value.includes('trade') ? 1 : 0
    ruleConfig.value.notify_on_signal = enabledCategories.value.includes('signal') ? 1 : 0
    ruleConfig.value.notify_on_task = enabledCategories.value.includes('task') ? 1 : 0
    ruleConfig.value.notify_on_system = enabledCategories.value.includes('system') ? 1 : 0
    ruleConfig.value.notify_on_risk = enabledCategories.value.includes('risk') ? 1 : 0

    // 保存渠道
    if (channelConfig.value.webhook_url) {
      await fetch(`/api/v1/ui/${accountId.value}/notification-channels`, {
        method: 'POST',
        headers: {
          'Authorization': 'Bearer ' + token,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(channelConfig.value)
      })
    }

    // 保存规则
    const rulePayload = {
      ...ruleConfig.value,
      time_range_start: timeRangeStart.value ? formatTime(timeRangeStart.value) : null,
      time_range_end: timeRangeEnd.value ? formatTime(timeRangeEnd.value) : null,
    }
    await fetch(`/api/v1/ui/${accountId.value}/notification-rules`, {
      method: 'POST',
      headers: {
        'Authorization': 'Bearer ' + token,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(rulePayload)
    })

    ElMessage.success('配置已保存')
  } catch (e) {
    ElMessage.error('保存失败')
  } finally {
    saving.value = false
  }
}

// 发送测试通知
const sendTestNotification = async () => {
  if (!channelConfig.value.webhook_url) {
    ElMessage.warning('请先填写飞书 Webhook URL')
    return
  }

  testing.value = true
  try {
    const token = localStorage.getItem('auth_token')

    // 先保存渠道
    await fetch(`/api/v1/ui/${accountId.value}/notification-channels`, {
      method: 'POST',
      headers: {
        'Authorization': 'Bearer ' + token,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(channelConfig.value)
    })

    // 发送测试通知
    const res = await fetch(`/api/v1/ui/${accountId.value}/notifications/test`, {
      method: 'POST',
      headers: { 'Authorization': 'Bearer ' + token }
    })
    const data = await res.json()
    if (data.success) {
      ElMessage.success('测试通知已发送，请检查飞书群')
    } else {
      ElMessage.error(data.message || '发送失败')
    }
  } catch (e) {
    ElMessage.error('发送失败')
  } finally {
    testing.value = false
  }
}

// 格式化时间
const formatTime = (date) => {
  if (!date) return null
  const h = date.getHours().toString().padStart(2, '0')
  const m = date.getMinutes().toString().padStart(2, '0')
  return `${h}:${m}`
}

onMounted(() => {
  loadSettings()
})
</script>

<style scoped>
.notification-settings {
  padding: 20px;
}

.settings-card {
  max-width: 800px;
}

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.hint {
  color: #909399;
  font-size: 12px;
  margin-top: 5px;
}

.el-checkbox-group {
  display: flex;
  flex-direction: column;
  gap: 10px;
}
</style>