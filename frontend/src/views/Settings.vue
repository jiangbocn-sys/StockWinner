<template>
  <div class="layout-container">
    <NavBar />
    <el-main class="main-content">
      <h2>系统设置 - {{ currentAccount?.display_name }}</h2>

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

      <el-card style="margin-top: 20px">
        <template #header>
          <div class="card-header">
            <span>Claude API 配置</span>
            <el-tag :type="llmConfig.configured ? 'success' : 'danger'">
              {{ llmConfig.configured ? '已配置' : '未配置' }}
            </el-tag>
          </div>
        </template>

        <el-alert
          title="如何获取 API 密钥"
          type="info"
          :closable="false"
          style="margin-bottom: 20px;"
        >
          <ol style="padding-left: 20px; margin: 10px 0;">
            <li>访问 <a href="https://console.anthropic.com/" target="_blank">Anthropic Console</a></li>
            <li>登录或注册账号</li>
            <li>进入 API Keys 页面</li>
            <li>创建新的 API 密钥</li>
            <li>复制密钥并粘贴到下方输入框</li>
          </ol>
        </el-alert>

        <el-form label-width="150px">
          <el-form-item label="API 密钥">
            <el-input
              v-model="form.api_key"
              type="password"
              placeholder="输入 Claude API 密钥"
              show-password
              style="width: 400px;"
            />
          </el-form-item>
          <el-form-item>
            <el-button type="primary" @click="saveConfig" :loading="saving">
              保存配置
            </el-button>
            <el-button @click="testConnection" :loading="testing">
              测试连接
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

const llmConfig = reactive({
  configured: false
})

const form = reactive({
  api_key: ''
})

const saving = ref(false)
const testing = ref(false)

// 加载 LLM 配置
const loadLLMConfig = async () => {
  try {
    const res = await fetch('/api/v1/ui/llm/config')
    const data = await res.json()
    if (data.success) {
      llmConfig.configured = data.data.configured
    }
  } catch (error) {
    console.error('加载 LLM 配置失败:', error)
  }
}

// 保存配置
const saveConfig = async () => {
  if (!form.api_key) {
    ElMessage.warning('请输入 API 密钥')
    return
  }

  saving.value = true
  try {
    const res = await fetch('/api/v1/ui/llm/config', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ api_key: form.api_key })
    })
    const data = await res.json()
    if (data.success) {
      ElMessage.success(data.message)
      llmConfig.configured = true
      form.api_key = ''
    } else {
      ElMessage.error(data.error || '保存失败')
    }
  } catch (error) {
    ElMessage.error('保存失败：' + error.message)
  } finally {
    saving.value = false
  }
}

// 测试连接
const testConnection = async () => {
  if (!form.api_key) {
    ElMessage.warning('请输入 API 密钥')
    return
  }

  testing.value = true
  try {
    const res = await fetch('/api/v1/ui/llm/config', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ api_key: form.api_key })
    })
    const data = await res.json()
    if (data.success) {
      if (data.api_valid) {
        ElMessage.success('API 密钥有效，连接成功！')
      } else {
        ElMessage.warning('API 密钥可能无效，但配置已保存')
      }
    } else {
      ElMessage.error(data.error || '测试失败')
    }
  } catch (error) {
    ElMessage.error('测试失败：' + error.message)
  } finally {
    testing.value = false
  }
}

onMounted(() => {
  loadLLMConfig()
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
