<template>
  <div class="layout-container">
    <NavBar />
    <div class="account-management">
      <div class="page-header">
        <h1>账户管理</h1>
        <el-button type="primary" @click="showCreateDialog = true">
          <i class="el-icon-plus"></i> 新增账户
        </el-button>
      </div>

      <!-- 账户列表 -->
      <el-card class="account-list">
        <div class="filter-section">
          <el-input
            v-model="searchQuery"
            placeholder="搜索账户名称或用户名..."
            style="width: 300px"
            clearable
            @input="handleSearch"
          >
            <template #prefix>
              <i class="el-icon-search"></i>
            </template>
          </el-input>
          <el-select v-model="statusFilter" placeholder="账户状态" style="width: 150px; margin-left: 10px" clearable @change="handleSearch">
            <el-option label="激活" :value="1" />
            <el-option label="禁用" :value="0" />
          </el-select>
        </div>

        <el-table :data="filteredAccounts" style="width: 100%" v-loading="loading">
          <el-table-column prop="account_id" label="账户 ID" width="100" />
          <el-table-column prop="name" label="用户名/账户名" width="150" />
          <el-table-column prop="display_name" label="显示名称" width="200" />
          <el-table-column prop="available_cash" label="可用资金" width="150">
            <template #default="{ row }">
              <span class="money-value">¥ {{ formatMoney(row.available_cash) }}</span>
            </template>
          </el-table-column>
          <el-table-column label="状态" width="100">
            <template #default="{ row }">
              <el-tag :type="row.is_active ? 'success' : 'danger'">
                {{ row.is_active ? '激活' : '禁用' }}
              </el-tag>
            </template>
          </el-table-column>
          <el-table-column prop="created_at" label="创建时间" width="180">
            <template #default="{ row }">
              {{ formatDate(row.created_at) }}
            </template>
          </el-table-column>
          <el-table-column label="操作" fixed="right" width="200">
            <template #default="{ row }">
              <el-button size="small" @click="handleEdit(row)">编辑</el-button>
              <el-button size="small" type="danger" @click="handleDelete(row)">删除</el-button>
            </template>
          </el-table-column>
        </el-table>

        <div class="pagination-section" v-if="accounts.length > 0">
          <el-pagination
            layout="total, sizes, prev, pager, next, jumper"
            :total="accounts.length"
            :page-size="pageSize"
            :current-page="currentPage"
            @size-change="handleSizeChange"
            @current-change="handleCurrentChange"
          />
        </div>
      </el-card>

      <!-- 统计信息 -->
      <el-row :gutter="20" style="margin-top: 20px">
        <el-col :span="8">
          <el-card>
            <template #header>
              <span>总账户数</span>
            </template>
            <div class="stat-value">{{ stats.total_accounts || 0 }}</div>
          </el-card>
        </el-col>
        <el-col :span="8">
          <el-card>
            <template #header>
              <span>激活账户</span>
            </template>
            <div class="stat-value success">{{ stats.active_accounts || 0 }}</div>
          </el-card>
        </el-col>
        <el-col :span="8">
          <el-card>
            <template #header>
              <span>禁用账户</span>
            </template>
            <div class="stat-value danger">{{ stats.inactive_accounts || 0 }}</div>
          </el-card>
        </el-col>
      </el-row>

      <!-- 创建/编辑账户对话框 -->
      <el-dialog
        v-model="showCreateDialog"
        :title="editingAccount ? '编辑账户' : '创建账户'"
        width="600px"
        @close="resetForm"
      >
        <el-form :model="accountForm" :rules="rules" ref="accountFormRef" label-width="120px">
          <el-form-item label="用户名/账户名" prop="name">
            <el-input v-model="accountForm.name" placeholder="唯一标识，用于登录" />
          </el-form-item>
          <el-form-item label="密码" :prop="editingAccount ? '' : 'password'" :required="!editingAccount">
            <el-input
              v-model="accountForm.password"
              type="password"
              :placeholder="editingAccount ? '留空则不修改' : '请输入密码'"
              show-password
            />
          </el-form-item>
          <el-form-item label="显示名称" prop="display_name">
            <el-input v-model="accountForm.display_name" placeholder="显示在界面上的名称" />
          </el-form-item>
          <el-form-item label="可用资金" prop="available_cash">
            <el-input-number
              v-model="accountForm.available_cash"
              :min="0"
              :precision="2"
              :step="1000"
              placeholder="0.00"
              style="width: 100%"
            />
          </el-form-item>

          <el-divider content-position="left">银河证券账户信息</el-divider>

          <el-form-item label="资金账号" prop="broker_account">
            <el-input v-model="accountForm.broker_account" placeholder="银河证券资金账号" />
          </el-form-item>
          <el-form-item label="资金密码" prop="broker_password">
            <el-input
              v-model="accountForm.broker_password"
              type="password"
              placeholder="银河证券资金密码"
              show-password
            />
          </el-form-item>
          <el-form-item label="开户券商" prop="broker_company">
            <el-input v-model="accountForm.broker_company" placeholder="如：银河证券北京分公司" />
          </el-form-item>
          <el-form-item label="服务器 IP" prop="broker_server_ip">
            <el-input v-model="accountForm.broker_server_ip" placeholder="如：101.230.159.234" />
          </el-form-item>
          <el-form-item label="服务器端口" prop="broker_server_port">
            <el-input-number v-model="accountForm.broker_server_port" :min="1" :max="65535" placeholder="8600" />
          </el-form-item>
          <el-form-item label="账户状态" prop="broker_status">
            <el-select v-model="accountForm.broker_status" placeholder="请选择账户状态">
              <el-option label="正常" value="normal" />
              <el-option label="冻结" value="frozen" />
              <el-option label="销户" value="closed" />
            </el-select>
          </el-form-item>

          <el-divider content-position="left">其他</el-divider>

          <el-form-item label="备注" prop="notes">
            <el-input
              v-model="accountForm.notes"
              type="textarea"
              :rows="3"
              placeholder="备注信息"
            />
          </el-form-item>
          <el-form-item label="状态" prop="is_active">
            <el-switch v-model="accountForm.is_active" :active-value="1" :inactive-value="0" />
          </el-form-item>
        </el-form>
        <template #footer>
          <el-button @click="showCreateDialog = false">取消</el-button>
          <el-button type="primary" @click="handleSubmit" :loading="submitting">
            {{ editingAccount ? '保存' : '创建' }}
          </el-button>
        </template>
      </el-dialog>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import NavBar from '../components/NavBar.vue'

const API_BASE = '/api/v1/ui/accounts'

// 调试日志
console.log('账户管理组件已加载')

// 状态
const loading = ref(false)
const submitting = ref(false)
const accounts = ref([])
const stats = ref({})
const searchQuery = ref('')
const statusFilter = ref(null)
const currentPage = ref(1)
const pageSize = ref(10)
const showCreateDialog = ref(false)
const editingAccount = ref(null)
const accountFormRef = ref(null)

// 表单数据
const accountForm = ref({
  name: '',
  password: '',
  display_name: '',
  available_cash: 0,
  broker_account: '',
  broker_password: '',
  broker_company: '',
  broker_server_ip: '',
  broker_server_port: 8600,
  broker_status: 'normal',
  notes: '',
  is_active: 1
})

// 表单验证规则
const rules = {
  name: [{ required: true, message: '请输入用户名/账户名', trigger: 'blur' }],
  password: [{ required: true, message: '请输入密码', trigger: 'blur', min: 6 }]
}

// 过滤后的账户列表
const filteredAccounts = computed(() => {
  let result = accounts.value
  if (searchQuery.value) {
    const query = searchQuery.value.toLowerCase()
    result = result.filter(acc =>
      acc.name.toLowerCase().includes(query) ||
      acc.display_name.toLowerCase().includes(query)
    )
  }
  if (statusFilter.value !== null && statusFilter.value !== undefined) {
    result = result.filter(acc => acc.is_active === statusFilter.value)
  }
  return result
})

// 加载账户列表
const loadAccounts = async () => {
  console.log('开始加载账户列表...')
  loading.value = true
  try {
    const res = await fetch(API_BASE)
    console.log('响应状态:', res.status)
    if (!res.ok) {
      throw new Error(`HTTP 错误：${res.status}`)
    }
    const data = await res.json()
    console.log('响应数据:', data)
    if (data.success) {
      accounts.value = data.data
      console.log('账户列表加载成功，数量:', data.data.length)
    } else {
      ElMessage.error('加载失败：' + data.message)
    }
  } catch (error) {
    console.error('加载账户列表错误:', error)
    ElMessage.error('加载账户列表失败：' + error.message)
  } finally {
    loading.value = false
  }
}

// 加载统计信息 - 从账户列表计算
const loadStats = () => {
  const total = accounts.value.length
  const active = accounts.value.filter(a => a.is_active).length
  stats.value = {
    total_accounts: total,
    active_accounts: active,
    inactive_accounts: total - active
  }
}

// 搜索处理
const handleSearch = () => {
  currentPage.value = 1
}

// 分页处理
const handleSizeChange = (size) => {
  pageSize.value = size
}

const handleCurrentChange = (page) => {
  currentPage.value = page
}

// 格式化日期
const formatDate = (dateStr) => {
  if (!dateStr) return ''
  const date = new Date(dateStr)
  return date.toLocaleString('zh-CN')
}

// 格式化金额
const formatMoney = (value) => {
  if (value === null || value === undefined) return '0.00'
  return Number(value).toLocaleString('zh-CN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })
}

// 重置表单
const resetForm = () => {
  editingAccount.value = null
  accountForm.value = {
    name: '',
    password: '',
    display_name: '',
    available_cash: 0,
    broker_account: '',
    broker_password: '',
    broker_company: '',
    broker_server_ip: '',
    broker_server_port: 8600,
    broker_status: 'normal',
    notes: '',
    is_active: 1
  }
  if (accountFormRef.value) {
    accountFormRef.value.clearValidate()
  }
}

// 编辑账户
const handleEdit = (row) => {
  editingAccount.value = row
  accountForm.value = {
    name: row.name,
    password: '',
    display_name: row.display_name,
    available_cash: row.available_cash || 0,
    broker_account: row.broker_account || '',
    broker_password: '',
    broker_company: row.broker_company || '',
    broker_server_ip: row.broker_server_ip || '',
    broker_server_port: row.broker_server_port || 8600,
    broker_status: row.broker_status || 'normal',
    notes: row.notes || '',
    is_active: row.is_active
  }
  showCreateDialog.value = true
}
const handleDelete = async (row) => {
  try {
    await ElMessageBox.confirm(`确定要删除账户 "${row.name}" 吗？`, '确认删除', {
      confirmButtonText: '确定',
      cancelButtonText: '取消',
      type: 'warning'
    })

    const res = await fetch(`${API_BASE}/${row.account_id}`, {
      method: 'DELETE'
    })
    const data = await res.json()

    if (data.success) {
      ElMessage.success('账户已删除')
      loadAccounts()
      loadStats()
    } else {
      ElMessage.error('删除失败：' + data.message)
    }
  } catch (error) {
    if (error !== 'cancel') {
      ElMessage.error('删除失败：' + error.message)
    }
  }
}

// 提交表单
const handleSubmit = async () => {
  if (!accountFormRef.value) return

  await accountFormRef.value.validate(async (valid) => {
    if (!valid) return

    submitting.value = true
    try {
      const submitData = { ...accountForm.value }
      if (editingAccount.value && !submitData.password) {
        delete submitData.password
      }

      let url, method
      if (editingAccount.value) {
        url = `${API_BASE}/${editingAccount.value.account_id}`
        method = 'PUT'
      } else {
        url = `${API_BASE}/create`
        method = 'POST'
      }

      const res = await fetch(url, {
        method,
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(submitData)
      })
      const data = await res.json()

      if (data.success) {
        ElMessage.success(editingAccount.value ? '账户已更新' : '账户已创建')
        showCreateDialog.value = false
        loadAccounts()
        loadStats()
      } else {
        ElMessage.error(editingAccount.value ? '更新失败：' + data.message : '创建失败：' + data.message)
      }
    } catch (error) {
      ElMessage.error('操作失败：' + error.message)
    } finally {
      submitting.value = false
    }
  })
}

// 初始化
onMounted(() => {
  console.log('onMounted 执行，开始加载数据...')
  loadAccounts()
  loadStats()
})
</script>

<style scoped>
.layout-container {
  height: 100%;
  display: flex;
  flex-direction: column;
}

.account-management {
  padding: 20px;
  flex: 1;
  overflow-y: auto;
  background-color: #f5f7fa;
}

.page-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 20px;
  background-color: #fff;
  padding: 15px 20px;
  border-radius: 4px;
}

.page-header h1 {
  font-size: 20px;
  margin: 0;
  color: #303133;
}

.account-list {
  margin-bottom: 20px;
  background-color: #fff;
}

.filter-section {
  margin-bottom: 20px;
  padding: 15px 0;
}

.pagination-section {
  margin-top: 20px;
  display: flex;
  justify-content: flex-end;
  padding: 10px 0;
}

.stat-value {
  font-size: 32px;
  font-weight: bold;
  text-align: center;
  padding: 10px 0;
}

.stat-value.success {
  color: #67c23a;
}

.stat-value.danger {
  color: #f56c6c;
}
</style>
