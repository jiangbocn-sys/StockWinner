<template>
  <div class="password-change-container">
    <el-card class="password-card" style="max-width: 500px; margin: 20px auto;">
      <template #header>
        <div class="card-header">
          <span>修改密码</span>
        </div>
      </template>

      <el-form :model="passwordForm" :rules="rules" ref="passwordFormRef" label-width="100px">
        <el-form-item label="旧密码" prop="old_password">
          <el-input
            v-model="passwordForm.old_password"
            type="password"
            placeholder="请输入当前密码"
            show-password
          />
        </el-form-item>

        <el-form-item label="新密码" prop="new_password">
          <el-input
            v-model="passwordForm.new_password"
            type="password"
            placeholder="请输入新密码（至少 6 位）"
            show-password
          />
        </el-form-item>

        <el-form-item label="确认密码" prop="confirm_password">
          <el-input
            v-model="passwordForm.confirm_password"
            type="password"
            placeholder="请再次输入新密码"
            show-password
          />
        </el-form-item>

        <el-form-item>
          <el-button type="primary" @click="handleChangePassword" :loading="loading" style="width: 100%">
            确认修改
          </el-button>
        </el-form-item>
      </el-form>
    </el-card>
  </div>
</template>

<script setup>
import { ref, reactive } from 'vue'
import { ElMessage } from 'element-plus'

const passwordFormRef = ref(null)
const loading = ref(false)

const passwordForm = reactive({
  old_password: '',
  new_password: '',
  confirm_password: ''
})

const validateConfirmPassword = (rule, value, callback) => {
  if (value !== passwordForm.new_password) {
    callback(new Error('两次输入的密码不一致'))
  } else {
    callback()
  }
}

const rules = {
  old_password: [{ required: true, message: '请输入旧密码', trigger: 'blur' }],
  new_password: [
    { required: true, message: '请输入新密码', trigger: 'blur' },
    { min: 6, message: '密码至少需要 6 位', trigger: 'blur' }
  ],
  confirm_password: [
    { required: true, message: '请确认新密码', trigger: 'blur' },
    { validator: validateConfirmPassword, trigger: 'blur' }
  ]
}

const handleChangePassword = async () => {
  if (!passwordFormRef.value) return

  await passwordFormRef.value.validate(async (valid) => {
    if (!valid) return

    loading.value = true

    try {
      const token = localStorage.getItem('auth_token')

      const res = await fetch('/api/auth/password', {
        method: 'PUT',
        headers: {
          'Content-Type': 'application/json',
          'X-Auth-Token': token
        },
        body: JSON.stringify({
          old_password: passwordForm.old_password,
          new_password: passwordForm.new_password
        })
      })

      const data = await res.json()

      if (data.success || res.ok) {
        ElMessage.success('密码修改成功，请重新登录')
        // 清除本地存储
        localStorage.removeItem('auth_token')
        localStorage.removeItem('current_user')
        // 跳转到登录页
        setTimeout(() => {
          window.location.href = '/ui/#/login'
        }, 1500)
      } else {
        ElMessage.error('修改失败：' + (data.detail || data.message))
      }
    } catch (error) {
      ElMessage.error('修改失败：' + error.message)
    } finally {
      loading.value = false
    }
  })
}
</script>

<style scoped>
.password-change-container {
  min-height: calc(100vh - 60px);
  background-color: #f5f7fa;
  padding-top: 20px;
}

.card-header {
  font-size: 16px;
  font-weight: bold;
}
</style>
