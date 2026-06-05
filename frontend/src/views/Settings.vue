<template>
  <div class="layout-container">
    <NavBar />
    <el-main class="main-content">
      <h2>系统设置 - {{ currentAccount?.display_name }}
        <el-tag :type="isAdmin ? 'danger' : 'info'" size="small" style="margin-left: 10px;">
          {{ isAdmin ? '管理员' : '普通用户' }}
        </el-tag>
      </h2>

      <el-tabs v-model="activeTab" type="card">
        <!-- 个人设置（所有用户可见） -->
        <el-tab-pane label="个人设置" name="personal">
          <!-- 个人信息编辑 -->
          <el-card>
            <template #header>
              <div class="card-header">
                <span>个人信息</span>
                <el-button size="small" type="primary" @click="saveProfile" :loading="profileSaving">保存</el-button>
              </div>
            </template>

            <el-form label-width="120px">
              <el-form-item label="显示名称">
                <el-input v-model="profileForm.display_name" style="width: 300px" />
              </el-form-item>
              <el-form-item label="可用资金">
                <el-input-number v-model="profileForm.available_cash" :min="0" :precision="2" :step="1000" style="width: 200px" />
              </el-form-item>

              <el-divider content-position="left">通知设置</el-divider>

              <el-form-item label="暂停通知">
                <el-switch v-model="profileForm.notifications_paused" :active-value="1" :inactive-value="0" />
                <span class="field-hint" style="margin-left: 8px">开启后暂停所有飞书通知（测试时使用）</span>
              </el-form-item>

              <el-divider content-position="left">交易成本参数</el-divider>

              <el-form-item label="佣金费率">
                <el-input-number v-model="profileForm.commission_rate" :min="0" :precision="6" :step="0.0001" style="width: 200px" />
                <span class="field-hint">默认 0.0003（万分三）</span>
              </el-form-item>
              <el-form-item label="印花税">
                <el-input-number v-model="profileForm.stamp_tax" :min="0" :precision="6" :step="0.0001" style="width: 200px" />
                <span class="field-hint">默认 0.0005（万分五），仅卖出</span>
              </el-form-item>
              <el-form-item label="过户费">
                <el-input-number v-model="profileForm.transfer_fee" :min="0" :precision="6" :step="0.00001" style="width: 200px" />
                <span class="field-hint">默认 0.00002</span>
              </el-form-item>
              <el-form-item label="最低佣金">
                <el-input-number v-model="profileForm.min_commission" :min="0" :precision="2" :step="1" style="width: 200px" />
                <span class="field-hint">默认 5 元</span>
              </el-form-item>

              <el-divider content-position="left">券商账户信息</el-divider>

              <el-form-item label="资金账号">
                <el-input v-model="profileForm.broker_account" placeholder="银河证券资金账号" style="width: 300px" />
              </el-form-item>
              <el-form-item label="资金密码">
                <el-input v-model="profileForm.broker_password" type="password" placeholder="银河证券资金密码" show-password style="width: 300px" />
              </el-form-item>
              <el-form-item label="开户券商">
                <el-input v-model="profileForm.broker_company" placeholder="如：银河证券北京分公司" style="width: 300px" />
              </el-form-item>
              <el-form-item label="服务器 IP">
                <el-input v-model="profileForm.broker_server_ip" placeholder="如：101.230.159.234" style="width: 200px" />
              </el-form-item>
              <el-form-item label="服务器端口">
                <el-input-number v-model="profileForm.broker_server_port" :min="1" :max="65535" style="width: 150px" />
              </el-form-item>
              <el-form-item label="备注">
                <el-input v-model="profileForm.notes" type="textarea" :rows="2" style="width: 400px" />
              </el-form-item>
            </el-form>
          </el-card>

          <!-- 修改密码 -->
          <el-card style="margin-top: 20px">
            <template #header>
              <span>修改密码</span>
            </template>
            <el-form label-width="120px">
              <el-form-item label="旧密码">
                <el-input v-model="passwordForm.old_password" type="password" show-password style="width: 300px" />
              </el-form-item>
              <el-form-item label="新密码">
                <el-input v-model="passwordForm.new_password" type="password" show-password style="width: 300px" />
              </el-form-item>
              <el-form-item>
                <el-button type="primary" @click="changePassword" :loading="passwordSaving">修改密码</el-button>
              </el-form-item>
            </el-form>
          </el-card>

          <!-- 飞书 Webhook -->
          <el-card style="margin-top: 20px">
            <template #header>
              <div class="card-header">
                <span>飞书 Webhook 通知</span>
                <el-tag :type="notificationConfigured ? 'success' : 'warning'" size="small">
                  {{ notificationConfigured ? '已配置' : '未配置' }}
                </el-tag>
              </div>
            </template>

            <el-form label-width="120px">
              <el-form-item label="Webhook URL">
                <el-input v-model="notifForm.webhook_url" placeholder="https://open.feishu.cn/open-apis/bot/v2/hook/..." style="width: 500px" />
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
                <el-button type="primary" @click="saveNotification" :loading="notifSaving">保存配置</el-button>
                <el-button @click="testNotification" :loading="notifTesting" :disabled="!notificationConfigured">发送测试</el-button>
              </el-form-item>
            </el-form>
          </el-card>

          <!-- LLM API 配置 -->
          <el-card style="margin-top: 20px">
            <template #header>
              <div class="card-header">
                <span>LLM API 配置</span>
                <el-tag :type="llmConfig.configured ? 'success' : 'danger'" size="small">
                  {{ llmConfig.configured ? '已配置' : '未配置' }}
                </el-tag>
              </div>
            </template>

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
                <el-input v-model="llmForm.base_url" placeholder="https://api.example.com/v1/chat/completions" style="width: 500px" />
              </el-form-item>
              <el-form-item label="API Key">
                <el-input v-model="llmForm.api_key" type="password" show-password style="width: 400px" />
                <span v-if="llmConfig.api_key_masked" style="margin-left: 10px; color: #999; font-size: 12px;">当前: {{ llmConfig.api_key_masked }}</span>
              </el-form-item>
              <el-form-item label="模型名称">
                <el-input v-model="llmForm.model_name" placeholder="gpt-4o, qwen-plus" style="width: 300px" />
              </el-form-item>
              <el-form-item>
                <el-button type="primary" @click="saveLLMConfig" :loading="saving">保存配置</el-button>
                <el-button @click="testLLMConnection" :loading="testing">测试连接</el-button>
                <el-button type="danger" @click="deleteLLMConfig" :loading="deleting" v-if="llmConfig.configured">删除配置</el-button>
              </el-form-item>
            </el-form>
          </el-card>

          <!-- 个人 Agent 管理 -->
          <el-card style="margin-top: 20px">
            <template #header>
              <div class="card-header">
                <span>我的 AI Agent</span>
                <el-button size="small" type="primary" @click="showCreateDialog">+ 创建 Agent</el-button>
              </div>
            </template>

            <el-table :data="agents" size="small">
              <el-table-column prop="agent_id" label="ID" width="100">
                <template #default="{ row }"><code style="font-size: 11px">{{ row.agent_id.substring(0, 8) }}...</code></template>
              </el-table-column>
              <el-table-column prop="name" label="名称" width="120" />
              <el-table-column prop="agent_type" label="类型" width="100" />
              <el-table-column prop="role" label="角色" width="80">
                <template #default="{ row }"><el-tag :type="roleTagType(row.role)" size="small">{{ row.role }}</el-tag></template>
              </el-table-column>
              <el-table-column prop="last_used_at" label="最后使用" width="140" />
              <el-table-column label="操作" width="180">
                <template #default="{ row }">
                  <el-button size="small" @click="showKeyDialog(row)">查看Key</el-button>
                  <el-button size="small" type="danger" @click="deleteAgent(row)">删除</el-button>
                </template>
              </el-table-column>
            </el-table>
          </el-card>
        </el-tab-pane>

        <!-- 数据源管理（管理员可见） -->
        <el-tab-pane label="数据源管理" name="datasources" v-if="isAdmin">
          <el-card>
            <template #header>
              <div class="card-header">
                <span>数据源管理</span>
                <div>
                  <el-button size="small" @click="openPriorityDialog">调整优先级</el-button>
                  <el-button size="small" @click="loadDataSources" :loading="dsLoading">刷新</el-button>
                </div>
              </div>
            </template>

            <el-table :data="dataSources" border>
              <el-table-column prop="display_name" label="数据源" width="130" />
              <el-table-column label="启用" width="80">
                <template #default="{ row }">
                  <el-switch v-model="row.is_enabled" @change="toggleDataSource(row)" :loading="row._toggling" />
                </template>
              </el-table-column>
              <el-table-column label="状态" width="100">
                <template #default="{ row }">
                  <el-tag :type="dsStatusTag(row.status)" size="small">{{ dsStatusText(row.status) }}</el-tag>
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
                  <span v-else style="color: #c0c4cc">无需配置</span>
                </template>
              </el-table-column>
            </el-table>
          </el-card>
        </el-tab-pane>

        <!-- Agent 功能权限（管理员可见） -->
        <el-tab-pane label="Agent功能权限" name="agentperms" v-if="isAdmin">
          <el-card>
            <template #header>
              <span>Agent 权限矩阵</span>
            </template>
            <el-alert type="info" :closable="false" style="margin-bottom: 20px">
              定义不同角色的 Agent 可以访问哪些 API 端点。权限按角色层级继承。
            </el-alert>

            <el-table :data="agentPermissions" border>
              <el-table-column prop="permission" label="权限" width="200" />
              <el-table-column label="viewer" width="80">
                <template #default="{ row }"><el-tag size="small" :type="row.viewer ? 'success' : 'danger'">{{ row.viewer ? '✓' : '-' }}</el-tag></template>
              </el-table-column>
              <el-table-column label="strategist" width="100">
                <template #default="{ row }"><el-tag size="small" :type="row.strategist ? 'success' : 'danger'">{{ row.strategist ? '✓' : '-' }}</el-tag></template>
              </el-table-column>
              <el-table-column label="operator" width="80">
                <template #default="{ row }"><el-tag size="small" :type="row.operator ? 'success' : 'danger'">{{ row.operator ? '✓' : '-' }}</el-tag></template>
              </el-table-column>
              <el-table-column label="admin" width="80">
                <template #default="{ row }"><el-tag size="small" :type="row.admin ? 'success' : 'danger'">{{ row.admin ? '✓' : '-' }}</el-tag></template>
              </el-table-column>
            </el-table>

            <div style="margin-top: 15px; color: #909399; font-size: 12px">
              注：权限矩阵在 services/agent/models.py 中定义，修改需更新代码。
            </div>
          </el-card>
        </el-tab-pane>

        <!-- 用户管理（管理员可见） -->
        <el-tab-pane label="用户管理" name="users" v-if="isAdmin">
          <el-card>
            <template #header>
              <div class="card-header">
                <span>用户列表</span>
                <el-button type="primary" size="small" @click="showUserCreateDialog">+ 创建用户</el-button>
              </div>
            </template>

            <el-table :data="users" border>
              <el-table-column prop="account_id" label="账户 ID" width="100" />
              <el-table-column prop="name" label="用户名" width="120" />
              <el-table-column prop="display_name" label="显示名称" width="150" />
              <el-table-column prop="role" label="角色" width="100">
                <template #default="{ row }">
                  <el-tag :type="row.role === 'admin' ? 'danger' : 'info'" size="small">{{ row.role === 'admin' ? '管理员' : '普通用户' }}</el-tag>
                </template>
              </el-table-column>
              <el-table-column label="状态" width="80">
                <template #default="{ row }">
                  <el-tag :type="row.is_active ? 'success' : 'danger'" size="small">{{ row.is_active ? '激活' : '锁定' }}</el-tag>
                </template>
              </el-table-column>
              <el-table-column prop="created_at" label="创建时间" width="160" />
              <el-table-column label="操作" width="220">
                <template #default="{ row }">
                  <el-button size="small" @click="showUserEditDialog(row)" :disabled="row.account_id === currentAccount?.account_id">编辑</el-button>
                  <el-button size="small" :type="row.is_active ? 'warning' : 'success'" @click="toggleUserStatus(row)" :disabled="row.account_id === currentAccount?.account_id">
                    {{ row.is_active ? '锁定' : '解锁' }}
                  </el-button>
                  <el-button size="small" type="danger" @click="deleteUser(row)" :disabled="row.account_id === currentAccount?.account_id">删除</el-button>
                </template>
              </el-table-column>
            </el-table>
          </el-card>
        </el-tab-pane>

        <!-- 系统配置（管理员） -->
        <el-tab-pane label="系统配置" name="system" v-if="isAdmin">
          <el-card>
            <el-descriptions title="缓存与监控参数" :column="2" border>
              <el-descriptions-item label="交易时段 TTL">
                <el-input-number v-model="systemConfig.price_cache_ttl_trading" :min="60" :max="3600" :step="30" size="small" />
                <span style="color: #909399; font-size: 12px; margin-left: 8px">秒 (60-3600)</span>
              </el-descriptions-item>
              <el-descriptions-item label="非交易时段 TTL">
                <el-input-number v-model="systemConfig.price_cache_ttl_non_trading" :min="3600" :max="86400" :step="3600" size="small" />
                <span style="color: #909399; font-size: 12px; margin-left: 8px">秒 (1-24小时)</span>
              </el-descriptions-item>
              <el-descriptions-item label="缓存容量上限">
                <el-input-number v-model="systemConfig.price_cache_max_size" :min="1000" :max="50000" :step="1000" size="small" />
                <span style="color: #909399; font-size: 12px; margin-left: 8px">条</span>
              </el-descriptions-item>
              <el-descriptions-item label="刷盘间隔">
                <el-input-number v-model="systemConfig.price_cache_flush_interval" :min="300" :max="3600" :step="60" size="small" />
                <span style="color: #909399; font-size: 12px; margin-left: 8px">秒</span>
              </el-descriptions-item>
              <el-descriptions-item label="监控循环间隔">
                <el-input-number v-model="systemConfig.monitor_interval" :min="30" :max="300" :step="10" size="small" />
                <span style="color: #909399; font-size: 12px; margin-left: 8px">秒</span>
              </el-descriptions-item>
              <el-descriptions-item label="自动启动监控">
                <el-switch v-model="systemConfig.auto_start_monitor" />
              </el-descriptions-item>
            </el-descriptions>
            <el-descriptions title="熔断器参数" :column="2" border style="margin-top: 20px">
              <el-descriptions-item label="恢复超时">
                <el-input-number v-model="systemConfig.circuit_breaker_recovery_timeout" :min="60" :max="600" :step="30" size="small" />
                <span style="color: #909399; font-size: 12px; margin-left: 8px">秒</span>
              </el-descriptions-item>
              <el-descriptions-item label="失败阈值">
                <el-input-number v-model="systemConfig.circuit_breaker_failure_threshold" :min="1" :max="20" :step="1" size="small" />
                <span style="color: #909399; font-size: 12px; margin-left: 8px">次</span>
              </el-descriptions-item>
            </el-descriptions>
            <div style="margin-top: 20px">
              <el-button type="primary" @click="saveSystemConfig" :loading="configSaving">保存配置</el-button>
              <el-button @click="resetSystemConfig">恢复默认</el-button>
            </div>
          </el-card>
        </el-tab-pane>
      </el-tabs>
    </el-main>

    <!-- 创建 Agent 对话框 -->
    <el-dialog v-model="createVisible" title="创建 AI Agent" width="450px">
      <el-form label-width="100px">
        <el-form-item label="Agent 名称"><el-input v-model="createForm.name" placeholder="如: OpenClaw-Agent" /></el-form-item>
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
    <el-dialog v-model="keyVisible" title="查看 API Key" width="500px">
      <div v-if="newApiKey">
        <el-alert type="warning" :closable="false" style="margin-bottom: 15px"><b>请妥善保存此 API Key，关闭后不会再次显示</b></el-alert>
        <el-input v-model="newApiKey" readonly>
          <template #append><el-button @click="copyKey">复制</el-button></template>
        </el-input>
      </div>
      <div v-else>
        <p style="color: #999">系统不存储明文 API Key，如需新 Key 请点击"重置"。</p>
      </div>
      <template #footer>
        <el-button v-if="keyResult" @click="resetKey(keyResult)" :loading="rotating">重置 Key</el-button>
        <el-button @click="keyVisible = false">关闭</el-button>
      </template>
    </el-dialog>

    <!-- 数据源配置对话框 -->
    <el-dialog v-model="dsConfigVisible" title="配置数据源" width="500px">
      <el-form label-width="120px">
        <el-form-item v-if="dsConfigForm.provider_id === 'tushare'" label="API Token">
          <el-input v-model="dsConfigForm.api_token" type="password" show-password />
        </el-form-item>
        <el-form-item v-if="!dsConfigForm.requires_config" label="配置"><span style="color: #909399">该数据源无需配置</span></el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="dsConfigVisible = false">取消</el-button>
        <el-button type="primary" @click="saveDsConfig" :loading="dsSaving">保存</el-button>
      </template>
    </el-dialog>

    <!-- 通道优先级对话框 -->
    <el-dialog v-model="priorityVisible" title="调整通道优先级" width="550px">
      <el-tabs v-model="priorityTab">
        <el-tab-pane label="交易通道" name="trading">
          <el-table :data="priorityForm.trading" border>
            <el-table-column type="index" label="顺序" width="60" />
            <el-table-column prop="provider_id" label="数据源" width="130" />
            <el-table-column>
              <template #default="{ $index }">
                <el-button size="small" :disabled="$index === 0" @click="moveUp('trading', $index)">↑</el-button>
                <el-button size="small" :disabled="$index === priorityForm.trading.length - 1" @click="moveDown('trading', $index)">↓</el-button>
              </template>
            </el-table-column>
          </el-table>
        </el-tab-pane>
        <el-tab-pane label="行情通道" name="market_data">
          <el-table :data="priorityForm.market_data" border>
            <el-table-column type="index" label="顺序" width="60" />
            <el-table-column prop="provider_id" label="数据源" width="130" />
            <el-table-column>
              <template #default="{ $index }">
                <el-button size="small" :disabled="$index === 0" @click="moveUp('market_data', $index)">↑</el-button>
                <el-button size="small" :disabled="$index === priorityForm.market_data.length - 1" @click="moveDown('market_data', $index)">↓</el-button>
              </template>
            </el-table-column>
          </el-table>
        </el-tab-pane>
        <el-tab-pane label="下载通道" name="download">
          <el-table :data="priorityForm.download" border>
            <el-table-column type="index" label="顺序" width="60" />
            <el-table-column prop="provider_id" label="数据源" width="130" />
            <el-table-column>
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

    <!-- 用户创建对话框 -->
    <el-dialog v-model="userCreateVisible" title="创建用户" width="450px">
      <el-form label-width="100px">
        <el-form-item label="用户名"><el-input v-model="userCreateForm.name" placeholder="输入用户名" /></el-form-item>
        <el-form-item label="密码"><el-input v-model="userCreateForm.password" type="password" placeholder="至少6位" show-password /></el-form-item>
        <el-form-item label="显示名称"><el-input v-model="userCreateForm.display_name" placeholder="可选" /></el-form-item>
        <el-form-item label="角色">
          <el-select v-model="userCreateForm.role" style="width: 200px">
            <el-option label="普通用户" value="user" />
            <el-option label="管理员" value="admin" />
          </el-select>
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="userCreateVisible = false">取消</el-button>
        <el-button type="primary" @click="createUser" :loading="userCreating">创建</el-button>
      </template>
    </el-dialog>

    <!-- 用户编辑对话框 -->
    <el-dialog v-model="userEditVisible" title="编辑用户" width="450px">
      <el-form label-width="100px">
        <el-form-item label="用户名"><el-input :value="userEditForm.name" disabled /></el-form-item>
        <el-form-item label="显示名称"><el-input v-model="userEditForm.display_name" /></el-form-item>
        <el-form-item label="角色">
          <el-select v-model="userEditForm.role" style="width: 200px">
            <el-option label="普通用户" value="user" />
            <el-option label="管理员" value="admin" />
          </el-select>
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="userEditVisible = false">取消</el-button>
        <el-button type="primary" @click="updateUser" :loading="userUpdating">保存</el-button>
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
const isAdmin = computed(() => accountStore.isAdmin)

const activeTab = ref('personal')
const getToken = () => localStorage.getItem('auth_token') || ''

// === 个人信息 ===
const profileForm = reactive({
  display_name: '',
  available_cash: 0,
  notifications_paused: 0,
  commission_rate: 0.0003,
  stamp_tax: 0.0005,
  transfer_fee: 0.00002,
  min_commission: 5.0,
  broker_account: '',
  broker_password: '',
  broker_company: '',
  broker_server_ip: '',
  broker_server_port: 8600,
  notes: ''
})
const profileSaving = ref(false)

// === 系统配置（管理员） ===
const systemConfig = reactive({
  price_cache_ttl_trading: 300,
  price_cache_ttl_non_trading: 43200,
  price_cache_max_size: 10000,
  price_cache_flush_interval: 900,
  monitor_interval: 60,
  monitor_watch_refresh_interval: 120,
  auto_start_monitor: true,
  circuit_breaker_recovery_timeout: 300,
  circuit_breaker_failure_threshold: 5,
})
const configSaving = ref(false)

const loadSystemConfig = async () => {
  try {
    const res = await fetch('/api/v1/ui/system-config', {
      headers: { 'Authorization': 'Bearer ' + getToken() }
    })
    const data = await res.json()
    if (data.success && data.config) {
      Object.assign(systemConfig, data.config)
    }
  } catch (e) {
    console.error('加载系统配置失败:', e)
  }
}

const saveSystemConfig = async () => {
  configSaving.value = true
  try {
    const res = await fetch('/api/v1/ui/system-config', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + getToken() },
      body: JSON.stringify(systemConfig)
    })
    const data = await res.json()
    if (data.success) {
      ElMessage.success('系统配置已保存并生效')
    } else {
      ElMessage.error(data.message || '保存失败')
    }
  } catch (e) {
    ElMessage.error('保存失败: ' + e.message)
  } finally {
    configSaving.value = false
  }
}

const resetSystemConfig = async () => {
  try {
    const res = await fetch('/api/v1/ui/system-config/defaults', {
      headers: { 'Authorization': 'Bearer ' + getToken() }
    })
    const data = await res.json()
    if (data.success && data.defaults) {
      Object.assign(systemConfig, data.defaults)
      ElMessage.success('已恢复默认值，点击保存生效')
    }
  } catch (e) {
    console.error('获取默认配置失败:', e)
  }
}

const loadProfile = () => {
  if (currentAccount.value) {
    profileForm.display_name = currentAccount.value.display_name || ''
    profileForm.available_cash = currentAccount.value.available_cash || 0
    profileForm.notifications_paused = currentAccount.value.notifications_paused || 0
    profileForm.commission_rate = currentAccount.value.commission_rate || 0.0003
    profileForm.stamp_tax = currentAccount.value.stamp_tax || 0.0005
    profileForm.transfer_fee = currentAccount.value.transfer_fee || 0.00002
    profileForm.min_commission = currentAccount.value.min_commission || 5.0
    profileForm.broker_account = currentAccount.value.broker_account || ''
    profileForm.broker_password = ''
    profileForm.broker_company = currentAccount.value.broker_company || ''
    profileForm.broker_server_ip = currentAccount.value.broker_server_ip || ''
    profileForm.broker_server_port = currentAccount.value.broker_server_port || 8600
    profileForm.notes = currentAccount.value.notes || ''
  }
}

const saveProfile = async () => {
  profileSaving.value = true
  try {
    const res = await fetch('/api/auth/me', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json', 'X-Auth-Token': getToken() },
      body: JSON.stringify(profileForm)
    })
    const data = await res.json()
    if (data.success) {
      ElMessage.success('个人信息已保存')
      // 更新 localStorage
      const savedUser = JSON.parse(localStorage.getItem('current_user') || '{}')
      Object.assign(savedUser, profileForm)
      localStorage.setItem('current_user', JSON.stringify(savedUser))
    } else {
      ElMessage.error(data.detail || '保存失败')
    }
  } catch (e) {
    ElMessage.error('保存失败: ' + e.message)
  } finally {
    profileSaving.value = false
  }
}

// === 修改密码 ===
const passwordForm = reactive({ old_password: '', new_password: '' })
const passwordSaving = ref(false)

const changePassword = async () => {
  if (!passwordForm.old_password || !passwordForm.new_password) {
    ElMessage.warning('请输入旧密码和新密码')
    return
  }
  if (passwordForm.new_password.length < 6) {
    ElMessage.warning('新密码至少6位')
    return
  }
  passwordSaving.value = true
  try {
    const res = await fetch('/api/auth/password', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json', 'X-Auth-Token': getToken() },
      body: JSON.stringify(passwordForm)
    })
    const data = await res.json()
    if (data.success) {
      ElMessage.success('密码已修改')
      passwordForm.old_password = ''
      passwordForm.new_password = ''
    } else {
      ElMessage.error(data.detail || '修改失败')
    }
  } catch (e) {
    ElMessage.error('修改失败: ' + e.message)
  } finally {
    passwordSaving.value = false
  }
}

// === 通知配置 ===
const notificationConfigured = ref(false)
const notifForm = reactive({ webhook_url: '', enabled: 1, events: [] })
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
    }
  } catch (e) { console.error('加载通知配置失败:', e) }
}

const saveNotification = async () => {
  notifSaving.value = true
  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/notifications/config`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        channel: 'feishu',
        webhook_url: notifForm.webhook_url,
        enabled: notifForm.enabled,
        notify_on_trade: notifForm.events.includes('notify_on_trade') ? 1 : 0,
        notify_on_signal: notifForm.events.includes('notify_on_signal') ? 1 : 0,
        notify_on_task: notifForm.events.includes('notify_on_task') ? 1 : 0,
      })
    })
    const data = await res.json()
    if (data.success) {
      ElMessage.success('通知配置已保存')
      notificationConfigured.value = true
    } else {
      ElMessage.error(data.detail || '保存失败')
    }
  } catch (e) {
    ElMessage.error('保存失败: ' + e.message)
  } finally {
    notifSaving.value = false
  }
}

const testNotification = async () => {
  notifTesting.value = true
  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/notifications/test`, { method: 'POST' })
    const data = await res.json()
    if (data.success) ElMessage.success('测试通知已发送')
    else ElMessage.error(data.detail || '发送失败')
  } catch (e) {
    ElMessage.error('发送失败: ' + e.message)
  } finally {
    notifTesting.value = false
  }
}

// === LLM 配置 ===
const llmConfig = reactive({ configured: false, api_key_masked: '' })
const llmForm = reactive({ provider: 'custom', base_url: '', api_key: '', model_name: '' })
const saving = ref(false)
const testing = ref(false)
const deleting = ref(false)

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

const loadLLMConfig = async () => {
  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/llm/config`)
    const data = await res.json()
    if (data.success) {
      llmConfig.configured = data.data.configured
      llmConfig.api_key_masked = data.data.api_key_masked
      llmForm.provider = data.data.provider
      llmForm.base_url = data.data.base_url
      llmForm.model_name = data.data.model_name
    }
  } catch (e) { console.error('加载 LLM 配置失败:', e) }
}

const saveLLMConfig = async () => {
  saving.value = true
  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/llm/config`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(llmForm)
    })
    const data = await res.json()
    if (data.success) {
      ElMessage.success('LLM 配置已保存')
      llmConfig.configured = true
      loadLLMConfig()
    } else ElMessage.error(data.error || '保存失败')
  } catch (e) {
    ElMessage.error('保存失败: ' + e.message)
  } finally {
    saving.value = false
  }
}

const testLLMConnection = async () => {
  testing.value = true
  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/llm/config`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(llmForm)
    })
    const data = await res.json()
    if (data.success && data.api_valid) ElMessage.success('API 连接成功')
    else ElMessage.warning(data.message || 'API 可能无效')
  } catch (e) {
    ElMessage.error('测试失败: ' + e.message)
  } finally {
    testing.value = false
  }
}

const deleteLLMConfig = async () => {
  deleting.value = true
  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/llm/config`, { method: 'DELETE' })
    const data = await res.json()
    if (data.success) {
      ElMessage.success('配置已删除')
      llmConfig.configured = false
      loadLLMConfig()
    } else ElMessage.error(data.error || '删除失败')
  } catch (e) {
    ElMessage.error('删除失败: ' + e.message)
  } finally {
    deleting.value = false
  }
}

// === Agent 管理 ===
const agents = ref([])
const createVisible = ref(false)
const keyVisible = ref(false)
const creating = ref(false)
const deletingAgentId = ref(null)
const rotating = ref(false)
const keyResult = ref(null)
const newApiKey = ref('')
const createForm = reactive({ name: '', agent_type: 'claude_code', role: 'viewer' })

const roleTagType = (role) => {
  const map = { viewer: 'info', strategist: '', operator: 'warning', admin: 'danger' }
  return map[role] || 'info'
}

const loadAgents = async () => {
  try {
    const res = await fetch('/api/auth/agents', { headers: { 'X-Auth-Token': getToken() } })
    const data = await res.json()
    if (data.success) agents.value = data.agents
  } catch (e) { console.error('加载 Agent 列表失败:', e) }
}

const showCreateDialog = () => {
  createForm.name = ''
  createForm.agent_type = 'claude_code'
  createForm.role = 'viewer'
  createVisible.value = true
}

const createAgent = async () => {
  if (!createForm.name) { ElMessage.warning('请输入 Agent 名称'); return }
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
      loadAgents()
    } else ElMessage.error(data.detail || '创建失败')
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
      ElMessage.success('API Key 已重置')
    } else ElMessage.error(data.detail || '重置失败')
  } catch (e) {
    ElMessage.error('重置失败: ' + e.message)
  } finally {
    rotating.value = false
  }
}

const deleteAgent = async (agent) => {
  try {
    await ElMessageBox.confirm(`确定要删除 Agent "${agent.name}" 吗？`, '确认删除', { type: 'warning' })
  } catch { return }
  deletingAgentId.value = agent.agent_id
  try {
    const res = await fetch(`/api/auth/agent/${agent.agent_id}`, {
      method: 'DELETE',
      headers: { 'X-Auth-Token': getToken() }
    })
    const data = await res.json()
    if (data.success) {
      ElMessage.success(`Agent "${agent.name}" 已删除`)
      loadAgents()
    } else ElMessage.error(data.detail || '删除失败')
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
  } catch { ElMessage.error('复制失败') }
}

// === 数据源管理 ===
const dataSources = ref([])
const dsLoading = ref(false)
const dsConfigVisible = ref(false)
const dsSaving = ref(false)
const dsConfigForm = reactive({ provider_id: '', api_token: '', requires_config: false })

const dsStatusTag = (status) => {
  const map = { connected: 'success', disconnected: 'warning', error: 'danger', not_configured: 'info' }
  return map[status] || 'info'
}
const dsStatusText = (status) => {
  const map = { connected: '已连接', disconnected: '未连接', error: '连接失败', not_configured: '未配置' }
  return map[status] || '未知'
}

const loadDataSources = async () => {
  dsLoading.value = true
  try {
    const res = await fetch('/api/v1/ui/data-sources', { headers: { 'X-Auth-Token': getToken() } })
    const data = await res.json()
    if (data.success) dataSources.value = data.data
  } catch (e) { console.error('加载数据源失败:', e) }
  finally { dsLoading.value = false }
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
    if (data.success) ElMessage.success(data.message)
    else { ElMessage.error(data.message || '操作失败'); row.is_enabled = !row.is_enabled }
  } catch (e) {
    ElMessage.error('操作失败: ' + e.message); row.is_enabled = !row.is_enabled
  } finally { row._toggling = false }
}

const openConfigDialog = (row) => {
  dsConfigForm.provider_id = row.provider_id
  dsConfigForm.api_token = ''
  dsConfigForm.requires_config = row.requires_config
  dsConfigVisible.value = true
}

const saveDsConfig = async () => {
  dsSaving.value = true
  try {
    const payload = {}
    if (dsConfigForm.provider_id === 'tushare' && dsConfigForm.api_token) payload.api_token = dsConfigForm.api_token
    const res = await fetch(`/api/v1/ui/data-sources/${dsConfigForm.provider_id}/config`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Auth-Token': getToken() },
      body: JSON.stringify(payload)
    })
    const data = await res.json()
    if (data.success) { ElMessage.success(data.message); dsConfigVisible.value = false; loadDataSources() }
    else ElMessage.error(data.message || '保存失败')
  } catch (e) { ElMessage.error('保存失败: ' + e.message) }
  finally { dsSaving.value = false }
}

// === 通道优先级 ===
const priorityVisible = ref(false)
const priorityTab = ref('trading')
const priorityForm = reactive({ trading: [], market_data: [], download: [] })

const openPriorityDialog = () => {
  const enabledProviders = dataSources.value.filter(ds => ds.is_enabled)
  for (const channel of ['trading', 'market_data', 'download']) {
    const sorted = [...enabledProviders].sort((a, b) => (a.channel_priority_json?.[channel] ?? 999) - (b.channel_priority_json?.[channel] ?? 999))
    priorityForm[channel] = sorted.map(ds => ({ provider_id: ds.provider_id, display_name: ds.display_name }))
  }
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
      if (!data.success) { ElMessage.error(`保存 ${channel} 优先级失败`); return }
    }
    ElMessage.success('通道优先级已更新')
    priorityVisible.value = false
    loadDataSources()
  } catch (e) { ElMessage.error('保存失败: ' + e.message) }
  finally { dsSaving.value = false }
}

// === Agent 权限矩阵 ===
const agentPermissions = ref([
  { permission: 'query:*', viewer: true, strategist: true, operator: true, admin: true },
  { permission: 'strategy:create', viewer: false, strategist: true, operator: true, admin: true },
  { permission: 'strategy:update', viewer: false, strategist: true, operator: true, admin: true },
  { permission: 'strategy:delete', viewer: false, strategist: false, operator: true, admin: true },
  { permission: 'strategy:execute', viewer: false, strategist: true, operator: true, admin: true },
  { permission: 'screening:create', viewer: false, strategist: true, operator: true, admin: true },
  { permission: 'watchlist:manage', viewer: false, strategist: true, operator: true, admin: true },
  { permission: 'scheduler:start', viewer: false, strategist: false, operator: true, admin: true },
  { permission: 'scheduler:stop', viewer: false, strategist: false, operator: true, admin: true },
  { permission: 'monitoring:start', viewer: false, strategist: false, operator: true, admin: true },
  { permission: 'monitoring:stop', viewer: false, strategist: false, operator: true, admin: true },
  { permission: 'trading:execute', viewer: false, strategist: false, operator: false, admin: true },
  { permission: 'data:export', viewer: false, strategist: false, operator: true, admin: true },
  { permission: 'account:read', viewer: false, strategist: false, operator: false, admin: true },
  { permission: 'account:write', viewer: false, strategist: false, operator: false, admin: true },
  { permission: 'system:config', viewer: false, strategist: false, operator: false, admin: true },
  { permission: 'agent:manage', viewer: false, strategist: false, operator: false, admin: true },
])

// === 用户管理 ===
const users = ref([])
const userCreateVisible = ref(false)
const userEditVisible = ref(false)
const userCreating = ref(false)
const userUpdating = ref(false)
const userCreateForm = reactive({ name: '', password: '', display_name: '', role: 'user' })
const userEditForm = reactive({ account_id: '', name: '', display_name: '', role: 'user' })

const loadUsers = async () => {
  try {
    const res = await fetch('/api/auth/users', { headers: { 'X-Auth-Token': getToken() } })
    const data = await res.json()
    if (data.success) users.value = data.users
  } catch (e) { console.error('加载用户列表失败:', e) }
}

const showUserCreateDialog = () => {
  userCreateForm.name = ''
  userCreateForm.password = ''
  userCreateForm.display_name = ''
  userCreateForm.role = 'user'
  userCreateVisible.value = true
}

const createUser = async () => {
  if (!userCreateForm.name) { ElMessage.warning('请输入用户名'); return }
  if (!userCreateForm.password || userCreateForm.password.length < 6) { ElMessage.warning('密码至少6位'); return }
  userCreating.value = true
  try {
    const res = await fetch('/api/auth/users', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Auth-Token': getToken() },
      body: JSON.stringify(userCreateForm)
    })
    const data = await res.json()
    if (data.success) { ElMessage.success('用户创建成功'); userCreateVisible.value = false; loadUsers() }
    else ElMessage.error(data.detail || '创建失败')
  } catch (e) { ElMessage.error('创建失败: ' + e.message) }
  finally { userCreating.value = false }
}

const showUserEditDialog = (user) => {
  userEditForm.account_id = user.account_id
  userEditForm.name = user.name
  userEditForm.display_name = user.display_name || ''
  userEditForm.role = user.role
  userEditVisible.value = true
}

const updateUser = async () => {
  userUpdating.value = true
  try {
    const res = await fetch(`/api/auth/users/${userEditForm.account_id}/role`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json', 'X-Auth-Token': getToken() },
      body: JSON.stringify({ role: userEditForm.role })
    })
    const data = await res.json()
    if (data.success) { ElMessage.success('用户已更新'); userEditVisible.value = false; loadUsers() }
    else ElMessage.error(data.detail || '更新失败')
  } catch (e) { ElMessage.error('更新失败: ' + e.message) }
  finally { userUpdating.value = false }
}

const toggleUserStatus = async (user) => {
  const action = user.is_active ? '锁定' : '解锁'
  try {
    await ElMessageBox.confirm(`确定要${action}用户 "${user.name}" 吗？`, `确认${action}`, { type: 'warning' })
  } catch { return }
  try {
    const res = await fetch(`/api/auth/users/${user.account_id}/status`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json', 'X-Auth-Token': getToken() },
      body: JSON.stringify({ is_active: user.is_active ? 0 : 1 })
    })
    const data = await res.json()
    if (data.success) { ElMessage.success(`用户已${action}`); loadUsers() }
    else ElMessage.error(data.detail || '操作失败')
  } catch (e) { ElMessage.error('操作失败: ' + e.message) }
}

const deleteUser = async (user) => {
  try {
    await ElMessageBox.confirm(`确定要删除用户 "${user.name}" 吗？`, '确认删除', { type: 'warning' })
  } catch { return }
  try {
    const res = await fetch(`/api/auth/users/${user.account_id}`, {
      method: 'DELETE',
      headers: { 'X-Auth-Token': getToken() }
    })
    const data = await res.json()
    if (data.success) { ElMessage.success(`用户 "${user.name}" 已删除`); loadUsers() }
    else ElMessage.error(data.detail || '删除失败')
  } catch (e) { ElMessage.error('删除失败: ' + e.message) }
}

// === 初始化 ===
onMounted(async () => {
  loadProfile()
  loadNotificationConfig()
  loadLLMConfig()
  loadAgents()
  if (isAdmin.value) {
    loadDataSources()
    loadUsers()
    loadSystemConfig()
  }
})
</script>

<style scoped>
.layout-container { height: 100%; display: flex; flex-direction: column; }
.main-content { padding: 20px; background-color: #f5f7fa; }
h2 { margin-bottom: 20px; color: #303133; }
.card-header { display: flex; justify-content: space-between; align-items: center; }
.field-hint { margin-left: 10px; color: #909399; font-size: 12px; }
</style>