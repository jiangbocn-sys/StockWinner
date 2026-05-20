<template>
  <div class="layout-container">
    <NavBar />
    <el-main class="main-content">
      <div class="page-header">
        <h2>策略管理 - {{ currentAccount?.display_name }}</h2>
        <el-button @click="loadAllData">
          <el-icon><Refresh /></el-icon>
          刷新
        </el-button>
      </div>

      <!-- 三种策略类型标签页 -->
      <el-tabs v-model="activeTab" type="border-card">
        <!-- 持仓策略 -->
        <el-tab-pane label="持仓策略" name="position">
          <el-card>
            <template #header>
              <span>仓位参数设置</span>
            </template>
            <el-form :model="positionStrategy" label-width="180px">
              <el-form-item label="总仓位上限">
                <el-slider
                  v-model="positionStrategy.max_total_position_pct"
                  :min="0.1"
                  :max="1.0"
                  :step="0.05"
                  :marks="{0.1: '10%', 0.5: '50%', 0.8: '80%', 1.0: '100%'}"
                  show-input
                />
                <div class="hint">股票资产占总资产的最大比例</div>
              </el-form-item>
              <el-form-item label="单股仓位上限">
                <el-slider
                  v-model="positionStrategy.max_single_position_pct"
                  :min="0.01"
                  :max="0.3"
                  :step="0.01"
                  :marks="{0.01: '1%', 0.05: '5%', 0.15: '15%', 0.3: '30%'}"
                  show-input
                />
                <div class="hint">单只股票占总资产的最大比例</div>
              </el-form-item>
              <el-form-item label="现金保留比例">
                <el-slider
                  v-model="positionStrategy.cash_reserve_pct"
                  :min="0.05"
                  :max="0.5"
                  :step="0.05"
                  :marks="{0.05: '5%', 0.2: '20%', 0.3: '30%', 0.5: '50%'}"
                  show-input
                />
                <div class="hint">必须保留的现金比例，用于应对突发情况</div>
              </el-form-item>
              <el-form-item>
                <el-button type="primary" @click="savePositionStrategy" :loading="savingPosition">
                  保存持仓策略
                </el-button>
              </el-form-item>
            </el-form>
          </el-card>

          <!-- 持仓调整规则 -->
          <el-card class="margin-top-20">
            <template #header>
              <div class="card-header">
                <span>动态调整规则</span>
                <el-button type="primary" size="small" @click="showRuleDialog = true">
                  <el-icon><Plus /></el-icon>
                  新建规则
                </el-button>
              </div>
            </template>
            <el-alert type="info" :closable="false" class="margin-bottom-15">
              根据市场条件自动调整仓位参数，系统会在条件触发时向用户发送调整建议
            </el-alert>
            <el-table :data="positionRules" stripe v-loading="loadingRules">
              <el-table-column prop="trigger_condition" label="触发条件" width="200" />
              <el-table-column prop="trigger_description" label="说明" min-width="150" />
              <el-table-column prop="target_max_total_pct" label="总仓位上限" width="100">
                <template #default="{ row }">
                  {{ (row.target_max_total_pct * 100).toFixed(0) }}%
                </template>
              </el-table-column>
              <el-table-column prop="target_max_single_pct" label="单股上限" width="100">
                <template #default="{ row }">
                  {{ row.target_max_single_pct > 0 ? (row.target_max_single_pct * 100).toFixed(0) + '%' : '-' }}
                </template>
              </el-table-column>
              <el-table-column prop="priority" label="优先级" width="80" />
              <el-table-column prop="is_active" label="状态" width="80">
                <template #default="{ row }">
                  <el-tag :type="row.is_active ? 'success' : 'info'" size="small">
                    {{ row.is_active ? '启用' : '停用' }}
                  </el-tag>
                </template>
              </el-table-column>
              <el-table-column label="操作" width="150">
                <template #default="{ row }">
                  <el-button type="warning" size="small" @click="toggleRule(row)">
                    {{ row.is_active ? '停用' : '启用' }}
                  </el-button>
                  <el-button type="danger" size="small" @click="deleteRule(row)">
                    删除
                  </el-button>
                </template>
              </el-table-column>
            </el-table>
            <el-empty v-if="!loadingRules && positionRules.length === 0" description="暂无调整规则" />
          </el-card>
        </el-tab-pane>

        <!-- 选股策略 -->
        <el-tab-pane label="选股策略" name="screening">
          <el-card>
            <template #header>
              <div class="card-header">
                <span>选股策略列表</span>
                <el-space>
                  <el-button type="primary" size="small" @click="showCreateScreeningDialog = true">
                    <el-icon><Plus /></el-icon>
                    新建策略
                  </el-button>
                  <el-button type="success" size="small" @click="showLLMDialog = true" :disabled="generating">
                    <el-icon><MagicStick /></el-icon>
                    LLM生成
                  </el-button>
                </el-space>
              </div>
            </template>

            <el-table :data="configScreeningStrategies" stripe v-loading="loadingScreening">
              <el-table-column prop="name" label="策略名称" width="180" />
              <el-table-column prop="description" label="描述" min-width="200" show-overflow-tooltip />
              <el-table-column prop="status" label="状态" width="80">
                <template #default="{ row }">
                  <el-tag :type="getStatusType(row.status)" size="small">
                    {{ getStatusText(row.status) }}
                  </el-tag>
                </template>
              </el-table-column>
              <el-table-column prop="created_at" label="创建时间" width="160" />
              <el-table-column label="操作" width="320">
                <template #default="{ row }">
                  <el-button type="primary" size="small" @click="editScreeningStrategy(row)">
                    编辑
                  </el-button>
                  <el-button type="info" size="small" @click="viewScreeningStrategy(row)">
                    详情
                  </el-button>
                  <el-button
                    :type="row.status === 'active' ? 'warning' : 'success'"
                    size="small"
                    @click="toggleScreeningStrategy(row)"
                  >
                    {{ row.status === 'active' ? '停用' : '激活' }}
                  </el-button>
                  <el-button type="danger" size="small" @click="deleteScreeningStrategy(row)">
                    删除
                  </el-button>
                </template>
              </el-table-column>
            </el-table>
            <el-empty v-if="!loadingScreening && configScreeningStrategies.length === 0" description="暂无选股策略" />
          </el-card>
        </el-tab-pane>

        <!-- 代码策略 -->
        <el-tab-pane label="代码策略" name="code">
          <el-tabs v-model="activeCodeTab" type="card">
            <!-- 代码选股策略 -->
            <el-tab-pane label="代码选股" name="screening_code">
              <el-card>
                <template #header>
                  <div class="card-header">
                    <span>Python 代码选股策略</span>
                    <el-button type="primary" size="small" @click="resetCodeForm(); codeStrategyForm.code_scope = 'screening'; showCreateCodeDialog = true">
                      <el-icon><Plus /></el-icon>
                      新建策略
                    </el-button>
                  </div>
                </template>
                <el-alert type="info" :closable="false" class="margin-bottom-15">
                  用 Python 代码编写选股策略，在候选组股票池上运行，输出交易信号到 watchlist
                </el-alert>
                <el-table :data="screeningCodeStrategies" stripe v-loading="loadingCodeStrategies">
                  <el-table-column prop="name" label="策略名称" width="180" />
                  <el-table-column prop="description" label="描述" min-width="200" show-overflow-tooltip />
                  <el-table-column prop="function_name" label="入口函数" width="100" />
                  <el-table-column label="卖出策略" width="180">
                    <template #default="{ row }">
                      <el-select
                        v-model="row.sell_strategy_id"
                        placeholder="选择卖出策略"
                        clearable
                        size="small"
                        @change="saveSellStrategyLink(row)"
                        @click="loadSellStrategyOptions"
                      >
                        <el-option
                          v-for="s in sellStrategies"
                          :key="s.id"
                          :label="s.name"
                          :value="s.id"
                        />
                      </el-select>
                    </template>
                  </el-table-column>
                  <el-table-column prop="status" label="状态" width="80">
                    <template #default="{ row }">
                      <el-tag :type="getStatusType(row.status)" size="small">{{ getStatusText(row.status) }}</el-tag>
                    </template>
                  </el-table-column>
                  <el-table-column label="操作" width="520">
                    <template #default="{ row }">
                      <el-button type="info" size="small" @click="viewCodeStrategy(row)">详情</el-button>
                      <el-button type="primary" size="small" @click="editCodeStrategy(row)">编辑</el-button>
                      <el-button type="warning" size="small" @click="testRunStrategy(row)">试运行</el-button>
                      <el-button :type="row.status === 'active' ? 'warning' : 'success'" size="small" @click="toggleScreeningStrategy(row)">
                        {{ row.status === 'active' ? '停用' : '激活' }}
                      </el-button>
                      <el-button type="danger" size="small" @click="deleteScreeningStrategy(row)">删除</el-button>
                    </template>
                  </el-table-column>
                </el-table>
                <el-empty v-if="!loadingCodeStrategies && screeningCodeStrategies.length === 0" description="暂无代码选股策略" />
              </el-card>
            </el-tab-pane>

            <!-- 代码交易策略 -->
            <el-tab-pane label="代码交易" name="trading_code">
              <el-card>
                <template #header>
                  <div class="card-header">
                    <span>Python 代码交易策略</span>
                    <el-button type="primary" size="small" @click="resetCodeForm(); codeStrategyForm.code_scope = 'trading'; showCreateCodeDialog = true">
                      <el-icon><Plus /></el-icon>
                      新建策略
                    </el-button>
                  </div>
                </template>
                <el-alert type="info" :closable="false" class="margin-bottom-15">
                  用 Python 代码编写交易策略，接收选股信号后自动执行买入卖出
                </el-alert>
                <el-table :data="tradingCodeStrategies" stripe v-loading="loadingCodeStrategies">
                  <el-table-column prop="name" label="策略名称" width="180" />
                  <el-table-column prop="description" label="描述" min-width="200" show-overflow-tooltip />
                  <el-table-column prop="function_name" label="入口函数" width="100" />
                  <el-table-column prop="status" label="状态" width="80">
                    <template #default="{ row }">
                      <el-tag :type="getStatusType(row.status)" size="small">{{ getStatusText(row.status) }}</el-tag>
                    </template>
                  </el-table-column>
                  <el-table-column label="操作" width="520">
                    <template #default="{ row }">
                      <el-button type="info" size="small" @click="viewCodeStrategy(row)">详情</el-button>
                      <el-button type="primary" size="small" @click="editCodeStrategy(row)">编辑</el-button>
                      <el-button type="warning" size="small" @click="testRunStrategy(row)">试运行</el-button>
                      <el-button :type="row.status === 'active' ? 'warning' : 'success'" size="small" @click="toggleScreeningStrategy(row)">
                        {{ row.status === 'active' ? '停用' : '激活' }}
                      </el-button>
                      <el-button type="danger" size="small" @click="deleteScreeningStrategy(row)">删除</el-button>
                    </template>
                  </el-table-column>
                </el-table>
                <el-empty v-if="!loadingCodeStrategies && tradingCodeStrategies.length === 0" description="暂无代码交易策略" />
              </el-card>
            </el-tab-pane>
          </el-tabs>
        </el-tab-pane>

        <!-- 个股策略 -->
        <el-tab-pane label="个股策略" name="trading">
          <el-card>
            <template #header>
              <div class="card-header">
                <span>个股交易策略</span>
                <el-space>
                  <el-input
                    v-model="searchStockCode"
                    placeholder="搜索股票代码"
                    size="small"
                    style="width: 150px;"
                    clearable
                  />
                  <el-button type="primary" size="small" @click="showTradingDialog = true">
                    <el-icon><Plus /></el-icon>
                    新建策略
                  </el-button>
                </el-space>
              </div>
            </template>

            <el-alert type="info" :closable="false" class="margin-bottom-15">
              为持仓股或候选股设置具体的交易参数：建仓价、止损止盈、单次买卖数量
            </el-alert>

            <el-table :data="filteredTradingStrategies" stripe v-loading="loadingTrading">
              <el-table-column prop="stock_code" label="股票代码" width="120" />
              <el-table-column prop="stock_name" label="股票名称" width="120" />
              <el-table-column prop="entry_price" label="建仓价" width="100">
                <template #default="{ row }">
                  {{ row.entry_price > 0 ? row.entry_price.toFixed(2) : '-' }}
                </template>
              </el-table-column>
              <el-table-column prop="stop_loss_price" label="止损价" width="100">
                <template #default="{ row }">
                  {{ row.stop_loss_price > 0 ? row.stop_loss_price.toFixed(2) : '-' }}
                </template>
              </el-table-column>
              <el-table-column prop="stop_loss_pct" label="止损比例" width="100">
                <template #default="{ row }">
                  {{ row.stop_loss_pct > 0 ? (row.stop_loss_pct * 100).toFixed(1) + '%' : '-' }}
                </template>
              </el-table-column>
              <el-table-column prop="take_profit_price" label="止盈价" width="100">
                <template #default="{ row }">
                  {{ row.take_profit_price > 0 ? row.take_profit_price.toFixed(2) : '-' }}
                </template>
              </el-table-column>
              <el-table-column prop="take_profit_pct" label="止盈比例" width="100">
                <template #default="{ row }">
                  {{ row.take_profit_pct > 0 ? (row.take_profit_pct * 100).toFixed(1) + '%' : '-' }}
                </template>
              </el-table-column>
              <el-table-column prop="max_trade_quantity" label="单次买卖数量" width="120">
                <template #default="{ row }">
                  {{ row.max_trade_quantity > 0 ? row.max_trade_quantity + '股' : '-' }}
                </template>
              </el-table-column>
              <el-table-column label="操作" width="150">
                <template #default="{ row }">
                  <el-button type="primary" size="small" @click="editTradingStrategy(row)">
                    编辑
                  </el-button>
                  <el-button type="danger" size="small" @click="deleteTradingStrategy(row)">
                    删除
                  </el-button>
                </template>
              </el-table-column>
            </el-table>
            <el-empty v-if="!loadingTrading && tradingStrategies.length === 0" description="暂无交易策略" />
          </el-card>
        </el-tab-pane>

        <!-- 告警策略 -->
        <el-tab-pane label="告警策略" name="alert">
          <el-card>
            <template #header>
              <div class="card-header">
                <span>告警策略</span>
                <el-space>
                  <el-button type="primary" size="small" @click="showAlertStrategyDialog">
                    <el-icon><Plus /></el-icon>
                    新建告警策略
                  </el-button>
                  <el-button size="small" @click="loadAlertStrategies">
                    <el-icon><Refresh /></el-icon>
                    刷新
                  </el-button>
                </el-space>
              </div>
            </template>

            <el-table :data="alertStrategyList" stripe style="width: 100%">
              <el-table-column prop="name" label="策略名称" width="150" />
              <el-table-column label="策略类型" width="120">
                <template #default="{ row }">
                  {{ formatAlertStrategyType(row.strategy_type) }}
                </template>
              </el-table-column>
              <el-table-column label="方向" width="70">
                <template #default="{ row }">
                  <el-tag :type="row.action === 'buy' ? 'danger' : 'success'" size="small">
                    {{ row.action === 'buy' ? '买入' : '卖出' }}
                  </el-tag>
                </template>
              </el-table-column>
              <el-table-column label="状态" width="70">
                <template #default="{ row }">
                  <el-tag :type="row.enabled ? 'success' : 'info'" size="small">
                    {{ row.enabled ? '启用' : '停用' }}
                  </el-tag>
                </template>
              </el-table-column>
              <el-table-column prop="cooldown_seconds" label="冷却(秒)" width="90" />
              <el-table-column label="操作" width="120">
                <template #default="{ row }">
                  <el-button size="small" @click="editAlertStrategy(row)">编辑</el-button>
                  <el-button size="small" type="danger" @click="deleteAlertStrategy(row.id)">删除</el-button>
                </template>
              </el-table-column>
            </el-table>
            <el-empty v-if="!loadingAlertStrategies && alertStrategyList.length === 0" description="暂无告警策略" />
          </el-card>
        </el-tab-pane>
      </el-tabs>

      <!-- 持仓调整规则对话框 -->
      <el-dialog v-model="showRuleDialog" title="新建调整规则" width="550px">
        <el-form :model="newRule" label-width="120px">
          <el-form-item label="触发条件描述" required>
            <el-input
              v-model="newRule.natural_description"
              type="textarea"
              :rows="2"
              placeholder="用自然语言描述，如：'上证指数RSI跌破30时降低仓位'"
              :disabled="translating"
            />
            <div class="hint">可用：上证/深证/创业板指数，MACD金叉/死叉，RSI超买/超卖，成交量放大等</div>
          </el-form-item>
          <el-form-item>
            <el-button type="warning" @click="translateCondition" :loading="translating" :disabled="!newRule.natural_description">
              <el-icon v-if="!translating"><MagicStick /></el-icon>
              {{ translating ? '翻译中...' : 'LLM翻译' }}
            </el-button>
          </el-form-item>
          <el-form-item label="翻译结果" v-if="newRule.trigger_expression">
            <el-alert type="success" :closable="false">
              <div><strong>表达式：</strong>{{ newRule.trigger_expression }}</div>
              <div><strong>说明：</strong>{{ newRule.trigger_description }}</div>
            </el-alert>
          </el-form-item>
          <el-form-item label="目标总仓位(占总资产)" v-if="newRule.trigger_expression">
            <el-slider
              v-model="newRule.target_max_total_pct"
              :min="0.1"
              :max="1.0"
              :step="0.05"
              :marks="{0.1: '10%', 0.5: '50%', 0.8: '80%', 1.0: '100%'}"
              show-input
            />
          </el-form-item>
          <el-form-item label="单股上限(占总资产)" v-if="newRule.trigger_expression">
            <el-slider
              v-model="newRule.target_max_single_pct"
              :min="0"
              :max="0.3"
              :step="0.01"
              show-input
            />
            <div class="hint">0 表示不限制单股仓位；总资产 = 现金 + 持仓市值</div>
          </el-form-item>
          <el-form-item label="优先级" v-if="newRule.trigger_expression">
            <el-input-number v-model="newRule.priority" :min="0" :max="100" />
          </el-form-item>
        </el-form>
        <template #footer>
          <el-button @click="showRuleDialog = false">取消</el-button>
          <el-button type="primary" @click="createRule" :loading="creatingRule" :disabled="!newRule.trigger_expression">
            创建规则
          </el-button>
        </template>
      </el-dialog>

      <!-- 新建交易策略对话框 -->
      <el-dialog v-model="showTradingDialog" title="新建交易策略" width="500px">
        <el-form :model="newTrading" label-width="120px">
          <el-form-item label="股票代码" required>
            <el-input v-model="newTrading.stock_code" placeholder="如：600000.SH" />
          </el-form-item>
          <el-form-item label="建仓价">
            <el-input-number v-model="newTrading.entry_price" :min="0" :precision="2" />
          </el-form-item>
          <el-form-item label="止损价（固定）">
            <el-input-number v-model="newTrading.stop_loss_price" :min="0" :precision="2" />
          </el-form-item>
          <el-form-item label="止损比例">
            <el-slider
              v-model="newTrading.stop_loss_pct"
              :min="0"
              :max="0.3"
              :step="0.01"
              show-input
            />
            <div class="hint">固定止损价优先，为空时用比例计算</div>
          </el-form-item>
          <el-form-item label="止盈价（固定）">
            <el-input-number v-model="newTrading.take_profit_price" :min="0" :precision="2" />
          </el-form-item>
          <el-form-item label="止盈比例">
            <el-slider
              v-model="newTrading.take_profit_pct"
              :min="0"
              :max="0.5"
              :step="0.01"
              show-input
            />
          </el-form-item>
          <el-form-item label="单次买卖数量">
            <el-input-number v-model="newTrading.max_trade_quantity" :min="0" :step="100" />
            <div class="hint">一次交易允许买卖的最大股数</div>
          </el-form-item>
        </el-form>
        <template #footer>
          <el-button @click="showTradingDialog = false">取消</el-button>
          <el-button type="primary" @click="saveTradingStrategy" :loading="savingTrading">保存</el-button>
        </template>
      </el-dialog>

      <!-- 编辑交易策略对话框 -->
      <el-dialog v-model="showEditTradingDialog" title="编辑交易策略" width="500px">
        <el-form :model="editingTrading" label-width="120px">
          <el-form-item label="股票代码">
            <el-input :value="editingTrading.stock_code" disabled />
          </el-form-item>
          <el-form-item label="股票名称">
            <el-input v-model="editingTrading.stock_name" placeholder="股票名称" />
          </el-form-item>
          <el-form-item label="建仓价">
            <el-input-number v-model="editingTrading.entry_price" :min="0" :precision="2" />
          </el-form-item>
          <el-form-item label="止损价">
            <el-input-number v-model="editingTrading.stop_loss_price" :min="0" :precision="2" />
          </el-form-item>
          <el-form-item label="止损比例">
            <el-slider
              v-model="editingTrading.stop_loss_pct"
              :min="0"
              :max="0.3"
              :step="0.01"
              show-input
            />
          </el-form-item>
          <el-form-item label="止盈价">
            <el-input-number v-model="editingTrading.take_profit_price" :min="0" :precision="2" />
          </el-form-item>
          <el-form-item label="止盈比例">
            <el-slider
              v-model="editingTrading.take_profit_pct"
              :min="0"
              :max="0.5"
              :step="0.01"
              show-input
            />
          </el-form-item>
          <el-form-item label="单次买卖数量">
            <el-input-number v-model="editingTrading.max_trade_quantity" :min="0" :step="100" />
          </el-form-item>
        </el-form>
        <template #footer>
          <el-button @click="showEditTradingDialog = false">取消</el-button>
          <el-button type="primary" @click="updateTradingStrategy" :loading="savingTrading">保存</el-button>
        </template>
      </el-dialog>

      <!-- LLM生成选股策略对话框 -->
      <el-dialog v-model="showLLMDialog" title="LLM 生成选股策略" width="550px">
        <el-form :model="llmStrategy" label-width="120px">
          <el-form-item label="策略描述" required>
            <el-input
              v-model="llmStrategy.description"
              type="textarea"
              :rows="4"
              placeholder="描述你想要的策略，例如：'总市值小于50亿，MACD金叉，成交量放大'"
              :disabled="generating"
            />
          </el-form-item>
        </el-form>
        <template #footer>
          <el-button @click="showLLMDialog = false" :disabled="generating">取消</el-button>
          <el-button type="warning" @click="generateLLMStrategy" :loading="generating">
            <el-icon v-if="!generating"><MagicStick /></el-icon>
            {{ generating ? '正在生成...' : '生成策略' }}
          </el-button>
        </template>
      </el-dialog>

      <!-- 编辑选股策略对话框 -->
      <el-dialog v-model="showEditScreeningDialog" title="编辑选股策略" width="650px">
        <el-form :model="editingScreening" label-width="120px">
          <el-form-item label="策略名称" required>
            <el-input v-model="editingScreening.name" placeholder="策略名称" />
          </el-form-item>
          <el-form-item label="策略描述">
            <el-input v-model="editingScreening.description" type="textarea" :rows="2" placeholder="策略描述" />
          </el-form-item>

          <el-divider>筛选条件（基本面）</el-divider>
          <el-row :gutter="16">
            <el-col :span="12">
              <el-form-item label="总市值上限(亿)">
                <el-input-number v-model="editingScreening.filters.total_market_cap_max" :min="0" :precision="0" :controls="false" style="width:100%" />
              </el-form-item>
            </el-col>
            <el-col :span="12">
              <el-form-item label="总市值下限(亿)">
                <el-input-number v-model="editingScreening.filters.total_market_cap_min" :min="0" :precision="0" :controls="false" style="width:100%" />
              </el-form-item>
            </el-col>
          </el-row>
          <el-row :gutter="16">
            <el-col :span="12">
              <el-form-item label="流通市值上限(亿)">
                <el-input-number v-model="editingScreening.filters.circ_market_cap_max" :min="0" :precision="0" :controls="false" style="width:100%" />
              </el-form-item>
            </el-col>
            <el-col :span="12">
              <el-form-item label="流通市值下限(亿)">
                <el-input-number v-model="editingScreening.filters.circ_market_cap_min" :min="0" :precision="0" :controls="false" style="width:100%" />
              </el-form-item>
            </el-col>
          </el-row>
          <el-row :gutter="16">
            <el-col :span="12">
              <el-form-item label="PE上限(倍)">
                <el-input-number v-model="editingScreening.filters.pe_ttm_max" :min="0" :precision="1" :controls="false" style="width:100%" />
              </el-form-item>
            </el-col>
            <el-col :span="12">
              <el-form-item label="PB上限(倍)">
                <el-input-number v-model="editingScreening.filters.pb_max" :min="0" :precision="2" :controls="false" style="width:100%" />
              </el-form-item>
            </el-col>
          </el-row>
          <el-row :gutter="16">
            <el-col :span="12">
              <el-form-item label="ROE下限(%)">
                <el-input-number v-model="editingScreening.filters.roe_min" :min="0" :precision="1" :controls="false" style="width:100%" />
              </el-form-item>
            </el-col>
            <el-col :span="12">
              <el-form-item label="毛利率下限(%)">
                <el-input-number v-model="editingScreening.filters.gross_margin_min" :min="0" :precision="1" :controls="false" style="width:100%" />
              </el-form-item>
            </el-col>
          </el-row>
          <el-row :gutter="16">
            <el-col :span="12">
              <el-form-item label="营收增长下限(%)">
                <el-input-number v-model="editingScreening.filters.revenue_growth_yoy_min" :min="0" :precision="1" :controls="false" style="width:100%" />
              </el-form-item>
            </el-col>
            <el-col :span="12">
              <el-form-item label="申万一级行业">
                <el-input v-model="editingScreening.filters.sw_level1" placeholder="如：电子" />
              </el-form-item>
            </el-col>
          </el-row>

          <el-divider>市场范围</el-divider>
          <el-form-item label="允许市场">
            <el-checkbox-group v-model="editingScreening.markets">
              <el-checkbox value="SH">沪市</el-checkbox>
              <el-checkbox value="SZ">深市</el-checkbox>
              <el-checkbox value="BJ">北交所</el-checkbox>
            </el-checkbox-group>
            <div class="hint" style="margin-left:120px">默认全选，取消勾选即可排除对应市场</div>
          </el-form-item>

          <el-divider>买入条件（技术信号）</el-divider>
          <el-form-item label="买入条件">
            <el-select
              v-model="editingScreening.buyConditions"
              multiple
              filterable
              allow-create
              default-first-option
              placeholder="选择或输入技术信号"
              style="width:100%"
            >
              <el-option label="MACD金叉 (DIF_CROSS_UP_DEA)" value="DIF_CROSS_UP_DEA" />
              <el-option label="MACD死叉 (DIF_CROSS_DOWN_DEA)" value="DIF_CROSS_DOWN_DEA" />
              <el-option label="MA5上穿MA10" value="MA5_CROSS_UP_MA10" />
              <el-option label="MA5下穿MA10" value="MA5_CROSS_DOWN_MA10" />
              <el-option label="量比>1.5" value="VOLUME_RATIO > 1.5" />
              <el-option label="量比>2" value="VOLUME_RATIO > 2" />
              <el-option label="量比>3" value="VOLUME_RATIO > 3" />
              <el-option label="RSI超卖(<30)" value="RSI < 30" />
              <el-option label="RSI超买(>70)" value="RSI > 70" />
              <el-option label="价格>MA5" value="PRICE > MA5" />
              <el-option label="价格<MA5" value="PRICE < MA5" />
              <el-option label="MACD>0" value="MACD > 0" />
              <el-option label="MACD<0" value="MACD < 0" />
            </el-select>
            <div class="hint" style="margin-left:120px">可下拉选择，也可手动输入自定义条件</div>
          </el-form-item>
        </el-form>
        <template #footer>
          <el-button @click="showEditScreeningDialog = false">取消</el-button>
          <el-button type="primary" @click="saveEditScreening" :loading="savingEditScreening">保存</el-button>
        </template>
      </el-dialog>

      <!-- 选股策略详情对话框 -->
      <el-dialog v-model="showScreeningDetailDialog" :title="selectedScreening?.name" width="650px">
        <el-descriptions :column="2" border v-if="selectedScreening">
          <el-descriptions-item label="策略名称">{{ selectedScreening.name }}</el-descriptions-item>
          <el-descriptions-item label="状态">
            <el-tag :type="getStatusType(selectedScreening.status)" size="small">
              {{ getStatusText(selectedScreening.status) }}
            </el-tag>
          </el-descriptions-item>
          <el-descriptions-item label="描述" :span="2">{{ selectedScreening.description }}</el-descriptions-item>
          <el-descriptions-item label="筛选条件" :span="2">
            <condition-tree :node="normalizeConditions(parseConfig(selectedScreening.config)?.stock_filters)" />
            <span v-if="!parseConfig(selectedScreening.config)?.stock_filters" style="color: #999;">无</span>
          </el-descriptions-item>
          <el-descriptions-item label="买入条件" :span="2">
            <condition-tree :node="normalizeConditions(parseConfig(selectedScreening.config)?.buy_conditions)" />
            <span v-if="!parseConfig(selectedScreening.config)?.buy_conditions" style="color: #999;">无</span>
          </el-descriptions-item>
          <el-descriptions-item label="创建时间">{{ selectedScreening.created_at }}</el-descriptions-item>
          <el-descriptions-item label="更新时间">{{ selectedScreening.updated_at }}</el-descriptions-item>
        </el-descriptions>
      </el-dialog>

      <!-- 新建/编辑代码型策略对话框 -->
      <el-dialog v-model="showCreateCodeDialog" :title="editingCodeId ? '编辑代码策略' : '新建代码策略'" width="800px" :close-on-click-modal="false">
        <el-form label-width="100px">
          <el-form-item label="策略名称" required>
            <el-input v-model="codeStrategyForm.name" placeholder="如：尾盘选股策略" />
          </el-form-item>
          <el-form-item label="策略类型">
            <el-tag :type="codeStrategyForm.code_scope === 'trading' ? 'warning' : 'success'" size="large">
              {{ codeStrategyForm.code_scope === 'trading' ? '交易型（自动执行买卖）' : '选股型（输出信号到 watchlist）' }}
            </el-tag>
            <div class="hint">类型由所在标签页决定，无需手动选择</div>
          </el-form-item>
          <el-form-item label="描述">
            <el-input v-model="codeStrategyForm.description" type="textarea" :rows="2" />
          </el-form-item>
          <el-form-item label="入口函数">
            <el-input v-model="codeStrategyForm.function_name" style="width: 150px" />
            <span class="hint" style="margin-left: 8px">策略代码中的主函数名，默认 run</span>
          </el-form-item>
          <el-form-item label="Python 代码">
            <el-collapse style="margin-bottom: 8px">
              <el-collapse-item title="📖 编写规则" name="rules">
                <div class="code-hints">
                  <p><b>基本要求：</b></p>
                  <ul>
                    <li>必须包含 <code>def run(context):</code> 入口函数</li>
                    <li>不能使用 <code>async def</code>，必须是同步函数</li>
                  </ul>
                  <p><b>允许的模块：</b></p>
                  <ul>
                    <li><code>import pandas as pd</code>、<code>import numpy as np</code></li>
                    <li><code>import datetime</code>、<code>import json</code>、<code>import math</code>、<code>import time</code></li>
                    <li>禁止 <code>os</code>、<code>sys</code>、<code>subprocess</code> 等系统模块</li>
                  </ul>
                  <p><b>沙盒注入的函数（直接调用，无需 import）：</b></p>
                  <ul>
                    <li><code>get_kline_smart(codes, lookback=100)</code> — 智能 K 线（盘中自动拼接实时数据）</li>
                    <li><code>get_batch_kline(codes, limit=100)</code> — 批量获取 K 线</li>
                    <li><code>get_factors(code, date)</code> — 获取日频因子</li>
                    <li><code>get_kline_spliced(codes, lookback=100)</code> — 本地历史+实时拼接</li>
                    <li><code>kronos_predict(df_hist, pred_len=5)</code> — Kronos 时间序列预测</li>
                    <li><code>kronos_available</code> — 布尔值，Kronos 模型是否已加载</li>
                  </ul>
                  <p><b>返回值：</b>返回信号列表，每条信号包含 <code>action</code>（'buy'/'sell'/'watch'）、<code>stock_code</code>、<code>buy_price</code>、<code>reason</code> 等字段</p>
                  <p><b>禁止：</b>直接调用 SDK、使用 <code>open()</code> 文件操作、<code>eval()</code>/<code>exec()</code></p>
                </div>
              </el-collapse-item>
            </el-collapse>
            <div style="display: flex; align-items: center; gap: 8px; margin-bottom: 8px">
              <el-button type="primary" link size="small" @click="codeFileInput?.click()">
                从 .py 文件导入
              </el-button>
              <el-tag v-if="importedFileName" type="success" size="small">已导入：{{ importedFileName }}</el-tag>
              <input
                ref="codeFileInput"
                type="file"
                accept=".py,.pyw"
                style="display: none"
                @change="handleCodeFileImport"
              />
            </div>
            <el-input
              v-model="codeStrategyForm.code"
              type="textarea"
              :rows="20"
              style="font-family: monospace; font-size: 13px"
              :placeholder="codeStrategyForm.code_scope === 'trading'
                ? 'def run(context):\n    # context.signals: 待处理的交易信号列表\n    # return [{action, stock_code, quantity, price, ...}]'
                : 'def run(context):\n    # context.stocks: 候选组股票列表\n    # context.account_id: 账户ID\n    # return [{action, stock_code, buy_price, ...}]'
              "
            />
          </el-form-item>
          <el-form-item>
            <el-button type="warning" size="small" @click="validateCode" :loading="validatingCode">
              验证代码
            </el-button>
            <el-tag v-if="codeValidationResult" :type="codeValidationResult.valid ? 'success' : 'danger'" style="margin-left: 8px">
              {{ codeValidationResult.valid ? '语法正确' : codeValidationResult.error }}
            </el-tag>
            <el-tag v-if="kronosStatus !== null" :type="kronosStatus.available ? 'success' : 'warning'" style="margin-left: 8px">
              {{ kronosStatus.available ? `Kronos可用(${kronosStatus.device})` : 'Kronos不可用' }}
            </el-tag>
          </el-form-item>
        </el-form>
        <template #footer>
          <el-button @click="showCreateCodeDialog = false">取消</el-button>
          <el-button type="primary" @click="saveCodeStrategy" :loading="savingCodeStrategy">保存</el-button>
        </template>
      </el-dialog>

      <!-- 代码策略详情对话框 -->
      <el-dialog v-model="showCodeDetailDialog" :title="selectedCodeStrategy?.name" width="800px">
        <el-descriptions :column="2" border v-if="selectedCodeStrategy">
          <el-descriptions-item label="策略名称">{{ selectedCodeStrategy.name }}</el-descriptions-item>
          <el-descriptions-item label="状态">
            <el-tag :type="getStatusType(selectedCodeStrategy.status)" size="small">
              {{ getStatusText(selectedCodeStrategy.status) }}
            </el-tag>
          </el-descriptions-item>
          <el-descriptions-item label="类型">
            <el-tag :type="selectedCodeStrategy.code_scope === 'trading' ? 'warning' : 'success'">
              {{ selectedCodeStrategy.code_scope === 'trading' ? '交易型' : '选股型' }}
            </el-tag>
          </el-descriptions-item>
          <el-descriptions-item label="入口函数">{{ selectedCodeStrategy.function_name || 'run' }}</el-descriptions-item>
          <el-descriptions-item label="描述" :span="2">{{ selectedCodeStrategy.description || '无' }}</el-descriptions-item>
        </el-descriptions>
        <el-divider>Python 代码</el-divider>
        <pre v-if="selectedCodeStrategy?.code" class="code-block">{{ selectedCodeStrategy.code }}</pre>
        <el-empty v-else description="暂无代码" :image-size="80" />
        <template #footer>
          <el-button @click="showCodeDetailDialog = false">关闭</el-button>
          <el-button type="primary" @click="showCodeDetailDialog = false; editCodeStrategy(selectedCodeStrategy)">编辑</el-button>
        </template>
      </el-dialog>

      <!-- 策略试运行对话框 -->
      <el-dialog v-model="showTestRunDialog" :title="`试运行策略 - ${testRunStrategyName}`" width="700px" :close-on-click-modal="false">
        <el-form label-width="120px">
          <el-form-item label="候选分组" required>
            <el-select v-model="testRunForm.groupId" placeholder="选择候选分组" style="width: 100%">
              <el-option v-for="g in candidateGroups" :key="g.id" :label="g.name" :value="g.id" />
            </el-select>
            <div class="hint">策略将在该分组的股票池上运行</div>
          </el-form-item>
          <el-form-item label="股票代码（可选）">
            <el-input v-model="testRunForm.stockCodes" placeholder="留空则使用分组全部股票，或手动指定如 600000.SH,000001.SZ" />
            <div class="hint">指定股票代码可快速测试特定股票，多个用逗号分隔</div>
          </el-form-item>
        </el-form>
        <template #footer>
          <el-button @click="showTestRunDialog = false">取消</el-button>
          <el-button type="primary" @click="doTestRun" :loading="testRunning">开始试运行</el-button>
        </template>
      </el-dialog>

      <!-- 试运行结果对话框 -->
      <el-dialog v-model="showTestRunResultDialog" :title="`试运行结果 - ${testRunStrategyName}`" width="750px">
        <el-alert type="success" :closable="false" class="margin-bottom-15">
          <template #title>仅模拟执行，未写入候选列表，未执行任何交易</template>
        </el-alert>
        <el-descriptions :column="3" border>
          <el-descriptions-item label="耗时">{{ testRunResult.duration_ms }}ms</el-descriptions-item>
          <el-descriptions-item label="信号数">{{ testRunResult.signal_count }}</el-descriptions-item>
          <el-descriptions-item label="状态">
            <el-tag :type="testRunResult.success ? 'success' : 'danger'">
              {{ testRunResult.success ? '执行成功' : '执行失败' }}
            </el-tag>
          </el-descriptions-item>
        </el-descriptions>

        <!-- 错误信息 -->
        <el-alert v-if="testRunResult.error" type="error" :closable="false" class="margin-top-10">
          <template #title>{{ testRunResult.error }}</template>
        </el-alert>

        <!-- Kronos 不可用警告 -->
        <el-alert v-if="testRunResult.kronos_unavailable" type="warning" :closable="false" class="margin-top-10">
          <template #title>⚠ Kronos 模型未加载，策略中的预测功能已跳过</template>
          <template v-if="testRunResult.kronos_error" #default>{{ testRunResult.kronos_error }}</template>
        </el-alert>

        <!-- 验证警告 -->
        <el-alert v-if="testRunResult.validation_warnings?.length" type="warning" :closable="false" class="margin-top-10">
          <template #title>代码警告：</template>
          <ul>
            <li v-for="(w, i) in testRunResult.validation_warnings" :key="i">{{ w }}</li>
          </ul>
        </el-alert>

        <!-- 控制台输出 -->
        <el-divider>控制台输出</el-divider>
        <pre class="console-output">{{ testRunResult.output || '(无输出)' }}</pre>

        <!-- 生成的信号 -->
        <el-divider>生成的信号</el-divider>
        <el-table v-if="testRunResult.signals?.length" :data="testRunResult.signals" stripe size="small">
          <el-table-column prop="action" label="动作" width="70">
            <template #default="{ row }">
              <el-tag :type="row.action === 'buy' ? 'success' : row.action === 'sell' ? 'danger' : 'info'" size="small">
                {{ row.action === 'buy' ? '买入' : row.action === 'sell' ? '卖出' : '观察' }}
              </el-tag>
            </template>
          </el-table-column>
          <el-table-column prop="stock_code" label="股票代码" width="110" />
          <el-table-column prop="stock_name" label="股票名称" width="120" />
          <el-table-column prop="buy_price" label="参考价格" width="90" />
          <el-table-column prop="reason" label="原因" min-width="200" show-overflow-tooltip />
        </el-table>
        <el-empty v-else description="未生成任何信号" :image-size="60" />

        <template #footer>
          <el-button @click="showTestRunResultDialog = false">关闭</el-button>
          <el-button type="primary" @click="showTestRunResultDialog = false; showTestRunDialog = true" :disabled="testRunning">重新运行</el-button>
        </template>
      </el-dialog>

      <!-- 告警策略创建/编辑对话框 -->
      <el-dialog v-model="alertStrategyDialogVisible" :title="alertStrategyDialogTitle" width="900px">
        <el-table :data="alertStrategyList" stripe style="width: 100%" max-height="250">
          <el-table-column prop="name" label="策略名称" width="150" />
          <el-table-column label="策略类型" width="120">
            <template #default="{ row }">{{ formatAlertStrategyType(row.strategy_type) }}</template>
          </el-table-column>
          <el-table-column label="方向" width="70">
            <template #default="{ row }">
              <el-tag :type="row.action === 'buy' ? 'danger' : 'success'" size="small">{{ row.action === 'buy' ? '买入' : '卖出' }}</el-tag>
            </template>
          </el-table-column>
          <el-table-column label="状态" width="70">
            <template #default="{ row }">
              <el-tag :type="row.enabled ? 'success' : 'info'" size="small">{{ row.enabled ? '启用' : '停用' }}</el-tag>
            </template>
          </el-table-column>
          <el-table-column prop="cooldown_seconds" label="冷却(秒)" width="90" />
          <el-table-column label="操作" width="120">
            <template #default="{ row }">
              <el-button size="small" @click="editAlertStrategy(row)">编辑</el-button>
              <el-button size="small" type="danger" @click="deleteAlertStrategy(row.id)">删除</el-button>
            </template>
          </el-table-column>
        </el-table>

        <el-divider />
        <el-form :model="alertStrategyForm" label-width="100px">
          <el-form-item label="策略名称"><el-input v-model="alertStrategyForm.name" placeholder="如：突破买入监测" /></el-form-item>
          <el-form-item label="策略类型">
            <el-select v-model="alertStrategyForm.strategy_type" placeholder="选择策略类型">
              <el-option label="价格监测" value="price_monitor" />
              <el-option label="涨跌幅监测" value="change_pct_monitor" />
              <el-option label="交易量监测" value="volume_monitor" />
            </el-select>
          </el-form-item>
          <el-form-item label="操作方向">
            <el-radio-group v-model="alertStrategyForm.action">
              <el-radio label="buy">买入</el-radio>
              <el-radio label="sell">卖出</el-radio>
            </el-radio-group>
          </el-form-item>
          <el-form-item label="目标股票">
            <el-radio-group v-model="alertStrategyForm.target_mode">
              <el-radio label="all">全部 watchlist</el-radio>
              <el-radio label="specific">指定代码</el-radio>
            </el-radio-group>
          </el-form-item>
          <el-form-item v-if="alertStrategyForm.target_mode === 'specific'" label="股票代码">
            <el-input v-model="alertStrategyForm.target_stocks_str" placeholder="多个代码用逗号分隔" />
          </el-form-item>
          <el-form-item label="冷却时间"><el-input-number v-model="alertStrategyForm.cooldown_seconds" :min="60" :max="3600" /> 秒</el-form-item>

          <template v-if="alertStrategyForm.strategy_type === 'price_monitor'">
            <el-form-item label="价格方向">
              <el-select v-model="alertStrategyForm.condition_direction">
                <el-option label="突破（高于）" value="above" />
                <el-option label="跌破（低于）" value="below" />
              </el-select>
            </el-form-item>
            <el-form-item label="目标价格">
              <el-input-number v-model="alertStrategyForm.condition_target_price" :precision="2" :min="0" />
            </el-form-item>
          </template>

          <template v-if="alertStrategyForm.strategy_type === 'change_pct_monitor'">
            <el-form-item label="涨跌方向">
              <el-select v-model="alertStrategyForm.condition_direction">
                <el-option label="上涨超过" value="up" />
                <el-option label="下跌超过" value="down" />
              </el-select>
            </el-form-item>
            <el-form-item label="阈值(%)">
              <el-input-number v-model="alertStrategyForm.condition_threshold" :precision="2" :min="0" :max="20" />
            </el-form-item>
          </template>

          <template v-if="alertStrategyForm.strategy_type === 'volume_monitor'">
            <el-form-item label="监测模式">
              <el-select v-model="alertStrategyForm.condition_mode">
                <el-option label="量比模式" value="ratio" />
                <el-option label="绝对量模式" value="absolute" />
              </el-select>
            </el-form-item>
            <el-form-item label="阈值">
              <el-input-number v-model="alertStrategyForm.condition_threshold" :precision="2" :min="0" />
            </el-form-item>
          </template>

          <el-form-item label="启用"><el-switch v-model="alertStrategyForm.enabled" :active-value="1" :inactive-value="0" /></el-form-item>
        </el-form>
        <template #footer>
          <el-button @click="alertStrategyDialogVisible = false">取消</el-button>
          <el-button type="primary" @click="saveAlertStrategy">保存</el-button>
        </template>
      </el-dialog>
    </el-main>
  </div>
</template>

<script setup>
import { ref, reactive, computed, onMounted } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { Plus, Refresh, MagicStick } from '@element-plus/icons-vue'
import { useAccountStore } from '../stores/account'
import NavBar from '../components/NavBar.vue'
import ConditionTree from '../components/ConditionTree.vue'

const accountStore = useAccountStore()
const currentAccountId = computed(() => accountStore.currentAccountId)
const currentAccount = computed(() => accountStore.currentAccount)

const activeTab = ref('position')
const activeCodeTab = ref('screening_code')

// 持仓策略
const positionStrategy = reactive({
  max_total_position_pct: 0.80,
  max_single_position_pct: 0.15,
  cash_reserve_pct: 0.20
})
const savingPosition = ref(false)

// 持仓调整规则
const positionRules = ref([])
const loadingRules = ref(false)
const showRuleDialog = ref(false)
const creatingRule = ref(false)
const translating = ref(false)  // LLM翻译中状态
const newRule = reactive({
  natural_description: '',       // 用户自然语言描述
  trigger_expression: '',         // LLM翻译后的表达式
  trigger_description: '',        // LLM翻译后的描述
  target_max_total_pct: 0.5,
  target_max_single_pct: 0,
  priority: 0
})

// 选股策略（配置型）
const screeningStrategies = ref([])
const configScreeningStrategies = computed(() => screeningStrategies.value)
const loadingScreening = ref(false)
const showCreateScreeningDialog = ref(false)
const showScreeningDetailDialog = ref(false)
const selectedScreening = ref(null)
const showEditScreeningDialog = ref(false)
const savingEditScreening = ref(false)
const editingScreening = reactive({
  id: null, name: '', description: '',
  filters: {}, markets: [], buyConditions: []
})
const showCreateCodeDialog = ref(false)
const editingCodeId = ref(null)
const validatingCode = ref(false)
const savingCodeStrategy = ref(false)
const codeValidationResult = ref(null)
const kronosStatus = ref(null)  // { available, error, device }
const codeFileInput = ref(null)
const importedFileName = ref(null)
const codeStrategyForm = reactive({ name: '', description: '', code: '', function_name: 'run', code_scope: 'screening' })

// 代码型策略（独立数据源）
const codeStrategies = ref([])
const screeningCodeStrategies = computed(() => codeStrategies.value.filter(s => s.code_scope !== 'trading'))
const tradingCodeStrategies = computed(() => codeStrategies.value.filter(s => s.code_scope === 'trading'))
const loadingCodeStrategies = ref(false)
const showCodeDetailDialog = ref(false)
const selectedCodeStrategy = ref(null)

// 卖出策略选项（用于下拉选择）
const sellStrategies = ref([])
const sellStrategyOptionsLoaded = ref(false)

// 策略试运行
const candidateGroups = ref([])
const showTestRunDialog = ref(false)
const showTestRunResultDialog = ref(false)
const testRunStrategyName = ref('')
const testRunStrategyId = ref(null)
const testRunning = ref(false)
const testRunForm = reactive({ groupId: null, stockCodes: '' })
const testRunResult = reactive({
  success: false,
  signals: [],
  signal_count: 0,
  output: '',
  error: null,
  duration_ms: 0,
  validation_warnings: [],
  kronos_unavailable: false,
  kronos_error: null,
})

// LLM生成
const showLLMDialog = ref(false)
const generating = ref(false)
const llmStrategy = reactive({
  description: ''
})

// 交易策略
const tradingStrategies = ref([])
const loadingTrading = ref(false)
const searchStockCode = ref('')
const showTradingDialog = ref(false)
const showEditTradingDialog = ref(false)
const savingTrading = ref(false)
const newTrading = reactive({
  stock_code: '',
  entry_price: 0,
  stop_loss_price: 0,
  stop_loss_pct: 0.05,
  take_profit_price: 0,
  take_profit_pct: 0.15,
  max_trade_quantity: 0
})
const editingTrading = reactive({
  stock_code: '',
  entry_price: 0,
  stop_loss_price: 0,
  stop_loss_pct: 0,
  take_profit_price: 0,
  take_profit_pct: 0,
  max_trade_quantity: 0
})

const filteredTradingStrategies = computed(() => {
  if (!searchStockCode.value) return tradingStrategies.value
  return tradingStrategies.value.filter(s =>
    s.stock_code.toLowerCase().includes(searchStockCode.value.toLowerCase())
  )
})

// 告警策略 CRUD
const alertStrategyDialogVisible = ref(false)
const alertStrategyList = ref([])
const loadingAlertStrategies = ref(false)
const alertStrategyForm = reactive({
  id: null,
  name: '',
  strategy_type: 'price_monitor',
  action: 'buy',
  target_mode: 'all',
  target_stocks_str: '',
  cooldown_seconds: 300,
  condition_direction: 'above',
  condition_target_price: 0,
  condition_threshold: 0,
  condition_mode: 'ratio',
  enabled: 1,
})

const alertStrategyDialogTitle = computed(() => alertStrategyForm.id ? '编辑告警策略' : '新建告警策略')

const showAlertStrategyDialog = async () => {
  alertStrategyForm.id = null
  alertStrategyForm.name = ''
  alertStrategyForm.strategy_type = 'price_monitor'
  alertStrategyForm.action = 'buy'
  alertStrategyForm.target_mode = 'all'
  alertStrategyForm.target_stocks_str = ''
  alertStrategyForm.cooldown_seconds = 300
  alertStrategyForm.condition_direction = 'above'
  alertStrategyForm.condition_target_price = 0
  alertStrategyForm.condition_threshold = 0
  alertStrategyForm.condition_mode = 'ratio'
  alertStrategyForm.enabled = 1
  await loadAlertStrategies()
  alertStrategyDialogVisible.value = true
}

const saveAlertStrategy = async () => {
  if (!alertStrategyForm.name) {
    ElMessage.warning('请输入策略名称')
    return
  }

  let conditions = []
  if (alertStrategyForm.strategy_type === 'price_monitor') {
    conditions = [{ type: 'price', direction: alertStrategyForm.condition_direction, target_price: alertStrategyForm.condition_target_price }]
  } else if (alertStrategyForm.strategy_type === 'change_pct_monitor') {
    conditions = [{ type: 'change_pct', direction: alertStrategyForm.condition_direction, threshold: alertStrategyForm.condition_threshold }]
  } else if (alertStrategyForm.strategy_type === 'volume_monitor') {
    conditions = [{ type: 'volume', mode: alertStrategyForm.condition_mode, threshold: alertStrategyForm.condition_threshold }]
  }

  const target_stocks = alertStrategyForm.target_mode === 'specific'
    ? alertStrategyForm.target_stocks_str.split(',').map(s => s.trim()).filter(s => s)
    : null

  const payload = {
    name: alertStrategyForm.name,
    strategy_type: alertStrategyForm.strategy_type,
    conditions: JSON.stringify(conditions),
    action: alertStrategyForm.action,
    target_stocks: target_stocks ? JSON.stringify(target_stocks) : null,
    cooldown_seconds: alertStrategyForm.cooldown_seconds,
    enabled: alertStrategyForm.enabled,
  }

  try {
    const url = alertStrategyForm.id
      ? `/api/v1/ui/${currentAccountId.value}/trading-strategies/${alertStrategyForm.id}`
      : `/api/v1/ui/${currentAccountId.value}/trading-strategies`
    const method = alertStrategyForm.id ? 'PUT' : 'POST'
    const resp = await fetch(url, { method, headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) })
    const data = await resp.json()
    if (data.success) {
      ElMessage.success(alertStrategyForm.id ? '策略已更新' : '策略已创建')
      alertStrategyDialogVisible.value = false
      loadAlertStrategies()
    } else {
      ElMessage.error(data.message || '保存失败')
    }
  } catch (error) {
    ElMessage.error('保存失败: ' + error.message)
  }
}

const loadAlertStrategies = async () => {
  loadingAlertStrategies.value = true
  try {
    const resp = await fetch(`/api/v1/ui/${currentAccountId.value}/trading-strategies`)
    const data = await resp.json()
    alertStrategyList.value = data.strategies || []
  } catch (error) {
    console.error('加载告警策略列表失败:', error)
  } finally {
    loadingAlertStrategies.value = false
  }
}

const formatAlertStrategyType = (type) => {
  const map = { price_monitor: '价格监测', change_pct_monitor: '涨跌幅监测', volume_monitor: '交易量监测' }
  return map[type] || type
}

const editAlertStrategy = (row) => {
  alertStrategyForm.id = row.id
  alertStrategyForm.name = row.name
  alertStrategyForm.strategy_type = row.strategy_type
  alertStrategyForm.action = row.action
  alertStrategyForm.target_mode = row.target_stocks ? 'specific' : 'all'
  try {
    const stocks = JSON.parse(row.target_stocks || 'null')
    alertStrategyForm.target_stocks_str = Array.isArray(stocks) ? stocks.join(',') : ''
  } catch {
    alertStrategyForm.target_stocks_str = ''
  }
  alertStrategyForm.cooldown_seconds = row.cooldown_seconds || 300
  alertStrategyForm.enabled = row.enabled
  try {
    const conditions = JSON.parse(row.conditions || '[]')
    const cond = conditions[0] || {}
    if (cond.type === 'price') {
      alertStrategyForm.condition_direction = cond.direction || 'above'
      alertStrategyForm.condition_target_price = cond.target_price || 0
    } else if (cond.type === 'change_pct') {
      alertStrategyForm.condition_direction = cond.direction || 'up'
      alertStrategyForm.condition_threshold = cond.threshold || 0
    } else if (cond.type === 'volume') {
      alertStrategyForm.condition_mode = cond.mode || 'ratio'
      alertStrategyForm.condition_threshold = cond.threshold || 0
    }
  } catch {}
  alertStrategyDialogVisible.value = true
}

const deleteAlertStrategy = async (id) => {
  try {
    await ElMessageBox.confirm('确定删除该策略？', '确认删除', { type: 'warning' })
    const resp = await fetch(`/api/v1/ui/${currentAccountId.value}/trading-strategies/${id}`, { method: 'DELETE' })
    const data = await resp.json()
    if (data.success) {
      ElMessage.success('策略已删除')
      loadAlertStrategies()
    } else {
      ElMessage.error(data.message || '删除失败')
    }
  } catch (error) {
    if (error !== 'cancel') {
      ElMessage.error('删除失败: ' + error.message)
    }
  }
}

// 加载所有数据
const loadAllData = async () => {
  await loadPositionStrategy()
  await loadPositionRules()
  await loadScreeningStrategies()
  await loadCodeStrategies()
  await loadTradingStrategies()
  await loadAlertStrategies()
  await loadCandidateGroups()
}

// 加载持仓策略
const loadPositionStrategy = async () => {
  try {
    const res = await fetch(`/api/v1/ui/accounts/${currentAccountId.value}/position-strategy`)
    const data = await res.json()
    if (data.success) {
      positionStrategy.max_total_position_pct = data.data.max_total_position_pct
      positionStrategy.max_single_position_pct = data.data.max_single_position_pct
      positionStrategy.cash_reserve_pct = data.data.cash_reserve_pct
    }
  } catch (error) {
    console.error('加载持仓策略失败:', error)
  }
}

// 保存持仓策略
const savePositionStrategy = async () => {
  savingPosition.value = true
  try {
    const res = await fetch(`/api/v1/ui/accounts/${currentAccountId.value}/position-strategy`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        max_total_position_pct: positionStrategy.max_total_position_pct,
        max_single_position_pct: positionStrategy.max_single_position_pct,
        cash_reserve_pct: positionStrategy.cash_reserve_pct
      })
    })
    const data = await res.json()
    if (data.success) {
      ElMessage.success('持仓策略保存成功')
    } else {
      ElMessage.error(data.message || '保存失败')
    }
  } catch (error) {
    ElMessage.error('保存失败：' + error.message)
  } finally {
    savingPosition.value = false
  }
}

// 加载持仓调整规则
const loadPositionRules = async () => {
  loadingRules.value = true
  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/position-rules`)
    const data = await res.json()
    if (data.success) {
      positionRules.value = data.rules
    }
  } catch (error) {
    console.error('加载调整规则失败:', error)
  } finally {
    loadingRules.value = false
  }
}

// LLM翻译触发条件
const translateCondition = async () => {
  if (!newRule.natural_description) {
    ElMessage.warning('请输入触发条件描述')
    return
  }

  translating.value = true
  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/position-rules/translate`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ description: newRule.natural_description })
    })
    const data = await res.json()
    if (data.success) {
      newRule.trigger_expression = data.translated.expression
      newRule.trigger_description = data.translated.description
      ElMessage.success('翻译成功')
    } else {
      // 显示可用参数提示
      if (data.available_params_hint) {
        ElMessageBox.alert(data.available_params_hint, '可用参数参考', {
          confirmButtonText: '知道了',
          customClass: 'params-hint-dialog'
        })
      } else {
        ElMessage.error(data.error || '翻译失败')
      }
    }
  } catch (error) {
    ElMessage.error('翻译失败：' + error.message)
  } finally {
    translating.value = false
  }
}

// 创建规则
const createRule = async () => {
  if (!newRule.trigger_expression) {
    ElMessage.warning('请先翻译触发条件')
    return
  }

  creatingRule.value = true
  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/position-rules`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        trigger_expression: newRule.trigger_expression,
        trigger_description: newRule.trigger_description,
        target_max_total_pct: newRule.target_max_total_pct,
        target_max_single_pct: newRule.target_max_single_pct,
        priority: newRule.priority
      })
    })
    const data = await res.json()
    if (data.success) {
      ElMessage.success('规则创建成功')
      showRuleDialog.value = false
      // 重置表单
      newRule.natural_description = ''
      newRule.trigger_expression = ''
      newRule.trigger_description = ''
      newRule.target_max_total_pct = 0.5
      newRule.target_max_single_pct = 0
      newRule.priority = 0
      await loadPositionRules()
    } else {
      ElMessage.error(data.detail || '创建失败')
    }
  } catch (error) {
    ElMessage.error('创建失败：' + error.message)
  } finally {
    creatingRule.value = false
  }
}

// 启用/停用规则
const toggleRule = async (rule) => {
  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/position-rules/${rule.id}/toggle`, {
      method: 'POST'
    })
    const data = await res.json()
    if (data.success) {
      ElMessage.success(data.message)
      await loadPositionRules()
    }
  } catch (error) {
    ElMessage.error('操作失败')
  }
}

// 删除规则
const deleteRule = async (rule) => {
  try {
    await ElMessageBox.confirm('确定删除该规则？', '确认删除', { type: 'warning' })
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/position-rules/${rule.id}`, {
      method: 'DELETE'
    })
    const data = await res.json()
    if (data.success) {
      ElMessage.success('规则已删除')
      await loadPositionRules()
    }
  } catch (error) {
    if (error !== 'cancel') {
      ElMessage.error('删除失败')
    }
  }
}

const resetCodeForm = () => {
  editingCodeId.value = null
  codeStrategyForm.name = ''
  codeStrategyForm.description = ''
  codeStrategyForm.code = ''
  codeStrategyForm.function_name = 'run'
  codeValidationResult.value = null
  importedFileName.value = null
}

// 代码型策略
const editCodeStrategy = (row) => {
  editingCodeId.value = row.id
  codeStrategyForm.name = row.name
  codeStrategyForm.description = row.description || ''
  codeStrategyForm.code = row.code || ''
  codeStrategyForm.function_name = row.function_name || 'run'
  codeStrategyForm.code_scope = row.code_scope || 'screening'
  showCreateCodeDialog.value = true
  codeValidationResult.value = null
  importedFileName.value = null
  // Switch to the correct sub-tab
  activeCodeTab.value = row.code_scope === 'trading' ? 'trading_code' : 'screening_code'
}

const viewCodeStrategy = (row) => {
  selectedCodeStrategy.value = row
  showCodeDetailDialog.value = true
}

const validateCode = async () => {
  if (!codeStrategyForm.code) {
    ElMessage.warning('请先输入代码')
    return
  }
  validatingCode.value = true
  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/strategies/validate-code`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ code: codeStrategyForm.code })
    })
    codeValidationResult.value = await res.json()
  } catch (e) {
    codeValidationResult.value = { valid: false, error: '请求失败' }
  } finally {
    validatingCode.value = false
  }
}

const saveCodeStrategy = async () => {
  if (!codeStrategyForm.name.trim()) {
    ElMessage.warning('请输入策略名称')
    return
  }
  if (!codeStrategyForm.code.trim()) {
    ElMessage.warning('请输入策略代码')
    return
  }
  savingCodeStrategy.value = true
  try {
    const url = editingCodeId.value
      ? `/api/v1/ui/${currentAccountId.value}/code-strategies/${editingCodeId.value}`
      : `/api/v1/ui/${currentAccountId.value}/strategies`
    const method = editingCodeId.value ? 'PUT' : 'POST'
    const body = {
      name: codeStrategyForm.name,
      description: codeStrategyForm.description,
      strategy_type: 'python',
      code_type: 'python',
      code_scope: codeStrategyForm.code_scope,
      code: codeStrategyForm.code,
      function_name: codeStrategyForm.function_name,
      status: 'draft',
    }
    const res = await fetch(url, { method, headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) })
    const data = await res.json()
    if (data.success || res.ok) {
      ElMessage.success(editingCodeId.value ? '策略已更新' : '策略已创建')
      showCreateCodeDialog.value = false
      editingCodeId.value = null
      codeStrategyForm.name = ''
      codeStrategyForm.description = ''
      codeStrategyForm.code = ''
      codeStrategyForm.function_name = 'run'
      codeStrategyForm.code_scope = 'screening'
      codeValidationResult.value = null
      importedFileName.value = null
      if (codeFileInput.value) codeFileInput.value.value = ''
      await loadCodeStrategies()
    } else {
      ElMessage.error(data.detail || '保存失败')
    }
  } catch (e) {
    ElMessage.error('保存失败')
  } finally {
    savingCodeStrategy.value = false
  }
}

// 加载卖出策略选项（用于选股策略的下拉关联）
const loadSellStrategyOptions = async () => {
  if (sellStrategyOptionsLoaded.value) return
  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/sell-strategies`)
    const data = await res.json()
    if (data.success) {
      sellStrategies.value = data.strategies
      sellStrategyOptionsLoaded.value = true
    }
  } catch (e) {
    console.error('加载卖出策略选项失败:', e)
  }
}

// 保存选股策略的卖出策略关联
const saveSellStrategyLink = async (row) => {
  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/strategies/${row.id}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sell_strategy_id: row.sell_strategy_id || null }),
    })
    const data = await res.json()
    if (data.success) {
      ElMessage.success(row.sell_strategy_id ? '卖出策略已设置' : '卖出策略已清除')
    } else {
      ElMessage.error(data.detail || '保存失败')
      // 恢复原值
      await loadCodeStrategies()
    }
  } catch (e) {
    ElMessage.error('保存失败')
    await loadCodeStrategies()
  }
}

// 代码型策略文件导入
const handleCodeFileImport = async (event) => {
  const file = event.target.files[0]
  if (!file) return
  if (!file.name.endsWith('.py') && !file.name.endsWith('.pyw')) {
    ElMessage.warning('请选择 .py 或 .pyw 文件')
    event.target.value = ''
    return
  }
  try {
    const text = await file.text()
    codeStrategyForm.code = text
    importedFileName.value = file.name
    // 尝试从文件名提取函数名（如 tail_screener → run）
    const baseName = file.name.replace(/\.(py|pyw)$/, '')
    if (codeStrategyForm.function_name === 'run') {
      // 检查文件中是否有 run 函数，没有则保持默认
      if (!/def\s+run\s*\(/.test(text)) {
        // 尝试找文件中第一个 def 函数
        const match = text.match(/def\s+(\w+)\s*\(/)
        if (match) codeStrategyForm.function_name = match[1]
      }
    }
    ElMessage.success(`已导入 ${file.name}，${text.split('\n').length} 行`)
  } catch (e) {
    ElMessage.error('文件读取失败')
  }
  event.target.value = ''
}

// 加载选股策略（配置型）
const loadScreeningStrategies = async () => {
  loadingScreening.value = true
  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/strategies`)
    const data = await res.json()
    if (data.strategies) {
      screeningStrategies.value = data.strategies.filter(s => s.strategy_type === 'screening')
    }
  } catch (error) {
    console.error('加载选股策略失败:', error)
  } finally {
    loadingScreening.value = false
  }
}

// 加载代码型策略（独立数据源）
const loadCodeStrategies = async () => {
  loadingCodeStrategies.value = true
  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/code-strategies`)
    const data = await res.json()
    if (data.strategies) {
      codeStrategies.value = data.strategies
    }
  } catch (error) {
    console.error('加载代码策略失败:', error)
  } finally {
    loadingCodeStrategies.value = false
  }
}

// 加载 Kronos 状态
const loadKronosStatus = async () => {
  try {
    const res = await fetch('/api/v1/ui/kronos/status')
    const data = await res.json()
    if (data.success) {
      kronosStatus.value = { available: data.available, error: data.error, device: data.device }
    }
  } catch (e) {
    kronosStatus.value = { available: false, error: '无法连接服务器', device: null }
  }
}

// 加载候选分组
const loadCandidateGroups = async () => {
  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/candidate-groups`)
    const data = await res.json()
    if (data.groups) {
      candidateGroups.value = data.groups
    }
  } catch (error) {
    console.error('加载候选分组失败:', error)
  }
}

// 打开试运行对话框
const testRunStrategy = (row) => {
  testRunStrategyName.value = row.name
  testRunStrategyId.value = row.id
  testRunForm.groupId = null
  testRunForm.stockCodes = ''
  showTestRunDialog.value = true
  // 确保分组已加载
  if (candidateGroups.value.length === 0) {
    loadCandidateGroups()
  }
}

// 执行试运行
const doTestRun = async () => {
  if (!testRunForm.groupId) {
    ElMessage.warning('请选择候选分组')
    return
  }
  testRunning.value = true
  try {
    const strategy = codeStrategies.value.find(s => s.id === testRunStrategyId.value)
    if (!strategy) {
      ElMessage.error('策略不存在')
      return
    }

    // 重置结果
    Object.assign(testRunResult, { success: false, signals: [], signal_count: 0, output: '', error: null, duration_ms: 0, validation_warnings: [] })

    const body = {
      code: strategy.code,
      function_name: strategy.function_name || 'run',
    }
    if (testRunForm.groupId) {
      body.group_id = testRunForm.groupId
    }
    if (testRunForm.stockCodes.trim()) {
      body.test_stocks = testRunForm.stockCodes.split(/[,，\s]+/).map(s => s.trim()).filter(s => s)
    }

    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/strategies/test-run`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    })
    const data = await res.json()
    Object.assign(testRunResult, data)
    showTestRunDialog.value = false
    showTestRunResultDialog.value = true
  } catch (error) {
    testRunResult.success = false
    testRunResult.error = error.message
    showTestRunDialog.value = false
    showTestRunResultDialog.value = true
  } finally {
    testRunning.value = false
  }
}

// 查看选股策略详情
const viewScreeningStrategy = (row) => {
  selectedScreening.value = row
  showScreeningDetailDialog.value = true
}

// 编辑选股策略（名称/描述/条件）
const editScreeningStrategy = (row) => {
  editingScreening.id = row.id
  editingScreening.name = row.name
  editingScreening.description = row.description || ''

  // 解析配置
  const cfg = parseConfig(row.config) || {}
  const filters = cfg.stock_filters || {}

  // 提取字段条件到表单
  editingScreening.filters = { ...filters }

  // 市场范围
  const markets = cfg.markets || ['SH', 'SZ', 'BJ']
  editingScreening.markets = markets

  // 买入条件（字符串数组）
  const buyConds = cfg.buy_conditions || []
  editingScreening.buyConditions = Array.isArray(buyConds)
    ? buyConds.map(c => typeof c === 'string' ? c : JSON.stringify(c))
    : []

  showEditScreeningDialog.value = true
}

// 保存编辑后的选股策略
const saveEditScreening = async () => {
  if (!editingScreening.name) {
    ElMessage.warning('策略名称不能为空')
    return
  }

  // 重建 stock_filters（从表单值中过滤出非空的）
  const stockFilters = {}
  for (const [key, val] of Object.entries(editingScreening.filters)) {
    if (val !== null && val !== undefined && val !== '') {
      stockFilters[key] = val
    }
  }
  // 如果有任何筛选条件，包装为逻辑组
  const stockFiltersNode = Object.keys(stockFilters).length > 0
    ? { logic: 'AND', conditions: Object.entries(stockFilters).map(([field, value]) => ({ field, value })) }
    : {}

  // 市场范围：如果三个全选，不设置 markets 字段（默认全市场）
  const allMarkets = ['SH', 'SZ', 'BJ']
  const markets = editingScreening.markets.length === 3 ? undefined : editingScreening.markets

  // 构建 config
  const config = {
    stock_filters: stockFiltersNode,
    buy_conditions: editingScreening.buyConditions,
  }
  if (markets) config.markets = markets

  savingEditScreening.value = true
  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/strategies/${editingScreening.id}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        name: editingScreening.name,
        description: editingScreening.description,
        config
      })
    })
    const data = await res.json()
    if (data.success) {
      ElMessage.success('策略已更新')
      showEditScreeningDialog.value = false
      await loadScreeningStrategies()
    } else {
      ElMessage.error(data.detail || '更新失败')
    }
  } catch (error) {
    ElMessage.error('更新失败：' + error.message)
  } finally {
    savingEditScreening.value = false
  }
}

// 激活/停用选股策略
const toggleScreeningStrategy = async (row) => {
  try {
    const action = row.status === 'active' ? 'deactivate' : 'activate'
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/strategies/${row.id}/${action}`, {
      method: 'POST'
    })
    const data = await res.json()
    if (data.success) {
      ElMessage.success(data.message)
      await loadScreeningStrategies()
    } else {
      ElMessage.error(data.detail || '操作失败')
    }
  } catch (error) {
    ElMessage.error('操作失败：' + error.message)
  }
}

// 删除选股策略
const deleteScreeningStrategy = async (row) => {
  try {
    await ElMessageBox.confirm('确定删除该策略？', '确认删除', { type: 'warning' })
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/strategies/${row.id}`, {
      method: 'DELETE'
    })
    const data = await res.json()
    if (data.success) {
      ElMessage.success('策略已删除')
      await loadScreeningStrategies()
      // 代码型策略同时刷新代码策略列表
      if (row.strategy_type === 'code' || row.code_scope !== undefined) {
        await loadCodeStrategies()
      }
    }
  } catch (error) {
    if (error !== 'cancel') {
      ElMessage.error('删除失败')
    }
  }
}

// LLM生成策略
const generateLLMStrategy = async () => {
  if (!llmStrategy.description) {
    ElMessage.warning('请输入策略描述')
    return
  }

  generating.value = true
  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/strategies/generate`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ description: llmStrategy.description })
    })
    const data = await res.json()
    if (data.success) {
      // 创建策略
      const createRes = await fetch(`/api/v1/ui/${currentAccountId.value}/strategies`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name: data.strategy.name,
          description: llmStrategy.description,
          strategy_type: 'screening',
          config: data.strategy.config
        })
      })
      const createData = await createRes.json()
      if (createData.success) {
        ElMessage.success('策略生成并创建成功')
        showLLMDialog.value = false
        llmStrategy.description = ''
        await loadScreeningStrategies()
      } else {
        ElMessage.error('创建失败：' + createData.message)
      }
    } else {
      ElMessage.error(data.message || '生成失败')
    }
  } catch (error) {
    ElMessage.error('生成失败：' + error.message)
  } finally {
    generating.value = false
  }
}

// 加载交易策略
const loadTradingStrategies = async () => {
  loadingTrading.value = true
  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/trading-strategies`)
    const data = await res.json()
    if (data.success) {
      tradingStrategies.value = data.strategies
    }
  } catch (error) {
    console.error('加载交易策略失败:', error)
  } finally {
    loadingTrading.value = false
  }
}

// 保存交易策略
const saveTradingStrategy = async () => {
  if (!newTrading.stock_code) {
    ElMessage.warning('请输入股票代码')
    return
  }

  savingTrading.value = true
  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/trading-strategies`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(newTrading)
    })
    const data = await res.json()
    if (data.success) {
      ElMessage.success(data.message)
      showTradingDialog.value = false
      // 重置表单
      newTrading.stock_code = ''
      newTrading.entry_price = 0
      newTrading.stop_loss_price = 0
      newTrading.stop_loss_pct = 0.05
      newTrading.take_profit_price = 0
      newTrading.take_profit_pct = 0.15
      newTrading.max_trade_quantity = 0
      await loadTradingStrategies()
    } else {
      ElMessage.error(data.message || '保存失败')
    }
  } catch (error) {
    ElMessage.error('保存失败：' + error.message)
  } finally {
    savingTrading.value = false
  }
}

// 编辑交易策略
const editTradingStrategy = (row) => {
  editingTrading.stock_code = row.stock_code
  editingTrading.entry_price = row.entry_price
  editingTrading.stop_loss_price = row.stop_loss_price
  editingTrading.stop_loss_pct = row.stop_loss_pct
  editingTrading.take_profit_price = row.take_profit_price
  editingTrading.take_profit_pct = row.take_profit_pct
  editingTrading.max_trade_quantity = row.max_trade_quantity
  showEditTradingDialog.value = true
}

// 更新交易策略
const updateTradingStrategy = async () => {
  savingTrading.value = true
  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/trading-strategies/${editingTrading.stock_code}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(editingTrading)
    })
    const data = await res.json()
    if (data.success) {
      ElMessage.success('策略更新成功')
      showEditTradingDialog.value = false
      await loadTradingStrategies()
    } else {
      ElMessage.error(data.message || '更新失败')
    }
  } catch (error) {
    ElMessage.error('更新失败：' + error.message)
  } finally {
    savingTrading.value = false
  }
}

// 删除交易策略
const deleteTradingStrategy = async (row) => {
  try {
    await ElMessageBox.confirm('确定删除该交易策略？', '确认删除', { type: 'warning' })
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/trading-strategies/${row.stock_code}`, {
      method: 'DELETE'
    })
    const data = await res.json()
    if (data.success) {
      ElMessage.success('策略已删除')
      await loadTradingStrategies()
    }
  } catch (error) {
    if (error !== 'cancel') {
      ElMessage.error('删除失败')
    }
  }
}

// 辅助函数
const getStatusType = (status) => {
  const types = { 'draft': 'info', 'active': 'success', 'inactive': 'warning' }
  return types[status] || 'info'
}

const getStatusText = (status) => {
  const texts = { 'draft': '草稿', 'active': '激活', 'inactive': '停用' }
  return texts[status] || status
}

const parseConfig = (config) => {
  if (!config) return {}
  if (typeof config === 'string') {
    try { return JSON.parse(config) } catch { return {} }
  }
  return config
}

// 规范化条件格式（支持新旧格式）
const normalizeConditions = (conditions) => {
  if (!conditions) return { logic: 'AND', conditions: [] }

  // 新格式（已有logic字段）
  if (conditions.logic && conditions.conditions) {
    return conditions
  }

  // 旧格式：列表 ["cond1", "cond2"]
  if (Array.isArray(conditions)) {
    return { logic: 'AND', conditions: conditions }
  }

  // 旧格式：stock_filters字典 {"field": value}
  if (typeof conditions === 'object') {
    const convertedConditions = []
    for (const [field, value] of Object.entries(conditions)) {
      convertedConditions.push({ field, value })
    }
    return { logic: 'AND', conditions: convertedConditions }
  }

  return { logic: 'AND', conditions: [] }
}

const formatFilter = (value) => {
  if (typeof value === 'object') {
    return JSON.stringify(value)
  }
  return value
}

onMounted(() => {
  loadAllData()
  loadKronosStatus()
  loadSellStrategyOptions()
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

.el-tabs--border-card {
  background: #fff;
}

.hint {
  color: #909399;
  font-size: 12px;
  margin-top: 5px;
}

.margin-top-20 {
  margin-top: 20px;
}

.margin-bottom-15 {
  margin-bottom: 15px;
}

.code-block {
  background: #1e1e1e;
  color: #d4d4d4;
  padding: 16px;
  border-radius: 6px;
  font-family: 'Consolas', 'Monaco', monospace;
  font-size: 13px;
  line-height: 1.5;
  overflow-x: auto;
  white-space: pre-wrap;
  word-break: break-all;
  max-height: 500px;
}

.margin-top-10 {
  margin-top: 10px;
}

.console-output {
  background: #1e1e1e;
  color: #d4d4d4;
  padding: 12px;
  border-radius: 6px;
  font-family: 'Consolas', 'Monaco', monospace;
  font-size: 12px;
  line-height: 1.4;
  max-height: 300px;
  overflow-y: auto;
  white-space: pre-wrap;
}

.code-hints {
  font-size: 12px;
  color: #606266;
  line-height: 1.6;
}
.code-hints p { margin: 6px 0 4px; }
.code-hints ul { margin: 0 0 4px 16px; padding: 0; }
.code-hints code {
  background: #f0f2f5;
  padding: 1px 4px;
  border-radius: 3px;
  font-family: 'Consolas', 'Monaco', monospace;
  font-size: 11px;
  color: #E6A23C;
}
.cron-quick-btn { transition: color 0.2s; }
.cron-quick-btn:hover { color: #66b1ff !important; }
</style>