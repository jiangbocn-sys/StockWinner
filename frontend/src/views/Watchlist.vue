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
          <el-button type="success" @click="showCreateGroupDialog = true">
            <el-icon><Plus /></el-icon>
            新建候选组
          </el-button>
          <el-button :disabled="!selectedGroupId" @click="startImport">
            <el-icon><Upload /></el-icon>
            导入文件
          </el-button>
          <el-button @click="loadAll" :loading="wlStore.loaded === false">
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
        </div>
      </el-card>

      <!-- 左右分栏：候选组 + 股票列表（宽度可调） -->
      <div class="main-row resizable-split">
        <!-- 左侧：候选组列表 -->
        <div class="left-panel" :style="{ width: leftPanelWidth + 'px' }">
          <el-card class="group-card">
            <template #header>
              <div class="card-header">
                <span>候选组</span>
                <el-button size="small" text @click="groupSortOrder = groupSortOrder === 'asc' ? 'desc' : 'asc'">
                  {{ groupSortOrder === 'asc' ? '↑' : '↓' }}
                </el-button>
              </div>
            </template>

            <div class="group-list" v-loading="groupsLoading">
              <div
                v-for="group in sortedGroups"
                :key="group.id"
                class="group-item"
                :class="{ active: selectedGroupId === group.id }"
                @click="selectGroup(group)"
              >
                <div class="group-info">
                  <el-tag :type="group.group_type === 'manual' ? 'success' : 'primary'" size="small" class="group-tag">
                    {{ group.group_type === 'manual' ? '自建' : '策略' }}
                  </el-tag>
                  <span class="group-name">{{ group.name }}</span>
                </div>
                <span class="group-count">{{ group.stock_count }} 只</span>
                <el-dropdown trigger="click" class="group-actions" @command="(cmd) => handleGroupAction(cmd, group)">
                  <el-icon><MoreFilled /></el-icon>
                  <template #dropdown>
                    <el-dropdown-menu>
                      <el-dropdown-item command="rename">重命名</el-dropdown-item>
                      <el-dropdown-item command="associate">关联策略</el-dropdown-item>
                      <el-dropdown-item command="schedule">调度设置</el-dropdown-item>
                      <el-dropdown-item command="delete" divided>删除</el-dropdown-item>
                    </el-dropdown-menu>
                  </template>
                </el-dropdown>
              </div>

              <el-empty v-if="!groupsLoading && candidateGroups.length === 0" description="暂无候选组" :image-size="60" />
            </div>
          </el-card>
        </div>

        <!-- 拖拽分隔条 -->
        <div class="split-handle" @mousedown="startResize"></div>

        <!-- 右侧：候选股列表 -->
        <div class="right-panel" :style="{ flex: 1 }">
          <el-card>
            <template #header>
              <div class="card-header stock-toolbar">
                <span>{{ currentGroup?.label || '候选股票' }}</span>
                <el-space wrap>
                  <el-radio-group v-model="filterStatus" size="small" @change="loadCurrentGroupStocks">
                    <el-radio-button value="">全部</el-radio-button>
                    <el-radio-button value="pending">待交易</el-radio-button>
                    <el-radio-button value="watching">观察中</el-radio-button>
                    <el-radio-button value="bought">已买入</el-radio-button>
                    <el-radio-button value="sold">已卖出</el-radio-button>
                  </el-radio-group>
                  <el-autocomplete
                    v-model="stockQuickSearch"
                    :fetch-suggestions="searchStocksForAdd"
                    placeholder="输入代码/名称定位股票"
                    :debounce="300"
                    size="small"
                    style="width: 200px;"
                    clearable
                    @select="onQuickSearchSelect"
                  >
                    <template #default="{ item }">
                      <span style="font-weight:500;min-width:90px">{{ item.stock_code }}</span>
                      <span style="color:#909399;margin-left:8px">{{ item.stock_name }}</span>
                    </template>
                  </el-autocomplete>
                  <el-button v-if="selectedGroupId" type="success" size="small" @click="showAddStockDialog = true">
                    <el-icon><Plus /></el-icon>
                    添加
                  </el-button>
                  <el-button type="primary" size="small" :disabled="selectedStocks.length !== 1" @click="editSelected">
                    <el-icon><Edit /></el-icon>
                    编辑{{ selectedStocks.length === 1 ? ` (${selectedStocks[0].stock_code})` : '' }}
                  </el-button>
                  <el-button type="danger" size="small" :disabled="selectedStocks.length === 0" @click="batchRemoveSelected">
                    <el-icon><Delete /></el-icon>
                    移除{{ selectedStocks.length > 0 ? ` (${selectedStocks.length})` : '' }}
                  </el-button>
                  <el-button type="info" size="small" :disabled="selectedStocks.length !== 1" @click="handleDsaAnalysisSelected" :loading="dsaAnalyzing">
                    DSA 分析{{ selectedStocks.length === 1 ? ` (${selectedStocks[0].stock_code})` : '' }}
                  </el-button>
                  <el-button type="warning" size="small" :disabled="selectedStocks.length === 0" @click="showBatchStatusDialog = true">
                    改状态{{ selectedStocks.length > 0 ? ` (${selectedStocks.length})` : '' }}
                  </el-button>
                  <el-dropdown @command="handleExportWatchlist">
                    <el-button type="success" size="small"><el-icon><Download /></el-icon>导出</el-button>
                    <template #dropdown>
                      <el-dropdown-menu>
                        <el-dropdown-item command="csv">CSV</el-dropdown-item>
                        <el-dropdown-item command="json">JSON</el-dropdown-item>
                        <el-dropdown-item command="md">Markdown</el-dropdown-item>
                        <el-dropdown-item command="txt">TXT</el-dropdown-item>
                        <el-dropdown-item command="excel">Excel</el-dropdown-item>
                      </el-dropdown-menu>
                    </template>
                  </el-dropdown>
                </el-space>
              </div>
            </template>

            <el-table :data="paginatedStocks" stripe style="width: 100%" v-loading="stocksLoading" @selection-change="handleStockSelectionChange" @row-dblclick="showKline" @sort-change="onTableSortChange" :cell-style="tableCellStyle">
              <el-table-column type="selection" width="50" />
              <el-table-column prop="stock_code" label="股票代码" width="120" sortable />
              <el-table-column prop="stock_name" label="股票名称" width="120" sortable />
              <el-table-column prop="reason" label="入选原因" min-width="180" show-overflow-tooltip />
              <el-table-column prop="trigger_price" label="触发价" width="90" align="right" sortable>
                <template #default="{ row }">¥{{ row.trigger_price?.toFixed(2) }}</template>
              </el-table-column>
              <el-table-column prop="current_price" label="现价" width="90" align="right" sortable>
                <template #default="{ row }">
                  <span
                    :class="{
                      'price-up-bg': priceChangeHighlight.get(row.stock_code) === 'up',
                      'price-down-bg': priceChangeHighlight.get(row.stock_code) === 'down'
                    }"
                    :style="{ color: row.current_price > row.trigger_price ? '#f56c6c' : row.current_price > 0 && row.current_price < row.trigger_price ? '#67c23a' : '' }"
                  >
                    {{ row.current_price > 0 ? '¥' + row.current_price.toFixed(2) : '-' }}
                  </span>
                </template>
              </el-table-column>
              <el-table-column prop="change_pct" label="涨跌" width="85" align="right" sortable>
                <template #default="{ row }">
                  <span :style="{ color: row.change_pct > 0 ? '#f56c6c' : row.change_pct < 0 ? '#67c23a' : '' }">
                    {{ row.change_pct ? (row.change_pct > 0 ? '+' : '') + row.change_pct.toFixed(2) + '%' : '-' }}
                  </span>
                </template>
              </el-table-column>
              <el-table-column prop="stop_loss_price" label="止损价" width="90" align="right">
                <template #default="{ row }">¥{{ row.stop_loss_price?.toFixed(2) }}</template>
              </el-table-column>
              <el-table-column prop="take_profit_price" label="止盈价" width="90" align="right">
                <template #default="{ row }">¥{{ row.take_profit_price?.toFixed(2) }}</template>
              </el-table-column>
              <el-table-column prop="position_quantity" label="数量" width="80" align="right">
                <template #default="{ row }">
                  {{ row.position_quantity || 0 }}
                </template>
              </el-table-column>
              <el-table-column prop="status" label="状态" width="90" sortable>
                <template #default="{ row }">
                  <el-tag :type="getStatusType(row.status)" size="small">{{ getStatusText(row.status) }}</el-tag>
                </template>
              </el-table-column>
              <el-table-column prop="signal_type" label="信号" width="80" align="center">
                <template #default="{ row }">
                  <el-tag v-if="row.signal_type === 'buy'" type="danger" size="small">买入</el-tag>
                  <el-tag v-else-if="row.signal_type === 'sell'" type="success" size="small">卖出</el-tag>
                  <span v-else>-</span>
                </template>
              </el-table-column>
              <el-table-column prop="created_at" label="入选日期" width="110" sortable>
                <template #default="{ row }">
                  {{ formatTime(row.created_at) }}
                </template>
              </el-table-column>
            </el-table>

            <div class="pagination-bar" v-if="currentStocks.length > pageSize">
              <el-pagination
                v-model:current-page="currentPage"
                :page-size="pageSize"
                :total="currentStocks.length"
                layout="prev, pager, next, total"
                small
              />
            </div>

            <el-empty v-if="!stocksLoading && currentStocks.length === 0" description="暂无候选股票，请添加或运行选股" />
          </el-card>
        </div>
      </div>

      <!-- 新建候选组对话框 -->
      <el-dialog v-model="showCreateGroupDialog" title="新建候选组" width="450px">
        <el-form :model="createGroupForm" label-width="100px">
          <el-form-item label="组名" required>
            <el-input v-model="createGroupForm.name" placeholder="如：关注组A、科技股" />
          </el-form-item>
          <el-form-item label="关联策略">
            <el-select v-model="createGroupForm.screeningStrategyId" placeholder="可选" clearable style="width: 100%">
              <el-option
                v-for="s in screeningStrategies"
                :key="s.id"
                :label="s.name"
                :value="s.id"
              />
            </el-select>
          </el-form-item>
        </el-form>
        <template #footer>
          <el-button @click="showCreateGroupDialog = false">取消</el-button>
          <el-button type="primary" @click="createGroup" :loading="creatingGroup">创建</el-button>
        </template>
      </el-dialog>

      <!-- 重命名对话框 -->
      <el-dialog v-model="showRenameDialog" title="重命名候选组" width="400px">
        <el-form :model="renameForm" label-width="80px">
          <el-form-item label="新名称" required>
            <el-input v-model="renameForm.name" />
          </el-form-item>
        </el-form>
        <template #footer>
          <el-button @click="showRenameDialog = false">取消</el-button>
          <el-button type="primary" @click="renameGroup" :loading="renamingGroup">保存</el-button>
        </template>
      </el-dialog>

      <!-- 关联策略对话框 -->
      <el-dialog v-model="showAssociateDialog" title="关联选股策略" width="450px">
        <el-form :model="associateForm" label-width="100px">
          <el-form-item label="选股策略">
            <el-select v-model="associateForm.screeningStrategyId" placeholder="选择策略" clearable style="width: 100%">
              <el-option
                v-for="s in screeningStrategies"
                :key="s.id"
                :label="s.name"
                :value="s.id"
              />
            </el-select>
          </el-form-item>
        </el-form>
        <template #footer>
          <el-button @click="showAssociateDialog = false">取消</el-button>
          <el-button type="primary" @click="associateStrategy" :loading="associatingStrategy">确认</el-button>
        </template>
      </el-dialog>

      <!-- 手动添加股票对话框 -->
      <el-dialog v-model="showAddStockDialog" title="添加候选股票" width="550px">
        <el-form :model="addStockForm" label-width="100px">
          <el-form-item label="选择股票" required>
            <el-select
              v-model="addStockForm.selectedStockCode"
              filterable remote
              :remote-method="searchStocksForAdd"
              :loading="searchingStocks"
              placeholder="输入代码、拼音首字母或名称搜索"
              style="width: 100%"
              value-key="stock_code"
              clearable
            >
              <el-option
                v-for="s in stockSearchResults"
                :key="s.stock_code"
                :label="`${s.stock_code} - ${s.stock_name?.trim()}${s.spell_initial ? ' (' + s.spell_initial + ')' : ''}`"
                :value="s.stock_code"
              >
                <span>{{ s.stock_code }}</span>
                <span style="margin-left: 8px">{{ s.stock_name?.trim() }}</span>
                <span v-if="s.spell_initial" style="margin-left: 4px; color: #909399; font-size: 12px">{{ s.spell_initial }}</span>
              </el-option>
            </el-select>
          </el-form-item>

          <el-form-item label="目标状态">
            <el-select v-model="addStockForm.status" style="width: 100%">
              <el-option label="watching — 观察中" value="watching" />
              <el-option label="pending — 待交易" value="pending" />
              <el-option label="bought — 已买入" value="bought" />
              <el-option label="sold — 已卖出" value="sold" />
              <el-option label="ignored — 已忽略" value="ignored" />
            </el-select>
          </el-form-item>

          <el-form-item label="买入价格">
            <el-input-number v-model="addStockForm.triggerPrice" :precision="2" :step="0.1" :min="0" />
          </el-form-item>
          <el-form-item label="止损价格">
            <el-input-number v-model="addStockForm.stopLoss" :precision="2" :step="0.1" :min="0" />
          </el-form-item>
          <el-form-item label="止盈价格">
            <el-input-number v-model="addStockForm.takeProfit" :precision="2" :step="0.1" :min="0" />
          </el-form-item>
          <el-form-item label="目标数量">
            <el-input-number v-model="addStockForm.quantity" :min="0" :step="100" placeholder="留空则按策略计算" />
          </el-form-item>
          <el-form-item label="原因">
            <el-input v-model="addStockForm.reason" type="textarea" :rows="2" />
          </el-form-item>
        </el-form>
        <template #footer>
          <el-button @click="showAddStockDialog = false">取消</el-button>
          <el-button type="primary" @click="submitAddStock" :loading="addingStock">添加</el-button>
        </template>
      </el-dialog>

      <!-- 策略选择对话框 -->
      <el-dialog v-model="showStrategySelectDialog" title="选择选股策略" width="500px">
        <el-alert
          title="选择策略进行选股"
          description="选股结果将添加到对应候选组"
          type="info"
          :closable="false"
          style="margin-bottom: 20px"
        />
        <el-form :model="strategySelectForm" label-width="120px">
          <el-form-item label="选择策略" required>
            <el-select v-model="strategySelectForm.strategyId" placeholder="请选择策略" style="width: 100%">
              <el-option
                v-for="s in screeningStrategies"
                :key="s.id"
                :label="`${s.name} (${s.status === 'active' ? '激活' : '停用'})`"
                :value="s.id"
                :disabled="s.status !== 'active'"
              />
            </el-select>
          </el-form-item>
          <el-form-item label="数据源">
            <el-radio-group v-model="strategySelectForm.useLocal">
              <el-radio :value="true">本地数据（快）</el-radio>
              <el-radio :value="false">SDK 实时（慢）</el-radio>
            </el-radio-group>
          </el-form-item>
          <el-form-item v-if="selectedStrategy?.strategy_type === 'python'" label="选股范围">
            <el-radio-group v-model="strategySelectForm.stockScope">
              <el-radio value="market">全市场</el-radio>
              <el-radio value="group">候选组内股票</el-radio>
            </el-radio-group>
          </el-form-item>
        </el-form>
        <template #footer>
          <el-button @click="showStrategySelectDialog = false">取消</el-button>
          <el-button type="primary" @click="runScreeningWithStrategy" :loading="screeningProgress.processing">开始选股</el-button>
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
          <el-table-column prop="trigger_price" label="触发价" width="80" align="right">
            <template #default="{ row }">¥{{ row.trigger_price?.toFixed(2) }}</template>
          </el-table-column>
          <el-table-column prop="match_score" label="匹配度" width="80" align="right">
            <template #default="{ row }">{{ (row.match_score * 100).toFixed(0) }}%</template>
          </el-table-column>
          <el-table-column prop="created_at" label="时间" width="160" />
        </el-table>
        <template #footer>
          <el-button @click="cancelCandidates">取消</el-button>
          <el-button type="danger" @click="rejectCandidates(false)" :loading="confirming">拒绝未选</el-button>
          <el-button type="warning" @click="rejectCandidates(true)" :loading="confirming">全部拒绝</el-button>
          <el-button type="primary" @click="confirmCandidates" :loading="confirming">确认已选 ({{ selectedCandidates.length }})</el-button>
        </template>
      </el-dialog>

      <!-- 编辑对话框 -->
      <el-dialog v-model="showEditDialog" title="编辑股票" width="500px">
        <el-form :model="editingStock" label-width="100px">
          <el-form-item label="股票代码"><el-input v-model="editingStock.stock_code" disabled /></el-form-item>
          <el-form-item label="股票名称"><el-input v-model="editingStock.stock_name" /></el-form-item>
          <el-form-item label="买入价格"><el-input-number v-model="editingStock.trigger_price" :precision="2" :step="0.1" /></el-form-item>
          <el-form-item label="止损价格"><el-input-number v-model="editingStock.stop_loss_price" :precision="2" :step="0.1" /></el-form-item>
          <el-form-item label="止盈价格"><el-input-number v-model="editingStock.take_profit_price" :precision="2" :step="0.1" /></el-form-item>
          <el-form-item label="数量">
            <el-tag>{{ editingStock.position_quantity || 0 }}</el-tag>
            <span class="text-muted" style="font-size:12px;color:#999;margin-left:8px">持仓数据，不可修改</span>
          </el-form-item>
          <el-form-item label="状态">
            <el-select v-model="editingStock.status">
              <el-option label="待交易" value="pending" />
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

      <!-- 文件导入对话框 -->
      <el-dialog v-model="showImportDialog" title="导入股票文件" width="700px" :close-on-click-modal="false">
        <!-- Step 1: 选择文件 -->
        <div v-if="importStep === 'select'" class="import-select">
          <el-alert title="支持 .txt / .csv / .json / .md 文件" :closable="false" type="info" style="margin-bottom: 16px" />
          <div class="file-upload-area" @click="$refs.fileInput.click()" @dragover.prevent @drop.prevent="handleFileDrop">
            <el-icon :size="40" color="#909399"><Upload /></el-icon>
            <p>点击选择文件或拖拽文件到此处</p>
            <input ref="fileInput" type="file" accept=".md,.json,.csv,.txt" style="display: none" @change="handleFileSelect" />
          </div>
          <div v-if="importParsing" class="parsing-status">
            <el-icon class="is-loading"><Loading /></el-icon>
            <span>正在解析文件...</span>
          </div>
        </div>

        <!-- Step 2: 预览确认 -->
        <div v-if="importStep === 'preview'">
          <el-alert :title="importSummaryText" type="info" :closable="false" style="margin-bottom: 12px" />
          <el-table :data="importResults" stripe style="width: 100%" max-height="400" @selection-change="handleImportSelection">
            <el-table-column type="selection" width="50" :selectable="r => r.status === 'new' || r.status === 'duplicate_other'" />
            <el-table-column prop="raw_code" label="原始代码" width="100" />
            <el-table-column prop="stock_code" label="股票代码" width="120" />
            <el-table-column prop="stock_name" label="股票名称" width="120" />
            <el-table-column prop="existing_groups" label="已存在于" width="120" show-overflow-tooltip />
            <el-table-column label="状态" width="110">
              <template #default="{ row }">
                <el-tag :type="importStatusType(row.status)" size="small">{{ importStatusText(row.status) }}</el-tag>
              </template>
            </el-table-column>
          </el-table>
          <div class="import-confirm-footer">
            <span>已选 {{ importSelected.length }} 只</span>
            <el-button type="primary" @click="confirmImport" :loading="importingStocks">确认导入</el-button>
          </div>
        </div>
      </el-dialog>

      <!-- 调度设置对话框（只读，仅显示当前组任务） -->
      <el-dialog v-model="showScheduleDialog" :title="`任务调度 - ${scheduleForm.groupName}`" width="800px">
        <el-alert :title="`当前组：${scheduleForm.groupName}`" :closable="false" type="info" style="margin-bottom: 16px" />

        <div v-loading="tasksLoading">
          <el-table :data="groupStrategyTasks" stripe style="width: 100%">
            <el-table-column label="类型" width="80">
              <template #default="{ row }">
                <el-tag :type="row.task_type === 'builtin' ? 'warning' : 'primary'" size="small">
                  {{ row.task_type === 'builtin' ? '内置' : '策略' }}
                </el-tag>
              </template>
            </el-table-column>
            <el-table-column label="名称" width="160">
              <template #default="{ row }">
                {{ row.task_name || row.strategy_name || '-' }}
              </template>
            </el-table-column>
            <el-table-column label="执行时间" width="200">
              <template #default="{ row }">
                <div>{{ row.cron_description || row.cron_expression }}</div>
                <div style="font-size: 11px; color: #909399; font-family: monospace">{{ row.cron_expression }}</div>
              </template>
            </el-table-column>
            <el-table-column label="状态" width="80">
              <template #default="{ row }">
                <el-tag :type="row.enabled ? 'success' : 'info'" size="small">{{ row.enabled ? '启用' : '停用' }}</el-tag>
              </template>
            </el-table-column>
            <el-table-column label="交易日" width="70">
              <template #default="{ row }">
                <el-tooltip :content="row.require_trading_day ? '仅交易日执行' : '按 cron 执行'" placement="top" :show-after="0">
                  <el-tag :type="row.require_trading_day ? 'warning' : 'info'" size="small">{{ row.require_trading_day ? '是' : '-' }}</el-tag>
                </el-tooltip>
              </template>
            </el-table-column>
            <el-table-column label="上次执行" width="170">
              <template #default="{ row }">
                <span v-if="row.last_run_at">{{ row.last_run_at?.split('.')[0] }}</span>
                <span v-else style="color: #999">未执行</span>
              </template>
            </el-table-column>
            <el-table-column label="上次状态" width="110">
              <template #default="{ row }">
                <el-tooltip v-if="row.realtime_status === 'running'" :content="row.realtime_progress?.message || '正在执行'" placement="top" :show-after="0">
                  <el-tag type="warning" size="small">运行中</el-tag>
                </el-tooltip>
                <el-tag v-else-if="row.last_status" :type="{success:'success',error:'danger',running:'warning'}[row.last_status] || 'info'" size="small">
                  {{ {success:'成功',error:'失败',running:'运行中'}[row.last_status] || row.last_status }}
                </el-tag>
                <el-tooltip v-if="row.last_status === 'error' && row.last_output" :content="parseTaskError(row)" placement="top" :show-after="0">
                  <el-icon style="color: #F56C6C; cursor: pointer; margin-left: 4px"><WarningFilled /></el-icon>
                </el-tooltip>
              </template>
            </el-table-column>
          </el-table>
          <el-empty v-if="!tasksLoading && groupStrategyTasks.length === 0" description="当前组暂无任务" />
        </div>

        <div class="hint" style="margin-top: 16px; text-align: center; color: #909399">
          任务的创建、编辑、删除请前往「数据维护」页面
        </div>
      </el-dialog>

      <!-- 批量修改状态对话框 -->
      <el-dialog v-model="showBatchStatusDialog" title="批量修改状态" width="450px">
        <el-alert :title="`已选中 ${selectedStocks.length} 只股票`" :closable="false" type="info" style="margin-bottom: 16px" />
        <el-form label-width="100px">
          <el-form-item label="目标状态">
            <el-select v-model="batchStatusForm.status" style="width: 100%">
              <el-option label="pending — 待交易" value="pending" />
              <el-option label="watching — 观察中" value="watching" />
              <el-option label="sold — 已卖出" value="sold" />
              <el-option label="ignored — 已忽略" value="ignored" />
              <el-option label="bought — 已买入" value="bought" />
            </el-select>
            <div class="hint">
              <code>pending</code> → 待交易（监控服务会轮询）<br/>
              <code>watching</code> → 监控中（持仓止损止盈）<br/>
              <code>sold</code> / <code>ignored</code> → 解除监控，可安全删除组
            </div>
          </el-form-item>
          <el-form-item>
            <el-button type="primary" @click="executeBatchStatus" :loading="batchStatusRunning">确认修改</el-button>
          </el-form-item>
        </el-form>
      </el-dialog>

      <!-- K 线图弹窗 -->
      <el-dialog v-model="klineVisible" :title="`${klineStockInfo.name} (${klineStockInfo.code}) K线走势`" width="85%" top="5vh">
        <div class="kline-nav">
          <el-button size="small" @click="prevStock" :disabled="!hasPrevStock">
            <el-icon><ArrowLeft /></el-icon> 上一只
          </el-button>
          <span class="kline-nav-text">{{ klineNavText }}</span>
          <el-button size="small" @click="nextStock" :disabled="!hasNextStock">
            下一只 <el-icon><ArrowRight /></el-icon>
          </el-button>
        </div>
        <div class="kline-controls" style="margin-bottom: 12px; display: flex; align-items: center; gap: 16px; flex-wrap: wrap">
          <el-radio-group v-model="klinePeriod" size="small" @change="reloadKline">
            <el-radio-button label="day">日线</el-radio-button>
            <el-radio-button label="week">周线</el-radio-button>
            <el-radio-button label="month">月线</el-radio-button>
          </el-radio-group>
          <el-radio-group v-model="klineAdjust" size="small" @change="reloadKline">
            <el-radio-button label="none">不复权</el-radio-button>
            <el-radio-button label="forward">前复权</el-radio-button>
          </el-radio-group>
          <!-- 技术指标选择器 -->
          <el-dropdown trigger="click" @command="toggleIndicator" style="margin-left: 8px">
            <el-button size="small">
              <el-icon><Setting /></el-icon> 技术指标
              <el-icon class="el-icon--right"><ArrowDown /></el-icon>
            </el-button>
            <template #dropdown>
              <el-dropdown-menu>
                <el-dropdown-item :class="{ 'is-active': selectedIndicators.includes('ma5') }" command="ma5">
                  MA5 均线 <el-tag v-if="selectedIndicators.includes('ma5')" size="small" type="success">已选</el-tag>
                </el-dropdown-item>
                <el-dropdown-item :class="{ 'is-active': selectedIndicators.includes('ma10') }" command="ma10">
                  MA10 均线 <el-tag v-if="selectedIndicators.includes('ma10')" size="small" type="success">已选</el-tag>
                </el-dropdown-item>
                <el-dropdown-item :class="{ 'is-active': selectedIndicators.includes('ma20') }" command="ma20">
                  MA20 均线 <el-tag v-if="selectedIndicators.includes('ma20')" size="small" type="success">已选</el-tag>
                </el-dropdown-item>
                <el-dropdown-item :class="{ 'is-active': selectedIndicators.includes('ma60') }" command="ma60">
                  MA60 均线 <el-tag v-if="selectedIndicators.includes('ma60')" size="small" type="success">已选</el-tag>
                </el-dropdown-item>
                <el-dropdown-item divided :class="{ 'is-active': selectedIndicators.includes('boll') }" command="boll">
                  布林带 (BOLL) <el-tag v-if="selectedIndicators.includes('boll')" size="small" type="success">已选</el-tag>
                </el-dropdown-item>
                <el-dropdown-item :class="{ 'is-active': selectedIndicators.includes('ema12') }" command="ema12">
                  EMA12 <el-tag v-if="selectedIndicators.includes('ema12')" size="small" type="success">已选</el-tag>
                </el-dropdown-item>
                <el-dropdown-item :class="{ 'is-active': selectedIndicators.includes('ema26') }" command="ema26">
                  EMA26 <el-tag v-if="selectedIndicators.includes('ema26')" size="small" type="success">已选</el-tag>
                </el-dropdown-item>
              </el-dropdown-menu>
            </template>
          </el-dropdown>
          <el-button size="small" @click="loadMoreKline" :disabled="klinePeriod === 'month' || klineLoadingMore" v-if="klinePeriod !== 'month'">
            <el-icon><Download /></el-icon> 加载更多
          </el-button>
          <span v-if="klinePeriod === 'month'" style="color: #909399; font-size: 12px">月线已显示全部数据</span>
        </div>
        <KlineChart ref="klineChartRef" :data="klineData" height="550px"
          :stockCode="klineStockInfo.code"
          :accountId="currentAccountId"
          :enableDrillDown="true"
          :adjust="klineAdjust"
          :indicators="klineIndicators"
          :indicatorConfig="klineIndicatorConfig" />
      </el-dialog>
    </el-main>
  </div>
</template>

<script setup>
import { ref, reactive, onMounted, onUnmounted, computed, watch, nextTick } from 'vue'
import { storeToRefs } from 'pinia'
import { ElMessage, ElMessageBox } from 'element-plus'
import { Search, Refresh, Plus, MoreFilled, Upload, Loading, WarningFilled, Edit, Delete, ArrowLeft, ArrowRight, Download, Setting, ArrowDown } from '@element-plus/icons-vue'
import { useAccountStore } from '../stores/account'
import { useWatchlistStore } from '../stores/watchlist'
import { switchStockPreservingDrillDown } from '../utils/drillDownHelper'
import NavBar from '../components/NavBar.vue'
import KlineChart from '../components/KlineChart.vue'

const accountStore = useAccountStore()
const wlStore = useWatchlistStore()
const { candidateGroups, strategies } = storeToRefs(wlStore)
const currentAccountId = computed(() => accountStore.currentAccountId)
const currentAccount = computed(() => accountStore.currentAccount)

// 候选组（从 storeToRefs 解构）
const selectedGroupId = ref(null)
const groupsLoading = ref(false)
const currentGroup = computed(() => candidateGroups.value.find(g => g.id === selectedGroupId.value))

// 候选组排序
const groupSortOrder = ref('asc')
const sortedGroups = computed(() => {
  const sorted = [...candidateGroups.value]
  sorted.sort((a, b) => {
    if (groupSortOrder.value === 'asc') {
      return (a.name || '').localeCompare(b.name || '')
    }
    return (b.name || '').localeCompare(a.name || '')
  })
  return sorted
})

// 当前组股票
const currentStocks = ref([])
const stocksLoading = ref(false)
const filterStatus = ref('')

// 上一次价格（用于检测变化高亮）
const prevPrices = ref(new Map())
const priceChangeHighlight = ref(new Map()) // stock_code -> 'up' | 'down' | null

// 分页
const currentPage = ref(1)
const pageSize = 20

// 排序状态
const sortProp = ref('updated_at')
const sortOrder = ref('descending')

const onTableSortChange = ({ prop, order }) => {
  sortProp.value = prop || 'updated_at'
  sortOrder.value = order || 'descending'
  currentPage.value = 1
}

// 当前组股票（对整个数组排序后分页）
const sortedCurrentStocks = computed(() => {
  const sorted = [...currentStocks.value]
  const prop = sortProp.value
  const desc = sortOrder.value === 'descending'
  sorted.sort((a, b) => {
    const aVal = a[prop]
    const bVal = b[prop]
    if (aVal == null && bVal == null) return 0
    if (aVal == null) return desc ? 1 : -1
    if (bVal == null) return desc ? -1 : 1
    if (typeof aVal === 'number' && typeof bVal === 'number') {
      return desc ? bVal - aVal : aVal - bVal
    }
    return desc ? String(bVal).localeCompare(String(aVal)) : String(aVal).localeCompare(String(bVal))
  })
  return sorted
})

const paginatedStocks = computed(() => {
  const sorted = sortedCurrentStocks.value
  const start = (currentPage.value - 1) * pageSize
  return sorted.slice(start, start + pageSize).map(s => ({
    ...s,
    _highlight: s.stock_code === highlightedStockCode.value
  }))
})

// K 线图
const klineVisible = ref(false)
const klineChartRef = ref(null)
const klineStockInfo = ref({ code: '', name: '' })
const klineStockIndex = ref(-1)
const klineData = ref([])

// 技术指标叠加功能
const selectedIndicators = ref([])
const klineIndicators = ref({})
const klineIndicatorConfig = computed(() => {
  const config = []
  const indicatorColors = {
    ma5: '#FF6B6B',      // 红色
    ma10: '#4ECDC4',     // 青色
    ma20: '#FFD93D',     // 黄色
    ma60: '#96CEB4',     // 绿色
    boll_upper: '#FF8C00',   // 深橙色（上轨）
    boll_middle: '#FF1493',  // 深粉色（中轨）
    boll_lower: '#9370DB',   // 紫色（下轨）
    ema12: '#00CED1',    // 深青色
    ema26: '#8B4513',    // 棕色
  }

  for (const key of selectedIndicators.value) {
    if (key === 'boll') {
      config.push({ key: 'boll_upper', name: 'BOLL上轨', color: indicatorColors.boll_upper, width: 1 })
      config.push({ key: 'boll_middle', name: 'BOLL中轨', color: indicatorColors.boll_middle, width: 1 })
      config.push({ key: 'boll_lower', name: 'BOLL下轨', color: indicatorColors.boll_lower, width: 1 })
    } else {
      const name = key.toUpperCase()
      config.push({ key, name, color: indicatorColors[key] || '#999', width: 1 })
    }
  }
  return config
})

// 切换指标选择
const toggleIndicator = (key) => {
  console.log('[Watchlist] toggleIndicator:', key)
  const idx = selectedIndicators.value.indexOf(key)
  if (idx >= 0) {
    selectedIndicators.value.splice(idx, 1)
  } else {
    selectedIndicators.value.push(key)
  }
  console.log('[Watchlist] 当前选中指标:', selectedIndicators.value)
  // 加载指标数据
  loadIndicatorData()
}

// 加载指标数据（从因子数据表）
const loadIndicatorData = async () => {
  console.log('[Watchlist] loadIndicatorData called, code:', klineStockInfo.value.code, 'selectedIndicators:', selectedIndicators.value)
  if (!klineStockInfo.value.code || selectedIndicators.value.length === 0) {
    klineIndicators.value = {}
    console.log('[Watchlist] 跳过加载：无代码或无选中指标')
    return
  }

  // 只在日线模式下支持指标叠加（因子数据是日频）
  if (klinePeriod.value !== 'day') {
    console.log('[Watchlist] 跳过加载：非日线模式')
    return
  }

  const code = klineStockInfo.value.code
  const dates = klineData.value.map(d => d.trade_date)
  if (dates.length === 0) {
    console.log('[Watchlist] 跳过加载：无K线数据')
    return
  }

  const startDate = dates[0]
  const endDate = dates[dates.length - 1]
  console.log('[Watchlist] 加载指标数据:', code, '日期范围:', startDate, '-', endDate)

  // 构建需要获取的指标字段列表
  const fields = []
  for (const key of selectedIndicators.value) {
    if (key === 'boll') {
      fields.push('boll_upper', 'boll_middle', 'boll_lower')
    } else {
      fields.push(key)
    }
  }

  console.log('[Watchlist] 请求字段:', fields)

  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/factors/${code}?start_date=${startDate}&end_date=${endDate}&fields=${fields.join(',')}`)
    const data = await res.json()
    console.log('[Watchlist] API 返回:', data.success, 'count:', data.count)

    if (data.success && data.factors) {
      const indicators = {}
      for (const field of fields) {
        indicators[field] = data.factors.map(f => ({
          trade_date: f.trade_date,
          value: f[field]
        }))
      }
      klineIndicators.value = indicators
      console.log('[Watchlist] 指标数据已设置:', Object.keys(indicators), '数据条数:', indicators[fields[0]]?.length)
    }
  } catch (e) {
    console.error('[Watchlist] 加载指标数据失败:', e)
  }
}

const hasPrevStock = computed(() => klineStockIndex.value > 0)
const hasNextStock = computed(() => klineStockIndex.value >= 0 && klineStockIndex.value < sortedCurrentStocks.value.length - 1)
const klineNavText = computed(() => {
  const total = sortedCurrentStocks.value.length
  const idx = klineStockIndex.value
  if (idx < 0 || total === 0) return ''
  return `${idx + 1} / ${total}`
})

// 导出功能
import { exportTable as doExport } from '@/utils/exportHelper'

const watchlistColumns = [
  { label: '股票代码', prop: 'stock_code' },
  { label: '股票名称', prop: 'stock_name' },
  { label: '入选原因', prop: 'reason' },
  { label: '触发价', prop: 'trigger_price' },
  { label: '现价', prop: 'current_price' },
  { label: '涨跌', prop: 'change_pct' },
  { label: '止损价', prop: 'stop_loss_price' },
  { label: '止盈价', prop: 'take_profit_price' },
  { label: '目标数量', prop: 'target_quantity' },
  { label: '信号', prop: 'signal_type' },
  { label: '状态', prop: 'status' },
]

const handleExportWatchlist = (format) => {
  doExport(watchlistColumns, currentStocks.value, '候选股票', format)
}

// 策略（从 storeToRefs 解构）
// 关联策略：配置型选股策略 + 代码型选股策略（code_scope=screening）
const screeningStrategies = computed(() => strategies.value.filter(s =>
  (s.strategy_type === 'screening' && s.code_type !== 'python') ||
  (s.strategy_type === 'python' && s.code_scope === 'screening')
))

// 当前选中的策略（用于条件显示选股范围选择器）
const selectedStrategy = computed(() => {
  if (!strategySelectForm.strategyId) return null
  return screeningStrategies.value.find(s => s.id === strategySelectForm.strategyId) || null
})

// 调度：仅显示当前组的任务（只读）
const groupStrategyTasks = computed(() => {
  if (!scheduleForm.groupId) return []
  return strategyTasks.value.filter(t => t.group_id === scheduleForm.groupId)
})

// 对话框
const showCreateGroupDialog = ref(false)
const showRenameDialog = ref(false)
const showAssociateDialog = ref(false)
const showAddStockDialog = ref(false)
const showStrategySelectDialog = ref(false)
const showCandidatesDialog = ref(false)
const showEditDialog = ref(false)
const clearing = ref(false)
const confirming = ref(false)
const creatingGroup = ref(false)
const renamingGroup = ref(false)
const associatingStrategy = ref(false)
const addingStock = ref(false)
const searchingStocks = ref(false)

// 文件导入
const showImportDialog = ref(false)
const importStep = ref('select') // select | preview
const importParsing = ref(false)
const importingStocks = ref(false)
const importResults = ref([])
const importSelected = ref([])
const fileInput = ref(null)

// 批量状态修改
const showBatchStatusDialog = ref(false)
const selectedStocks = ref([])
const batchStatusRunning = ref(false)
const batchStatusForm = reactive({ status: 'sold' })

// 调度
const showScheduleDialog = ref(false)
const tasksLoading = ref(false)
const strategyTasks = ref([])
const scheduleForm = reactive({ groupId: null, groupName: '' })

// 表单
const createGroupForm = reactive({ name: '', screeningStrategyId: null })
const renameForm = reactive({ groupId: null, name: '' })
const associateForm = reactive({ groupId: null, screeningStrategyId: null })
const addStockForm = reactive({
  selectedStockCode: '',
  stockName: '',
  status: 'watching',
  buyPrice: null,
  stopLoss: null,
  takeProfit: null,
  quantity: null,
  reason: '手动添加'
})
const strategySelectForm = reactive({ strategyId: null, useLocal: true, stockScope: 'group' })
const editingStock = reactive({ stock_code: '', stock_name: '', trigger_price: 0, stop_loss_price: 0, take_profit_price: 0, target_quantity: null, status: 'pending' })

// 候选
const candidates = ref([])
const selectedCandidates = ref([])

// 选股进度
const screeningProgress = reactive({ processing: false, total: 0, processed: 0, matched: 0, currentStock: '', percent: 0, status: '' })
let progressPollingTimer = null

const stockSearchResults = ref([])

// 可调分栏宽度
const leftPanelWidth = ref(260)
const isResizing = ref(false)
const resizeMouseMoveRef = ref(null)
const resizeMouseUpRef = ref(null)

const startResize = (e) => {
  isResizing.value = true
  e.preventDefault()
  const startX = e.clientX
  const startWidth = leftPanelWidth.value
  const onMouseMove = (ev) => {
    const newWidth = Math.min(500, Math.max(180, startWidth + ev.clientX - startX))
    leftPanelWidth.value = newWidth
  }
  const onMouseUp = () => {
    isResizing.value = false
    document.removeEventListener('mousemove', onMouseMove)
    document.removeEventListener('mouseup', onMouseUp)
    resizeMouseMoveRef.value = null
    resizeMouseUpRef.value = null
    document.querySelector('.split-handle')?.classList.remove('active')
  }
  resizeMouseMoveRef.value = onMouseMove
  resizeMouseUpRef.value = onMouseUp
  document.addEventListener('mousemove', onMouseMove)
  document.addEventListener('mouseup', onMouseUp)
}

const formatProgress = (percent) => `${percent}%`

// ========== 候选组管理 ==========

const loadGroups = async () => {
  groupsLoading.value = true
  try {
    await wlStore.loadGroups(currentAccountId.value)
  } catch (e) {
    console.error('加载候选组失败:', e)
  } finally {
    groupsLoading.value = false
  }
  if (selectedGroupId.value && !candidateGroups.value.find(g => g.id === selectedGroupId.value)) {
    selectedGroupId.value = candidateGroups.value.length > 0 ? candidateGroups.value[0].id : null
  }
}

const selectGroup = (group) => {
  selectedGroupId.value = group.id
  filterStatus.value = ''
  currentPage.value = 1
  loadCurrentGroupStocks()
}

const createGroup = async () => {
  if (!createGroupForm.name.trim()) {
    ElMessage.warning('请输入组名')
    return
  }
  creatingGroup.value = true
  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/candidate-groups`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        name: createGroupForm.name,
        screening_strategy_id: createGroupForm.screeningStrategyId || null
      })
    })
    const data = await res.json()
    if (res.ok) {
      ElMessage.success(`候选组「${createGroupForm.name}」已创建`)
      showCreateGroupDialog.value = false
      createGroupForm.name = ''
      createGroupForm.screeningStrategyId = null
      await loadGroups()
    } else {
      ElMessage.error(data.detail || '创建失败')
    }
  } catch (e) {
    ElMessage.error('创建失败')
  } finally {
    creatingGroup.value = false
  }
}

const handleGroupAction = (cmd, group) => {
  if (cmd === 'rename') {
    renameForm.groupId = group.id
    renameForm.name = group.name
    showRenameDialog.value = true
  } else if (cmd === 'associate') {
    associateForm.groupId = group.id
    associateForm.screeningStrategyId = group.screening_strategy_id
    showAssociateDialog.value = true
  } else if (cmd === 'schedule') {
    scheduleForm.groupId = group.id
    scheduleForm.groupName = group.name
    showScheduleDialog.value = true
    loadTasks()
  } else if (cmd === 'delete') {
    handleDeleteGroup(group)
  }
}

const handleDeleteGroup = async (group, force = false) => {
  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/candidate-groups/${group.id}?force=${force}`, { method: 'DELETE' })
    const data = await res.json()
    if (res.ok && data.success) {
      ElMessage.success('候选组已删除')
      if (selectedGroupId.value === group.id) selectedGroupId.value = null
      await loadGroups()
    } else if (data.warning && !force) {
      // 有监控中的股票，二次确认
      ElMessageBox.confirm(
        `候选组「${group.name}」包含以下监控中的股票：\n\n${data.details}\n\n删除后将解除这些股票的监控。是否继续？`,
        '确认删除',
        { type: 'warning', confirmButtonText: '确认删除', cancelButtonText: '取消' }
      ).then(() => {
        handleDeleteGroup(group, true)
      }).catch(() => {})
    } else {
      ElMessage.error(data.message || data.detail || '删除失败')
    }
  } catch (e) {
    ElMessage.error('删除失败')
  }
}

const renameGroup = async () => {
  if (!renameForm.name.trim()) {
    ElMessage.warning('请输入组名')
    return
  }
  renamingGroup.value = true
  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/candidate-groups/${renameForm.groupId}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name: renameForm.name })
    })
    if (res.ok) {
      ElMessage.success('重命名成功')
      showRenameDialog.value = false
      await loadGroups()
    } else {
      const data = await res.json()
      ElMessage.error(data.detail || '重命名失败')
    }
  } catch (e) {
    ElMessage.error('重命名失败')
  } finally {
    renamingGroup.value = false
  }
}

const associateStrategy = async () => {
  associatingStrategy.value = true
  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/candidate-groups/${associateForm.groupId}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ screening_strategy_id: associateForm.screeningStrategyId || null })
    })
    if (res.ok) {
      ElMessage.success('策略关联成功')
      showAssociateDialog.value = false
      await loadGroups()
    } else {
      const data = await res.json()
      ElMessage.error(data.detail || '关联失败')
    }
  } catch (e) {
    ElMessage.error('关联失败')
  } finally {
    associatingStrategy.value = false
  }
}

// ========== 策略任务（只读） ==========

const loadTasks = async () => {
  tasksLoading.value = true
  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/strategy-tasks`)
    const data = await res.json()
    strategyTasks.value = data.tasks || []
  } catch (e) {
    console.error('加载任务失败:', e)
  } finally {
    tasksLoading.value = false
  }
}

// 解析任务执行错误信息
const parseTaskError = (task) => {
  try {
    const output = JSON.parse(task.last_output)
    if (output.error) return output.error
    if (output.warnings && output.warnings.length > 0) return output.warnings.join('; ')
    return '执行失败，无详细错误信息'
  } catch {
    return '错误信息解析失败'
  }
}

// ========== 股票管理 ==========

const loadCurrentGroupStocks = async () => {
  if (!selectedGroupId.value) {
    currentStocks.value = []
    return
  }
  stocksLoading.value = true
  currentPage.value = 1
  try {
    // 保存旧价格用于对比
    const oldPrices = new Map()
    for (const s of currentStocks.value) {
      if (s.current_price > 0) {
        oldPrices.set(s.stock_code, s.current_price)
      }
    }

    let url = `/api/v1/ui/${currentAccountId.value}/watchlist?group_id=${selectedGroupId.value}`
    if (filterStatus.value) url += `&status=${filterStatus.value}`
    const res = await fetch(url)
    const data = await res.json()
    currentStocks.value = data.watchlist || []

    // 对比新旧价格，标记变化
    const changes = new Map()
    for (const s of currentStocks.value) {
      const oldPrice = oldPrices.get(s.stock_code)
      const newPrice = s.current_price
      if (oldPrice && newPrice && oldPrice > 0 && newPrice > 0) {
        if (newPrice > oldPrice) {
          changes.set(s.stock_code, 'up')
        } else if (newPrice < oldPrice) {
          changes.set(s.stock_code, 'down')
        }
      }
    }
    priceChangeHighlight.value = changes
    prevPrices.value = oldPrices

    // 3秒后清除高亮
    setTimeout(() => {
      priceChangeHighlight.value = new Map()
    }, 3000)
  } catch (e) {
    console.error('加载股票失败:', e)
  } finally {
    stocksLoading.value = false
  }
}

// ========== K 线图 ==========

const klinePeriod = ref('day')
const klineAdjust = ref('forward')
const klineMonths = ref(12)  // 当前加载的月数（日线）/ 根数（周线）
const klineLoadingMore = ref(false)

const showKline = async (row) => {
  const idx = sortedCurrentStocks.value.findIndex(s => s.stock_code === row.stock_code)
  klineStockIndex.value = idx
  klineVisible.value = true
  // 初始化加载量
  if (klinePeriod.value === 'day') {
    klineMonths.value = 12  // 日线初始 1 年
  } else if (klinePeriod.value === 'week') {
    klineMonths.value = 60  // 周线初始 60 个月参数（约 250 根）
  }
  await loadKlineData(row.stock_code, row.stock_name)
}

const reloadKline = () => {
  if (klineStockInfo.value.code) {
    // 切换周期时重置加载量
    if (klinePeriod.value === 'day') {
      klineMonths.value = 12
    } else if (klinePeriod.value === 'week') {
      klineMonths.value = 60
    }
    loadKlineData(klineStockInfo.value.code, klineStockInfo.value.name)
  }
}

const loadMoreKline = async () => {
  if (klinePeriod.value === 'month') return  // 月线不支持加载更多
  klineLoadingMore.value = true
  klineMonths.value += 12  // 每次增加 1 年
  try {
    await loadKlineData(klineStockInfo.value.code, klineStockInfo.value.name)
  } finally {
    klineLoadingMore.value = false
  }
}

const loadKlineData = async (code, name) => {
  console.log('[Watchlist] loadKlineData called, code:', code, 'selectedIndicators:', selectedIndicators.value)
  klineStockInfo.value = { code, name }
  klineData.value = []
  klineIndicators.value = {}  // 清空旧指标数据

  // 从本地 kline.db 读取（支持日线/周线/月线 + 前复权）
  // 日线: months 参数控制月数
  // 周线: months 参数转为根数（months * 5，默认至少 250 根）
  // 月线: 不限制，取全部
  const monthsParam = klinePeriod.value === 'month' ? 0 : klineMonths.value

  // 构建因子字段（用于合并请求）
  let factorFieldsParam = ''
  if (selectedIndicators.value.length > 0 && klinePeriod.value === 'day') {
    const fields = []
    for (const key of selectedIndicators.value) {
      if (key === 'boll') {
        fields.push('boll_upper', 'boll_middle', 'boll_lower')
      } else {
        fields.push(key)
      }
    }
    factorFieldsParam = `&include_factors=true&factor_fields=${fields.join(',')}`
    console.log('[Watchlist] 合并请求因子字段:', fields)
  }

  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/stocks/${code}/kline-local?months=${monthsParam}&period=${klinePeriod.value}&adjust=${klineAdjust.value}${factorFieldsParam}`)
    if (res.ok) {
      const data = await res.json()
      if (data.success && data.kline && data.kline.length > 0) {
        klineData.value = data.kline
        // 处理合并返回的因子数据
        if (data.factors && data.factors.length > 0) {
          console.log('[Watchlist] API 返回因子数据:', data.factor_count, '条')
          const indicators = {}
          // 解析因子字段
          const fields = []
          for (const key of selectedIndicators.value) {
            if (key === 'boll') {
              fields.push('boll_upper', 'boll_middle', 'boll_lower')
            } else {
              fields.push(key)
            }
          }
          for (const field of fields) {
            indicators[field] = data.factors.map(f => ({
              trade_date: f.trade_date,
              value: f[field]
            }))
          }
          klineIndicators.value = indicators
          console.log('[Watchlist] 因子数据已设置:', Object.keys(indicators))
        }
        return
      }
    }
  } catch (e) {
    console.warn('本地 K 线数据读取失败，回退 SDK:', e.message)
  }

  // 回退：SDK 查询（仅日线）
  if (klinePeriod.value !== 'day') {
    // 周线/月线只能从本地数据库读取，不支持 SDK fallback
    console.warn('周线/月线数据加载失败')
    return
  }

  const endDt = new Date()
  const startDt = new Date()
  startDt.setMonth(startDt.getMonth() - klineMonths.value)
  const start = startDt.toISOString().slice(0, 10).replace(/-/g, '')
  const end = endDt.toISOString().slice(0, 10).replace(/-/g, '')

  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/market/kline?stock_code=${code}&period=day&start_date=${start}&end_date=${end}`)
    const data = await res.json()
    klineData.value = data.data?.kline || []
    // 加载技术指标数据（如果有选中的指标）
    if (selectedIndicators.value.length > 0) {
      await loadIndicatorData()
    }
  } catch (e) {
    console.error('加载 K 线数据失败:', e)
  }
}

const prevStock = async () => {
  if (!hasPrevStock.value) return
  const idx = klineStockIndex.value - 1
  const row = sortedCurrentStocks.value[idx]
  if (!row) return
  klineStockIndex.value = idx
  klineIndicators.value = {}
  await switchStockPreservingDrillDown(klineChartRef, () => loadKlineData(row.stock_code, row.stock_name))
}

const nextStock = async () => {
  if (!hasNextStock.value) return
  const idx = klineStockIndex.value + 1
  const row = sortedCurrentStocks.value[idx]
  if (!row) return
  klineStockIndex.value = idx
  klineIndicators.value = {}
  await switchStockPreservingDrillDown(klineChartRef, () => loadKlineData(row.stock_code, row.stock_name))
}

// 搜索股票（智能搜索：代码/拼音/名称）
// 股票快速定位搜索
const stockQuickSearch = ref('')
const highlightedStockCode = ref('')
let highlightTimer = null

const tableCellStyle = ({ row }) => {
  return row._highlight ? { backgroundColor: '#ffe4e8' } : {}
}

const onQuickSearchSelect = (item) => {
  const found = sortedCurrentStocks.value.find(s => s.stock_code === item.stock_code)
  if (found) {
    // 在列表中 → 筛选显示
    filterStatus.value = ''
    // 用排序后的列表计算页码（与表格显示一致）
    const idx = sortedCurrentStocks.value.findIndex(s => s.stock_code === item.stock_code)
    if (idx >= 0) {
      currentPage.value = Math.floor(idx / pageSize) + 1
    }
    // 高亮显示该行（_highlight 标记在 paginatedStocks 中驱动）
    highlightedStockCode.value = item.stock_code
    if (highlightTimer) clearTimeout(highlightTimer)
    highlightTimer = setTimeout(() => {
      highlightedStockCode.value = ''
    }, 8000)
    ElMessage.success(`${item.stock_code} 已在当前分组中`)
  } else {
    // 不在列表中 → 询问添加
    ElMessageBox.confirm(
      `${item.stock_code} ${item.stock_name} 不在当前分组中，是否添加？`,
      '添加股票',
      { confirmButtonText: '添加', cancelButtonText: '取消', type: 'info' }
    ).then(() => {
      showAddStockDialog.value = true
      addStockForm.selectedStockCode = item.stock_code
      addStockForm.stockName = item.stock_name
    }).catch(() => {})
  }
  stockQuickSearch.value = ''
}

const searchStocksForAdd = async (query, callback) => {
  if (!query || query.length < 1) {
    stockSearchResults.value = []
    if (callback) callback([])
    return
  }
  searchingStocks.value = true
  try {
    const res = await fetch(`/api/v1/ui/stocks/search?q=${encodeURIComponent(query)}&limit=20`)
    const data = await res.json()
    const results = (data.success && data.stocks) ? data.stocks : []
    stockSearchResults.value = results
    if (callback) {
      // el-autocomplete fetch-suggestions 回调格式
      callback(results.map(s => ({ value: s.stock_code, ...s })))
    }
  } catch (e) {
    console.error('搜索股票失败:', e)
    stockSearchResults.value = []
    if (callback) callback([])
  } finally {
    searchingStocks.value = false
  }
}

const submitAddStock = async () => {
  if (!addStockForm.selectedStockCode) {
    ElMessage.warning('请选择股票')
    return
  }

  const stockCode = addStockForm.selectedStockCode
  let stockName = (addStockForm.stockName || '').trim()

  // 如果名称未带入（对话框内直接搜索），从搜索结果中补全
  if (!stockName) {
    const found = stockSearchResults.value.find(s => s.stock_code === stockCode)
    if (found) {
      stockName = (found.stock_name || '').trim()
    }
  }

  // 仍未获取到名称 → 直接查 API
  if (!stockName) {
    try {
      const infoRes = await fetch(`/api/v1/ui/stocks/search?q=${encodeURIComponent(stockCode)}&limit=1`)
      const infoData = await infoRes.json()
      if (infoData.success && infoData.stocks?.length > 0) {
        stockName = (infoData.stocks[0].stock_name || '').trim()
      }
    } catch (e) {
      // ignore
    }
    if (!stockName) {
      ElMessage.warning('未找到股票信息，请重新选择')
      return
    }
  }

  addingStock.value = true
  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/watchlist`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        stock_code: stockCode,
        stock_name: stockName,
        group_id: selectedGroupId.value,
        status: addStockForm.status,
        trigger_price: addStockForm.triggerPrice,
        stop_loss_price: addStockForm.stopLoss,
        take_profit_price: addStockForm.takeProfit,
        target_quantity: addStockForm.quantity,
        reason: addStockForm.reason || '手动添加'
      })
    })
    const data = await res.json()
    if (res.ok) {
      ElMessage.success(`已添加 ${stockCode}`)
      showAddStockDialog.value = false
      resetAddStockForm()
      await loadCurrentGroupStocks()
      await loadGroups()
    } else {
      ElMessage.error(data.detail || '添加失败')
    }
  } catch (e) {
    ElMessage.error('添加失败')
  } finally {
    addingStock.value = false
  }
}

const resetAddStockForm = () => {
  addStockForm.selectedStockCode = ''
  addStockForm.stockName = ''
  addStockForm.status = 'watching'
  addStockForm.triggerPrice = null
  addStockForm.stopLoss = null
  addStockForm.takeProfit = null
  addStockForm.quantity = null
  addStockForm.reason = '手动添加'
}

// ========== 文件导入 ==========

const importSummaryText = computed(() => {
  const s = importResults.value
  const newCount = s.filter(r => r.status === 'new').length
  const dupInGroup = s.filter(r => r.status === 'duplicate_in_group').length
  const dupOther = s.filter(r => r.status === 'duplicate_other').length
  const invalid = s.filter(r => r.status === 'invalid').length
  return `共解析 ${s.length} 项：${newCount} 只可添加，${dupInGroup} 只本组已有，${dupOther} 只其他组有，${invalid} 项无效`
})

const importStatusType = (status) => ({ new: 'success', duplicate_in_group: 'warning', duplicate_other: 'info', invalid: 'danger' }[status] || 'info')
const importStatusText = (status) => ({ new: '可添加', duplicate_in_group: '本组已有', duplicate_other: '其他组有', invalid: '无效' }[status] || status)

const startImport = () => {
  if (!selectedGroupId.value) {
    ElMessage.warning('请先选择候选组')
    return
  }
  importStep.value = 'select'
  importResults.value = []
  importSelected.value = []
  showImportDialog.value = true
}

const handleFileSelect = (e) => {
  const file = e.target.files[0]
  if (file) parseFile(file)
  // reset input so same file can be selected again
  e.target.value = ''
}

const handleFileDrop = (e) => {
  const file = e.dataTransfer.files[0]
  if (file) parseFile(file)
}

const parseFile = (file) => {
  const ext = file.name.split('.').pop().toLowerCase()
  if (!['txt', 'csv', 'json', 'md'].includes(ext)) {
    ElMessage.warning('不支持的文件格式，请上传 .txt/.csv/.json/.md 文件')
    return
  }
  importParsing.value = true
  const reader = new FileReader()
  reader.onload = (e) => {
    try {
      const text = e.target.result
      let items = []
      switch (ext) {
        case 'txt': items = parseTxt(text); break
        case 'csv': items = parseCsv(text); break
        case 'json': items = parseJson(text); break
        case 'md': items = parseMd(text); break
      }
      // 限制 5000 条
      if (items.length > 5000) {
        items = items.slice(0, 5000)
        ElMessage.warning('文件超过 5000 条，已截取前 5000 条')
      }
      if (items.length === 0) {
        ElMessage.warning('未解析到有效数据')
        importParsing.value = false
        return
      }
      // 请求服务端预览
      previewImport(items)
    } catch (err) {
      ElMessage.error('文件解析失败：' + err.message)
      importParsing.value = false
    }
  }
  reader.readAsText(file)
}

// 解析 .txt: 每行一个，支持 "code name" 空格/tab 分隔
const parseTxt = (text) => {
  const items = []
  for (const line of text.split('\n')) {
    const trimmed = line.trim()
    if (!trimmed || trimmed.startsWith('#')) continue
    const parts = trimmed.split(/\s+/)
    items.push({ code: parts[0] || '', name: parts[1] || '' })
  }
  return items
}

// 解析 .csv: 逗号分隔，自动跳过表头
const parseCsv = (text) => {
  const items = []
  const lines = text.split('\n').filter(l => l.trim())
  if (lines.length === 0) return items

  // 尝试检测表头行
  let startIdx = 0
  const firstLine = lines[0]
  const hasHeader = /代码|名称|code|name/i.test(firstLine)
  if (hasHeader) startIdx = 1

  for (let i = startIdx; i < lines.length; i++) {
    const cols = lines[i].split(/[,，\t]/).map(c => c.trim())
    if (cols.length === 0) continue
    // 找 6 位数字列作为代码
    let code = '', name = ''
    for (const col of cols) {
      if (/^\d{6}$/.test(col)) { code = col; continue }
      if (!code && /^\d{6}\.(SH|SZ|BJ)$/i.test(col)) { code = col; continue }
      // 非数字非空的作为名称
      if (col && !/^\d+$/.test(col) && !name) name = col
    }
    if (code) items.push({ code, name })
  }
  return items
}

// 解析 .json: 字符串数组或对象数组
const parseJson = (text) => {
  const data = JSON.parse(text)
  if (!Array.isArray(data)) return []
  const items = []
  for (const item of data) {
    if (typeof item === 'string') {
      items.push({ code: item.trim(), name: '' })
    } else if (typeof item === 'object') {
      const code = item.code || item.stock_code || item.Code || item.CODE || ''
      const name = item.name || item.stock_name || item.Name || item.NAME || ''
      if (code) items.push({ code: String(code).trim(), name: String(name).trim() })
    }
  }
  return items
}

// 解析 .md: 提取表格行，跳过表头和分隔线
const parseMd = (text) => {
  const items = []
  for (const line of text.split('\n')) {
    const trimmed = line.trim()
    if (!trimmed || trimmed.startsWith('|---') || trimmed.startsWith('| ---')) continue
    // 跳过含"代码""名称"的表头行
    if (/代码.*名称|名称.*代码/i.test(trimmed)) continue
    if (trimmed.startsWith('|') && trimmed.endsWith('|')) {
      const cells = trimmed.split('|').map(c => c.trim()).filter(c => c)
      if (cells.length >= 1) {
        let code = '', name = ''
        for (const cell of cells) {
          if (/^\d{6}$/.test(cell) || /^\d{6}\.(SH|SZ|BJ)$/i.test(cell)) { code = cell; continue }
          if (!code) {
            // 尝试作为代码
            if (/^\d+$/.test(cell)) { code = cell; continue }
          }
          if (!name && cell && cell !== code) name = cell
        }
        // 如果第一列就是代码，第二列是名称
        if (!code && cells[0]) {
          if (/^\d{6}$/.test(cells[0]) || /^\d{6}\.(SH|SZ|BJ)$/i.test(cells[0])) {
            code = cells[0]
            name = cells[1] || ''
          }
        }
        if (code) items.push({ code, name })
      }
    }
  }
  return items
}

const previewImport = async (items) => {
  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/watchlist/import-preview`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ group_id: selectedGroupId.value, items })
    })
    const data = await res.json()
    if (res.ok) {
      importResults.value = data.results || []
      importStep.value = 'preview'
    } else {
      ElMessage.error(data.detail || '预览失败')
    }
  } catch (e) {
    ElMessage.error('预览失败')
  } finally {
    importParsing.value = false
  }
}

const handleImportSelection = (selection) => {
  importSelected.value = selection
}

const confirmImport = async () => {
  if (importSelected.value.length === 0) {
    ElMessage.warning('请至少选择一只股票')
    return
  }
  importingStocks.value = true
  try {
    const items = importSelected.value.map(r => ({ stock_code: r.stock_code, stock_name: r.stock_name }))
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/watchlist/batch-add`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ group_id: selectedGroupId.value, items })
    })
    const data = await res.json()
    if (res.ok) {
      ElMessage.success(data.message || `成功导入 ${data.added} 只股票`)
      showImportDialog.value = false
      await loadCurrentGroupStocks()
      await loadGroups()
    } else {
      ElMessage.error(data.detail || '导入失败')
    }
  } catch (e) {
    ElMessage.error('导入失败')
  } finally {
    importingStocks.value = false
  }
}

const editStock = (row) => {
  Object.assign(editingStock, row)
  showEditDialog.value = true
}

// 编辑选中的股票（只能单选）
const editSelected = () => {
  if (selectedStocks.value.length === 1) {
    editStock(selectedStocks.value[0])
  }
}

// 批量移除选中的股票
const batchRemoveSelected = async () => {
  if (selectedStocks.value.length === 0) return
  const codes = selectedStocks.value.map(s => s.stock_code).join(', ')
  try {
    await ElMessageBox.confirm(
      `确定要移除以下 ${selectedStocks.value.length} 只股票吗？\n${codes}`,
      '批量移除确认',
      { type: 'warning', confirmButtonText: '确认移除', cancelButtonText: '取消' }
    )
  } catch (e) {
    // 用户取消
    return
  }
  try {
    for (const row of selectedStocks.value) {
      const url = `/api/v1/ui/${currentAccountId.value}/watchlist/${row.stock_code}?group_id=${selectedGroupId.value}`
      const res = await fetch(url, { method: 'DELETE' })
      if (!res.ok) {
        ElMessage.error(`移除 ${row.stock_code} 失败: ${res.status}`)
      }
    }
    ElMessage.success('已批量移除')
    selectedStocks.value = []
    await loadCurrentGroupStocks()
    await loadGroups()
  } catch (e) {
    ElMessage.error('批量移除异常: ' + e.message)
  }
}

const saveStock = async () => {
  try {
    await fetch(`/api/v1/ui/${currentAccountId.value}/watchlist/${editingStock.stock_code}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        group_id: selectedGroupId.value,
        stock_name: editingStock.stock_name,
        trigger_price: editingStock.trigger_price,
        stop_loss_price: editingStock.stop_loss_price,
        take_profit_price: editingStock.take_profit_price,
        target_quantity: editingStock.target_quantity,
        status: editingStock.status
      })
    })
    ElMessage.success('保存成功')
    showEditDialog.value = false
    await loadCurrentGroupStocks()
  } catch (e) {
    ElMessage.error('保存失败')
  }
}

const removeStock = async (row) => {
  try {
    const url = `/api/v1/ui/${currentAccountId.value}/watchlist/${row.stock_code}?group_id=${selectedGroupId.value}`
    const res = await fetch(url, { method: 'DELETE' })
    if (res.ok) {
      ElMessage.success('已移除')
      await loadCurrentGroupStocks()
      await loadGroups()
    }
  } catch (e) {
    ElMessage.error('移除失败')
  }
}

// ========== 批量状态 ==========

const handleStockSelectionChange = (selection) => {
  selectedStocks.value = selection
}

const executeBatchStatus = async () => {
  if (!batchStatusForm.status) {
    ElMessage.warning('请选择目标状态')
    return
  }
  if (selectedStocks.value.length === 0) {
    ElMessage.warning('请先勾选要修改的股票')
    return
  }
  if (!selectedGroupId.value) {
    ElMessage.error('请先选择候选组')
    return
  }
  batchStatusRunning.value = true
  try {
    const codes = selectedStocks.value.map(s => s.stock_code)
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/candidate-groups/${selectedGroupId.value}/batch-status`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        status: batchStatusForm.status,
        stock_codes: codes,
      })
    })
    const data = await res.json()
    if (res.ok) {
      ElMessage.success(data.message || '状态已更新')
      showBatchStatusDialog.value = false
      selectedStocks.value = []
      await loadCurrentGroupStocks()
      await loadGroups()
    } else {
      ElMessage.error(data.detail || '修改失败')
    }
  } catch (e) {
    ElMessage.error('修改失败')
  } finally {
    batchStatusRunning.value = false
  }
}

// ========== 选股策略 ==========

const loadStrategies = async () => {
  try {
    await wlStore.loadStrategies(currentAccountId.value)
  } catch (e) {
    console.error('加载策略失败:', e)
  }
}

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
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/screening/run`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        strategy_id: strategySelectForm.strategyId,
        use_local: strategySelectForm.useLocal,
        pending_to_temp: true,
        stock_scope: strategySelectForm.stockScope
      })
    })
    const data = await res.json()
    if (data.success) {
      progressPollingTimer = setInterval(pollProgress, 2000)
    } else {
      ElMessage.error(data.message || '选股失败')
      screeningProgress.processing = false
    }
  } catch (e) {
    ElMessage.error('选股失败')
    screeningProgress.processing = false
  }
}

const pollProgress = async () => {
  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/screening/status`)
    const data = await res.json()
    if (data.progress) {
      const p = data.progress
      screeningProgress.total = p.total_stocks || 0
      screeningProgress.processed = p.processed || 0
      screeningProgress.matched = p.matched || 0
      screeningProgress.currentStock = p.current_stock || ''
      if (screeningProgress.total > 0) {
        screeningProgress.percent = Math.round((screeningProgress.processed / screeningProgress.total) * 100)
      }
      const isCompleted = p.current_phase === 'done' || (p.total_stocks > 0 && p.processed >= p.total_stocks)
      if (isCompleted) {
        screeningProgress.processing = false
        screeningProgress.status = 'success'
        if (progressPollingTimer) { clearInterval(progressPollingTimer); progressPollingTimer = null }
        await checkTempCandidates()
        if (candidates.value.length === 0) {
          await loadGroups()
          if (selectedGroupId.value) await loadCurrentGroupStocks()
          ElMessage.success(`选股完成，共匹配 ${screeningProgress.matched} 只股票`)
        }
      } else if (p.current_phase === 'scanning') {
        screeningProgress.status = ''
        screeningProgress.processing = true
      }
    }
  } catch (e) {
    console.error('获取进度失败:', e)
  }
}

const checkTempCandidates = async () => {
  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/candidates`)
    const data = await res.json()
    if (data.candidates && data.candidates.length > 0) {
      candidates.value = data.candidates
      showCandidatesDialog.value = true
    }
  } catch (e) {
    console.error('加载候选股票失败:', e)
  }
}

const handleSelectionChange = (selection) => { selectedCandidates.value = selection.map(s => s.stock_code) }

const confirmCandidates = async () => {
  confirming.value = true
  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/candidates/confirm`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ stock_codes: selectedCandidates.value.length > 0 ? selectedCandidates.value : null, confirm: true })
    })
    const data = await res.json()
    if (data.success) {
      ElMessage.success(`已确认 ${data.confirmed} 只股票`)
      showCandidatesDialog.value = false
      await loadGroups()
      if (selectedGroupId.value) await loadCurrentGroupStocks()
    }
  } catch (e) {
    ElMessage.error('确认失败')
  } finally {
    confirming.value = false
  }
}

const rejectCandidates = async (rejectAll) => {
  confirming.value = true
  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/candidates/confirm`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ stock_codes: rejectAll ? null : selectedCandidates.value, confirm: false })
    })
    const data = await res.json()
    if (data.success) {
      ElMessage.success(`已拒绝 ${data.rejected} 只股票`)
      showCandidatesDialog.value = false
    }
  } catch (e) {
    ElMessage.error('拒绝失败')
  } finally {
    confirming.value = false
  }
}

const cancelCandidates = () => { showCandidatesDialog.value = false; screeningProgress.processing = false; screeningProgress.status = '' }

const loadAll = async () => {
  await loadGroups()
  if (selectedGroupId.value) await loadCurrentGroupStocks()
}

const getStatusType = (status) => ({ pending: 'info', watching: 'warning', bought: 'success', sold: 'success', ignored: 'info' }[status] || 'info')
const getStatusText = (status) => ({ pending: '待交易', watching: '观察中', bought: '已买入', sold: '已卖出', ignored: '已忽略' }[status] || status)

import { formatTime } from '../utils/format'

// DSA 分析
const dsaAnalyzing = ref(false)

const handleDsaAnalysisSelected = async () => {
  if (selectedStocks.value.length !== 1) {
    ElMessage.warning('请勾选一只股票')
    return
  }
  const row = selectedStocks.value[0]
  dsaAnalyzing.value = true
  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/stocks/${row.stock_code}/dsa-analyze`, { method: 'POST' })
    const data = await res.json()
    if (res.ok && data.success) {
      ElMessage.success(`${row.stock_code} DSA 分析完成`)
    } else if (data.code === 409) {
      ElMessage.info(data.message)
    } else {
      ElMessage.error(data.detail || '分析失败')
    }
  } catch (e) {
    ElMessage.error('请求失败，请检查网络连接')
  } finally {
    dsaAnalyzing.value = false
  }
}

let wlAbortController = null

onUnmounted(() => {
  wlAbortController?.abort()
  if (progressPollingTimer) { clearInterval(progressPollingTimer); progressPollingTimer = null }
  if (watchlistPriceTimer) { clearInterval(watchlistPriceTimer); watchlistPriceTimer = null }
  if (resizeMouseMoveRef.value) document.removeEventListener('mousemove', resizeMouseMoveRef.value)
  if (resizeMouseUpRef.value) document.removeEventListener('mouseup', resizeMouseUpRef.value)
  isResizing.value = false
})

// 静默刷新候选列表现价（从内存 PriceCache 取，不调用 SDK）
let watchlistPriceTimer = null
const startWatchlistPriceRefresh = () => {
  watchlistPriceTimer = setInterval(async () => {
    try {
      if (!selectedGroupId.value) return

      // 保存旧价格用于对比
      const oldPrices = new Map()
      for (const s of currentStocks.value) {
        if (s.current_price > 0) {
          oldPrices.set(s.stock_code, s.current_price)
        }
      }

      let url = `/api/v1/ui/${currentAccountId.value}/watchlist?group_id=${selectedGroupId.value}`
      if (filterStatus.value) url += `&status=${filterStatus.value}`
      const res = await fetch(url)
      const data = await res.json()
      currentStocks.value = data.watchlist || []

      // 对比新旧价格，标记变化
      const changes = new Map()
      for (const s of currentStocks.value) {
        const oldPrice = oldPrices.get(s.stock_code)
        const newPrice = s.current_price
        if (oldPrice && newPrice && oldPrice > 0 && newPrice > 0) {
          if (newPrice > oldPrice) {
            changes.set(s.stock_code, 'up')
          } else if (newPrice < oldPrice) {
            changes.set(s.stock_code, 'down')
          }
        }
      }
      if (changes.size > 0) {
        priceChangeHighlight.value = changes
        // 3秒后清除高亮
        setTimeout(() => {
          priceChangeHighlight.value = new Map()
        }, 3000)
      }
    } catch (e) {
      // 静默失败
    }
  }, 30000)  // 每 30 秒静默刷新
}

onMounted(async () => {
  wlAbortController = new AbortController()
  if (!wlStore.loaded) {
    await loadStrategies()
    await loadGroups()
  }
  if (candidateGroups.value.length > 0) {
    selectedGroupId.value = candidateGroups.value[0].id
    await loadCurrentGroupStocks()
  }
  await checkTempCandidates()
  // 启动定时静默刷新（从内存缓存取价）
  startWatchlistPriceRefresh()
})
</script>

<style scoped>
.layout-container { height: 100%; display: flex; flex-direction: column; }
.main-content { padding: 20px; }
.page-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; }
.page-header h2 { color: #303133; margin: 0; }
.card-header { display: flex; justify-content: space-between; align-items: center; }
.stock-toolbar { flex-wrap: wrap; gap: 8px; }
.progress-card { margin-bottom: 20px; }
.progress-details { display: flex; justify-content: space-between; margin-top: 10px; font-size: 13px; color: #606266; }
.main-row { margin-bottom: 20px; }

/* 价格变化高亮 */
.price-up-bg {
  background-color: #ffe4e4;  /* 淡粉 - 价格上涨 */
  padding: 2px 6px;
  border-radius: 3px;
  display: inline-block;
}
.price-down-bg {
  background-color: #e4ffe4;  /* 淡绿 - 价格下跌 */
  padding: 2px 6px;
  border-radius: 3px;
  display: inline-block;
}

.main-row.resizable-split { display: flex; gap: 0; align-items: stretch; }
.main-row.resizable-split .left-panel { min-width: 180px; max-width: 500px; }
.main-row.resizable-split .right-panel { flex: 1; min-width: 400px; }
.main-row.resizable-split .split-handle {
  width: 6px; cursor: col-resize; background: transparent; flex-shrink: 0;
  position: relative; z-index: 10;
}
.main-row.resizable-split .split-handle:hover,
.main-row.resizable-split .split-handle.active {
  background: #409EFF; opacity: 0.3;
}

.group-card { height: 100%; }
.group-header { display: flex; align-items: center; justify-content: space-between; }
.group-list { min-height: 300px; max-height: calc(20 * 42px); overflow-y: auto; }
.group-item {
  display: flex; align-items: center; padding: 10px 12px; cursor: pointer;
  border-bottom: 1px solid #f0f0f0; transition: background-color 0.2s;
}
.group-item:hover { background-color: #f5f7fa; }
.group-item.active { background-color: #ecf5ff; border-left: 3px solid #409EFF; }
.group-info { flex: 1; display: flex; align-items: center; gap: 8px; min-width: 0; }
.group-tag { flex-shrink: 0; }
.group-name { font-size: 14px; color: #303133; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.group-count { font-size: 12px; color: #909399; margin-right: 8px; }
.group-actions { cursor: pointer; color: #909399; }

/* 文件导入 */
.import-select { text-align: center; padding: 20px 0; }
.file-upload-area {
  border: 2px dashed #dcdfe6; border-radius: 8px; padding: 40px 20px;
  cursor: pointer; transition: border-color 0.2s;
}
.file-upload-area:hover { border-color: #409EFF; }
.file-upload-area p { color: #909399; margin: 12px 0 0; }
.parsing-status { display: flex; align-items: center; justify-content: center; gap: 8px; margin-top: 16px; color: #409EFF; }
.import-confirm-footer { display: flex; justify-content: space-between; align-items: center; margin-top: 12px; padding-top: 12px; border-top: 1px solid #ebeef5; }

/* 调度设置 */
.hint { font-size: 12px; color: #909399; margin-top: 8px; line-height: 1.6; }
.hint code { background: #f5f7fa; padding: 1px 4px; border-radius: 3px; font-family: monospace; color: #606266; }
.cron-quick-btn { transition: color 0.2s; }
.cron-quick-btn:hover { color: #66b1ff !important; }

.pagination-bar {
  display: flex; justify-content: center; padding: 12px 0;
}

.kline-nav {
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 16px;
  margin-bottom: 12px;
}

.kline-nav-text {
  font-size: 14px;
  color: #606266;
  min-width: 80px;
  text-align: center;
  font-family: monospace;
}

</style>
