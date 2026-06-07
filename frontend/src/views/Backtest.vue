<template>
  <div class="backtest-container">
    <NavBar />
    <div class="backtest-content">
      <h2>策略回测</h2>

      <!-- 回测配置 -->
      <el-card class="config-card">
        <template #header>
          <div class="card-header">
            <span>回测配置</span>
          </div>
        </template>

        <el-form :model="form" label-width="120px" label-position="right">
          <el-row :gutter="20">
            <el-col :span="8">
              <el-form-item label="回测名称">
                <el-input v-model="form.name" placeholder="请输入回测名称" />
              </el-form-item>
            </el-col>
            <el-col :span="8">
              <el-form-item label="回测模式">
                <el-tooltip content="撮合模拟盘：逐日推进，模拟真实交易（考虑仓位、现金、T+1）。收益率累积：快速信号配对，不考虑资金限制" placement="top">
                  <el-select v-model="form.mode" style="width: 100%">
                    <el-option label="撮合模拟盘" value="simulated" />
                    <el-option label="收益率累积" value="return_accumulation" />
                  </el-select>
                </el-tooltip>
              </el-form-item>
            </el-col>
            <el-col :span="8">
              <el-form-item label="选股策略">
                <el-tooltip content="可选。选择后使用该策略的买入条件和止盈止损参数；不选则使用下方手动配置" placement="top">
                  <el-select v-model="form.strategy_id" placeholder="可选" clearable style="width: 100%">
                    <el-option v-for="s in strategies" :key="s.id" :label="s.name" :value="s.id" />
                  </el-select>
                </el-tooltip>
              </el-form-item>
            </el-col>
            <el-col :span="8">
              <el-form-item label="交易策略">
                <el-tooltip content="可选。卖出信号策略，用于判断持仓何时卖出。支持多选，按顺序执行" placement="top">
                  <el-select v-model="form.trading_strategy_ids" multiple placeholder="可选（卖出策略）" clearable style="width: 100%">
                    <el-option v-for="s in tradingStrategies" :key="s.id" :label="s.name" :value="s.id" />
                  </el-select>
                </el-tooltip>
              </el-form-item>
            </el-col>
          </el-row>

          <el-row :gutter="20">
            <el-col :span="8">
              <el-form-item label="起始日期">
                <el-date-picker v-model="form.start_date" type="date" value-format="YYYY-MM-DD" style="width: 100%" />
              </el-form-item>
            </el-col>
            <el-col :span="8">
              <el-form-item label="结束日期">
                <el-date-picker v-model="form.end_date" type="date" value-format="YYYY-MM-DD" style="width: 100%" />
              </el-form-item>
            </el-col>
            <el-col :span="8">
              <el-form-item label="初始资金">
                <el-input-number v-model="form.initial_capital" :min="10000" :step="100000" style="width: 100%" />
              </el-form-item>
            </el-col>
          </el-row>

          <el-row :gutter="20">
            <el-col :span="8">
              <el-form-item label="市场选择">
                <el-select v-model="form.markets" multiple placeholder="全市场" style="width: 100%" clearable>
                  <el-option label="上海 (SH)" value="SH" />
                  <el-option label="深圳 (SZ)" value="SZ" />
                </el-select>
              </el-form-item>
            </el-col>
            <el-col :span="8">
              <el-form-item label="股票池">
                <el-tooltip content="选择候选组作为回测股票池，优先级高于市场选择" placement="top">
                  <el-select v-model="form.group_ids" multiple placeholder="使用市场选择" style="width: 100%" clearable filterable>
                    <el-option v-for="g in candidateGroups" :key="g.id" :label="`${g.name} (${g.stock_count}只)`" :value="g.id" />
                  </el-select>
                </el-tooltip>
              </el-form-item>
            </el-col>
            <el-col :span="8">
              <el-form-item label="动态股票池">
                <el-switch v-model="form.use_dynamic_pool" active-text="开启" inactive-text="关闭" />
                <el-tooltip content="开启后可按时间段分别指定不同的候选组，模拟动态调仓" placement="top">
                  <el-icon style="margin-left: 4px; color: #909399;"><QuestionFilled /></el-icon>
                </el-tooltip>
              </el-form-item>
            </el-col>
            <el-col :span="8">
              <el-form-item label="止损比例 (%)">
                <el-tooltip content="买入价下跌超过该比例时触发止损。例如填 5 表示亏损 5% 时卖出" placement="top">
                  <el-input-number v-model="form.stop_loss_pct" :min="0" :max="50" :step="1" :precision="1" style="width: 100%" />
                </el-tooltip>
              </el-form-item>
            </el-col>
            <el-col :span="8">
              <el-form-item label="止盈比例 (%)">
                <el-tooltip content="买入价上涨超过该比例时触发止盈。例如填 15 表示盈利 15% 时卖出" placement="top">
                  <el-input-number v-model="form.take_profit_pct" :min="0" :max="100" :step="1" :precision="1" style="width: 100%" />
                </el-tooltip>
              </el-form-item>
            </el-col>
            <el-col :span="8">
              <el-form-item label="移动止盈 (%)">
                <el-tooltip content="持仓期间，从最高点回撤超过该比例时触发卖出。例如填 3 表示从最高点回撤 3% 时止盈" placement="top">
                  <el-input-number v-model="form.trailing_stop_pct" :min="0" :max="50" :step="0.5" :precision="1" style="width: 100%" placeholder="可选" />
                </el-tooltip>
              </el-form-item>
            </el-col>
          </el-row>

          <el-row :gutter="20">
            <el-col :span="8">
              <el-form-item label="止盈止损成交价">
                <el-tooltip content="收盘价模式：触发后按当日收盘价成交；触发价模式：当日价格覆盖止盈/止损位即按触发价成交（更贴近实盘）" placement="top">
                  <el-select v-model="form.stop_execution_price" style="width: 100%">
                    <el-option label="收盘价" value="close" />
                    <el-option label="触发价" value="trigger" />
                  </el-select>
                </el-tooltip>
              </el-form-item>
            </el-col>
            <el-col :span="8">
              <el-form-item label="手续费率">
                <el-tooltip content="券商佣金费率，按成交金额计算。例如万分之0.86填 0.000086，万分之三填 0.0003。最低佣金 5 元（不足按 5 元收取）" placement="top">
                  <el-input-number v-model="form.commission_rate" :min="0" :max="0.01" :step="0.000001" :precision="6" style="width: 100%" />
                </el-tooltip>
              </el-form-item>
            </el-col>
            <el-col :span="8">
              <el-form-item label="滑点 (%)">
                <el-tooltip content="模拟成交价格的偏移，买入时成交价上浮、卖出时成交价下浮。例如填 0.1 表示买入按报价×1.001、卖出按报价×0.999 成交" placement="top">
                  <el-input-number v-model="form.slippage_pct" :min="0" :max="1" :step="0.01" :precision="2" style="width: 100%" />
                </el-tooltip>
              </el-form-item>
            </el-col>
          </el-row>

          <el-form-item label="回测说明">
            <el-input v-model="form.description" type="textarea" :rows="2" placeholder="简要记录回测目的、注意事项等（可选）" style="width: 100%" />
          </el-form-item>

          <!-- 动态股票池配置 -->
          <el-form-item v-if="form.use_dynamic_pool" label="池调度">
            <div style="width: 100%">
              <div style="margin-bottom: 8px; display: flex; gap: 8px;">
                <el-button size="small" @click="addPoolSegment" :icon="Plus">添加分段</el-button>
                <el-button size="small" @click="generateQuarterlySegments" :icon="Calendar">按季度生成</el-button>
                <span class="text-muted" style="font-size: 12px; line-height: 32px;">
                  共 {{ poolSchedule.length }} 段，覆盖 {{ getPoolDateRange() }}
                </span>
              </div>
              <el-table :data="poolSchedule" border size="small" style="width: 100%">
                <el-table-column label="起始日期" width="160">
                  <template #default="{ row }">
                    <el-date-picker v-model="row.start_date" type="date" value-format="YYYY-MM-DD" size="small" style="width: 100%" />
                  </template>
                </el-table-column>
                <el-table-column label="候选组" min-width="200">
                  <template #default="{ row }">
                    <el-select v-model="row.group_ids" multiple placeholder="选择候选组" size="small" style="width: 100%" filterable clearable>
                      <el-option v-for="g in candidateGroups" :key="g.id" :label="`${g.name} (${g.stock_count}只)`" :value="g.id" />
                    </el-select>
                  </template>
                </el-table-column>
                <el-table-column label="股票数" width="80">
                  <template #default="{ row }">
                    <span v-if="row.stock_pool !== undefined">{{ row.stock_pool.length }}</span>
                    <span v-else class="text-muted">—</span>
                  </template>
                </el-table-column>
                <el-table-column label="操作" width="60" fixed="right">
                  <template #default="{ $index }">
                    <el-button size="small" type="danger" link @click="removePoolSegment($index)">删除</el-button>
                  </template>
                </el-table-column>
              </el-table>
            </div>
          </el-form-item>

          <el-form-item>
            <el-button type="primary" @click="handleStartBacktest" :loading="running">
              开始回测
            </el-button>
            <el-button @click="handleCheckData">检查数据完整性</el-button>
          </el-form-item>
        </el-form>
      </el-card>

      <!-- 回测历史 -->
      <el-card class="history-card">
        <template #header>
          <div class="card-header">
            <span>回测历史</span>
            <div>
              <el-dropdown v-if="history.length > 0" @command="(fmt) => handleExportHistory(fmt)">
                <el-button type="success" size="small">
                  <el-icon><Download /></el-icon>导出
                </el-button>
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
              <el-button v-if="selectedRuns.length >= 2" size="small" type="success" @click="handleCompare">
                对比 ({{ selectedRuns.length }})
              </el-button>
              <el-button size="small" @click="loadHistory">
                <el-icon><Refresh /></el-icon>
                刷新
              </el-button>
            </div>
          </div>
        </template>

        <el-table :data="history" v-loading="loadingHistory" stripe @selection-change="handleSelectionChange" @row-dblclick="handleRowDblclick">
          <el-table-column type="selection" width="40" :selectable="(row) => row.status === 'completed'" />
          <el-table-column label="回测名称" min-width="100">
            <template #default="{ row }">
              <el-tooltip v-if="row.description" :content="row.description" placement="top">
                <span>{{ row.name }}</span>
              </el-tooltip>
              <span v-else>{{ row.name }}</span>
            </template>
          </el-table-column>
          <el-table-column prop="strategy_name" label="策略" width="100" show-overflow-tooltip>
            <template #default="{ row }">
              <span v-if="row.strategy_name">{{ row.strategy_name }}</span>
              <span v-else class="text-muted">手动</span>
            </template>
          </el-table-column>
          <el-table-column prop="mode" label="模式" width="120">
            <template #default="{ row }">
              {{ row.mode === 'simulated' ? '撮合模拟盘' : '收益率累积' }}
            </template>
          </el-table-column>
          <el-table-column prop="start_date" label="起始日" width="110" />
          <el-table-column prop="end_date" label="结束日" width="110" />
          <el-table-column prop="initial_capital" label="初始资金" width="120">
            <template #default="{ row }">{{ formatMoney(row.initial_capital) }}</template>
          </el-table-column>
          <el-table-column label="总收益" width="100">
            <template #default="{ row }">
              <span v-if="row.result_summary" :class="row.result_summary.total_return >= 0 ? 'text-green' : 'text-red'">
                {{ row.result_summary.total_return }}%
              </span>
              <span v-else-if="row.status === 'running'" class="text-muted">计算中...</span>
              <span v-else>-</span>
            </template>
          </el-table-column>
          <el-table-column label="年化" width="90">
            <template #default="{ row }">
              <span v-if="row.result_summary">{{ row.result_summary.annualized_return }}%</span>
              <span v-else-if="row.status === 'running'" class="text-muted">计算中...</span>
              <span v-else>-</span>
            </template>
          </el-table-column>
          <el-table-column label="最大回撤" width="100">
            <template #default="{ row }">
              <span v-if="row.result_summary" class="text-red">{{ row.result_summary.max_drawdown }}%</span>
              <span v-else-if="row.status === 'running'" class="text-muted">计算中...</span>
              <span v-else>-</span>
            </template>
          </el-table-column>
          <el-table-column label="夏普" width="80">
            <template #default="{ row }">
              <span v-if="row.result_summary">{{ row.result_summary.sharpe_ratio }}</span>
              <span v-else-if="row.status === 'running'" class="text-muted">计算中...</span>
              <span v-else>-</span>
            </template>
          </el-table-column>
          <el-table-column label="胜率" width="80">
            <template #default="{ row }">
              <span v-if="row.result_summary">{{ row.result_summary.win_rate }}%</span>
              <span v-else-if="row.status === 'running'" class="text-muted">计算中...</span>
              <span v-else>-</span>
            </template>
          </el-table-column>
          <el-table-column label="状态" width="160">
            <template #default="{ row }">
              <el-tag v-if="row.status === 'completed'" type="success" size="small">完成</el-tag>
              <el-tag v-else-if="row.status === 'running'" type="warning" size="small">运行中 {{ row.progress }}%</el-tag>
              <el-tooltip v-else-if="row.status === 'failed' && row.error_message" :content="row.error_message" placement="top">
                <el-tag type="danger" size="small">失败</el-tag>
              </el-tooltip>
              <el-tag v-else-if="row.status === 'failed'" type="danger" size="small">失败</el-tag>
              <el-tag v-else type="info" size="small">{{ row.status }}</el-tag>
              <div v-if="row.status === 'running' && row.current_trade_date" class="backtest-date">回测至 {{ row.current_trade_date }}</div>
            </template>
          </el-table-column>
          <el-table-column label="操作" width="220" fixed="right">
            <template #default="{ row }">
              <div class="action-buttons">
                <el-button size="small" type="primary" @click="viewDetail(row)" :disabled="row.status !== 'completed'">详情</el-button>
                <el-button v-if="row.status === 'failed'" size="small" type="warning" @click="rerunBacktest(row)">重试</el-button>
                <el-button size="small" type="danger" @click="deleteRun(row.id)">删除</el-button>
              </div>
            </template>
          </el-table-column>
        </el-table>

        <div style="display: flex; justify-content: flex-end; align-items: center; margin-top: 16px; gap: 16px">
          <span style="font-size: 13px; color: #909399">共 {{ totalRuns }} 条</span>
          <el-select v-model="pageSize" @change="handlePageSizeChange" style="width: 120px">
            <el-option v-for="size in [5, 10, 20, 50]" :key="size" :label="`${size} 条/页`" :value="size" />
          </el-select>
          <el-pagination
            v-model:current-page="currentPage"
            :page-size="pageSize"
            :total="totalRuns"
            layout="prev, pager, next"
            @current-change="handlePageChange"
          />
        </div>
      </el-card>

      <!-- 回测详情对话框 -->
      <el-dialog v-model="detailVisible" :title="`回测详情 - ${currentRun?.name}`" width="90%" top="5vh">
        <!-- 回测参数 -->
        <el-card style="margin-bottom: 16px">
          <template #header><span>回测参数</span></template>
          <el-descriptions :column="4" border size="small">
            <el-descriptions-item label="回测名称" :span="3">{{ currentRun.name }}</el-descriptions-item>
            <el-descriptions-item label="模式">
              {{ currentRun.mode === 'simulated' ? '撮合模拟盘' : '收益率累积' }}
            </el-descriptions-item>
            <el-descriptions-item label="策略">
              <span v-if="currentRun.strategy_name">{{ currentRun.strategy_name }}</span>
              <span v-else class="text-muted">手动配置</span>
            </el-descriptions-item>
            <el-descriptions-item label="日期范围">{{ currentRun.start_date }} ~ {{ currentRun.end_date }}</el-descriptions-item>
            <el-descriptions-item label="初始资金">{{ formatMoney(currentRun.initial_capital) }}</el-descriptions-item>
            <el-descriptions-item label="股票池">
              <span v-if="currentRun.group_ids && currentRun.group_ids.length > 0">候选组 {{ getGroupNames(currentRun.group_ids, currentRun.group_names) }}</span>
              <span v-else-if="currentRun.markets && currentRun.markets.length > 0">{{ currentRun.markets.join(', ') }}</span>
              <span v-else-if="currentRun.stock_pool && currentRun.stock_pool.length > 0">{{ currentRun.stock_pool.length }} 只</span>
              <span v-else class="text-muted">全市场</span>
            </el-descriptions-item>
            <el-descriptions-item v-if="currentRun.description" label="回测说明" :span="4">{{ currentRun.description }}</el-descriptions-item>
            <el-descriptions-item v-if="currentRun.pool_schedule && currentRun.pool_schedule.length > 0" label="动态股票池" :span="4">
              <el-table :data="currentRun.pool_schedule" border size="small" style="width: 100%">
                <el-table-column label="时间段" width="220">
                  <template #default="{ row, $index }">
                    <template v-if="row.start_date">
                      {{ row.start_date }} ~
                      <template v-if="row.end_date">{{ row.end_date }}</template>
                      <template v-else-if="$index + 1 < currentRun.pool_schedule.length">{{ currentRun.pool_schedule[$index + 1].start_date }}</template>
                      <template v-else>{{ currentRun.end_date }}</template>
                    </template>
                    <span v-else class="text-muted">—</span>
                  </template>
                </el-table-column>
                <el-table-column label="候选组" min-width="200">
                  <template #default="{ row }">
                    <span v-if="row.group_ids && row.group_names">
                      {{ row.group_names.join(', ') }}
                    </span>
                    <span v-else-if="row.group_ids" class="text-muted">
                      候选组 {{ row.group_ids.join(', ') }}
                    </span>
                    <span v-else-if="row.stock_pool" class="text-muted">
                      {{ row.stock_pool.length }} 只
                    </span>
                    <span v-else class="text-muted">—</span>
                  </template>
                </el-table-column>
              </el-table>
            </el-descriptions-item>
            <el-descriptions-item label="止损 / 止盈">
              {{ formatPct(currentRun.stop_loss_pct) }} / {{ formatPct(currentRun.take_profit_pct) }}
            </el-descriptions-item>
            <el-descriptions-item label="移动止盈">
              <span v-if="currentRun.trailing_stop_pct">{{ formatPct(currentRun.trailing_stop_pct) }}</span>
              <span v-else class="text-muted">未设置</span>
            </el-descriptions-item>
            <el-descriptions-item label="止盈止损成交价">
              {{ currentRun.stop_execution_price === 'trigger' ? '触发价' : '收盘价' }}
            </el-descriptions-item>
            <el-descriptions-item label="滑点">{{ formatPct(currentRun.slippage_pct) }}</el-descriptions-item>
            <el-descriptions-item label="手续费率">{{ formatPct(currentRun.commission_rate) }}</el-descriptions-item>
            <el-descriptions-item label="最低佣金">¥{{ currentRun.min_commission || 5.0 }}</el-descriptions-item>
            <el-descriptions-item label="印花税">{{ formatPct(currentRun.stamp_tax) }}</el-descriptions-item>
            <el-descriptions-item label="过户费">{{ formatPct(currentRun.transfer_fee) }}</el-descriptions-item>
            <el-descriptions-item label="最大总仓位">{{ formatPct(currentRun.max_total_position_pct) }}</el-descriptions-item>
            <el-descriptions-item label="单股最大仓位">{{ formatPct(currentRun.max_single_position_pct) }}</el-descriptions-item>
            <el-descriptions-item label="现金预留">{{ formatPct(currentRun.cash_reserve_pct) }}</el-descriptions-item>
          </el-descriptions>
        </el-card>

        <!-- 绩效指标 -->
        <el-row :gutter="16" style="margin-bottom: 20px">
          <el-col :span="4" v-for="item in metricCards" :key="item.label">
            <el-card shadow="hover" class="metric-card">
              <div class="metric-label">{{ item.label }}</div>
              <div class="metric-value" :class="item.class">{{ item.value }}</div>
            </el-card>
          </el-col>
        </el-row>

        <!-- 分年度统计 -->
        <el-card style="margin-bottom: 16px">
          <template #header><span>分年度统计</span></template>
          <el-table :data="yearlyReturns" stripe>
            <el-table-column prop="year" label="年份" width="100" />
            <el-table-column label="收益率" width="120">
              <template #default="{ row }">
                <span :class="row.return >= 0 ? 'text-green' : 'text-red'">{{ row.return?.toFixed(2) || '-' }}%</span>
              </template>
            </el-table-column>
            <el-table-column label="最大回撤" width="120">
              <template #default="{ row }">
                <span class="text-red">{{ row.max_drawdown?.toFixed(2) || '-' }}%</span>
              </template>
            </el-table-column>
          </el-table>
        </el-card>

        <!-- 基准对比 -->
        <el-card style="margin-bottom: 16px" v-if="benchmarkData">
          <template #header><span>基准对比（沪深300）</span></template>
          <el-descriptions :column="4" border size="small">
            <el-descriptions-item label="策略收益">{{ currentRun?.result_summary?.annualized_return ?? '-' }}%</el-descriptions-item>
            <el-descriptions-item label="基准收益">{{ benchmarkData.benchmark_return }}</el-descriptions-item>
            <el-descriptions-item label="Alpha" :class="Number(benchmarkData.alpha) >= 0 ? 'text-green' : 'text-red'">{{ benchmarkData.alpha }}</el-descriptions-item>
            <el-descriptions-item label="Beta">{{ benchmarkData.beta ?? '-' }}</el-descriptions-item>
          </el-descriptions>
        </el-card>

        <!-- 净值曲线 -->
        <el-card style="margin-top: 16px">
          <template #header><span>净值曲线</span></template>
          <div ref="navChartRef" style="width: 100%; height: 400px"></div>
        </el-card>

        <!-- 交易记录 -->
        <el-card style="margin-top: 16px">
          <template #header>
            <div class="card-header">
              <span>交易记录 ({{ trades.length }} 笔)</span>
              <el-dropdown v-if="trades.length > 0" @command="(fmt) => handleExportTrades(fmt)">
                <el-button type="success" size="small">
                  <el-icon><Download /></el-icon>导出
                </el-button>
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
            </div>
          </template>
          <el-table :data="trades" stripe max-height="400" @row-dblclick="showTradeKline">
            <el-table-column prop="stock_code" label="股票代码" width="110" />
            <el-table-column prop="stock_name" label="股票名称" width="100" />
            <el-table-column label="买入日期" width="110">
              <template #default="{ row }">{{ row.buy_date || '-' }}</template>
            </el-table-column>
            <el-table-column label="买入数量" width="90">
              <template #default="{ row }">{{ row.buy_quantity || '-' }}</template>
            </el-table-column>
            <el-table-column label="买入价格" width="90">
              <template #default="{ row }">{{ row.buy_price?.toFixed(2) || '-' }}</template>
            </el-table-column>
            <el-table-column label="卖出日期" width="110">
              <template #default="{ row }">{{ row.sell_date || '-' }}</template>
            </el-table-column>
            <el-table-column label="卖出数量" width="90">
              <template #default="{ row }">{{ row.sell_quantity || '-' }}</template>
            </el-table-column>
            <el-table-column label="剩余持仓" width="90">
              <template #default="{ row }">{{ row.remaining_quantity || '-' }}</template>
            </el-table-column>
            <el-table-column label="卖出价格" width="90">
              <template #default="{ row }">{{ row.sell_price?.toFixed(2) || '-' }}</template>
            </el-table-column>
            <el-table-column label="盈亏(%)" width="100">
              <template #default="{ row }">
                <span :class="row.pnl_pct >= 0 ? 'text-green' : 'text-red'">
                  {{ row.pnl_pct?.toFixed(2) || '0.00' }}%
                </span>
              </template>
            </el-table-column>
            <el-table-column label="持仓天数" width="90">
              <template #default="{ row }">{{ row.holding_days || '-' }}</template>
            </el-table-column>
            <el-table-column label="卖出原因" width="200">
              <template #default="{ row }">{{ row.sell_reason || '-' }}</template>
            </el-table-column>
          </el-table>
        </el-card>
      </el-dialog>

      <!-- K 线图弹窗 -->
      <el-dialog v-model="klineVisible" :title="`${klineStockInfo.name} (${klineStockInfo.code}) K线图`" width="80%" top="5vh" @close="destroyKlineChart">
        <div ref="klineChartRef" style="width: 100%; height: 600px"></div>
      </el-dialog>

      <!-- 数据完整性检查对话框 -->
      <el-dialog v-model="dataCheckVisible" title="数据完整性检查" width="60%">
        <div v-if="dataCheckReport">
          <el-descriptions :column="2" border>
            <el-descriptions-item label="检查股票数">{{ dataCheckReport.total_stocks_checked }}</el-descriptions-item>
            <el-descriptions-item label="完整覆盖">{{ dataCheckReport.stocks_with_full_coverage }}</el-descriptions-item>
            <el-descriptions-item label="覆盖率">{{ dataCheckReport.coverage_pct }}%</el-descriptions-item>
            <el-descriptions-item label="可继续">
              <el-tag :type="dataCheckReport.can_proceed ? 'success' : 'danger'">
                {{ dataCheckReport.can_proceed ? '是' : '否' }}
              </el-tag>
            </el-descriptions-item>
          </el-descriptions>

          <div v-if="dataCheckReport.blocking_count > 0" style="margin-top: 16px">
            <h4 style="color: #f56c6c">严重问题 ({{ dataCheckReport.blocking_count }})</h4>
            <el-alert v-for="(s, i) in dataCheckReport.blocking_summary" :key="i" :title="s" type="error" :closable="false" style="margin-bottom: 8px" />
          </div>

          <div v-if="dataCheckReport.warning_count > 0" style="margin-top: 16px">
            <h4 style="color: #e6a23c">警告 ({{ dataCheckReport.warning_count }})</h4>
            <el-alert v-for="(s, i) in dataCheckReport.warning_summary" :key="i" :title="s" type="warning" :closable="false" style="margin-bottom: 8px" />
          </div>
        </div>
        <template #footer>
          <el-button @click="dataCheckVisible = false">关闭</el-button>
        </template>
      </el-dialog>

      <!-- 回测对比对话框 -->
      <el-dialog v-model="compareVisible" title="回测对比" width="90%" top="5vh" @close="destroyCompareChart">
        <!-- 指标对比表 -->
        <el-table :data="compareMetrics" stripe style="margin-bottom: 20px">
          <el-table-column prop="label" label="指标" width="120" fixed />
          <el-table-column v-for="run in compareRuns" :key="run.id" :label="run.name" min-width="120" show-overflow-tooltip>
            <template #default="{ row }">
              <span :class="row[`best_${run.id}`] ? 'text-green' : ''">{{ row[`run_${run.id}`] }}</span>
            </template>
          </el-table-column>
        </el-table>

        <!-- 净值曲线对比 -->
        <el-card>
          <template #header><span>净值曲线对比</span></template>
          <div ref="compareChartRef" style="width: 100%; height: 450px"></div>
        </el-card>
      </el-dialog>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, nextTick, onBeforeUnmount } from 'vue'
import { ElMessage, ElMessageBox, ElTooltip } from 'element-plus'
import { Refresh, Plus, Calendar, Download } from '@element-plus/icons-vue'
import NavBar from '../components/NavBar.vue'
import { useAccountStore } from '../stores/account'
import { exportTable as doExport } from '@/utils/exportHelper'
import * as echarts from 'echarts'

const accountStore = useAccountStore()
const currentAccountId = computed(() => accountStore.currentAccountId)

// 表单数据
const form = ref({
  name: '',
  mode: 'simulated',
  strategy_id: null,
  trading_strategy_ids: [],
  start_date: '2024-01-01',
  end_date: '2025-12-31',
  initial_capital: 1000000,
  markets: [],
  group_ids: [],
  stop_loss_pct: 5,
  take_profit_pct: 15,
  trailing_stop_pct: null,
  stop_execution_price: 'trigger',
  commission_rate: 0.0001,
  slippage_pct: 0,
  description: '',
  use_dynamic_pool: false,
})

const candidateGroups = ref([])

const strategies = ref([])
const tradingStrategies = ref([])
const history = ref([])
const running = ref(false)
const loadingHistory = ref(false)

// 分页
const currentPage = ref(1)
const pageSize = ref(10)
const totalRuns = ref(0)

// 回测详情
const detailVisible = ref(false)
const currentRun = ref(null)
const trades = ref([])
const navData = ref([])
const navChartRef = ref(null)
let navChart = null

// 数据完整性检查
const dataCheckVisible = ref(false)
const dataCheckReport = ref(null)

// 回测对比
const selectedRuns = ref([])
const compareVisible = ref(false)
const compareRuns = ref([])
const compareNavData = ref({})
const compareMetrics = ref([])
const compareChartRef = ref(null)
let compareChart = null

// K 线弹窗
const klineVisible = ref(false)
const klineChartRef = ref(null)
const klineStockInfo = ref({ code: '', name: '' })
let klineChart = null

// 动态股票池
const poolSchedule = ref([])

// 指标卡片
const metricCards = computed(() => {
  const r = currentRun.value?.result_summary || {}
  return [
    { label: '总收益率', value: r.total_return != null ? r.total_return + '%' : '-', class: r.total_return >= 0 ? 'text-green' : 'text-red' },
    { label: '年化收益率', value: r.annualized_return != null ? r.annualized_return + '%' : '-', class: r.annualized_return >= 0 ? 'text-green' : 'text-red' },
    { label: '最大回撤', value: r.max_drawdown != null ? r.max_drawdown + '%' : '-', class: 'text-red' },
    { label: '夏普比率', value: r.sharpe_ratio ?? '-' },
    { label: '卡玛比率', value: r.calmar_ratio ?? '-' },
    { label: '胜率', value: r.win_rate != null ? r.win_rate + '%' : '-' },
    { label: '盈亏比', value: r.profit_factor ?? '-' },
    { label: '总交易次数', value: r.total_trades ?? '-' },
    { label: '平均持仓', value: r.avg_holding_days != null ? r.avg_holding_days + '天' : '-' },
    { label: '最佳交易', value: r.best_trade != null ? r.best_trade + '%' : '-', class: 'text-green' },
    { label: '最差交易', value: r.worst_trade != null ? r.worst_trade + '%' : '-', class: 'text-red' },
    { label: '最终净值', value: r.final_nav ?? '-' },
  ]
})

// 分年度统计
const yearlyReturns = computed(() => {
  return currentRun.value?.result_summary?.yearly_returns || []
})

// 基准对比
const benchmarkData = computed(() => {
  const r = currentRun.value?.result_summary || {}
  if (r.benchmark_return == null) return null
  return {
    benchmark_return: r.benchmark_return + '%',
    benchmark_annualized: r.benchmark_annualized + '%',
    alpha: r.alpha + '%',
    beta: r.beta,
  }
})

// 加载策略列表
const loadStrategies = async () => {
  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/strategies`)
    const data = await res.json()
    const allStrategies = data.strategies || []
    // 分类：选股策略（screening）和交易策略（trading）
    strategies.value = allStrategies.filter(s => s.code_scope === 'screening' || !s.code_scope)
    tradingStrategies.value = allStrategies.filter(s => s.code_scope === 'trading')
  } catch (e) {
    console.error('加载策略失败:', e)
  }
}

// 加载候选组列表
const loadCandidateGroups = async () => {
  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/candidate-groups`)
    const data = await res.json()
    candidateGroups.value = data.groups || []
  } catch (e) {
    console.error('加载候选组失败:', e)
  }
}

// 加载回测历史
const loadHistory = async (silent = false) => {
  if (!silent) loadingHistory.value = true
  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/backtest/runs?page=${currentPage.value}&page_size=${pageSize.value}`)
    const data = await res.json()
    history.value = data.runs || []
    totalRuns.value = data.pagination?.total || 0
  } catch (e) {
    console.error('加载回测历史失败:', e)
  } finally {
    if (!silent) loadingHistory.value = false
  }
}

// 开始回测
const historyColumns = [
  { label: '回测名称', prop: 'name' },
  { label: '策略', prop: 'strategy_name' },
  { label: '模式', prop: 'mode' },
  { label: '起始日', prop: 'start_date' },
  { label: '结束日', prop: 'end_date' },
  { label: '初始资金', prop: 'initial_capital' },
  { label: '总收益', prop: 'total_return' },
  { label: '年化', prop: 'annualized_return' },
  { label: '最大回撤', prop: 'max_drawdown' },
  { label: '夏普', prop: 'sharpe_ratio' },
  { label: '胜率', prop: 'win_rate' },
  { label: '状态', prop: 'status' },
]

const handleExportHistory = (format) => {
  const data = history.value.map(row => ({
    ...row,
    mode: row.mode === 'simulated' ? '撮合模拟盘' : '收益率累积',
    initial_capital: formatMoney(row.initial_capital),
    total_return: row.result_summary ? row.result_summary.total_return + '%' : '-',
    annualized_return: row.result_summary ? row.result_summary.annualized_return + '%' : '-',
    max_drawdown: row.result_summary ? row.result_summary.max_drawdown + '%' : '-',
    sharpe_ratio: row.result_summary ? row.result_summary.sharpe_ratio : '-',
    win_rate: row.result_summary ? row.result_summary.win_rate + '%' : '-',
    status: row.status === 'completed' ? '完成' : row.status === 'running' ? `运行中 ${row.progress}%` : row.status,
  }))
  doExport(historyColumns, data, '回测历史', format)
}

const tradeColumns = [
  { label: '股票代码', prop: 'stock_code' },
  { label: '股票名称', prop: 'stock_name' },
  { label: '买入日期', prop: 'buy_date' },
  { label: '买入价格', prop: 'buy_price' },
  { label: '卖出日期', prop: 'sell_date' },
  { label: '卖出价格', prop: 'sell_price' },
  { label: '买入佣金', prop: 'buy_commission' },
  { label: '卖出费用', prop: 'sell_commission' },
  { label: '盈亏(%)', prop: 'pnl_pct' },
  { label: '持仓天数', prop: 'holding_days' },
  { label: '卖出原因', prop: 'sell_reason' },
]

const handleExportTrades = (format) => {
  const data = trades.value.map(row => ({
    ...row,
    buy_price: row.buy_price != null ? row.buy_price.toFixed(2) : '-',
    sell_price: row.sell_price != null ? row.sell_price.toFixed(2) : '-',
    buy_commission: row.buy_commission != null ? row.buy_commission.toFixed(2) : '-',
    sell_commission: row.sell_commission != null ? row.sell_commission.toFixed(2) : '-',
    pnl_pct: row.pnl_pct != null ? row.pnl_pct.toFixed(2) + '%' : '0.00%',
    sell_date: row.sell_date || '-',
    holding_days: row.holding_days || '-',
    sell_reason: row.sell_reason || '-',
  }))
  doExport(tradeColumns, data, '回测交易记录', format)
}

// 动态股票池操作
const addPoolSegment = () => {
  poolSchedule.value.push({
    start_date: '',
    group_ids: [],
    stock_pool: [],
  })
}

const removePoolSegment = (index) => {
  poolSchedule.value.splice(index, 1)
}

const generateQuarterlySegments = () => {
  // 根据 form 的起止日期，按季度生成
  const start = new Date(form.value.start_date)
  const end = new Date(form.value.end_date)
  if (isNaN(start.getTime()) || isNaN(end.getTime())) {
    ElMessage.warning('请先设置起始和结束日期')
    return
  }

  const segments = []
  let year = start.getFullYear()
  let quarter = Math.floor(start.getMonth() / 3) + 1

  while (true) {
    const qStart = new Date(year, (quarter - 1) * 3, 1)
    const qEnd = new Date(year, quarter * 3, 0)
    // 截断到用户设置的起止日期范围内
    const segStart = qStart < start ? new Date(start) : qStart
    const segEnd = qEnd > end ? new Date(end) : qEnd

    if (segStart >= segEnd) break

    segments.push({
      start_date: segStart.toISOString().slice(0, 10),
      group_ids: [],
      stock_pool: [],
    })

    quarter++
    if (quarter > 4) {
      quarter = 1
      year++
    }
    if (year > end.getFullYear() || (year === end.getFullYear() && (quarter - 1) * 3 > end.getMonth())) {
      break
    }
  }

  poolSchedule.value = segments
  ElMessage.success(`已生成 ${segments.length} 个季度分段`)
}

const getPoolDateRange = () => {
  if (poolSchedule.value.length === 0) return '—'
  const starts = poolSchedule.value
    .filter(s => s.start_date)
    .map(s => s.start_date)
    .sort()
  if (starts.length === 0) return '—'
  return `${starts[0]} 起`
}

const handleStartBacktest = async () => {
  if (!form.value.start_date || !form.value.end_date) {
    ElMessage.warning('请选择起始和结束日期')
    return
  }

  // 后端已有时段判断，前端不再拦截
  running.value = true
  try {
    const body = {
      name: form.value.name || `回测 ${form.value.start_date} ~ ${form.value.end_date}`,
      mode: form.value.mode,
      strategy_id: form.value.strategy_id,
      trading_strategy_ids: form.value.trading_strategy_ids.length > 0 ? form.value.trading_strategy_ids : null,
      start_date: form.value.start_date,
      end_date: form.value.end_date,
      initial_capital: form.value.initial_capital,
      stop_loss_pct: form.value.stop_loss_pct / 100,
      take_profit_pct: form.value.take_profit_pct / 100,
      trailing_stop_pct: form.value.trailing_stop_pct ? form.value.trailing_stop_pct / 100 : null,
      stop_execution_price: form.value.stop_execution_price,
      commission_rate: form.value.commission_rate,
      slippage_pct: form.value.slippage_pct / 100,
      description: form.value.description || null,
      markets: form.value.markets.length > 0 ? form.value.markets : null,
      group_ids: form.value.group_ids.length > 0 ? form.value.group_ids : null,
      config: {},
    }

    // 动态股票池
    if (form.value.use_dynamic_pool && poolSchedule.value.length > 0) {
      body.pool_schedule = poolSchedule.value.map(s => ({
        start_date: s.start_date,
        group_ids: s.group_ids,
      })).filter(s => s.start_date && s.group_ids.length > 0)
      if (body.pool_schedule.length === 0) {
        ElMessage.warning('动态股票池至少需要配置一个有效分段（日期+候选组）')
        return
      }
      // 动态池模式下，跳过全市场检查
    }

    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/backtest/runs`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    })
    const data = await res.json()

    if (data.success) {
      ElMessage.success('回测任务已启动，请稍后刷新查看结果')
      loadHistory()
    } else {
      ElMessage.error(data.message || data.error || '回测启动失败')
    }
  } catch (e) {
    ElMessage.error('网络错误: ' + e.message)
  } finally {
    running.value = false
  }
}

// 重试失败的回测
const rerunBacktest = async (row) => {
  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/backtest/runs/${row.id}/retry`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
    })
    const data = await res.json()

    if (data.success) {
      ElMessage.success(`回测任务已重新启动 (ID: ${data.run_id})`)
      loadHistory()
    } else {
      ElMessage.error(data.message || data.error || '回测启动失败')
    }
  } catch (e) {
    ElMessage.error('网络错误: ' + e.message)
  }
}

// 检查数据完整性
const handleCheckData = async () => {
  try {
    const body = {
      start_date: form.value.start_date,
      end_date: form.value.end_date,
      markets: form.value.markets.length > 0 ? form.value.markets : null,
      group_ids: form.value.group_ids.length > 0 ? form.value.group_ids : null,
    }

    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/backtest/check-data`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    })
    const data = await res.json()

    if (data.success) {
      dataCheckReport.value = data.report
      dataCheckVisible.value = true
    } else {
      ElMessage.error('数据检查失败')
    }
  } catch (e) {
    ElMessage.error('网络错误: ' + e.message)
  }
}

// 查看回测详情
const viewDetail = async (run) => {
  currentRun.value = run
  detailVisible.value = true

  // 加载回测详情（获取完整参数）
  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/backtest/runs/${run.id}`)
    const data = await res.json()
    currentRun.value = data.run

    // 解析 pool_schedule 中的候选组名称
    if (currentRun.value?.pool_schedule && currentRun.value.pool_schedule.length > 0) {
      for (const seg of currentRun.value.pool_schedule) {
        if (seg.group_ids && seg.group_ids.length > 0) {
          const ids = seg.group_ids
          const placeholders = ids.map(() => '?').join(',')
          const groupRes = await fetch(`/api/v1/ui/${currentAccountId.value}/candidate-groups`)
          const groupData = await groupRes.json()
          const groups = groupData.groups || []
          seg.group_names = ids.map(id => {
            const g = groups.find(g => g.id === id)
            return g ? g.name : `ID:${id}`
          })
        }
      }
    }
  } catch (e) {
    console.error('加载回测详情失败:', e)
  }

  // 加载交易记录
  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/backtest/runs/${run.id}/trades`)
    const data = await res.json()
    trades.value = data.trades || []
  } catch (e) {
    console.error('加载交易记录失败:', e)
  }

  // 加载净值数据并渲染图表
  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/backtest/runs/${run.id}/nav`)
    const data = await res.json()
    navData.value = data.nav || []
    await nextTick()
    renderNavChart()
  } catch (e) {
    console.error('加载净值数据失败:', e)
  }
}

// 渲染净值曲线
const renderNavChart = () => {
  if (!navChartRef.value) return

  if (!navChart) {
    navChart = echarts.init(navChartRef.value)
  }

  const dates = navData.value.map(d => d.trade_date)
  const navs = navData.value.map(d => d.nav)
  const values = navData.value.map(d => d.total_value)
  const drawdowns = navData.value.map(d => d.drawdown * 100)

  navChart.setOption({
    tooltip: {
      trigger: 'axis',
      formatter: (params) => {
        if (!params || params.length === 0) return ''
        const idx = params[0].dataIndex
        const date = params[0].axisValue
        const row = navData.value[idx]
        let html = `<b>${date}</b><br/>`
        for (const p of params) {
          const val = typeof p.value === 'number' ? p.value.toFixed(4) : p.value
          html += `${p.marker} ${p.seriesName}: ${val}<br/>`
        }
        const positions = row?.positions
        if (positions && positions.length > 0) {
          html += '<b>持仓:</b><br/>'
          for (const pos of positions) {
            html += `${pos.stock_name}(${pos.stock_code}): ${pos.quantity}<br/>`
          }
        } else {
          html += '<b>持仓: 空仓</b>'
        }
        return html
      },
    },
    grid: { left: '3%', right: '4%', bottom: '3%', containLabel: true },
    xAxis: { type: 'category', data: dates, axisLabel: { rotate: 45 } },
    yAxis: [
      { type: 'value', name: '净值', position: 'left' },
      { type: 'value', name: '总资产', position: 'right' },
    ],
    series: [
      {
        name: '净值',
        type: 'line',
        data: navs,
        smooth: true,
        yAxisIndex: 0,
        lineStyle: { width: 2 },
        areaStyle: {
          color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
            { offset: 0, color: 'rgba(64, 158, 255, 0.3)' },
            { offset: 1, color: 'rgba(64, 158, 255, 0.05)' },
          ]),
        },
      },
      {
        name: '总资产',
        type: 'line',
        data: values,
        smooth: true,
        yAxisIndex: 1,
        lineStyle: { width: 1, type: 'dashed' },
        showSymbol: false,
      },
    ],
    dataZoom: [{ type: 'inside' }, { type: 'slider' }],
  })

  // 窗口大小变化时重新渲染
  window.addEventListener('resize', () => navChart.resize())
}

// 显示交易 K 线图
const showTradeKline = async (row) => {
  klineStockInfo.value = { code: row.stock_code, name: row.stock_name }
  klineVisible.value = true

  const buyDate = row.buy_date
  const sellDate = row.sell_date || new Date().toISOString().slice(0, 10)

  // 买入当月1日
  const buyMonth = buyDate.slice(0, 7)
  const start = buyMonth + '-01'

  // 卖出月月末
  const sellMonth = sellDate.slice(0, 7)
  const nextMonth = new Date(sellMonth + '-01')
  nextMonth.setMonth(nextMonth.getMonth() + 1)
  nextMonth.setDate(0)
  const end = nextMonth.toISOString().slice(0, 10)

  const startFmt = start.replace(/-/g, '')
  const endFmt = end.replace(/-/g, '')

  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/market/kline?stock_code=${row.stock_code}&period=day&start_date=${startFmt}&end_date=${endFmt}`)
    const data = await res.json()
    await nextTick()
    renderKlineChart(data.data?.kline || [], row)
  } catch (e) {
    console.error('加载 K 线数据失败:', e)
  }
}

// 渲染 K 线图
const renderKlineChart = (klineData, trade) => {
  if (!klineChartRef.value) return
  if (!klineChart) {
    klineChart = echarts.init(klineChartRef.value)
  }

  const dates = klineData.map(d => String(d.trade_date))
  const values = klineData.map(d => [d.open, d.close, d.low, d.high])

  // 标准化日期用于匹配
  const normalizeDate = (ds) => {
    if (!ds) return ''
    return String(ds).replace(/-/g, '')
  }

  const buyDateNorm = normalizeDate(trade.buy_date)
  const sellDateNorm = normalizeDate(trade.sell_date)

  // 调试：打印日期匹配信息
  console.log('Trade buy_date:', trade.buy_date, 'normalized:', buyDateNorm)
  console.log('Trade sell_date:', trade.sell_date, 'normalized:', sellDateNorm)
  console.log('K-line data count:', klineData.length, 'first item:', klineData[0])
  const nonEmptyDates = dates.filter(d => d && d !== '')
  console.log('K-line dates with values:', nonEmptyDates.length, '/', dates.length)
  if (nonEmptyDates.length > 0) {
    console.log('K-line dates range:', nonEmptyDates[0], '~', nonEmptyDates[nonEmptyDates.length - 1])
  }

  // 找最高/最低价及对应索引
  let maxPrice = -Infinity, minPrice = Infinity, maxIdx = -1, minIdx = -1
  for (let i = 0; i < klineData.length; i++) {
    const d = klineData[i]
    if (d.high > maxPrice) { maxPrice = d.high; maxIdx = i }
    if (d.low < minPrice) { minPrice = d.low; minIdx = i }
  }

  // 找买入/卖出日期对应的 K线索引
  let buyIdx = -1, sellIdx = -1
  for (let i = 0; i < dates.length; i++) {
    const dn = normalizeDate(dates[i])
    if (dn === buyDateNorm) buyIdx = i
    if (sellDateNorm && dn === sellDateNorm) sellIdx = i
  }
  console.log('buyIdx:', buyIdx, 'sellIdx:', sellIdx)

  // 用 markLine 画水平线标注价格，用 markPoint 标注位置
  const markPointData = []
  if (maxIdx >= 0) {
    markPointData.push({
      name: '最高价',
      xAxis: maxIdx,
      yAxis: maxPrice,
      value: maxPrice.toFixed(2),
      label: { formatter: '最高\n{c}' },
      itemStyle: { color: '#ffd700' },
    })
  }
  if (minIdx >= 0) {
    markPointData.push({
      name: '最低价',
      xAxis: minIdx,
      yAxis: minPrice,
      value: minPrice.toFixed(2),
      label: { formatter: '最低\n{c}' },
      itemStyle: { color: '#00bfff' },
    })
  }
  if (buyIdx >= 0) {
    markPointData.push({
      name: '买入',
      xAxis: buyIdx,
      yAxis: trade.buy_price,
      value: trade.buy_price.toFixed(2),
      label: { formatter: '买入\n{c}' },
      itemStyle: { color: '#ff4444' },
    })
  }
  if (sellIdx >= 0 && trade.sell_price) {
    markPointData.push({
      name: '卖出',
      xAxis: sellIdx,
      yAxis: trade.sell_price,
      value: trade.sell_price.toFixed(2),
      label: { formatter: '卖出\n{c}' },
      itemStyle: { color: '#00aa00' },
    })
  }

  // 画买入/卖出价格水平线
  const markLineData = []
  if (buyIdx >= 0) {
    markLineData.push({
      name: '买入价',
      yAxis: trade.buy_price,
      lineStyle: { color: '#ff4444', type: 'dashed' },
      label: { formatter: '买入 {c}' },
    })
  }
  if (sellIdx >= 0 && trade.sell_price) {
    markLineData.push({
      name: '卖出价',
      yAxis: trade.sell_price,
      lineStyle: { color: '#00aa00', type: 'dashed' },
      label: { formatter: '卖出 {c}' },
    })
  }

  const pnlText = trade.pnl_pct != null ? `${trade.pnl_pct >= 0 ? '+' : ''}${trade.pnl_pct.toFixed(2)}%` : '-'
  const buyStr = trade.buy_price?.toFixed(2) || '-'
  const sellStr = trade.sell_price?.toFixed(2) || '-'

  klineChart.setOption({
    title: { text: `${klineStockInfo.value.name} (${klineStockInfo.value.code})  盈亏: ${pnlText}`, left: 'center', textStyle: { fontSize: 14 } },
    tooltip: {
      trigger: 'axis',
      formatter: (params) => {
        const p = params[0]
        if (!p) return ''
        const v = p.value
        // 查找对应日期的成交量
        const idx = dates.indexOf(p.name)
        const vol = idx >= 0 && klineData[idx] ? klineData[idx].volume : '-'
        return `${p.name}<br/>开: ${v[1]}  收: ${v[2]}  低: ${v[3]}  高: ${v[4]}<br/>量: ${vol}`
      },
    },
    xAxis: { type: 'category', data: dates },
    yAxis: { type: 'value', scale: true },
    series: [{
      name: 'K线',
      type: 'candlestick',
      data: values,
      itemStyle: { color: '#ef232a', color0: '#14b143', borderColor: '#ef232a', borderColor0: '#14b143' },
      markPoint: { symbolSize: 50, data: markPointData, label: { show: true, fontSize: 11, position: 'top' } },
      markLine: {
        data: markLineData,
        symbol: 'none',
        lineStyle: { width: 1, type: 'dashed' },
        label: { show: true, fontSize: 10 },
      },
    }],
    dataZoom: [{ type: 'inside' }, { type: 'slider' }],
  })
  window.addEventListener('resize', () => klineChart.resize())
}

const destroyKlineChart = () => {
  if (klineChart) { klineChart.dispose(); klineChart = null }
}

// 删除回测任务
const deleteRun = async (runId) => {
  try {
    await ElMessageBox.confirm('确定要删除此回测任务及其所有数据吗？', '确认删除', {
      confirmButtonText: '删除',
      cancelButtonText: '取消',
      type: 'warning',
    })

    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/backtest/runs/${runId}`, {
      method: 'DELETE',
    })
    const data = await res.json()

    if (data.success) {
      ElMessage.success('删除成功')
      loadHistory()
    } else {
      ElMessage.error('删除失败')
    }
  } catch (e) {
    // 用户取消
  }
}

import { formatMoney, formatPct } from '../utils/format'

// 获取候选组名称列表
const getGroupNames = (groupIds, groupNamesMap) => {
  if (!groupIds || !groupNamesMap) return groupIds?.join(', ') || ''
  return groupIds.map(id => groupNamesMap[String(id)] || `候选组 ${id}`).join(', ')
}

// 选中变化（仅当前页）
const handleSelectionChange = (selection) => {
  selectedRuns.value = selection
}

// 翻页
const handlePageChange = () => {
  selectedRuns.value = []
  loadHistory()
}

const handlePageSizeChange = () => {
  currentPage.value = 1
  selectedRuns.value = []
  loadHistory()
}

// 双击列表行，回填参数到配置栏
const handleRowDblclick = async (row) => {
  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/backtest/runs/${row.id}`)
    const data = await res.json()
    if (!data.success || !data.run) {
      ElMessage.error(data.message || '加载回测详情失败')
      return
    }
    const run = data.run

    form.value.name = run.name || ''
    form.value.mode = run.mode || 'simulated'
    form.value.strategy_id = run.strategy_id || null
    form.value.trading_strategy_ids = run.trading_strategy_ids || []
    form.value.start_date = run.start_date || '2024-01-01'
    form.value.end_date = run.end_date || '2025-12-31'
    form.value.initial_capital = run.initial_capital || 1000000
    form.value.markets = run.markets || []
    form.value.group_ids = run.group_ids || []
    form.value.stop_loss_pct = run.stop_loss_pct ? Math.round(run.stop_loss_pct * 1000) / 10 : 5
    form.value.take_profit_pct = run.take_profit_pct ? Math.round(run.take_profit_pct * 1000) / 10 : 15
    form.value.trailing_stop_pct = run.trailing_stop_pct ? Math.round(run.trailing_stop_pct * 1000) / 10 : null
    form.value.stop_execution_price = run.stop_execution_price || 'close'
    form.value.commission_rate = run.commission_rate || 0.0001
    form.value.slippage_pct = run.slippage_pct ? run.slippage_pct * 100 : 0
    form.value.description = run.description || ''

    // 动态股票池
    if (run.pool_schedule && run.pool_schedule.length > 0) {
      form.value.use_dynamic_pool = true
      poolSchedule.value = run.pool_schedule.map(s => ({
        start_date: s.start_date || '',
        group_ids: s.group_ids || [],
        stock_pool: s.stock_pool || [],
      }))
      // 清除普通候选组选择，避免混淆
      form.value.group_ids = []
    } else {
      form.value.use_dynamic_pool = false
      poolSchedule.value = []
    }

    ElMessage.success('已加载回测参数到配置栏')
  } catch (e) {
    ElMessage.error('加载回测参数失败: ' + e.message)
  }
}

// 对比回测
const handleCompare = async () => {
  compareRuns.value = [...selectedRuns.value]
  compareVisible.value = true
  compareNavData.value = {}

  // 并发加载各回测净值
  const promises = compareRuns.value.map(async (run) => {
    try {
      const res = await fetch(`/api/v1/ui/${currentAccountId.value}/backtest/runs/${run.id}/nav`)
      const data = await res.json()
      compareNavData.value[run.id] = data.nav || []
    } catch (e) {
      console.error('加载净值失败:', e)
      compareNavData.value[run.id] = []
    }
  })
  await Promise.all(promises)

  // 构建指标对比表
  buildCompareMetrics()

  // 渲染对比图
  await nextTick()
  renderCompareChart()
}

const buildCompareMetrics = () => {
  const fields = [
    { key: 'total_return', label: '总收益率', fmt: (v) => (v != null ? v.toFixed(2) + '%' : '-'), lower: false },
    { key: 'annualized_return', label: '年化收益率', fmt: (v) => (v != null ? v.toFixed(2) + '%' : '-'), lower: false },
    { key: 'max_drawdown', label: '最大回撤', fmt: (v) => (v != null ? v.toFixed(2) + '%' : '-'), lower: true },
    { key: 'sharpe_ratio', label: '夏普比率', fmt: (v) => (v != null ? v.toFixed(2) : '-'), lower: false },
    { key: 'calmar_ratio', label: '卡玛比率', fmt: (v) => (v != null ? v.toFixed(2) : '-'), lower: false },
    { key: 'win_rate', label: '胜率', fmt: (v) => (v != null ? v.toFixed(2) + '%' : '-'), lower: false },
    { key: 'profit_factor', label: '盈亏比', fmt: (v) => (v != null ? v.toFixed(2) : '-'), lower: false },
    { key: 'total_trades', label: '交易次数', fmt: (v) => (v != null ? v : '-'), lower: false },
    { key: 'avg_holding_days', label: '平均持仓', fmt: (v) => (v != null ? v.toFixed(1) + '天' : '-'), lower: false },
    { key: 'best_trade', label: '最佳交易', fmt: (v) => (v != null ? v.toFixed(2) + '%' : '-'), lower: false },
    { key: 'worst_trade', label: '最差交易', fmt: (v) => (v != null ? v.toFixed(2) + '%' : '-'), lower: false },
    { key: 'final_nav', label: '最终净值', fmt: (v) => (v != null ? v.toFixed(4) : '-'), lower: false },
  ]

  const runs = compareRuns.value
  compareMetrics.value = fields.map((f) => {
    const row = { label: f.label }
    const values = []
    runs.forEach((run) => {
      const r = run.result_summary || {}
      const v = r[f.key]
      row[`run_${run.id}`] = f.fmt(v)
      values.push({ id: run.id, v })
    })
    // 标记最优值
    const validValues = values.filter((x) => x.v != null && typeof x.v === 'number')
    if (validValues.length > 0) {
      const best = f.lower
        ? Math.min(...validValues.map((x) => x.v))
        : Math.max(...validValues.map((x) => x.v))
      validValues.forEach((x) => {
        if (x.v === best) row[`best_${x.id}`] = true
      })
    }
    return row
  })
}

const renderCompareChart = () => {
  if (!compareChartRef.value) return

  if (compareChart) {
    compareChart.dispose()
  }
  compareChart = echarts.init(compareChartRef.value)

  const colors = ['#409EFF', '#67C23A', '#E6A23C', '#F56C6C', '#909399', '#722ED1', '#13C2C2', '#EB2F96']
  const series = compareRuns.value.map((run, idx) => {
    const nav = compareNavData.value[run.id] || []
    return {
      name: run.name,
      type: 'line',
      data: nav.map((d) => d.nav),
      smooth: true,
      lineStyle: { width: 2 },
      itemStyle: { color: colors[idx % colors.length] },
      showSymbol: false,
    }
  })

  const dates = compareRuns.value.length > 0
    ? (compareNavData.value[compareRuns.value[0].id] || []).map((d) => d.trade_date)
    : []

  compareChart.setOption({
    tooltip: { trigger: 'axis' },
    legend: { data: compareRuns.value.map((r) => r.name), top: 0 },
    grid: { left: '3%', right: '4%', bottom: '3%', containLabel: true },
    xAxis: { type: 'category', data: dates, axisLabel: { rotate: 45 } },
    yAxis: { type: 'value', name: '净值' },
    series,
    dataZoom: [{ type: 'inside' }, { type: 'slider' }],
  })
}

const destroyCompareChart = () => {
  if (compareChart) {
    compareChart.dispose()
    compareChart = null
  }
}

let statusTimer = null

onMounted(() => {
  loadStrategies()
  loadCandidateGroups()
  loadHistory()
  // 自动轮询运行中的回测任务状态（静默刷新）
  statusTimer = setInterval(async () => {
    const hasRunning = history.value.some(r => r.status === 'running' || r.status === 'pending')
    if (hasRunning) {
      await loadHistory(true)
    }
  }, 5000)
})

onBeforeUnmount(() => {
  if (statusTimer) clearInterval(statusTimer)
  if (klineChart) { klineChart.dispose(); klineChart = null }
})
</script>

<style scoped>
.backtest-container {
  min-height: 100vh;
  background-color: #f5f7fa;
}

.backtest-content {
  padding: 20px;
  max-width: 1400px;
  margin: 0 auto;
}

.backtest-content h2 {
  margin-bottom: 20px;
  color: #303133;
}

.config-card {
  margin-bottom: 20px;
}

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.metric-card {
  text-align: center;
  padding: 8px;
}

.metric-label {
  font-size: 12px;
  color: #909399;
  margin-bottom: 4px;
}

.metric-value {
  font-size: 18px;
  font-weight: bold;
  color: #303133;
}

.text-green {
  color: #67c23a;
}

.text-red {
  color: #f56c6c;
}

.text-muted {
  color: #909399;
  font-size: 12px;
}

.backtest-date {
  font-size: 11px;
  color: #e6a23c;
  margin-top: 2px;
}

.action-buttons {
  display: flex;
  gap: 4px;
  flex-wrap: nowrap;
}

:deep(.el-table) {
  font-size: 13px;
}
</style>
