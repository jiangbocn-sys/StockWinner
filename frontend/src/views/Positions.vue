<template>
  <div class="layout-container">
    <NavBar />
    <el-main class="main-content">
      <h2>持仓分析 - {{ currentAccount?.display_name }}</h2>

      <!-- 总体概览 -->
      <el-card class="overview-card">
        <el-descriptions :column="4" border>
          <el-descriptions-item label="总资产">¥{{ formatNumber(totalAssets) }}</el-descriptions-item>
          <el-descriptions-item label="可用资金">¥{{ formatNumber(availableCash) }}</el-descriptions-item>
          <el-descriptions-item label="持仓市值">¥{{ formatNumber(marketValue) }}</el-descriptions-item>
          <el-descriptions-item label="总盈亏">
            <span :class="totalPnl >= 0 ? 'profit-positive' : 'profit-negative'">
              {{ totalPnl >= 0 ? '+' : '' }}¥{{ formatNumber(Math.abs(totalPnl)) }} ({{ pnlPercent }}%)
            </span>
          </el-descriptions-item>
        </el-descriptions>
      </el-card>

      <!-- 按策略分组统计 -->
      <el-card v-if="filteredStrategyStats.length > 0" class="strategy-stats-card">
        <template #header>
          <div class="card-header">
            <span>策略持仓统计</span>
            <span v-if="selectedStrategyId != null" class="filter-hint">（已筛选）</span>
          </div>
        </template>
        <el-table :data="filteredStrategyStats" stripe size="small" @row-click="onStrategyRowClick" :row-class-name="strategyRowClass">
          <el-table-column prop="strategy_name" label="策略名称" min-width="120" />
          <el-table-column label="买入" width="70" align="center">
            <template #default="{ row }">{{ row.buy_count }}笔</template>
          </el-table-column>
          <el-table-column label="买入金额" width="110" align="right">
            <template #default="{ row }">¥{{ formatNumber(row.buy_amount) }}</template>
          </el-table-column>
          <el-table-column label="卖出" width="70" align="center">
            <template #default="{ row }">{{ row.sell_count }}笔</template>
          </el-table-column>
          <el-table-column label="卖出金额" width="110" align="right">
            <template #default="{ row }">¥{{ formatNumber(row.sell_amount) }}</template>
          </el-table-column>
          <el-table-column prop="total_pnl" label="盈亏" width="110" align="right">
            <template #default="{ row }">
              <span :class="row.total_pnl >= 0 ? 'profit-positive' : 'profit-negative'">
                {{ row.total_pnl >= 0 ? '+' : '' }}¥{{ formatNumber(Math.abs(row.total_pnl)) }}
              </span>
            </template>
          </el-table-column>
          <el-table-column label="收益率" width="90" align="right">
            <template #default="{ row }">
              <span :class="row.profit_rate >= 0 ? 'profit-positive' : 'profit-negative'">
                {{ row.profit_rate >= 0 ? '+' : '' }}{{ row.profit_rate }}%
              </span>
            </template>
          </el-table-column>
          <el-table-column label="年化收益" width="90" align="right">
            <template #default="{ row }">
              <span :class="row.annualized_rate >= 0 ? 'profit-positive' : 'profit-negative'">
                {{ row.annualized_rate >= 0 ? '+' : '' }}{{ row.annualized_rate }}%
              </span>
            </template>
          </el-table-column>
          <el-table-column prop="position_count" label="持仓数" width="70" align="center" />
          <el-table-column prop="total_mv" label="持仓市值" width="110" align="right">
            <template #default="{ row }">¥{{ formatNumber(row.total_mv) }}</template>
          </el-table-column>
          <el-table-column prop="strategy_cash" label="可用现金" width="100" align="right">
            <template #default="{ row }">¥{{ formatNumber(row.strategy_cash || 0) }}</template>
          </el-table-column>
        </el-table>
      </el-card>

      <!-- 持仓明细 -->
      <el-card>
        <template #header>
          <div class="card-header">
            <el-tabs v-model="activeTab" class="detail-tabs">
              <el-tab-pane label="当前持仓" name="holding" />
              <el-tab-pane :label="`已清仓 (${closedCount})`" name="closed" />
            </el-tabs>
            <el-button v-if="activeTab === 'holding'" type="primary" size="small" @click="refreshPrices" :loading="refreshing">
              <el-icon><Refresh /></el-icon>
              刷新行情
            </el-button>
            <el-dropdown @command="handleExportPositions">
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
          </div>
        </template>

        <!-- 当前持仓表格 -->
        <el-table v-show="activeTab === 'holding'" :data="paginatedPositions" stripe style="width: 100%" @row-dblclick="showKline" @sort-change="onPosSortChange">
          <el-table-column type="index" label="序号" width="60" align="center" :index="indexMethod" />
          <el-table-column prop="stock_code" label="股票代码" width="100" sortable="custom" />
          <el-table-column prop="stock_name" label="股票名称" width="120" sortable="custom" />
          <el-table-column prop="quantity" label="数量" width="100" align="right" sortable="custom" />
          <el-table-column prop="avg_cost" label="成本价" width="100" align="right" sortable="custom">
            <template #default="{ row }">¥{{ Number(row.avg_cost || 0).toFixed(2) }}</template>
          </el-table-column>
          <el-table-column prop="current_price" label="当前价" width="100" align="right" sortable="custom">
            <template #default="{ row }">
              <span :style="{
                color: row.current_price > row.avg_cost ? '#f56c6c' : row.current_price > 0 && row.current_price < row.avg_cost ? '#67c23a' : '',
                backgroundColor: priceChangeHighlight.get(row.stock_code) === 'up' ? '#ffe4e4' : priceChangeHighlight.get(row.stock_code) === 'down' ? '#e4ffe4' : ''
              }">
                {{ row.current_price > 0 ? '¥' + row.current_price.toFixed(2) : '-' }}
              </span>
            </template>
          </el-table-column>
          <el-table-column prop="change_pct" label="涨跌" width="85" align="right" sortable="custom">
            <template #default="{ row }">
              <span :style="{ color: row.change_pct > 0 ? '#f56c6c' : row.change_pct < 0 ? '#67c23a' : '' }">
                {{ row.change_pct ? (row.change_pct > 0 ? '+' : '') + row.change_pct.toFixed(2) + '%' : '-' }}
              </span>
            </template>
          </el-table-column>
          <el-table-column prop="market_value" label="市值" width="120" align="right" sortable="custom">
            <template #default="{ row }">¥{{ formatNumber(row.market_value) }}</template>
          </el-table-column>
          <el-table-column prop="profit_loss" label="盈亏" width="120" align="right" sortable="custom">
            <template #default="{ row }">
              <span :class="row.profit_loss >= 0 ? 'profit-positive' : 'profit-negative'">
                {{ row.profit_loss >= 0 ? '+' : '' }}¥{{ formatNumber(Math.abs(row.profit_loss)) }}
              </span>
            </template>
          </el-table-column>
          <el-table-column prop="profit_percent" label="盈亏%" width="100" align="right">
            <template #default="{ row }">
              <span :class="row.profit_loss >= 0 ? 'profit-positive' : 'profit-negative'">
                {{ ((row.profit_loss / (row.avg_cost * row.quantity)) * 100).toFixed(2) }}%
              </span>
            </template>
          </el-table-column>
          <el-table-column prop="first_buy_date" label="建仓日期" width="110" sortable="custom">
            <template #default="{ row }">{{ row.first_buy_date ? row.first_buy_date.substring(0, 10) : '-' }}</template>
          </el-table-column>
          <el-table-column prop="stop_loss_price" label="止损价" width="100" align="right">
            <template #default="{ row }">{{ row.stop_loss_price ? '¥' + Number(row.stop_loss_price).toFixed(2) : '-' }}</template>
          </el-table-column>
          <el-table-column prop="take_profit_price" label="止盈价" width="100" align="right">
            <template #default="{ row }">{{ row.take_profit_price ? '¥' + Number(row.take_profit_price).toFixed(2) : '-' }}</template>
          </el-table-column>
          <el-table-column label="操作" fixed="right" width="360">
            <template #default="{ row }">
              <el-button type="success" size="small" plain @click="openStrategyDialog(row)">止损止盈</el-button>
              <el-button type="info" size="small" @click="handleDsaAnalysis(row)" :loading="dsaAnalyzing === row.stock_code">DSA</el-button>
              <el-button type="primary" size="small" @click="handleAction(row, 'add')">加仓</el-button>
              <el-button type="warning" size="small" @click="handleAction(row, 'reduce')">减仓</el-button>
              <el-button type="danger" size="small" @click="handleAction(row, 'clear')">清仓</el-button>
            </template>
          </el-table-column>
        </el-table>

        <div class="pagination-bar" v-if="activeTab === 'holding' && positions.length > posPageSize">
          <el-pagination
            v-model:current-page="posCurrentPage"
            v-model:page-size="posPageSize"
            :total="positions.length"
            :page-sizes="[10, 20, 50, 100]"
            layout="sizes, prev, pager, next, total"
            small
          />
        </div>

        <!-- 已清仓明细表格 -->
        <el-table v-show="activeTab === 'closed'" :data="paginatedClosed" stripe style="width: 100%">
          <el-table-column type="index" label="序号" width="60" align="center" :index="closedIndexMethod" />
          <el-table-column prop="stock_code" label="股票代码" width="100" />
          <el-table-column prop="stock_name" label="股票名称" width="120" />
          <el-table-column label="策略" width="100">
            <template #default="{ row }">{{ getStrategyName(row.strategy_id) }}</template>
          </el-table-column>
          <el-table-column prop="buy_quantity" label="数量" width="80" align="right" />
          <el-table-column label="买入价" width="100" align="right">
            <template #default="{ row }">¥{{ row.avg_buy_price }}</template>
          </el-table-column>
          <el-table-column label="卖出价" width="100" align="right">
            <template #default="{ row }">¥{{ row.avg_sell_price }}</template>
          </el-table-column>
          <el-table-column label="买入时间" width="110">
            <template #default="{ row }">{{ row.first_buy_time }}</template>
          </el-table-column>
          <el-table-column label="卖出时间" width="110">
            <template #default="{ row }">{{ row.last_sell_time }}</template>
          </el-table-column>
          <el-table-column prop="holding_days" label="持有天数" width="80" align="right" />
          <el-table-column label="交易成本" width="110" align="right">
            <template #default="{ row }">¥{{ formatNumber(row.total_commission) }}</template>
          </el-table-column>
          <el-table-column label="清仓收益" width="120" align="right">
            <template #default="{ row }">
              <span :class="row.net_profit >= 0 ? 'profit-positive' : 'profit-negative'">
                {{ row.net_profit >= 0 ? '+' : '' }}¥{{ formatNumber(Math.abs(row.net_profit)) }}
              </span>
            </template>
          </el-table-column>
          <el-table-column label="收益率" width="100" align="right">
            <template #default="{ row }">
              <span :class="row.profit_pct >= 0 ? 'profit-positive' : 'profit-negative'">
                {{ row.profit_pct >= 0 ? '+' : '' }}{{ row.profit_pct }}%
              </span>
            </template>
          </el-table-column>
          <el-table-column label="年化收益" width="100" align="right">
            <template #default="{ row }">
              <span :class="row.annualized_pct >= 0 ? 'profit-positive' : 'profit-negative'">
                {{ row.annualized_pct >= 0 ? '+' : '' }}{{ formatPct(row.annualized_pct) }}%
              </span>
            </template>
          </el-table-column>
        </el-table>

        <div class="pagination-bar" v-if="activeTab === 'closed' && filteredClosedPositions.length > closedPageSize">
          <el-pagination
            v-model:current-page="closedCurrentPage"
            v-model:page-size="closedPageSize"
            :total="filteredClosedPositions.length"
            :page-sizes="[10, 20, 50, 100]"
            layout="sizes, prev, pager, next, total"
            small
          />
        </div>
      </el-card>

      <!-- DSA 分析结果弹窗 -->
      <el-dialog
        v-model="dsaDialogVisible"
        :title="`DSA 分析 - ${dsaStock.stock_name}(${dsaStock.stock_code})`"
        width="700px"
        :close-on-click-modal="false"
        :close-on-press-escape="false"
        :show-close="false"
      >
        <div v-if="dsaAnalyzing" class="dsa-loading">
          <el-icon class="is-loading" size="40"><Loading /></el-icon>
          <p>正在分析中，请稍候...</p>
        </div>
        <div v-else-if="dsaResult" class="dsa-content">
          <el-descriptions :column="2" border size="small" class="mb-16">
            <el-descriptions-item label="当前价">¥{{ dsaResult.meta?.current_price || '-' }}</el-descriptions-item>
            <el-descriptions-item label="涨跌幅">{{ dsaResult.meta?.change_pct?.toFixed(2) || '-' }}%</el-descriptions-item>
          </el-descriptions>

          <h4 class="section-title">市场情绪</h4>
          <el-tag :type="dsaResult.summary?.sentiment_label === '看多' ? 'danger' : dsaResult.summary?.sentiment_label === '看空' ? 'success' : 'info'" size="large">
            {{ dsaResult.summary?.sentiment_label || '-' }}
          </el-tag>
          <el-tag v-if="dsaResult.summary?.sentiment_score" size="small" style="margin-left: 8px">
            评分: {{ dsaResult.summary.sentiment_score }}
          </el-tag>

          <h4 class="section-title">分析摘要</h4>
          <div class="analysis-text">{{ dsaResult.summary?.analysis_summary || '暂无分析' }}</div>

          <h4 class="section-title">操作建议</h4>
          <div class="analysis-text">{{ dsaResult.summary?.operation_advice || '暂无建议' }}</div>

          <h4 class="section-title">交易参考</h4>
          <el-descriptions :column="3" border size="small">
            <el-descriptions-item label="理想买入">¥{{ dsaResult.strategy?.ideal_buy || '-' }}</el-descriptions-item>
            <el-descriptions-item label="止损价">¥{{ dsaResult.strategy?.stop_loss || '-' }}</el-descriptions-item>
            <el-descriptions-item label="止盈价">¥{{ dsaResult.strategy?.take_profit || '-' }}</el-descriptions-item>
          </el-descriptions>
        </div>
        <div v-else-if="dsaError" class="dsa-error">
          <el-alert :title="dsaError" type="error" :closable="false" />
        </div>
        <template #footer>
          <el-button type="primary" @click="dsaDialogVisible = false">关闭</el-button>
        </template>
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
          :indicators="klineIndicators"
          :indicatorConfig="klineIndicatorConfig" />
      </el-dialog>

      <!-- 交易对话框（加仓/减仓/清仓） -->
      <el-dialog
        v-model="tradeDialogVisible"
        :title="tradeDialogTitle"
        width="480px"
        :close-on-click-modal="false"
      >
        <el-form :model="tradeForm" label-width="90px" label-position="right">
          <el-form-item label="股票代码">
            <el-input :value="tradeForm.stock_code" disabled />
          </el-form-item>
          <el-form-item label="股票名称">
            <el-input :value="tradeForm.stock_name" disabled />
          </el-form-item>
          <el-form-item label="当前价">
            <span :class="tradeQuote.current_price ? (tradeQuote.current_price >= tradeQuote.prev_close ? 'profit-positive' : 'profit-negative') : ''">
              ¥{{ tradeQuote.current_price?.toFixed(2) || '-' }}
            </span>
            <span v-if="tradeQuote.change_percent != null" :class="tradeQuote.change_percent >= 0 ? 'profit-positive' : 'profit-negative'" style="margin-left: 8px">
              {{ tradeQuote.change_percent >= 0 ? '+' : '' }}{{ tradeQuote.change_percent.toFixed(2) }}%
            </span>
          </el-form-item>

          <!-- 五档盘口展示 -->
          <el-form-item label="五档盘口" v-if="tradeAskLevels.length > 0 || tradeBidLevels.length > 0">
            <div class="level5-container">
              <div class="level5-header">
                <span class="col-ask">卖盘</span>
                <span class="col-price">卖价</span>
                <span class="col-price">买价</span>
                <span class="col-bid">买盘</span>
              </div>
              <div v-for="i in 5" :key="i" class="level5-row">
                <span class="col-ask level-volume">{{ formatVolume(tradeAskVolumes[5 - i]) }}</span>
                <span class="col-price level-price ask-price">{{ tradeAskLevels[5 - i]?.toFixed(2) || '-' }}</span>
                <span class="col-price level-price bid-price">{{ tradeBidLevels[i - 1]?.toFixed(2) || '-' }}</span>
                <span class="col-bid level-volume">{{ formatVolume(tradeBidVolumes[i - 1]) }}</span>
              </div>
            </div>
          </el-form-item>

          <el-form-item label="委托价格">
            <el-input-number
              v-model="tradeForm.price"
              :precision="2"
              :step="0.01"
              :min="0.01"
              controls-position="right"
              style="width: 100%"
            />
            <div style="display: flex; gap: 4px; margin-top: 4px">
              <el-button size="small" @click="usePrice('bid1')" v-if="tradeForm.trade_type === 'sell' && tradeQuote.bid1">买一 ¥{{ tradeQuote.bid1?.toFixed(2) }}</el-button>
              <el-button size="small" @click="usePrice('ask1')" v-if="tradeForm.trade_type === 'buy' && tradeQuote.ask1">卖一 ¥{{ tradeQuote.ask1?.toFixed(2) }}</el-button>
              <el-button size="small" @click="usePrice('current')" v-if="tradeQuote.current_price">现价 ¥{{ tradeQuote.current_price?.toFixed(2) }}</el-button>
            </div>
          </el-form-item>

          <el-form-item label="委托数量">
            <el-input-number
              v-model="tradeForm.quantity"
              :step="100"
              :min="100"
              :max="tradeForm.trade_type === 'sell' ? tradeForm.maxQuantity : (fundLimitQty > 0 ? fundLimitQty : 999999)"
              controls-position="right"
              style="width: 100%"
            />
            <div style="display: flex; gap: 4px; margin-top: 4px">
              <el-button size="small" @click="useQuantity('half')">1/2</el-button>
              <el-button size="small" @click="useQuantity('all')" :disabled="tradeForm.maxQuantity === 0">{{ tradeForm.trade_type === 'sell' ? '全部' : '最大' }}</el-button>
            </div>
            <div v-if="tradeForm.trade_type === 'sell'" class="position-hint">
              持仓 {{ positionQtyDisplay }} 股，可卖 {{ tradeForm.maxQuantity }} 股
            </div>
          </el-form-item>
        </el-form>

        <template #footer>
          <el-button @click="tradeDialogVisible = false">取消</el-button>
          <el-button
            :type="tradeForm.trade_type === 'buy' ? 'danger' : 'success'"
            :loading="tradeSubmitting"
            :disabled="!canTradeSubmit"
            @click="submitTrade"
          >
            {{ tradeSubmitText }}
          </el-button>
        </template>
      </el-dialog>

      <!-- 止损止盈设置对话框 -->
      <el-dialog v-model="strategyDialogVisible" title="设置止损止盈" width="450px">
        <el-form :model="strategyForm" label-width="100px">
          <el-form-item label="股票代码">
            <el-input :model-value="strategyForm.stock_code" disabled />
          </el-form-item>
          <el-form-item label="股票名称">
            <el-input :model-value="strategyForm.stock_name" disabled />
          </el-form-item>
          <el-form-item label="止损价">
            <el-input-number v-model="strategyForm.stop_loss_price" :min="0" :precision="2" controls-position="right" style="width:100%" />
          </el-form-item>
          <el-form-item label="止损比例">
            <el-input-number v-model="strategyForm.stop_loss_pct" :min="0" :max="0.5" :step="0.01" :precision="2" controls-position="right" style="width:100%" />
            <div style="font-size:12px;color:#909399">止损价优先，为空时用比例</div>
          </el-form-item>
          <el-form-item label="止盈价">
            <el-input-number v-model="strategyForm.take_profit_price" :min="0" :precision="2" controls-position="right" style="width:100%" />
          </el-form-item>
          <el-form-item label="止盈比例">
            <el-input-number v-model="strategyForm.take_profit_pct" :min="0" :max="1.0" :step="0.01" :precision="2" controls-position="right" style="width:100%" />
          </el-form-item>
        </el-form>
        <template #footer>
          <el-button @click="strategyDialogVisible = false">取消</el-button>
          <el-button type="primary" :loading="strategySaving" @click="saveStrategy">保存</el-button>
        </template>
      </el-dialog>
    </el-main>
  </div>
</template>

<script setup>
import { ref, onMounted, onUnmounted, computed, nextTick } from 'vue'
import { storeToRefs } from 'pinia'
import { ElMessage, ElMessageBox } from 'element-plus'
import { ArrowLeft, ArrowRight, Refresh, Download, Setting, ArrowDown } from '@element-plus/icons-vue'
import { exportTable as doExport } from '@/utils/exportHelper'
import { useAccountStore } from '../stores/account'
import { usePositionsStore } from '../stores/positions'
import { switchStockPreservingDrillDown } from '../utils/drillDownHelper'
import NavBar from '../components/NavBar.vue'
import KlineChart from '../components/KlineChart.vue'

const accountStore = useAccountStore()
const posStore = usePositionsStore()
const currentAccount = computed(() => accountStore.currentAccount)
const currentAccountId = computed(() => accountStore.currentAccountId)

// 数据从 store 读取（storeToRefs 保持响应性）
const { positions, availableCash, closedPositions, closedCount, strategyStats } = storeToRefs(posStore)
const marketValue = computed(() => posStore.marketValue)
const totalPnl = computed(() => posStore.totalPnl)
const totalAssets = computed(() => posStore.totalAssets)
const pnlPercent = computed(() => posStore.pnlPercent)

// 过滤策略持仓统计：只显示有持仓或有可用现金的策略
const filteredStrategyStats = computed(() => {
  return strategyStats.value.filter(s => (s.position_count > 0) || (s.strategy_cash > 0))
})

// 策略ID -> 名称映射
const getStrategyName = (strategyId) => {
  if (strategyId == null) return '手动买入'
  const s = strategyStats.value.find(s => s.strategy_id === strategyId)
  return s?.strategy_name || `策略#${strategyId}`
}

// 策略筛选状态
const selectedStrategyId = ref(null)

// 点击策略行：切换筛选
const onStrategyRowClick = (row) => {
  if (selectedStrategyId.value === row.strategy_id) {
    // 再次点击同一策略 → 取消筛选
    selectedStrategyId.value = null
  } else {
    // 点击其他策略 → 筛选该策略
    selectedStrategyId.value = row.strategy_id
  }
  posCurrentPage.value = 1 // 重置持仓分页
  closedCurrentPage.value = 1 // 重置已清仓分页
}

// 策略表格行样式
const strategyRowClass = ({ row }) => {
  if (row.strategy_id === selectedStrategyId.value) {
    return 'strategy-row-selected'
  }
  return ''
}

// 排序
const posSortProp = ref('profit_loss')
const posSortOrder = ref('descending')
const onPosSortChange = ({ prop, order }) => {
  posSortProp.value = prop || 'profit_loss'
  posSortOrder.value = order || 'descending'
  posCurrentPage.value = 1
}

// 分页（先筛选再全量排序再分页）
const sortedPositions = computed(() => {
  // 先按策略筛选
  let arr = positions.value
  if (selectedStrategyId.value !== null) {
    if (selectedStrategyId.value === 0) {
      // 手动买入：筛选 strategy_id 为 null/undefined/0 的数据
      arr = arr.filter(p => !p.strategy_id || p.strategy_id === 0)
    } else {
      arr = arr.filter(p => p.strategy_id === selectedStrategyId.value)
    }
  }
  // 再排序
  arr = [...arr]
  const prop = posSortProp.value
  const desc = posSortOrder.value === 'descending'
  arr.sort((a, b) => {
    const av = a[prop]; const bv = b[prop]
    if (av == null && bv == null) return 0
    if (av == null) return desc ? 1 : -1
    if (bv == null) return desc ? -1 : 1
    const cmp = typeof av === 'number' ? av - bv : String(av).localeCompare(String(bv))
    return desc ? -cmp : cmp
  })
  return arr
})
const posCurrentPage = ref(1)
const posPageSize = ref(20)
const paginatedPositions = computed(() => {
  const start = (posCurrentPage.value - 1) * posPageSize.value
  return sortedPositions.value.slice(start, start + posPageSize.value)
})
const indexMethod = (index) => (posCurrentPage.value - 1) * posPageSize.value + index + 1

// DSA 分析状态
const dsaDialogVisible = ref(false)
const dsaAnalyzing = ref(false)
const dsaStock = ref({ stock_code: '', stock_name: '' })
const dsaResult = ref(null)
const dsaError = ref('')

const refreshing = ref(false)
// 价格变化高亮（使用 composable）
const { highlight: priceChangeHighlight, prepareForRefresh, compareAndHighlight } = usePriceChangeHighlight(positions)

// 策略持仓统计
const loadStrategyStats = async () => {
  try {
    await posStore.loadStrategyStats(currentAccountId.value)
  } catch (e) {
    console.error('加载策略统计失败:', e)
  }
}

// 已清仓明细（含策略筛选）
const activeTab = ref('holding')
const closedCurrentPage = ref(1)
const closedPageSize = ref(20)
const filteredClosedPositions = computed(() => {
  // 按策略筛选：手动买入(strategy_id=0)对应 null/undefined 的数据
  if (selectedStrategyId.value !== null) {
    if (selectedStrategyId.value === 0) {
      // 手动买入：筛选 strategy_id 为 null/undefined/0 的数据
      return closedPositions.value.filter(p => !p.strategy_id || p.strategy_id === 0)
    }
    return closedPositions.value.filter(p => p.strategy_id === selectedStrategyId.value)
  }
  return closedPositions.value
})
const paginatedClosed = computed(() => {
  const start = (closedCurrentPage.value - 1) * closedPageSize.value
  return filteredClosedPositions.value.slice(start, start + closedPageSize.value)
})
const closedIndexMethod = (index) => (closedCurrentPage.value - 1) * closedPageSize.value + index + 1

const loadClosedPositions = async () => {
  try {
    await posStore.loadClosedPositions(currentAccountId.value)
    closedCurrentPage.value = 1
  } catch (error) {
    console.error('加载已清仓明细失败:', error)
  }
}

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
  const idx = selectedIndicators.value.indexOf(key)
  if (idx >= 0) {
    selectedIndicators.value.splice(idx, 1)
  } else {
    selectedIndicators.value.push(key)
  }
  // 加载指标数据
  loadIndicatorData()
}

// 加载指标数据（从因子数据表）
const loadIndicatorData = async () => {
  console.log('[指标] loadIndicatorData 被调用, klineStockInfo:', klineStockInfo.value)
  if (!klineStockInfo.value.code || selectedIndicators.value.length === 0) {
    klineIndicators.value = {}
    console.log('[指标] 清空指标: code=', klineStockInfo.value.code, 'selected=', selectedIndicators.value.length)
    return
  }

  // 只在日线模式下支持指标叠加（因子数据是日频）
  if (klinePeriod.value !== 'day') {
    ElMessage.warning('技术指标仅支持日线模式')
    return
  }

  const code = klineStockInfo.value.code
  console.log('[指标] 当前股票代码:', code, '选中指标:', selectedIndicators.value)
  const dates = klineData.value.map(d => d.trade_date)
  if (dates.length === 0) return

  const startDate = dates[0]
  const endDate = dates[dates.length - 1]

  // 构建需要获取的指标字段列表
  const fields = []
  for (const key of selectedIndicators.value) {
    if (key === 'boll') {
      fields.push('boll_upper', 'boll_middle', 'boll_lower')
    } else {
      fields.push(key)
    }
  }

  try {
    console.log('[指标] 加载参数:', { accountId: currentAccountId.value, code, startDate, endDate, fields })
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/factors/${code}?start_date=${startDate}&end_date=${endDate}&fields=${fields.join(',')}`)
    const data = await res.json()
    console.log('[指标] API响应:', { success: data.success, count: data.count })

    if (data.success && data.factors) {
      // 将因子数据转换为指标格式
      const indicators = {}
      for (const field of fields) {
        indicators[field] = data.factors.map(f => ({
          trade_date: f.trade_date,
          value: f[field]
        }))
      }
      console.log('[指标] 转换结果:', Object.keys(indicators), '条数:', indicators[fields[0]]?.length)
      klineIndicators.value = indicators
    } else {
      console.warn('[指标] API失败:', data.message)
    }
  } catch (e) {
    console.error('[指标] 加载失败:', e)
  }
}

const hasPrevStock = computed(() => klineStockIndex.value > 0)
const hasNextStock = computed(() => klineStockIndex.value >= 0 && klineStockIndex.value < positions.value.length - 1)
const klineNavText = computed(() => {
  const total = positions.value.length
  const idx = klineStockIndex.value
  if (idx < 0 || total === 0) return ''
  return `${idx + 1} / ${total}`
})

// 导出功能
const holdingColumns = [
  { label: '股票代码', prop: 'stock_code' },
  { label: '股票名称', prop: 'stock_name' },
  { label: '数量', prop: 'quantity' },
  { label: '成本价', prop: 'avg_cost' },
  { label: '当前价', prop: 'current_price' },
  { label: '市值', prop: 'market_value' },
  { label: '盈亏', prop: 'profit_loss' },
  { label: '盈亏比例', prop: 'profit_percent' },
  { label: '止损价', prop: 'stop_loss_price' },
  { label: '止盈价', prop: 'take_profit_price' },
]
const closedColumns = [
  { label: '股票代码', prop: 'stock_code' },
  { label: '股票名称', prop: 'stock_name' },
  { label: '策略', prop: 'strategy_id' },
  { label: '买入数量', prop: 'buy_quantity' },
  { label: '买入均价', prop: 'avg_buy_price' },
  { label: '卖出均价', prop: 'avg_sell_price' },
  { label: '持有天数', prop: 'holding_days' },
  { label: '手续费', prop: 'commission' },
  { label: '净盈亏', prop: 'net_profit' },
  { label: '收益率', prop: 'profit_pct' },
  { label: '年化收益', prop: 'annualized_pct' },
]

const handleExportPositions = (format) => {
  if (activeTab.value === 'holding') {
    let data = positions.value
    if (selectedStrategyId.value !== null) {
      if (selectedStrategyId.value === 0) {
        data = positions.value.filter(p => !p.strategy_id || p.strategy_id === 0)
      } else {
        data = positions.value.filter(p => p.strategy_id === selectedStrategyId.value)
      }
    }
    doExport(holdingColumns, data, '当前持仓', format)
  } else {
    doExport(closedColumns, filteredClosedPositions.value, '已清仓', format)
  }
}

const loadPositions = async () => {
  try {
    await posStore.loadPositions(currentAccountId.value)
    posCurrentPage.value = 1
  } catch (error) {
    console.error('加载持仓失败:', error)
  }
}

const refreshPrices = async () => {
  const oldPrices = prepareForRefresh()
  refreshing.value = true
  try {
    await posStore.refreshPrices(currentAccountId.value)
    posCurrentPage.value = 1
    ElMessage.success('行情已刷新')
    compareAndHighlight(oldPrices)
  } catch (error) {
    console.error('刷新行情失败:', error)
    ElMessage.error('刷新行情失败')
  } finally {
    refreshing.value = false
  }
}

import { formatNumber, formatPct } from '../utils/format'

// 止损止盈设置
const strategyDialogVisible = ref(false)
const strategySaving = ref(false)
const strategyForm = ref({
  stock_code: '',
  stock_name: '',
  stop_loss_price: null,
  stop_loss_pct: 0.05,
  take_profit_price: null,
  take_profit_pct: 0.15,
})

const openStrategyDialog = async (row) => {
  // 先从 API 加载现有策略
  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/trading-strategies/stock/${row.stock_code}`)
    const data = await res.json()
    if (data.success && data.exists && data.strategy) {
      // 已有策略，加载现有数据
      strategyForm.value = {
        stock_code: row.stock_code,
        stock_name: data.strategy.stock_name || row.stock_name || row.stock_code,
        stop_loss_price: data.strategy.stop_loss_price || null,
        stop_loss_pct: data.strategy.stop_loss_pct || 0.05,
        take_profit_price: data.strategy.take_profit_price || null,
        take_profit_pct: data.strategy.take_profit_pct || 0.15,
        strategy_type: data.strategy.strategy_type || 'fixed',
        entry_price: data.strategy.entry_price || null,
        max_trade_quantity: data.strategy.max_trade_quantity || null,
      }
    } else {
      // 无策略，使用默认值
      strategyForm.value = {
        stock_code: row.stock_code,
        stock_name: row.stock_name || row.stock_code,
        stop_loss_price: row.stop_loss_price || null,
        stop_loss_pct: 0.05,
        take_profit_price: row.take_profit_price || null,
        take_profit_pct: 0.15,
      }
    }
  } catch (e) {
    // API 失败，使用默认值
    strategyForm.value = {
      stock_code: row.stock_code,
      stock_name: row.stock_name || row.stock_code,
      stop_loss_price: row.stop_loss_price || null,
      stop_loss_pct: 0.05,
      take_profit_price: row.take_profit_price || null,
      take_profit_pct: 0.15,
    }
  }
  strategyDialogVisible.value = true
}

const saveStrategy = async () => {
  if (!strategyForm.value.stock_code) return
  strategySaving.value = true
  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/trading-strategies/stock`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(strategyForm.value)
    })
    const data = await res.json()
    if (data.success) {
      ElMessage.success(data.message || '已保存')
      strategyDialogVisible.value = false
    } else {
      ElMessage.error(data.message || '保存失败')
    }
  } catch (e) {
    ElMessage.error('保存失败')
  } finally {
    strategySaving.value = false
  }
}

const handleAction = async (row, action) => {
  if (action === 'clear') {
    // 清仓：先弹确认，再调接口
    try {
      await ElMessageBox.confirm(
        `确认清仓 ${row.stock_name}（${row.stock_code}）？\n\n将卖出全部持仓 ${row.quantity} 股。\n交易时间内将以买盘对手价成交，非交易时间将创建委托单等待执行。`,
        '确认清仓',
        { type: 'warning', confirmButtonText: '确认清仓', cancelButtonText: '取消' }
      )
    } catch {
      return
    }
    await executeClear(row)
  } else {
    // 加仓/减仓：弹出交易对话框
    await openTradeDialog(row, action)
  }
}

// ============================================================
// 交易对话框（加仓/减仓/清仓）
// ============================================================

const tradeDialogVisible = ref(false)
const tradeAction = ref('') // 'add' | 'reduce'
const tradeSubmitting = ref(false)
const tradeForm = ref({
  stock_code: '',
  stock_name: '',
  trade_type: 'buy', // 'buy' | 'sell'
  price: 0,
  quantity: 100,
  maxQuantity: 0, // 卖出时的可卖数量上限
})
const tradeQuote = ref({})
const tradeBidLevels = ref([])
const tradeBidVolumes = ref([])
const tradeAskLevels = ref([])
const tradeAskVolumes = ref([])
const fundLimitQty = ref(0)
const positionQtyDisplay = ref(0)

const tradeDialogTitle = computed(() => {
  if (tradeAction.value === 'clear') return '一键清仓'
  return tradeForm.value.trade_type === 'buy' ? '加仓买入' : '减仓卖出'
})

const tradeSubmitText = computed(() => {
  return tradeForm.value.trade_type === 'buy'
    ? `买入委托 ¥${tradeForm.value.price.toFixed(2)} × ${tradeForm.value.quantity}股`
    : `卖出委托 ¥${tradeForm.value.price.toFixed(2)} × ${tradeForm.value.quantity}股`
})

const canTradeSubmit = computed(() => {
  return (
    tradeForm.value.stock_code &&
    tradeForm.value.price > 0 &&
    tradeForm.value.quantity > 0 &&
    tradeForm.value.quantity % 100 === 0 &&
    !tradeSubmitting.value
  )
})

const openTradeDialog = async (row, action) => {
  tradeAction.value = action
  const isBuy = action === 'add'
  tradeForm.value = {
    stock_code: row.stock_code,
    stock_name: row.stock_name,
    trade_type: isBuy ? 'buy' : 'sell',
    price: 0,
    quantity: 100,
    maxQuantity: isBuy ? 0 : row.quantity,
  }
  positionQtyDisplay.value = row.quantity
  tradeQuote.value = {}
  tradeBidLevels.value = []
  tradeBidVolumes.value = []
  tradeAskLevels.value = []
  tradeAskVolumes.value = []
  fundLimitQty.value = 0
  tradeDialogVisible.value = true

  // 获取行情数据
  await fetchTradeQuote(row.stock_code)
}

const fetchTradeQuote = async (code) => {
  try {
    const resp = await fetch(`/api/v1/ui/${currentAccountId.value}/manual-order/quote`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ stock_code: code }),
    })
    const data = await resp.json()
    if (data.success) {
      tradeQuote.value = {
        current_price: data.current_price,
        bid1: data.bid1,
        ask1: data.ask1,
        change_percent: data.change_percent,
        prev_close: data.prev_close,
      }
      tradeBidLevels.value = data.bid_levels || []
      tradeBidVolumes.value = data.bid_volumes || []
      tradeAskLevels.value = data.ask_levels || []
      tradeAskVolumes.value = data.ask_volumes || []
      fundLimitQty.value = data.fund_limit_quantity || 0

      if (tradeForm.value.trade_type === 'buy') {
        tradeForm.value.maxQuantity = data.max_buy_quantity || 0
        if (data.current_price && data.current_price > 0) {
          tradeForm.value.price = data.current_price
        }
        if (tradeForm.value.maxQuantity > 0) {
          tradeForm.value.quantity = tradeForm.value.maxQuantity
        }
      } else {
        tradeForm.value.maxQuantity = data.available_quantity || 0
        if (data.current_price && data.current_price > 0) {
          tradeForm.value.price = data.current_price
        }
        if (tradeForm.value.maxQuantity > 0) {
          tradeForm.value.quantity = tradeForm.value.maxQuantity
        }
      }
    }
  } catch (e) {
    console.error('获取行情失败:', e)
  }
}

const usePrice = (type) => {
  if (type === 'bid1' && tradeQuote.value.bid1) {
    tradeForm.value.price = tradeQuote.value.bid1
  } else if (type === 'ask1' && tradeQuote.value.ask1) {
    tradeForm.value.price = tradeQuote.value.ask1
  } else if (type === 'current' && tradeQuote.value.current_price) {
    tradeForm.value.price = tradeQuote.value.current_price
  }
}

const useQuantity = (type) => {
  const max = tradeForm.value.trade_type === 'sell'
    ? tradeForm.value.maxQuantity
    : (fundLimitQty.value > 0 ? fundLimitQty.value : tradeForm.value.maxQuantity)

  if (type === 'all') {
    tradeForm.value.quantity = max
  } else if (type === 'half') {
    tradeForm.value.quantity = Math.max(100, Math.floor(max / 2 / 100) * 100)
  }
}

const formatVolume = (v) => {
  if (v == null || v === 0) return '-'
  if (v >= 10000) return (v / 10000).toFixed(1) + '万'
  return v.toLocaleString()
}

const executeClear = async (row) => {
  try {
    const resp = await fetch(
      `/api/v1/ui/${currentAccountId.value}/positions/${row.stock_code}/immediate-sell`,
      { method: 'POST' }
    )
    const data = await resp.json()

    if (data.success) {
      if (data.trading_time) {
        ElMessage.success(`${data.message}，监控程序将扫描执行`)
      } else {
        ElMessage.info(data.message)
      }
      await loadPositions()
    } else {
      ElMessage.error(data.message || '清仓失败')
    }
  } catch (e) {
    ElMessage.error('清仓失败：' + e.message)
  }
}

const submitTrade = async () => {
  tradeSubmitting.value = true
  try {
    const resp = await fetch(`/api/v1/ui/${currentAccountId.value}/manual-order/submit`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        stock_code: tradeForm.value.stock_code,
        stock_name: tradeForm.value.stock_name,
        trade_type: tradeForm.value.trade_type,
        price: tradeForm.value.price,
        quantity: tradeForm.value.quantity,
        order_type: 'day',
      }),
    })
    const data = await resp.json()
    if (data.success) {
      const dir = tradeForm.value.trade_type === 'buy' ? '买入' : '卖出'
      ElMessage.success(`${dir}委托已提交（${tradeForm.value.quantity}股 @ ¥${tradeForm.value.price.toFixed(2)}），监控程序将扫描执行`)
      tradeDialogVisible.value = false
      await loadPositions()
    } else {
      ElMessage.error(data.message || '委托失败')
    }
  } catch (e) {
    ElMessage.error('提交失败：' + e.message)
  } finally {
    tradeSubmitting.value = false
  }
}

const handleDsaAnalysis = async (row) => {
  dsaStock.value = { stock_code: row.stock_code, stock_name: row.stock_name }
  dsaDialogVisible.value = true
  dsaAnalyzing.value = true
  dsaResult.value = null
  dsaError.value = ''

  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/positions/${row.stock_code}/dsa-analyze`, {
      method: 'POST',
    })
    const data = await res.json()

    if (!res.ok) {
      if (data.code === 409) {
        dsaError.value = data.message
        return
      }
      dsaError.value = data.detail || '分析失败'
      return
    }

    dsaResult.value = data
  } catch (e) {
    dsaError.value = '请求失败，请检查网络连接'
  } finally {
    dsaAnalyzing.value = false
  }
}

// ========== K 线图 ==========

const klinePeriod = ref('day')
const klineAdjust = ref('forward')
const klineMonths = ref(12)  // 当前加载的月数（日线）/ 根数（周线）
const klineLoadingMore = ref(false)

const showKline = async (row) => {
  const idx = positions.value.findIndex(s => s.stock_code === row.stock_code)
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
    // 切换周期时重置加载量并清空指标
    klineIndicators.value = {}
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
  klineStockInfo.value = { code, name }
  klineData.value = []
  // 不在这里清空指标数据，等 loadIndicatorData 加载完新数据后自然替换

  // 从本地 kline.db 读取（支持日线/周线/月线 + 前复权）
  // 日线: months 参数控制月数
  // 周线: months 参数转为根数（months * 5，默认至少 250 根）
  // 月线: 不限制，取全部
  const monthsParam = klinePeriod.value === 'month' ? 0 : klineMonths.value
  try {
    const res = await fetch(`/api/v1/ui/${currentAccountId.value}/stocks/${code}/kline-local?months=${monthsParam}&period=${klinePeriod.value}&adjust=${klineAdjust.value}`)
    if (res.ok) {
      const data = await res.json()
      if (data.success && data.kline && data.kline.length > 0) {
        klineData.value = data.kline
        console.log('[K线] 加载完成:', code, '条数:', data.kline.length, '选中指标:', selectedIndicators.value)
        // 加载指标数据（仅日线模式）
        if (klinePeriod.value === 'day' && selectedIndicators.value.length > 0) {
          console.log('[K线] 开始加载指标数据...')
          await loadIndicatorData()
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
    // 加载指标数据
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
  const row = positions.value[idx]
  if (!row) return
  klineStockIndex.value = idx
  klineIndicators.value = {}
  await switchStockPreservingDrillDown(klineChartRef, () => loadKlineData(row.stock_code, row.stock_name))
}

const nextStock = async () => {
  if (!hasNextStock.value) return
  const idx = klineStockIndex.value + 1
  const row = positions.value[idx]
  if (!row) return
  klineStockIndex.value = idx
  klineIndicators.value = {}
  await switchStockPreservingDrillDown(klineChartRef, () => loadKlineData(row.stock_code, row.stock_name))
}

// 静默刷新当前价（从内存 PriceCache 取，不触发 SDK 调用）
let priceRefreshTimer = null
const startPriceRefresh = () => {
  priceRefreshTimer = setInterval(async () => {
    const oldPrices = prepareForRefresh()
    try {
      // 并发加载持仓和策略统计
      await Promise.all([
        posStore.loadPositions(currentAccountId.value),
        posStore.loadStrategyStats(currentAccountId.value)
      ])
      compareAndHighlight(oldPrices)
    } catch (e) {
      // 静默失败，不弹提示
    }
  }, 30000)  // 每 30 秒静默刷新
}

let posAbortController = null

onUnmounted(() => {
  posAbortController?.abort()
  if (priceRefreshTimer) { clearInterval(priceRefreshTimer); priceRefreshTimer = null }
})

onMounted(async () => {
  posAbortController = new AbortController()
  if (!posStore.loaded) {
    await loadPositions()
    await loadClosedPositions()
    await loadStrategyStats()
  }
  await nextTick()
  setTimeout(() => refreshPrices(), 0)
  startPriceRefresh()
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
}

h2 {
  margin-bottom: 20px;
  color: #303133;
}

.overview-card {
  margin-bottom: 20px;
}

.strategy-stats-card {
  margin-bottom: 20px;
}

.text-muted {
  color: #909399;
}

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.filter-hint {
  color: #409eff;
  font-size: 12px;
  margin-left: 8px;
}

.detail-tabs {
  flex: 1;
}

.detail-tabs :deep(.el-tabs__header) {
  margin-bottom: 0;
}

.detail-tabs :deep(.el-tabs__nav-wrap::after) {
  display: none;
}

.profit-positive {
  color: #f56c6c;
  font-weight: bold;
}

.profit-negative {
  color: #67c23a;
  font-weight: bold;
}

/* 策略表格行选中高亮（覆盖 stripe 灰度） */
:deep(.strategy-row-selected td),
:deep(.strategy-row-selected.el-table__row--striped td) {
  background-color: #d9ecff !important;
}

:deep(.el-table__row) {
  cursor: pointer;
}

.dsa-loading {
  text-align: center;
  padding: 40px 0;
}

.dsa-loading p {
  margin-top: 16px;
  color: #909399;
}

.dsa-content .section-title {
  margin: 16px 0 8px;
  font-size: 14px;
  color: #303133;
  border-left: 3px solid #409EFF;
  padding-left: 8px;
}

.dsa-content .analysis-text {
  background: #f5f7fa;
  padding: 12px;
  border-radius: 4px;
  font-size: 13px;
  line-height: 1.6;
  color: #606266;
  white-space: pre-wrap;
}

.dsa-content .mb-16 {
  margin-bottom: 16px;
}

.dsa-error {
  padding: 20px 0;
}

.pagination-bar {
  display: flex; justify-content: center; padding: 12px 0;
}

.bid-ask-display {
  display: flex;
  flex-direction: column;
  gap: 2px;
  font-size: 12px;
  font-family: monospace;
}

.bid-level {
  color: #f56c6c;
}

.position-hint {
  font-size: 12px;
  color: #909399;
  margin-top: 4px;
}

/* 五档盘口左右排列 */
.level5-container {
  font-size: 13px;
  font-family: monospace;
  border: 1px solid #ebeef5;
  border-radius: 4px;
  overflow: hidden;
  width: 380px;
}

.level5-header {
  display: grid;
  grid-template-columns: 1fr 1fr 1fr 1fr;
  background: #f5f7fa;
  padding: 4px 8px;
  font-size: 12px;
  color: #909399;
  font-weight: 500;
  text-align: center;
}

.level5-row {
  display: grid;
  grid-template-columns: 1fr 1fr 1fr 1fr;
  padding: 3px 8px;
  text-align: center;
  border-top: 1px solid #f0f0f0;
}

.col-ask {
  color: #67c23a;
}

.col-bid {
  color: #f56c6c;
}

.col-price {
  padding: 0 4px;
  font-weight: 600;
}

.ask-price {
  color: #67c23a;
}

.bid-price {
  color: #f56c6c;
}

.level-volume {
  font-size: 12px;
  color: #606266;
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
