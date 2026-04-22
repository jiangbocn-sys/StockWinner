<template>
  <div class="market-data-container">
    <el-card class="box-card">
      <template #header>
        <div class="card-header">
          <span>实时行情</span>
          <el-input
            v-model="stockCode"
            placeholder="输入股票代码"
            style="width: 150px"
            @keyup.enter="loadQuote"
          >
            <template #append>
              <el-button @click="loadQuote">查询</el-button>
            </template>
          </el-input>
        </div>
      </template>

      <!-- 加载中 -->
      <div v-if="loading" class="loading">
        <el-spinner />
        <p>加载中...</p>
      </div>

      <!-- 错误信息 -->
      <div v-else-if="error" class="error">
        <el-alert type="error" :title="error" show-icon />
      </div>

      <!-- 行情数据 -->
      <div v-else-if="quote" class="quote-info">
        <div class="stock-header">
          <h3>{{ quote.stock_name }}</h3>
          <span class="stock-code">{{ quote.stock_code }}</span>
        </div>

        <div class="price-section">
          <div class="current-price" :class="priceClass">
            {{ formatPrice(quote.current_price) }}
          </div>
          <div class="change-percent" :class="priceClass">
            {{ formatPercent(quote.change_percent) }}
          </div>
        </div>

        <el-row :gutter="16" class="data-grid">
          <el-col :span="12">
            <div class="data-item">
              <span class="label">最高</span>
              <span class="value up">{{ formatPrice(quote.high) }}</span>
            </div>
          </el-col>
          <el-col :span="12">
            <div class="data-item">
              <span class="label">最低</span>
              <span class="value down">{{ formatPrice(quote.low) }}</span>
            </div>
          </el-col>
          <el-col :span="12">
            <div class="data-item">
              <span class="label">开盘</span>
              <span class="value">{{ formatPrice(quote.open_price) }}</span>
            </div>
          </el-col>
          <el-col :span="12">
            <div class="data-item">
              <span class="label">昨收</span>
              <span class="value">{{ formatPrice(quote.prev_close) }}</span>
            </div>
          </el-col>
          <el-col :span="12">
            <div class="data-item">
              <span class="label">成交量</span>
              <span class="value">{{ formatVolume(quote.volume) }}</span>
            </div>
          </el-col>
          <el-col :span="12">
            <div class="data-item">
              <span class="label">成交额</span>
              <span class="value">{{ formatAmount(quote.amount) }}</span>
            </div>
          </el-col>
        </el-row>
      </div>

      <!-- 空状态 -->
      <el-empty v-else description="请输入股票代码查询" />
    </el-card>
  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue';
import { ElMessage } from 'element-plus';

// API 配置
const API_BASE_URL = '/api/v1/ui';
const ACCOUNT_ID = '8229DE7E'; // 替换为实际账户 ID

// 响应式数据
const stockCode = ref('600519');
const quote = ref(null);
const loading = ref(false);
const error = ref(null);

// 计算属性：价格涨跌颜色
const priceClass = computed(() => {
  if (!quote.value) return '';
  return quote.value.change_percent >= 0 ? 'up' : 'down';
});

/**
 * 获取单只股票实时行情
 */
async function loadQuote() {
  if (!stockCode.value) {
    ElMessage.warning('请输入股票代码');
    return;
  }

  loading.value = true;
  error.value = null;

  try {
    const response = await fetch(`${API_BASE_URL}/${ACCOUNT_ID}/market/quote/${stockCode.value}`);
    const result = await response.json();

    if (!result.success) {
      throw new Error(result.detail || '获取行情失败');
    }

    quote.value = result.data;
  } catch (err) {
    error.value = err.message;
    ElMessage.error(`获取行情失败：${err.message}`);
    quote.value = null;
  } finally {
    loading.value = false;
  }
}

/**
 * 格式化价格
 */
function formatPrice(price) {
  return price.toFixed(2);
}

/**
 * 格式化涨跌幅
 */
function formatPercent(percent) {
  const sign = percent >= 0 ? '+' : '';
  return `${sign}${percent.toFixed(2)}%`;
}

/**
 * 格式化成交量
 */
function formatVolume(volume) {
  if (volume >= 100000000) {
    return `${(volume / 100000000).toFixed(2)}亿手`;
  } else if (volume >= 10000) {
    return `${(volume / 10000).toFixed(2)}万手`;
  }
  return `${volume}手`;
}

/**
 * 格式化成交额
 */
function formatAmount(amount) {
  if (amount >= 100000000) {
    return `${(amount / 100000000).toFixed(2)}亿`;
  } else if (amount >= 10000) {
    return `${(amount / 10000).toFixed(2)}万`;
  }
  return amount.toFixed(2);
}

// 组件挂载时加载默认股票
onMounted(() => {
  loadQuote();
});
</script>

<style scoped>
.market-data-container {
  max-width: 600px;
  margin: 20px auto;
}

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.loading {
  text-align: center;
  padding: 40px;
}

.loading p {
  margin-top: 16px;
  color: #999;
}

.error {
  padding: 20px;
}

.quote-info {
  padding: 10px 0;
}

.stock-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 20px;
}

.stock-header h3 {
  margin: 0;
  font-size: 20px;
  color: #333;
}

.stock-code {
  color: #999;
  font-size: 14px;
}

.price-section {
  text-align: center;
  margin-bottom: 24px;
}

.current-price {
  font-size: 36px;
  font-weight: bold;
  line-height: 1;
}

.change-percent {
  font-size: 18px;
  margin-top: 8px;
}

.up {
  color: #f56c6c;
}

.down {
  color: #67c23a;
}

.data-grid {
  margin-top: 20px;
}

.data-item {
  display: flex;
  flex-direction: column;
  padding: 12px;
  background: #f5f7fa;
  border-radius: 4px;
}

.data-item .label {
  font-size: 12px;
  color: #999;
  margin-bottom: 4px;
}

.data-item .value {
  font-size: 16px;
  font-weight: 500;
  color: #333;
}

.data-item .value.up {
  color: #f56c6c;
}

.data-item .value.down {
  color: #67c23a;
}
</style>
