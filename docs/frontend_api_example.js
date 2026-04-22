/**
 * 市场行情数据 API 前端调用示例
 *
 * 本文件演示如何调用 StockWinner 系统的市场行情数据 API
 * 数据源：AmazingData SDK（通过银河网关统一接口）
 */

// ==================== 配置项 ====================
const API_BASE_URL = 'http://localhost:8080/api/v1/ui';
const ACCOUNT_ID = '8229DE7E'; // 替换为你的账户 ID

// ==================== API 调用函数 ====================

/**
 * 获取单只股票实时行情
 * @param {string} stockCode - 股票代码（如：600519, 000001）
 * @returns {Promise<Object>} 行情数据
 */
async function getStockQuote(stockCode) {
  const response = await fetch(`${API_BASE_URL}/${ACCOUNT_ID}/market/quote/${stockCode}`);
  const result = await response.json();

  if (!result.success) {
    throw new Error(result.detail || '获取行情失败');
  }

  return result.data;
}

/**
 * 批量获取股票实时行情
 * @param {string[]} stockCodes - 股票代码数组
 * @returns {Promise<Object>} 行情数据集合
 */
async function getBatchQuotes(stockCodes) {
  const response = await fetch(`${API_BASE_URL}/${ACCOUNT_ID}/market/quotes`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({ stock_codes: stockCodes })
  });

  const result = await response.json();

  if (!result.success) {
    throw new Error(result.detail || '批量获取行情失败');
  }

  return result.data;
}

/**
 * 获取 K 线历史数据
 * @param {string} stockCode - 股票代码
 * @param {string} period - K 线周期 (1m/5m/15m/30m/60m/day/week/month)
 * @param {number} limit - 返回数量（默认 100）
 * @param {string} startDate - 开始日期（可选，格式：YYYYMMDD）
 * @param {string} endDate - 结束日期（可选，格式：YYYYMMDD）
 * @returns {Promise<Object>} K 线数据
 */
async function getKlineData(stockCode, period = 'day', limit = 100, startDate = null, endDate = null) {
  const params = new URLSearchParams({
    stock_code: stockCode,
    period: period,
    limit: limit
  });

  if (startDate) params.append('start_date', startDate);
  if (endDate) params.append('end_date', endDate);

  const response = await fetch(`${API_BASE_URL}/${ACCOUNT_ID}/market/kline?${params}`);
  const result = await response.json();

  if (!result.success) {
    throw new Error(result.detail || '获取 K 线数据失败');
  }

  return result.data;
}

/**
 * 获取最新一根 K 线数据
 * @param {string} stockCode - 股票代码
 * @param {string} period - K 线周期 (1m/5m/15m/30m/60m/day/week/month)
 * @returns {Promise<Object>} 最新 K 线数据
 */
async function getLatestKline(stockCode, period = 'day') {
  const params = new URLSearchParams({
    stock_code: stockCode,
    period: period
  });

  const response = await fetch(`${API_BASE_URL}/${ACCOUNT_ID}/market/kline/latest?${params}`);
  const result = await response.json();

  if (!result.success) {
    throw new Error(result.detail || '获取 K 线数据失败');
  }

  return result.data;
}

// ==================== 使用示例 ====================

/**
 * 示例 1: 获取单只股票行情并显示
 */
async function example1_showStockQuote() {
  try {
    const quote = await getStockQuote('600519');
    console.log('=== 贵州茅台实时行情 ===');
    console.log(`当前价格：${quote.current_price.toFixed(2)} 元`);
    console.log(`涨跌幅：${quote.change_percent.toFixed(2)}%`);
    console.log(`最高价：${quote.high.toFixed(2)} 元`);
    console.log(`最低价：${quote.low.toFixed(2)} 元`);
    console.log(`成交量：${quote.volume.toLocaleString()} 股`);
    console.log(`成交额：${(quote.amount / 100000000).toFixed(2)} 亿元`);
  } catch (error) {
    console.error('获取行情失败:', error.message);
  }
}

/**
 * 示例 2: 批量获取多只股票行情
 */
async function example2_batchQuotes() {
  try {
    const stocks = ['600519', '000001', '601398', '000858'];
    const result = await getBatchQuotes(stocks);

    console.log('=== 批量行情数据 ===');
    console.log(`成功获取：${result.count} 只股票`);
    console.log(`获取失败：${result.failed_count} 只股票`);

    result.quotes.forEach(quote => {
      const sign = quote.change_percent >= 0 ? '↑' : '↓';
      console.log(`${quote.stock_name} (${quote.stock_code}): ${quote.current_price.toFixed(2)}元 ${sign}${Math.abs(quote.change_percent).toFixed(2)}%`);
    });

    if (result.failed_count > 0) {
      console.log('获取失败的股票:', result.failed.join(', '));
    }
  } catch (error) {
    console.error('批量获取失败:', error.message);
  }
}

/**
 * 示例 3: 获取 K 线数据并绘制（控制台简易版）
 */
async function example3_klineChart() {
  try {
    const klineResult = await getKlineData('600519', 'day', 10);

    console.log('=== 贵州茅台近 10 日 K 线 ===');
    console.log('日期        开盘      最高      最低      收盘');
    console.log('-'.repeat(50));

    klineResult.kline.forEach(day => {
      const date = day.time;
      console.log(`${date}  ${day.open.toFixed(2)}  ${day.high.toFixed(2)}  ${day.low.toFixed(2)}  ${day.close.toFixed(2)}`);
    });
  } catch (error) {
    console.error('获取 K 线失败:', error.message);
  }
}

/**
 * 示例 4: 获取最新日线数据
 */
async function example4_latestDaily() {
  try {
    const latest = await getLatestKline('600519', 'day');
    const kline = latest.kline;

    console.log('=== 贵州茅台最新日线 ===');
    console.log(`日期：${kline.time}`);
    console.log(`开盘：${kline.open.toFixed(2)} 元`);
    console.log(`最高：${kline.high.toFixed(2)} 元`);
    console.log(`最低：${kline.low.toFixed(2)} 元`);
    console.log(`收盘：${kline.close.toFixed(2)} 元`);
    console.log(`成交量：${kline.volume.toLocaleString()} 股`);
    console.log(`成交额：${(kline.amount / 100000000).toFixed(2)} 亿元`);
  } catch (error) {
    console.error('获取最新 K 线失败:', error.message);
  }
}

/**
 * 示例 5: Vue 3 组合式 API 集成示例
 */
function example5_vue3Integration() {
  // 这是在 Vue 3 组件中的使用方式
  /*
  import { ref, onMounted } from 'vue';

  export default {
    setup() {
      const quote = ref(null);
      const loading = ref(false);
      const error = ref(null);

      // 加载行情数据
      const loadQuote = async (stockCode) => {
        loading.value = true;
        error.value = null;
        try {
          quote.value = await getStockQuote(stockCode);
        } catch (err) {
          error.value = err.message;
        } finally {
          loading.value = false;
        }
      };

      // 组件挂载时自动加载
      onMounted(() => {
        loadQuote('600519');
      });

      return {
        quote,
        loading,
        error,
        loadQuote
      };
    },
    template: `
      <div v-if="loading">加载中...</div>
      <div v-else-if="error">错误：{{ error }}</div>
      <div v-else-if="quote">
        <h3>{{ quote.stock_name }} 实时行情</h3>
        <p>当前价格：{{ quote.current_price.toFixed(2) }} 元</p>
        <p :class="quote.change_percent >= 0 ? 'up' : 'down'">
          涨跌幅：{{ quote.change_percent.toFixed(2) }}%
        </p>
      </div>
    `
  };
  */
  console.log('Vue 3 集成示例代码请查看注释部分');
}

// ==================== 运行示例 ====================

async function runExamples() {
  console.log('开始运行 API 调用示例...\n');

  console.log('【示例 1】获取单只股票行情:');
  await example1_showStockQuote();
  console.log();

  console.log('【示例 2】批量获取股票行情:');
  await example2_batchQuotes();
  console.log();

  console.log('【示例 3】获取 K 线数据:');
  await example3_klineChart();
  console.log();

  console.log('【示例 4】获取最新日线:');
  await example4_latestDaily();
  console.log();

  console.log('【示例 5】Vue 3 集成:');
  example5_vue3Integration();
  console.log();

  console.log('所有示例运行完成！');
}

// 如果直接在 Node.js 环境运行
if (typeof require !== 'undefined' && require.main === module) {
  runExamples().catch(console.error);
}

// 导出函数供其他模块使用
export {
  getStockQuote,
  getBatchQuotes,
  getKlineData,
  getLatestKline
};
