<template>
  <div class="condition-tree">
    <!-- 逻辑组节点 -->
    <span v-if="isLogicNode(node)" class="logic-group">
      <!-- OR节点加括号 -->
      <span v-if="node.logic === 'OR'" class="parenthesis">(</span>

      <template v-for="(cond, idx) in node.conditions" :key="idx">
        <condition-tree :node="cond" />
        <span v-if="idx < node.conditions.length - 1" class="logic-operator">
          {{ node.logic }}
        </span>
      </template>

      <!-- OR节点加括号 -->
      <span v-if="node.logic === 'OR'" class="parenthesis">)</span>
    </span>

    <!-- 字段条件节点（stock_filters）带tooltip -->
    <el-tooltip v-else-if="isFieldNode(node)" :content="getFieldTooltip(node)" placement="top">
      <el-tag size="small" class="condition-tag">
        {{ formatFieldNode(node) }}
      </el-tag>
    </el-tooltip>

    <!-- 字符串条件节点（buy_conditions）带tooltip -->
    <el-tooltip v-else-if="typeof node === 'string'" :content="getConditionTooltip(node)" placement="top">
      <el-tag size="small" class="condition-tag">
        {{ node }}
      </el-tag>
    </el-tooltip>

    <!-- 列表节点（旧格式） -->
    <span v-else-if="Array.isArray(node)" class="legacy-list">
      <template v-for="(cond, idx) in node" :key="idx">
        <condition-tree :node="cond" />
        <span v-if="idx < node.length - 1" class="logic-operator">AND</span>
      </template>
    </span>

    <!-- 无法识别的节点 -->
    <span v-else class="unknown-node">{{ JSON.stringify(node) }}</span>
  </div>
</template>

<script setup>
const props = defineProps({
  node: {
    type: [Object, String, Array],
    required: true
  }
})

// 字段显示名称映射
const FIELD_NAMES = {
  'total_market_cap_max': '总市值<',
  'total_market_cap_min': '总市值>',
  'circ_market_cap_max': '流通市值<',
  'circ_market_cap_min': '流通市值>',
  'pe_ttm_max': 'PE<',
  'pe_ttm_min': 'PE>',
  'pb_max': 'PB<',
  'pb_min': 'PB>',
  'roe_min': 'ROE>',
  'roe_max': 'ROE<',
  'gross_margin_min': '毛利率>',
  'net_margin_min': '净利率>',
  'revenue_growth_yoy_min': '营收增长>',
  'sw_level1': '行业'
}

// 字段含义说明映射（用于tooltip）
const FIELD_DESCRIPTIONS = {
  'total_market_cap_max': '总市值上限（亿元）：筛选总市值小于该值的股票',
  'total_market_cap_min': '总市值下限（亿元）：筛选总市值大于该值的股票',
  'circ_market_cap_max': '流通市值上限（亿元）：筛选流通市值小于该值的股票',
  'circ_market_cap_min': '流通市值下限（亿元）：筛选流通市值大于该值的股票',
  'pe_ttm_max': '市盈率TTM上限（倍）：筛选PE小于该值的股票，数值越低估值越低',
  'pe_ttm_min': '市盈率TTM下限（倍）：筛选PE大于该值的股票',
  'pb_max': '市净率上限（倍）：筛选PB小于该值的股票',
  'pb_min': '市净率下限（倍）：筛选PB大于该值的股票',
  'roe_min': 'ROE下限（%）：净资产收益率，衡量盈利能力，数值越高越好',
  'roe_max': 'ROE上限（%）：筛选ROE小于该值的股票',
  'gross_margin_min': '毛利率下限（%）：衡量产品盈利能力',
  'net_margin_min': '净利率下限（%）：衡量整体盈利能力',
  'revenue_growth_yoy_min': '营收同比增长下限（%）：衡量成长性',
  'sw_level1': '申万一级行业分类'
}

// 技术条件含义说明映射（用于tooltip）
const CONDITION_DESCRIPTIONS = {
  'DIF_CROSS_UP_DEA': 'MACD金叉信号：DIF线向上穿越DEA线，通常被视为买入信号',
  'DIF_CROSS_DOWN_DEA': 'MACD死叉信号：DIF线向下穿越DEA线，通常被视为卖出信号',
  'MA5_CROSS_UP_MA10': '均线金叉：5日均线向上穿越10日均线，短线走强信号',
  'MA5_CROSS_DOWN_MA10': '均线死叉：5日均线向下穿越10日均线，短线走弱信号',
  'MA10_CROSS_UP_MA20': '均线金叉：10日均线向上穿越20日均线',
  'RSI < 30': 'RSI超卖区域（<30）：股价可能被过度抛售，存在反弹机会',
  'RSI > 70': 'RSI超买区域（>70）：股价可能被过度买入，存在回调风险',
  'RSI_14 < 30': 'RSI(14)超卖：14日RSI低于30，股价可能被过度抛售',
  'RSI_14 > 70': 'RSI(14)超买：14日RSI高于70，股价可能被过度买入',
  'VOLUME_RATIO > 2': '量比大于2：当日成交量是过去5日平均的2倍以上，放量明显',
  'VOLUME_RATIO > 1.5': '量比大于1.5：当日成交量是过去5日平均的1.5倍以上',
  'VOLUME_RATIO > 3': '量比大于3：当日成交量是过去5日平均的3倍以上，剧烈放量',
  'PRICE > MA5': '价格站上5日均线：收盘价高于5日均线',
  'PRICE < MA5': '价格跌破5日均线：收盘价低于5日均线',
  'CLOSE > MA5': '收盘价高于5日均线',
  'CLOSE < MA5': '收盘价低于5日均线',
  'MACD > 0': 'MACD柱为正值：DIF高于DEA，多头动能',
  'MACD < 0': 'MACD柱为负值：DIF低于DEA，空头动能'
}

// 判断是否为逻辑组节点
const isLogicNode = (node) => {
  return node && typeof node === 'object' && 'logic' in node && 'conditions' in node
}

// 判断是否为字段条件节点
const isFieldNode = (node) => {
  return node && typeof node === 'object' && 'field' in node
}

// 格式化字段条件节点
const formatFieldNode = (node) => {
  const field = node.field || ''
  const value = node.value

  const displayName = FIELD_NAMES[field] || field

  if (field === 'sw_level1') {
    return `${displayName}=${value}`
  }

  // 数值条件
  if (typeof value === 'number') {
    // 根据字段名判断单位
    if (field.includes('market_cap') || field.includes('MarketCap')) {
      return `${displayName}${value}亿`
    }
    if (field.includes('margin') || field.includes('Margin') ||
        field.includes('growth') || field.includes('Growth') ||
        field.includes('roe') || field.includes('ROE')) {
      return `${displayName}${value}%`
    }
    return `${displayName}${value}`
  }

  return `${displayName}${value}`
}

// 获取字段条件的tooltip内容
const getFieldTooltip = (node) => {
  const field = node.field || ''
  const value = node.value

  const description = FIELD_DESCRIPTIONS[field] || field
  const unit = getUnitHint(field, value)

  return `${description}\n当前值: ${value}${unit}`
}

// 获取技术条件的tooltip内容
const getConditionTooltip = (condition) => {
  // 精确匹配
  if (CONDITION_DESCRIPTIONS[condition]) {
    return CONDITION_DESCRIPTIONS[condition]
  }

  // 模式匹配（如 "VOLUME_RATIO > 1.8"）
  const volumeMatch = condition.match(/VOLUME_RATIO\s*>\s*(\d+\.?\d*)/)
  if (volumeMatch) {
    const ratio = volumeMatch[1]
    return `量比大于${ratio}：当日成交量是过去5日平均的${ratio}倍以上，放量信号`
  }

  const rsiMatch = condition.match(/RSI[_\s]*14\s*[<>]\s*(\d+)/)
  if (rsiMatch) {
    const value = rsiMatch[1]
    const op = condition.includes('<') ? '低于' : '高于'
    const meaning = value < 30 ? '超卖区域，可能反弹' : value > 70 ? '超买区域，可能回调' : 'RSI指标区间'
    return `RSI(14) ${op}${value}：${meaning}`
  }

  const priceMaMatch = condition.match(/PRICE|CLOSE\s*[<>]\s*MA(\d+)/)
  if (priceMaMatch) {
    const ma = priceMaMatch[1] || condition.match(/MA(\d+)/)?.[1]
    const op = condition.includes('>') ? '高于' : '低于'
    return `价格${op}${ma}日均线：衡量股价相对均线位置`
  }

  const macdMatch = condition.match(/MACD\s*[<>]\s*0/)
  if (macdMatch) {
    const op = condition.includes('>') ? '正值' : '负值'
    return `MACD柱${op}：DIF${condition.includes('>') ? '高于' : '低于'}DEA`
  }

  // 无法识别的条件
  return `技术条件：${condition}`
}

// 获取单位提示
const getUnitHint = (field, value) => {
  if (field.includes('market_cap')) return '亿元'
  if (field.includes('margin') || field.includes('roe') || field.includes('growth')) return '%'
  if (field.includes('pe') || field.includes('pb')) return '倍'
  return ''
}
</script>

<style scoped>
.condition-tree {
  display: inline-flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 4px;
}

.logic-group {
  display: inline-flex;
  align-items: center;
  gap: 4px;
}

.logic-operator {
  color: #409EFF;
  font-weight: bold;
  font-size: 12px;
  margin: 0 2px;
}

.logic-operator.or {
  color: #E6A23C;
}

.parenthesis {
  color: #E6A23C;
  font-weight: bold;
}

.condition-tag {
  margin: 2px;
  cursor: help;
}

.legacy-list {
  display: inline-flex;
  align-items: center;
  gap: 4px;
}

.unknown-node {
  color: #999;
  font-size: 12px;
}
</style>