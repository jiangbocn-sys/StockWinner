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

    <!-- 字段条件节点（stock_filters） -->
    <el-tag v-else-if="isFieldNode(node)" size="small" class="condition-tag">
      {{ formatFieldNode(node) }}
    </el-tag>

    <!-- 字符串条件节点（buy_conditions） -->
    <el-tag v-else-if="typeof node === 'string'" size="small" class="condition-tag">
      {{ node }}
    </el-tag>

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
import { computed } from 'vue'

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