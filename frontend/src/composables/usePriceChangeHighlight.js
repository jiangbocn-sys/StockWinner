/**
 * 价格变化高亮 composable
 * 统一各 View 文件中重复的价格对比和高亮逻辑
 */
import { ref } from 'vue'

/**
 * @param {Ref<Array>} itemsRef - 包含价格数据的响应式数组
 * @param {string} codeKey - 股票代码字段名，默认 'stock_code'
 * @param {string} priceKey - 价格字段名，默认 'current_price'
 * @returns {Object} { highlight, compareAndHighlight, clearHighlight }
 */
export function usePriceChangeHighlight(itemsRef, codeKey = 'stock_code', priceKey = 'current_price') {
  const highlight = ref(new Map())
  const clearTimer = ref(null)

  /**
   * 保存旧价格到 Map
   * @returns {Map<string, number>}
   */
  const collectOldPrices = () => {
    const oldPrices = new Map()
    for (const item of itemsRef.value) {
      const price = item[priceKey]
      const code = item[codeKey]
      if (price > 0 && code) {
        oldPrices.set(code, price)
      }
    }
    return oldPrices
  }

  /**
   * 对比新旧价格，标记变化
   * @param {Map<string, number>} oldPrices
   */
  const compareAndHighlight = (oldPrices) => {
    const changes = new Map()
    for (const item of itemsRef.value) {
      const code = item[codeKey]
      const newPrice = item[priceKey]
      const oldPrice = oldPrices.get(code)
      if (oldPrice > 0 && newPrice > 0) {
        if (newPrice > oldPrice) {
          changes.set(code, 'up')
        } else if (newPrice < oldPrice) {
          changes.set(code, 'down')
        }
      }
    }
    if (changes.size > 0) {
      highlight.value = changes
      scheduleClear()
    }
    return changes
  }

  /**
   * 安排3秒后清除高亮
   */
  const scheduleClear = () => {
    if (clearTimer.value) {
      clearTimeout(clearTimer.value)
    }
    clearTimer.value = setTimeout(() => {
      highlight.value = new Map()
      clearTimer.value = null
    }, 3000)
  }

  /**
   * 立即清除高亮
   */
  const clearHighlight = () => {
    if (clearTimer.value) {
      clearTimeout(clearTimer.value)
      clearTimer.value = null
    }
    highlight.value = new Map()
  }

  /**
   * 一站式：保存旧价格 → (等待数据刷新) → 对比并高亮
   * 返回保存的旧价格 Map，供调用者在数据刷新后调用 compareAndHighlight
   */
  const prepareForRefresh = () => {
    return collectOldPrices()
  }

  return {
    highlight,
    prepareForRefresh,
    compareAndHighlight,
    clearHighlight,
  }
}