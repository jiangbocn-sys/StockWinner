/**
 * K线钻取状态保留 helper
 * 切换股票时自动保留钻取模式并重新进入
 */

/**
 * 保存钻取状态
 * @param {Object} klineRef - KlineChart 组件 ref
 * @returns {Object|null} { wasDrillDown, drillDate, drillPeriod } 或 null
 */
export function saveDrillDownState(klineRef) {
  if (!klineRef?.value) return null
  const wasDrillDown = klineRef.value.drillDownMode?.value
  const drillDate = klineRef.value.drillDownDate?.value
  const drillPeriod = klineRef.value.minutePeriod?.value
  if (wasDrillDown && drillDate && drillPeriod) {
    return { wasDrillDown, drillDate, drillPeriod }
  }
  return null
}

/**
 * 退出钻取模式（如果处于钻取状态）
 * @param {Object} klineRef - KlineChart 组件 ref
 * @param {Object} savedState - 保存的钻取状态
 * @returns {Promise<void>}
 */
export async function exitDrillDownIfNeeded(klineRef, savedState) {
  if (savedState?.wasDrillDown && klineRef?.value) {
    klineRef.value.exitDrillDown?.()
    // 等待组件更新
    await new Promise(resolve => setTimeout(resolve, 50))
  }
}

/**
 * 恢复钻取模式（如果之前处于钻取状态）
 * @param {Object} klineRef - KlineChart 组件 ref
 * @param {Object} savedState - 保存的钻取状态
 * @returns {Promise<void>}
 */
export async function restoreDrillDownIfNeeded(klineRef, savedState) {
  if (savedState?.wasDrillDown && savedState.drillDate && savedState.drillPeriod && klineRef?.value) {
    // 等待日线数据渲染完成
    await new Promise(resolve => setTimeout(resolve, 100))
    klineRef.value.enterDrillDown?.(savedState.drillDate, savedState.drillPeriod)
  }
}

/**
 * 一站式：切换股票时保留钻取状态
 * @param {Object} klineRef - KlineChart 组件 ref
 * @param {Function} loadFn - 加载新股票数据的异步函数
 * @returns {Promise<void>}
 */
export async function switchStockPreservingDrillDown(klineRef, loadFn) {
  const savedState = saveDrillDownState(klineRef)
  await exitDrillDownIfNeeded(klineRef, savedState)
  await loadFn()
  await restoreDrillDownIfNeeded(klineRef, savedState)
}