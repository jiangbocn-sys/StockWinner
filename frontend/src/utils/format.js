/**
 * 格式化工具函数
 *
 * 统一各 View 文件中重复的格式化函数。
 */

export function formatNumber(num) {
  return Number(num || 0).toLocaleString('zh-CN', { minimumFractionDigits: 2 })
}

export function formatMoney(val) {
  if (val === null || val === undefined) return '-'
  return Number(val).toLocaleString('zh-CN', { minimumFractionDigits: 0, maximumFractionDigits: 0 })
}

export function formatPct(val) {
  if (val == null) return '-'
  const n = Number(val)
  if (n === 0) return '0'
  return (n * 100).toFixed(2) + '%'
}

export function formatTime(time) {
  if (!time) return '-'
  // naive string 默认中国时间，显式附加 +08:00
  const str = time.includes('+') || time.endsWith('Z') ? time : time + '+08:00'
  const date = new Date(str)
  if (isNaN(date.getTime())) return time
  const pad = (n) => n.toString().padStart(2, '0')
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())} ${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`
}
