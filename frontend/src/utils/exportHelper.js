/**
 * 通用数据表导出工具
 *
 * 支持格式：txt, json, md, csv, excel
 * 使用方式：
 *   import { exportTable } from '@/utils/exportHelper'
 *   exportTable(columns, data, '文件名', 'csv')
 *
 * columns: [{ label: '列标题', prop: '字段名' }, ...]
 * data: 要导出的数据数组
 */

/**
 * 触发浏览器下载
 */
function downloadBlob(content, fileName, mimeType) {
  const blob = new Blob([content], { type: `${mimeType};charset=utf-8` })
  // BOM 用于 Excel 正确打开 CSV/UTF-8 文件
  if (mimeType.includes('csv') || mimeType.includes('excel')) {
    const bom = new Uint8Array([0xEF, 0xBB, 0xBF])
    const blobWithBom = new Blob([bom, blob], { type: mimeType })
    const url = URL.createObjectURL(blobWithBom)
    triggerDownload(url, fileName)
  } else {
    const url = URL.createObjectURL(blob)
    triggerDownload(url, fileName)
  }
}

function triggerDownload(url, fileName) {
  const a = document.createElement('a')
  a.href = url
  a.download = fileName
  document.body.appendChild(a)
  a.click()
  document.body.removeChild(a)
  URL.revokeObjectURL(url)
}

/**
 * 转义 CSV 字段（处理逗号、引号、换行）
 */
function escapeCsvField(val) {
  if (val == null) return ''
  const str = String(val)
  if (str.includes(',') || str.includes('"') || str.includes('\n') || str.includes('\r')) {
    return '"' + str.replace(/"/g, '""') + '"'
  }
  return str
}

/**
 * 转义 MD 表格字段
 */
function escapeMdField(val) {
  if (val == null) return ''
  return String(val).replace(/\|/g, '\\|').replace(/\n/g, '<br>')
}

/**
 * 通用导出函数
 *
 * @param {Array} columns - [{ label: '列标题', prop: '字段名' }, ...]
 * @param {Array} data - 要导出的数据数组
 * @param {string} fileName - 文件名（不含扩展名）
 * @param {string} format - 导出格式：'txt' | 'json' | 'md' | 'csv' | 'excel'
 */
export function exportTable(columns, data, fileName, format = 'csv') {
  if (!data || data.length === 0) {
    alert('没有可导出的数据')
    return
  }

  const ts = new Date().toISOString().slice(0, 19).replace(/[T:]/g, '-').replace(/ /g, '_')
  const safeName = fileName || 'export'

  switch (format) {
    case 'json': {
      const content = JSON.stringify(data, null, 2)
      downloadBlob(content, `${safeName}_${ts}.json`, 'application/json')
      break
    }
    case 'txt': {
      const header = columns.map(c => c.label).join('\t')
      const separator = columns.map(() => '---').join('\t')
      const rows = data.map(row =>
        columns.map(col => {
          const val = row[col.prop]
          return val == null ? '' : String(val)
        }).join('\t')
      )
      const content = [header, separator, ...rows].join('\n')
      downloadBlob(content, `${safeName}_${ts}.txt`, 'text/plain')
      break
    }
    case 'md': {
      const header = '| ' + columns.map(c => escapeMdField(c.label)).join(' | ') + ' |'
      const separator = '| ' + columns.map(() => '---').join(' | ') + ' |'
      const rows = data.map(row =>
        '| ' + columns.map(col => escapeMdField(row[col.prop])).join(' | ') + ' |'
      )
      const content = [header, separator, ...rows].join('\n')
      downloadBlob(content, `${safeName}_${ts}.md`, 'text/markdown')
      break
    }
    case 'csv': {
      const header = columns.map(c => escapeCsvField(c.label)).join(',')
      const rows = data.map(row =>
        columns.map(col => escapeCsvField(row[col.prop])).join(',')
      )
      const content = [header, ...rows].join('\n')
      downloadBlob(content, `${safeName}_${ts}.csv`, 'text/csv')
      break
    }
    case 'excel': {
      // 轻量方案：生成 HTML table + application/vnd.ms-excel，浏览器原生支持
      const ths = columns.map(c => `<th>${escapeHtml(c.label)}</th>`).join('')
      const trs = data.map(row =>
        '<tr>' + columns.map(col =>
          `<td>${escapeHtml(row[col.prop] ?? '')}</td>`
        ).join('') + '</tr>'
      ).join('')
      const html = `<html xmlns:o="urn:schemas-microsoft-com:office:office" xmlns:x="urn:schemas-microsoft-com:office:excel">
<head><meta charset="utf-8"><!--[if gte mso 9]><xml><x:ExcelWorkbook><x:ExcelWorksheets><x:ExcelWorksheet><x:Name>${escapeHtml(safeName)}</x:Name><x:WorksheetOptions><x:DisplayGridlines/></x:WorksheetOptions></x:ExcelWorksheet></x:ExcelWorksheets></x:ExcelWorkbook></xml><![endif]--></head>
<body><table border="1">${ths}${trs}</table></body></html>`
      downloadBlob(html, `${safeName}_${ts}.xls`, 'application/vnd.ms-excel')
      break
    }
    default:
      alert(`不支持的导出格式：${format}`)
  }
}

function escapeHtml(str) {
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
}
