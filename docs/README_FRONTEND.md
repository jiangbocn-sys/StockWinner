# StockWinner 前端 UI

## 技术栈

- **框架**: Vue 3 (Composition API)
- **构建工具**: Vite
- **UI 组件库**: Element Plus
- **状态管理**: Pinia
- **路由**: Vue Router
- **HTTP 客户端**: Axios (内置 fetch)
- **图表**: ECharts

## 目录结构

```
frontend/
├── src/
│   ├── main.js              # 入口文件
│   ├── App.vue              # 根组件
│   ├── components/          # 公共组件
│   │   └── NavBar.vue       # 导航栏
│   ├── views/               # 页面组件
│   │   ├── Dashboard.vue    # 仪表盘
│   │   ├── Trades.vue       # 交易监控
│   │   ├── Positions.vue    # 持仓分析
│   │   ├── Strategies.vue   # 策略管理
│   │   └── Settings.vue     # 系统设置
│   ├── router/              # 路由配置
│   │   └── index.js
│   └── stores/              # Pinia 状态管理
│       └── account.js       # 账户状态
├── dist/                    # 构建输出
└── package.json
```

## 功能模块

### 1. 导航栏 (NavBar.vue)

**功能**:
- 系统 Logo 和版本号
- 主导航菜单（仪表盘、交易监控、持仓分析、策略管理、系统设置）
- 账户切换下拉框

**账户切换流程**:
1. 用户点击账户下拉框
2. 选择要切换的账户
3. 调用 `accountStore.setCurrentAccount()`
4. 页面自动刷新，加载新账户数据

### 2. 账户状态管理 (account.js)

```javascript
import { useAccountStore } from './stores/account'

const accountStore = useAccountStore()

// 当前账户 ID
const currentAccountId = computed(() => accountStore.currentAccountId)

// 账户列表
const accounts = computed(() => accountStore.accounts)

// 当前账户信息
const currentAccount = computed(() => accountStore.currentAccount)

// 切换账户
accountStore.setCurrentAccount('haoge')

// 加载账户列表
await accountStore.loadAccounts()
```

### 3. 页面路由

| 路径 | 组件 | 说明 |
|------|------|------|
| `/` | 重定向 | 重定向到 `/dashboard` |
| `/dashboard` | Dashboard.vue | 仪表盘 |
| `/trades` | Trades.vue | 交易监控 |
| `/positions` | Positions.vue | 持仓分析 |
| `/strategies` | Strategies.vue | 策略管理 |
| `/settings` | Settings.vue | 系统设置 |

### 4. 仪表盘页面 (Dashboard.vue)

**功能模块**:
- 系统健康度卡片
- 今日交易统计卡片
- 资源开销卡片
- 持仓概览卡片
- 控制面板（刷新、启动服务）

**API 调用**:
```javascript
// 加载仪表盘数据
const response = await fetch(`/api/v1/ui/${currentAccountId.value}/dashboard`)
const data = await response.json()
```

### 5. 交易监控页面 (Trades.vue)

**功能**:
- 今日交易统计卡片
- 交易明细表格
- 日期范围筛选
- 刷新功能

### 6. 持仓分析页面 (Positions.vue)

**功能**:
- 总体概览（总资产、可用资金、持仓市值、总盈亏）
- 持仓明细表格
- 操作按钮（加仓、减仓、清仓）

## 开发

```bash
cd frontend

# 安装依赖
npm install

# 启动开发服务器（代理到后端 8080 端口）
npm run dev

# 构建生产版本
npm run build
```

## 生产部署

```bash
# 构建
npm run build

# 构建产物在 dist/ 目录
# 可通过 Nginx 或 FastAPI 静态文件服务提供
```

### FastAPI 集成静态文件

```python
from fastapi.staticfiles import StaticFiles

app.mount("/static", StaticFiles(directory="frontend/dist"), name="static")
```

## 样式约定

- 盈亏颜色：红色（盈利 `#f56c6c`）、绿色（亏损 `#67c23a`）
- 主色调：Element Plus 默认蓝（`#409EFF`）
- 导航栏背景：`#304156`
