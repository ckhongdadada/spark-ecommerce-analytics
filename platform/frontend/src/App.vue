<template>
  <div class="app-shell" :class="{ 'is-sidebar-collapsed': sidebarCollapsed }">
    <aside class="workspace-sidebar">
      <div class="sidebar-top">
        <button class="brand-card" type="button" @click="activeSection = 'overview'">
          <span class="brand-mark">E</span>
          <span class="brand-text">
            <strong>电商数据平台</strong>
            <small>Spark Analytics Console</small>
          </span>
        </button>

        <button class="sidebar-toggle" type="button" @click="sidebarCollapsed = !sidebarCollapsed">
          {{ sidebarCollapsed ? '>' : '<' }}
        </button>
      </div>

      <nav class="navbar-menu" aria-label="主导航">
        <button
          v-for="item in navItems"
          :key="item.key"
          class="nav-link"
          :class="{ active: activeSection === item.key }"
          type="button"
          :data-label="item.label"
          @click="activeSection = item.key"
        >
          <span class="nav-icon">{{ item.icon }}</span>
          <span class="nav-text">{{ item.label }}</span>
        </button>
      </nav>

      <div class="workspace-status-card">
        <span class="workspace-avatar" :class="health.status">{{ health.status === 'ok' ? 'OK' : '!' }}</span>
        <span class="workspace-summary">
          <span class="note-kicker">API 状态</span>
          <strong>{{ healthLabel }}</strong>
          <small>{{ lastRefreshText }}</small>
        </span>
      </div>
    </aside>

    <div class="workspace-shell">
      <header class="topbar">
        <div>
          <p class="topbar-kicker">Ecommerce full-path data analytics</p>
          <h1>电商全路径数据分析控制台</h1>
        </div>
        <div class="topbar-actions">
          <span class="topbar-pill">课程展示版</span>
          <button class="btn btn-primary" type="button" :disabled="loading" @click="loadDashboard">
            {{ loading ? '刷新中...' : '刷新数据' }}
          </button>
        </div>
      </header>

      <main class="main-content">
        <section class="hero-card">
          <div>
            <span class="eyebrow">当前视图</span>
            <h2>{{ currentSectionTitle }}</h2>
            <p>
              这里把 Spark 批处理、Streaming、数据仓库和平台服务收口到一个展示界面。
              后端优先读取 FastAPI 数据，服务未启动时自动展示演示数据，方便课堂答辩。
            </p>
          </div>
          <div class="hero-metric">
            <strong>{{ formatNumber(summary.totalInteractions) }}</strong>
            <span>累计交互</span>
          </div>
        </section>

        <section id="overview" class="content-section" v-show="activeSection === 'overview'">
          <div class="stats-grid">
            <article v-for="card in statCards" :key="card.label" class="stat-card">
              <span class="stat-kicker">{{ card.kicker }}</span>
              <strong>{{ card.value }}</strong>
              <small>{{ card.label }}</small>
            </article>
          </div>

          <div class="dashboard-grid two-columns">
            <article class="card">
              <div class="card-header">
                <h3>类目热度排行</h3>
                <span class="badge">ADS</span>
              </div>
              <div class="rank-list">
                <div v-for="category in topCategories" :key="category.category" class="rank-item">
                  <span class="rank-index">#{{ category.category_rank || '-' }}</span>
                  <div class="rank-main">
                    <strong>{{ category.category }}</strong>
                    <div class="bar-track">
                      <span class="bar-fill" :style="{ width: metricWidth(category.total_interactions, maxCategoryInteractions) }"></span>
                    </div>
                  </div>
                  <span>{{ formatNumber(category.total_interactions) }}</span>
                </div>
              </div>
            </article>

            <article class="card">
              <div class="card-header">
                <h3>服务健康状态</h3>
                <span class="badge" :class="health.status === 'ok' ? 'badge-success' : 'badge-warning'">{{ health.status }}</span>
              </div>
              <div class="service-grid">
                <div class="service-card">
                  <span>FastAPI</span>
                  <strong>{{ health.status }}</strong>
                </div>
                <div class="service-card">
                  <span>ClickHouse</span>
                  <strong>{{ health.clickhouse }}</strong>
                </div>
                <div class="service-card">
                  <span>MySQL</span>
                  <strong>{{ health.mysql }}</strong>
                </div>
              </div>
              <p class="helper-text">
                前端默认访问 `/api/v1` 与 `/health`，Docker 部署时由 Nginx 转发到 `fastapi:8000`。
              </p>
            </article>
          </div>
        </section>

        <section id="funnel" class="content-section" v-show="activeSection === 'funnel'">
          <article class="card wide-card">
            <div class="card-header">
              <h3>用户行为漏斗</h3>
              <span class="badge">浏览 -> 收藏 -> 加购 -> 购买</span>
            </div>
            <div class="funnel-board">
              <div v-for="step in normalizedFunnel" :key="step.key" class="funnel-step">
                <div class="funnel-shape" :style="{ width: metricWidth(step.count, maxFunnelCount) }">
                  <strong>{{ step.label }}</strong>
                  <span>{{ formatNumber(step.count) }}</span>
                </div>
                <small>相对浏览转化率 {{ step.conversion_rate || 0 }}%</small>
              </div>
            </div>
          </article>
        </section>

        <section id="categories" class="content-section" v-show="activeSection === 'categories'">
          <article class="card wide-card">
            <div class="card-header">
              <h3>类目经营分析</h3>
              <span class="badge">DWS / ADS</span>
            </div>
            <div class="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>排名</th>
                    <th>类目</th>
                    <th>互动数</th>
                    <th>用户数</th>
                    <th>购买数</th>
                    <th>转化率</th>
                    <th>市场份额</th>
                  </tr>
                </thead>
                <tbody>
                  <tr v-for="row in categoryMetrics" :key="row.category">
                    <td>#{{ row.category_rank }}</td>
                    <td>{{ row.category }}</td>
                    <td>{{ formatNumber(row.total_interactions) }}</td>
                    <td>{{ formatNumber(row.total_users) }}</td>
                    <td>{{ formatNumber(row.total_buys) }}</td>
                    <td>{{ row.avg_conversion_rate }}%</td>
                    <td>{{ row.market_share }}%</td>
                  </tr>
                </tbody>
              </table>
            </div>
          </article>
        </section>

        <section id="users" class="content-section" v-show="activeSection === 'users'">
          <article class="card wide-card">
            <div class="card-header">
              <h3>高价值用户</h3>
              <span class="badge">Top Users</span>
            </div>
            <div class="user-grid">
              <div v-for="user in topUsers" :key="user.user_id" class="user-card">
                <span class="user-avatar">{{ user.user_id.slice(-2) }}</span>
                <div>
                  <strong>{{ user.user_id }}</strong>
                  <small>{{ user.user_level }} · 活跃 {{ user.active_days }} 天</small>
                </div>
                <span class="score">{{ user.user_value_score || 0 }}</span>
              </div>
            </div>
          </article>
        </section>

        <section id="realtime" class="content-section" v-show="activeSection === 'realtime'">
          <article class="card wide-card">
            <div class="card-header">
              <h3>实时窗口指标</h3>
              <span class="badge">Streaming</span>
            </div>
            <div class="timeline-list">
              <div v-for="item in realtimeMetrics" :key="`${item.window_start}-${item.category}`" class="timeline-item">
                <span class="timeline-dot"></span>
                <div>
                  <strong>{{ item.category }}</strong>
                  <p>{{ item.window_start }} 至 {{ item.window_end }}</p>
                </div>
                <div class="timeline-stats">
                  <span>{{ formatNumber(item.total_events) }} 事件</span>
                  <span>{{ item.conversion_rate || 0 }}% 转化</span>
                </div>
              </div>
            </div>
          </article>
        </section>

        <section id="alerts" class="content-section" v-show="activeSection === 'alerts'">
          <article class="card wide-card">
            <div class="card-header">
              <h3>告警与质量信号</h3>
              <span class="badge badge-warning">Guard</span>
            </div>
            <div class="alert-list">
              <div v-for="alert in alerts" :key="`${alert.alert_time}-${alert.category}`" class="alert-item">
                <span class="alert-level">{{ alert.alert_level }}</span>
                <div>
                  <strong>{{ alert.category }} · {{ alert.metric_name }}</strong>
                  <p>{{ alert.description }}</p>
                </div>
                <small>{{ alert.alert_time }}</small>
              </div>
            </div>
          </article>
        </section>

        <section id="architecture" class="content-section" v-show="activeSection === 'architecture'">
          <article class="card wide-card">
            <div class="card-header">
              <h3>前后端与数据链路</h3>
              <span class="badge">Architecture</span>
            </div>
            <div class="architecture-flow">
              <span>Vue 前端</span>
              <i></i>
              <span>FastAPI 查询层</span>
              <i></i>
              <span>ClickHouse / MySQL</span>
              <i></i>
              <span>Spark / Flink / Kafka</span>
            </div>
            <p class="helper-text">
              当前新增的是展示前端；已有后端是 FastAPI 查询层。Spark 仍负责离线分析与仓库构建，
              Flink/Kafka/ClickHouse 负责实时和 OLAP 场景展示。
            </p>
          </article>
        </section>
      </main>
    </div>
  </div>
</template>

<script setup>
import { computed, onMounted, ref } from 'vue'
import {
  fetchAlerts,
  fetchCategoryMetrics,
  fetchFunnel,
  fetchHealth,
  fetchRealtimeMetrics,
  fetchTopUsers
} from './api'

const navItems = [
  { key: 'overview', label: '总览', icon: '⌂' },
  { key: 'funnel', label: '行为漏斗', icon: '◇' },
  { key: 'categories', label: '类目分析', icon: '▤' },
  { key: 'users', label: '用户价值', icon: '◎' },
  { key: 'realtime', label: '实时监控', icon: '◷' },
  { key: 'alerts', label: '告警信号', icon: '!' },
  { key: 'architecture', label: '链路架构', icon: '↔' }
]

const sampleCategories = [
  { category: '数码家电', active_days: 7, total_interactions: 128900, total_users: 24310, total_items: 420, total_views: 90200, total_carts: 18420, total_buys: 9280, avg_conversion_rate: 10.29, market_share: 32.6, category_rank: 1 },
  { category: '服饰鞋包', active_days: 7, total_interactions: 103420, total_users: 21120, total_items: 680, total_views: 74200, total_carts: 13940, total_buys: 6180, avg_conversion_rate: 8.33, market_share: 26.1, category_rank: 2 },
  { category: '美妆个护', active_days: 7, total_interactions: 76210, total_users: 15890, total_items: 360, total_views: 53110, total_carts: 10600, total_buys: 4550, avg_conversion_rate: 8.57, market_share: 19.3, category_rank: 3 },
  { category: '食品生鲜', active_days: 7, total_interactions: 53190, total_users: 12440, total_items: 290, total_views: 35540, total_carts: 8020, total_buys: 3920, avg_conversion_rate: 11.03, market_share: 13.4, category_rank: 4 },
  { category: '图书文创', active_days: 7, total_interactions: 33800, total_users: 8310, total_items: 210, total_views: 24100, total_carts: 4210, total_buys: 1680, avg_conversion_rate: 6.97, market_share: 8.6, category_rank: 5 }
]

const sampleUsers = [
  { user_id: 'U10086', user_level: '高价值', active_days: 7, total_behaviors: 186, total_views: 121, total_carts: 42, total_buys: 18, avg_conversion_rate: 14.88, user_value_score: 4.72, last_active_date: '2026-04-16' },
  { user_id: 'U10240', user_level: '高价值', active_days: 6, total_behaviors: 151, total_views: 97, total_carts: 31, total_buys: 12, avg_conversion_rate: 12.37, user_value_score: 3.92, last_active_date: '2026-04-16' },
  { user_id: 'U20480', user_level: '活跃', active_days: 5, total_behaviors: 122, total_views: 88, total_carts: 20, total_buys: 7, avg_conversion_rate: 7.95, user_value_score: 2.66, last_active_date: '2026-04-15' }
]

const sampleRealtime = [
  { window_start: '2026-04-16 10:55', window_end: '2026-04-16 11:00', category: '数码家电', total_events: 5820, unique_users: 1830, view_count: 4120, cart_count: 820, buy_count: 216, fav_count: 664, conversion_rate: 5.24 },
  { window_start: '2026-04-16 10:50', window_end: '2026-04-16 10:55', category: '服饰鞋包', total_events: 4910, unique_users: 1512, view_count: 3508, cart_count: 701, buy_count: 182, fav_count: 519, conversion_rate: 5.19 },
  { window_start: '2026-04-16 10:45', window_end: '2026-04-16 10:50', category: '美妆个护', total_events: 3960, unique_users: 1268, view_count: 2780, cart_count: 602, buy_count: 141, fav_count: 437, conversion_rate: 5.07 }
]

const sampleAlerts = [
  { alert_type: 'conversion', alert_level: 'warning', category: '图书文创', metric_name: 'conversion_rate', metric_value: 6.97, threshold: 7.5, alert_time: '2026-04-16 10:58', description: '图书文创类目转化率低于目标阈值，建议检查加购到购买链路。' },
  { alert_type: 'traffic', alert_level: 'info', category: '食品生鲜', metric_name: 'total_events', metric_value: 53190, threshold: 50000, alert_time: '2026-04-16 10:30', description: '食品生鲜类目流量上升，可结合库存与促销策略继续观察。' }
]

const sampleFunnel = [
  { step_name: 'pv', count: 277150, conversion_rate: 100 },
  { step_name: 'fav', count: 64520, conversion_rate: 23.28 },
  { step_name: 'cart', count: 55190, conversion_rate: 19.91 },
  { step_name: 'buy', count: 25610, conversion_rate: 9.24 }
]

const sidebarCollapsed = ref(false)
const activeSection = ref('overview')
const loading = ref(false)
const lastRefresh = ref(null)
const health = ref({ status: 'demo', clickhouse: 'demo', mysql: 'demo' })
const categoryMetrics = ref(sampleCategories)
const topUsers = ref(sampleUsers)
const realtimeMetrics = ref(sampleRealtime)
const alerts = ref(sampleAlerts)
const funnel = ref(sampleFunnel)

const currentSectionTitle = computed(() => navItems.find(item => item.key === activeSection.value)?.label || '总览')

const healthLabel = computed(() => {
  if (health.value.status === 'ok') return '服务在线'
  if (health.value.status === 'degraded') return '部分服务降级'
  return '演示数据模式'
})

const lastRefreshText = computed(() => {
  if (!lastRefresh.value) return '尚未刷新'
  return `刷新于 ${lastRefresh.value.toLocaleTimeString('zh-CN', { hour12: false })}`
})

const summary = computed(() => {
  const totalInteractions = categoryMetrics.value.reduce((sum, item) => sum + Number(item.total_interactions || 0), 0)
  const totalUsers = categoryMetrics.value.reduce((sum, item) => sum + Number(item.total_users || 0), 0)
  const totalBuys = categoryMetrics.value.reduce((sum, item) => sum + Number(item.total_buys || 0), 0)
  const avgConversion = categoryMetrics.value.length
    ? categoryMetrics.value.reduce((sum, item) => sum + Number(item.avg_conversion_rate || 0), 0) / categoryMetrics.value.length
    : 0
  return { totalInteractions, totalUsers, totalBuys, avgConversion }
})

const statCards = computed(() => [
  { kicker: 'Traffic', label: '全站行为事件', value: formatNumber(summary.value.totalInteractions) },
  { kicker: 'Users', label: '覆盖用户数', value: formatNumber(summary.value.totalUsers) },
  { kicker: 'Orders', label: '购买行为数', value: formatNumber(summary.value.totalBuys) },
  { kicker: 'CVR', label: '平均转化率', value: `${summary.value.avgConversion.toFixed(2)}%` }
])

const topCategories = computed(() => categoryMetrics.value.slice(0, 5))
const maxCategoryInteractions = computed(() => Math.max(...categoryMetrics.value.map(item => Number(item.total_interactions || 0)), 1))

const normalizedFunnel = computed(() => {
  const labels = { pv: '浏览', fav: '收藏', cart: '加购', buy: '购买' }
  const order = ['pv', 'fav', 'cart', 'buy']
  return order.map(key => {
    const row = funnel.value.find(item => item.step_name === key) || { step_name: key, count: 0, conversion_rate: 0 }
    return { key, label: labels[key], count: Number(row.count || 0), conversion_rate: row.conversion_rate }
  })
})

const maxFunnelCount = computed(() => Math.max(...normalizedFunnel.value.map(item => item.count), 1))

const formatNumber = value => new Intl.NumberFormat('zh-CN').format(Number(value || 0))
const metricWidth = (value, max) => `${Math.max(16, Math.round(Number(value || 0) / Number(max || 1) * 100))}%`

const loadDashboard = async () => {
  loading.value = true
  try {
    const [healthRes, categoryRes, userRes, realtimeRes, alertRes, funnelRes] = await Promise.all([
      fetchHealth(),
      fetchCategoryMetrics(),
      fetchTopUsers(),
      fetchRealtimeMetrics(),
      fetchAlerts(),
      fetchFunnel()
    ])

    health.value = healthRes.data
    categoryMetrics.value = categoryRes.data.length ? categoryRes.data : sampleCategories
    topUsers.value = userRes.data.length ? userRes.data : sampleUsers
    realtimeMetrics.value = realtimeRes.data.length ? realtimeRes.data : sampleRealtime
    alerts.value = alertRes.data.length ? alertRes.data : sampleAlerts
    funnel.value = funnelRes.data.funnel?.length ? funnelRes.data.funnel : sampleFunnel
  } catch (error) {
    health.value = { status: 'demo', clickhouse: 'demo', mysql: 'demo' }
    categoryMetrics.value = sampleCategories
    topUsers.value = sampleUsers
    realtimeMetrics.value = sampleRealtime
    alerts.value = sampleAlerts
    funnel.value = sampleFunnel
  } finally {
    lastRefresh.value = new Date()
    loading.value = false
  }
}

onMounted(loadDashboard)
</script>
